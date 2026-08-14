package shard

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	reloadTestCell    = "cell1"
	reloadTestDesired = "R2" // desired reload-hash
	reloadTestRestart = "S1" // desired restart-hash
	reloadTestPod     = "pool-pod-0"
)

func reloadTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add multigres scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	return s
}

func reloadTestShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shard0",
			Namespace: "ns",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "clu"},
			Annotations: map[string]string{
				metadata.AnnotationPostgresReloadHash: reloadTestDesired,
				metadata.AnnotationPostgresConfigHash: reloadTestRestart,
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "0",
		},
	}
}

// reloadTestPodObj builds a pool pod carrying the shard's selector labels plus
// the given reload-hash / restart-hash annotations.
func reloadTestPodObj(reloadHash, restartHash string, extra map[string]string) *corev1.Pod {
	ann := map[string]string{}
	if reloadHash != "" {
		ann[metadata.AnnotationPostgresReloadHash] = reloadHash
	}
	if restartHash != "" {
		ann[metadata.AnnotationPostgresConfigHash] = restartHash
	}
	maps.Copy(ann, extra)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reloadTestPod,
			Namespace: "ns",
			Labels: map[string]string{
				metadata.LabelMultigresCluster:    "clu",
				metadata.LabelMultigresDatabase:   "db",
				metadata.LabelMultigresTableGroup: "tg",
				metadata.LabelMultigresShard:      "0",
				metadata.LabelMultigresCell:       reloadTestCell,
			},
			Annotations: ann,
		},
	}
}

// syncedConfigMap returns the operator-owned ConfigMap already stamped with the
// desired reload target far enough in the past that the sync-lag gate is open.
func syncedConfigMap() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PostgresConfigMapName("shard0"),
			Namespace: "ns",
			Annotations: map[string]string{
				metadata.AnnotationPostgresReloadHash: reloadTestDesired,
				metadata.AnnotationPostgresReloadHashUpdatedAt: time.Now().
					UTC().Add(-2 * reloadConfigSyncCeiling).Format(time.RFC3339),
			},
		},
	}
}

// reloadTestStore returns a memory-topo store with the pool pod's pooler
// registered, and the pooler-id key the FakeClient uses for that pooler.
func reloadTestStore(t *testing.T) (topoclient.Store, topoclient.ComponentID) {
	t.Helper()
	_, factory := memorytopo.NewServerAndFactory(context.Background(), reloadTestCell)
	store := topoclient.NewWithFactory(
		factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
	)
	id := &clustermetadata.ID{Cell: reloadTestCell, Name: reloadTestPod}
	if err := store.RegisterMultipooler(context.Background(), &clustermetadata.Multipooler{
		Id:       id,
		Hostname: reloadTestPod,
		ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
	}, false); err != nil {
		t.Fatalf("register pooler: %v", err)
	}
	return store, topoclient.ComponentIDString(id)
}

func newReloadReconciler(
	scheme *runtime.Scheme,
	rpc rpcclient.MultipoolerClient,
	objs ...client.Object,
) *ShardReconciler {
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &ShardReconciler{
		Client:    c,
		Scheme:    scheme,
		Recorder:  record.NewFakeRecorder(20),
		RPCClient: rpc,
	}
}

func callLogHas(log []string, method string) bool {
	for _, e := range log {
		if len(e) >= len(method) && e[:len(method)] == method {
			return true
		}
	}
	return false
}

func TestReconcileReloadStateReloadsStalePod(t *testing.T) {
	scheme := reloadTestScheme(t)
	shard := reloadTestShard()
	store, poolerID := reloadTestStore(t)
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	rpc.ReloadConfigResponses[poolerID] = &multipoolermanagerdatapb.ReloadConfigResponse{
		ConfigLoadTime: timestamppb.Now(),
	}

	pod := reloadTestPodObj("R1", reloadTestRestart, nil) // stale reload-hash, current restart-hash
	r := newReloadReconciler(scheme, rpc, shard, syncedConfigMap(), pod)

	wait, err := r.reconcileReloadState(context.Background(), store, shard)
	if err != nil {
		t.Fatalf("reconcileReloadState: %v", err)
	}
	if wait != 0 {
		t.Errorf("wait = %v, want 0 (reload completed)", wait)
	}
	if !callLogHas(rpc.GetCallLog(), "ReloadConfig") {
		t.Errorf("ReloadConfig was not called; call log = %v", rpc.GetCallLog())
	}

	got := &corev1.Pod{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: reloadTestPod}, got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if h := got.Annotations[metadata.AnnotationPostgresReloadHash]; h != reloadTestDesired {
		t.Errorf("pod reload-hash = %q, want %q (stamped after reload)", h, reloadTestDesired)
	}
}

func TestReconcileReloadStatePostgresNotRunning(t *testing.T) {
	scheme := reloadTestScheme(t)
	shard := reloadTestShard()
	store, _ := reloadTestStore(t)
	defer func() { _ = store.Close() }()

	// No ReloadConfigResponse seeded → FakeClient returns an empty response
	// (nil config_load_time), the "postgres not running" case.
	rpc := rpcclient.NewFakeClient()

	pod := reloadTestPodObj("R1", reloadTestRestart, nil)
	r := newReloadReconciler(scheme, rpc, shard, syncedConfigMap(), pod)

	wait, err := r.reconcileReloadState(context.Background(), store, shard)
	if err != nil {
		t.Fatalf("reconcileReloadState: %v", err)
	}
	if wait != reloadRetryDelay {
		t.Errorf("wait = %v, want %v (retry)", wait, reloadRetryDelay)
	}
	got := &corev1.Pod{}
	_ = r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: reloadTestPod}, got)
	if h := got.Annotations[metadata.AnnotationPostgresReloadHash]; h == reloadTestDesired {
		t.Errorf("pod reload-hash was stamped despite no reload (postgres down)")
	}
}

func TestReconcileReloadStateSkips(t *testing.T) {
	cases := []struct {
		name       string
		reloadHash string
		restart    string
		extraAnn   map[string]string
	}{
		{"already current", reloadTestDesired, reloadTestRestart, nil},
		{"draining", "R1", reloadTestRestart, map[string]string{metadata.AnnotationDrainState: metadata.DrainStateRequested}},
		{"restart pending", "R1", "S0", nil}, // restart-hash stale → recreation owns it
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := reloadTestScheme(t)
			shard := reloadTestShard()
			store, poolerID := reloadTestStore(t)
			defer func() { _ = store.Close() }()

			rpc := rpcclient.NewFakeClient()
			rpc.ReloadConfigResponses[poolerID] = &multipoolermanagerdatapb.ReloadConfigResponse{
				ConfigLoadTime: timestamppb.Now(),
			}

			pod := reloadTestPodObj(tc.reloadHash, tc.restart, tc.extraAnn)
			r := newReloadReconciler(scheme, rpc, shard, syncedConfigMap(), pod)

			if _, err := r.reconcileReloadState(context.Background(), store, shard); err != nil {
				t.Fatalf("reconcileReloadState: %v", err)
			}
			if callLogHas(rpc.GetCallLog(), "ReloadConfig") {
				t.Errorf("ReloadConfig should not be called for %q; call log = %v", tc.name, rpc.GetCallLog())
			}
		})
	}
}

func TestReloadTargetSynced(t *testing.T) {
	scheme := reloadTestScheme(t)
	shard := reloadTestShard()
	ctx := context.Background()

	t.Run("target not yet stamped by delivery retries (read-only)", func(t *testing.T) {
		// A ConfigMap the delivery path has not stamped yet (no reload
		// annotations): reloadTargetSynced must NOT write to it and must ask to
		// retry until delivery stamps the target.
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: PostgresConfigMapName("shard0"), Namespace: "ns",
		}}
		r := newReloadReconciler(scheme, nil, cm)
		ready, wait, err := r.reloadTargetSynced(ctx, shard, reloadTestDesired)
		if err != nil {
			t.Fatal(err)
		}
		if ready || wait != reloadRetryDelay {
			t.Errorf("ready=%v wait=%v, want ready=false wait=%v", ready, wait, reloadRetryDelay)
		}
		got := &corev1.ConfigMap{}
		_ = r.Get(ctx, client.ObjectKey{Namespace: "ns", Name: PostgresConfigMapName("shard0")}, got)
		if got.Annotations[metadata.AnnotationPostgresReloadHash] != "" {
			t.Errorf("reloadTargetSynced must not write the ConfigMap; got annotations=%v", got.Annotations)
		}
	})

	t.Run("within ceiling reports remaining wait", func(t *testing.T) {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: PostgresConfigMapName("shard0"), Namespace: "ns",
			Annotations: map[string]string{
				metadata.AnnotationPostgresReloadHash:          reloadTestDesired,
				metadata.AnnotationPostgresReloadHashUpdatedAt: time.Now().UTC().Format(time.RFC3339),
			},
		}}
		r := newReloadReconciler(scheme, nil, cm)
		ready, wait, err := r.reloadTargetSynced(ctx, shard, reloadTestDesired)
		if err != nil {
			t.Fatal(err)
		}
		if ready || wait <= 0 || wait > reloadConfigSyncCeiling {
			t.Errorf("ready=%v wait=%v, want ready=false 0<wait<=ceiling", ready, wait)
		}
	})

	t.Run("after ceiling is ready", func(t *testing.T) {
		r := newReloadReconciler(scheme, nil, syncedConfigMap())
		ready, wait, err := r.reloadTargetSynced(ctx, shard, reloadTestDesired)
		if err != nil {
			t.Fatal(err)
		}
		if !ready || wait != 0 {
			t.Errorf("ready=%v wait=%v, want ready=true wait=0", ready, wait)
		}
	})

	t.Run("stale target on ConfigMap is not yet ready", func(t *testing.T) {
		// ConfigMap still stamped for an OLD target (delivery has not yet stamped
		// the new desired): even though its timestamp is old, the target does not
		// match desired, so it is not ready — retry until delivery catches up.
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: PostgresConfigMapName("shard0"), Namespace: "ns",
			Annotations: map[string]string{
				metadata.AnnotationPostgresReloadHash: "R1",
				metadata.AnnotationPostgresReloadHashUpdatedAt: time.Now().
					UTC().Add(-2 * reloadConfigSyncCeiling).Format(time.RFC3339),
			},
		}}
		r := newReloadReconciler(scheme, nil, cm)
		ready, wait, err := r.reloadTargetSynced(ctx, shard, reloadTestDesired)
		if err != nil {
			t.Fatal(err)
		}
		if ready || wait != reloadRetryDelay {
			t.Errorf("ready=%v wait=%v, want ready=false wait=%v (target mismatch)", ready, wait, reloadRetryDelay)
		}
	})

	t.Run("missing ConfigMap retries", func(t *testing.T) {
		r := newReloadReconciler(scheme, nil) // no ConfigMap
		ready, wait, err := r.reloadTargetSynced(ctx, shard, reloadTestDesired)
		if err != nil {
			t.Fatal(err)
		}
		if ready || wait != reloadRetryDelay {
			t.Errorf("ready=%v wait=%v, want ready=false wait=%v", ready, wait, reloadRetryDelay)
		}
	})
}
