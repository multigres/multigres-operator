package shard

import (
	"context"
	"maps"
	"testing"

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

// reloadTestRendered is the once-per-reconcile render result the reload step
// consumes: the desired reload-hash and the reload-safe settings it passes to
// the RPC as expected_settings.
func reloadTestRendered() renderedConfig {
	return renderedConfig{
		hash:           reloadTestRestart,
		reloadHash:     reloadTestDesired,
		reloadSettings: map[string]string{"work_mem": "32MB"},
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
				metadata.LabelAppComponent:        PoolComponentName,
			},
			Annotations: ann,
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

func podReloadHash(t *testing.T, r *ShardReconciler) string {
	t.Helper()
	got := &corev1.Pod{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: reloadTestPod}, got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	return got.Annotations[metadata.AnnotationPostgresReloadHash]
}

// TestReconcileReloadStateStampsWhenVerified: the RPC confirms the reload took
// effect (config_load_time set), so the pod is stamped current.
func TestReconcileReloadStateStampsWhenVerified(t *testing.T) {
	scheme := reloadTestScheme(t)
	shard := reloadTestShard()
	store, poolerID := reloadTestStore(t)
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	rpc.ReloadConfigResponses[poolerID] = &multipoolermanagerdatapb.ReloadConfigResponse{
		ConfigLoadTime: timestamppb.Now(),
	}

	pod := reloadTestPodObj("R1", reloadTestRestart, nil) // stale reload-hash, current restart-hash
	r := newReloadReconciler(scheme, rpc, shard, pod)

	wait, err := r.reconcileReloadState(context.Background(), store, shard, reloadTestRendered())
	if err != nil {
		t.Fatalf("reconcileReloadState: %v", err)
	}
	if wait != 0 {
		t.Errorf("wait = %v, want 0 (reload completed)", wait)
	}
	if !callLogHas(rpc.GetCallLog(), "ReloadConfig") {
		t.Errorf("ReloadConfig was not called; call log = %v", rpc.GetCallLog())
	}
	if h := podReloadHash(t, r); h != reloadTestDesired {
		t.Errorf("pod reload-hash = %q, want %q (stamped after verified reload)", h, reloadTestDesired)
	}
}

// TestReconcileReloadStateNotSyncedRetries: the RPC returns no config_load_time
// (mounted file not yet caught up, or postgres down) — the pod must NOT be
// stamped and the step must requeue.
func TestReconcileReloadStateNotSyncedRetries(t *testing.T) {
	scheme := reloadTestScheme(t)
	shard := reloadTestShard()
	store, poolerID := reloadTestStore(t)
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	// No config_load_time, a plain mismatch (file still carries the old value).
	rpc.ReloadConfigResponses[poolerID] = &multipoolermanagerdatapb.ReloadConfigResponse{
		Mismatches: []*multipoolermanagerdatapb.SettingMismatch{{Name: "work_mem"}},
	}

	pod := reloadTestPodObj("R1", reloadTestRestart, nil)
	r := newReloadReconciler(scheme, rpc, shard, pod)

	wait, err := r.reconcileReloadState(context.Background(), store, shard, reloadTestRendered())
	if err != nil {
		t.Fatalf("reconcileReloadState: %v", err)
	}
	if wait != reloadRetryDelay {
		t.Errorf("wait = %v, want %v (retry until file syncs)", wait, reloadRetryDelay)
	}
	if h := podReloadHash(t, r); h == reloadTestDesired {
		t.Errorf("pod reload-hash was stamped despite an unsynced file")
	}
}

// TestReconcileReloadStateNeedsRestart: a reload-classified setting actually
// needs a restart — surfaced (requeue), not stamped.
func TestReconcileReloadStateNeedsRestart(t *testing.T) {
	scheme := reloadTestScheme(t)
	shard := reloadTestShard()
	store, poolerID := reloadTestStore(t)
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	rpc.ReloadConfigResponses[poolerID] = &multipoolermanagerdatapb.ReloadConfigResponse{
		NeedsRestart: true,
		Mismatches:   []*multipoolermanagerdatapb.SettingMismatch{{Name: "work_mem", RequiresRestart: true}},
	}

	pod := reloadTestPodObj("R1", reloadTestRestart, nil)
	r := newReloadReconciler(scheme, rpc, shard, pod)

	wait, err := r.reconcileReloadState(context.Background(), store, shard, reloadTestRendered())
	if err != nil {
		t.Fatalf("reconcileReloadState: %v", err)
	}
	if wait != reloadRetryDelay {
		t.Errorf("wait = %v, want %v", wait, reloadRetryDelay)
	}
	if h := podReloadHash(t, r); h == reloadTestDesired {
		t.Errorf("pod reload-hash was stamped despite needs_restart")
	}
}

// TestReconcileReloadStateSkips: pods that are already current, draining, or
// restart-pending are never sent to the RPC.
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
			r := newReloadReconciler(scheme, rpc, shard, pod)

			if _, err := r.reconcileReloadState(context.Background(), store, shard, reloadTestRendered()); err != nil {
				t.Fatalf("reconcileReloadState: %v", err)
			}
			if callLogHas(rpc.GetCallLog(), "ReloadConfig") {
				t.Errorf("ReloadConfig should not be called for %q; call log = %v", tc.name, rpc.GetCallLog())
			}
		})
	}
}
