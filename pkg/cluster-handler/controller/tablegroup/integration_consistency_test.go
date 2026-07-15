//go:build integration
// +build integration

package tablegroup_test

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/testutil"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

// TestTableGroup_ConsistencyConvergence runs the controller against a real
// apiserver and cache (envtest) and asserts it converges to the correct terminal
// state (children, status, ObservedGeneration) without resourceVersion conflicts
// or status flapping, including across add/remove scenarios. A fake client can't
// model cache lag or conflicts, so this is the envtest counterpart to the unit
// tests.
func TestTableGroup_ConsistencyConvergence(t *testing.T) {
	t.Parallel()

	globalTopo := multigresv1alpha1.GlobalTopoServerRef{
		Address:        "etcd-client:2379",
		RootPath:       "/multigres/global",
		Implementation: "etcd",
	}

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths("../../../../config/crd/bases"),
	)

	if err := (&tablegroup.TableGroupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
		t.Fatalf("Failed to set up controller: %v", err)
	}

	watcher := testutil.NewResourceWatcher(t, t.Context(), mgr,
		testutil.WithCmpOpts(testutil.IgnoreMetaRuntimeFields()),
		testutil.WithExtraResource(
			&multigresv1alpha1.TableGroup{},
			&multigresv1alpha1.Shard{},
		),
		testutil.WithTimeout(15*time.Second),
	)

	k8sClient := mgr.GetClient()
	ctx := t.Context()

	const (
		clusterName = "consistency-cluster"
		dbName      = "db1"
		tgName      = "tg1"
		namespace   = "default"
	)

	shardSpec := func(name, cell string) multigresv1alpha1.ShardResolvedSpec {
		return multigresv1alpha1.ShardResolvedSpec{
			Name: name,
			Multiorch: multigresv1alpha1.MultiorchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
				Cells:         []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cell)},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Type:            "readWrite",
					ReplicasPerCell: ptr.To(int32(1)),
					Cells:           []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cell)},
				},
			},
		}
	}

	childName := func(shard string) string {
		return nameutil.JoinWithConstraints(
			nameutil.DefaultConstraints, clusterName, dbName, tgName, shard,
		)
	}

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-consistency",
			Namespace: namespace,
			Labels:    map[string]string{"multigres.com/cluster": clusterName},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:     dbName,
			TableGroupName:   tgName,
			GlobalTopoServer: globalTopo,
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "orch:latest",
				Multipooler: "pooler:latest",
				Postgres:    "postgres:15",
			},
			Shards: []multigresv1alpha1.ShardResolvedSpec{
				shardSpec("s1", "zone-a"),
				shardSpec("s2", "zone-b"),
				shardSpec("s3", "zone-c"),
			},
		},
	}
	setTestPostgresPasswordSecretRef(tg)

	// The initial three-shard spec establishes a healthy terminal baseline under
	// a real apiserver and cache before the test introduces an add/remove change.
	if err := k8sClient.Create(ctx, tg); err != nil {
		t.Fatalf("Failed to create TableGroup: %v", err)
	}

	waitForChildShards(t, ctx, k8sClient, namespace, clusterName, dbName, tgName,
		[]string{childName("s1"), childName("s2"), childName("s3")})

	// TableGroup readiness depends on child status, not child existence alone.
	// The test drives the Shard status the way the Shard controller would.
	for _, shard := range []string{"s1", "s2", "s3"} {
		driveShardHealthy(t, ctx, k8sClient, childName(shard), namespace)
	}

	healthy := waitForTableGroup(t, ctx, k8sClient,
		client.ObjectKeyFromObject(tg), 15*time.Second,
		func(g *multigresv1alpha1.TableGroup) bool {
			return g.Status.Phase == multigresv1alpha1.PhaseHealthy &&
				g.Status.ReadyShards == 3 &&
				g.Status.TotalShards == 3 &&
				g.Status.ObservedGeneration == g.Generation &&
				isConditionTrue(g.Status.Conditions, "Available")
		},
	)

	// A reconciled terminal state should remain quiet across repeated watches:
	// no status flapping, stale generations, or cache-conflict churn.
	stableGen := healthy.Status.ObservedGeneration
	assertTableGroupStaysStable(t, ctx, k8sClient, client.ObjectKeyFromObject(tg),
		2*time.Second, func(g *multigresv1alpha1.TableGroup) bool {
			return g.Status.Phase == multigresv1alpha1.PhaseHealthy &&
				g.Status.ObservedGeneration == stableGen
		})

	// The add/remove update forces the controller to create one desired child,
	// retire one orphan through PendingDeletion, and keep status truthful while
	// both old and new children may briefly exist.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &multigresv1alpha1.TableGroup{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tg), latest); err != nil {
			return err
		}
		latest.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{
			shardSpec("s1", "zone-a"),
			shardSpec("s2", "zone-b"),
			shardSpec("s4", "zone-d"),
		}
		return k8sClient.Update(ctx, latest)
	}); err != nil {
		t.Fatalf("Failed to update TableGroup spec: %v", err)
	}

	// The old child can still exist while it drains, so the useful assertion here
	// is that the new desired child appears, not that the child set is already
	// exact.
	waitForShardExists(t, ctx, k8sClient, childName("s4"), namespace)

	// The orphan remains visible until it acknowledges PendingDeletion. That
	// retained object is what lets the child controller drain before deletion.
	s3Name := childName("s3")
	waitForShardAnnotation(t, ctx, k8sClient, s3Name, namespace,
		multigresv1alpha1.AnnotationPendingDeletion)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &multigresv1alpha1.Shard{}
		if err := k8sClient.Get(
			ctx,
			client.ObjectKey{Name: s3Name, Namespace: namespace},
			latest,
		); err != nil {
			return err
		}
		cond := metav1.Condition{
			Type:               multigresv1alpha1.ConditionReadyForDeletion,
			Status:             metav1.ConditionTrue,
			Reason:             "DrainComplete",
			Message:            "Simulated by integration test",
			LastTransitionTime: metav1.Now(),
		}
		latest.Status.Conditions = append(latest.Status.Conditions, cond)
		return k8sClient.Status().Update(ctx, latest)
	}); err != nil {
		t.Fatalf("Failed to set ReadyForDeletion on orphan shard: %v", err)
	}

	if err := watcher.WaitForDeletion(&multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: s3Name, Namespace: namespace},
	}); err != nil {
		t.Fatalf("Orphan shard s3 was not pruned: %v", err)
	}

	// Once the replacement child reports Healthy, the parent should converge to a
	// stable terminal status for the new generation.
	for _, shard := range []string{"s1", "s2", "s4"} {
		driveShardHealthy(t, ctx, k8sClient, childName(shard), namespace)
	}

	reconverged := waitForTableGroup(t, ctx, k8sClient,
		client.ObjectKeyFromObject(tg), 15*time.Second,
		func(g *multigresv1alpha1.TableGroup) bool {
			return g.Status.Phase == multigresv1alpha1.PhaseHealthy &&
				g.Status.ReadyShards == 3 &&
				g.Status.TotalShards == 3 &&
				g.Status.ObservedGeneration == g.Generation &&
				isConditionTrue(g.Status.Conditions, "Available")
		},
	)

	finalGen := reconverged.Status.ObservedGeneration
	assertTableGroupStaysStable(t, ctx, k8sClient, client.ObjectKeyFromObject(tg),
		2*time.Second, func(g *multigresv1alpha1.TableGroup) bool {
			return g.Status.Phase == multigresv1alpha1.PhaseHealthy &&
				g.Status.ObservedGeneration == finalGen
		})
}

// waitForChildShards polls until exactly the expected child Shard names exist
// for the given cluster/database/tablegroup labels (the same single label
// selector the controller reads with).
func waitForChildShards(
	t testing.TB,
	ctx context.Context,
	k8sClient client.Client,
	namespace, cluster, db, tg string,
	wantNames []string,
) {
	t.Helper()

	want := make(map[string]bool, len(wantNames))
	for _, n := range wantNames {
		want[n] = true
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		list := &multigresv1alpha1.ShardList{}
		if err := k8sClient.List(ctx, list,
			client.InNamespace(namespace),
			client.MatchingLabels{
				"multigres.com/cluster":    cluster,
				"multigres.com/database":   db,
				"multigres.com/tablegroup": tg,
			},
		); err == nil {
			got := make(map[string]bool, len(list.Items))
			for i := range list.Items {
				got[list.Items[i].Name] = true
			}
			if mapsEqual(want, got) {
				return
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for child shards %v", wantNames)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// driveShardHealthy simulates the Shard controller reporting a Healthy phase with
// ObservedGeneration matching the spec generation, which is the condition the
// TableGroup controller requires to count a child as ready.
func driveShardHealthy(
	t testing.TB,
	ctx context.Context,
	k8sClient client.Client,
	name, namespace string,
) {
	t.Helper()

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &multigresv1alpha1.Shard{}
		if err := k8sClient.Get(
			ctx,
			client.ObjectKey{Name: name, Namespace: namespace},
			latest,
		); err != nil {
			return err
		}
		latest.Status.Phase = multigresv1alpha1.PhaseHealthy
		latest.Status.Message = "Ready"
		latest.Status.ObservedGeneration = latest.Generation
		return k8sClient.Status().Update(ctx, latest)
	}); err != nil {
		t.Fatalf("Failed to drive shard %s healthy: %v", name, err)
	}
}

// waitForShardExists polls until the named Shard exists.
func waitForShardExists(
	t testing.TB,
	ctx context.Context,
	k8sClient client.Client,
	name, namespace string,
) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		s := &multigresv1alpha1.Shard{}
		if err := k8sClient.Get(
			ctx,
			client.ObjectKey{Name: name, Namespace: namespace},
			s,
		); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for shard %s to exist", name)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitForShardAnnotation polls until the named Shard carries a non-empty value
// for the given annotation key.
func waitForShardAnnotation(
	t testing.TB,
	ctx context.Context,
	k8sClient client.Client,
	name, namespace, annotation string,
) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		s := &multigresv1alpha1.Shard{}
		if err := k8sClient.Get(
			ctx,
			client.ObjectKey{Name: name, Namespace: namespace},
			s,
		); err == nil {
			if s.Annotations[annotation] != "" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for shard %s annotation %q", name, annotation)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitForTableGroup polls until the TableGroup satisfies the predicate, returning
// the matching object. It fails the test on timeout.
func waitForTableGroup(
	t testing.TB,
	ctx context.Context,
	k8sClient client.Client,
	key client.ObjectKey,
	timeout time.Duration,
	predicate func(*multigresv1alpha1.TableGroup) bool,
) *multigresv1alpha1.TableGroup {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last multigresv1alpha1.TableGroup
	for {
		g := &multigresv1alpha1.TableGroup{}
		if err := k8sClient.Get(ctx, key, g); err == nil {
			last = *g
			if predicate(g) {
				return g
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"timed out waiting for TableGroup to converge; last observed phase=%q ready=%d total=%d observedGen=%d gen=%d",
				last.Status.Phase,
				last.Status.ReadyShards,
				last.Status.TotalShards,
				last.Status.ObservedGeneration,
				last.Generation,
			)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// assertTableGroupStaysStable verifies the TableGroup continues to satisfy the
// predicate across a settle window, guarding against status flapping under a
// real cache.
func assertTableGroupStaysStable(
	t testing.TB,
	ctx context.Context,
	k8sClient client.Client,
	key client.ObjectKey,
	window time.Duration,
	predicate func(*multigresv1alpha1.TableGroup) bool,
) {
	t.Helper()

	deadline := time.Now().Add(window)
	for {
		g := &multigresv1alpha1.TableGroup{}
		if err := k8sClient.Get(ctx, key, g); err != nil {
			t.Fatalf("Failed to get TableGroup during stability check: %v", err)
		}
		if !predicate(g) {
			t.Fatalf(
				"TableGroup left its stable terminal state: phase=%q observedGen=%d gen=%d",
				g.Status.Phase, g.Status.ObservedGeneration, g.Generation,
			)
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func isConditionTrue(conditions []metav1.Condition, condType string) bool {
	for i := range conditions {
		if conditions[i].Type == condType {
			return conditions[i].Status == metav1.ConditionTrue
		}
	}
	return false
}

func mapsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
