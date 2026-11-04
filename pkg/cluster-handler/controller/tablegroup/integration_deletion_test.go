//go:build integration
// +build integration

package tablegroup_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/testutil"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

// completionCounter wraps a reconcile.Reconciler to count completed passes, so
// a test can poll for reconcile activity to go quiet instead of sleeping a
// fixed duration and hoping it was long enough.
type completionCounter struct {
	reconcile.Reconciler
	completed atomic.Int64
}

func (c *completionCounter) Reconcile(
	ctx context.Context,
	req reconcile.Request,
) (reconcile.Result, error) {
	res, err := c.Reconciler.Reconcile(ctx, req)
	c.completed.Add(1)
	return res, err
}

// waitForReconcilesToSettle blocks until completed stops increasing for a full
// debounce window, or fails the test if that never happens within timeout.
func waitForReconcilesToSettle(
	t *testing.T,
	completed *atomic.Int64,
	timeout time.Duration,
) {
	t.Helper()
	const (
		debounce = 500 * time.Millisecond
		poll     = 50 * time.Millisecond
	)
	deadline := time.Now().Add(timeout)
	last := completed.Load()
	quietSince := time.Now()
	for {
		time.Sleep(poll)
		cur := completed.Load()
		if cur != last {
			last = cur
			quietSince = time.Now()
		}
		if time.Since(quietSince) >= debounce {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reconciles never settled: %d completed, still active after %s", cur, timeout)
		}
	}
}

// TestTableGroup_PendingDeletionAnnotationAloneEnqueues asserts that setting
// only the PendingDeletion annotation on a TableGroup, with nothing else about
// it changing, is enough for the controller's own watch to enqueue it and for
// stepHandlePendingDeletion to run. It deliberately never calls Reconcile
// directly: the bug this pins lives in the predicate the manager's watch
// evaluates before a request ever reaches Reconcile, so a direct call would
// pass regardless of the predicate and prove nothing.
//
// Only TableGroupReconciler is registered against this envtest manager, so
// the child Shard never changes on its own once created: no Shard controller
// runs to touch it, and nothing else in this test writes to it. That closes
// off the confound seen in production and in test/suite, where an unrelated
// update to the owned Shard (Owns() events are not filtered by the For
// predicate) can re-enqueue the parent well after the annotation was set,
// making the handshake appear to start even though the annotation-only event
// was itself dropped. Here, the only path that can deliver the child's own
// PendingDeletion annotation is the TableGroup's own For predicate letting
// the annotation-only update event through.
func TestTableGroup_PendingDeletionAnnotationAloneEnqueues(t *testing.T) {
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

	tableGroupReconciler := &tablegroup.TableGroupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}
	counter := &completionCounter{Reconciler: tableGroupReconciler}
	if err := tableGroupReconciler.SetupWithManagerReconciler(
		mgr, counter, controller.Options{SkipNameValidation: ptr.To(true)},
	); err != nil {
		t.Fatal(err)
	}

	watcher := testutil.NewResourceWatcher(t, t.Context(), mgr,
		testutil.WithCmpOpts(testutil.IgnoreMetaRuntimeFields()),
		testutil.WithExtraResource(&multigresv1alpha1.Shard{}),
		testutil.WithTimeout(10*time.Second),
	)
	watcher.SetCmpOpts(testutil.CompareSpecOnly()...)
	k8sClient := mgr.GetClient()
	ctx := t.Context()

	const (
		clusterName = "annotation-enqueue-cluster"
		dbName      = "db1"
		tgName      = "tg1"
		namespace   = "default"
		shardName   = "only-shard"
	)

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-annotation-enqueue",
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
				{
					Name: shardName,
					Multiorch: multigresv1alpha1.MultiorchSpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
				},
			},
		},
	}
	setTestPostgresPasswordSecretRef(tg)
	if err := k8sClient.Create(ctx, tg); err != nil {
		t.Fatal(err)
	}

	childName := nameutil.JoinWithConstraints(
		nameutil.DefaultConstraints, clusterName, dbName, tgName, shardName,
	)
	expectedShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: childName, Namespace: namespace},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     dbName,
			TableGroupName:   tgName,
			ShardName:        shardName,
			GlobalTopoServer: globalTopo,
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "orch:latest",
				Multipooler: "pooler:latest",
				Postgres:    "postgres:15",
			},
			Multiorch: multigresv1alpha1.MultiorchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
			Pools:    map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
			Replicas: ptr.To(int32(0)),
		},
	}
	setTestShardPostgresPasswordSecretRef(expectedShard)
	if err := watcher.WaitForMatch(expectedShard); err != nil {
		t.Fatalf("child Shard was not created: %v", err)
	}

	// Let the reconcile the Shard's own creation triggers through Owns(Shard)
	// (unfiltered by any predicate) fully drain before touching the parent.
	// Otherwise that unrelated, already in-flight pass could re-read the
	// TableGroup after the annotation patch below lands and satisfy this test
	// for the wrong reason, exactly the confound documented at the top of this
	// test. Polling the reconciler's own completion count for quiet, rather
	// than sleeping a fixed duration, risks only a false negative if the
	// debounce window is too short for the confounding pass to start: it can
	// never fail against a correct predicate, and it does not need a duration
	// long enough to cover the slowest plausible CI run.
	waitForReconcilesToSettle(t, &counter.completed, 10*time.Second)
	var preShard multigresv1alpha1.Shard
	if err := k8sClient.Get(
		ctx, client.ObjectKey{Name: childName, Namespace: namespace}, &preShard,
	); err != nil {
		t.Fatal(err)
	}
	if preShard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" {
		t.Fatal("child Shard already carries PendingDeletion before the parent was ever annotated")
	}

	// Fetch fresh state and record the generation, so the patch below can be
	// proven not to have bumped it.
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tg), tg); err != nil {
		t.Fatal(err)
	}
	generationBefore := tg.Generation

	// Stamp only the PendingDeletion annotation, the same write
	// multigrescluster's orphan-pruning loop makes on a TableGroup that fell
	// out of the cluster spec. A merge patch touches nothing else: no spec
	// field, no other annotation.
	patch := client.MergeFrom(tg.DeepCopy())
	if tg.Annotations == nil {
		tg.Annotations = make(map[string]string)
	}
	tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = metav1.Now().
		UTC().Format(time.RFC3339)
	if err := k8sClient.Patch(ctx, tg, patch); err != nil {
		t.Fatalf("failed to set PendingDeletion annotation: %v", err)
	}
	if tg.Generation != generationBefore {
		t.Fatalf(
			"annotation patch changed Generation: got %d, want %d (test setup invalid)",
			tg.Generation, generationBefore,
		)
	}

	// The only actor that can now put PendingDeletion onto the child Shard is
	// this TableGroup's own reconcile, and the only way that reconcile gets
	// enqueued is the manager's watch evaluating the For predicate against the
	// annotation-only update just made. Nothing else touches either object
	// from here.
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var shard multigresv1alpha1.Shard
	for {
		if err := k8sClient.Get(
			waitCtx, client.ObjectKey{Name: childName, Namespace: namespace}, &shard,
		); err == nil && shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf(
				"child Shard %s never received PendingDeletion after the parent's "+
					"annotation-only update; the For predicate dropped the event and "+
					"nothing else re-enqueued the TableGroup: %v",
				childName, waitCtx.Err(),
			)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
