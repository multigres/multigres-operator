package shard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// Two pools are each short of a pod at a full quota, and a pass ends at the
// first create that is turned down. Which pool that is changes from pass to
// pass, since pools are reconciled in map order. What status says about the
// wait must not follow it: sixty passes with the pools in alternating order
// give one text.
func TestQuotaWaitInStatusDoesNotFollowPoolOrder(t *testing.T) {
	f := newTwoPoolFixture(t, []int{1, 2}, []int{1, 2})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	key := client.ObjectKeyFromObject(f.shard)
	orders := [][]string{{"main", "other"}, {"other", "main"}}
	texts := map[string]int{}
	for retry := range 60 {
		ctx := withReplacementPass(t.Context())
		f.reconciler.setConditions(f.shard, 6, 4)
		f.reconciler.nameQuotaWaits(f.shard, quotaObjectsSeen{})
		texts[meta.FindStatusCondition(f.shard.Status.Conditions, "Available").Message]++
		var err error
		for _, poolName := range orders[retry%2] {
			err = f.reconciler.reconcilePoolPods(ctx, f.shard, poolName, "z1", f.pool,
				&shardRolloutTracker{})
			if err != nil {
				break
			}
		}
		require.ErrorContains(t, err, fullQuotaMessage)
		f.reconciler.endPass(ctx, key, err)
		now = now.Add(time.Hour)
	}
	delete(texts, "4/6 pods ready")
	require.Len(t, texts, 1, "the text changed while the wait did not: %v", texts)
	for text := range texts {
		require.Equal(t, "4/6 pods ready; pod "+BuildPoolPodName(f.shard, "main", "z1", 0)+
			" waiting on ResourceQuota full (pods) since 2026-01-01T00:00:00Z", text)
	}
}

// poolStatusText computes the Available message the way the status update
// does: from the pods it lists, and with the quota waits checked against
// them.
func poolStatusText(t *testing.T, r *ShardReconciler, shard *multigresv1alpha1.Shard) string {
	t.Helper()
	ps, err := r.updatePoolsStatus(
		t.Context(),
		shard,
		map[multigresv1alpha1.CellName]bool{},
		"",
		"",
	)
	require.NoError(t, err)
	r.setConditions(shard, ps.totalPods, ps.readyPods)
	r.nameQuotaWaits(shard, ps.seen)
	return meta.FindStatusCondition(shard.Status.Conditions, "Available").Message
}

// A maintenance surge pod is turned down by a full pod quota and named in
// status. The surge is then withdrawn, which changes no generation, while
// another pod of the shard is still missing and keeps the wait going. Status
// must stop naming the surge pod, which nothing tries to create any more,
// and name the pod whose create is turned down, with the wait's start time.
func TestQuotaWaitInStatusLeavesAWithdrawnSurgePod(t *testing.T) {
	f := newTwoPoolFixture(t, []int{0}, []int{0, 1})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	f.reconciler.Recorder = record.NewFakeRecorder(1000)
	key := client.ObjectKeyFromObject(f.shard)
	ctx := t.Context()
	pool := f.pool
	pool.ReplicasPerCell = ptr.To(int32(1))
	pool.Cells = []multigresv1alpha1.CellName{"z1", "z2"}
	f.shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy
	for _, pod := range f.listPods(t) {
		if pod.Labels[metadata.LabelMultigresPool] == "other" {
			require.NoError(t, f.client.Delete(ctx, &pod))
		}
	}
	f.podLimit = 1
	serving := BuildPoolPodName(f.shard, "main", "z1", 0)
	maintenance := func(requested bool) {
		pod := &corev1.Pod{}
		require.NoError(t, f.client.Get(ctx,
			client.ObjectKey{Namespace: f.shard.Namespace, Name: serving}, pod))
		if requested {
			pod.Annotations[metadata.AnnotationMaintenanceRequested] = "true"
		} else {
			delete(pod.Annotations, metadata.AnnotationMaintenanceRequested)
		}
		require.NoError(t, f.client.Update(ctx, pod))
	}
	pass := func() (text string, err error) {
		passCtx := withReplacementPass(ctx)
		text = poolStatusText(t, f.reconciler, f.shard)
		rollout := &shardRolloutTracker{}
		for _, cell := range []string{"z1", "z2"} {
			if err = f.reconciler.reconcilePoolPods(passCtx, f.shard, "main", cell, pool,
				rollout); err != nil {
				break
			}
		}
		f.reconciler.endPass(passCtx, key, err)
		now = now.Add(time.Hour)
		return text, err
	}
	const since = " waiting on ResourceQuota full (pods) since 2026-01-01T00:00:00Z"
	surge := BuildPoolPodName(f.shard, "main", "z1", 1)
	missing := BuildPoolPodName(f.shard, "main", "z2", 0)

	maintenance(true)
	_, err := pass()
	require.ErrorContains(t, err, "create maintenance surge pod "+surge)
	text, err := pass()
	require.ErrorContains(t, err, "create maintenance surge pod "+surge)
	require.Equal(t, "1/2 pods ready; pod "+surge+since, text,
		"a surge pod whose create is the one turned down is named")

	maintenance(false)
	_, err = pass()
	require.ErrorContains(t, err, "failed to create pod "+missing)
	for range 4 {
		text, err = pass()
		require.ErrorContains(t, err, "failed to create pod "+missing)
		require.Equal(t, "1/2 pods ready; pod "+missing+since, text)
	}
}

// Two pools in two cells each need a maintenance surge, and a full pod quota
// turns both surge pods down. A pass ends at the first of them, and which
// that is follows the pools' map order. Neither pod is within a replica
// count, so neither is a better name than the other: status keeps the first
// one named, whichever is turned down after it.
func TestQuotaWaitInStatusKeepsOneOfTwoSurgePods(t *testing.T) {
	ctx := t.Context()
	f := newTwoPoolFixture(t, []int{0, 1, 2}, nil)
	for _, pod := range f.listPods(t) {
		require.NoError(t, f.client.Delete(ctx, &pod))
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	f.reconciler.Recorder = record.NewFakeRecorder(10000)
	f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy
	f.podLimit = 2
	cells := map[string]string{"main": "z1", "other": "z2"}
	for pool, cell := range cells {
		spec := f.pool
		spec.Cells = []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cell)}
		spec.ReplicasPerCell = ptr.To(int32(1))
		f.shard.Spec.Pools[multigresv1alpha1.PoolName(pool)] = spec
		pod, err := BuildPoolPod(f.shard, pool, cell, spec, 0, f.scheme)
		require.NoError(t, err)
		pod.Annotations[metadata.AnnotationMaintenanceRequested] = "true"
		require.NoError(t, f.client.Create(ctx, pod))
	}
	for _, pod := range f.listPods(t) {
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		require.NoError(t, f.client.Status().Update(ctx, &pod))
	}
	key := client.ObjectKeyFromObject(f.shard)
	// pass reconciles the pools in the given order, as one reconcile of the
	// Shard does, and returns the status it starts with.
	pass := func(order ...string) string {
		passCtx := withReplacementPass(ctx)
		text := poolStatusText(t, f.reconciler, f.shard)
		rollout := &shardRolloutTracker{}
		var err error
		for _, pool := range order {
			spec := f.shard.Spec.Pools[multigresv1alpha1.PoolName(pool)]
			if err = f.reconciler.reconcilePoolPods(passCtx, f.shard, pool, cells[pool], spec,
				rollout); err != nil {
				break
			}
		}
		require.ErrorContains(t, err,
			"create maintenance surge pod "+BuildPoolPodName(f.shard, order[0], cells[order[0]], 1))
		f.reconciler.endPass(passCtx, key, err)
		now = now.Add(time.Hour)
		return text
	}
	want := "All 2 pods are ready; pod " + BuildPoolPodName(f.shard, "main", "z1", 1) +
		" waiting on ResourceQuota full (pods) since 2026-01-01T00:00:00Z"

	pass("main", "other")
	// Ten passes with both pools in alternating order, then eight
	// that each reach one pool only.
	for retry := range 10 {
		order := [][]string{{"other", "main"}, {"main", "other"}}[retry%2]
		require.Equal(t, want, pass(order...), "retry %d", retry)
	}
	for retry := range 8 {
		require.Equal(t, want, pass([]string{"other", "main"}[retry%2]), "retry %d", retry)
	}
}

// A ready pod beyond the replica count is not one of the pods the spec asks
// for. With a desired pod missing at a full quota it must not make the count
// whole: status would say every pod is ready and name a pod that is waiting
// to be created in the same message.
func TestPoolStatusCountsOnlyDesiredPodsAsReady(t *testing.T) {
	f := newTwoPoolFixture(t, []int{0, 1, 3}, []int{0, 1, 2})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }

	ps, err := f.reconciler.updatePoolsStatus(
		t.Context(), f.shard, map[multigresv1alpha1.CellName]bool{}, "", "")
	require.NoError(t, err)
	require.EqualValues(t, 6, ps.totalPods)
	require.EqualValues(t, 5, ps.readyPods)

	// The missing pod's create is turned down, and status names it next to
	// a count that is short of it.
	ctx := withReplacementPass(t.Context())
	err = f.reconciler.reconcilePoolPods(ctx, f.shard, "main", "z1", f.pool,
		&shardRolloutTracker{})
	f.reconciler.endPass(ctx, client.ObjectKeyFromObject(f.shard), err)
	text := poolStatusText(t, f.reconciler, f.shard)
	require.Contains(t, text, "5/6 pods ready; pod "+
		BuildPoolPodName(f.shard, "main", "z1", 2)+" waiting on ResourceQuota")
}
