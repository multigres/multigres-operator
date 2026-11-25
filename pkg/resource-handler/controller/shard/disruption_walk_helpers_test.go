package shard

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// The helpers below drive the disruption walk the way a reconcile pass does,
// for tests that exercise one pool/cell's maintenance step and then expect
// the pass's disruption decision.

// rollingUpdatePass is one pool/cell's rolling-update request followed by the
// shard's disruption walk. The pool spec and the pool/cell's pods are taken
// for the callers' convenience; the walk reads both itself.
func (r *ShardReconciler) rollingUpdatePass(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	_ multigresv1alpha1.PoolSpec,
	_ map[string]*corev1.Pod,
	driftedCount int,
	actionTaken, isAnyPodDraining bool,
	rollout *shardRolloutTracker,
) error {
	r.handleRollingUpdates(shard, poolName, cellName, driftedCount, actionTaken,
		isAnyPodDraining, rollout)
	rollout.generation = shard.Generation
	return r.startDisruption(ctx, shard, rollout)
}

// replaceUnready is replaceUnreadyDriftedPod on a fresh read of the shard's
// pool Pods, in a pass where every pool/cell's maintenance completed and
// asked for a rolling update.
func (r *ShardReconciler) replaceUnready(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
) (bool, error) {
	pods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return false, err
	}
	observed := listedPodNames(pods.Items)
	pods.Items = r.managedPoolPods(ctx, shard, pods.Items)
	if disruptionInFlight(pods) {
		return true, nil
	}
	for poolName, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			rollout.cell(string(poolName), string(cell)).rolling = true
		}
	}
	return r.replaceUnreadyDriftedPod(ctx, shard, rollout, pods, observed)
}

// isShardHealthy is shardPodsHealthy on a fresh read of the shard's pool Pods.
func (r *ShardReconciler) isShardHealthy(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (bool, error) {
	pods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return false, err
	}
	return shardPodsHealthy(shard, r.managedPoolPods(ctx, shard, pods.Items)), nil
}

// canStartDisruption observes the shard and reports whether target may be
// removed on that observation.
func (r *ShardReconciler) canStartDisruption(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	target *corev1.Pod,
	rollout *shardRolloutTracker,
) (bool, error) {
	pods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return false, err
	}
	observed := listedPodNames(pods.Items)
	pods.Items = r.managedPoolPods(ctx, shard, pods.Items)
	preflight, err := r.observeForWalk(ctx, shard, rollout, pods, observed)
	if err != nil || preflight == nil {
		return false, err
	}
	// The check reads the target as the preflight's own list has it, not as
	// the caller holds it.
	for i := range pods.Items {
		if pods.Items[i].Name == target.Name {
			return r.disruptionCheck(shard, preflight, &pods.Items[i]) == nil, nil
		}
	}
	return r.disruptionCheck(shard, preflight, target) == nil, nil
}

// selectPodToDrain is the first pod rankPodsToDrain would remove.
func (r *ShardReconciler) selectPodToDrain(
	_ context.Context,
	extraPods []*corev1.Pod,
	shard *multigresv1alpha1.Shard,
) *corev1.Pod {
	ranked := rankPodsToDrain(extraPods, shard)
	if len(ranked) == 0 {
		return nil
	}
	return ranked[0]
}

// scaleDownPass is one pool/cell's scale-down maintenance followed by the
// shard's disruption walk. It reports an action when either acted.
func (r *ShardReconciler) scaleDownPass(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	poolSpec multigresv1alpha1.PoolSpec,
	existingPods map[string]*corev1.Pod,
	replicas int32,
	effectiveReplicas int32,
	actionTaken bool,
	trackers ...*shardRolloutTracker,
) (bool, bool, error) {
	rollout := firstShardRolloutTracker(trackers)
	started := rollout.HasStarted()
	actionTaken, inProgress, err := r.handleScaleDown(ctx, shard, poolName, poolSpec,
		existingPods, replicas, effectiveReplicas, actionTaken, rollout)
	if err != nil {
		return actionTaken, inProgress, err
	}
	rollout.generation = shard.Generation
	err = r.startDisruption(ctx, shard, rollout)
	return actionTaken || (rollout.HasStarted() && !started), inProgress, err
}

// poolPodsPass is one pool/cell's maintenance followed by the shard's
// disruption walk.
func (r *ShardReconciler) poolPodsPass(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	rollout *shardRolloutTracker,
) error {
	if err := r.reconcilePoolPods(ctx, shard, poolName, cellName, poolSpec, rollout); err != nil {
		return err
	}
	rollout.generation = shard.Generation
	return r.startDisruption(ctx, shard, rollout)
}

// drainStale is isDrainStale for a test whose PVC reads cannot fail.
func (r *ShardReconciler) drainStale(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	state string,
) bool {
	t.Helper()
	stale, err := r.isDrainStale(t.Context(), shard, pod, state)
	if err != nil {
		t.Fatal(err)
	}
	return stale
}
