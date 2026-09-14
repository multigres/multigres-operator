package shard

import (
	"context"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const disruptionRecoveryRequeue = 5 * time.Second

// listDisruptionPods reads from the API server so a new reconcile cannot miss
// a drain annotation that a previous reconcile just wrote through the cache.
func (r *ShardReconciler) listDisruptionPods(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (*corev1.PodList, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	pods := &corev1.PodList{}
	err := reader.List(ctx, pods, client.InNamespace(shard.Namespace),
		client.MatchingLabels(map[string]string{
			metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
			metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
			metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
			metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
			metadata.LabelAppComponent:        PoolComponentName,
		}))
	return pods, err
}

// canStartDisruption is a fresh preflight, independent of cached Shard status.
// Existing operations must still be allowed to complete when it returns false.
func (r *ShardReconciler) canStartDisruption(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	target *corev1.Pod,
	tracker *shardRolloutTracker,
) (allowed bool, err error) {
	if tracker.waitingForRecovery {
		return false, nil
	}
	defer func() {
		if !allowed {
			tracker.waitingForRecovery = true
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return false, fmt.Errorf("list ongoing shard disruptions: %w", err)
	}
	var available []string
	disruptionTarget := posture.DisruptionTarget{Name: target.Name}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			return false, nil
		}
		if isAvailablePooler(pod) {
			available = append(available, pod.Name)
		}
		if pod.Name == target.Name && pod.UID == target.UID {
			disruptionTarget.Unscheduled = pod.Spec.NodeName == "" &&
				pod.Status.Phase == corev1.PodPending
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodScheduled &&
					condition.Status == corev1.ConditionTrue {
					disruptionTarget.Unscheduled = false
				}
			}
		}
	}
	if r.PoolerClients == nil {
		return false, nil
	}
	rpc, err := r.PoolerClients.ClientFor(ctx, shard)
	if err != nil || rpc == nil {
		return false, nil
	}
	store, err := r.topoStore(ctx, shard)
	if err != nil || store == nil {
		return false, nil
	}
	defer func() { _ = store.Close() }()
	if err := posture.CheckDisruption(
		ctx,
		store,
		rpc,
		shard,
		available,
		disruptionTarget,
	); err != nil {
		if r.Recorder != nil {
			r.Recorder.Eventf(
				shard,
				"Normal",
				"DisruptionBlocked",
				"Waiting for shard recovery before removing %s: %v",
				target.Name,
				err,
			)
		}
		return false, nil
	}
	return true, nil
}

// selectShardScaleDownPod ranks removable pods across pool/cell boundaries.
// Active surges in other cells are retained by their maintenance workflow;
// released local surges are already present in localExtras.
func (r *ShardReconciler) selectShardScaleDownPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	localExtras []*corev1.Pod,
) (*corev1.Pod, error) {
	pods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("list shard scale-down candidates: %w", err)
	}
	candidates := map[string]*corev1.Pod{}
	for _, pod := range localExtras {
		candidates[pod.Name] = pod
	}
	groups := map[string]map[string]*corev1.Pod{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		key := pod.Labels[metadata.LabelMultigresPool] + "/" + pod.Labels[metadata.LabelMultigresCell]
		if groups[key] == nil {
			groups[key] = map[string]*corev1.Pod{}
		}
		groups[key][pod.Name] = pod
	}
	for poolName, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			group := groups[string(poolName)+"/"+string(cell)]
			replicas := poolReplicas(pool)
			for _, pod := range group {
				index, ok := resolvePodIndex(pod.Name)
				if ok && index >= int(replicas) && !isMaintenanceSurge(pod) {
					candidates[pod.Name] = pod
				}
			}
		}
	}
	names := make([]string, 0, len(candidates))
	for name := range candidates {
		names = append(names, name)
	}
	slices.Sort(names)
	ordered := make([]*corev1.Pod, 0, len(names))
	for _, name := range names {
		ordered = append(ordered, candidates[name])
	}
	return r.selectPodToDrain(ctx, ordered, shard), nil
}
