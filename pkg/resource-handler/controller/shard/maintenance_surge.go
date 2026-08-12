package shard

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

const maintenanceAnnotationTrue = "true"

// reconcileCellMaintenanceSurge maintains one temporary pooler in a cell when
// a two-cell MULTI_CELL_AT_LEAST_2 shard would otherwise disrupt its last ready
// member there. The surge remains until every desired pod in the cell is back
// on the current spec and ready.
func (r *ShardReconciler) reconcileCellMaintenanceSurge(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	existingPods map[string]*corev1.Pod,
	existingPVCs map[string]*corev1.PersistentVolumeClaim,
	replicas int32,
	rollout *shardRolloutTracker,
) (activeLocalSurges int32, actionTaken bool, err error) {
	if !requiresTwoCellMaintenanceSurge(shard) {
		return 0, false, nil
	}
	if rollout == nil {
		rollout = &shardRolloutTracker{}
	}

	cellPods, err := r.listCellPoolers(ctx, shard, cellName)
	if err != nil {
		return 0, false, err
	}

	var surges []*corev1.Pod
	readyCount := 0
	explicitRequest := false
	localExplicitRequest := false
	baseUnsettled := false
	localInternalTrigger := false
	for i := range cellPods.Items {
		pod := &cellPods.Items[i]
		if isAvailablePooler(pod) {
			readyCount++
		}
		if isMaintenanceSurge(pod) {
			if isDesiredPooler(shard, pod) {
				base := pod.DeepCopy()
				delete(pod.Annotations, metadata.AnnotationMaintenanceSurge)
				if err := r.Patch(ctx, pod, client.MergeFrom(base)); err != nil {
					return 0, false, fmt.Errorf(
						"promote maintenance surge %s to desired capacity: %w",
						pod.Name,
						err,
					)
				}
				if localPod := existingPods[pod.Name]; localPod != nil {
					delete(localPod.Annotations, metadata.AnnotationMaintenanceSurge)
				}
				actionTaken = true
				continue
			}
			surges = append(surges, pod)
			continue
		}
		if pod.Annotations[metadata.AnnotationMaintenanceRequested] == maintenanceAnnotationTrue {
			explicitRequest = true
			if pod.Labels[metadata.LabelMultigresPool] == poolName {
				localExplicitRequest = true
			}
		}
	}

	for desiredPoolName, desiredPool := range shard.Spec.Pools {
		if !poolUsesCell(desiredPool, cellName) {
			continue
		}
		desiredReplicas := poolReplicas(desiredPool)
		for index := int32(0); index < desiredReplicas; index++ {
			podName := BuildPoolPodName(
				shard,
				string(desiredPoolName),
				cellName,
				int(index),
			)
			pod := findPodByName(cellPods.Items, podName)
			if pod == nil {
				baseUnsettled = true
				continue
			}
			stable := isAvailablePooler(pod) &&
				pod.Annotations[metadata.AnnotationDrainState] == ""
			if !stable {
				baseUnsettled = true
			}
			if podNeedsUpdate(
				pod,
				shard,
				string(desiredPoolName),
				cellName,
				desiredPool,
				int(index),
				r.Scheme,
			) {
				baseUnsettled = true
				if stable && string(desiredPoolName) == poolName {
					localInternalTrigger = true
				}
			}
		}
	}

	// A pending filesystem resize is also an operator-initiated disruption.
	for index := int32(0); index < replicas; index++ {
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, int(index))
		if pvcNeedsFilesystemResize(existingPVCs, pvcName) {
			baseUnsettled = true
			localInternalTrigger = true
		}
	}

	hasSurge := len(surges) > 0
	keepSurge := explicitRequest || (hasSurge && baseUnsettled)
	if hasSurge && keepSurge {
		for _, surge := range surges {
			if surge.Labels[metadata.LabelMultigresPool] == poolName {
				activeLocalSurges++
			}
		}
	}

	needsNewSurge := !hasSurge &&
		readyCount <= 1 &&
		(localExplicitRequest || localInternalTrigger)
	if needsNewSurge {
		if rollout.HasSurgeStarted(cellName) {
			return 0, false, nil
		}
		if err := r.createOrAdoptMaintenanceSurge(
			ctx,
			shard,
			poolName,
			cellName,
			poolSpec,
			existingPods,
			existingPVCs,
			replicas,
		); err != nil {
			return 0, false, err
		}
		rollout.SetSurgeStarted(cellName)
		return 0, true, nil
	}

	for _, pod := range existingPods {
		requested := pod.Annotations[metadata.AnnotationMaintenanceRequested] ==
			maintenanceAnnotationTrue
		currentlyReady := pod.Annotations[metadata.AnnotationMaintenanceReady] ==
			maintenanceAnnotationTrue
		wantReady := false
		if requested {
			wantReady, err = r.hasMaintenanceCapacityForPod(
				ctx,
				shard,
				cellName,
				pod.Name,
			)
			if err != nil {
				return activeLocalSurges, false, err
			}
		}
		if currentlyReady == wantReady {
			continue
		}

		base := pod.DeepCopy()
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		if wantReady {
			pod.Annotations[metadata.AnnotationMaintenanceReady] = maintenanceAnnotationTrue
		} else {
			delete(pod.Annotations, metadata.AnnotationMaintenanceReady)
		}
		if err := r.Patch(ctx, pod, client.MergeFrom(base)); err != nil {
			return activeLocalSurges, false, fmt.Errorf(
				"patch maintenance readiness on pod %s: %w",
				pod.Name,
				err,
			)
		}
		actionTaken = true
	}

	return activeLocalSurges, actionTaken, nil
}

func (r *ShardReconciler) createOrAdoptMaintenanceSurge(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	existingPods map[string]*corev1.Pod,
	existingPVCs map[string]*corev1.PersistentVolumeClaim,
	replicas int32,
) error {
	logger := log.FromContext(ctx)
	index := replicas
	podName := BuildPoolPodName(shard, poolName, cellName, int(index))
	pvcName := BuildPoolDataPVCName(shard, poolName, cellName, int(index))

	if pod := existingPods[podName]; pod != nil {
		if !pod.DeletionTimestamp.IsZero() || pod.Annotations[metadata.AnnotationDrainState] != "" {
			return nil
		}
		base := pod.DeepCopy()
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations[metadata.AnnotationMaintenanceSurge] = maintenanceAnnotationTrue
		if err := r.Patch(ctx, pod, client.MergeFrom(base)); err != nil {
			return fmt.Errorf("adopt pod %s as maintenance surge: %w", pod.Name, err)
		}
		return nil
	}

	pvc := existingPVCs[pvcName]
	if pvc == nil {
		var err error
		pvc, err = BuildPoolDataPVC(
			shard,
			poolName,
			cellName,
			poolSpec,
			int(index),
			ShouldDeletePVCOnShardRemoval(shard, poolSpec),
			r.Scheme,
		)
		if err != nil {
			return fmt.Errorf("build maintenance surge PVC %s: %w", pvcName, err)
		}
		if err := r.Create(ctx, pvc); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create maintenance surge PVC %s: %w", pvcName, err)
		}
	} else {
		if err := pvcutil.ClearOrphan(ctx, logger, r.Client, pvc); err != nil {
			return fmt.Errorf("clear orphan label on maintenance surge PVC %s: %w", pvcName, err)
		}
		if err := r.expandPVCIfNeeded(ctx, shard, pvc, poolSpec); err != nil {
			return err
		}
	}

	pod, err := BuildPoolPod(shard, poolName, cellName, poolSpec, int(index), r.Scheme)
	if err != nil {
		return fmt.Errorf("build maintenance surge pod %s: %w", podName, err)
	}
	pod.Annotations[metadata.AnnotationMaintenanceSurge] = maintenanceAnnotationTrue
	if err := r.Create(ctx, pod); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create maintenance surge pod %s: %w", podName, err)
	}

	r.Recorder.Eventf(
		shard,
		"Normal",
		"MaintenanceSurgeCreated",
		"Created temporary pooler %s in cell %s before disruption",
		podName,
		cellName,
	)
	return nil
}

func (r *ShardReconciler) hasMaintenanceCapacityForPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellName string,
	excludedPodName string,
) (bool, error) {
	if !requiresTwoCellMaintenanceSurge(shard) {
		return true, nil
	}
	labels := shardPDBLabels(shard)
	poolers := &corev1.PodList{}
	if err := r.List(
		ctx,
		poolers,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(metadata.GetSelectorLabels(labels)),
	); err != nil {
		return false, fmt.Errorf("list shard poolers for maintenance capacity: %w", err)
	}

	var surgeCount int32
	readyAfterDisruption := int32(0)
	readyInCellAfterDisruption := 0
	for i := range poolers.Items {
		pod := &poolers.Items[i]
		if isActiveMaintenanceSurge(shard, pod) {
			surgeCount++
		}
		if pod.Name == excludedPodName || !isAvailablePooler(pod) {
			continue
		}
		readyAfterDisruption++
		if pod.Labels[metadata.LabelMultigresCell] == cellName {
			readyInCellAfterDisruption++
		}
	}

	requiredReady := minAvailableForTotal(shardTotalReplicas(shard) + surgeCount)
	return readyAfterDisruption >= requiredReady && readyInCellAfterDisruption >= 1, nil
}

func (r *ShardReconciler) listCellPoolers(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellName string,
) (*corev1.PodList, error) {
	labels := shardPDBLabels(shard)
	metadata.AddCellLabel(labels, multigresv1alpha1.CellName(cellName))
	pods := &corev1.PodList{}
	if err := r.List(
		ctx,
		pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(metadata.GetSelectorLabels(labels)),
	); err != nil {
		return nil, fmt.Errorf("list poolers in cell %s: %w", cellName, err)
	}
	return pods, nil
}

func requiresTwoCellMaintenanceSurge(shard *multigresv1alpha1.Shard) bool {
	return shard.Spec.DurabilityPolicy == multiCellAtLeast2Policy && len(shardCells(shard)) == 2
}

func poolReplicas(pool multigresv1alpha1.PoolSpec) int32 {
	if pool.ReplicasPerCell != nil {
		return *pool.ReplicasPerCell
	}
	return DefaultPoolReplicas
}

func poolUsesCell(pool multigresv1alpha1.PoolSpec, cellName string) bool {
	for _, cell := range pool.Cells {
		if string(cell) == cellName {
			return true
		}
	}
	return false
}

func findPodByName(pods []corev1.Pod, name string) *corev1.Pod {
	for i := range pods {
		if pods[i].Name == name {
			return &pods[i]
		}
	}
	return nil
}

func isMaintenanceSurge(pod *corev1.Pod) bool {
	return pod != nil &&
		pod.Annotations[metadata.AnnotationMaintenanceSurge] == maintenanceAnnotationTrue
}

// isActiveMaintenanceSurge distinguishes temporary capacity from a surge Pod
// whose deterministic index has since entered the desired replica range after
// a scale-up. The latter is ordinary desired capacity even before its stale
// annotation is removed from the API object.
func isActiveMaintenanceSurge(shard *multigresv1alpha1.Shard, pod *corev1.Pod) bool {
	return isMaintenanceSurge(pod) && !isDesiredPooler(shard, pod)
}

func isDesiredPooler(shard *multigresv1alpha1.Shard, pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	poolName := pod.Labels[metadata.LabelMultigresPool]
	cellName := pod.Labels[metadata.LabelMultigresCell]
	pool, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	if !ok || !poolUsesCell(pool, cellName) {
		return false
	}
	for index := int32(0); index < poolReplicas(pool); index++ {
		if pod.Name == BuildPoolPodName(shard, poolName, cellName, int(index)) {
			return true
		}
	}
	return false
}

func isAvailablePooler(pod *corev1.Pod) bool {
	return pod != nil &&
		pod.DeletionTimestamp.IsZero() &&
		pod.Annotations[metadata.AnnotationDrainState] == "" &&
		isPodReady(pod)
}
