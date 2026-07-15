package shard

import (
	"context"
	"fmt"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

// updateStatus updates the Shard status based on observed state.
func (r *ShardReconciler) updateStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	oldPhase := shard.Status.Phase
	cellsSet := make(map[multigresv1alpha1.CellName]bool)

	// Update pools status
	totalPods, readyPods, poolDegraded, err := r.updatePoolsStatus(ctx, shard, cellsSet)
	if err != nil {
		return err
	}

	// Update Multiorch status
	orchDegraded, err := r.updateMultiorchStatus(ctx, shard, cellsSet)
	if err != nil {
		return err
	}

	// Update cells list from all observed cells
	shard.Status.Cells = cellSetToSlice(cellsSet)

	// Update aggregate status fields
	shard.Status.PoolsReady = (totalPods > 0 && totalPods == readyPods)
	shard.Status.ReadyReplicas = readyPods

	// Update Phase — Degraded takes priority over Healthy so crash-looping
	// pods are always surfaced even when the old replica is still serving.
	switch {
	case poolDegraded || orchDegraded:
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		if poolDegraded {
			shard.Status.Message = "One or more pool pods are crash-looping"
		} else {
			shard.Status.Message = "One or more Multiorch pods are crash-looping"
		}
	case shard.Status.PoolsReady && shard.Status.OrchReady:
		shard.Status.Phase = multigresv1alpha1.PhaseHealthy
		shard.Status.Message = "Ready"
	default:
		shard.Status.Phase = multigresv1alpha1.PhaseProgressing
		shard.Status.Message = fmt.Sprintf(
			"PoolsReady: %v, OrchReady: %v",
			shard.Status.PoolsReady,
			shard.Status.OrchReady,
		)
	}

	// Update conditions
	r.setConditions(shard, totalPods, readyPods)

	shard.Status.ObservedGeneration = shard.Generation

	// Filter conditions for the SSA patch. Each condition type must belong to
	// exactly one SSA field manager
	var patchConditions []metav1.Condition
	for i := range shard.Status.Conditions {
		if shard.Status.Conditions[i].Type == conditionStorageClassValid {
			continue
		}
		patchConditions = append(patchConditions, shard.Status.Conditions[i])
	}

	patchObj := &multigresv1alpha1.Shard{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "Shard",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      shard.Name,
			Namespace: shard.Namespace,
		},
		Status: multigresv1alpha1.ShardStatus{
			Phase:              shard.Status.Phase,
			Message:            shard.Status.Message,
			ObservedGeneration: shard.Status.ObservedGeneration,
			PoolsReady:         shard.Status.PoolsReady,
			OrchReady:          shard.Status.OrchReady,
			ReadyReplicas:      shard.Status.ReadyReplicas,
			Cells:              shard.Status.Cells,
			Conditions:         patchConditions,
			LastBackupTime:     shard.Status.LastBackupTime,
			LastBackupType:     shard.Status.LastBackupType,
			PodRoles:           shard.Status.PodRoles,
		},
	}

	// 2. Apply the Patch
	if oldPhase != shard.Status.Phase {
		r.Recorder.Eventf(
			shard,
			"Normal",
			"PhaseChange",
			"Transitioned from '%s' to '%s'",
			oldPhase,
			shard.Status.Phase,
		)
	}

	// Note: We rely on Server-Side Apply (SSA) to handle idempotency.
	// If the status hasn't changed, the API server will treat this Patch as a no-op,
	// so we don't need a manual DeepEqual check here.
	if err := r.Status().Patch(
		ctx,
		patchObj,
		client.Apply,
		client.FieldOwner("multigres-resource-handler"),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to patch status: %w", err)
	}

	return nil
}

// updatePoolsStatus aggregates status from all pool pods.
// Returns total desired pods, ready pods, whether any pod is degraded (crash-looping),
// and tracks cells in the cellsSet.
func (r *ShardReconciler) updatePoolsStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellsSet map[multigresv1alpha1.CellName]bool,
) (int32, int32, bool, error) {
	var totalPods, readyPods int32
	var poolDegraded bool
	clusterName := shard.Labels[metadata.LabelMultigresCluster]

	for poolName, poolSpec := range shard.Spec.Pools {
		var poolDesired, poolReady int32

		for _, cell := range poolSpec.Cells {
			cellName := string(cell)
			cellsSet[cell] = true

			// List pods for this specific pool and cell
			labels := buildPoolLabelsWithCell(shard, string(poolName), cellName)
			selector := metadata.GetSelectorLabels(labels)
			podList := &corev1.PodList{}
			if err := r.List(
				ctx,
				podList,
				client.InNamespace(shard.Namespace),
				client.MatchingLabels(selector),
			); err != nil {
				return 0, 0, false, fmt.Errorf("failed to list pods for status: %w", err)
			}

			var cellReady int32
			for i := range podList.Items {
				pod := &podList.Items[i]

				// Exclude terminating pods from total/ready counts
				if !pod.DeletionTimestamp.IsZero() {
					continue
				}

				// Draining pods are transitioning connections away — not considered ready.
				// They still count toward the desired total (spec-driven), so readyPods < totalPods
				// naturally sets PoolsReady=false → Phase=Progressing while drain is in flight.
				if pod.Annotations[metadata.AnnotationDrainState] != "" {
					continue
				}

				// Detect crash-looping pods so the phase can escalate to Degraded.
				if status.IsCrashLooping(pod) {
					poolDegraded = true
				}

				// Check if pod is ready
				for _, cond := range pod.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						cellReady++
						break
					}
				}
			}

			// Emit a warning explicitly if the cell pool should have replicas but is empty
			replicas := DefaultPoolReplicas
			if poolSpec.ReplicasPerCell != nil {
				replicas = *poolSpec.ReplicasPerCell
			}
			if replicas > 0 && cellReady == 0 {
				r.Recorder.Eventf(
					shard,
					"Warning",
					"PoolEmpty",
					"Pool %s in cell %s has 0 ready replicas",
					poolName,
					cellName,
				)
			}

			poolDesired += replicas
			poolReady += cellReady
		}

		totalPods += poolDesired
		readyPods += poolReady

		monitoring.SetShardPoolReplicas(
			clusterName, shard.Name, string(poolName), "", shard.Namespace,
			poolDesired, poolReady,
		)
	}

	return totalPods, readyPods, poolDegraded, nil
}

// updateMultiorchStatus checks Multiorch Deployments and sets OrchReady status.
// Returns whether any Multiorch pod is crash-looping (degraded).
// Also tracks cells in the cellsSet.
func (r *ShardReconciler) updateMultiorchStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellsSet map[multigresv1alpha1.CellName]bool,
) (orchDegraded bool, err error) {
	multiorchCells, cellsErr := getMultiorchCells(shard)
	if cellsErr != nil {
		shard.Status.OrchReady = false
		return false, nil
	}

	orchReady := true
	for _, cell := range multiorchCells {
		cellName := string(cell)
		cellsSet[cell] = true

		// Check Multiorch Deployment status (deployments use long names)
		deployName := buildMultiorchNameWithCell(shard, cellName, name.DefaultConstraints)
		deploy := &appsv1.Deployment{}
		if getErr := r.Get(
			ctx,
			client.ObjectKey{Namespace: shard.Namespace, Name: deployName},
			deploy,
		); getErr != nil {
			if errors.IsNotFound(getErr) {
				orchReady = false
				break
			}
			return false, fmt.Errorf("failed to get Multiorch Deployment for status: %w", getErr)
		}

		// Check if deployment is ready
		if deploy.Spec.Replicas == nil ||
			deploy.Status.ObservedGeneration != deploy.Generation ||
			deploy.Status.ReadyReplicas != *deploy.Spec.Replicas {
			orchReady = false
		}

		// Check if any Multiorch pods are crash-looping
		orchSelector := metadata.GetSelectorLabels(
			buildMultiorchLabelsWithCell(shard, cellName),
		)
		podList := &corev1.PodList{}
		if listErr := r.List(ctx, podList,
			client.InNamespace(shard.Namespace),
			client.MatchingLabels(orchSelector),
		); listErr != nil {
			return false, fmt.Errorf("failed to list Multiorch pods for status: %w", listErr)
		}
		if status.AnyCrashLooping(podList.Items) {
			orchDegraded = true
		}
	}

	shard.Status.OrchReady = orchReady
	return orchDegraded, nil
}

// cellSetToSlice converts a cell set (map) to a slice.
func cellSetToSlice(cellsSet map[multigresv1alpha1.CellName]bool) []multigresv1alpha1.CellName {
	cells := make([]multigresv1alpha1.CellName, 0, len(cellsSet))
	for cell := range cellsSet {
		cells = append(cells, cell)
	}
	slices.Sort(cells)
	return cells
}

// setConditions creates status conditions based on observed state.
func (r *ShardReconciler) setConditions(
	shard *multigresv1alpha1.Shard,
	totalPods, readyPods int32,
) {
	// Available condition
	availableCondition := metav1.Condition{
		Type:               "Available",
		ObservedGeneration: shard.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             "NotAllPodsReady",
		Message:            fmt.Sprintf("%d/%d pods ready", readyPods, totalPods),
	}

	if readyPods == totalPods && totalPods > 0 {
		availableCondition.Status = metav1.ConditionTrue
		availableCondition.Reason = "AllPodsReady"
		availableCondition.Message = fmt.Sprintf("All %d pods are ready", readyPods)
	}

	meta.SetStatusCondition(&shard.Status.Conditions, availableCondition)
}
