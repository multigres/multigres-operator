package cell

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

func (r *CellReconciler) updateLiteStatus(ctx context.Context, cell *multigresv1alpha1.Cell) error {
	oldPhase := cell.Status.Phase
	selector := map[string]string{
		metadata.LabelAppInstance:          cell.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresGatewayRole: metadata.GatewayRoleLite,
	}
	metadata.AddCellLabel(selector, cell.Spec.Name)
	pods := &corev1.PodList{}
	if err := r.List(
		ctx,
		pods,
		client.InNamespace(cell.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return fmt.Errorf("list Lite gateway pods: %w", err)
	}
	var ready int32
	for _, pod := range pods.Items {
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready++
			}
		}
	}
	cell.Status.GatewayReplicas = 1
	cell.Status.GatewayReadyReplicas = ready
	cell.Status.GatewayServiceName = BuildMultigatewayServiceName(cell)
	result := status.ComputeWorkloadPhase(
		status.WorkloadPhaseInput{
			Ready:              ready,
			Total:              1,
			GenerationCurrent:  cell.Generation,
			GenerationObserved: cell.Generation,
			Pods:               pods.Items,
			ComponentName:      "Lite gateway",
		},
	)
	cell.Status.Phase, cell.Status.Message = result.Phase, result.Message
	cell.Status.ObservedGeneration = cell.Generation
	for _, kind := range []string{"Available", "Ready"} {
		condition := metav1.Condition{
			Type:               kind,
			ObservedGeneration: cell.Generation,
			Status:             metav1.ConditionFalse,
			Reason:             "LiteGatewayUnavailable",
			Message:            "Waiting for the Lite pod to serve SQL",
		}
		if ready == 1 {
			condition.Status = metav1.ConditionTrue
			condition.Reason = "LiteGatewayReady"
			condition.Message = "The Lite pod is ready to serve SQL"
		}
		meta.SetStatusCondition(&cell.Status.Conditions, condition)
	}
	monitoring.SetCellGatewayReplicas(cell.Name, cell.Namespace, 1, ready)
	return r.patchWorkloadStatus(ctx, cell, oldPhase)
}
