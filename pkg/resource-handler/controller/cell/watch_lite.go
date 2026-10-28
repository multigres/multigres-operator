package cell

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

// Lite pods belong to Shards, so owner references cannot route their events to Cells.
func (r *CellReconciler) cellsForLitePod(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	if !isLiteGatewayPod(obj) {
		return nil
	}
	cells := &multigresv1alpha1.CellList{}
	if err := r.List(ctx, cells, client.InNamespace(obj.GetNamespace()), client.MatchingLabels{
		metadata.LabelMultigresCluster: obj.GetLabels()[metadata.LabelAppInstance],
	}); err != nil {
		log.FromContext(ctx).
			Error(err, "Failed to find Cell for Lite pod", "pod", client.ObjectKeyFromObject(obj))
		return nil
	}
	var requests []reconcile.Request
	for _, cell := range cells.Items {
		if cell.Spec.Mode == multigresv1alpha1.ModeLite &&
			string(cell.Spec.Name) == obj.GetLabels()[metadata.LabelMultigresCell] {
			requests = append(
				requests,
				reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&cell)},
			)
		}
	}
	return requests
}

func isLiteGatewayPod(obj client.Object) bool {
	if _, ok := obj.(*corev1.Pod); !ok {
		return false
	}
	labels := obj.GetLabels()
	return labels[metadata.LabelMultigresGatewayRole] == metadata.GatewayRoleLite &&
		labels[metadata.LabelAppInstance] != "" && labels[metadata.LabelMultigresCell] != ""
}

// Only changes used by Lite Cell status or routing need another reconcile.
// The update handler maps both old and new objects, including label removal.
func litePodChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return isLiteGatewayPod(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return isLiteGatewayPod(e.Object) },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, oldOK := e.ObjectOld.(*corev1.Pod)
			newPod, newOK := e.ObjectNew.(*corev1.Pod)
			if !oldOK || !newOK || (!isLiteGatewayPod(oldPod) && !isLiteGatewayPod(newPod)) {
				return false
			}
			for _, key := range []string{metadata.LabelAppInstance, metadata.LabelMultigresCell, metadata.LabelMultigresGatewayRole} {
				if oldPod.Labels[key] != newPod.Labels[key] {
					return true
				}
			}
			return litePodReady(oldPod) != litePodReady(newPod) ||
				oldPod.DeletionTimestamp.IsZero() != newPod.DeletionTimestamp.IsZero() ||
				status.IsCrashLooping(oldPod) != status.IsCrashLooping(newPod)
		},
	}
}

func litePodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
