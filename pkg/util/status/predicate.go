package status

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// PhaseClearedPredicate admits an update of a T that empties the phase.
//
// Each controller's For() filter drops status-only updates, so a status that
// is wiped out of band (a restore, a manual edit of the subresource) would
// otherwise never be rebuilt until an unrelated event arrived: a Healthy
// object requeues nowhere, and its parent keeps reading the empty phase as
// not Healthy. The reconcile it triggers rewrites the phase, so this cannot
// loop on the controller's own writes. Only the phase is checked: a partial
// wipe that leaves it set is not caught.
func PhaseClearedPredicate[T client.Object](
	phase func(T) multigresv1alpha1.Phase,
) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, ok := e.ObjectOld.(T)
			if !ok {
				return false
			}
			newObj, ok := e.ObjectNew.(T)
			if !ok {
				return false
			}
			return phase(oldObj) != "" && phase(newObj) == ""
		},
	}
}
