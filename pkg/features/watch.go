package features

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// LabelsChanged reports a change only when the reconcile label's value differs
// between old and new. It deliberately ignores every other label/annotation
// edit, or a busy cluster produces requeue storms from unrelated namespace
// churn.
//
// On the primary resource this must be OR'd with the controller's existing
// generation/spec predicate (predicate.Or(existing, LabelsChanged())), never
// used alone: its UpdateFunc returns false for every change except the
// reconcile label, so used by itself it would silently drop every spec update.
func LabelsChanged() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetLabels()[ReconcileLabel] != e.ObjectNew.GetLabels()[ReconcileLabel]
		},
	}
}

// enqueueAllInNamespace returns a handler.MapFunc that, given a Namespace
// event, lists every object of newList()'s kind in that namespace and
// requests all of them. The Namespace object is cluster-scoped, so its
// namespace name is GetName().
//
// In laptop mode any namespace but the claimed one is skipped before listing:
// the cache is scoped to the claimed namespace, so a List elsewhere
// errors and would flood the log on every namespace edit. The requests are not
// filtered by the gate; the Reconcile backstop decides.
func (g Gate) enqueueAllInNamespace(
	c client.Client,
	newList func() client.ObjectList,
) func(ctx context.Context, o client.Object) []reconcile.Request {
	return func(ctx context.Context, o client.Object) []reconcile.Request {
		if g.laptop != "" && o.GetName() != g.laptop {
			return nil
		}
		list := newList()
		if err := c.List(ctx, list, client.InNamespace(o.GetName())); err != nil {
			log.FromContext(ctx).
				Error(err, "reconcile gating: listing objects for namespace label change", "namespace", o.GetName())
			return nil
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			log.FromContext(ctx).
				Error(err, "reconcile gating: extracting list for namespace label change", "namespace", o.GetName())
			return nil
		}
		requests := make([]reconcile.Request, 0, len(items))
		for _, item := range items {
			obj, ok := item.(client.Object)
			if !ok {
				continue
			}
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(obj),
			})
		}
		return requests
	}
}
