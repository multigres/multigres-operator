// Package features implements reconcile gating: the label that pauses an
// object or a namespace, the event filter and Reconcile backstop that honor it,
// and the namespace watch that resumes objects when the label is removed.
//
// It deliberately imports neither api/v1alpha1 nor pkg/monitoring, which keeps
// it reusable and independently testable.
package features

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// ReconcileLabel is the label, on an object or its namespace, that gates it.
	ReconcileLabel = "multigres.com/reconcile"
	// Disabled is the only value of ReconcileLabel that means anything. It is
	// exported so an operator's startup label check shares the one definition.
	Disabled = "disabled"
)

// Gate decides whether an object is paused. The zero value is a gate with no
// laptop mode.
type Gate struct {
	// laptop is the namespace claimed with --namespace; empty means laptop
	// mode is off.
	laptop string
}

// NewGate returns a Gate for the namespace claimed by laptop mode, if any.
func NewGate(laptopNamespace string) Gate { return Gate{laptop: laptopNamespace} }

// Gated is the read-only check the Reconcile backstop calls. It reports whether
// obj is gated; the one I/O it does is a Namespace Get through reader, which
// must be cache-backed.
//
// In laptop mode any namespace but the claimed one is gated unconditionally.
// The claimed namespace is reconciled only
// while it still carries the disabled label: the label is what keeps the
// deployed operator out, so the moment it is removed the deployed operator
// resumes, and the laptop must stop rather than fight it. Object labels are
// not consulted there; the claim covers the namespace.
//
// A Namespace missing from the cache is NOT gated here, unlike in the event
// filter. Requests only reach Reconcile after the Namespace was seen, and
// fake-client unit tests seed no Namespace at all, so failing closed would
// make every one of them a no-op.
func (g Gate) Gated(ctx context.Context, reader client.Reader, obj client.Object) (bool, error) {
	return g.gated(ctx, reader, obj, false)
}

func (g Gate) gated(
	ctx context.Context,
	reader client.Reader,
	obj client.Object,
	missingNamespaceGated bool,
) (bool, error) {
	ns := obj.GetNamespace()
	laptop := g.laptop != ""
	if laptop && ns != g.laptop {
		return true, nil
	}
	var namespace corev1.Namespace
	if err := reader.Get(ctx, client.ObjectKey{Name: ns}, &namespace); err != nil {
		if apierrors.IsNotFound(err) {
			return missingNamespaceGated, nil
		}
		return false, fmt.Errorf("getting namespace %q for reconcile gating: %w", ns, err)
	}
	nsDisabled := namespace.Labels[ReconcileLabel] == Disabled
	if laptop {
		return !nsDisabled, nil
	}
	return nsDisabled || obj.GetLabels()[ReconcileLabel] == Disabled, nil
}

// Wire adds the gate's event filter and the namespace re-enqueue watch to b.
// newList returns a fresh, empty list of the controller's kind, for the
// namespace watch to enqueue from. The caller still wraps its own For()
// predicate in predicate.Or(existing, LabelsChanged()) where it has one.
func (g Gate) Wire(
	b *builder.Builder,
	mgr ctrl.Manager,
	newList func() client.ObjectList,
) *builder.Builder {
	return b.WithEventFilter(g.ReconcileGate(mgr.GetCache())).
		Watches(&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(g.enqueueAllInNamespace(mgr.GetClient(), newList)),
			builder.WithPredicates(LabelsChanged()))
}

// ReconcileGate is the event filter applied to every source of a gated
// controller, so child events in a gated namespace stop waking the parent as
// well. A gated object therefore produces no reconcile at all.
//
// A Namespace not yet in the informer counts as gated. The API server refuses
// objects in a nonexistent namespace, so a cache miss only means the Namespace
// informer is behind the object informer; failing open would give a freshly
// created gated namespace one unguarded reconcile. The Namespace's own create
// event re-enqueues its objects through the namespace watch.
//
// Namespace objects (cluster-scoped, empty namespace) always pass, or the
// namespace watch could not un-gate anything. A read error drops the event and
// is logged; the object's next event or the resync retries it.
func (g Gate) ReconcileGate(reader client.Reader) predicate.Predicate {
	pass := func(obj client.Object) bool {
		if obj == nil {
			return false
		}
		if obj.GetNamespace() == "" {
			return true
		}
		// The event predicate has no request context to inherit from.
		gated, err := g.gated(context.Background(), reader, obj, true)
		if err != nil {
			log.Log.WithName("reconcile-gate").
				Error(err, "dropping event", "object", client.ObjectKeyFromObject(obj))
			return false
		}
		return !gated
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return pass(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return pass(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return pass(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return pass(e.ObjectNew) },
	}
}
