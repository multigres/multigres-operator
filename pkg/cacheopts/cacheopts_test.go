package cacheopts

import (
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// cachedKinds are the per-type entries New is expected to populate.
func cachedKinds() []client.Object {
	return []client.Object{
		&corev1.Secret{}, &appsv1.StatefulSet{}, &corev1.Service{}, &corev1.Pod{},
	}
}

// namespacesFor finds the ByObject entry for the kind of want. ByObject is keyed
// by client.Object pointer identity, so a fresh &corev1.Secret{} cannot be used
// as a map index; match on the dynamic type instead.
func namespacesFor(t *testing.T, opts cache.Options, want client.Object) map[string]cache.Config {
	t.Helper()
	wantType := reflect.TypeOf(want)
	for obj, byObj := range opts.ByObject {
		if reflect.TypeOf(obj) == wantType {
			return byObj.Namespaces
		}
	}
	t.Fatalf("ByObject has no entry for %T", want)
	return nil
}

// wantFilteredSelector is the selector every "elsewhere" (non-operator) entry
// is expected to carry.
func wantFilteredSelector(t *testing.T) labels.Selector {
	t.Helper()
	req, err := labels.NewRequirement(
		metadata.LabelAppManagedBy, selection.Equals, []string{metadata.ManagedByMultigres},
	)
	if err != nil {
		t.Fatalf("building expected selector: %v", err)
	}
	return labels.NewSelector().Add(*req)
}

// New("op", "") must be a no-op relative to the pre-laptop-mode configuration:
// no DefaultNamespaces, and the per-type hybrid map still spans the operator's
// own namespace plus cache.AllNamespaces.
func TestNewWithoutLaptopNamespacesPreservesHybridMap(t *testing.T) {
	opts := New("op", "")

	if opts.DefaultNamespaces != nil {
		t.Fatalf("DefaultNamespaces = %v, want nil", opts.DefaultNamespaces)
	}

	for _, obj := range cachedKinds() {
		hybrid := namespacesFor(t, opts, obj)
		if hybrid == nil {
			t.Fatalf("ByObject[%T].Namespaces is nil, want a hybrid map", obj)
		}

		all, ok := hybrid[cache.AllNamespaces]
		if !ok {
			t.Fatalf("ByObject[%T].Namespaces has no %q key", obj, cache.AllNamespaces)
		}
		if got, want := all.LabelSelector.String(), wantFilteredSelector(t).String(); got != want {
			t.Errorf("ByObject[%T][%q] selector = %q, want %q", obj, cache.AllNamespaces, got, want)
		}

		own, ok := hybrid["op"]
		if !ok {
			t.Fatalf("ByObject[%T].Namespaces has no operator-namespace key", obj)
		}
		if own.LabelSelector != nil {
			t.Errorf("ByObject[%T][op] selector = %v, want unfiltered", obj, own.LabelSelector)
		}
	}
}

// New("op", "dev") must confine every cached type to exactly the claimed
// namespace, in the per-type map and in DefaultNamespaces, so types absent from
// ByObject (ConfigMaps) cannot stay cached cluster-wide. The operator's own
// namespace is left out: laptop mode refuses leader election and webhooks, the
// only readers of it.
func TestNewWithLaptopNamespaceScopesToItAlone(t *testing.T) {
	opts := New("op", "dev")

	if len(opts.DefaultNamespaces) != 1 {
		t.Fatalf("DefaultNamespaces = %v, want exactly {dev}", opts.DefaultNamespaces)
	}
	if cfg, ok := opts.DefaultNamespaces["dev"]; !ok || cfg.LabelSelector != nil {
		t.Fatalf("DefaultNamespaces[dev] = %v (present %v), want an unfiltered entry", cfg, ok)
	}

	for _, obj := range cachedKinds() {
		hybrid := namespacesFor(t, opts, obj)
		if len(hybrid) != 1 {
			t.Errorf("ByObject[%T].Namespaces = %v, want exactly {dev}", obj, hybrid)
		}
		dev, ok := hybrid["dev"]
		if !ok {
			t.Fatalf("ByObject[%T].Namespaces missing the claimed namespace", obj)
		}
		if got, want := dev.LabelSelector.String(), wantFilteredSelector(t).String(); got != want {
			t.Errorf("ByObject[%T][dev] selector = %q, want %q", obj, got, want)
		}
	}
}

// A watch allowlist must never broaden a laptop manager beyond its claimed namespace.
func TestLaptopNamespaceTakesPrecedenceOverWatchAllowlist(t *testing.T) {
	want := New("op", "dev")
	got := New("op", "dev", "dev,other")
	if !reflect.DeepEqual(got.DefaultNamespaces, want.DefaultNamespaces) {
		t.Fatalf("DefaultNamespaces = %v, want %v", got.DefaultNamespaces, want.DefaultNamespaces)
	}
	for _, obj := range cachedKinds() {
		if !reflect.DeepEqual(namespacesFor(t, got, obj), namespacesFor(t, want, obj)) {
			t.Errorf("%T cache escaped the laptop namespace", obj)
		}
	}
}
