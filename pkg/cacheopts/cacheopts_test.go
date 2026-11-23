package cacheopts

import (
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// cachedKinds are the per-type entries New is expected to populate.
func cachedKinds() []client.Object {
	return []client.Object{
		&corev1.Secret{}, &appsv1.StatefulSet{}, &corev1.Service{}, &corev1.Pod{},
		&corev1.ServiceAccount{},
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

// The shard reconciler reads the shared backup PVC through the informer cache
// and decides what to apply from the operator's server-side-apply ownership in
// its managedFields. A transform that strips managedFields would make every
// claim look unowned there, so the operator would re-send fields the API server
// refuses to change on a live claim and wedge the shard reconcile, with every
// test that reads through a direct client still green.
func TestNewKeepsManagedFields(t *testing.T) {
	for _, laptop := range []string{"", "dev"} {
		opts := New("op", laptop)
		if opts.DefaultTransform != nil {
			t.Errorf("namespace %q: DefaultTransform is set, it may strip managedFields", laptop)
		}
		for obj, byObj := range opts.ByObject {
			// The operator never applies a ResourceQuota, so nothing reads
			// one's managedFields.
			if _, quota := obj.(*corev1.ResourceQuota); quota {
				continue
			}
			if byObj.Transform != nil {
				t.Errorf("namespace %q: ByObject[%T].Transform is set, it may strip managedFields",
					laptop, obj)
			}
		}
	}
}

// The cache keeps a ResourceQuota's spec and status, which is what the shard
// controller's watch compares, in every namespace Shards are reconciled in,
// and drops the rest of the object.
func TestNewCachesQuotaSpecAndStatusOnly(t *testing.T) {
	for name, tc := range map[string]struct {
		opts       cache.Options
		namespaces []string
	}{
		"cluster-wide": {New("op", ""), []string{cache.AllNamespaces}},
		"allowlist":    {New("op", "", "a,b"), []string{"op", "a", "b"}},
		"laptop":       {New("op", "dev"), []string{"dev"}},
	} {
		var entry *cache.ByObject
		for obj, byObj := range tc.opts.ByObject {
			if _, quota := obj.(*corev1.ResourceQuota); quota {
				entry = &byObj
			}
		}
		if entry == nil {
			t.Fatalf("%s: no ByObject entry for ResourceQuota", name)
		}
		if len(entry.Namespaces) != len(tc.namespaces) {
			t.Errorf("%s: namespaces = %v, want %v", name, entry.Namespaces, tc.namespaces)
		}
		for _, namespace := range tc.namespaces {
			if cfg, ok := entry.Namespaces[namespace]; !ok || cfg.LabelSelector != nil {
				t.Errorf("%s: namespace %q = %v (present %v), want unfiltered",
					name, namespace, cfg, ok)
			}
		}

		quantity := resource.MustParse("4")
		full := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name: "q", Namespace: "ns", UID: "uid", ResourceVersion: "7",
				Labels:        map[string]string{"team": "a"},
				Annotations:   map[string]string{"note": "a"},
				ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
			},
			Spec: corev1.ResourceQuotaSpec{
				Hard:   corev1.ResourceList{corev1.ResourcePods: quantity},
				Scopes: []corev1.ResourceQuotaScope{corev1.ResourceQuotaScopeBestEffort},
				ScopeSelector: &corev1.ScopeSelector{
					MatchExpressions: []corev1.ScopedResourceSelectorRequirement{{
						ScopeName: corev1.ResourceQuotaScopePriorityClass,
						Operator:  corev1.ScopeSelectorOpIn,
						Values:    []string{"high"},
					}},
				},
			},
			Status: corev1.ResourceQuotaStatus{
				Hard: corev1.ResourceList{corev1.ResourcePods: quantity},
				Used: corev1.ResourceList{corev1.ResourcePods: quantity},
			},
		}
		got, err := entry.Transform(full.DeepCopy())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		want := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name: "q", Namespace: "ns", UID: "uid", ResourceVersion: "7",
			},
			Spec:   full.Spec,
			Status: full.Status,
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: cached quota = %+v, want %+v", name, got, want)
		}
		// A delete may arrive as a tombstone, which is not a quota.
		tombstone := toolscache.DeletedFinalStateUnknown{Key: "ns/q", Obj: full}
		if got, err := entry.Transform(tombstone); err != nil ||
			!reflect.DeepEqual(got, tombstone) {
			t.Errorf("%s: tombstone = %+v, %v", name, got, err)
		}
	}
}
