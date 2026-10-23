package features

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestLabelsChanged(t *testing.T) {
	p := LabelsChanged()

	t.Run("unrelated label edit is ignored", func(t *testing.T) {
		old := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"other": "a"}},
		}
		new := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"other": "b"}},
		}
		if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: new}) {
			t.Fatal("Update() = true for unrelated label change, want false")
		}
	})

	t.Run("reconcile label value edit is reported", func(t *testing.T) {
		flagLabels := map[string]string{ReconcileLabel: Disabled}
		old := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Labels: flagLabels}}
		new := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}}
		if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: new}) {
			t.Fatal("Update() = false when reconcile label removed, want true")
		}
	})

	t.Run("create is always reported", func(t *testing.T) {
		if !p.Create(event.CreateEvent{Object: &corev1.ConfigMap{}}) {
			t.Fatal("Create() = false, want true")
		}
	})

	t.Run("delete is always reported", func(t *testing.T) {
		if !p.Delete(event.DeleteEvent{Object: &corev1.ConfigMap{}}) {
			t.Fatal("Delete() = false, want true")
		}
	})

	t.Run("generic is never reported", func(t *testing.T) {
		if p.Generic(event.GenericEvent{Object: &corev1.ConfigMap{}}) {
			t.Fatal("Generic() = true, want false")
		}
	})
}

func TestEnqueueAllInNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	seed := []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: "a"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "a2", Namespace: "a"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "b"}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed...).Build()

	// The real call site is a Namespace watch, so the event object is the
	// cluster-scoped *corev1.Namespace whose Name is the target namespace.
	eventObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a"}}

	fn := Gate{}.enqueueAllInNamespace(
		c,
		func() client.ObjectList { return &corev1.ConfigMapList{} },
	)
	got := fn(t.Context(), eventObj)

	keys := make(map[string]bool, len(got))
	for _, r := range got {
		keys[r.Namespace+"/"+r.Name] = true
	}
	if len(got) != 2 || !keys["a/a1"] || !keys["a/a2"] {
		t.Fatalf("enqueueAllInNamespace() = %v, want exactly a/a1 and a/a2", keys)
	}
	if keys["b/b1"] {
		t.Fatal("enqueueAllInNamespace() requested an object from another namespace")
	}
}

// listFailingClient fails the test on any List, so a skipped namespace is
// proven to have been skipped before the client is touched.
type listFailingClient struct {
	client.Client
	lists int
}

func (c *listFailingClient) List(
	ctx context.Context, list client.ObjectList, opts ...client.ListOption,
) error {
	c.lists++
	return c.Client.List(ctx, list, opts...)
}

func TestEnqueueAllInNamespace_LaptopModeSkipsUnclaimedWithoutListing(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	c := &listFailingClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "a1", Namespace: "a"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: "b"}},
	).Build()}
	fn := NewGate("a").enqueueAllInNamespace(
		c,
		func() client.ObjectList { return &corev1.ConfigMapList{} },
	)

	if got := fn(
		t.Context(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "b"}},
	); got != nil {
		t.Fatalf("unclaimed namespace enqueued %v, want nothing", got)
	}
	if c.lists != 0 {
		t.Fatalf("unclaimed namespace issued %d List calls, want 0", c.lists)
	}
	if got := fn(
		t.Context(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "a"}},
	); len(
		got,
	) != 1 {
		t.Fatalf("claimed namespace enqueued %v, want a/a1", got)
	}
	if c.lists != 1 {
		t.Fatalf("claimed namespace issued %d List calls, want 1", c.lists)
	}
}
