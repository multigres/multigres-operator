package features

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

var disabledLabel = map[string]string{ReconcileLabel: Disabled}

func gateFixture(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func ns(name string, labels map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
}

func cm(namespace string, labels map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "o", Namespace: namespace, Labels: labels},
	}
}

func TestGated(t *testing.T) {
	tests := map[string]struct {
		namespaces []client.Object
		obj        client.Object
		laptop     string
		want       bool
	}{
		"ungated object": {
			namespaces: []client.Object{ns("a", nil)},
			obj:        cm("a", nil),
			want:       false,
		},
		"gated by namespace label": {
			namespaces: []client.Object{ns("a", disabledLabel)},
			obj:        cm("a", nil),
			want:       true,
		},
		"gated by object label": {
			namespaces: []client.Object{ns("a", nil)},
			obj:        cm("a", disabledLabel),
			want:       true,
		},
		"malformed object label values are ignored": {
			namespaces: []client.Object{ns("a", nil)},
			obj:        cm("a", map[string]string{ReconcileLabel: "Disabled"}),
			want:       false,
		},
		"TRUE object label value is ignored": {
			namespaces: []client.Object{ns("a", nil)},
			obj:        cm("a", map[string]string{ReconcileLabel: "TRUE"}),
			want:       false,
		},
		"1 object label value is ignored": {
			namespaces: []client.Object{ns("a", nil)},
			obj:        cm("a", map[string]string{ReconcileLabel: "1"}),
			want:       false,
		},
		"empty object label value is ignored": {
			namespaces: []client.Object{ns("a", nil)},
			obj:        cm("a", map[string]string{ReconcileLabel: ""}),
			want:       false,
		},
		"malformed namespace label values are ignored": {
			namespaces: []client.Object{ns("a", map[string]string{ReconcileLabel: "Disabled"})},
			obj:        cm("a", nil),
			want:       false,
		},
		"TRUE namespace label value is ignored": {
			namespaces: []client.Object{ns("a", map[string]string{ReconcileLabel: "TRUE"})},
			obj:        cm("a", nil),
			want:       false,
		},
		"1 namespace label value is ignored": {
			namespaces: []client.Object{ns("a", map[string]string{ReconcileLabel: "1"})},
			obj:        cm("a", nil),
			want:       false,
		},
		"empty namespace label value is ignored": {
			namespaces: []client.Object{ns("a", map[string]string{ReconcileLabel: ""})},
			obj:        cm("a", nil),
			want:       false,
		},
		"missing namespace is not gated by the backstop": {
			obj:  cm("a", nil),
			want: false,
		},
		"laptop claimed namespace bypasses the namespace label": {
			namespaces: []client.Object{ns("a", disabledLabel)},
			obj:        cm("a", nil),
			laptop:     "a",
			want:       false,
		},
		"laptop claimed namespace bypasses the object label": {
			namespaces: []client.Object{ns("a", disabledLabel)},
			obj:        cm("a", disabledLabel),
			laptop:     "a",
			want:       false,
		},
		"laptop claimed namespace whose label was removed is handed back": {
			namespaces: []client.Object{ns("a", nil)},
			obj:        cm("a", nil),
			laptop:     "a",
			want:       true,
		},
		"laptop claimed namespace with a non-disabled label value is handed back": {
			namespaces: []client.Object{
				ns("a", map[string]string{ReconcileLabel: "enabled"}),
			},
			obj:    cm("a", nil),
			laptop: "a",
			want:   true,
		},
		"laptop unclaimed namespace is gated even when ungated": {
			namespaces: []client.Object{ns("op", nil)},
			obj:        cm("op", nil),
			laptop:     "a",
			want:       true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := NewGate(tc.laptop).
				Gated(t.Context(), gateFixture(t, tc.namespaces...), tc.obj)
			if err != nil {
				t.Fatalf("Gated() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("Gated() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGated_NamespaceReadErrorIsNotGated(t *testing.T) {
	boom := errors.New("boom")
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return boom
		},
	}).Build()
	got, err := Gate{}.Gated(t.Context(), c, cm("a", nil))
	if !errors.Is(err, boom) {
		t.Fatalf("Gated() error = %v, want %v", err, boom)
	}
	if got {
		t.Fatal("Gated() = true on error, want false so the caller surfaces the error")
	}
}

func TestReconcileGate(t *testing.T) {
	c := gateFixture(t, ns("open", nil), ns("closed", disabledLabel))
	p := Gate{}.ReconcileGate(c)

	for name, tc := range map[string]struct {
		obj  client.Object
		want bool
	}{
		"ungated object":      {cm("open", nil), true},
		"gated by namespace":  {cm("closed", nil), false},
		"gated by object":     {cm("open", disabledLabel), false},
		"missing namespace":   {cm("nowhere", nil), false},
		"Namespace object":    {ns("closed", disabledLabel), true},
		"Namespace not found": {ns("nowhere", nil), true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := p.Create(event.CreateEvent{Object: tc.obj}); got != tc.want {
				t.Errorf("Create() = %v, want %v", got, tc.want)
			}
			if got := p.Delete(event.DeleteEvent{Object: tc.obj}); got != tc.want {
				t.Errorf("Delete() = %v, want %v", got, tc.want)
			}
			if got := p.Generic(event.GenericEvent{Object: tc.obj}); got != tc.want {
				t.Errorf("Generic() = %v, want %v", got, tc.want)
			}
			if got := p.Update(
				event.UpdateEvent{ObjectOld: tc.obj, ObjectNew: tc.obj},
			); got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("update judges the new object so un-gating an object passes", func(t *testing.T) {
		if !p.Update(event.UpdateEvent{
			ObjectOld: cm("open", disabledLabel), ObjectNew: cm("open", nil),
		}) {
			t.Fatal("Update() = false after the object label was removed, want true")
		}
		if p.Update(event.UpdateEvent{
			ObjectOld: cm("open", nil), ObjectNew: cm("open", disabledLabel),
		}) {
			t.Fatal("Update() = true after the object label was added, want false")
		}
	})

	t.Run("read error drops the event", func(t *testing.T) {
		failing := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errors.New("boom")
			},
		}).Build()
		if (Gate{}).ReconcileGate(failing).Create(event.CreateEvent{Object: cm("open", nil)}) {
			t.Fatal("Create() = true on a namespace read error, want false")
		}
	})
}

func TestReconcileGate_LaptopMode(t *testing.T) {
	c := gateFixture(t, ns("claimed", disabledLabel), ns("operator-ns", nil))
	p := NewGate("claimed").ReconcileGate(c)

	if !p.Create(event.CreateEvent{Object: cm("claimed", nil)}) {
		t.Error("claimed namespace dropped, want passed despite the disabled label")
	}
	if p.Create(event.CreateEvent{Object: cm("operator-ns", nil)}) {
		t.Error("unclaimed namespace passed, want dropped in laptop mode")
	}
	if !p.Create(event.CreateEvent{Object: ns("operator-ns", nil)}) {
		t.Error("Namespace object dropped, want always passed")
	}
}
