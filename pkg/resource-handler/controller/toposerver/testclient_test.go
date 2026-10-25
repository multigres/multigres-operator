package toposerver

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

// newFakeBase builds the unwrapped fake client the package's tests share, for
// the tests that must layer something (failure injection, an interceptor) or
// read the store without a field owner. Status subresources are enabled for
// the kinds the reconciler writes status on, but only those scheme knows:
// the fake builder panics on an unregistered type, and some tests build
// against a deliberately incomplete scheme.
func newFakeBase(t *testing.T, scheme *runtime.Scheme, objs ...client.Object) client.WithWatch {
	t.Helper()
	b := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...)
	for _, o := range []client.Object{
		&multigresv1alpha1.TopoServer{},
		&appsv1.StatefulSet{},
		&corev1.Pod{},
	} {
		if gvks, _, err := scheme.ObjectKinds(o); err == nil && len(gvks) > 0 {
			b = b.WithStatusSubresource(o)
		}
	}
	return b.Build()
}

// newTestClient returns newFakeBase wrapped with the controller's own field
// manager. The fake client rejects an Apply with no field manager, and the
// reconciler literals in these tests bypass SetupWithManagerReconciler, which
// is where production wires the owner in.
func newTestClient(t *testing.T, scheme *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return client.WithFieldOwner(newFakeBase(t, scheme, objs...), ownership.ManagerTopoServer)
}
