package multigrescluster

import (
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

// newTestClient returns a fake client seeded with objs, wrapped with the
// MultigresCluster controller's field manager. Reconciler literals bypass
// SetupWithManagerReconciler, which is where production applies that wrap, and
// the fake client rejects an Apply that names no manager.
func newTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return newTestClientWith(t, interceptor.Funcs{}, objs...)
}

// newTestClientWith is newTestClient with interceptors installed beneath the
// field-manager wrap, so they observe the same calls the controller makes.
func newTestClientWith(
	t *testing.T,
	funcs interceptor.Funcs,
	objs ...client.Object,
) client.Client {
	t.Helper()
	return client.WithFieldOwner(
		fake.NewClientBuilder().
			WithScheme(setupScheme()).
			WithObjects(objs...).
			WithStatusSubresource(
				&multigresv1alpha1.MultigresCluster{},
				&multigresv1alpha1.Cell{},
				&multigresv1alpha1.TableGroup{},
				&multigresv1alpha1.TopoServer{},
			).
			WithInterceptorFuncs(funcs).
			Build(),
		ownership.ManagerCluster,
	)
}
