package tablegroup

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

// newTestClient returns a fake client seeded with objs, wrapped with the
// TableGroup controller's field manager. Reconciler literals bypass
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
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	return client.WithFieldOwner(
		fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objs...).
			WithStatusSubresource(&multigresv1alpha1.TableGroup{}, &multigresv1alpha1.Shard{}).
			WithInterceptorFuncs(funcs).
			Build(),
		ownership.ManagerTableGroup,
	)
}
