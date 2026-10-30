package cell

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

type testClientConfig struct {
	scheme *runtime.Scheme
	wrap   func(client.WithWatch) client.Client
}

type testClientOption func(*testClientConfig)

// withTestScheme replaces cellTestScheme, for the tests that need a scheme the
// reconciler's types are missing from.
func withTestScheme(s *runtime.Scheme) testClientOption {
	return func(c *testClientConfig) { c.scheme = s }
}

// withTestWrap decorates the fake (fault injection, interceptors) before the
// field owner is applied, so the owner stays outermost as it is in production.
func withTestWrap(wrap func(client.WithWatch) client.Client) testClientOption {
	return func(c *testClientConfig) { c.wrap = wrap }
}

// withCellOwner stamps the Cell controller's field manager on c. The fake
// rejects an Apply that names no manager, and a CellReconciler literal bypasses
// SetupWithManagerReconciler, which is what stamps it in production. Tests that
// build their client with bespoke options wrap it here; the rest use
// newTestClient.
func withCellOwner(c client.Client) client.Client {
	return client.WithFieldOwner(c, ownership.ManagerCell)
}

// newTestClient returns the fake client the Cell reconciler's tests share,
// seeded with objs, wrapped once with the Cell controller's field manager. The
// fake rejects an Apply that names no field manager, and reconciler literals
// bypass SetupWithManagerReconciler, which is what normally supplies it.
func newTestClient(t testing.TB, objs ...client.Object) client.Client {
	return newTestClientWith(t, objs)
}

func newTestClientWith(t testing.TB, objs []client.Object, opts ...testClientOption) client.Client {
	t.Helper()
	cfg := testClientConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.scheme == nil {
		cfg.scheme = cellTestScheme(t)
	}
	base := fake.NewClientBuilder().
		WithScheme(cfg.scheme).
		WithObjects(objs...).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		Build()
	var c client.Client = base
	if cfg.wrap != nil {
		c = cfg.wrap(base)
	}
	return withCellOwner(c)
}

// Exported for the external cell_test package, which cannot see the helpers
// above.
var (
	NewTestClient  = newTestClientWith
	WithTestScheme = withTestScheme
	WithTestWrap   = withTestWrap
)
