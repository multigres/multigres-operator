//go:build integration
// +build integration

package multigrescluster_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

// operatorManagers narrows managedfields.Managers to the operator's own
// "multigres-" names: the creating test client and the apiserver also appear,
// and are not the subject here.
func operatorManagers(obj client.Object) []string {
	return slices.DeleteFunc(managedfields.Managers(obj), func(m string) bool {
		return !strings.HasPrefix(m, "multigres-")
	})
}

// requireOnlyManagers asserts got names main and nothing but main and the
// facets, and no legacy name.
func requireOnlyManagers(t *testing.T, got []string, main string, facets ...string) {
	t.Helper()
	require.Contains(t, got, main)
	allowed := append([]string{main}, facets...)
	for _, m := range got {
		require.Contains(t, allowed, m, "unexpected operator field manager %q", m)
		require.False(t, ownership.IsLegacy(m), "legacy field manager %q present", m)
	}
}

// hasLegacyManager reports whether obj carries an entry from a legacy manager.
func hasLegacyManager(obj client.Object) bool {
	return slices.ContainsFunc(managedfields.Managers(obj), ownership.IsLegacy)
}

// requireLegacyEntries asserts the seed left a legacy entry on the main
// resource and on status, as one last written by the pre-rename operator does.
func requireLegacyEntries(t *testing.T, obj client.Object) {
	t.Helper()
	entries, err := managedfields.Entries(obj)
	require.NoError(t, err)
	for _, sub := range []string{"", "status"} {
		require.True(t, slices.ContainsFunc(entries, func(e managedfields.Entry) bool {
			return ownership.IsLegacy(e.Manager) && e.Subresource == sub
		}), "seed left no legacy entry on subresource %q", sub)
	}
}

func TestMultigresCluster_WritesUnderOwnManager(t *testing.T) {
	t.Parallel()

	// The first reconcile waits for the legacy seed, so no new-name write
	// lands between the two seeding applies.
	seeded := make(chan struct{})
	k8s, _, reader := setupIntegrationCore(t,
		func(inner reconcile.Reconciler) reconcile.Reconciler {
			return reconcile.Func(
				func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
					select {
					case <-seeded:
					case <-ctx.Done():
						return reconcile.Result{}, ctx.Err()
					}
					return inner.Reconcile(ctx, req)
				},
			)
		}, nil)
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "own-cluster", Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
		},
	}
	setTestPostgresPasswordSecretRef(cluster)
	ctx := t.Context()
	// Applied twice, as the pre-rename operator wrote it: the main resource,
	// then status with the fields the CRD requires. The controller is held back
	// until the seed is done, or its own reconcile races it. The reader is
	// uncached so it sees the seed at once.
	require.NoError(t, managedfields.Seed(ctx, k8s, cluster, ownership.LegacyOperator))
	cluster.Status = multigresv1alpha1.MultigresClusterStatus{ObservedGeneration: 1}
	require.NoError(t, managedfields.Seed(ctx, k8s, cluster, ownership.LegacyOperator,
		managedfields.OnStatus()))
	require.NoError(t, reader.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
	requireLegacyEntries(t, cluster)
	seededSpec := cluster.Spec.DeepCopy()
	close(seeded)

	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.MultigresCluster{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cluster), got); err != nil {
			return false
		}
		return slices.Contains(operatorManagers(got), ownership.ManagerCluster)
	}, 15*time.Second, 100*time.Millisecond, "MultigresCluster never carried its own manager")

	// Only DropLegacy retires legacy entries, so this waits for the reconcile.
	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.MultigresCluster{}
		return reader.Get(ctx, client.ObjectKeyFromObject(cluster), got) == nil &&
			!hasLegacyManager(got)
	}, 15*time.Second, 100*time.Millisecond, "legacy field manager entries never dropped")
	got := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(cluster), got))
	require.Equal(t, *seededSpec, got.Spec, "spec changed across the legacy drop")
	requireOnlyManagers(t, operatorManagers(got), ownership.ManagerCluster)

	// Children are written by the cluster controller; only it runs here.
	cells := &multigresv1alpha1.CellList{}
	require.Eventually(t, func() bool {
		return k8s.List(ctx, cells, client.InNamespace(testNamespace)) == nil &&
			len(cells.Items) == 1
	}, 10*time.Second, 100*time.Millisecond, "child Cell never created")
	requireOnlyManagers(t, operatorManagers(&cells.Items[0]), ownership.ManagerCluster)
}
