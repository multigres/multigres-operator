//go:build integration
// +build integration

package cell_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// operatorManagers returns the sorted manager names on obj that start with
// "multigres-", i.e. the operator's own. The creating test client and the
// apiserver appear under other names and are not the subject here.
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

// hasLegacyManager reports whether obj carries a legacy-named entry on any
// resource or subresource.
func hasLegacyManager(obj client.Object) bool {
	return slices.ContainsFunc(managedfields.Managers(obj), ownership.IsLegacy)
}

func TestCell_WritesUnderOwnManager(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	raw := testutil.SetUpClient(t, cfg, scheme)
	mgr := testutil.SetUpManager(t, cfg, scheme)
	if err := (&cellcontroller.CellReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
		t.Fatal(err)
	}

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: "own-cell", Namespace: "default",
			Labels: map[string]string{"multigres.com/cluster": "own-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name:   "zone1",
			ZoneID: "usw1-az1",
			Images: multigresv1alpha1.CellImages{Multigateway: "ghcr.io/multigres/multigres:main"},
			Multigateway: multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "global-topo:2379", RootPath: "/multigres/global", Implementation: "etcd",
			},
		},
	}
	ctx := t.Context()
	k8s := raw
	// Both seeds, so the object carries legacy Apply entries on the main
	// resource and on status as one last written by the pre-rename operator
	// does. The controller is not running yet, so its own reconcile cannot race
	// them. The zero gateway counts are required by the CRD.
	require.NoError(t, managedfields.Seed(ctx, k8s, cell, ownership.LegacyOperator))
	cell.Status = multigresv1alpha1.CellStatus{ObservedGeneration: 1}
	require.NoError(
		t,
		managedfields.Seed(ctx, k8s, cell, ownership.LegacyOperator, managedfields.OnStatus()),
	)
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(cell), cell))
	entries, err := managedfields.Entries(cell)
	require.NoError(t, err)
	var seededOn []string
	for _, e := range entries {
		if ownership.IsLegacy(e.Manager) {
			seededOn = append(seededOn, e.Subresource)
		}
	}
	require.Contains(t, seededOn, "", "seed left no legacy entry on the main resource")
	require.Contains(t, seededOn, "status", "seed left no legacy entry on status")
	seededSpec := cell.Spec.DeepCopy()
	testutil.StartManager(t, mgr)

	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.Cell{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cell), got); err != nil {
			return false
		}
		return slices.Contains(operatorManagers(got), ownership.ManagerCell)
	}, 10*time.Second, 100*time.Millisecond, "Cell never carried its own manager")

	require.Eventually(t, func() bool {
		cur := &multigresv1alpha1.Cell{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cell), cur); err != nil {
			return false
		}
		return !hasLegacyManager(cur)
	}, 15*time.Second, 100*time.Millisecond, "legacy field manager entries never dropped")
	got := &multigresv1alpha1.Cell{}
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(cell), got))
	require.Equal(t, *seededSpec, got.Spec, "spec changed across the legacy drop")
	requireOnlyManagers(t, operatorManagers(got), ownership.ManagerCell)
}
