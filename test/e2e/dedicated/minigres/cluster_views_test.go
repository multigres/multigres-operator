//go:build e2e

package minigres_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestMinigresClusterViews(t *testing.T) {
	cr, pod := startLiteRuntime(t, "")
	ns := cr.Namespace
	c, err := cluster.CRClient()
	require.NoError(t, err)
	sqlEventually(
		t,
		ns,
		pod.Name,
		"CREATE TABLE views_e2e (id integer PRIMARY KEY)",
		"CREATE TABLE",
	)
	sqlMust(t, ns, pod.Name, "INSERT INTO views_e2e VALUES (1)", "INSERT 0 1")
	pvc := dataPVC(t, ns, pod)
	assertDisabled := func() {
		t.Helper()
		for _, view := range []string{"multigateways", "multipoolers"} {
			out, err := sql(ns, pod.Name, "SELECT * FROM multigres."+view)
			require.Error(t, err)
			require.Contains(t, out, `relation "multigres.`+view+`" does not exist`)
		}
	}
	assertDisabled()
	for _, enabled := range []bool{true, false} {
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cr), cr))
		patch := client.MergeFrom(cr.DeepCopy())
		if cr.Spec.Cells[0].Spec == nil {
			cr.Spec.Cells[0].Spec = &multigresv1alpha1.CellInlineSpec{}
		}
		cr.Spec.Cells[0].Spec.Multigateway.ClusterViews = &multigresv1alpha1.GatewayClusterViewsConfig{
			Enabled: ptr.To(enabled),
		}
		require.NoError(t, c.Patch(t.Context(), cr, patch))
		pod = waitPod(t, ns, cr.Name, pod.UID)
		sqlEventually(t, ns, pod.Name, "SELECT id FROM views_e2e", "1")
		require.Equal(t, pvc.UID, dataPVC(t, ns, pod).UID)
		if enabled {
			sqlEventually(
				t,
				ns,
				pod.Name,
				"SELECT count(*) FROM multigres.multigateways WHERE is_self",
				"1",
			)
			sqlEventually(t, ns, pod.Name, "SELECT count(*) FROM multigres.multipoolers", "1")
		} else {
			assertDisabled()
		}
		t.Logf(
			"clusterViews.enabled=%t: pod=%s pvc=%s, persisted row intact",
			enabled,
			pod.UID,
			pvc.UID,
		)
	}
}
