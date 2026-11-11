//go:build integration

package toposerver

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

func TestTopologyAuthCannotBeDisabledAfterRequested(t *testing.T) {
	scheme := certScheme()
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths(filepath.Join("../../../..", "config", "crd", "bases")))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ts := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "auth-transition", Namespace: "default"},
		Spec:       api.TopoServerSpec{Etcd: &api.EtcdSpec{}},
	}
	require.NoError(t, c.Create(t.Context(), ts))
	ts.Spec.Auth = &api.TopoServerAuthSpec{Enabled: true}
	require.NoError(t, c.Update(t.Context(), ts), "authorization can be enabled after creation")
	for _, scenario := range []string{"false", "remove auth", "remove enabled"} {
		t.Run(scenario, func(t *testing.T) {
			fresh := &api.TopoServer{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh))
			if scenario == "remove enabled" {
				// JSON Merge Patch exercises omission independently of typed defaults.
				err = c.Patch(t.Context(), fresh, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"auth":{"enabled":null}}}`)))
			} else {
				fresh.Spec.Auth.Enabled = false
				if scenario == "remove auth" {
					fresh.Spec.Auth = nil
				}
				err = c.Update(t.Context(), fresh)
			}
			require.True(t, apierrors.IsInvalid(err), "expected transition validation error, got %v", err)
			require.ErrorContains(t, err, "managed authorization cannot be disabled")
		})
	}
}
