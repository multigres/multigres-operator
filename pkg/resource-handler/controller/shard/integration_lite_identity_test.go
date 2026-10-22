//go:build integration

package shard_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// No webhook is installed here: these checks exercise the generated CRD CEL
// rules in the API server, including clearing/removing an optional mode field.
func TestLiteSchemaGuards(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	mgr := testutil.SetUpEnvtestManager(
		t,
		scheme,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	c := testutil.SetUpClient(t, mgr.GetConfig(), scheme)
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		name := "regular"
		if mode != "" {
			name = "lite"
		}
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Mode:  mode,
				Cells: []multigresv1alpha1.CellConfig{{Name: "z1", ZoneID: "us-central1-a"}},
				PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
					Name: "postgres-password",
					Key:  "password",
				},
			},
		}
		require.NoError(t, c.Create(t.Context(), cluster))
		updated := cluster.DeepCopy()
		if mode == "" {
			updated.Spec.Mode = multigresv1alpha1.ModeLite
		} else {
			updated.Spec.Mode = ""
		}
		require.ErrorContains(t, c.Update(t.Context(), updated), "mode is immutable")
		if mode == multigresv1alpha1.ModeLite {
			updated = cluster.DeepCopy()
			updated.Spec.Cells = append(
				updated.Spec.Cells,
				multigresv1alpha1.CellConfig{Name: "z2", ZoneID: "us-central1-b"},
			)
			require.ErrorContains(t, c.Update(t.Context(), updated), "exactly one cluster cell")
			updated.ResourceVersion = ""
			updated.UID = ""
			updated.Name = "extra-cells"
			require.ErrorContains(t, c.Create(t.Context(), updated), "exactly one cluster cell")
		}
	}
	shard := newLiteTestShard("identity-shard", "identity-cluster")
	setTestPostgresPasswordSecretRef(shard)
	require.NoError(t, c.Create(t.Context(), shard))
	for _, mutate := range []func(*multigresv1alpha1.Shard){
		func(s *multigresv1alpha1.Shard) { s.Spec.Mode = "" },
		func(s *multigresv1alpha1.Shard) {
			s.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"replacement": s.Spec.Pools["default"]}
		},
		func(s *multigresv1alpha1.Shard) {
			p := s.Spec.Pools["default"]
			p.Cells = append(p.Cells, "zone2")
			s.Spec.Pools["default"] = p
		},
	} {
		updated := shard.DeepCopy()
		mutate(updated)
		require.ErrorContains(t, c.Update(t.Context(), updated), "immutable")
	}
	updated := shard.DeepCopy()
	pool := updated.Spec.Pools["default"]
	pool.Storage.Size = "5Gi"
	updated.Spec.Pools["default"] = pool
	require.NoError(
		t,
		c.Update(t.Context(), updated),
		"ordinary storage growth must remain supported",
	)
}
