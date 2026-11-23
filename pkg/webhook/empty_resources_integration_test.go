//go:build integration

package webhook_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestWebhook_EmptyResourcesSurviveDefaulting empties resources blocks that
// held entries on a cluster that uses no templates, through the API server
// and the real defaulting webhook, which writes the resolved components back
// into the stored cluster. The stored cluster must keep each block as an
// explicitly empty one, and resolving it must give no requests or limits: a
// block the webhook dropped would resolve to the built-in defaults instead.
func TestWebhook_EmptyResourcesSurviveDefaulting(t *testing.T) {
	ctx := t.Context()
	const ns = "empty-resources-no-templates"
	require.NoError(t, client.IgnoreAlreadyExists(
		k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})))

	limited := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("321Mi")},
	}
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "empty-resources", Namespace: ns},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Resources: *limited.DeepCopy()},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name: "postgres", Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name: "default", Default: true,
					Shards: []multigresv1alpha1.ShardConfig{{
						Name: "0-inf",
						Spec: &multigresv1alpha1.ShardInlineSpec{
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"default": {
									Type:            "readWrite",
									Cells:           []multigresv1alpha1.CellName{"zone-a"},
									ReplicasPerCell: new(int32(2)),
									Postgres: multigresv1alpha1.ContainerConfig{
										Resources: *limited.DeepCopy(),
									},
								},
							},
						},
					}},
				}},
			}},
		},
	}
	setTestPostgresPasswordSecretRef(cluster)
	require.NoError(t, k8sClient.Create(ctx, cluster))
	key := client.ObjectKeyFromObject(cluster)

	etcdPath := []any{"spec", "globalTopoServer", "etcd", "resources"}
	postgresPath := []any{
		"spec", "databases", 0, "tablegroups", 0, "shards", 0,
		"spec", "pools", "default", "postgres", "resources",
	}
	require.NoError(t, testutil.UpdateUnstructured(ctx, k8sClient,
		multigresv1alpha1.GroupVersion.WithKind("MultigresCluster"), key,
		func(content map[string]any) error {
			for _, path := range [][]any{etcdPath, postgresPath} {
				empty := map[string]any{"limits": map[string]any{}}
				if err := testutil.SetUnstructuredPath(content, path, empty); err != nil {
					return err
				}
			}
			return nil
		}))

	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("MultigresCluster"))
	require.NoError(t, k8sClient.Get(ctx, key, stored))
	_, found, err := unstructured.NestedMap(
		stored.Object,
		"spec",
		"globalTopoServer",
		"etcd",
		"resources",
	)
	require.NoError(t, err)
	require.True(t, found, "the stored cluster lost the global etcd resources block")
	databases, _, err := unstructured.NestedSlice(stored.Object, "spec", "databases")
	require.NoError(t, err)
	shardContent := databases[0].(map[string]any)["tablegroups"].([]any)[0].(map[string]any)["shards"].([]any)[0]
	_, found, err = unstructured.NestedMap(shardContent.(map[string]any),
		"spec", "pools", "default", "postgres", "resources")
	require.NoError(t, err)
	require.True(t, found, "the stored cluster lost the postgres resources block")

	typed := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, k8sClient.Get(ctx, key, typed))
	res := resolver.NewResolver(k8sClient, ns)
	topo, err := res.ResolveGlobalTopo(ctx, typed)
	require.NoError(t, err)
	requireNoEntries(t, "global etcd", topo.Etcd.Resources)
	shard, err := res.ResolveShard(ctx, &typed.Spec.Databases[0].TableGroups[0].Shards[0],
		resolver.ResolveShardOptions{AllCellNames: []multigresv1alpha1.CellName{"zone-a"}})
	require.NoError(t, err)
	requireNoEntries(t, "postgres", shard.Pools["default"].Postgres.Resources)
}

func requireNoEntries(t *testing.T, what string, res corev1.ResourceRequirements) {
	t.Helper()
	require.Empty(t, res.Requests, "%s requests were defaulted", what)
	require.Empty(t, res.Limits, "%s limits were defaulted", what)
}
