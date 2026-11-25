//go:build integration

package tablegroup_test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// startTableGroupController runs a TableGroup controller against a control
// plane of its own and returns a client for it.
func startTableGroupController(t *testing.T) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	require.NoError(t, (&tablegroup.TableGroupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: new(true)}))
	return mgr.GetClient()
}

// createTableGroup creates a TableGroup named tg-<cluster> holding one shard.
func createTableGroup(
	t *testing.T,
	c client.Client,
	cluster string,
	shard multigresv1alpha1.ShardResolvedSpec,
) *multigresv1alpha1.TableGroup {
	t.Helper()
	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-" + cluster,
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster:    cluster,
				metadata.LabelMultigresDatabase:   "db1",
				metadata.LabelMultigresTableGroup: "tg1",
			},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "db1",
			TableGroupName: "tg1",
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "orch:latest",
				Multipooler: "pooler:latest",
				Postgres:    "postgres:15",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "etcd:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			Shards: []multigresv1alpha1.ShardResolvedSpec{shard},
		},
	}
	setTestPostgresPasswordSecretRef(tg)
	require.NoError(t, c.Create(t.Context(), tg))
	return tg
}

// setTableGroupPaths writes value into the TableGroup at every path, in one
// update, through
// unstructured content so that an empty value is sent as written instead of
// being dropped by the typed struct's omitempty tags.
func setTableGroupPaths(
	t *testing.T,
	c client.Client,
	tg *multigresv1alpha1.TableGroup,
	paths [][]any,
	value map[string]any,
) {
	t.Helper()
	require.NoError(t, testutil.UpdateUnstructured(t.Context(), c,
		multigresv1alpha1.GroupVersion.WithKind("TableGroup"),
		client.ObjectKeyFromObject(tg),
		func(content map[string]any) error {
			for _, path := range paths {
				copied := (&unstructured.Unstructured{Object: value}).DeepCopy().Object
				if err := testutil.SetUnstructuredPath(content, path, copied); err != nil {
					return fmt.Errorf("%v: %w", path, err)
				}
			}
			return nil
		}))
}

// requireClusterShard waits for the cluster's one Shard to satisfy ok, and
// fails with the last Shard spec seen otherwise.
func requireClusterShard(
	t *testing.T,
	c client.Client,
	cluster string,
	what string,
	ok func(spec multigresv1alpha1.ShardSpec) bool,
) {
	t.Helper()
	var last multigresv1alpha1.ShardSpec
	if err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, 30*time.Second, true,
		func(ctx context.Context) (bool, error) {
			shards := &multigresv1alpha1.ShardList{}
			if err := c.List(ctx, shards, client.InNamespace("default"),
				client.MatchingLabels{metadata.LabelMultigresCluster: cluster},
			); err != nil || len(shards.Items) != 1 {
				return false, nil
			}
			last = shards.Items[0].Spec
			return ok(last), nil
		}); err != nil {
		testutil.LogWarnings(t, c, "default", "")
		t.Fatalf("Shard never converged (%s): %+v", what, last)
	}
}

// TestEmptyResourcesBlocksReachShard writes each empty resources shape (such
// as `{limits: {}}`) into a TableGroup whose shard had entries for multiorch,
// postgres and multipooler, and requires the TableGroup controller's Shard
// apply to drop the blocks. An apply that sent `resources: {}` over a block
// that had entries is rejected by the API server, which would leave the Shard
// with the old values. Each shape is followed by a return to entries.
func TestEmptyResourcesBlocksReachShard(t *testing.T) {
	t.Parallel()

	c := startTableGroupController(t)

	tg := createTableGroup(t, c, "empty-resources", multigresv1alpha1.ShardResolvedSpec{
		Name: "s1",
		Multiorch: multigresv1alpha1.MultiorchSpec{
			StatelessSpec: multigresv1alpha1.StatelessSpec{
				Replicas:  new(int32(1)),
				Resources: testutil.PopulatedResources(),
			},
			Cells: []multigresv1alpha1.CellName{"zone-a"},
		},
		Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
			"primary": {
				Type:            "readWrite",
				ReplicasPerCell: new(int32(1)),
				Cells:           []multigresv1alpha1.CellName{"zone-a"},
				Postgres: multigresv1alpha1.ContainerConfig{
					Resources: testutil.PopulatedResources(),
				},
				Multipooler: multigresv1alpha1.ContainerConfig{
					Resources: testutil.PopulatedResources(),
				},
			},
			// A container config with another field set still reaches
			// the apply when its resources block is omitted.
			"replica": {
				Type:            "readWrite",
				ReplicasPerCell: new(int32(1)),
				Cells:           []multigresv1alpha1.CellName{"zone-a"},
				Postgres: multigresv1alpha1.ContainerConfig{
					Resources: testutil.PopulatedResources(),
					RunAsUser: new(int64(999)),
				},
				Multipooler: multigresv1alpha1.ContainerConfig{
					Resources: testutil.PopulatedResources(),
					RunAsUser: new(int64(999)),
				},
			},
		},
	})

	paths := [][]any{
		{"spec", "shards", 0, "multiorch", "resources"},
		{"spec", "shards", 0, "pools", "primary", "postgres", "resources"},
		{"spec", "shards", 0, "pools", "primary", "multipooler", "resources"},
		{"spec", "shards", 0, "pools", "replica", "postgres", "resources"},
		{"spec", "shards", 0, "pools", "replica", "multipooler", "resources"},
	}
	set := func(shape map[string]any) {
		t.Helper()
		setTableGroupPaths(t, c, tg, paths, shape)
	}
	converged := func(populated bool) {
		t.Helper()
		requireClusterShard(t, c, "empty-resources", fmt.Sprintf("want populated=%v", populated),
			func(spec multigresv1alpha1.ShardSpec) bool {
				for _, r := range []corev1.ResourceRequirements{
					spec.Multiorch.Resources,
					spec.Pools["primary"].Postgres.Resources,
					spec.Pools["primary"].Multipooler.Resources,
					spec.Pools["replica"].Postgres.Resources,
					spec.Pools["replica"].Multipooler.Resources,
				} {
					if populated != testutil.IsPopulatedResources(r) ||
						!populated && !testutil.IsEmptyResources(r) {
						return false
					}
				}
				return true
			})
	}

	testutil.RoundTripEmptyResources(t, "the Shard", testutil.EmptyResourcesShapesAndBlock(),
		set, converged)
}

// TestEmptyMultiorchReachesShard empties a TableGroup shard's whole multiorch
// block, which held resources and a placement, and requires the Shard apply
// to succeed. multiorch is a required field, so the apply cannot leave it
// out, and an apply of `multiorch: {}` over a block that had entries is
// rejected; the Shard must instead name the pool cells, which is where the
// Shard controller runs multiorch when no cells are set. It covers `{}`, the
// `{resources: {}}` a TableGroup can hold for a Lite shard, and
// `{placement: {}}`.
func TestEmptyMultiorchReachesShard(t *testing.T) {
	t.Parallel()

	c := startTableGroupController(t)

	populated := map[string]any{
		"resources": map[string]any{"limits": map[string]any{"memory": "321Mi"}},
		"placement": map[string]any{"tolerations": []any{
			map[string]any{"key": "example.com/dedicated", "operator": "Exists"},
		}},
	}
	tg := createTableGroup(t, c, "empty-multiorch", multigresv1alpha1.ShardResolvedSpec{
		Name: "s1",
		Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
			"primary": {
				Type:            "readWrite",
				ReplicasPerCell: new(int32(1)),
				Cells:           []multigresv1alpha1.CellName{"zone-b", "zone-a"},
			},
		},
	})

	multiorch := [][]any{{"spec", "shards", 0, "multiorch"}}
	requireMultiorch := func(wantLimit bool) {
		t.Helper()
		requireClusterShard(t, c, "empty-multiorch", fmt.Sprintf("want limit=%v", wantLimit),
			func(spec multigresv1alpha1.ShardSpec) bool {
				m := spec.Multiorch
				if wantLimit {
					return m.Resources.Limits.Memory().String() == "321Mi" &&
						m.Placement != nil && len(m.Placement.Tolerations) == 1
				}
				return len(m.Resources.Limits) == 0 && len(m.Resources.Requests) == 0 &&
					m.Placement == nil &&
					slices.Equal(m.Cells, []multigresv1alpha1.CellName{"zone-a", "zone-b"})
			})
	}

	for _, s := range []struct {
		name  string
		block map[string]any
	}{
		{"empty block", map[string]any{}},
		{"empty resources", map[string]any{"resources": map[string]any{}}},
		{"empty placement", map[string]any{"placement": map[string]any{}}},
	} {
		setTableGroupPaths(t, c, tg, multiorch, populated)
		requireMultiorch(true)
		setTableGroupPaths(t, c, tg, multiorch, s.block)
		t.Logf("%s: waiting for the Shard to drop its multiorch blocks", s.name)
		requireMultiorch(false)
	}
}

// TestEmptyClusterViewsReachShard empties a TableGroup shard's
// gatewayClusterViews block, and requires the Shard apply to leave it out
// rather than send {} over the block that had enabled set.
func TestEmptyClusterViewsReachShard(t *testing.T) {
	t.Parallel()

	c := startTableGroupController(t)

	tg := createTableGroup(t, c, "empty-views", multigresv1alpha1.ShardResolvedSpec{
		Name: "s1",
		Multiorch: multigresv1alpha1.MultiorchSpec{
			Cells: []multigresv1alpha1.CellName{"zone-a"},
		},
		GatewayClusterViews: &multigresv1alpha1.GatewayClusterViewsConfig{
			Enabled: new(true),
		},
		Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
			"primary": {
				Type:            "readWrite",
				ReplicasPerCell: new(int32(1)),
				Cells:           []multigresv1alpha1.CellName{"zone-a"},
			},
		},
	})

	views := [][]any{{"spec", "shards", 0, "gatewayClusterViews"}}
	requireViews := func(want bool) {
		t.Helper()
		requireClusterShard(t, c, "empty-views", fmt.Sprintf("want set=%v", want),
			func(spec multigresv1alpha1.ShardSpec) bool {
				return (spec.GatewayClusterViews != nil) == want
			})
	}

	requireViews(true)
	setTableGroupPaths(t, c, tg, views, map[string]any{})
	requireViews(false)
	setTableGroupPaths(t, c, tg, views, map[string]any{"enabled": true})
	requireViews(true)
}
