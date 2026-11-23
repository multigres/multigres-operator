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
	"k8s.io/apimachinery/pkg/api/resource"
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

// TestEmptyResourcesBlocksReachShard writes each empty resources shape (such
// as `{limits: {}}`) into a TableGroup whose shard had entries for multiorch,
// postgres and multipooler, and requires the TableGroup controller's Shard
// apply to drop the blocks. An apply that sent `resources: {}` over a block
// that had entries is rejected by the API server, which would leave the Shard
// with the old values. Each shape is followed by a return to entries.
func TestEmptyResourcesBlocksReachShard(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	reconciler := &tablegroup.TableGroupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}
	require.NoError(t, reconciler.SetupWithManager(mgr, controller.Options{
		SkipNameValidation: new(true),
	}))
	c := mgr.GetClient()
	ctx := t.Context()

	populated := func() corev1.ResourceRequirements {
		return corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("123m")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("321Mi")},
		}
	}
	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-empty-resources",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster:    "empty-resources",
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
			Shards: []multigresv1alpha1.ShardResolvedSpec{{
				Name: "s1",
				Multiorch: multigresv1alpha1.MultiorchSpec{
					StatelessSpec: multigresv1alpha1.StatelessSpec{
						Replicas:  new(int32(1)),
						Resources: populated(),
					},
					Cells: []multigresv1alpha1.CellName{"zone-a"},
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Type:            "readWrite",
						ReplicasPerCell: new(int32(1)),
						Cells:           []multigresv1alpha1.CellName{"zone-a"},
						Postgres:        multigresv1alpha1.ContainerConfig{Resources: populated()},
						Multipooler:     multigresv1alpha1.ContainerConfig{Resources: populated()},
					},
					// A container config with another field set still reaches
					// the apply when its resources block is omitted.
					"replica": {
						Type:            "readWrite",
						ReplicasPerCell: new(int32(1)),
						Cells:           []multigresv1alpha1.CellName{"zone-a"},
						Postgres: multigresv1alpha1.ContainerConfig{
							Resources: populated(),
							RunAsUser: new(int64(999)),
						},
						Multipooler: multigresv1alpha1.ContainerConfig{
							Resources: populated(),
							RunAsUser: new(int64(999)),
						},
					},
				},
			}},
		},
	}
	setTestPostgresPasswordSecretRef(tg)
	require.NoError(t, c.Create(ctx, tg))

	paths := map[string][]any{
		"multiorch":        {"spec", "shards", 0, "multiorch", "resources"},
		"postgres":         {"spec", "shards", 0, "pools", "primary", "postgres", "resources"},
		"multipooler":      {"spec", "shards", 0, "pools", "primary", "multipooler", "resources"},
		"replica postgres": {"spec", "shards", 0, "pools", "replica", "postgres", "resources"},
		"replica multipooler": {
			"spec", "shards", 0, "pools", "replica", "multipooler", "resources",
		},
	}
	setResources := func(shape map[string]any) {
		t.Helper()
		require.NoError(t, testutil.UpdateUnstructured(ctx, c,
			multigresv1alpha1.GroupVersion.WithKind("TableGroup"),
			client.ObjectKeyFromObject(tg),
			func(content map[string]any) error {
				for name, path := range paths {
					value := (&unstructured.Unstructured{Object: shape}).DeepCopy().Object
					if err := testutil.SetUnstructuredPath(content, path, value); err != nil {
						return fmt.Errorf("%s: %w", name, err)
					}
				}
				return nil
			}))
	}
	requireShardResources := func(want bool) {
		t.Helper()
		var last map[string]corev1.ResourceRequirements
		if err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, 30*time.Second, true,
			func(ctx context.Context) (bool, error) {
				shards := &multigresv1alpha1.ShardList{}
				if err := c.List(
					ctx,
					shards,
					client.InNamespace("default"),
					client.MatchingLabels{
						metadata.LabelMultigresCluster: "empty-resources",
					},
				); err != nil ||
					len(shards.Items) != 1 {
					return false, nil
				}
				spec := shards.Items[0].Spec
				last = map[string]corev1.ResourceRequirements{
					"multiorch":           spec.Multiorch.Resources,
					"postgres":            spec.Pools["primary"].Postgres.Resources,
					"multipooler":         spec.Pools["primary"].Multipooler.Resources,
					"replica postgres":    spec.Pools["replica"].Postgres.Resources,
					"replica multipooler": spec.Pools["replica"].Multipooler.Resources,
				}
				for _, r := range last {
					if want {
						if r.Requests.Cpu().String() != "123m" ||
							r.Limits.Memory().String() != "321Mi" {
							return false, nil
						}
					} else if len(r.Requests) != 0 || len(r.Limits) != 0 || len(r.Claims) != 0 {
						return false, nil
					}
				}
				return true, nil
			}); err != nil {
			testutil.LogWarnings(t, c, "default", "")
			t.Fatalf("Shard never converged (want populated=%v): %v", want, last)
		}
	}

	populatedJSON, err := runtime.DefaultUnstructuredConverter.ToUnstructured(new(populated()))
	require.NoError(t, err)

	requireShardResources(true)
	for _, s := range []struct {
		name  string
		shape map[string]any
	}{
		{"empty requests", map[string]any{"requests": map[string]any{}}},
		{"empty limits", map[string]any{"limits": map[string]any{}}},
		{"empty claims", map[string]any{"claims": []any{}}},
		{"all empty", map[string]any{
			"requests": map[string]any{},
			"limits":   map[string]any{},
			"claims":   []any{},
		}},
		{"empty block", map[string]any{}},
	} {
		setResources(s.shape)
		t.Logf("shape %s: waiting for the Shard to drop its resources", s.name)
		requireShardResources(false)

		setResources(populatedJSON)
		t.Logf("shape %s: waiting for the Shard to carry resources again", s.name)
		requireShardResources(true)
	}
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
	c := mgr.GetClient()
	ctx := t.Context()

	populated := map[string]any{
		"resources": map[string]any{"limits": map[string]any{"memory": "321Mi"}},
		"placement": map[string]any{"tolerations": []any{
			map[string]any{"key": "example.com/dedicated", "operator": "Exists"},
		}},
	}
	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-empty-multiorch",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster:    "empty-multiorch",
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
			Shards: []multigresv1alpha1.ShardResolvedSpec{{
				Name: "s1",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"primary": {
						Type:            "readWrite",
						ReplicasPerCell: new(int32(1)),
						Cells:           []multigresv1alpha1.CellName{"zone-b", "zone-a"},
					},
				},
			}},
		},
	}
	setTestPostgresPasswordSecretRef(tg)
	require.NoError(t, c.Create(ctx, tg))

	setMultiorch := func(block map[string]any) {
		t.Helper()
		require.NoError(t, testutil.UpdateUnstructured(ctx, c,
			multigresv1alpha1.GroupVersion.WithKind("TableGroup"),
			client.ObjectKeyFromObject(tg),
			func(content map[string]any) error {
				value := (&unstructured.Unstructured{Object: block}).DeepCopy().Object
				return testutil.SetUnstructuredPath(content,
					[]any{"spec", "shards", 0, "multiorch"}, value)
			}))
	}
	requireShardMultiorch := func(wantLimit bool) {
		t.Helper()
		var last multigresv1alpha1.MultiorchSpec
		if err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, 30*time.Second, true,
			func(ctx context.Context) (bool, error) {
				shards := &multigresv1alpha1.ShardList{}
				if err := c.List(
					ctx,
					shards,
					client.InNamespace("default"),
					client.MatchingLabels{
						metadata.LabelMultigresCluster: "empty-multiorch",
					},
				); err != nil ||
					len(shards.Items) != 1 {
					return false, nil
				}
				last = shards.Items[0].Spec.Multiorch
				if wantLimit {
					return last.Resources.Limits.Memory().String() == "321Mi" &&
						last.Placement != nil && len(last.Placement.Tolerations) == 1, nil
				}
				return len(last.Resources.Limits) == 0 && len(last.Resources.Requests) == 0 &&
					last.Placement == nil &&
					slices.Equal(last.Cells, []multigresv1alpha1.CellName{"zone-a", "zone-b"}), nil
			}); err != nil {
			testutil.LogWarnings(t, c, "default", "")
			t.Fatalf("Shard multiorch never converged (want limit=%v): %+v", wantLimit, last)
		}
	}

	for _, s := range []struct {
		name  string
		block map[string]any
	}{
		{"empty block", map[string]any{}},
		{"empty resources", map[string]any{"resources": map[string]any{}}},
		{"empty placement", map[string]any{"placement": map[string]any{}}},
	} {
		setMultiorch(populated)
		requireShardMultiorch(true)
		setMultiorch(s.block)
		t.Logf("%s: waiting for the Shard to drop its multiorch blocks", s.name)
		requireShardMultiorch(false)
	}
}

// TestEmptyClusterViewsReachShard empties a TableGroup shard's
// gatewayClusterViews block, and requires the Shard apply to leave it out
// rather than send {} over the block that had enabled set.
func TestEmptyClusterViewsReachShard(t *testing.T) {
	t.Parallel()

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
	c := mgr.GetClient()
	ctx := t.Context()

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-empty-views",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster:    "empty-views",
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
			Shards: []multigresv1alpha1.ShardResolvedSpec{{
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
			}},
		},
	}
	setTestPostgresPasswordSecretRef(tg)
	require.NoError(t, c.Create(ctx, tg))

	requireViews := func(want bool) {
		t.Helper()
		var last *multigresv1alpha1.GatewayClusterViewsConfig
		if err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, 30*time.Second, true,
			func(ctx context.Context) (bool, error) {
				shards := &multigresv1alpha1.ShardList{}
				if err := c.List(ctx, shards, client.InNamespace("default"),
					client.MatchingLabels{metadata.LabelMultigresCluster: "empty-views"},
				); err != nil || len(shards.Items) != 1 {
					return false, nil
				}
				last = shards.Items[0].Spec.GatewayClusterViews
				return (last != nil) == want, nil
			}); err != nil {
			testutil.LogWarnings(t, c, "default", "")
			t.Fatalf("Shard gatewayClusterViews never converged (want set=%v): %+v", want, last)
		}
	}

	setViews := func(views map[string]any) {
		t.Helper()
		require.NoError(t, testutil.UpdateUnstructured(ctx, c,
			multigresv1alpha1.GroupVersion.WithKind("TableGroup"),
			client.ObjectKeyFromObject(tg),
			func(content map[string]any) error {
				return testutil.SetUnstructuredPath(content,
					[]any{"spec", "shards", 0, "gatewayClusterViews"}, views)
			}))
	}

	requireViews(true)
	setViews(map[string]any{})
	requireViews(false)
	setViews(map[string]any{"enabled": true})
	requireViews(true)
}
