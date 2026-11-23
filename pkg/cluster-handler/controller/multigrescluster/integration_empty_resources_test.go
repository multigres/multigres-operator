//go:build integration

package multigrescluster_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func populatedResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("123m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("321Mi")},
	}
}

// emptyResourcesShapes are the non-zero ways a resources block can ask for
// nothing. `{limits: {}}` is what kubectl edit leaves after the last limit is
// deleted.
var emptyResourcesShapes = []struct {
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
}

// childResources reports the resources blocks of a cluster's children that a
// test follows, keyed by a name for failure messages.
type childResources func(ctx context.Context, c client.Client, clusterName string) (
	map[string]corev1.ResourceRequirements, error)

// TestEmptyResourcesBlocksReachChildren sets every resources block on a
// MultigresCluster to entries, waits for the children the cluster controller
// applies (TableGroup, Cell, global TopoServer) to carry them, then replaces
// each block with an empty shape. A block set to empty overrides the defaults,
// so the children must drop it. An apply that sent `resources: {}` over a
// block that had entries is rejected by the API server, and the cluster
// controller would fail every pass with the children stuck on the old values.
// Each shape is followed by a return to entries, so the trip back is covered.
//
// A cluster with a managed local TopoServer does not create its TableGroups
// until that TopoServer is ready, and the Cell controller does not run here,
// so the Cell's local etcd block is covered by a second cluster.
func TestEmptyResourcesBlocksReachChildren(t *testing.T) {
	t.Parallel()

	k8sClient, _ := setupIntegration(t)
	ctx := t.Context()
	// The default CellTemplate sets a gateway replica count, which admission
	// refuses for a Lite cluster.
	liteCells := &multigresv1alpha1.CellTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "lite", Namespace: testNamespace},
	}
	if err := k8sClient.Create(ctx, liteCells); client.IgnoreAlreadyExists(err) != nil {
		t.Fatal(err)
	}

	shardOverrides := []any{"spec", "databases", 0, "tablegroups", 0, "shards", 0, "overrides"}
	variants := []struct {
		cluster  *multigresv1alpha1.MultigresCluster
		paths    map[string][]any
		children childResources
	}{
		{
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "empty-resources", Namespace: testNamespace},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "default",
						CellTemplate:  "default",
						ShardTemplate: "default",
					},
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{
							Image:     "etcd:latest",
							Resources: populatedResources(),
						},
					},
					Cells: []multigresv1alpha1.CellConfig{{
						Name:   "zone-a",
						ZoneID: "use1-az1",
						Spec: &multigresv1alpha1.CellInlineSpec{
							Multigateway: multigresv1alpha1.MultigatewaySpec{
								StatelessSpec: multigresv1alpha1.StatelessSpec{
									Resources: populatedResources(),
								},
							},
						},
					}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						Name:    "postgres",
						Default: true,
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Name:    "default",
							Default: true,
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "0-inf",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Multiorch: &multigresv1alpha1.MultiorchSpec{
										StatelessSpec: multigresv1alpha1.StatelessSpec{
											Resources: populatedResources(),
										},
									},
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Type:            "readWrite",
											ReplicasPerCell: new(int32(1)),
											// RunAsUser keeps the postgres
											// config non-empty once its
											// resources block is omitted.
											Postgres: multigresv1alpha1.ContainerConfig{
												Resources: populatedResources(),
												RunAsUser: new(int64(999)),
											},
											Multipooler: multigresv1alpha1.ContainerConfig{
												Resources: populatedResources(),
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			paths: map[string][]any{
				"globalTopoServer.etcd": {"spec", "globalTopoServer", "etcd", "resources"},
				"cell.multigateway":     {"spec", "cells", 0, "spec", "multigateway", "resources"},
				"shard.multiorch":       appendPath(shardOverrides, "multiorch", "resources"),
				"shard.postgres": appendPath(shardOverrides,
					"pools", "default", "postgres", "resources"),
				"shard.multipooler": appendPath(shardOverrides,
					"pools", "default", "multipooler", "resources"),
			},
			children: func(ctx context.Context, c client.Client, clusterName string) (
				map[string]corev1.ResourceRequirements, error,
			) {
				inCluster := clusterSelector(clusterName)
				tgs := &multigresv1alpha1.TableGroupList{}
				if err := c.List(ctx, tgs, inCluster...); err != nil {
					return nil, err
				}
				if len(tgs.Items) != 1 || len(tgs.Items[0].Spec.Shards) != 1 {
					return nil, fmt.Errorf("want 1 TableGroup with 1 shard, got %d TableGroups",
						len(tgs.Items))
				}
				cells := &multigresv1alpha1.CellList{}
				if err := c.List(ctx, cells, inCluster...); err != nil {
					return nil, err
				}
				if len(cells.Items) != 1 {
					return nil, fmt.Errorf("want 1 Cell, got %d", len(cells.Items))
				}
				topos := &multigresv1alpha1.TopoServerList{}
				if err := c.List(ctx, topos, inCluster...); err != nil {
					return nil, err
				}
				if len(topos.Items) != 1 || topos.Items[0].Spec.Etcd == nil {
					return nil, fmt.Errorf("want 1 TopoServer with etcd, got %d", len(topos.Items))
				}
				shard := tgs.Items[0].Spec.Shards[0]
				return map[string]corev1.ResourceRequirements{
					"TableGroup multiorch":   shard.Multiorch.Resources,
					"TableGroup postgres":    shard.Pools["default"].Postgres.Resources,
					"TableGroup multipooler": shard.Pools["default"].Multipooler.Resources,
					"Cell multigateway":      cells.Items[0].Spec.Multigateway.Resources,
					"TopoServer etcd":        topos.Items[0].Spec.Etcd.Resources,
				}, nil
			},
		},
		{
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "empty-resources-local",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "default",
						CellTemplate:  "default",
						ShardTemplate: "default",
					},
					Cells: []multigresv1alpha1.CellConfig{{
						Name:   "zone-a",
						ZoneID: "use1-az1",
						Spec: &multigresv1alpha1.CellInlineSpec{
							LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
								Etcd: &multigresv1alpha1.EtcdSpec{
									Image:     "etcd:local",
									Resources: populatedResources(),
								},
							},
						},
					}},
				},
			},
			paths: map[string][]any{
				"cell.localTopoServer": {
					"spec",
					"cells",
					0,
					"spec",
					"localTopoServer",
					"etcd",
					"resources",
				},
			},
			children: func(ctx context.Context, c client.Client, clusterName string) (
				map[string]corev1.ResourceRequirements, error,
			) {
				cells := &multigresv1alpha1.CellList{}
				if err := c.List(ctx, cells, clusterSelector(clusterName)...); err != nil {
					return nil, err
				}
				if len(cells.Items) != 1 || cells.Items[0].Spec.TopoServer == nil ||
					cells.Items[0].Spec.TopoServer.Etcd == nil {
					return nil, fmt.Errorf(
						"want 1 Cell with a local etcd, got %d Cells",
						len(cells.Items),
					)
				}
				return map[string]corev1.ResourceRequirements{
					"Cell local etcd": cells.Items[0].Spec.TopoServer.Etcd.Resources,
				}, nil
			},
		},
		{
			// Lite resolves multiorch and multigateway without defaults, so an
			// emptied block reaches the child applies through its own branch.
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "empty-resources-lite",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Mode: multigresv1alpha1.ModeLite,
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "default",
						CellTemplate:  "lite",
						ShardTemplate: "default",
					},
					Cells: []multigresv1alpha1.CellConfig{{
						Name:   "zone-a",
						ZoneID: "use1-az1",
						Overrides: &multigresv1alpha1.CellOverrides{
							Multigateway: &multigresv1alpha1.MultigatewaySpec{
								StatelessSpec: multigresv1alpha1.StatelessSpec{
									Resources: populatedResources(),
								},
							},
						},
					}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						Name:    "postgres",
						Default: true,
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Name:    "default",
							Default: true,
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "0-inf",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Multiorch: &multigresv1alpha1.MultiorchSpec{
										StatelessSpec: multigresv1alpha1.StatelessSpec{
											Resources: populatedResources(),
										},
									},
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Type:            "readWrite",
											Cells:           []multigresv1alpha1.CellName{"zone-a"},
											ReplicasPerCell: new(int32(1)),
											Postgres: multigresv1alpha1.ContainerConfig{
												Resources: populatedResources(),
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			paths: map[string][]any{
				"cell.multigateway": {"spec", "cells", 0, "overrides", "multigateway", "resources"},
				"shard.multiorch":   appendPath(shardOverrides, "multiorch", "resources"),
				"shard.postgres": appendPath(shardOverrides,
					"pools", "default", "postgres", "resources"),
			},
			children: func(ctx context.Context, c client.Client, clusterName string) (
				map[string]corev1.ResourceRequirements, error,
			) {
				inCluster := clusterSelector(clusterName)
				tgs := &multigresv1alpha1.TableGroupList{}
				if err := c.List(ctx, tgs, inCluster...); err != nil {
					return nil, err
				}
				if len(tgs.Items) != 1 || len(tgs.Items[0].Spec.Shards) != 1 {
					return nil, fmt.Errorf("want 1 TableGroup with 1 shard, got %d TableGroups",
						len(tgs.Items))
				}
				cells := &multigresv1alpha1.CellList{}
				if err := c.List(ctx, cells, inCluster...); err != nil {
					return nil, err
				}
				if len(cells.Items) != 1 {
					return nil, fmt.Errorf("want 1 Cell, got %d", len(cells.Items))
				}
				shard := tgs.Items[0].Spec.Shards[0]
				return map[string]corev1.ResourceRequirements{
					"TableGroup multiorch": shard.Multiorch.Resources,
					"TableGroup postgres":  shard.Pools["default"].Postgres.Resources,
					"Cell multigateway":    cells.Items[0].Spec.Multigateway.Resources,
				}, nil
			},
		},
	}

	populatedJSON, err := runtime.DefaultUnstructuredConverter.ToUnstructured(
		new(populatedResources()),
	)
	require.NoError(t, err)

	for _, v := range variants {
		setTestPostgresPasswordSecretRef(v.cluster)
		require.NoError(t, k8sClient.Create(ctx, v.cluster))
	}
	for _, v := range variants {
		name := v.cluster.Name
		requireChildResources(t, k8sClient, name, v.children, true)
		for _, s := range emptyResourcesShapes {
			setClusterResources(t, k8sClient, name, v.paths, s.shape)
			t.Logf("%s, shape %s: waiting for children to drop their resources", name, s.name)
			requireChildResources(t, k8sClient, name, v.children, false)

			setClusterResources(t, k8sClient, name, v.paths, populatedJSON)
			t.Logf("%s, shape %s: waiting for children to carry resources again", name, s.name)
			requireChildResources(t, k8sClient, name, v.children, true)
		}
	}
}

func clusterSelector(clusterName string) []client.ListOption {
	return []client.ListOption{
		client.InNamespace(testNamespace),
		client.MatchingLabels{metadata.LabelMultigresCluster: clusterName},
	}
}

func appendPath(base []any, rest ...any) []any {
	return slices.Concat(base, rest)
}

// setClusterResources writes shape at each of paths through unstructured
// content, so a non-nil empty map reaches the API server as written.
func setClusterResources(
	t *testing.T,
	c client.Client,
	clusterName string,
	paths map[string][]any,
	shape map[string]any,
) {
	t.Helper()
	require.NoError(t, testutil.UpdateUnstructured(t.Context(), c,
		multigresv1alpha1.GroupVersion.WithKind("MultigresCluster"),
		client.ObjectKey{Namespace: testNamespace, Name: clusterName},
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

// requireChildResources waits until every block children reports either
// carries the populated entries (want true) or is empty (want false).
func requireChildResources(
	t *testing.T,
	c client.Client,
	clusterName string,
	children childResources,
	want bool,
) {
	t.Helper()
	var last map[string]corev1.ResourceRequirements
	var lastErr error
	if err := wait.PollUntilContextTimeout(t.Context(), pollInterval, 3*testTimeout, true,
		func(ctx context.Context) (bool, error) {
			last, lastErr = children(t.Context(), c, clusterName)
			if lastErr != nil {
				return false, nil
			}
			for _, r := range last {
				if want {
					if r.Requests.Cpu().String() != "123m" || r.Limits.Memory().String() != "321Mi" {
						return false, nil
					}
				} else if len(r.Requests) != 0 || len(r.Limits) != 0 || len(r.Claims) != 0 {
					return false, nil
				}
			}
			return true, nil
		}); err != nil {
		testutil.LogWarnings(t, c, testNamespace, clusterName)
		t.Fatalf("%s: children never converged (want populated=%v): %v, err %v (%v)",
			clusterName, want, last, lastErr, err)
	}
}
