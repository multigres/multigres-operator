//go:build integration

package multigrescluster_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestEmptyPointerBlocksReachChildren sets the affinity, placement, etcd
// maintenance, observability, internal TLS, topology pruning and backup
// blocks of a MultigresCluster to entries, waits for the TableGroup, Cell and
// global TopoServer to carry them, then edits each to a value that asks for
// nothing ({}, or a storage block holding only an empty access mode list) and
// back. An emptied block must leave the child without it: an apply that sent
// {} over a block that had entries is rejected by the API server, and the
// cluster controller would fail every pass with the children stuck on the old
// values. A filesystem backup keeps its block, which the CRD requires, with
// the default path.
func TestEmptyPointerBlocksReachChildren(t *testing.T) {
	t.Parallel()

	k8sClient, _ := setupIntegration(t)
	ctx := t.Context()
	const name = "empty-blocks"

	affinity := map[string]any{"nodeAffinity": map[string]any{
		"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{
			"nodeSelectorTerms": []any{map[string]any{"matchExpressions": []any{
				map[string]any{"key": "example.com/pool", "operator": "Exists"},
			}}},
		},
	}}
	toleration := map[string]any{"tolerations": []any{
		map[string]any{"key": "example.com/dedicated", "operator": "Exists"},
	}}
	topoPlacement := map[string]any{"nodeSelector": map[string]any{"example.com/topo": "true"}}
	maintenance := map[string]any{"autoCompactionMode": "revision"}

	overrides := []any{"spec", "databases", 0, "tablegroups", 0, "shards", 0, "overrides"}
	blocks := []struct {
		path      []any
		populated map[string]any
		// empty is what the block is edited to; nil means {}.
		empty map[string]any
	}{
		{appendPath(overrides, "multiorch", "affinity"), affinity, nil},
		{appendPath(overrides, "multiorch", "placement"), toleration, nil},
		{appendPath(overrides, "pools", "default", "affinity"), affinity, nil},
		{[]any{"spec", "cells", 0, "spec", "multigateway", "affinity"}, affinity, nil},
		{[]any{"spec", "cells", 0, "spec", "multigatewayPlacement"}, toleration, nil},
		{[]any{"spec", "globalTopoServer", "placement"}, topoPlacement, nil},
		{[]any{"spec", "globalTopoServer", "etcd", "maintenance"}, maintenance, nil},
		{[]any{"spec", "observability"}, map[string]any{"otlpEndpoint": "http://otel:4318"}, nil},
		{[]any{"spec", "internalTLS"}, map[string]any{"enabled": false}, nil},
		{[]any{"spec", "topologyPruning"}, map[string]any{"enabled": false}, nil},
		// A storage block that asks for nothing can also hold an empty
		// access mode list, which encodes as {} just the same.
		{[]any{"spec", "backup", "filesystem"}, map[string]any{
			"path":    "/data/backups",
			"storage": map[string]any{"size": "5Gi", "accessModes": []any{"ReadWriteOnce"}},
		}, map[string]any{"storage": map[string]any{"accessModes": []any{}}}},
		{
			[]any{"spec", "backup", "pgbackrestTLS"},
			//nolint:gosec // the name of a Secret, not a credential
			map[string]any{"secretName": "pgbackrest-tls"},
			nil,
		},
	}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default",
				CellTemplate:  "default",
				ShardTemplate: "default",
			},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:latest"},
			},
			Backup: &multigresv1alpha1.BackupConfig{
				Type:       multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/data/backups"},
			},
			Cells: []multigresv1alpha1.CellConfig{{
				Name:   "zone-a",
				ZoneID: "use1-az1",
				Spec:   &multigresv1alpha1.CellInlineSpec{},
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
							Multiorch: &multigresv1alpha1.MultiorchSpec{},
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"default": {
									Type:            "readWrite",
									ReplicasPerCell: new(int32(1)),
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

	set := func(empty bool) {
		t.Helper()
		require.NoError(t, testutil.UpdateUnstructured(ctx, k8sClient,
			multigresv1alpha1.GroupVersion.WithKind("MultigresCluster"),
			client.ObjectKeyFromObject(cluster),
			func(content map[string]any) error {
				for _, b := range blocks {
					value := map[string]any{}
					if empty && b.empty != nil {
						value = (&unstructured.Unstructured{Object: b.empty}).DeepCopy().Object
					}
					if !empty {
						value = (&unstructured.Unstructured{Object: b.populated}).DeepCopy().Object
					}
					if err := testutil.SetUnstructuredPath(content, b.path, value); err != nil {
						return fmt.Errorf("%v: %w", b.path, err)
					}
				}
				return nil
			}))
	}
	// children reports, per child block, whether it is present.
	children := func(ctx context.Context) (map[string]bool, error) {
		inCluster := clusterSelector(name)
		tgs := &multigresv1alpha1.TableGroupList{}
		if err := k8sClient.List(ctx, tgs, inCluster...); err != nil {
			return nil, err
		}
		if len(tgs.Items) != 1 || len(tgs.Items[0].Spec.Shards) != 1 {
			return nil, fmt.Errorf("want 1 TableGroup with 1 shard, got %d", len(tgs.Items))
		}
		cells := &multigresv1alpha1.CellList{}
		if err := k8sClient.List(ctx, cells, inCluster...); err != nil {
			return nil, err
		}
		topos := &multigresv1alpha1.TopoServerList{}
		if err := k8sClient.List(ctx, topos, inCluster...); err != nil {
			return nil, err
		}
		if len(cells.Items) != 1 || len(topos.Items) != 1 || topos.Items[0].Spec.Etcd == nil {
			return nil, fmt.Errorf("want 1 Cell and 1 TopoServer, got %d and %d",
				len(cells.Items), len(topos.Items))
		}
		shard := tgs.Items[0].Spec.Shards[0]
		cell := cells.Items[0].Spec
		topo := topos.Items[0].Spec
		backup := tgs.Items[0].Spec.Backup
		// The CRD requires a filesystem block while the type is filesystem,
		// so an emptied one reaches the TableGroup with the default path.
		if backup == nil || backup.Filesystem == nil {
			return nil, fmt.Errorf("TableGroup backup has no filesystem block: %+v", backup)
		}
		filesystem := backup.Filesystem.Path == "/data/backups" &&
			backup.Filesystem.Storage.Size == "5Gi"
		if !filesystem && (backup.Filesystem.Path != "/backups" ||
			!reflect.ValueOf(backup.Filesystem.Storage).IsZero()) {
			return nil, fmt.Errorf("TableGroup backup filesystem %+v", backup.Filesystem)
		}
		return map[string]bool{
			"TableGroup observability":        tgs.Items[0].Spec.Observability != nil,
			"TableGroup internalTLS":          tgs.Items[0].Spec.InternalTLS != nil,
			"TableGroup topologyPruning":      tgs.Items[0].Spec.TopologyPruning != nil,
			"TableGroup backup filesystem":    filesystem,
			"TableGroup backup pgbackrestTLS": backup.PgBackRestTLS != nil,
			"Cell observability":              cell.Observability != nil,
			"Cell internalTLS":                cell.InternalTLS != nil,
			"TableGroup multiorch affinity":   shard.Multiorch.Affinity != nil,
			"TableGroup multiorch placement":  shard.Multiorch.Placement != nil,
			"TableGroup pool affinity":        shard.Pools["default"].Affinity != nil,
			"Cell multigateway affinity":      cell.Multigateway.Affinity != nil,
			"Cell multigateway placement":     cell.MultigatewayPlacement != nil,
			"TopoServer placement":            topo.Placement != nil,
			"TopoServer etcd maintenance":     topo.Etcd.Maintenance != nil,
		}, nil
	}
	requireChildren := func(present bool) {
		t.Helper()
		var last map[string]bool
		var lastErr error
		if err := wait.PollUntilContextTimeout(t.Context(), pollInterval, 3*testTimeout, true,
			func(ctx context.Context) (bool, error) {
				got, err := children(ctx)
				last, lastErr = got, err
				if err != nil {
					return false, nil
				}
				for _, p := range got {
					if p != present {
						return false, nil
					}
				}
				return true, nil
			}); err != nil {
			testutil.LogWarnings(t, k8sClient, testNamespace, name)
			t.Fatalf(
				"children never converged (want present=%v): %v, err %v",
				present,
				last,
				lastErr,
			)
		}
	}

	set(false)
	requireChildren(true)
	set(true)
	requireChildren(false)
	set(false)
	requireChildren(true)
}
