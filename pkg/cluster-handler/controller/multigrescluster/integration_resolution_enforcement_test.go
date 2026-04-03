//go:build integration
// +build integration

package multigrescluster_test

import (
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/testutil"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestMultigresCluster_ResolutionLogic validates the "4-Level Override Chain"
// and complex list merging behaviors specified in the architecture design.
func TestMultigresCluster_ResolutionLogic(t *testing.T) {
	t.Parallel()

	t.Run("4-Level Override Precedence", func(t *testing.T) {
		t.Parallel()
		k8sClient, watcher := setupIntegration(t)

		// 1. Setup Templates
		// "Small" Template (Cluster Default) -> Replicas: 1
		smallTpl := &multigresv1alpha1.CellTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "small", Namespace: testNamespace},
			Spec: multigresv1alpha1.CellTemplateSpec{
				MultiGateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
		}
		// "Large" Template -> Replicas: 5
		largeTpl := &multigresv1alpha1.CellTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "large", Namespace: testNamespace},
			Spec: multigresv1alpha1.CellTemplateSpec{
				MultiGateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(5))},
			},
		}

		if err := k8sClient.Create(t.Context(), smallTpl); err != nil {
			t.Fatal(err)
		}
		if err := k8sClient.Create(t.Context(), largeTpl); err != nil {
			t.Fatal(err)
		}

		// 2. Create Cluster with various levels of overrides
		clusterName := "precedence-test"
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				// Level 2: Cluster Default is "small" (1 replica)
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{
					CellTemplate: "small",
				},
				Cells: []multigresv1alpha1.CellConfig{
					// Case A: Fallback to Cluster Default (Expect 1)
					{Name: "zone-a", Zone: "us-east-1a"},

					// Case B: Explicit Template Ref (Expect 5)
					{
						Name:         "zone-b",
						Zone:         "us-east-1b",
						CellTemplate: "large",
					},

					// Case C: Explicit Template Ref + Inline Override (Expect 3)
					{
						Name:         "zone-c",
						Zone:         "us-east-1c",
						CellTemplate: "large",
						Overrides: &multigresv1alpha1.CellOverrides{
							MultiGateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(3))},
						},
					},

					// Case D: Inline Spec (Highest Priority) (Expect 9)
					{
						Name: "zone-d",
						Zone: "us-east-1d",
						Spec: &multigresv1alpha1.CellInlineSpec{
							MultiGateway: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(9))},
						},
					},
				},
			},
		}

		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		// 3. Verify Results
		// Use CompareSpecOnly to avoid needing to construct OwnerRefs/UIDs manually
		watcher.SetCmpOpts(testutil.CompareSpecOnly()...)

		// Helper to build expectation
		makeExpected := func(zone string, replicas int32, allCells []string) *multigresv1alpha1.Cell {
			cellNames := make([]multigresv1alpha1.CellName, len(allCells))
			for i, c := range allCells {
				cellNames[i] = multigresv1alpha1.CellName(c)
			}
			return &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name: nameutil.JoinWithConstraints(
						nameutil.DefaultConstraints,
						clusterName,
						zone,
					),
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: multigresv1alpha1.CellName(zone),
					Zone: multigresv1alpha1.Zone("us-east-1" + zone[len(zone)-1:]), // construct zone from name (e.g. zone-a -> us-east-1a)
					Images: multigresv1alpha1.CellImages{
						MultiGateway:    resolver.DefaultMultiGatewayImage,
						ImagePullPolicy: resolver.DefaultImagePullPolicy,
					},
					MultiGateway: multigresv1alpha1.StatelessSpec{
						Replicas:  ptr.To(replicas),
						Resources: resolver.DefaultResourcesGateway(), // FIX: Expect defaults
					},
					AllCells: cellNames,
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        clusterName + "-global-topo." + testNamespace + ".svc:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					TopologyReconciliation: multigresv1alpha1.TopologyReconciliation{
						RegisterCell: true,
						PrunePoolers: true,
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			}
		}

		allCells := []string{"zone-a", "zone-b", "zone-c", "zone-d"}
		cases := []struct {
			zone         string
			wantReplicas int32
		}{
			{"zone-a", 1},
			{"zone-b", 5},
			{"zone-c", 3},
			{"zone-d", 9},
		}

		for _, tc := range cases {
			if err := watcher.WaitForMatch(makeExpected(tc.zone, tc.wantReplicas, allCells)); err != nil {
				t.Errorf("Precedence failed for %s: %v", tc.zone, err)
			}
		}
	})

	t.Run("Implicit Namespace Defaulting", func(t *testing.T) {
		t.Parallel()
		k8sClient, watcher := setupIntegration(t)
		clusterName := "implicit-default-test"

		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{}, // Empty, relying on implicit "default"
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "zone-a", Zone: "us-east-1a"},
				},
			},
		}

		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatal(err)
		}

		// We expect the "default" template (created in setupIntegration) to be used.
		// That template has Replicas: 1.
		watcher.SetCmpOpts(testutil.CompareSpecOnly()...)
		wantCell := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name: nameutil.JoinWithConstraints(
					nameutil.DefaultConstraints,
					clusterName,
					"zone-a",
				),
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.CellSpec{
				Name: "zone-a",
				Zone: "us-east-1a",
				Images: multigresv1alpha1.CellImages{
					MultiGateway:    resolver.DefaultMultiGatewayImage,
					ImagePullPolicy: resolver.DefaultImagePullPolicy,
				},
				MultiGateway: multigresv1alpha1.StatelessSpec{
					Replicas:  ptr.To(int32(1)),
					Resources: resolver.DefaultResourcesGateway(), // FIX: Expect defaults
				},
				AllCells: []multigresv1alpha1.CellName{"zone-a"},
				GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
					Address:        clusterName + "-global-topo." + testNamespace + ".svc:2379",
					RootPath:       "/multigres/global",
					Implementation: "etcd",
				},
				TopologyReconciliation: multigresv1alpha1.TopologyReconciliation{
					RegisterCell: true,
					PrunePoolers: true,
				},
				LogLevels: multigresv1alpha1.ComponentLogLevels{
					Pgctld:       "info",
					Multipooler:  "info",
					Multiorch:    "info",
					Multiadmin:   "info",
					Multigateway: "info",
				},
			},
		}

		if err := watcher.WaitForMatch(wantCell); err != nil {
			t.Error("Failed to implicitly resolve namespace 'default' template")
		}
	})

	t.Run("List Replacement Logic (Cells)", func(t *testing.T) {
		t.Parallel()
		k8sClient, watcher := setupIntegration(t)

		// Setup ShardTemplate
		tplName := "multi-cell-shard"
		tpl := &multigresv1alpha1.ShardTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: tplName, Namespace: testNamespace},
			Spec: multigresv1alpha1.ShardTemplateSpec{
				MultiOrch: &multigresv1alpha1.MultiOrchSpec{
					Cells: []multigresv1alpha1.CellName{"zone-a", "zone-b"},
				},
			},
		}
		if err := k8sClient.Create(t.Context(), tpl); err != nil {
			t.Fatal(err)
		}

		// Create cluster
		clusterName := "list-replace-test"
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "zone-a", Zone: "us-east-1a"},
					{Name: "zone-c", Zone: "us-east-1c"},
				},
				Databases: []multigresv1alpha1.DatabaseConfig{
					{
						Name: "postgres", Default: true,
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{
								Name: "default", Default: true,
								Shards: []multigresv1alpha1.ShardConfig{
									{
										Name:          "0-inf",
										ShardTemplate: multigresv1alpha1.TemplateRef(tplName),
										Overrides: &multigresv1alpha1.ShardOverrides{
											MultiOrch: &multigresv1alpha1.MultiOrchSpec{
												// Should REPLACE, not append
												Cells: []multigresv1alpha1.CellName{"zone-c"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatal(err)
		}

		// Verify by checking the TableGroup (since Shard controller isn't running)
		// We expect the resolved Spec in TableGroup to have the correct list.
		watcher.SetCmpOpts(testutil.CompareSpecOnly()...)

		wantTG := &multigresv1alpha1.TableGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: nameutil.JoinWithConstraints(
					nameutil.DefaultConstraints,
					clusterName,
					"postgres",
					"default",
				),
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.TableGroupSpec{
				CellTopologyLabels: map[multigresv1alpha1.CellName]map[string]string{
					"zone-a": {"topology.kubernetes.io/zone": "us-east-1a"},
					"zone-c": {"topology.kubernetes.io/zone": "us-east-1c"},
				},
				DatabaseName: "postgres",
				LogLevels: multigresv1alpha1.ComponentLogLevels{
					Pgctld:       "info",
					Multipooler:  "info",
					Multiorch:    "info",
					Multiadmin:   "info",
					Multigateway: "info",
				},
				TableGroupName: "default",
				IsDefault:      true,
				Images: multigresv1alpha1.ShardImages{
					MultiOrch:       resolver.DefaultMultiOrchImage,
					MultiPooler:     resolver.DefaultMultiPoolerImage,
					Postgres:        resolver.DefaultPostgresImage,
					ImagePullPolicy: resolver.DefaultImagePullPolicy,
				},
				GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
					Address:        clusterName + "-global-topo." + testNamespace + ".svc:2379",
					RootPath:       "/multigres/global",
					Implementation: "etcd",
				},
				Shards: []multigresv1alpha1.ShardResolvedSpec{
					{
						Name: "0-inf",
						MultiOrch: multigresv1alpha1.MultiOrchSpec{
							// VERIFICATION: Only zone-c should be present
							Cells: []multigresv1alpha1.CellName{"zone-c"},
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas:  ptr.To(int32(1)),                // From implicit defaults
								Resources: resolver.DefaultResourcesOrch(), // FIX: Expect defaults
							},
						},
						Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
							"default": {
								Type:            "readWrite",
								Cells:           []multigresv1alpha1.CellName{"zone-c"},
								ReplicasPerCell: ptr.To(int32(3)),
								Storage:         multigresv1alpha1.StorageSpec{Size: "1Gi"},
								Postgres: multigresv1alpha1.ContainerConfig{
									Resources: resolver.DefaultResourcesPostgres(),
								},
								Multipooler: multigresv1alpha1.ContainerConfig{
									Resources: resolver.DefaultResourcesPooler(),
								},
							},
						},
						PVCDeletionPolicy: nil, // Shard-level is nil
						Backup: &multigresv1alpha1.BackupConfig{
							Type:       multigresv1alpha1.BackupTypeFilesystem,
							Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
							Retention: &multigresv1alpha1.RetentionPolicy{
								FullCount:         ptr.To(int32(4)),
								DifferentialCount: ptr.To(int32(1)),
							},
						},
					},
				},
				PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
					WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
					WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
				},
				Backup: &multigresv1alpha1.BackupConfig{
					Type:       multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
				},
				TopologyPruning:  &multigresv1alpha1.TopologyPruningConfig{Enabled: ptr.To(true)},
				DurabilityPolicy: "AT_LEAST_2",
			},
		}

		if err := watcher.WaitForMatch(wantTG); err != nil {
			t.Errorf("List replacement failed: %v", err)
		}
	})
}

// TestMultigresCluster_EnforcementLogic verifies that the controller actively
// enforces the desired state, including reverting manual changes (immutability).
func TestMultigresCluster_EnforcementLogic(t *testing.T) {
	t.Parallel()
	k8sClient, watcher := setupIntegration(t)
	clusterName := "enforcement-test"

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Cells: []multigresv1alpha1.CellConfig{
				{
					Name: "zone-a", Zone: "us-east-1a",
					Spec: &multigresv1alpha1.CellInlineSpec{
						MultiGateway: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(2))},
					},
				},
			},
		},
	}

	if err := k8sClient.Create(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}

	watcher.SetCmpOpts(testutil.CompareSpecOnly()...)

	// 1. Expected Cell state
	wantCell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: nameutil.JoinWithConstraints(
				nameutil.DefaultConstraints,
				clusterName,
				"zone-a",
			),
			Namespace: testNamespace,
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone-a",
			Zone: "us-east-1a",
			Images: multigresv1alpha1.CellImages{
				MultiGateway:    resolver.DefaultMultiGatewayImage,
				ImagePullPolicy: resolver.DefaultImagePullPolicy,
			},
			MultiGateway: multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(int32(2)),
				Resources: resolver.DefaultResourcesGateway(), // FIX: Expect defaults
			},
			AllCells: []multigresv1alpha1.CellName{"zone-a"},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        clusterName + "-global-topo." + testNamespace + ".svc:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			TopologyReconciliation: multigresv1alpha1.TopologyReconciliation{
				RegisterCell: true,
				PrunePoolers: true,
			},
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},
		},
	}

	if err := watcher.WaitForMatch(wantCell); err != nil {
		t.Fatal("Initial cell creation failed")
	}

	// 2. Tamper (Scale up manually)
	cellKey := client.ObjectKey{Name: wantCell.Name, Namespace: wantCell.Namespace}
	cell := &multigresv1alpha1.Cell{}
	if err := k8sClient.Get(t.Context(), cellKey, cell); err != nil {
		t.Fatal(err)
	}
	cell.Spec.MultiGateway.Replicas = ptr.To(int32(100))
	if err := k8sClient.Update(t.Context(), cell); err != nil {
		t.Fatal("Failed to tamper with cell")
	}

	// 3. Verify Reversion
	// We wait for the object to match 'wantCell' again.
	// Since client.Update succeeded, the object *was* changed. The fact that it matches
	// wantCell (2 replicas) afterwards proves the controller reverted it.
	if err := watcher.WaitForMatch(wantCell); err != nil {
		t.Errorf("Controller failed to revert manual change: %v", err)
	}
}

// TestMultigresCluster_V1Alpha1Constraints verifies strict v1alpha1 validations
func TestMultigresCluster_V1Alpha1Constraints(t *testing.T) {
	t.Parallel()
	k8sClient, _ := setupIntegration(t)

	cases := []struct {
		name        string
		clusterSpec multigresv1alpha1.MultigresClusterSpec
		errContains string
	}{
		{
			name: "Wrong Database Name",
			clusterSpec: multigresv1alpha1.MultigresClusterSpec{
				Databases: []multigresv1alpha1.DatabaseConfig{
					{Name: "my-db", Default: true}, // Should be "postgres"
				},
			},
			errContains: "postgres",
		},
		{
			name: "Wrong TableGroup Name",
			clusterSpec: multigresv1alpha1.MultigresClusterSpec{
				Databases: []multigresv1alpha1.DatabaseConfig{
					{
						Name: "postgres", Default: true,
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{Name: "analytics", Default: true}, // Should be "default"
						},
					},
				},
			},
			errContains: "default",
		},
		{
			name: "Default False for System DB",
			clusterSpec: multigresv1alpha1.MultigresClusterSpec{
				Databases: []multigresv1alpha1.DatabaseConfig{
					{Name: "postgres", Default: false}, // Should be true
				},
			},
			errContains: "default",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: strings.ToLower(strings.ReplaceAll(tc.name, " ", "-")), Namespace: testNamespace},
				Spec:       tc.clusterSpec,
			}
			err := k8sClient.Create(t.Context(), cluster)
			if err == nil {
				t.Error("Expected validation error, got nil")
			} else if !strings.Contains(strings.ToLower(err.Error()), tc.errContains) {
				t.Errorf("Expected error to contain %q, got: %v", tc.errContains, err)
			}
		})
	}
}

// TestMultigresCluster_TemplateOverrides verifies that properties defined in
// ShardTemplates (specifically PVCDeletionPolicy) are correctly applied.
func TestMultigresCluster_TemplateOverrides(t *testing.T) {
	t.Parallel()
	k8sClient, watcher := setupIntegration(t)

	// 1. Create a ShardTemplate with specific PVC Policy
	tplName := "policy-template"
	tpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: tplName, Namespace: testNamespace},
		Spec: multigresv1alpha1.ShardTemplateSpec{
			PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
		},
	}
	if err := k8sClient.Create(t.Context(), tpl); err != nil {
		t.Fatal(err)
	}

	// 2. Create Cluster using this template in Defaults
	clusterName := "template-policy-test"
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				ShardTemplate: multigresv1alpha1.TemplateRef(tplName),
			},
			Cells: []multigresv1alpha1.CellConfig{
				{Name: "zone-a", Zone: "us-east-1a"},
			},
			Databases: []multigresv1alpha1.DatabaseConfig{
				{
					Name: "postgres", Default: true,
					TableGroups: []multigresv1alpha1.TableGroupConfig{
						{
							Name: "default", Default: true,
							Shards: []multigresv1alpha1.ShardConfig{
								{Name: "0-inf"}, // No inline spec, should use template
							},
						},
					},
				},
			},
		},
	}

	if err := k8sClient.Create(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}

	// 3. Verify TableGroup has the correct Resolved Shard Spec
	watcher.SetCmpOpts(testutil.CompareSpecOnly()...)

	wantTG := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: nameutil.JoinWithConstraints(
				nameutil.DefaultConstraints,
				clusterName,
				"postgres",
				"default",
			),
			Namespace: testNamespace,
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			CellTopologyLabels: map[multigresv1alpha1.CellName]map[string]string{
				"zone-a": {"topology.kubernetes.io/zone": "us-east-1a"},
			},
			DatabaseName: "postgres",
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},
			TableGroupName: "default",
			IsDefault:      true,
			Images: multigresv1alpha1.ShardImages{
				MultiOrch:       resolver.DefaultMultiOrchImage,
				MultiPooler:     resolver.DefaultMultiPoolerImage,
				Postgres:        resolver.DefaultPostgresImage,
				ImagePullPolicy: resolver.DefaultImagePullPolicy,
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        clusterName + "-global-topo." + testNamespace + ".svc:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			Shards: []multigresv1alpha1.ShardResolvedSpec{
				{
					Name: "0-inf",
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a"},
						StatelessSpec: multigresv1alpha1.StatelessSpec{
							Replicas:  ptr.To(int32(1)),                // From implicit defaults in resolver
							Resources: resolver.DefaultResourcesOrch(), // Defaults
						},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"default": {
							Type:            "readWrite",
							Cells:           []multigresv1alpha1.CellName{"zone-a"},
							ReplicasPerCell: ptr.To(int32(3)),
							Storage:         multigresv1alpha1.StorageSpec{Size: "1Gi"},
							Postgres: multigresv1alpha1.ContainerConfig{
								Resources: resolver.DefaultResourcesPostgres(),
							},
							Multipooler: multigresv1alpha1.ContainerConfig{
								Resources: resolver.DefaultResourcesPooler(),
							},
						},
					},
					// FIX Verification: verify that the POLICY FROM TEMPLATE IS HERE!
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type:       multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
						Retention: &multigresv1alpha1.RetentionPolicy{
							FullCount:         ptr.To(int32(4)),
							DifferentialCount: ptr.To(int32(1)),
						},
					},
				},
			},
			// TG/Cluster level defaults (Retain/Retain)
			PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			Backup: &multigresv1alpha1.BackupConfig{
				Type:       multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
			},
			TopologyPruning:  &multigresv1alpha1.TopologyPruningConfig{Enabled: ptr.To(true)},
			DurabilityPolicy: "AT_LEAST_2",
		},
	}

	if err := watcher.WaitForMatch(wantTG); err != nil {
		t.Errorf("Template override validation failed: %v", err)
	}
}
