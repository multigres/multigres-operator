package resolver

import (
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestResolver_ValidateClusterIntegrity verifies checking template existence.
func TestResolver_ValidateClusterIntegrity(t *testing.T) {
	t.Parallel()
	scheme := setupScheme()

	// Setup Base Objects
	coreTpl := &multigresv1alpha1.CoreTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-core", Namespace: "default"},
	}
	cellTpl := &multigresv1alpha1.CellTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-cell", Namespace: "default"},
	}
	shardTpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-shard", Namespace: "default"},
	}

	objs := []client.Object{coreTpl, cellTpl, shardTpl}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	tests := map[string]struct {
		cluster *multigresv1alpha1.MultigresCluster
		wantErr string
	}{
		"Valid": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "prod-core",
						CellTemplate:  "prod-cell",
						ShardTemplate: "prod-shard",
					},
					Multiadmin: &multigresv1alpha1.MultiadminConfig{TemplateRef: "prod-core"},
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "prod-core",
					},
				},
			},
		},
		"Missing Core": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{CoreTemplate: "missing"},
				},
			},
			wantErr: "referenced CoreTemplate 'missing' not found",
		},
		"Missing Cell (Inline)": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "c1", CellTemplate: "missing"}},
				},
			},
			wantErr: "referenced CellTemplate 'missing' not found",
		},
		"Missing MultiadminWeb": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{TemplateRef: "missing"},
				},
			},
			wantErr: "referenced CoreTemplate 'missing' not found",
		},
		"Missing Multiadmin": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{TemplateRef: "missing"},
				},
			},
			wantErr: "referenced CoreTemplate 'missing' not found",
		},
		"Missing GlobalTopoServer": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "missing",
					},
				},
			},
			wantErr: "referenced CoreTemplate 'missing' not found",
		},
		"Missing CellTemplate Default": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{CellTemplate: "missing"},
				},
			},
			wantErr: "referenced CellTemplate 'missing' not found",
		},
		"Missing ShardTemplate Default": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{ShardTemplate: "missing"},
				},
			},
			wantErr: "referenced ShardTemplate 'missing' not found",
		},
		"Missing ShardTemplate Inline": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "missing",
							}},
						}},
					}},
				},
			},
			wantErr: "referenced ShardTemplate 'missing' not found",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := NewResolver(fakeClient, "default")
			err := r.ValidateClusterIntegrity(t.Context(), tc.cluster)

			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("Expected nil error, got %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("Expected error containing '%s', got %v", tc.wantErr, err)
				}
			}
		})
	}
}

// TestResolver_ValidateClusterLogic verifies complex logical validations.
func TestResolver_ValidateClusterLogic(t *testing.T) {
	t.Parallel()
	scheme := setupScheme()

	// Setup Base Objects
	shardTpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-shard", Namespace: "default"},
		Spec: multigresv1alpha1.ShardTemplateSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {Type: "readWrite"},
			},
		},
	}

	badPoolTpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-pool-tpl", Namespace: "default"},
		Spec: multigresv1alpha1.ShardTemplateSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"BAD_POOL": {Type: "readWrite"},
			},
		},
	}

	postgresUIDTpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-uid-tpl", Namespace: "default"},
		Spec: multigresv1alpha1.ShardTemplateSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {
					Type: "readWrite",
					Postgres: multigresv1alpha1.ContainerConfig{
						RunAsUser: ptr.To(int64(1000)),
					},
				},
			},
		},
	}

	defaultSC := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "standard",
			Annotations: map[string]string{"storageclass.kubernetes.io/is-default-class": "true"},
		},
		Provisioner: "k8s.io/fake",
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardTpl, badPoolTpl, postgresUIDTpl, defaultSC).
		Build()

	noSCClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardTpl, badPoolTpl, postgresUIDTpl).
		Build()

	nodeRegionEast := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-region-east",
			Labels: map[string]string{"topology.kubernetes.io/region": "us-east-1"},
		},
	}
	nodeZoneID := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-zone-id",
			Labels: map[string]string{"topology.k8s.aws/zone-id": "use1-az1"},
		},
	}
	withRegionClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardTpl, badPoolTpl, postgresUIDTpl, defaultSC, nodeRegionEast).
		Build()
	withZoneIDClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardTpl, badPoolTpl, postgresUIDTpl, defaultSC, nodeZoneID).
		Build()
	noMatchingNodeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardTpl, badPoolTpl, postgresUIDTpl, defaultSC).
		Build()

	tests := map[string]struct {
		cluster        *multigresv1alpha1.MultigresCluster
		wantWarnings   []string
		wantNoWarnings bool
		wantErr        string
		customClient   client.Client
		clientFailures *testutil.FailureConfig
	}{
		"Valid Logic": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1", CellTemplate: "prod-cell"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
		},
		"Orphan Override Warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1", CellTemplate: "prod-cell"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"ghost": {Type: "read"},
									},
								},
							}},
						}},
					}},
				},
			},
			wantWarnings: []string{"does not exist in template 'prod-shard'"},
		},
		"Invalid Cell Reference (Orch)": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1", CellTemplate: "prod-cell"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Multiorch: &multigresv1alpha1.MultiorchSpec{
										Cells: []multigresv1alpha1.CellName{"zone-missing"},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "assigned to non-existent cell 'zone-missing'",
		},
		"Invalid Cell Reference (Pool)": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1", CellTemplate: "prod-cell"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Cells: []multigresv1alpha1.CellName{"zone-missing"},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "pool 'default' in shard 's0' is assigned to non-existent cell 'zone-missing'",
		},
		"Empty Cells (Orphan Shard)": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{}, // No cells
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantErr: "matches NO cells",
		},
		"Empty Cells (Pool)": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{}, // No cluster cells prevents defaulting
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Multiorch: multigresv1alpha1.MultiorchSpec{
										Cells: []multigresv1alpha1.CellName{
											"ghost",
										}, // Bypass Check 1
									},
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Cells: []multigresv1alpha1.CellName{}, // Explicit empty
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "pool 'default' in shard 's0' matches NO cells",
		},
		"Resolution Error": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "missing-template",
							}},
						}},
					}},
				},
			},
			wantErr: "cannot resolve shard 's0'",
		},
		"Orphan Check Resolution Error": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"ghost": {},
									},
								},
							}},
						}},
					}},
				},
			},
			clientFailures: &testutil.FailureConfig{
				OnGet: func(key client.ObjectKey) error {
					return testutil.ErrInjected
				},
			},
			wantErr: "failed to resolve template for orphan check",
		},
		"No default SC and no explicit class rejects": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "no-sc", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			// Use a client with no StorageClasses at all
			customClient: noSCClient,
			wantErr:      "no default StorageClass found",
		},
		"No default SC but explicit class set passes": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "explicit-sc", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{
							Storage: multigresv1alpha1.StorageSpec{Class: "my-sc"},
						},
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3,
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Type: "readWrite",
											Storage: multigresv1alpha1.StorageSpec{
												Class: "my-sc",
												Size:  "10Gi",
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			customClient: noSCClient,
		},
		"Default SC exists with no explicit class passes": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "default-sc", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			// Uses fakeClient which HAS a default StorageClass
		},
		// COVERAGE: validateCellTopology zoneId/region branches
		"Cell ZoneID matches node via withZoneIDClient - no topology warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a", ZoneID: "use1-az1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			customClient: withZoneIDClient,
		},
		"Cell ZoneID no matching node - topology warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-b", ZoneID: "usw2-az1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			customClient: noMatchingNodeClient,
			wantWarnings: []string{
				"no nodes currently match topology.k8s.aws/zone-id=usw2-az1",
			},
		},
		"Cell Region matches node - no topology warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "region-east", Region: "us-east-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			customClient: withRegionClient,
		},
		"Cell Region no matching node - topology warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "region-eu", Region: "eu-west-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			customClient: noMatchingNodeClient,
			wantWarnings: []string{
				"no nodes currently match topology.kubernetes.io/region=eu-west-1",
			},
		},
		"Cell ZoneID matches node - no topology warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "az-a", ZoneID: "use1-az1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{
								Shards: []multigresv1alpha1.ShardConfig{
									{Name: "s0", ShardTemplate: "prod-shard"},
								},
							},
						},
					}},
				},
			},
			customClient: withZoneIDClient,
		},
		// COVERAGE: durability warning for readWrite pools below AT_LEAST_2 achievability
		"Durability Warning: AT_LEAST_2 pool with 1 total pooler": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Storage: multigresv1alpha1.StorageSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteMany,
								},
							},
						},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								// Force a single pooler to exercise the
								// durability warning (default is now 2).
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {ReplicasPerCell: ptr.To(int32(1))},
									},
								},
							}},
						}},
					}},
				},
			},
			wantWarnings: []string{
				"replicasPerCell=1 across 1 cell(s) (1 total)",
				"durability policy \"AT_LEAST_2\" requires at least 2 total poolers",
			},
		},
		"No Durability Warning: AT_LEAST_2 pool with 2 replicas in one cell": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Storage: multigresv1alpha1.StorageSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteMany,
								},
							},
						},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											ReplicasPerCell: ptr.To(int32(2)),
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantNoWarnings: true,
		},
		"No Durability Warning: AT_LEAST_2 pool with 2 cells and 1 replica per cell": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
						{Name: "zone-2"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantNoWarnings: true,
		},
		"No Quorum Warning: three cells with 1 replica per cell": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
						{Name: "zone-2"},
						{Name: "zone-3"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											ReplicasPerCell: ptr.To(int32(1)),
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantNoWarnings: true,
		},
		"No Quorum Warning: three cells with omitted replicas (1 per cell, 3 total)": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
						{Name: "zone-2"},
						{Name: "zone-3"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantNoWarnings: true,
		},
		"No Quorum Warning: readWrite pool with 3 replicas": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											ReplicasPerCell: ptr.To(int32(3)),
										},
									},
								},
							}},
						}},
					}},
				},
			},
		},
		"Durability Warning: inline AT_LEAST_2 pool with 1 total pooler": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"main-rw": {
											Type:            "readWrite",
											ReplicasPerCell: ptr.To(int32(1)),
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantWarnings: []string{"has replicasPerCell=1 across 1 cell(s) (1 total)"},
		},
		"Durability Warning: MULTI_CELL_AT_LEAST_2 pool in one cell": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					DurabilityPolicy: "MULTI_CELL_AT_LEAST_2",
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											ReplicasPerCell: ptr.To(int32(3)),
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantWarnings: []string{
				"spans 1 cell(s)",
				"durability policy \"MULTI_CELL_AT_LEAST_2\" requires at least 2 distinct cells",
			},
		},
		"No Durability Warning: MULTI_CELL_AT_LEAST_2 pool in two cells": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					DurabilityPolicy: "MULTI_CELL_AT_LEAST_2",
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
						{Name: "zone-2"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantNoWarnings: true,
		},
		"No Durability Warning: MULTI_CELL_AT_LEAST_2 pool in three cells": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					DurabilityPolicy: "MULTI_CELL_AT_LEAST_2",
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
						{Name: "zone-2"},
						{Name: "zone-3"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantNoWarnings: true,
		},
		"Etcd 1 replica warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(1))},
					},
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantWarnings: []string{"etcd has 1 replica \u2014 no fault tolerance"},
		},
		"Resource error: etcd limits < requests": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("2")},
								Limits:   corev1.ResourceList{corev1.ResourceCPU: parseQty("1")},
							},
						},
					},
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantErr: "etcd: resource cpu limit (1) must be >= request (2)",
		},
		"Resource error: multiadmin limits < requests": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						Spec: &multigresv1alpha1.StatelessSpec{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("2")},
								Limits:   corev1.ResourceList{corev1.ResourceCPU: parseQty("1")},
							},
						},
					},
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantErr: "multiadmin: resource cpu limit (1) must be >= request (2)",
		},
		"Resource error: multiadmin-web limits < requests": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{
						Spec: &multigresv1alpha1.StatelessSpec{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("2")},
								Limits:   corev1.ResourceList{corev1.ResourceCPU: parseQty("1")},
							},
						},
					},
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantErr: "multiadmin-web: resource cpu limit (1) must be >= request (2)",
		},
		"Resource error: cell multigateway limits < requests": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{
							Name: "zone-1",
							Spec: &multigresv1alpha1.CellInlineSpec{
								Multigateway: multigresv1alpha1.StatelessSpec{
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: parseQty("2"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU: parseQty("1"),
										},
									},
								},
							},
						},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantErr: "cell 'zone-1' multigateway: resource cpu limit (1) must be >= request (2)",
		},
		"Resource error: shard multiorch limits < requests": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Multiorch: multigresv1alpha1.MultiorchSpec{
										StatelessSpec: multigresv1alpha1.StatelessSpec{
											Resources: corev1.ResourceRequirements{
												Requests: corev1.ResourceList{
													corev1.ResourceCPU: parseQty("2"),
												},
												Limits: corev1.ResourceList{
													corev1.ResourceCPU: parseQty("1"),
												},
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "shard 's0' multiorch: resource cpu limit (1) must be >= request (2)",
		},
		"Resource error: shard pool postgres limits < requests": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Postgres: multigresv1alpha1.ContainerConfig{
												Resources: corev1.ResourceRequirements{
													Requests: corev1.ResourceList{
														corev1.ResourceCPU: parseQty("2"),
													},
													Limits: corev1.ResourceList{
														corev1.ResourceCPU: parseQty("1"),
													},
												},
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "shard 's0' pool 'default' postgres: resource cpu limit (1) must be >= request (2)",
		},
		"Resource error: shard pool multipooler limits < requests": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Multipooler: multigresv1alpha1.ContainerConfig{
												Resources: corev1.ResourceRequirements{
													Requests: corev1.ResourceList{
														corev1.ResourceCPU: parseQty("2"),
													},
													Limits: corev1.ResourceList{
														corev1.ResourceCPU: parseQty("1"),
													},
												},
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "shard 's0' pool 'default' multipooler: resource cpu limit (1) must be >= request (2)",
		},
		"Runtime identity: partial override resolves to matching PGDATA UID": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "postgres-uid-tpl",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Multipooler: multigresv1alpha1.ContainerConfig{
												RunAsUser: ptr.To(int64(1000)),
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
		},
		"Runtime identity error: multipooler UID without resolved postgres UID": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Multipooler: multigresv1alpha1.ContainerConfig{
												RunAsUser: ptr.To(int64(1000)),
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "shard 's0' pool 'default': multipooler runAsUser 1000 requires matching postgres runAsUser",
		},
		"Runtime identity error: postgres and multipooler UIDs differ after resolution": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "postgres-uid-tpl",
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											Multipooler: multigresv1alpha1.ContainerConfig{
												RunAsUser: ptr.To(int64(2000)),
											},
										},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "shard 's0' pool 'default': postgres runAsUser 1000 and multipooler runAsUser 2000 must match",
		},
		"External Topo Server Sets Effective Replicas To 0": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						External: &multigresv1alpha1.ExternalTopoServerSpec{
							CASecret: "my-secret",
						},
					},
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0", ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
		},
		"SC check error on 2nd shard resolution": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "err-2nd", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			clientFailures: func() *testutil.FailureConfig {
				var calls int
				return &testutil.FailureConfig{
					OnGet: func(key client.ObjectKey) error {
						calls++
						if calls == 2 { // first shard template resolve succeeds, second fails
							return testutil.ErrInjected
						}
						return nil
					},
				}
			}(),
		},
		// COVERAGE: filesystem backup AccessModes branch (isRWO=false path)
		"Filesystem RWX backup suppresses replica warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Storage: multigresv1alpha1.StorageSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteMany,
								},
							},
						},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			// wantWarnings is nil: ReadWriteMany sets isRWO=false, suppressing the replica warning
		},
		"SC list error propagated": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "list-err", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			clientFailures: &testutil.FailureConfig{
				OnList: func(_ client.ObjectList) error {
					return testutil.ErrInjected
				},
			},
			wantErr: "failed to check for default StorageClass",
		},
		"Two cells same zoneId - topology dedup": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "cell-a", ZoneID: "use1-az1"},
						{Name: "cell-b", ZoneID: "use1-az1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			customClient: noMatchingNodeClient,
			wantWarnings: []string{
				"cell 'cell-a': no nodes currently match topology.k8s.aws/zone-id=use1-az1",
				"cell 'cell-b': no nodes currently match topology.k8s.aws/zone-id=use1-az1",
			},
		},
		"Invalid Pool Name (uppercase and special chars)": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"INVALID_NAME!": {Type: "readWrite"},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "pool name 'INVALID_NAME!' is invalid",
		},
		"Invalid Pool Name (too long)": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"this-pool-name-exceeds-max": {Type: "readWrite"},
									},
								},
							}},
						}},
					}},
				},
			},
			wantErr: "pool name 'this-pool-name-exceeds-max' must be between 1 and 25 characters",
		},
		"Invalid Pool Name from template": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "bad-pool-tpl",
							}},
						}},
					}},
				},
			},
			wantErr: "pool name 'BAD_POOL' is invalid",
		},
		"TemplateDefaults.ShardTemplate propagates to validation": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						ShardTemplate: "prod-shard",
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "s0",
								// No ShardTemplate set — relies on TemplateDefaults
								Overrides: &multigresv1alpha1.ShardOverrides{
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"default": {
											ReplicasPerCell: ptr.To(int32(4)),
										},
									},
								},
							}},
						}},
					}},
				},
			},
			// No warnings expected: "default" pool exists in prod-shard template,
			// and replicasPerCell=4 is above quorum threshold.
		},
		"Node list error skips topology validation silently": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "valid", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a", ZoneID: "use1-az1"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			clientFailures: &testutil.FailureConfig{
				OnList: func(list client.ObjectList) error {
					if _, ok := list.(*corev1.NodeList); ok {
						return testutil.ErrInjected
					}
					return nil
				},
			},
		},
		"External Gateway Enabled Without ExternalIPs Warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "gw-no-ips", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1", CellTemplate: "prod-cell"},
					},
					ExternalGateway: &multigresv1alpha1.ExternalGatewayConfig{
						Enabled: true,
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantWarnings: []string{"externalGateway is enabled but no externalIPs specified"},
		},
		"External Gateway Invalid IP Rejected": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "gw-bad-ip", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1", CellTemplate: "prod-cell"},
					},
					ExternalGateway: &multigresv1alpha1.ExternalGatewayConfig{
						Enabled:     true,
						ExternalIPs: []multigresv1alpha1.IPAddress{"999.999.999.999"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantErr: "invalid IP address",
		},
		"External Admin Web Enabled Without ExternalIPs Warning": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "aw-no-ips", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1", CellTemplate: "prod-cell"},
					},
					ExternalAdminWeb: &multigresv1alpha1.ExternalAdminWebConfig{
						Enabled: true,
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantWarnings: []string{"externalAdminWeb is enabled but no externalIPs specified"},
		},
		"External Admin Web Invalid IP Rejected": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "aw-bad-ip", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-1", CellTemplate: "prod-cell"},
					},
					ExternalAdminWeb: &multigresv1alpha1.ExternalAdminWebConfig{
						Enabled:     true,
						ExternalIPs: []multigresv1alpha1.IPAddress{"999.999.999.999"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{{
						TableGroups: []multigresv1alpha1.TableGroupConfig{{
							Shards: []multigresv1alpha1.ShardConfig{{
								Name:          "s0",
								ShardTemplate: "prod-shard",
							}},
						}},
					}},
				},
			},
			wantErr: "invalid IP address",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var c client.Client = fakeClient
			if tc.customClient != nil {
				c = tc.customClient
			}
			if tc.clientFailures != nil {
				c = testutil.NewFakeClientWithFailures(c, tc.clientFailures)
			}
			r := NewResolver(c, "default")
			warnings, err := r.ValidateClusterLogic(t.Context(), tc.cluster)

			// Check Error
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("Expected nil error, got %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("Expected error containing '%s', got %v", tc.wantErr, err)
				}
			}

			// Check Warnings
			if len(tc.wantWarnings) > 0 {
				if len(warnings) == 0 {
					t.Errorf("Expected warnings containing %v, got none", tc.wantWarnings)
				}
				for _, want := range tc.wantWarnings {
					found := false
					for _, got := range warnings {
						if strings.Contains(got, want) {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Expected warning containing '%s', got %v", want, warnings)
					}
				}
			}
			if tc.wantNoWarnings && len(warnings) > 0 {
				t.Errorf("Expected no warnings, got %v", warnings)
			}
		})
	}
}
