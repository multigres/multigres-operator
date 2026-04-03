package handlers

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMultigresClusterDefaulter_Handle(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	baseObjs := []client.Object{
		&multigresv1alpha1.ShardTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "exists-shard", Namespace: "test-ns"},
		},
		&multigresv1alpha1.CellTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "exists-cell", Namespace: "test-ns"},
		},
		&multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "exists-core", Namespace: "test-ns"},
		},
		// Fallbacks
		&multigresv1alpha1.ShardTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "test-ns"},
		},
		&multigresv1alpha1.CellTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "test-ns"},
		},
		&multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "test-ns"},
		},
	}

	tests := map[string]struct {
		input           *multigresv1alpha1.MultigresCluster
		existingObjects []client.Object
		failureConfig   *testutil.FailureConfig
		nilResolver     bool
		wrongType       bool
		expectError     string
		validate        func(testing.TB, *multigresv1alpha1.MultigresCluster)
	}{
		"Happy Path: No Template -> Materializes Defaults": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "no-template", Namespace: "test-ns"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "c1"}},
				},
			},
			existingObjects: []client.Object{},
			validate: func(t testing.TB, cluster *multigresv1alpha1.MultigresCluster) {
				t.Helper()
				want := &multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Postgres:        resolver.DefaultPostgresImage,
						MultiAdmin:      resolver.DefaultMultiAdminImage,
						MultiOrch:       resolver.DefaultMultiOrchImage,
						MultiPooler:     resolver.DefaultMultiPoolerImage,
						MultiGateway:    resolver.DefaultMultiGatewayImage,
						MultiAdminWeb:   resolver.DefaultMultiAdminWebImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       resolver.DefaultLogLevel,
						Multipooler:  resolver.DefaultLogLevel,
						Multiorch:    resolver.DefaultLogLevel,
						Multiadmin:   resolver.DefaultLogLevel,
						Multigateway: resolver.DefaultLogLevel,
					},
					Cells: []multigresv1alpha1.CellConfig{
						{
							Name: "c1",
							Spec: &multigresv1alpha1.CellInlineSpec{
								MultiGateway: multigresv1alpha1.StatelessSpec{
									Replicas:  ptr.To(int32(1)),
									Resources: resolver.DefaultResourcesGateway(),
								},
							},
						},
					},
					MultiAdmin: &multigresv1alpha1.MultiAdminConfig{
						Spec: &multigresv1alpha1.StatelessSpec{
							Replicas:  ptr.To(int32(1)),
							Resources: resolver.DefaultResourcesAdmin(),
						},
					},
					MultiAdminWeb: &multigresv1alpha1.MultiAdminWebConfig{
						Spec: &multigresv1alpha1.StatelessSpec{
							Replicas:  ptr.To(int32(1)),
							Resources: resolver.DefaultResourcesAdminWeb(),
						},
					},
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{
							Image:     resolver.DefaultEtcdImage,
							Replicas:  ptr.To(int32(3)),
							RootPath:  resolver.DefaultTopoRootPath,
							Resources: resolver.DefaultResourcesEtcd(),
							Storage:   multigresv1alpha1.StorageSpec{Size: "1Gi"},
						},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name:    "postgres",
							Default: true,
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name:    "default",
									Default: true,
									Shards: []multigresv1alpha1.ShardConfig{
										{
											Name: "0-inf",
											Spec: &multigresv1alpha1.ShardInlineSpec{
												MultiOrch: multigresv1alpha1.MultiOrchSpec{
													StatelessSpec: multigresv1alpha1.StatelessSpec{
														Replicas:  ptr.To(int32(1)),
														Resources: resolver.DefaultResourcesOrch(),
													},
													// Cells should be nil to avoid sticky defaults
													Cells: nil,
												},
												Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
													"default": {
														Type: "readWrite",
														// Cells should be nil to avoid sticky defaults
														Cells:           nil,
														ReplicasPerCell: ptr.To(int32(3)),
														Storage: multigresv1alpha1.StorageSpec{
															Size: "1Gi",
														},
														Postgres: multigresv1alpha1.ContainerConfig{
															Resources: resolver.DefaultResourcesPostgres(),
														},
														Multipooler: multigresv1alpha1.ContainerConfig{
															Resources: resolver.DefaultResourcesPooler(),
														},
													},
												},
											},
											Backup: &multigresv1alpha1.BackupConfig{
												Type: multigresv1alpha1.BackupTypeFilesystem,
												Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
													Path: resolver.DefaultBackupPath,
													Storage: multigresv1alpha1.StorageSpec{
														Size: resolver.DefaultBackupStorageSize,
													},
												},
												Retention: &multigresv1alpha1.RetentionPolicy{
													FullCount:         ptr.To(int32(4)),
													DifferentialCount: ptr.To(int32(1)),
												},
											},
										},
									},
								},
							},
						},
					},
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(true),
					},
					DurabilityPolicy: "AT_LEAST_2",
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path: resolver.DefaultBackupPath,
							Storage: multigresv1alpha1.StorageSpec{
								Size: resolver.DefaultBackupStorageSize,
							},
						},
					},
				}
				if diff := cmp.Diff(want, &cluster.Spec, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("Cluster mismatch (-want +got):\n%s", diff)
				}
			},
		},
		"Happy Path: Fallbacks -> Promotes to Explicit": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "fallback-promote", Namespace: "test-ns"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "c1"}},
				},
			},
			existingObjects: baseObjs,
			validate: func(t testing.TB, cluster *multigresv1alpha1.MultigresCluster) {
				t.Helper()
				if cluster.Spec.TemplateDefaults.CoreTemplate != "default" ||
					cluster.Spec.TemplateDefaults.CellTemplate != "default" ||
					cluster.Spec.TemplateDefaults.ShardTemplate != "default" {
					t.Errorf("Fallbacks were not promoted. Got: %+v", cluster.Spec.TemplateDefaults)
				}
			},
		},
		"Error: Resolver Nil": {
			input:       &multigresv1alpha1.MultigresCluster{},
			nilResolver: true,
			expectError: "resolver is nil",
		},
		"Error: Wrong Type": {
			wrongType:   true,
			expectError: "expected MultigresCluster, got",
		},
		"Error: PopulateDefaults failure": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "test-ns"},
			},
			existingObjects: baseObjs,
			failureConfig: &testutil.FailureConfig{
				OnGet: func(key client.ObjectKey) error { return testutil.ErrInjected },
			},
			expectError: "failed to populate cluster defaults",
		},
		"Error: ResolveMultiAdmin failure": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "test-ns"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "exists-core",
					},
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailKeyAfterNCalls(2, testutil.ErrInjected),
			},
			expectError: "failed to resolve multiadmin",
		},
		"Error: ResolveMultiAdminWeb failure": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "test-ns"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					// Explicit ShardTemplate avoids PopulateDefaults check for "default"
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						ShardTemplate: "exists-shard",
					},
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "exists-core",
					},
					MultiAdmin: &multigresv1alpha1.MultiAdminConfig{
						TemplateRef: "exists-core",
					},
					// MultiAdminWeb empty -> triggers ResolveMultiAdminWeb using "default"
				},
			},
			existingObjects: []client.Object{},
			// Calls:
			// 1. ResolveCoreTemplate("exists-core") (GlobalTopo)
			// 2. ResolveCoreTemplate("exists-core") (MultiAdmin) -> CACHE HIT
			// 3. ResolveCoreTemplate("default") (MultiAdminWeb) -> FAIL
			failureConfig: &testutil.FailureConfig{
				OnGet: func(key client.ObjectKey) error {
					if key.Name == "default" {
						return testutil.ErrInjected
					}
					return nil
				},
			},
			expectError: "failed to resolve multiadmin-web",
		},
		"Error: ResolveGlobalTopo failure": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "test-ns"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						ShardTemplate: "exists-shard",
					},
				},
			},
			existingObjects: baseObjs,
			failureConfig: &testutil.FailureConfig{
				OnGet: func(key client.ObjectKey) error {
					if key.Name == "default" {
						return testutil.ErrInjected
					}
					return nil
				},
			},
			expectError: "failed to resolve globalTopoServer",
		},
		"Error: ResolveCell failure": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "test-ns"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						ShardTemplate: "exists-shard",
						CoreTemplate:  "exists-core",
					},
					Cells: []multigresv1alpha1.CellConfig{{Name: "c1"}},
				},
			},
			existingObjects: baseObjs,
			failureConfig: &testutil.FailureConfig{
				OnGet: func(key client.ObjectKey) error {
					if key.Name == "default" {
						return testutil.ErrInjected
					}
					return nil
				},
			},
			expectError: "failed to resolve cell 'c1'",
		},
		"Error: ResolveShard failure": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "test-ns"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate: "exists-core",
						CellTemplate: "exists-cell",
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{Shards: []multigresv1alpha1.ShardConfig{{Name: "s1"}}},
							},
						},
					},
				},
			},
			existingObjects: baseObjs,
			failureConfig: &testutil.FailureConfig{
				OnGet: func() func(client.ObjectKey) error {
					count := 0
					return func(key client.ObjectKey) error {
						if key.Name == "default" {
							count++
							if count >= 3 {
								return testutil.ErrInjected
							}
							return errors.NewNotFound(schema.GroupResource{}, key.Name)
						}
						return nil
					}
				}(),
			},
			expectError: "failed to resolve shard 's1'",
		},
		"PVC Policy Preservation": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{{
								Name: "tg",
								Shards: []multigresv1alpha1.ShardConfig{{
									Name: "s1",
									Spec: &multigresv1alpha1.ShardInlineSpec{
										PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
											WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
										},
									},
								}},
							}},
						},
					},
				},
			},
			validate: func(t testing.TB, cluster *multigresv1alpha1.MultigresCluster) {
				t.Helper()
				want := &multigresv1alpha1.MultigresCluster{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
					Spec: multigresv1alpha1.MultigresClusterSpec{
						TemplateDefaults: multigresv1alpha1.TemplateDefaults{
							CoreTemplate:  "",
							CellTemplate:  "",
							ShardTemplate: "",
						},
						GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
							Etcd: &multigresv1alpha1.EtcdSpec{
								Image:    resolver.DefaultEtcdImage,
								Replicas: ptr.To(int32(3)),
								RootPath: resolver.DefaultTopoRootPath,
								Storage: multigresv1alpha1.StorageSpec{
									Size: resolver.DefaultEtcdStorageSize,
								},
								Resources: resolver.DefaultResourcesEtcd(),
							},
						},
						MultiAdmin: &multigresv1alpha1.MultiAdminConfig{
							Spec: &multigresv1alpha1.StatelessSpec{
								Replicas:  ptr.To(int32(1)),
								Resources: resolver.DefaultResourcesAdmin(),
							},
						},
						MultiAdminWeb: &multigresv1alpha1.MultiAdminWebConfig{
							Spec: &multigresv1alpha1.StatelessSpec{
								Replicas:  ptr.To(int32(1)),
								Resources: resolver.DefaultResourcesAdminWeb(),
							},
						},
						Images: multigresv1alpha1.ClusterImages{
							Postgres:        resolver.DefaultPostgresImage,
							MultiAdmin:      resolver.DefaultMultiAdminImage,
							MultiAdminWeb:   resolver.DefaultMultiAdminWebImage,
							MultiOrch:       resolver.DefaultMultiOrchImage,
							MultiPooler:     resolver.DefaultMultiPoolerImage,
							MultiGateway:    resolver.DefaultMultiGatewayImage,
							ImagePullPolicy: resolver.DefaultImagePullPolicy,
						},
						LogLevels: multigresv1alpha1.ComponentLogLevels{
							Pgctld:       resolver.DefaultLogLevel,
							Multipooler:  resolver.DefaultLogLevel,
							Multiorch:    resolver.DefaultLogLevel,
							Multiadmin:   resolver.DefaultLogLevel,
							Multigateway: resolver.DefaultLogLevel,
						},
						Databases: []multigresv1alpha1.DatabaseConfig{
							{
								Name: "db",
								TableGroups: []multigresv1alpha1.TableGroupConfig{{
									Name: "tg",
									Shards: []multigresv1alpha1.ShardConfig{{
										Name: "s1",
										Spec: &multigresv1alpha1.ShardInlineSpec{
											PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
												WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
											},
											MultiOrch: multigresv1alpha1.MultiOrchSpec{
												StatelessSpec: multigresv1alpha1.StatelessSpec{
													Replicas:  ptr.To(int32(1)),
													Resources: resolver.DefaultResourcesOrch(),
												},
												Cells: nil, // Dynamic
											},
											Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
												"default": {
													Type:            "readWrite",
													ReplicasPerCell: ptr.To(int32(3)),
													Storage: multigresv1alpha1.StorageSpec{
														Size: resolver.DefaultEtcdStorageSize,
													},
													Postgres: multigresv1alpha1.ContainerConfig{
														Resources: resolver.DefaultResourcesPostgres(),
													},
													Multipooler: multigresv1alpha1.ContainerConfig{
														Resources: resolver.DefaultResourcesPooler(),
													},
												},
											},
										},
										Backup: &multigresv1alpha1.BackupConfig{
											Type: multigresv1alpha1.BackupTypeFilesystem,
											Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
												Path: resolver.DefaultBackupPath,
												Storage: multigresv1alpha1.StorageSpec{
													Size: resolver.DefaultBackupStorageSize,
												},
											},
											Retention: &multigresv1alpha1.RetentionPolicy{
												FullCount:         ptr.To(int32(4)),
												DifferentialCount: ptr.To(int32(1)),
											},
										},
									}},
								}},
							},
						},
						TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
							Enabled: ptr.To(true),
						},
						DurabilityPolicy: "AT_LEAST_2",
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
						Backup: &multigresv1alpha1.BackupConfig{
							Type: multigresv1alpha1.BackupTypeFilesystem,
							Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
								Path: resolver.DefaultBackupPath,
								Storage: multigresv1alpha1.StorageSpec{
									Size: resolver.DefaultBackupStorageSize,
								},
							},
						},
					},
				}
				if diff := cmp.Diff(want, cluster, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("Cluster mismatch (-want +got):\n%s", diff)
				}
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var res *resolver.Resolver
			if !tc.nilResolver {
				objs := make([]client.Object, len(tc.existingObjects))
				for i, obj := range tc.existingObjects {
					objs[i] = obj.DeepCopyObject().(client.Object)
				}
				var c client.Client = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(objs...).
					Build()

				if tc.failureConfig != nil {
					c = testutil.NewFakeClientWithFailures(c, tc.failureConfig)
				}

				res = resolver.NewResolver(c, "test-ns")
			}

			defaulter := NewMultigresClusterDefaulter(res)

			var obj runtime.Object = tc.input
			if tc.wrongType {
				obj = &multigresv1alpha1.Cell{}
			}

			err := defaulter.Default(t.Context(), obj)

			if tc.expectError != "" {
				if err == nil {
					t.Fatalf("Expected error containing %q, got nil", tc.expectError)
				}
				if !strings.Contains(err.Error(), tc.expectError) {
					t.Fatalf("Expected error containing %q, got: %v", tc.expectError, err)
				}
			} else if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if tc.validate != nil {
				tc.validate(t, tc.input)
			}
		})
	}
}
