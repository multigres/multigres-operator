package resolver

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolver_PopulateClusterDefaults(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	// Fixture: An implicit "default" ShardTemplate exists in the namespace
	shardTplDefault := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "default",
		},
	}

	tests := map[string]struct {
		input   *multigresv1alpha1.MultigresCluster
		objects []client.Object
		want    *multigresv1alpha1.MultigresCluster
	}{
		"Empty Cluster: Applies All System Defaults": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
			},
			want: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Postgres:        DefaultPostgresImage,
						Multiadmin:      DefaultMultiadminImage,
						MultiadminWeb:   DefaultMultiadminWebImage,
						Multiorch:       DefaultMultiorchImage,
						Multipooler:     DefaultMultipoolerImage,
						Multigateway:    DefaultMultigatewayImage,
						ImagePullPolicy: DefaultImagePullPolicy,
					},
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "",
						CellTemplate:  "",
						ShardTemplate: "",
					},
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(true),
					},
					DurabilityPolicy: DefaultDurabilityPolicy,
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       DefaultLogLevel,
						Multipooler:  DefaultLogLevel,
						Multiorch:    DefaultLogLevel,
						Multiadmin:   DefaultLogLevel,
						Multigateway: DefaultLogLevel,
					},
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    DefaultBackupPath,
							Storage: multigresv1alpha1.StorageSpec{Size: DefaultBackupStorageSize},
						},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name:    DefaultSystemDatabaseName,
							Default: true,
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name:    DefaultSystemTableGroupName,
									Default: true,
									Shards: []multigresv1alpha1.ShardConfig{
										{Name: "0-inf"},
									},
								},
							},
						},
					},
				},
			},
		},
		"Existing Database but No TableGroups: Inject TG and Shard": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{
						{Name: "custom-db"},
					},
				},
			},
			want: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Postgres:        DefaultPostgresImage,
						Multiadmin:      DefaultMultiadminImage,
						MultiadminWeb:   DefaultMultiadminWebImage,
						Multiorch:       DefaultMultiorchImage,
						Multipooler:     DefaultMultipoolerImage,
						Multigateway:    DefaultMultigatewayImage,
						ImagePullPolicy: DefaultImagePullPolicy,
					},
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "",
						CellTemplate:  "",
						ShardTemplate: "",
					},
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(true),
					},
					DurabilityPolicy: DefaultDurabilityPolicy,
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       DefaultLogLevel,
						Multipooler:  DefaultLogLevel,
						Multiorch:    DefaultLogLevel,
						Multiadmin:   DefaultLogLevel,
						Multigateway: DefaultLogLevel,
					},
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    DefaultBackupPath,
							Storage: multigresv1alpha1.StorageSpec{Size: DefaultBackupStorageSize},
						},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "custom-db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name:    DefaultSystemTableGroupName,
									Default: true,
									Shards: []multigresv1alpha1.ShardConfig{
										{Name: "0-inf"},
									},
								},
							},
						},
					},
				},
			},
		},
		"Existing TableGroup but No Shards: Inject Shard 0 with Default Cells": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a"},
						{Name: "zone-b"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "custom-db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{Name: "my-tg"},
							},
						},
					},
				},
			},
			want: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Postgres:        DefaultPostgresImage,
						Multiadmin:      DefaultMultiadminImage,
						MultiadminWeb:   DefaultMultiadminWebImage,
						Multiorch:       DefaultMultiorchImage,
						Multipooler:     DefaultMultipoolerImage,
						Multigateway:    DefaultMultigatewayImage,
						ImagePullPolicy: DefaultImagePullPolicy,
					},
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "",
						CellTemplate:  "",
						ShardTemplate: "",
					},
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(true),
					},
					DurabilityPolicy: DefaultDurabilityPolicy,
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       DefaultLogLevel,
						Multipooler:  DefaultLogLevel,
						Multiorch:    DefaultLogLevel,
						Multiadmin:   DefaultLogLevel,
						Multigateway: DefaultLogLevel,
					},
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    DefaultBackupPath,
							Storage: multigresv1alpha1.StorageSpec{Size: DefaultBackupStorageSize},
						},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a"},
						{Name: "zone-b"},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "custom-db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name: "my-tg",
									Shards: []multigresv1alpha1.ShardConfig{
										{
											Name: "0-inf",
											Spec: &multigresv1alpha1.ShardInlineSpec{
												Multiorch: multigresv1alpha1.MultiorchSpec{},
												Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
													"default": {
														Type: "readWrite",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		// COVERAGE: Implicit Default ShardTemplate exists -> Shard 0 created, but defaults NOT injected.
		// This hits the `else { shouldInjectDefaults = false }` branch.
		"Implicit Default ShardTemplate Exists -> Shard 0 Created Empty": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a"}},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{Name: "tg"},
							},
						},
					},
				},
			},
			objects: []client.Object{shardTplDefault}, // "default" exists!
			want: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Postgres:        DefaultPostgresImage,
						Multiadmin:      DefaultMultiadminImage,
						MultiadminWeb:   DefaultMultiadminWebImage,
						Multiorch:       DefaultMultiorchImage,
						Multipooler:     DefaultMultipoolerImage,
						Multigateway:    DefaultMultigatewayImage,
						ImagePullPolicy: DefaultImagePullPolicy,
					},
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "",
						CellTemplate:  "",
						ShardTemplate: "",
					},
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(true),
					},
					DurabilityPolicy: DefaultDurabilityPolicy,
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       DefaultLogLevel,
						Multipooler:  DefaultLogLevel,
						Multiorch:    DefaultLogLevel,
						Multiadmin:   DefaultLogLevel,
						Multigateway: DefaultLogLevel,
					},
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    DefaultBackupPath,
							Storage: multigresv1alpha1.StorageSpec{Size: DefaultBackupStorageSize},
						},
					},
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a"}},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name: "tg",
									Shards: []multigresv1alpha1.ShardConfig{
										{
											Name: "0-inf",
											Spec: &multigresv1alpha1.ShardInlineSpec{
												Multiorch: multigresv1alpha1.MultiorchSpec{},
												// KEY: Pools is empty because implicit template takes over
												Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"Existing Shards: Do Not Inject": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "custom-db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name: "my-tg",
									Shards: []multigresv1alpha1.ShardConfig{
										{Name: "custom-shard"},
									},
								},
							},
						},
					},
				},
			},
			want: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Postgres:        DefaultPostgresImage,
						Multiadmin:      DefaultMultiadminImage,
						MultiadminWeb:   DefaultMultiadminWebImage,
						Multiorch:       DefaultMultiorchImage,
						Multipooler:     DefaultMultipoolerImage,
						Multigateway:    DefaultMultigatewayImage,
						ImagePullPolicy: DefaultImagePullPolicy,
					},
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "",
						CellTemplate:  "",
						ShardTemplate: "",
					},
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(true),
					},
					DurabilityPolicy: DefaultDurabilityPolicy,
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       DefaultLogLevel,
						Multipooler:  DefaultLogLevel,
						Multiorch:    DefaultLogLevel,
						Multiadmin:   DefaultLogLevel,
						Multigateway: DefaultLogLevel,
					},
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    DefaultBackupPath,
							Storage: multigresv1alpha1.StorageSpec{Size: DefaultBackupStorageSize},
						},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "custom-db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name: "my-tg",
									Shards: []multigresv1alpha1.ShardConfig{
										{Name: "custom-shard"},
									},
								},
							},
						},
					},
				},
			},
		},
		"Pre-populated Fields: Preserves Values": {
			input: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Postgres:        "custom/postgres:16",
						Multiadmin:      "custom/admin:1",
						MultiadminWeb:   "custom/web:1",
						Multiorch:       "custom/orch:1",
						Multipooler:     "custom/pooler:1",
						Multigateway:    "custom/gateway:1",
						ImagePullPolicy: "Always",
					},
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "custom-core",
						CellTemplate:  "custom-cell",
						ShardTemplate: "custom-shard",
					},
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(false),
					},
					DurabilityPolicy: "MULTI_CELL_AT_LEAST_2",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       DefaultLogLevel,
						Multipooler:  DefaultLogLevel,
						Multiorch:    DefaultLogLevel,
						Multiadmin:   DefaultLogLevel,
						Multigateway: DefaultLogLevel,
					},
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3,
						S3: &multigresv1alpha1.S3BackupConfig{
							Bucket: "my-bucket",
							Region: "us-east-1",
						},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "custom-db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name: "custom-tg",
									Shards: []multigresv1alpha1.ShardConfig{
										{Name: "custom-0"},
									},
								},
							},
						},
					},
				},
			},
			want: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Postgres:        "custom/postgres:16",
						Multiadmin:      "custom/admin:1",
						MultiadminWeb:   "custom/web:1",
						Multiorch:       "custom/orch:1",
						Multipooler:     "custom/pooler:1",
						Multigateway:    "custom/gateway:1",
						ImagePullPolicy: "Always",
					},
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "custom-core",
						CellTemplate:  "custom-cell",
						ShardTemplate: "custom-shard",
					},
					TopologyPruning: &multigresv1alpha1.TopologyPruningConfig{
						Enabled: ptr.To(false),
					},
					DurabilityPolicy: "MULTI_CELL_AT_LEAST_2",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       DefaultLogLevel,
						Multipooler:  DefaultLogLevel,
						Multiorch:    DefaultLogLevel,
						Multiadmin:   DefaultLogLevel,
						Multigateway: DefaultLogLevel,
					},
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3,
						S3: &multigresv1alpha1.S3BackupConfig{
							Bucket: "my-bucket",
							Region: "us-east-1",
						},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "custom-db",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name: "custom-tg",
									Shards: []multigresv1alpha1.ShardConfig{
										{Name: "custom-0"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := NewResolver(
				fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build(),
				"default",
			)

			got := tc.input.DeepCopy()
			if _, err := r.PopulateClusterDefaults(t.Context(), got); err != nil {
				t.Fatalf("PopulateClusterDefaults failed: %v", err)
			}

			if diff := cmp.Diff(
				tc.want,
				got,
				cmpopts.IgnoreUnexported(resource.Quantity{}),
				cmpopts.EquateEmpty(),
			); diff != "" {
				t.Errorf("Diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolver_PopulateClusterDefaults_ClientError(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	errSim := errors.New("simulated error")
	mc := testutil.NewFakeClientWithFailures(fake.NewClientBuilder().WithScheme(scheme).Build(),
		&testutil.FailureConfig{
			OnGet: func(_ client.ObjectKey) error { return errSim },
		})

	r := NewResolver(mc, "default")
	// This input triggers the "check implicit shard template" path because ShardTemplate is empty
	input := &multigresv1alpha1.MultigresCluster{
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Databases: []multigresv1alpha1.DatabaseConfig{{Name: "db"}},
		},
	}

	_, err := r.PopulateClusterDefaults(t.Context(), input)
	if err == nil || !errors.Is(err, errSim) {
		t.Errorf("Expected simulated error, got %v", err)
	}
}

func TestResolver_ResolveGlobalTopo(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	coreTpl, _, _, ns := setupFixtures(t)

	tests := map[string]struct {
		cluster *multigresv1alpha1.MultigresCluster
		objects []client.Object
		want    *multigresv1alpha1.GlobalTopoServerSpec
		wantErr bool
	}{
		"Inline Takes Precedence": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{Image: "inline"},
					},
				},
			},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     "inline",
					Replicas:  ptr.To(DefaultEtcdReplicas),
					RootPath:  DefaultTopoRootPath,
					Resources: DefaultResourcesEtcd(),
					Storage:   multigresv1alpha1.StorageSpec{Size: DefaultEtcdStorageSize},
				},
			},
		},
		"Template Reference": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "default",
					},
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     "core-default", // From fixture
					Replicas:  ptr.To(DefaultEtcdReplicas),
					RootPath:  DefaultTopoRootPath,
					Resources: DefaultResourcesEtcd(),
					Storage:   multigresv1alpha1.StorageSpec{Size: DefaultEtcdStorageSize},
				},
			},
		},
		// COVERAGE: Explicit Override of ALL fields to ensure merge logic branches are hit
		"Template Reference + Full Inline Override": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "default",
						Etcd: &multigresv1alpha1.EtcdSpec{
							Image:    "override-image",
							Replicas: ptr.To(int32(99)),
							RootPath: "/override/path",
							Storage:  multigresv1alpha1.StorageSpec{Size: "99Gi"},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{
									corev1.ResourceMemory: resource.MustParse("99Gi"),
								},
							},
						},
					},
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:    "override-image",
					Replicas: ptr.To(int32(99)),
					RootPath: "/override/path",
					Storage:  multigresv1alpha1.StorageSpec{Size: "99Gi"},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("99Gi"),
						},
					},
				},
			},
		},
		"Cluster Default Template": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate: "default",
					},
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     "core-default",
					Replicas:  ptr.To(DefaultEtcdReplicas),
					RootPath:  DefaultTopoRootPath,
					Resources: DefaultResourcesEtcd(),
					Storage:   multigresv1alpha1.StorageSpec{Size: DefaultEtcdStorageSize},
				},
			},
		},
		"Fallback (Implicit Default Not Found) -> Default Etcd": {
			cluster: &multigresv1alpha1.MultigresCluster{},
			objects: nil,
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     DefaultEtcdImage,
					Replicas:  ptr.To(DefaultEtcdReplicas),
					RootPath:  DefaultTopoRootPath,
					Resources: DefaultResourcesEtcd(),
					Storage:   multigresv1alpha1.StorageSpec{Size: DefaultEtcdStorageSize},
				},
			},
		},
		"Missing Explicit Template -> Error": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "missing",
					},
				},
			},
			wantErr: true,
		},
		"External Spec": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						External: &multigresv1alpha1.ExternalTopoServerSpec{
							Endpoints: []multigresv1alpha1.EndpointUrl{"https://1.2.3.4:2379"},
						},
						Etcd: &multigresv1alpha1.EtcdSpec{Image: "ignored"},
					},
				},
			},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints:      []multigresv1alpha1.EndpointUrl{"https://1.2.3.4:2379"},
					Implementation: DefaultTopoImplementation,
					RootPath:       DefaultTopoRootPath,
				},
				Etcd: nil, // Explicitly nilled out
			},
		},
		"CoreTemplate with GlobalTopo": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "with-topo",
					},
				},
			},
			objects: []client.Object{
				&multigresv1alpha1.CoreTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "with-topo", Namespace: "default"},
					Spec: multigresv1alpha1.CoreTemplateSpec{
						GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
							Etcd: &multigresv1alpha1.EtcdSpec{Image: "core-image"},
						},
					},
				},
			},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     "core-image",
					Replicas:  ptr.To(DefaultEtcdReplicas), // Defaults applied
					RootPath:  DefaultTopoRootPath,
					Resources: DefaultResourcesEtcd(),
					Storage:   multigresv1alpha1.StorageSpec{Size: DefaultEtcdStorageSize},
				},
			},
		},
		"Template External -> Inline Etcd Override": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "topo-external",
						Etcd:        &multigresv1alpha1.EtcdSpec{Image: "new-etcd"},
					},
				},
			},
			objects: []client.Object{
				&multigresv1alpha1.CoreTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "topo-external", Namespace: "default"},
					Spec: multigresv1alpha1.CoreTemplateSpec{
						GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
							// Template defines external, so Etcd is nil
							// Note: TopoServerSpec in CoreTemplate only has Etcd field in current definition?
							// Let's check TopoServerSpec definition again.
							// It says: type TopoServerSpec struct { Etcd *EtcdSpec }
							// It DOES NOT have External.
							// Wait, if it only has Etcd, how can it be nil? It's a pointer.
							// Use case: User creates CoreTemplate with empty spec? Or nil TopoServer?
							Etcd: nil,
						},
					},
				},
			},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     "new-etcd",
					Replicas:  ptr.To(DefaultEtcdReplicas),
					RootPath:  DefaultTopoRootPath,
					Resources: DefaultResourcesEtcd(),
					Storage:   multigresv1alpha1.StorageSpec{Size: DefaultEtcdStorageSize},
				},
			},
		},
		"Partial Storage Override Preserves Base Fields": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "with-storage",
						Etcd: &multigresv1alpha1.EtcdSpec{
							Storage: multigresv1alpha1.StorageSpec{Class: "gp3"},
						},
					},
				},
			},
			objects: []client.Object{
				&multigresv1alpha1.CoreTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "with-storage", Namespace: "default"},
					Spec: multigresv1alpha1.CoreTemplateSpec{
						GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
							Etcd: &multigresv1alpha1.EtcdSpec{
								Image: "template-image",
								Storage: multigresv1alpha1.StorageSpec{
									Size:  "50Gi",
									Class: "standard",
									AccessModes: []corev1.PersistentVolumeAccessMode{
										corev1.ReadWriteOnce,
									},
								},
							},
						},
					},
				},
			},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     "template-image",
					Replicas:  ptr.To(DefaultEtcdReplicas),
					RootPath:  DefaultTopoRootPath,
					Resources: DefaultResourcesEtcd(),
					Storage: multigresv1alpha1.StorageSpec{
						Size:        "50Gi",
						Class:       "gp3",
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					},
				},
			},
		},
		// COVERAGE: inline AccessModes override replaces base AccessModes (mergeEtcdSpec branch)
		"Inline AccessModes Override": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "access-modes-base",
						Etcd: &multigresv1alpha1.EtcdSpec{
							Storage: multigresv1alpha1.StorageSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteMany,
								},
							},
						},
					},
				},
			},
			objects: []client.Object{
				&multigresv1alpha1.CoreTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "access-modes-base", Namespace: "default"},
					Spec: multigresv1alpha1.CoreTemplateSpec{
						GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
							Etcd: &multigresv1alpha1.EtcdSpec{
								Image: "base-image",
								Storage: multigresv1alpha1.StorageSpec{
									Size: "10Gi",
									AccessModes: []corev1.PersistentVolumeAccessMode{
										corev1.ReadWriteOnce,
									},
								},
							},
						},
					},
				},
			},
			want: &multigresv1alpha1.GlobalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Image:     "base-image",
					Replicas:  ptr.To(DefaultEtcdReplicas),
					RootPath:  DefaultTopoRootPath,
					Resources: DefaultResourcesEtcd(),
					Storage: multigresv1alpha1.StorageSpec{
						Size:        "10Gi",
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()
			r := NewResolver(c, ns)

			got, err := r.ResolveGlobalTopo(t.Context(), tc.cluster)
			if tc.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(
				tc.want,
				got,
				cmpopts.IgnoreUnexported(resource.Quantity{}),
				cmpopts.EquateEmpty(),
			); diff != "" {
				t.Errorf("Diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolver_ResolveMultiadmin(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	coreTpl, _, _, ns := setupFixtures(t)

	tests := map[string]struct {
		cluster *multigresv1alpha1.MultigresCluster
		objects []client.Object
		want    *multigresv1alpha1.StatelessSpec
		wantErr bool
	}{
		"Inline": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(10))},
					},
				},
			},
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(int32(10)),
				Resources: DefaultResourcesAdmin(),
			},
		},
		"Template": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						TemplateRef: "default",
					},
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.StatelessSpec{
				// No Image, uses global
				Replicas:  ptr.To(DefaultAdminReplicas),
				Resources: DefaultResourcesAdmin(),
			},
		},
		"Missing Template": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{TemplateRef: "gone"},
				},
			},
			wantErr: true,
		},
		"Fallback (No Spec, No Template) -> Defaults": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Multiadmin: &multigresv1alpha1.MultiadminConfig{}, // Empty
				},
			},
			objects: nil, // No core template found
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(DefaultAdminReplicas),
				Resources: DefaultResourcesAdmin(),
			},
		},
		"TemplateDefaults.CoreTemplate propagates to Multiadmin": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate: "default",
					},
					// Multiadmin is nil — no inline spec or templateRef
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(DefaultAdminReplicas),
				Resources: DefaultResourcesAdmin(),
			},
		},
		"TemplateDefaults.CoreTemplate with inline override": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate: "default",
					},
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(7))},
					},
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(int32(7)),
				Resources: DefaultResourcesAdmin(),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()
			r := NewResolver(c, ns)

			got, err := r.ResolveMultiadmin(t.Context(), tc.cluster)
			if tc.wantErr {
				if err == nil {
					t.Error("Expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(
				tc.want,
				got,
				cmpopts.IgnoreUnexported(resource.Quantity{}),
				cmpopts.EquateEmpty(),
			); diff != "" {
				t.Errorf("Diff (-want +got):\n%s", diff)
			}
		})
	}
}

// TestResolver_ResolveCoreTemplate hits specific error branches for Implicit vs Explicit missing.
func TestResolver_ResolveCoreTemplate(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	r := NewResolver(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		"default",
	)

	// 1. Implicit Fallback ("default" or "") -> Not Found -> Returns nil, nil (No Error)
	// This covers: "if isImplicitFallback { return ... nil }"
	t.Run("Implicit Fallback Missing", func(t *testing.T) {
		tpl, err := r.ResolveCoreTemplate(t.Context(), "")
		if err != nil {
			t.Errorf("Expected nil error for implicit missing, got %v", err)
		}
		if tpl.Name != "" { // Empty struct
			t.Errorf("Expected empty template, got %v", tpl)
		}
	})

	// 2. Explicit Template ("custom") -> Not Found -> Returns Error
	// This covers: "return nil, fmt.Errorf(...)"
	t.Run("Explicit Template Missing", func(t *testing.T) {
		_, err := r.ResolveCoreTemplate(t.Context(), "missing-custom")
		if err == nil {
			t.Error("Expected error for explicit missing template")
		}
	})
}

func TestResolver_ClientErrors_Core(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	errSim := testutil.ErrInjected
	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	failConfig := &testutil.FailureConfig{
		OnGet: func(_ client.ObjectKey) error { return errSim },
	}
	c := testutil.NewFakeClientWithFailures(baseClient, failConfig)

	r := NewResolver(c, "default")

	_, err := r.ResolveCoreTemplate(t.Context(), "any")
	if err == nil || !errors.Is(err, errSim) {
		t.Errorf("Error mismatch: got %v, want %v", err, errSim)
	}
}

func TestResolver_ResolveMultiadminWeb(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	coreTpl, _, _, ns := setupFixtures(t)

	tests := map[string]struct {
		cluster *multigresv1alpha1.MultigresCluster
		objects []client.Object
		want    *multigresv1alpha1.StatelessSpec
		wantErr bool
	}{
		"Inline": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{
						Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(10))},
					},
				},
			},
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(int32(10)),
				Resources: DefaultResourcesAdminWeb(),
			},
		},
		"Template": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{
						TemplateRef: "default",
					},
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(int32(3)),
				Resources: DefaultResourcesAdminWeb(),
			},
		},
		"Fallback (No Spec, No Template) -> Defaults": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{},
				},
			},
			objects: nil,
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(DefaultMultiadminWebReplicas),
				Resources: DefaultResourcesAdminWeb(),
			},
		},
		"Explicit Missing Template -> Error": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{
						TemplateRef: "missing",
					},
				},
			},
			wantErr: true,
		},
		"TemplateDefaults.CoreTemplate propagates to MultiadminWeb": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate: "default",
					},
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(int32(3)), // From CoreTemplate fixture
				Resources: DefaultResourcesAdminWeb(),
			},
		},
		"TemplateDefaults.CoreTemplate with inline override": {
			cluster: &multigresv1alpha1.MultigresCluster{
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate: "default",
					},
					MultiadminWeb: &multigresv1alpha1.MultiadminWebConfig{
						Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(7))},
					},
				},
			},
			objects: []client.Object{coreTpl},
			want: &multigresv1alpha1.StatelessSpec{
				Replicas:  ptr.To(int32(7)), // Inline wins over template
				Resources: DefaultResourcesAdminWeb(),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()
			r := NewResolver(c, ns)

			got, err := r.ResolveMultiadminWeb(t.Context(), tc.cluster)
			if tc.wantErr {
				if err == nil {
					t.Error("Expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(
				tc.want,
				got,
				cmpopts.IgnoreUnexported(resource.Quantity{}),
				cmpopts.EquateEmpty(),
			); diff != "" {
				t.Errorf("Diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveGlobalTopo_PVCDeletionPolicy(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	r := &Resolver{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
	}

	t.Run("GlobalTopoServerSpec Level", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
					},
				},
			},
		}
		got, err := r.ResolveGlobalTopo(t.Context(), cluster)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.PVCDeletionPolicy == nil ||
			got.PVCDeletionPolicy.WhenDeleted != multigresv1alpha1.DeletePVCRetentionPolicy {
			t.Errorf("Expected GlobalTopo PVCDeletionPolicy=Delete, got %v", got.PVCDeletionPolicy)
		}
	})

	t.Run("EtcdSpec Level Override", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
						},
					},
				},
			},
		}
		got, err := r.ResolveGlobalTopo(t.Context(), cluster)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// mergeEtcdSpec should have applied this to the base
		if got.Etcd.PVCDeletionPolicy == nil ||
			got.Etcd.PVCDeletionPolicy.WhenDeleted != multigresv1alpha1.RetainPVCRetentionPolicy {
			t.Errorf("Expected Etcd PVCDeletionPolicy=Retain, got %v", got.Etcd.PVCDeletionPolicy)
		}
	})
}
