//go:build integration
// +build integration

package shard_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"

	"github.com/multigres/testkit/assert"
)

func TestSetupWithManager(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)

	assert.NewAborting(t).NoError((&shardcontroller.ShardReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("shard-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}), "Failed to create controller")
}

func setTestPostgresPasswordSecretRef(shard *multigresv1alpha1.Shard) {
	if shard == nil || shard.Spec.PostgresPasswordSecretRef.Name != "" {
		return
	}
	shard.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: "multigres-admin-password",
		Key:  "password",
	}
}

func createTestPostgresPasswordSecret(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	namespace string,
) {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multigres-admin-password",
			Namespace: namespace,
		},
		Data: map[string][]byte{
			"password": []byte("postgres"),
		},
	}
	err := c.Create(ctx, secret)
	assert.NewAborting(t).
		NoError(client.IgnoreAlreadyExists(err), "Failed to create postgres password Secret: %v", err)
}

func createTestPostgresInitSecretsSecret(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	namespace, name, key string,
) {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			key: []byte(
				`{"roles":{"app":"app-password"},"database_settings":{"testdb":{"work_mem":"64MB"}}}`,
			),
		},
	}
	err := c.Create(ctx, secret)
	assert.NewAborting(t).
		NoError(client.IgnoreAlreadyExists(err), "Failed to create postgres init-secrets Secret: %v", err)
}

func TestShardReconciliation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	tests := map[string]struct {
		shard         *multigresv1alpha1.Shard
		wantResources []client.Object
	}{
		"simple shard with single replica pool": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "testdb",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},

					TableGroupName: "default",
					ShardName:      "0",
					Images: multigresv1alpha1.ShardImages{
						Multiorch:   "ghcr.io/multigres/multigres:main",
						Multipooler: "ghcr.io/multigres/multigres:main",
						Postgres:    "postgres:17",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a", "zone-b"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone-a"},
							Type:            "readWrite",
							ReplicasPerCell: ptr.To(int32(2)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    "/backups",
							Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
						},
					},
				},
			},
			wantResources: []client.Object{
				// Multiorch Deployment for zone-a
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-multiorch-zone-a",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"test-shard-multiorch-zone-a",
							"multiorch",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(
								shardLabels(
									t,
									"test-shard-multiorch-zone-a",
									"multiorch",
									"zone-a",
								),
							),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(
									t,
									"test-shard-multiorch-zone-a",
									"multiorch",
									"zone-a",
								),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiorch",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multiorch",
											"--http-port=15300",
											"--grpc-port=15370",
											"--topo-global-server-addresses=global-topo:2379",
											"--topo-global-root=/multigres/global",
											"--cell=zone-a",
											"--watch-targets=testdb/default/0",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15300),
											tcpPort(t, "grpc", 15370),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
							},
						},
					},
				},
				// Multiorch Service for zone-a
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-multiorch-zone-a",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"test-shard-multiorch-zone-a",
							"multiorch",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(t, "test-shard-multiorch-zone-a", "multiorch", "zone-a"),
						),
					},
				},
				// Multiorch Deployment for zone-b
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-multiorch-zone-b",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"test-shard-multiorch-zone-b",
							"multiorch",
							"zone-b",
						),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(
								shardLabels(
									t,
									"test-shard-multiorch-zone-b",
									"multiorch",
									"zone-b",
								),
							),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(
									t,
									"test-shard-multiorch-zone-b",
									"multiorch",
									"zone-b",
								),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiorch",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multiorch",
											"--http-port=15300",
											"--grpc-port=15370",
											"--topo-global-server-addresses=global-topo:2379",
											"--topo-global-root=/multigres/global",
											"--cell=zone-b",
											"--watch-targets=testdb/default/0",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15300),
											tcpPort(t, "grpc", 15370),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
							},
						},
					},
				},
				// Multiorch Service for zone-b
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-multiorch-zone-b",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"test-shard-multiorch-zone-b",
							"multiorch",
							"zone-b",
						),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(t, "test-shard-multiorch-zone-b", "multiorch", "zone-b"),
						),
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-shard-pool-primary-zone-a-headless",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"test-shard-pool-primary-zone-a",
							"shard-pool",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type:      corev1.ServiceTypeClusterIP,
						ClusterIP: corev1.ClusterIPNone,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15200),
							tcpServicePort(t, "grpc", 15270),
							tcpServicePort(t, "postgres", 5432),
							tcpServicePort(t, "metrics", 9187),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"test-shard-pool-primary-zone-a",
								"shard-pool",
								"zone-a",
							),
						),
						PublishNotReadyAddresses: true,
					},
				},
			},
		},
		"shard with postgres init secrets ref": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "init-secrets-shard",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "testdb",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},

					TableGroupName: "default",
					ShardName:      "0",
					PostgresInitSecretsRef: &multigresv1alpha1.PostgresInitSecretsRef{
						Name: "init-secrets-shard-init-secrets",
						Key:  shardcontroller.PostgresInitSecretsFileName,
					},
					Images: multigresv1alpha1.ShardImages{
						Multiorch:   "ghcr.io/multigres/multigres:main",
						Multipooler: "ghcr.io/multigres/multigres:main",
						Postgres:    "postgres:17",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone-a"},
							Type:            "readWrite",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "1Gi",
							},
						},
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    "/backups",
							Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
						},
					},
				},
			},
			wantResources: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "init-secrets-shard-multiorch-zone-a",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"init-secrets-shard-multiorch-zone-a",
							"multiorch",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "init-secrets-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(
								shardLabels(
									t,
									"init-secrets-shard-multiorch-zone-a",
									"multiorch",
									"zone-a",
								),
							),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(
									t,
									"init-secrets-shard-multiorch-zone-a",
									"multiorch",
									"zone-a",
								),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiorch",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multiorch",
											"--http-port=15300",
											"--grpc-port=15370",
											"--topo-global-server-addresses=global-topo:2379",
											"--topo-global-root=/multigres/global",
											"--cell=zone-a",
											"--watch-targets=testdb/default/0",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15300),
											tcpPort(t, "grpc", 15370),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
							},
						},
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "init-secrets-shard-multiorch-zone-a",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"init-secrets-shard-multiorch-zone-a",
							"multiorch",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "init-secrets-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"init-secrets-shard-multiorch-zone-a",
								"multiorch",
								"zone-a",
							),
						),
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "init-secrets-shard-pool-primary-zone-a-headless",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"init-secrets-shard-pool-primary-zone-a",
							"shard-pool",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "init-secrets-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type:      corev1.ServiceTypeClusterIP,
						ClusterIP: corev1.ClusterIPNone,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15200),
							tcpServicePort(t, "grpc", 15270),
							tcpServicePort(t, "postgres", 5432),
							tcpServicePort(t, "metrics", 9187),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"init-secrets-shard-pool-primary-zone-a",
								"shard-pool",
								"zone-a",
							),
						),
						PublishNotReadyAddresses: true,
					},
				},
			},
		},
		"shard with delete pvc policy": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "delete-policy-shard",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "testdb",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},

					TableGroupName: "default",
					ShardName:      "0",
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
					Images: multigresv1alpha1.ShardImages{
						Multiorch:   "ghcr.io/multigres/multigres:main",
						Multipooler: "ghcr.io/multigres/multigres:main",
						Postgres:    "postgres:17",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone-a"},
							Type:            "readWrite",
							ReplicasPerCell: ptr.To(int32(1)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "1Gi",
							},
						},
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    "/backups",
							Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
						},
					},
				},
			},
			wantResources: []client.Object{
				// Multiorch Deployment for zone-a
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "delete-policy-shard-multiorch-zone-a",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"delete-policy-shard-multiorch-zone-a",
							"multiorch",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "delete-policy-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(
								shardLabels(
									t,
									"delete-policy-shard-multiorch-zone-a",
									"multiorch",
									"zone-a",
								),
							),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(
									t,
									"delete-policy-shard-multiorch-zone-a",
									"multiorch",
									"zone-a",
								),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiorch",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multiorch",
											"--http-port=15300",
											"--grpc-port=15370",
											"--topo-global-server-addresses=global-topo:2379",
											"--topo-global-root=/multigres/global",
											"--cell=zone-a",
											"--watch-targets=testdb/default/0",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15300),
											tcpPort(t, "grpc", 15370),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
							},
						},
					},
				},
				// Multiorch Service for zone-a
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "delete-policy-shard-multiorch-zone-a",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"delete-policy-shard-multiorch-zone-a",
							"multiorch",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "delete-policy-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"delete-policy-shard-multiorch-zone-a",
								"multiorch",
								"zone-a",
							),
						),
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "delete-policy-shard-pool-primary-zone-a-headless",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"delete-policy-shard-pool-primary-zone-a",
							"shard-pool",
							"zone-a",
						),
						OwnerReferences: shardOwnerRefs(t, "delete-policy-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type:      corev1.ServiceTypeClusterIP,
						ClusterIP: corev1.ClusterIPNone,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15200),
							tcpServicePort(t, "grpc", 15270),
							tcpServicePort(t, "postgres", 5432),
							tcpServicePort(t, "metrics", 9187),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"delete-policy-shard-pool-primary-zone-a",
								"shard-pool",
								"zone-a",
							),
						),
						PublishNotReadyAddresses: true,
					},
				},
			},
		},
		"shard with pool spanning two cells": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "multi-cell-shard",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName: "testdb",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},

					TableGroupName: "default",
					ShardName:      "0",
					Images: multigresv1alpha1.ShardImages{
						Multiorch:   "ghcr.io/multigres/multigres:main",
						Multipooler: "ghcr.io/multigres/multigres:main",
						Postgres:    "postgres:17",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					Multiorch: multigresv1alpha1.MultiorchSpec{
						Cells: []multigresv1alpha1.CellName{"zone1", "zone2"},
					},
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"primary": {
							Cells:           []multigresv1alpha1.CellName{"zone1", "zone2"},
							Type:            "readWrite",
							ReplicasPerCell: ptr.To(int32(2)),
							Storage: multigresv1alpha1.StorageSpec{
								Size: "10Gi",
							},
						},
					},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
							Path:    "/backups",
							Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
						},
					},
				},
			},
			wantResources: []client.Object{
				// Multiorch Deployment for zone1
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "multi-cell-shard-multiorch-zone1",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"multi-cell-shard-multiorch-zone1",
							"multiorch",
							"zone1",
						),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(
								shardLabels(
									t,
									"multi-cell-shard-multiorch-zone1",
									"multiorch",
									"zone1",
								),
							),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(
									t,
									"multi-cell-shard-multiorch-zone1",
									"multiorch",
									"zone1",
								),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiorch",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multiorch",
											"--http-port=15300",
											"--grpc-port=15370",
											"--topo-global-server-addresses=global-topo:2379",
											"--topo-global-root=/multigres/global",
											"--cell=zone1",
											"--watch-targets=testdb/default/0",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15300),
											tcpPort(t, "grpc", 15370),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
							},
						},
					},
				},
				// Multiorch Service for zone1
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "multi-cell-shard-multiorch-zone1",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"multi-cell-shard-multiorch-zone1",
							"multiorch",
							"zone1",
						),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"multi-cell-shard-multiorch-zone1",
								"multiorch",
								"zone1",
							),
						),
					},
				},
				// Multiorch Deployment for zone2
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "multi-cell-shard-multiorch-zone2",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"multi-cell-shard-multiorch-zone2",
							"multiorch",
							"zone2",
						),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(
								shardLabels(
									t,
									"multi-cell-shard-multiorch-zone2",
									"multiorch",
									"zone2",
								),
							),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(
									t,
									"multi-cell-shard-multiorch-zone2",
									"multiorch",
									"zone2",
								),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiorch",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multiorch",
											"--http-port=15300",
											"--grpc-port=15370",
											"--topo-global-server-addresses=global-topo:2379",
											"--topo-global-root=/multigres/global",
											"--cell=zone2",
											"--watch-targets=testdb/default/0",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15300),
											tcpPort(t, "grpc", 15370),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15300),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
							},
						},
					},
				},
				// Multiorch Service for zone2
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "multi-cell-shard-multiorch-zone2",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"multi-cell-shard-multiorch-zone2",
							"multiorch",
							"zone2",
						),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"multi-cell-shard-multiorch-zone2",
								"multiorch",
								"zone2",
							),
						),
					},
				},
				// Headless Service for zone1
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "multi-cell-shard-pool-primary-zone1-headless",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"multi-cell-shard-pool-primary-zone1",
							"shard-pool",
							"zone1",
						),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type:      corev1.ServiceTypeClusterIP,
						ClusterIP: corev1.ClusterIPNone,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15200),
							tcpServicePort(t, "grpc", 15270),
							tcpServicePort(t, "postgres", 5432),
							tcpServicePort(t, "metrics", 9187),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"multi-cell-shard-pool-primary-zone1",
								"shard-pool",
								"zone1",
							),
						),
						PublishNotReadyAddresses: true,
					},
				},
				// Headless Service for zone2
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "multi-cell-shard-pool-primary-zone2-headless",
						Namespace: "default",
						Labels: shardLabels(
							t,
							"multi-cell-shard-pool-primary-zone2",
							"shard-pool",
							"zone2",
						),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type:      corev1.ServiceTypeClusterIP,
						ClusterIP: corev1.ClusterIPNone,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15200),
							tcpServicePort(t, "grpc", 15270),
							tcpServicePort(t, "postgres", 5432),
							tcpServicePort(t, "metrics", 9187),
						},
						Selector: metadata.GetSelectorLabels(
							shardLabels(
								t,
								"multi-cell-shard-pool-primary-zone2",
								"shard-pool",
								"zone2",
							),
						),
						PublishNotReadyAddresses: true,
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ck := assert.NewCollecting(t)
			ctx := t.Context()
			mgr := testutil.SetUpEnvtestManager(t, scheme,
				testutil.WithCRDPaths(
					filepath.Join("../../../../", "config", "crd", "bases"),
				),
			)

			watcher := testutil.NewResourceWatcher(t, ctx, mgr,
				testutil.WithCmpOpts(
					testutil.IgnoreMetaRuntimeFields(),
					testutil.IgnoreServiceRuntimeFields(),
					testutil.IgnoreDeploymentRuntimeFields(),
					testutil.IgnorePVCRuntimeFields(),

					testutil.IgnorePodSpecDefaults(),
					testutil.IgnoreProbeDefaults(),
					testutil.IgnoreDeploymentSpecDefaults(),
					testutil.IgnoreStatus(),
				),
				testutil.WithExtraResource(&multigresv1alpha1.Shard{}),
				testutil.WithExtraResource(&corev1.Pod{}),
				testutil.WithExtraResource(&corev1.PersistentVolumeClaim{}),
				testutil.WithTimeout(30*time.Second),
			)
			k8sClient := mgr.GetClient()

			// Mark pods as Ready in the background so the controller can
			// create subsequent replicas (it blocks when pods aren't Ready).
			go func() {
				ticker := time.NewTicker(200 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						podList := &corev1.PodList{}
						if err := k8sClient.List(
							ctx,
							podList,
							client.InNamespace(tc.shard.Namespace),
						); err != nil {
							continue
						}
						for i := range podList.Items {
							p := &podList.Items[i]
							ready := false
							for _, c := range p.Status.Conditions {
								if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
									ready = true
									break
								}
							}
							if !ready {
								p.Status.Phase = corev1.PodRunning
								p.Status.Conditions = []corev1.PodCondition{
									{Type: corev1.PodReady, Status: corev1.ConditionTrue},
								}
								_ = k8sClient.Status().Update(ctx, p)
							}
						}
					}
				}
			}()

			// 3. Setup and Start Controller
			shardReconciler := &shardcontroller.ShardReconciler{
				Client:   mgr.GetClient(),
				Scheme:   mgr.GetScheme(),
				Recorder: mgr.GetEventRecorderFor("shard-controller"),
			}
			ck.Require().NoError(shardReconciler.SetupWithManager(mgr, controller.Options{
				SkipNameValidation: ptr.To(true),
			}), "Failed to create controller")

			setTestPostgresPasswordSecretRef(tc.shard)
			createTestPostgresPasswordSecret(t, ctx, k8sClient, tc.shard.Namespace)
			if ref := tc.shard.Spec.PostgresInitSecretsRef; ref != nil {
				key := ref.Key
				if key == "" {
					key = shardcontroller.PostgresInitSecretsFileName
				}
				createTestPostgresInitSecretsSecret(
					t,
					ctx,
					k8sClient,
					tc.shard.Namespace,
					ref.Name,
					key,
				)
			}
			ck.Require().
				NoError(k8sClient.Create(ctx, tc.shard), "Failed to create the initial item")

			// Patch wantResources with hashed names
			for _, obj := range tc.wantResources {
				labels := obj.GetLabels()
				component := labels["app.kubernetes.io/component"]
				cellName := labels["multigres.com/cell"]
				clusterName := tc.shard.Labels["multigres.com/cluster"]

				if component == "multiorch" {
					// Deployment name uses DefaultConstraints
					hashedDeployName := nameutil.JoinWithConstraints(
						nameutil.DefaultConstraints,
						clusterName,
						string(tc.shard.Spec.DatabaseName),
						string(tc.shard.Spec.TableGroupName),
						string(tc.shard.Spec.ShardName),
						"multiorch",
						cellName,
					)
					// Service name uses ServiceConstraints
					hashedSvcName := nameutil.JoinWithConstraints(
						nameutil.ServiceConstraints,
						clusterName,
						string(tc.shard.Spec.DatabaseName),
						string(tc.shard.Spec.TableGroupName),
						string(tc.shard.Spec.ShardName),
						"multiorch",
						cellName,
					)

					labels["app.kubernetes.io/instance"] = clusterName // Instance is cluster name
					obj.SetLabels(labels)

					if deploy, ok := obj.(*appsv1.Deployment); ok {
						obj.SetName(hashedDeployName)
						deploy.Spec.Selector.MatchLabels["app.kubernetes.io/instance"] = clusterName
						deploy.Spec.Template.ObjectMeta.Labels["app.kubernetes.io/instance"] = clusterName
					}
					if svc, ok := obj.(*corev1.Service); ok {
						obj.SetName(hashedSvcName)
						svc.Spec.Selector["app.kubernetes.io/instance"] = clusterName
					}
				} else if component == "shard-pool" {
					poolName := "primary" // Hardcoded as per tests
					hashedSvcName := nameutil.JoinWithConstraints(
						nameutil.ServiceConstraints,
						clusterName,
						string(tc.shard.Spec.DatabaseName),
						string(tc.shard.Spec.TableGroupName),
						string(tc.shard.Spec.ShardName),
						"pool",
						poolName,
						cellName,
						"headless",
					)

					if svc, ok := obj.(*corev1.Service); ok {
						obj.SetName(hashedSvcName)
						labels["app.kubernetes.io/instance"] = clusterName
						obj.SetLabels(labels)
						svc.Spec.Selector["app.kubernetes.io/instance"] = clusterName
					}
				}

			}

			filteredResources := append([]client.Object{}, tc.wantResources...)

			// The controller stamps the rendered-config hashes on the shard, which
			// BuildPoolPod stamps onto each pod (the restart-hash also folds into
			// the spec-hash). Reproduce them here so the expected pods match the
			// ones the controller creates. These test shards have no
			// PostgresConfigRef, so the ref content is empty.
			_, hashes, err := shardcontroller.RenderPostgresConfig(tc.shard, "")
			ck.Require().NoError(err, "Failed to render postgres config hash")
			if tc.shard.Annotations == nil {
				tc.shard.Annotations = map[string]string{}
			}
			tc.shard.Annotations[metadata.AnnotationPostgresConfigHash] = hashes.RestartHash
			tc.shard.Annotations[metadata.AnnotationPostgresReloadHash] = hashes.ReloadHash

			// Append literal expected Pods and PVCs based on Shard Spec
			backupPVCAdded := false
			for poolName, poolSpec := range tc.shard.Spec.Pools {
				for _, cellName := range poolSpec.Cells {
					replicas := shardcontroller.DefaultPoolReplicas
					if poolSpec.ReplicasPerCell != nil {
						replicas = *poolSpec.ReplicasPerCell
					}
					for i := 0; i < int(replicas); i++ {
						pod, err := shardcontroller.BuildPoolPod(
							tc.shard,
							string(poolName),
							string(cellName),
							poolSpec,
							i,
							mgr.GetScheme(),
						)
						ck.Require().NoError(err, "Failed to build pod")
						filteredResources = append(filteredResources, pod)

						pvc, err := shardcontroller.BuildPoolDataPVC(
							tc.shard,
							string(poolName),
							string(cellName),
							poolSpec,
							i,
							shardcontroller.ShouldDeletePVCOnShardRemoval(tc.shard, poolSpec),
							mgr.GetScheme(),
						)
						ck.Require().NoError(err, "Failed to build pvc")
						filteredResources = append(filteredResources, pvc)
					}

					// Shared backup PVC is per-shard, not per-pod or per-cell.
					if tc.shard.Spec.Backup != nil &&
						tc.shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeFilesystem &&
						!backupPVCAdded {
						backupPVC, err := shardcontroller.BuildSharedBackupPVC(
							tc.shard,
							shardcontroller.ShouldDeleteShardLevelPVCOnRemoval(tc.shard),
							mgr.GetScheme(),
						)
						ck.Require().NoError(err, "Failed to build backup pvc")
						filteredResources = append(filteredResources, backupPVC)
						backupPVCAdded = true
					}
				}
			}

			ck.NoError(watcher.WaitForMatch(filteredResources...), "Resources mismatch:\n")
		})
	}
}

// Test helpers

// shardLabels returns standard labels for shard resources
func shardLabels(t testing.TB, instanceName, component, cell string) map[string]string {
	t.Helper()
	labels := map[string]string{
		"app.kubernetes.io/component": component,
		// In new logic, instance is cluster name.
		// Tests calling this MUST now pass the correct name (hashed name or cluster name depending on what we want to test).
		// But wait, the standard labels logic sets instance to CLUSTER NAME.
		// So checking "instanceName" arg here is tricky if the caller passes the RESOURCE name.
		// I will just hardcode "test-cluster" if instanceName matches legacy expectation, or update callers?
		// Better: update this helper to take clusterName AND resourceName?
		// Or just update the body:
		"app.kubernetes.io/instance":   "test-cluster",
		"app.kubernetes.io/managed-by": "multigres-operator",
		"app.kubernetes.io/name":       "multigres",
		"app.kubernetes.io/part-of":    "multigres",
		"multigres.com/cell":           cell,
		"multigres.com/cluster":        "test-cluster",
		"multigres.com/database":       "testdb",
		"multigres.com/tablegroup":     "default",
	}

	if component == "shard-pool" {
		labels["multigres.com/pool"] = "primary"
		labels["multigres.com/shard"] = "0"
	}

	return labels
}

// shardOwnerRefs returns owner references for a Shard resource
func shardOwnerRefs(t testing.TB, shardName string) []metav1.OwnerReference {
	t.Helper()
	return []metav1.OwnerReference{{
		APIVersion:         "multigres.com/v1alpha1",
		Kind:               "Shard",
		Name:               shardName,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}}
}

// tcpPort creates a simple TCP container port
func tcpPort(t testing.TB, name string, port int32) corev1.ContainerPort {
	t.Helper()
	return corev1.ContainerPort{Name: name, ContainerPort: port, Protocol: corev1.ProtocolTCP}
}

// tcpServicePort creates a TCP service port with named target
func tcpServicePort(t testing.TB, name string, port int32) corev1.ServicePort {
	t.Helper()
	return corev1.ServicePort{
		Name:       name,
		Port:       port,
		TargetPort: intstr.FromString(name),
		Protocol:   corev1.ProtocolTCP,
	}
}

// multipoolerPorts returns the standard multipooler container ports
func multipoolerPorts(t testing.TB) []corev1.ContainerPort {
	t.Helper()
	return []corev1.ContainerPort{
		tcpPort(t, "http", 15200),
		tcpPort(t, "grpc", 15270),
		tcpPort(t, "postgres", 5432),
	}
}

func TestReconcileDeletions(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)

	// Setup controller with manager
	c.NoError((&shardcontroller.ShardReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("shard-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}), "Failed to create controller")

	ctx := t.Context()
	k8sClient := mgr.GetClient()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard-deletion-reconcile",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "testdb",
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},

			TableGroupName: "default",
			ShardName:      "0",
			Multiorch: multigresv1alpha1.MultiorchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "ghcr.io/multigres/multigres:main",
				Multipooler: "ghcr.io/multigres/multigres:main",
				Postgres:    "postgres:17",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					Type:            "readWrite",
					ReplicasPerCell: ptr.To(int32(1)),
					Storage: multigresv1alpha1.StorageSpec{
						Size: "10Gi",
					},
				},
			},
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
					Path:    "/backups",
					Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
	}

	setTestPostgresPasswordSecretRef(shard)
	createTestPostgresPasswordSecret(t, ctx, k8sClient, shard.Namespace)
	c.NoError(k8sClient.Create(ctx, shard), "Failed to create Shard")

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardcontroller.PgHbaConfigMapName("test-shard-deletion-reconcile"),
			Namespace: "default",
		},
	}

	// 1. Wait for ConfigMap to be created initially
	// We use polling to avoid strict content matching on Data
	pollFound := false
	for i := 0; i < 20; i++ {
		err := k8sClient.Get(
			ctx,
			types.NamespacedName{
				Name:      shardcontroller.PgHbaConfigMapName("test-shard-deletion-reconcile"),
				Namespace: "default",
			},
			cm,
		)
		if err == nil {
			pollFound = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.True(pollFound, "ConfigMap not initially created")

	// 2. Delete ConfigMap
	// We need to fetch it first to get UID/ResourceVersion for proper deletion if needed,
	// strictly speaking not needed for k8s deletion by name if we construct it,
	// but better to be safe with client usage.
	c.NoError(k8sClient.Delete(ctx, cm), "Failed to delete ConfigMap")

	// 3. Wait for ConfigMap to be recreated
	// Since the controller watches ConfigMaps, the deletion event should trigger Reconcile.
	// Reconcile should recreate it.
	timeout := 10 * time.Second
	interval := 500 * time.Millisecond
	ctxWait, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	found := false
	for {
		select {
		case <-ctxWait.Done():
			t.Fatalf("Timed out waiting for ConfigMap to be recreated")
		default:
		}

		err := k8sClient.Get(
			ctx,
			types.NamespacedName{
				Name:      shardcontroller.PgHbaConfigMapName("test-shard-deletion-reconcile"),
				Namespace: "default",
			},
			cm,
		)
		if err == nil {
			found = true
			break
		}
		time.Sleep(interval)
	}

	c.True(found, "ConfigMap was not recreated")
}

func TestShardReconciliation_DanglingPostgresInitSecretsRef(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)

	c.Require().NoError((&shardcontroller.ShardReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("shard-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}), "Failed to create controller")

	ctx := t.Context()
	k8sClient := mgr.GetClient()

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dangling-init-secrets-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "testdb",
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},
			TableGroupName: "default",
			ShardName:      "0",
			PostgresInitSecretsRef: &multigresv1alpha1.PostgresInitSecretsRef{
				Name: "does-not-exist-init-secrets",
				Key:  shardcontroller.PostgresInitSecretsFileName,
			},
			Multiorch: multigresv1alpha1.MultiorchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "ghcr.io/multigres/multigres:main",
				Multipooler: "ghcr.io/multigres/multigres:main",
				Postgres:    "postgres:17",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					Type:            "readWrite",
					ReplicasPerCell: ptr.To(int32(1)),
					Storage: multigresv1alpha1.StorageSpec{
						Size: "1Gi",
					},
				},
			},
		},
	}

	setTestPostgresPasswordSecretRef(shard)
	createTestPostgresPasswordSecret(t, ctx, k8sClient, shard.Namespace)
	c.Require().NoError(k8sClient.Create(ctx, shard), "Failed to create Shard")

	c.Require().EventuallyTrue(10*time.Second, 100*time.Millisecond, func() bool {
		events := &corev1.EventList{}
		if err := k8sClient.List(ctx, events, client.InNamespace(shard.Namespace)); err != nil {
			return false
		}
		for _, event := range events.Items {
			if event.Type == corev1.EventTypeWarning &&
				event.Reason == "ConfigError" &&
				event.InvolvedObject.UID == shard.UID {
				return true
			}
		}
		return false
	}, "expected a ConfigError event for the dangling init-secrets reference")

	podList := &corev1.PodList{}
	c.Require().NoError(k8sClient.List(ctx, podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels{
			"multigres.com/cluster": "test-cluster",
			"multigres.com/shard":   "0",
		},
	), "Failed to list pods")
	c.Empty(
		podList.Items,
		"expected no pool pods for shard with dangling PostgresInitSecretsRef, got %d",
		len(podList.Items),
	)
}

// TestReloadVsRestartRollout drives a real reconcile against envtest and proves
// the hash split at the pod level: a reload-safe change (work_mem, user context)
// updates the rendered ConfigMap but leaves the pod's UID and spec-hash
// untouched (no recreation), while a restart change (shared_buffers, postmaster
// context) moves the spec-hash and makes the controller initiate a drain — the
// recreation trigger.
//
// Topology and Status responses model a committed Multigres cohort. No actual
// PostgreSQL processes run, so applying SIGHUP remains an e2e concern.
func TestReloadVsRestartRollout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		replicas      int32
		missingStatus bool
		blockedReason string
	}{
		{name: "healthy cohort", replicas: 3},
		{name: "missing status", replicas: 3, missingStatus: true, blockedReason: "no matching consensus identity"},
		{name: "insufficient quorum", replicas: 2, blockedReason: "remaining cohort cannot satisfy durability"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testReloadVsRestartRollout(t, tc.replicas, tc.missingStatus, tc.blockedReason)
		})
	}
}

func testReloadVsRestartRollout(
	t *testing.T,
	replicas int32,
	missingStatus bool,
	blockedReason string,
) {
	t.Helper()
	c := assert.NewAborting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	ctx := t.Context()
	// Test writes use fresh resource versions while the reconciler exercises
	// its normal cache. Data-plane observations also update Shard status.
	k8sClient := testutil.SetUpClient(t, mgr.GetConfig(), scheme)

	const (
		shardName   = "test-shard-reload-rollout"
		clusterName = "test-cluster-reload-rollout"
	)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardName,
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": clusterName},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0",
			Replicas:       ptr.To(replicas),
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},
			Multiorch: multigresv1alpha1.MultiorchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "ghcr.io/multigres/multigres:main",
				Multipooler: "ghcr.io/multigres/multigres:main",
				Postgres:    "postgres:17",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "global-topo:2379", RootPath: "/multigres/global", Implementation: "etcd",
			},
			PostgresConfig: map[string]string{"work_mem": "4MB"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					Type:            "readWrite",
					ReplicasPerCell: ptr.To(replicas),
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
					Path:    "/backups",
					Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
	}

	rpc, topoFactory, registerPoolers := rolloutDataPlane(t, shard, replicas, missingStatus)
	recorder := &rolloutEventRecorder{
		EventRecorder: mgr.GetEventRecorderFor("shard-controller"),
		blocked:       make(chan string, 1),
		started:       make(chan string, 1),
	}
	c.NoError((&shardcontroller.ShardReconciler{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		Scheme:          mgr.GetScheme(),
		Recorder:        recorder,
		PoolerClients:   poolerclient.Static(rpc),
		CreateTopoStore: topoFactory,
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}), "Failed to create controller")

	setTestPostgresPasswordSecretRef(shard)
	createTestPostgresPasswordSecret(t, ctx, k8sClient, shard.Namespace)
	c.NoError(k8sClient.Create(ctx, shard), "Failed to create Shard")

	// Keep pool pods Ready in the background so the controller does not block.
	go markPoolPodsReady(ctx, k8sClient, clusterName)

	poolSelector := client.MatchingLabels{
		metadata.LabelMultigresCluster: clusterName,
		metadata.LabelAppComponent:     shardcontroller.PoolComponentName,
	}

	// Wait for every desired member before testing either configuration change.
	var original corev1.PodList
	c.EventuallyTrue(30*time.Second, 200*time.Millisecond, func() bool {
		if err := mgr.GetClient().List(
			ctx,
			&original,
			client.InNamespace("default"),
			poolSelector,
		); err != nil ||
			len(original.Items) != int(replicas) {
			return false
		}
		for _, pod := range original.Items {
			if pod.Annotations[metadata.AnnotationSpecHash] == "" || !podReadyForRolloutTest(&pod) {
				return false
			}
		}
		return true
	}, "wait for the complete ready cohort")
	// Multipoolers register only after their pods exist. Registering before
	// the controller cache sees them lets dead-pooler cleanup mark them shut down.
	registerPoolers()

	// --- Reload-only change: work_mem (user context) must NOT recreate the pod. ---
	setInlineConfig(t, ctx, k8sClient, shardName, "work_mem", "8MB")
	// Wait until the rendered ConfigMap reflects the change (reconcile processed it).
	waitForConfigMapContains(t, ctx, k8sClient, shardName, "work_mem = '8MB'")
	// Reaching the reload RPC proves the controller processed the pool rollout
	// decision, not merely the earlier ConfigMap write.
	c.EventuallyTrue(30*time.Second, 100*time.Millisecond, func() bool {
		for _, call := range rpc.GetCallLog() {
			if strings.HasPrefix(call, "ReloadConfig") {
				return true
			}
		}
		return false
	}, "reload-only change did not reach ReloadConfig")

	assertUnchanged := func() {
		t.Helper()
		for _, orig := range original.Items {
			var got corev1.Pod
			c.NoError(mgr.GetAPIReader().Get(ctx, client.ObjectKeyFromObject(&orig), &got))
			c.EqDeep(orig.UID, got.UID, "pod %s was recreated", orig.Name)
			c.Empty(got.Annotations[metadata.AnnotationDrainState], "pod %s was drained", orig.Name)
			c.EqDeep(
				orig.Annotations[metadata.AnnotationSpecHash],
				got.Annotations[metadata.AnnotationSpecHash],
			)
		}
	}
	assertUnchanged()

	// --- Restart change: shared_buffers (postmaster context) must trigger recreation. ---
	setInlineConfig(t, ctx, k8sClient, shardName, "shared_buffers", "256MB")
	if blockedReason != "" {
		// Observe an actual failed preflight, not merely the absence of a drain
		// before the controller processes the config update.
		c.EventuallyTrue(40*time.Second, 200*time.Millisecond, func() bool {
			select {
			case message := <-recorder.blocked:
				return strings.Contains(message, blockedReason)
			default:
				return false
			}
		}, "did not observe a blocked disruption: %s", blockedReason)
		assertUnchanged()
		return
	}
	// A drain can advance through all annotations between polling intervals in
	// envtest. Observe its initiation directly instead of racing pod deletion.
	c.EventuallyTrue(40*time.Second, 100*time.Millisecond, func() bool {
		select {
		case message := <-recorder.started:
			return strings.Contains(message, "Initiated drain for drifted replica pod")
		default:
			return false
		}
	}, "restart change did not initiate a replica drain")
	var pods corev1.PodList
	c.NoError(k8sClient.List(ctx, &pods, client.InNamespace(shard.Namespace), poolSelector))
	draining := 0
	for _, pod := range pods.Items {
		if pod.Annotations[metadata.AnnotationDrainState] != "" {
			draining++
		}
	}
	c.LessOrEqual(1, draining, "restart initiated overlapping drains")
}

// Observe preflight events without depending on the API event broadcaster's
// rate limit, and without blocking the controller when nobody is listening.
type rolloutEventRecorder struct {
	record.EventRecorder
	blocked chan string
	started chan string
}

func (r *rolloutEventRecorder) Eventf(
	object runtime.Object,
	eventType, reason, messageFmt string,
	args ...interface{},
) {
	if reason == "DisruptionBlocked" {
		select {
		case r.blocked <- fmt.Sprintf(messageFmt, args...):
		default:
		}
	}
	if reason == "PodUpdated" {
		select {
		case r.started <- fmt.Sprintf(messageFmt, args...):
		default:
		}
	}
	r.EventRecorder.Eventf(object, eventType, reason, messageFmt, args...)
}

// rolloutDataPlane registers the same members the controller will create and
// supplies their committed rule, eligibility, PostgreSQL role and connections.
// Healthy leadership status is intentionally omitted, matching Multigres.
func rolloutDataPlane(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	replicas int32,
	missingStatus bool,
) (*rpcclient.FakeClient, func(*multigresv1alpha1.Shard) (topoclient.Store, error), func()) {
	t.Helper()
	c := assert.NewAborting(t)
	store, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { c.NoError(store.Close()) })
	ids := make([]*cm.ID, replicas)
	for i := range ids {
		ids[i] = &cm.ID{
			Cell: "zone1",
			Name: shardcontroller.BuildPoolPodName(shard, "primary", "zone1", i),
		}
	}
	leader := ids[len(ids)-1]
	rule := &cm.ShardRule{
		RuleNumber:       &cm.RuleNumber{CoordinatorTerm: 2},
		LeaderId:         leader,
		CohortMembers:    ids,
		DurabilityPolicy: topoclient.AtLeastN(2),
	}
	rpc := rpcclient.NewFakeClient()
	var poolers []*cm.Multipooler
	for _, id := range ids {
		role := cm.RoutingRole_ROUTING_ROLE_REPLICA
		response := &md.StatusResponse{
			Status: &md.Status{
				IsInitialized:  true,
				PostgresReady:  true,
				PostgresStatus: md.PostgresStatus_POSTGRES_STATUS_STANDBY,
			},
			AvailabilityStatus: &cm.AvailabilityStatus{
				CohortEligibilityStatus: &cm.CohortEligibilityStatus{
					Signal: cm.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
			ConsensusStatus: &cm.ConsensusStatus{
				Id:              id,
				CurrentPosition: &cm.PoolerPosition{Position: &cm.RulePosition{Decision: rule}},
			},
		}
		if id == leader {
			role = cm.RoutingRole_ROUTING_ROLE_PRIMARY
			response.Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_PRIMARY
			response.Status.PrimaryStatus = &md.PrimaryStatus{
				Ready:              true,
				ConnectedFollowers: ids[:len(ids)-1],
			}
		}
		poolers = append(poolers, &cm.Multipooler{
			Id:       id,
			Hostname: id.Name,
			ShardKey: &cm.ShardKey{
				Database:   string(shard.Spec.DatabaseName),
				TableGroup: string(shard.Spec.TableGroupName),
				Shard:      string(shard.Spec.ShardName),
			},
			RoutingState: &cm.RoutingState{Role: role},
		})
		// Missing primary Status must block every possible removal, including
		// when Kubernetes still reports the pods Ready.
		if !missingStatus || id != leader {
			rpc.SetStatusResponse(topoclient.ComponentIDString(id), response)
		}
	}
	return rpc, func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return topoclient.NewWithFactory(
				factory,
				"",
				[]string{""},
				topoclient.NewDefaultTopoConfig(),
			), nil
		}, func() {
			for _, pooler := range poolers {
				c.NoError(store.RegisterMultipooler(t.Context(), pooler, false))
			}
		}
}

func podReadyForRolloutTest(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// markPoolPodsReady keeps every pool pod for the cluster marked Ready so the
// reconciler (which blocks on readiness before acting on further replicas) can
// make progress under envtest, where no kubelet runs.
func markPoolPodsReady(ctx context.Context, c client.Client, clusterName string) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			list := &corev1.PodList{}
			if err := c.List(ctx, list, client.InNamespace("default"),
				client.MatchingLabels{metadata.LabelMultigresCluster: clusterName}); err != nil {
				continue
			}
			for i := range list.Items {
				p := &list.Items[i]
				if !podReadyForRolloutTest(p) {
					p.Status.Phase = corev1.PodRunning
					found := false
					for j := range p.Status.Conditions {
						if p.Status.Conditions[j].Type == corev1.PodReady {
							p.Status.Conditions[j].Status = corev1.ConditionTrue
							found = true
						}
					}
					if !found {
						p.Status.Conditions = append(
							p.Status.Conditions,
							corev1.PodCondition{
								Type:   corev1.PodReady,
								Status: corev1.ConditionTrue,
							},
						)
					}
					_ = c.Status().Update(ctx, p)
				}
			}
		}
	}
}

func setInlineConfig(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	shardName, key, val string,
) {
	t.Helper()
	assert.NewAborting(t).NoError(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		s := &multigresv1alpha1.Shard{}
		if err := c.Get(
			ctx,
			types.NamespacedName{Name: shardName, Namespace: "default"},
			s,
		); err != nil {
			return err
		}
		base := s.DeepCopy()
		if s.Spec.PostgresConfig == nil {
			s.Spec.PostgresConfig = map[string]string{}
		}
		s.Spec.PostgresConfig[key] = val
		return c.Patch(ctx, s, client.MergeFrom(base))
	}), "update shard inline config %s=%s", key, val)
}

func waitForConfigMapContains(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	shardName, want string,
) {
	t.Helper()
	name := shardcontroller.PostgresConfigMapName(shardName)
	for range 200 {
		cm := &corev1.ConfigMap{}
		if err := c.Get(
			ctx,
			types.NamespacedName{Name: name, Namespace: "default"},
			cm,
		); err == nil {
			if strings.Contains(cm.Data[shardcontroller.PostgresConfigMapKey], want) {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for ConfigMap %s to contain %q", name, want)
}
