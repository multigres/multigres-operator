//go:build integration
// +build integration

package shard_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
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

	if err := (&shardcontroller.ShardReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("shard-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}); err != nil {
		t.Fatalf("Failed to create controller, %v", err)
	}
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
						MultiOrch:   "ghcr.io/multigres/multigres:main",
						MultiPooler: "ghcr.io/multigres/multigres:main",
						Postgres:    "postgres:17",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
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
						Type:       multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/backups", Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"}},
					},
				},
			},
			wantResources: []client.Object{
				// MultiOrch Deployment for zone-a
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-shard-multiorch-zone-a",
						Namespace:       "default",
						Labels:          shardLabels(t, "test-shard-multiorch-zone-a", "multiorch", "zone-a"),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(shardLabels(t, "test-shard-multiorch-zone-a", "multiorch", "zone-a")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(t, "test-shard-multiorch-zone-a", "multiorch", "zone-a"),
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
				// MultiOrch Service for zone-a
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-shard-multiorch-zone-a",
						Namespace:       "default",
						Labels:          shardLabels(t, "test-shard-multiorch-zone-a", "multiorch", "zone-a"),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(shardLabels(t, "test-shard-multiorch-zone-a", "multiorch", "zone-a")),
					},
				},
				// MultiOrch Deployment for zone-b
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-shard-multiorch-zone-b",
						Namespace:       "default",
						Labels:          shardLabels(t, "test-shard-multiorch-zone-b", "multiorch", "zone-b"),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(shardLabels(t, "test-shard-multiorch-zone-b", "multiorch", "zone-b")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(t, "test-shard-multiorch-zone-b", "multiorch", "zone-b"),
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
				// MultiOrch Service for zone-b
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-shard-multiorch-zone-b",
						Namespace:       "default",
						Labels:          shardLabels(t, "test-shard-multiorch-zone-b", "multiorch", "zone-b"),
						OwnerReferences: shardOwnerRefs(t, "test-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(shardLabels(t, "test-shard-multiorch-zone-b", "multiorch", "zone-b")),
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-shard-pool-primary-zone-a-headless",
						Namespace:       "default",
						Labels:          shardLabels(t, "test-shard-pool-primary-zone-a", "shard-pool", "zone-a"),
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
						Selector:                 metadata.GetSelectorLabels(shardLabels(t, "test-shard-pool-primary-zone-a", "shard-pool", "zone-a")),
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
						MultiOrch:   "ghcr.io/multigres/multigres:main",
						MultiPooler: "ghcr.io/multigres/multigres:main",
						Postgres:    "postgres:17",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
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
						Type:       multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/backups", Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"}},
					},
				},
			},
			wantResources: []client.Object{
				// MultiOrch Deployment for zone-a
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "delete-policy-shard-multiorch-zone-a",
						Namespace:       "default",
						Labels:          shardLabels(t, "delete-policy-shard-multiorch-zone-a", "multiorch", "zone-a"),
						OwnerReferences: shardOwnerRefs(t, "delete-policy-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(shardLabels(t, "delete-policy-shard-multiorch-zone-a", "multiorch", "zone-a")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(t, "delete-policy-shard-multiorch-zone-a", "multiorch", "zone-a"),
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
				// MultiOrch Service for zone-a
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "delete-policy-shard-multiorch-zone-a",
						Namespace:       "default",
						Labels:          shardLabels(t, "delete-policy-shard-multiorch-zone-a", "multiorch", "zone-a"),
						OwnerReferences: shardOwnerRefs(t, "delete-policy-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(shardLabels(t, "delete-policy-shard-multiorch-zone-a", "multiorch", "zone-a")),
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "delete-policy-shard-pool-primary-zone-a-headless",
						Namespace:       "default",
						Labels:          shardLabels(t, "delete-policy-shard-pool-primary-zone-a", "shard-pool", "zone-a"),
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
						Selector:                 metadata.GetSelectorLabels(shardLabels(t, "delete-policy-shard-pool-primary-zone-a", "shard-pool", "zone-a")),
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
						MultiOrch:   "ghcr.io/multigres/multigres:main",
						MultiPooler: "ghcr.io/multigres/multigres:main",
						Postgres:    "postgres:17",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
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
						Type:       multigresv1alpha1.BackupTypeFilesystem,
						Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/backups", Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"}},
					},
				},
			},
			wantResources: []client.Object{
				// MultiOrch Deployment for zone1
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "multi-cell-shard-multiorch-zone1",
						Namespace:       "default",
						Labels:          shardLabels(t, "multi-cell-shard-multiorch-zone1", "multiorch", "zone1"),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(shardLabels(t, "multi-cell-shard-multiorch-zone1", "multiorch", "zone1")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(t, "multi-cell-shard-multiorch-zone1", "multiorch", "zone1"),
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
				// MultiOrch Service for zone1
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "multi-cell-shard-multiorch-zone1",
						Namespace:       "default",
						Labels:          shardLabels(t, "multi-cell-shard-multiorch-zone1", "multiorch", "zone1"),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(shardLabels(t, "multi-cell-shard-multiorch-zone1", "multiorch", "zone1")),
					},
				},
				// MultiOrch Deployment for zone2
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "multi-cell-shard-multiorch-zone2",
						Namespace:       "default",
						Labels:          shardLabels(t, "multi-cell-shard-multiorch-zone2", "multiorch", "zone2"),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(shardLabels(t, "multi-cell-shard-multiorch-zone2", "multiorch", "zone2")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: shardLabels(t, "multi-cell-shard-multiorch-zone2", "multiorch", "zone2"),
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
				// MultiOrch Service for zone2
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "multi-cell-shard-multiorch-zone2",
						Namespace:       "default",
						Labels:          shardLabels(t, "multi-cell-shard-multiorch-zone2", "multiorch", "zone2"),
						OwnerReferences: shardOwnerRefs(t, "multi-cell-shard"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15300),
							tcpServicePort(t, "grpc", 15370),
						},
						Selector: metadata.GetSelectorLabels(shardLabels(t, "multi-cell-shard-multiorch-zone2", "multiorch", "zone2")),
					},
				},
				// Headless Service for zone1
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "multi-cell-shard-pool-primary-zone1-headless",
						Namespace:       "default",
						Labels:          shardLabels(t, "multi-cell-shard-pool-primary-zone1", "shard-pool", "zone1"),
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
						Selector:                 metadata.GetSelectorLabels(shardLabels(t, "multi-cell-shard-pool-primary-zone1", "shard-pool", "zone1")),
						PublishNotReadyAddresses: true,
					},
				},
				// Headless Service for zone2
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "multi-cell-shard-pool-primary-zone2-headless",
						Namespace:       "default",
						Labels:          shardLabels(t, "multi-cell-shard-pool-primary-zone2", "shard-pool", "zone2"),
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
						Selector:                 metadata.GetSelectorLabels(shardLabels(t, "multi-cell-shard-pool-primary-zone2", "shard-pool", "zone2")),
						PublishNotReadyAddresses: true,
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
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
						if err := k8sClient.List(ctx, podList, client.InNamespace(tc.shard.Namespace)); err != nil {
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
			if err := shardReconciler.SetupWithManager(mgr, controller.Options{
				// Needed for the parallel test runs
				SkipNameValidation: ptr.To(true),
			}); err != nil {
				t.Fatalf("Failed to create controller, %v", err)
			}

			if err := k8sClient.Create(ctx, tc.shard); err != nil {
				t.Fatalf("Failed to create the initial item, %v", err)
			}

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

			// Append literal expected Pods and PVCs based on Shard Spec
			backupCells := map[string]bool{}
			for poolName, poolSpec := range tc.shard.Spec.Pools {
				for _, cellName := range poolSpec.Cells {
					replicas := shardcontroller.DefaultPoolReplicas
					if poolSpec.ReplicasPerCell != nil {
						replicas = *poolSpec.ReplicasPerCell
					}
					for i := 0; i < int(replicas); i++ {
						pod, err := shardcontroller.BuildPoolPod(tc.shard, string(poolName), string(cellName), poolSpec, i, mgr.GetScheme())
						if err != nil {
							t.Fatalf("Failed to build pod: %v", err)
						}
						filteredResources = append(filteredResources, pod)

						pvc, err := shardcontroller.BuildPoolDataPVC(tc.shard, string(poolName), string(cellName), poolSpec, i, shardcontroller.ShouldDeletePVCOnShardRemoval(tc.shard, poolSpec), mgr.GetScheme())
						if err != nil {
							t.Fatalf("Failed to build pvc: %v", err)
						}
						filteredResources = append(filteredResources, pvc)
					}

					// Shared backup PVC is per-cell, not per-pod
					if tc.shard.Spec.Backup != nil && tc.shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeFilesystem && !backupCells[string(cellName)] {
						backupPVC, err := shardcontroller.BuildSharedBackupPVC(tc.shard, string(cellName), shardcontroller.ShouldDeleteShardLevelPVCOnRemoval(tc.shard), mgr.GetScheme())
						if err != nil {
							t.Fatalf("Failed to build backup pvc: %v", err)
						}
						filteredResources = append(filteredResources, backupPVC)
						backupCells[string(cellName)] = true
					}
				}
			}

			if err := watcher.WaitForMatch(filteredResources...); err != nil {
				t.Errorf("Resources mismatch:\n%v", err)
			}
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
	return corev1.ServicePort{Name: name, Port: port, TargetPort: intstr.FromString(name), Protocol: corev1.ProtocolTCP}
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
	if err := (&shardcontroller.ShardReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("shard-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}); err != nil {
		t.Fatalf("Failed to create controller, %v", err)
	}

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
			MultiOrch: multigresv1alpha1.MultiOrchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			Images: multigresv1alpha1.ShardImages{
				MultiOrch:   "ghcr.io/multigres/multigres:main",
				MultiPooler: "ghcr.io/multigres/multigres:main",
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
				Type:       multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/backups", Storage: multigresv1alpha1.StorageSpec{Size: "10Gi"}},
			},
		},
	}

	if err := k8sClient.Create(ctx, shard); err != nil {
		t.Fatalf("Failed to create Shard: %v", err)
	}

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
		err := k8sClient.Get(ctx, types.NamespacedName{Name: shardcontroller.PgHbaConfigMapName("test-shard-deletion-reconcile"), Namespace: "default"}, cm)
		if err == nil {
			pollFound = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !pollFound {
		t.Fatalf("ConfigMap not initially created")
	}

	// 2. Delete ConfigMap
	// We need to fetch it first to get UID/ResourceVersion for proper deletion if needed,
	// strictly speaking not needed for k8s deletion by name if we construct it,
	// but better to be safe with client usage.
	if err := k8sClient.Delete(ctx, cm); err != nil {
		t.Fatalf("Failed to delete ConfigMap: %v", err)
	}

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

		err := k8sClient.Get(ctx, types.NamespacedName{Name: shardcontroller.PgHbaConfigMapName("test-shard-deletion-reconcile"), Namespace: "default"}, cm)
		if err == nil {
			found = true
			break
		}
		time.Sleep(interval)
	}

	if !found {
		t.Fatalf("ConfigMap was not recreated")
	}
}
