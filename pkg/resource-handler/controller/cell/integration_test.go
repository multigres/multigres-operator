//go:build integration
// +build integration

package cell_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/stretchr/testify/require"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/features"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
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

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)

	if err := (&cellcontroller.CellReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}); err != nil {
		t.Fatalf("Failed to create controller, %v", err)
	}
}

func TestCellReconciliation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		cell            *multigresv1alpha1.Cell
		existingObjects []client.Object
		wantResources   []client.Object
		wantErr         bool
		assertFunc      func(t *testing.T, c client.Client, cell *multigresv1alpha1.Cell)
	}{
		"simple cell with default replicas": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},

					ZoneID: "usw1-az1",
					Images: multigresv1alpha1.CellImages{
						Multigateway: "ghcr.io/multigres/multigres:main",
					},
					Multigateway: multigresv1alpha1.MultigatewaySpec{StatelessSpec: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(2)),
					}},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			},
			wantResources: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-cell-multigateway",
						Namespace:       "default",
						Labels:          cellLabels(t, "test-cell-multigateway", "multigateway", "zone1", "usw1-az1"),
						OwnerReferences: cellOwnerRefs(t, "test-cell"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(cellLabels(t, "test-cell-multigateway", "multigateway", "zone1", "usw1-az1")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: cellLabels(t, "test-cell-multigateway", "multigateway", "zone1", "usw1-az1"),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multigateway",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multigateway",
											"--http-port", "15100",
											"--grpc-port", "15170",
											"--pg-port", "5432",
											"--pg-replica-port", "5433",
											"--topo-global-server-addresses", "global-topo:2379",
											"--topo-global-root", "/multigres/global",
											"--cell", "zone1",
											"--log-level", "info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15100),
											tcpPort(t, "grpc", 15170),
											tcpPort(t, "postgres", 5432),
											tcpPort(t, "pg-replica", 5433),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
								NodeSelector: map[string]string{
									"topology.k8s.aws/zone-id": "usw1-az1",
								},
							},
						},
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-cell-multigateway",
						Namespace:       "default",
						Labels:          cellLabels(t, "test-cell-multigateway", "multigateway", "zone1", "usw1-az1"),
						OwnerReferences: cellOwnerRefs(t, "test-cell"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15100),
							tcpServicePort(t, "grpc", 15170),
							tcpServicePort(t, "postgres", 5432),
						},
						Selector: metadata.GetSelectorLabels(cellLabels(t, "test-cell-multigateway", "multigateway", "zone1", "usw1-az1")),
					},
				},
			},
		},
		"cell with custom replicas": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "custom-replicas-cell",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone2",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},

					ZoneID: "usw1-az2",
					Images: multigresv1alpha1.CellImages{
						Multigateway: "ghcr.io/multigres/multigres:main",
					},
					Multigateway: multigresv1alpha1.MultigatewaySpec{StatelessSpec: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(3)),
					}},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			},
			wantResources: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "custom-replicas-cell-multigateway",
						Namespace:       "default",
						Labels:          cellLabels(t, "custom-replicas-cell-multigateway", "multigateway", "zone2", "usw1-az2"),
						OwnerReferences: cellOwnerRefs(t, "custom-replicas-cell"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(3)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(cellLabels(t, "custom-replicas-cell-multigateway", "multigateway", "zone2", "usw1-az2")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: cellLabels(t, "custom-replicas-cell-multigateway", "multigateway", "zone2", "usw1-az2"),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multigateway",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multigateway",
											"--http-port", "15100",
											"--grpc-port", "15170",
											"--pg-port", "5432",
											"--pg-replica-port", "5433",
											"--topo-global-server-addresses", "global-topo:2379",
											"--topo-global-root", "/multigres/global",
											"--cell", "zone2",
											"--log-level", "info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15100),
											tcpPort(t, "grpc", 15170),
											tcpPort(t, "postgres", 5432),
											tcpPort(t, "pg-replica", 5433),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
								NodeSelector: map[string]string{
									"topology.k8s.aws/zone-id": "usw1-az2",
								},
							},
						},
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "custom-replicas-cell-multigateway",
						Namespace:       "default",
						Labels:          cellLabels(t, "custom-replicas-cell-multigateway", "multigateway", "zone2", "usw1-az2"),
						OwnerReferences: cellOwnerRefs(t, "custom-replicas-cell"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15100),
							tcpServicePort(t, "grpc", 15170),
							tcpServicePort(t, "postgres", 5432),
						},
						Selector: metadata.GetSelectorLabels(cellLabels(t, "custom-replicas-cell-multigateway", "multigateway", "zone2", "usw1-az2")),
					},
				},
			},
		},
		"cell with custom images": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "custom-images-cell",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone3",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},

					ZoneID: "usw1-az3",
					Images: multigresv1alpha1.CellImages{
						Multigateway: "custom/multigateway:v1.0.0",
					},
					Multigateway: multigresv1alpha1.MultigatewaySpec{StatelessSpec: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(2)),
					}},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			},
			wantResources: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "custom-images-cell-multigateway",
						Namespace:       "default",
						Labels:          cellLabels(t, "custom-images-cell-multigateway", "multigateway", "zone3", "usw1-az3"),
						OwnerReferences: cellOwnerRefs(t, "custom-images-cell"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(cellLabels(t, "custom-images-cell-multigateway", "multigateway", "zone3", "usw1-az3")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: cellLabels(t, "custom-images-cell-multigateway", "multigateway", "zone3", "usw1-az3"),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multigateway",
										Image: "custom/multigateway:v1.0.0",
										Args: []string{
											"multigateway",
											"--http-port", "15100",
											"--grpc-port", "15170",
											"--pg-port", "5432",
											"--pg-replica-port", "5433",
											"--topo-global-server-addresses", "global-topo:2379",
											"--topo-global-root", "/multigres/global",
											"--cell", "zone3",
											"--log-level", "info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15100),
											tcpPort(t, "grpc", 15170),
											tcpPort(t, "postgres", 5432),
											tcpPort(t, "pg-replica", 5433),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
								NodeSelector: map[string]string{
									"topology.k8s.aws/zone-id": "usw1-az3",
								},
							},
						},
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "custom-images-cell-multigateway",
						Namespace:       "default",
						Labels:          cellLabels(t, "custom-images-cell-multigateway", "multigateway", "zone3", "usw1-az3"),
						OwnerReferences: cellOwnerRefs(t, "custom-images-cell"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15100),
							tcpServicePort(t, "grpc", 15170),
							tcpServicePort(t, "postgres", 5432),
						},
						Selector: metadata.GetSelectorLabels(cellLabels(t, "custom-images-cell-multigateway", "multigateway", "zone3", "usw1-az3")),
					},
				},
			},
		},
		"cell with affinity": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "affinity-cell",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone4",
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},

					ZoneID: "usw1-az4",
					Images: multigresv1alpha1.CellImages{
						Multigateway: "ghcr.io/multigres/multigres:main",
					},
					Multigateway: multigresv1alpha1.MultigatewaySpec{StatelessSpec: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(2)),
						Affinity: &corev1.Affinity{
							NodeAffinity: &corev1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
									NodeSelectorTerms: []corev1.NodeSelectorTerm{
										{
											MatchExpressions: []corev1.NodeSelectorRequirement{
												{
													Key:      "node-type",
													Operator: corev1.NodeSelectorOpIn,
													Values:   []string{"gateway"},
												},
											},
										},
									},
								},
							},
						},
					}},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{},
					},
				},
			},
			wantResources: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "affinity-cell-multigateway",
						Namespace:       "default",
						Labels:          cellLabels(t, "affinity-cell-multigateway", "multigateway", "zone4", "usw1-az4"),
						OwnerReferences: cellOwnerRefs(t, "affinity-cell"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(cellLabels(t, "affinity-cell-multigateway", "multigateway", "zone4", "usw1-az4")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: cellLabels(t, "affinity-cell-multigateway", "multigateway", "zone4", "usw1-az4"),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multigateway",
										Image: "ghcr.io/multigres/multigres:main",
										Args: []string{
											"multigateway",
											"--http-port", "15100",
											"--grpc-port", "15170",
											"--pg-port", "5432",
											"--pg-replica-port", "5433",
											"--topo-global-server-addresses", "global-topo:2379",
											"--topo-global-root", "/multigres/global",
											"--cell", "zone4",
											"--log-level", "info",
										},
										Ports: []corev1.ContainerPort{
											tcpPort(t, "http", 15100),
											tcpPort(t, "grpc", 15170),
											tcpPort(t, "postgres", 5432),
											tcpPort(t, "pg-replica", 5433),
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds:    5,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/live",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds: 10,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/ready",
													Port: intstr.FromInt32(15100),
												},
											},
											PeriodSeconds: 5,
										},
									},
								},
								Affinity: &corev1.Affinity{
									NodeAffinity: &corev1.NodeAffinity{
										RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
											NodeSelectorTerms: []corev1.NodeSelectorTerm{
												{
													MatchExpressions: []corev1.NodeSelectorRequirement{
														{
															Key:      "node-type",
															Operator: corev1.NodeSelectorOpIn,
															Values:   []string{"gateway"},
														},
													},
												},
											},
										},
									},
								},
								NodeSelector: map[string]string{
									"topology.k8s.aws/zone-id": "usw1-az4",
								},
							},
						},
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "affinity-cell-multigateway",
						Namespace:       "default",
						Labels:          cellLabels(t, "affinity-cell-multigateway", "multigateway", "zone4", "usw1-az4"),
						OwnerReferences: cellOwnerRefs(t, "affinity-cell"),
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "http", 15100),
							tcpServicePort(t, "grpc", 15170),
							tcpServicePort(t, "postgres", 5432),
						},
						Selector: metadata.GetSelectorLabels(cellLabels(t, "affinity-cell-multigateway", "multigateway", "zone4", "usw1-az4")),
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
					testutil.IgnorePodSpecDefaults(),
					testutil.IgnoreProbeDefaults(),
					testutil.IgnoreDeploymentSpecDefaults(),
				),
				testutil.WithExtraResource(&multigresv1alpha1.Cell{}),
			)
			client := mgr.GetClient()

			cellReconciler := &cellcontroller.CellReconciler{
				Client:   mgr.GetClient(),
				Scheme:   mgr.GetScheme(),
				Recorder: mgr.GetEventRecorderFor("cell-controller"),
			}
			if err := cellReconciler.SetupWithManager(mgr, controller.Options{
				// Needed for the parallel test runs
				SkipNameValidation: ptr.To(true),
			}); err != nil {
				t.Fatalf("Failed to create controller, %v", err)
			}

			if err := client.Create(ctx, tc.cell); err != nil {
				t.Fatalf("Failed to create the initial item, %v", err)
			}
			markManagedLocalTopoServerHealthy(t, ctx, client, tc.cell)

			// Patch wantResources with hashed names
			for _, obj := range tc.wantResources {
				clusterName := tc.cell.Labels["multigres.com/cluster"]

				// Replicate controller logic for naming
				hashedDeployName := nameutil.JoinWithConstraints(
					nameutil.DefaultConstraints,
					clusterName,
					string(tc.cell.Spec.Name),
					"multigateway",
				)
				hashedSvcName := nameutil.JoinWithConstraints(
					nameutil.ServiceConstraints,
					clusterName,
					string(tc.cell.Spec.Name),
					"multigateway",
				)

				// Update labels
				labels := obj.GetLabels()
				if labels != nil {
					labels["app.kubernetes.io/instance"] = clusterName // Instance is cluster name
					obj.SetLabels(labels)
				}

				// Update selector if Deployment
				if deploy, ok := obj.(*appsv1.Deployment); ok {
					obj.SetName(hashedDeployName)
					if deploy.Spec.Selector != nil {
						deploy.Spec.Selector.MatchLabels["app.kubernetes.io/instance"] = clusterName
					}
					if deploy.Spec.Template.ObjectMeta.Labels != nil {
						deploy.Spec.Template.ObjectMeta.Labels["app.kubernetes.io/instance"] = clusterName
					}
				}
				// Update selector if Service
				if svc, ok := obj.(*corev1.Service); ok {
					obj.SetName(hashedSvcName)
					if svc.Spec.Selector != nil {
						svc.Spec.Selector["app.kubernetes.io/instance"] = clusterName
					}
				}
			}

			if err := watcher.WaitForMatch(tc.wantResources...); err != nil {
				t.Errorf("Resources mismatch:\n%v", err)
			}
		})
	}
}

// TestCellReconcileGatingNamespaceLabelStormGuard pins that the namespace watch
// only enqueues the Cell for reconcile-label changes, not for unrelated namespace
// label churn. It substitutes a counting reconciler via
// SetupWithManagerReconciler so the enqueue path can be observed directly.
func TestCellReconcileGatingNamespaceLabelStormGuard(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)
	c := mgr.GetClient()

	const nsName = "gating-storm"
	if err := c.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: nsName},
	}); err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	reconciler := &cellcontroller.CellReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller-storm"),
	}
	counter := testutil.NewCountingReconciler(reconciler)
	if err := reconciler.SetupWithManagerReconciler(mgr, counter, controller.Options{
		SkipNameValidation: ptr.To(true),
	}); err != nil {
		t.Fatalf("Failed to create controller, %v", err)
	}

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storm-cell",
			Namespace: nsName,
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name:   "zone1",
			ZoneID: "usw1-az1",
			Images: multigresv1alpha1.CellImages{
				Multigateway: "ghcr.io/multigres/multigres:main",
			},
			Multigateway: multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{
					Replicas: ptr.To(int32(1)),
				},
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
		},
	}
	if err := c.Create(ctx, cell); err != nil {
		t.Fatalf("Failed to create Cell: %v", err)
	}

	key := types.NamespacedName{Namespace: nsName, Name: cell.Name}
	if err := wait.PollUntilContextTimeout(
		ctx,
		100*time.Millisecond,
		30*time.Second,
		true,
		func(context.Context) (bool, error) { return counter.CountFor(key) >= 1, nil },
	); err != nil {
		t.Fatalf("Cell was never reconciled: %v", err)
	}
	counter.WaitQuiescent(t, key)

	// An unrelated namespace label edit must not enqueue the Cell.
	before := counter.CountFor(key)
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: nsName}, ns); err != nil {
		t.Fatalf("Failed to re-read namespace: %v", err)
	}
	if ns.Labels == nil {
		ns.Labels = map[string]string{}
	}
	ns.Labels["unrelated"] = "churn"
	if err := c.Update(ctx, ns); err != nil {
		t.Fatalf("Failed to edit unrelated namespace label: %v", err)
	}
	time.Sleep(time.Second)
	if got := counter.CountFor(key); got != before {
		t.Fatalf("unrelated namespace label edit enqueued reconciles: %d -> %d", before, got)
	}

	// A reconcile-label change on the namespace must enqueue the Cell.
	if err := c.Get(ctx, client.ObjectKey{Name: nsName}, ns); err != nil {
		t.Fatalf("Failed to re-read namespace: %v", err)
	}
	ns.Labels[features.ReconcileLabel] = "disabled"
	if err := c.Update(ctx, ns); err != nil {
		t.Fatalf("Failed to gate namespace: %v", err)
	}
	if err := wait.PollUntilContextTimeout(
		ctx,
		100*time.Millisecond,
		10*time.Second,
		true,
		func(context.Context) (bool, error) { return counter.CountFor(key) > before, nil },
	); err != nil {
		t.Fatalf("reconcile-label change on namespace did not enqueue the Cell: %v", err)
	}
}

// Test helpers

func markManagedLocalTopoServerHealthy(
	t testing.TB,
	ctx context.Context,
	k8sClient client.Client,
	cell *multigresv1alpha1.Cell,
) {
	t.Helper()
	if cell.Spec.TopoServer == nil || cell.Spec.TopoServer.Etcd == nil {
		return
	}

	toposerver := &multigresv1alpha1.TopoServer{}
	key := client.ObjectKey{
		Namespace: cell.Namespace,
		Name:      cellcontroller.BuildLocalTopoServerName(cell),
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true,
		func(ctx context.Context) (bool, error) {
			if err := k8sClient.Get(ctx, key, toposerver); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			return true, nil
		}); err != nil {
		t.Fatalf("Timed out waiting for managed local TopoServer %s/%s: %v", key.Namespace, key.Name, err)
	}

	toposerver.Status.Phase = multigresv1alpha1.PhaseHealthy
	toposerver.Status.ObservedGeneration = toposerver.Generation
	if err := k8sClient.Status().Update(ctx, toposerver); err != nil {
		t.Fatalf("Failed to mark managed local TopoServer %s/%s healthy: %v", key.Namespace, key.Name, err)
	}
}

// cellLabels returns standard labels for cell resources in tests
func cellLabels(t testing.TB, instanceName, component, cellName, zoneID string) map[string]string {
	t.Helper()
	return map[string]string{
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/instance":   "test-cluster", // Use literal cluster name for instance label
		"app.kubernetes.io/managed-by": "multigres-operator",
		"app.kubernetes.io/name":       "multigres",
		"app.kubernetes.io/part-of":    "multigres",
		"multigres.com/cell":           cellName,
		"multigres.com/zone-id":        zoneID,
	}
}

// cellOwnerRefs returns owner references for a Cell resource
func cellOwnerRefs(t testing.TB, cellName string) []metav1.OwnerReference {
	t.Helper()
	return []metav1.OwnerReference{{
		APIVersion:         "multigres.com/v1alpha1",
		Kind:               "Cell",
		Name:               cellName,
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

// TestCellReconcileGatingObjectLevel pins that the reconcile-disabled label on
// the Cell itself (not its namespace) pauses and resumes reconciliation. The
// resume half is driven by the label removal alone - the namespace and the spec
// are never touched again - so it fails against a For() predicate that omits
// features.LabelsChanged(): the metadata-only update would never enqueue.
func TestCellReconcileGatingObjectLevel(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)
	c := mgr.GetClient()

	const nsName = "gating-objectlevel"
	if err := c.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: nsName},
	}); err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	reconciler := &cellcontroller.CellReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller-objectlevel"),
	}
	counter := testutil.NewCountingReconciler(reconciler)
	if err := reconciler.SetupWithManagerReconciler(mgr, counter, controller.Options{
		SkipNameValidation: ptr.To(true),
	}); err != nil {
		t.Fatalf("Failed to create controller, %v", err)
	}

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "object-level-cell",
			Namespace: nsName,
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name:   "zone1",
			ZoneID: "usw1-az1",
			Images: multigresv1alpha1.CellImages{
				Multigateway: "ghcr.io/multigres/multigres:main",
			},
			Multigateway: multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{
					Replicas: ptr.To(int32(1)),
				},
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{},
			},
		},
	}
	if err := c.Create(ctx, cell); err != nil {
		t.Fatalf("Failed to create Cell: %v", err)
	}

	key := types.NamespacedName{Namespace: nsName, Name: cell.Name}
	markManagedLocalTopoServerHealthy(t, ctx, c, cell)

	// 1. Reconciled normally: the Multigateway Deployment exists with 1 replica.
	deployKey := client.ObjectKey{
		Namespace: nsName,
		Name:      cellcontroller.BuildMultigatewayDeploymentName(cell),
	}
	deploymentReplicas := func() (int32, bool) {
		d := &appsv1.Deployment{}
		if err := c.Get(ctx, deployKey, d); err != nil {
			return 0, false
		}
		return ptr.Deref(d.Spec.Replicas, 0), true
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true,
		func(context.Context) (bool, error) {
			replicas, ok := deploymentReplicas()
			return ok && replicas == 1, nil
		}); err != nil {
		t.Fatalf("Multigateway Deployment never reached 1 replica: %v", err)
	}

	// 2. Gate the object itself. The namespace is untouched.
	counter.WaitQuiescent(t, key)
	testutil.PatchLabel(t, c, cell, "multigres.com/reconcile", "disabled")
	counter.WaitQuiescent(t, key)

	// 3. A spec mutation while gated must not reach the Deployment, and the
	// Cell must get no write from the operator after it.
	require.NoError(t, c.Get(ctx, key, cell))
	cell.Spec.Multigateway.Replicas = ptr.To(int32(2))
	require.NoError(t, c.Update(ctx, cell))

	require.Never(t, func() bool {
		replicas, ok := deploymentReplicas()
		return ok && replicas != 1
	}, 2*time.Second, 200*time.Millisecond,
		"gated Cell reconciled its spec change into the Multigateway Deployment")
	testutil.RequireNoWrites(ctx, t, mgr.GetAPIReader(), cell, time.Second)

	// 4. Remove the object's own label - spec and namespace untouched.
	counter.WaitQuiescent(t, key)
	before := counter.CountFor(key)
	testutil.PatchLabel(t, c, cell, "multigres.com/reconcile", "")

	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true,
		func(context.Context) (bool, error) {
			return counter.CountFor(key) > before, nil
		}); err != nil {
		t.Fatalf("removing the object's own label did not re-enqueue the Cell: %v", err)
	}

	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true,
		func(context.Context) (bool, error) {
			replicas, ok := deploymentReplicas()
			return ok && replicas == 2, nil
		}); err != nil {
		t.Fatalf(
			"removing the object's own label did not converge the pending spec change: %v",
			err,
		)
	}
}
