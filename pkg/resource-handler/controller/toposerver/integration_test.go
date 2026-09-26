//go:build integration
// +build integration

package toposerver_test

import (
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	toposervercontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

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

	assert.NewAborting(t).NoError((&toposervercontroller.TopoServerReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("toposerver-controller"),
	}).SetupWithManager(mgr, controller.Options{
		SkipNameValidation: ptr.To(true),
	}), "Failed to create controller")
}

func TestTopoServerReconciliation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	preferDualStack := corev1.IPFamilyPolicyPreferDualStack

	tests := map[string]struct {
		toposerver      *multigresv1alpha1.TopoServer
		existingObjects []client.Object
		failureConfig   *testutil.FailureConfig
		wantResources   []client.Object
		wantErr         bool
		wantRequeue     bool
		assertFunc      func(t *testing.T, c client.Client, toposerver *multigresv1alpha1.TopoServer)
	}{
		"simple toposerver input": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{},
				},
			},
			wantResources: []client.Object{
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-toposerver",
						Namespace:       "default",
						Labels:          toposerverLabels(t, "test-cluster"),
						OwnerReferences: toposerverOwnerRefs(t, "test-toposerver"),
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas:    ptr.To(int32(3)),
						ServiceName: "test-toposerver-headless",
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(
								toposerverLabels(t, "test-cluster"),
							),
						},
						PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
							WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
							WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: toposerverLabels(t, "test-cluster"),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "etcd",
										Image: multigresv1alpha1.DefaultEtcdImage,
										Ports: []corev1.ContainerPort{
											tcpPort(t, "client", 2379),
											tcpPort(t, "peer", 2380),
											tcpPort(t, "metrics", 2381),
										},
										Env: []corev1.EnvVar{
											{
												Name: "POD_NAME",
												ValueFrom: &corev1.EnvVarSource{
													FieldRef: &corev1.ObjectFieldSelector{
														APIVersion: "v1",
														FieldPath:  "metadata.name",
													},
												},
											},
											{
												Name: "POD_NAMESPACE",
												ValueFrom: &corev1.EnvVarSource{
													FieldRef: &corev1.ObjectFieldSelector{
														APIVersion: "v1",
														FieldPath:  "metadata.namespace",
													},
												},
											},
											{Name: "ETCD_NAME", Value: "$(POD_NAME)"},
											{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
											{
												Name:  "ETCD_LISTEN_CLIENT_URLS",
												Value: "http://[::]:2379",
											},
											{
												Name:  "ETCD_LISTEN_PEER_URLS",
												Value: "http://[::]:2380",
											},
											{
												Name:  "ETCD_LISTEN_METRICS_URLS",
												Value: "http://[::]:2381",
											},
											{
												Name:  "ETCD_ADVERTISE_CLIENT_URLS",
												Value: "http://$(POD_NAME).test-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2379",
											},
											{
												Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
												Value: "http://$(POD_NAME).test-toposerver-headless.$(POD_NAMESPACE).svc.cluster.local:2380",
											},
											{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: "new"},
											{
												Name:  "ETCD_INITIAL_CLUSTER_TOKEN",
												Value: "test-toposerver",
											},
											{
												Name:  "ETCD_INITIAL_CLUSTER",
												Value: "test-toposerver-0=http://test-toposerver-0.test-toposerver-headless.default.svc.cluster.local:2380,test-toposerver-1=http://test-toposerver-1.test-toposerver-headless.default.svc.cluster.local:2380,test-toposerver-2=http://test-toposerver-2.test-toposerver-headless.default.svc.cluster.local:2380",
											},
											{Name: "ETCD_AUTO_COMPACTION_MODE", Value: "periodic"},
											{Name: "ETCD_AUTO_COMPACTION_RETENTION", Value: "1h"},
											{Name: "ETCD_QUOTA_BACKEND_BYTES", Value: "2147483648"},
										},
										VolumeMounts: []corev1.VolumeMount{
											{Name: "data", MountPath: "/var/lib/etcd"},
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/readyz",
													Port: intstr.FromInt32(
														toposervercontroller.MetricsPort,
													),
													Scheme: corev1.URISchemeHTTP,
												},
											},
											TimeoutSeconds:   1,
											PeriodSeconds:    5,
											SuccessThreshold: 1,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/livez",
													Port: intstr.FromInt32(
														toposervercontroller.MetricsPort,
													),
													Scheme: corev1.URISchemeHTTP,
												},
											},
											TimeoutSeconds:   1,
											PeriodSeconds:    10,
											SuccessThreshold: 1,
											FailureThreshold: 3,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/readyz",
													Port: intstr.FromInt32(
														toposervercontroller.MetricsPort,
													),
													Scheme: corev1.URISchemeHTTP,
												},
											},
											TimeoutSeconds:   1,
											PeriodSeconds:    5,
											SuccessThreshold: 1,
											FailureThreshold: 3,
										},
									},
								},
							},
						},
						VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
							{
								ObjectMeta: metav1.ObjectMeta{Name: "data"},
								Spec: corev1.PersistentVolumeClaimSpec{
									AccessModes: []corev1.PersistentVolumeAccessMode{
										corev1.ReadWriteOnce,
									},
									Resources: corev1.VolumeResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceStorage: resource.MustParse("10Gi"),
										},
									},
									VolumeMode: ptr.To(corev1.PersistentVolumeFilesystem),
								},
								Status: corev1.PersistentVolumeClaimStatus{
									Phase: corev1.ClaimPending,
								},
							},
						},
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-toposerver",
						Namespace:       "default",
						Labels:          toposerverLabels(t, "test-cluster"),
						OwnerReferences: toposerverOwnerRefs(t, "test-toposerver"),
					},
					Spec: corev1.ServiceSpec{
						IPFamilyPolicy: &preferDualStack,
						Type:           corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "client", 2379),
						},
						Selector: metadata.GetSelectorLabels(toposerverLabels(t, "test-cluster")),
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "test-toposerver-headless",
						Namespace:       "default",
						Labels:          toposerverLabels(t, "test-cluster"),
						OwnerReferences: toposerverOwnerRefs(t, "test-toposerver"),
					},
					Spec: corev1.ServiceSpec{
						IPFamilyPolicy: &preferDualStack,
						Type:           corev1.ServiceTypeClusterIP,
						ClusterIP:      corev1.ClusterIPNone,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "client", 2379),
							tcpServicePort(t, "peer", 2380),
						},
						Selector: metadata.GetSelectorLabels(
							toposerverLabels(t, "test-cluster"),
						),
						PublishNotReadyAddresses: true,
					},
				},
			},
		},
		"toposerver with delete pvc policy": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "delete-policy-topo",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{},
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
						WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
					},
				},
			},
			wantResources: []client.Object{
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "delete-policy-topo",
						Namespace:       "default",
						Labels:          toposerverLabels(t, "test-cluster"),
						OwnerReferences: toposerverOwnerRefs(t, "delete-policy-topo"),
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas:    ptr.To(int32(3)),
						ServiceName: "delete-policy-topo-headless",
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(
								toposerverLabels(t, "test-cluster"),
							),
						},
						PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
							WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
							WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: toposerverLabels(t, "test-cluster"),
								Annotations: map[string]string{
									"multigres.com/project-ref": "test-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "etcd",
										Image: multigresv1alpha1.DefaultEtcdImage,
										Ports: []corev1.ContainerPort{
											tcpPort(t, "client", 2379),
											tcpPort(t, "peer", 2380),
											tcpPort(t, "metrics", 2381),
										},
										Env: []corev1.EnvVar{
											{
												Name: "POD_NAME",
												ValueFrom: &corev1.EnvVarSource{
													FieldRef: &corev1.ObjectFieldSelector{
														APIVersion: "v1",
														FieldPath:  "metadata.name",
													},
												},
											},
											{
												Name: "POD_NAMESPACE",
												ValueFrom: &corev1.EnvVarSource{
													FieldRef: &corev1.ObjectFieldSelector{
														APIVersion: "v1",
														FieldPath:  "metadata.namespace",
													},
												},
											},
											{Name: "ETCD_NAME", Value: "$(POD_NAME)"},
											{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
											{
												Name:  "ETCD_LISTEN_CLIENT_URLS",
												Value: "http://[::]:2379",
											},
											{
												Name:  "ETCD_LISTEN_PEER_URLS",
												Value: "http://[::]:2380",
											},
											{
												Name:  "ETCD_LISTEN_METRICS_URLS",
												Value: "http://[::]:2381",
											},
											{
												Name:  "ETCD_ADVERTISE_CLIENT_URLS",
												Value: "http://$(POD_NAME).delete-policy-topo-headless.$(POD_NAMESPACE).svc.cluster.local:2379",
											},
											{
												Name:  "ETCD_INITIAL_ADVERTISE_PEER_URLS",
												Value: "http://$(POD_NAME).delete-policy-topo-headless.$(POD_NAMESPACE).svc.cluster.local:2380",
											},
											{Name: "ETCD_INITIAL_CLUSTER_STATE", Value: "new"},
											{
												Name:  "ETCD_INITIAL_CLUSTER_TOKEN",
												Value: "delete-policy-topo",
											},
											{
												Name:  "ETCD_INITIAL_CLUSTER",
												Value: "delete-policy-topo-0=http://delete-policy-topo-0.delete-policy-topo-headless.default.svc.cluster.local:2380,delete-policy-topo-1=http://delete-policy-topo-1.delete-policy-topo-headless.default.svc.cluster.local:2380,delete-policy-topo-2=http://delete-policy-topo-2.delete-policy-topo-headless.default.svc.cluster.local:2380",
											},
											{Name: "ETCD_AUTO_COMPACTION_MODE", Value: "periodic"},
											{Name: "ETCD_AUTO_COMPACTION_RETENTION", Value: "1h"},
											{Name: "ETCD_QUOTA_BACKEND_BYTES", Value: "2147483648"},
										},
										VolumeMounts: []corev1.VolumeMount{
											{Name: "data", MountPath: "/var/lib/etcd"},
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/readyz",
													Port: intstr.FromInt32(
														toposervercontroller.MetricsPort,
													),
													Scheme: corev1.URISchemeHTTP,
												},
											},
											TimeoutSeconds:   1,
											PeriodSeconds:    5,
											SuccessThreshold: 1,
											FailureThreshold: 30,
										},
										LivenessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/livez",
													Port: intstr.FromInt32(
														toposervercontroller.MetricsPort,
													),
													Scheme: corev1.URISchemeHTTP,
												},
											},
											TimeoutSeconds:   1,
											PeriodSeconds:    10,
											SuccessThreshold: 1,
											FailureThreshold: 3,
										},
										ReadinessProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path: "/readyz",
													Port: intstr.FromInt32(
														toposervercontroller.MetricsPort,
													),
													Scheme: corev1.URISchemeHTTP,
												},
											},
											TimeoutSeconds:   1,
											PeriodSeconds:    5,
											SuccessThreshold: 1,
											FailureThreshold: 3,
										},
									},
								},
							},
						},
						VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
							{
								ObjectMeta: metav1.ObjectMeta{Name: "data"},
								Spec: corev1.PersistentVolumeClaimSpec{
									AccessModes: []corev1.PersistentVolumeAccessMode{
										corev1.ReadWriteOnce,
									},
									Resources: corev1.VolumeResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceStorage: resource.MustParse("10Gi"),
										},
									},
									VolumeMode: ptr.To(corev1.PersistentVolumeFilesystem),
								},
								Status: corev1.PersistentVolumeClaimStatus{
									Phase: corev1.ClaimPending,
								},
							},
						},
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "delete-policy-topo",
						Namespace:       "default",
						Labels:          toposerverLabels(t, "test-cluster"),
						OwnerReferences: toposerverOwnerRefs(t, "delete-policy-topo"),
					},
					Spec: corev1.ServiceSpec{
						IPFamilyPolicy: &preferDualStack,
						Type:           corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "client", 2379),
						},
						Selector: metadata.GetSelectorLabels(toposerverLabels(t, "test-cluster")),
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "delete-policy-topo-headless",
						Namespace:       "default",
						Labels:          toposerverLabels(t, "test-cluster"),
						OwnerReferences: toposerverOwnerRefs(t, "delete-policy-topo"),
					},
					Spec: corev1.ServiceSpec{
						IPFamilyPolicy: &preferDualStack,
						Type:           corev1.ServiceTypeClusterIP,
						ClusterIP:      corev1.ClusterIPNone,
						Ports: []corev1.ServicePort{
							tcpServicePort(t, "client", 2379),
							tcpServicePort(t, "peer", 2380),
						},
						Selector: metadata.GetSelectorLabels(
							toposerverLabels(t, "test-cluster"),
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
			c := assert.NewCollecting(t)
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
					testutil.IgnoreStatefulSetRuntimeFields(),
					testutil.IgnorePodSpecDefaults(),
					testutil.IgnoreStatefulSetSpecDefaults(),
				),
				testutil.WithExtraResource(&multigresv1alpha1.TopoServer{}),
			)
			client := mgr.GetClient()

			toposerverReconciler := &toposervercontroller.TopoServerReconciler{
				Client:   mgr.GetClient(),
				Scheme:   mgr.GetScheme(),
				Recorder: mgr.GetEventRecorderFor("toposerver-controller"),
			}
			c.Require().NoError(toposerverReconciler.SetupWithManager(mgr, controller.Options{
				SkipNameValidation: ptr.To(true),
			}), "Failed to create controller")

			c.Require().
				NoError(client.Create(ctx, tc.toposerver), "Failed to create the initial item")

			c.NoError(watcher.WaitForMatch(tc.wantResources...), "Resources mismatch:\n")
		})
	}
}

// Test helpers

// toposerverLabels returns standard labels for toposerver resources in tests
func toposerverLabels(t testing.TB, instanceName string) map[string]string {
	t.Helper()
	return map[string]string{
		"app.kubernetes.io/component":  "toposerver",
		"app.kubernetes.io/instance":   instanceName,
		"app.kubernetes.io/managed-by": "multigres-operator",
		"app.kubernetes.io/name":       "multigres",
		"app.kubernetes.io/part-of":    "multigres",
		"multigres.com/cluster":        instanceName,
	}
}

// toposerverOwnerRefs returns owner references for a TopoServer resource
func toposerverOwnerRefs(t testing.TB, toposerverName string) []metav1.OwnerReference {
	t.Helper()
	return []metav1.OwnerReference{{
		APIVersion:         "multigres.com/v1alpha1",
		Kind:               "TopoServer",
		Name:               toposerverName,
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
