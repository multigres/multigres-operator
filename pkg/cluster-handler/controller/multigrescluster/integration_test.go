//go:build integration
// +build integration

package multigrescluster_test

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/stretchr/testify/require"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
)

// ============================================================================
// Shared Test Setup & Helpers
// ============================================================================

const (
	testNamespace = "default"
	testTimeout   = 10 * time.Second
)

func canonicalGlobalRoot(clusterName string) string {
	return "/multigres/" + testNamespace + "/" + clusterName + "/global"
}

// setupIntegration bootstraps the test environment, controller, and default templates.
// It returns a ready-to-use K8s Client and a ResourceWatcher.
func setupIntegration(t *testing.T) (client.Client, *testutil.ResourceWatcher) {
	t.Helper()
	k8sClient, watcher, _ := setupIntegrationCore(t, nil, nil)
	return k8sClient, watcher
}

// setupIntegrationCore is setupIntegration with two hooks the gating tests need:
// wrap sits on the reconcile boundary (to count reconciles) and mutate edits the
// reconciler before it starts. The receiver is the same reconciler either way,
// so the watch configuration is identical to production. It also returns the
// manager's uncached reader, for asserting on resourceVersions.
func setupIntegrationCore(
	t *testing.T,
	wrap func(reconcile.Reconciler) reconcile.Reconciler,
	mutate func(*multigrescluster.MultigresClusterReconciler),
) (client.Client, *testutil.ResourceWatcher, client.Reader) {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)

	// 1. Setup Envtest and Manager
	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)

	// 2. Setup Watcher
	watcher := testutil.NewResourceWatcher(t, t.Context(), mgr,
		testutil.WithCmpOpts(
			testutil.IgnoreMetaRuntimeFields(),
			testutil.IgnoreServiceRuntimeFields(),
			testutil.IgnoreDeploymentRuntimeFields(),
			testutil.IgnorePodSpecDefaults(),
			testutil.IgnoreDeploymentSpecDefaults(),
		),
		testutil.WithExtraResource(
			&multigresv1alpha1.MultigresCluster{},
			&multigresv1alpha1.TopoServer{},
			&multigresv1alpha1.Cell{},
			&multigresv1alpha1.TableGroup{},
			&appsv1.Deployment{},
		),
		testutil.WithTimeout(testTimeout),
	)

	// 3. Setup Controller
	reconciler := &multigrescluster.MultigresClusterReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("multigres-cluster-controller"),
		CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			_, factory := memorytopo.NewServerAndFactory(t.Context())
			store := topoclient.NewWithFactory(
				factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
			)
			return store, nil
		},
	}
	if mutate != nil {
		mutate(reconciler)
	}

	var rec reconcile.Reconciler = reconciler
	if wrap != nil {
		rec = wrap(reconciler)
	}
	if err := reconciler.SetupWithManagerReconciler(mgr, rec, controller.Options{
		SkipNameValidation: ptr.To(true),
	}); err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	k8sClient := mgr.GetClient()

	// 4. Create Standard Default Templates (Core, Cell, Shard)
	// These are required for most tests to pass basic validation/resolution.
	defaults := []client.Object{
		&multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
			Spec: multigresv1alpha1.CoreTemplateSpec{
				GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:default"},
				},
			},
		},
		&multigresv1alpha1.CellTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
			Spec: multigresv1alpha1.CellTemplateSpec{
				Multigateway: &multigresv1alpha1.MultigatewaySpec{StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))}},
			},
		},
		&multigresv1alpha1.ShardTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
			Spec:       multigresv1alpha1.ShardTemplateSpec{},
		},
	}

	for _, obj := range defaults {
		if err := k8sClient.Create(t.Context(), obj); client.IgnoreAlreadyExists(err) != nil {
			t.Fatalf("Failed to create default template %s: %v", obj.GetName(), err)
		}
	}

	return k8sClient, watcher, mgr.GetAPIReader()
}

func setTestPostgresPasswordSecretRef(cluster *multigresv1alpha1.MultigresCluster) {
	if cluster == nil || cluster.Spec.PostgresPasswordSecretRef.Name != "" {
		return
	}
	cluster.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: "multigres-admin-password",
		Key:  "password",
	}
}

func setTestTableGroupPostgresPasswordSecretRef(tableGroup *multigresv1alpha1.TableGroup) {
	if tableGroup == nil || tableGroup.Spec.PostgresPasswordSecretRef.Name != "" {
		return
	}
	tableGroup.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: "multigres-admin-password",
		Key:  "password",
	}
}

func clusterLabels(t testing.TB, clusterName, oldApp, cell string) map[string]string {
	t.Helper()
	var component string
	if cell != "" {
		component = metadata.ComponentCell
	} else if oldApp == "multiadmin" {
		component = metadata.ComponentMultiadmin
	} else if oldApp != "" {
		component = oldApp
	} else {
		// Default to global-topo if app is empty
		component = metadata.ComponentGlobalTopo
	}

	labels := metadata.BuildStandardLabels(clusterName, component)
	metadata.AddClusterLabel(labels, clusterName)
	if cell != "" {
		metadata.AddCellLabel(labels, multigresv1alpha1.CellName(cell))
	}
	return labels
}

func tableGroupLabels(clusterName, db, tg string) map[string]string {
	labels := metadata.BuildStandardLabels(clusterName, metadata.ComponentTableGroup)
	metadata.AddClusterLabel(labels, clusterName)
	metadata.AddDatabaseLabel(labels, multigresv1alpha1.DatabaseName(db))
	metadata.AddTableGroupLabel(labels, multigresv1alpha1.TableGroupName(tg))
	return labels
}

func clusterOwnerRefs(t testing.TB, clusterName string) []metav1.OwnerReference {
	t.Helper()
	return []metav1.OwnerReference{{
		APIVersion:         "multigres.com/v1alpha1",
		Kind:               "MultigresCluster",
		Name:               clusterName,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}}
}

// ============================================================================
// Happy Path Tests
// ============================================================================

func TestMultigresCluster_HappyPath(t *testing.T) {
	t.Parallel()

	const clusterName = "test-cluster"

	tests := map[string]struct {
		cluster       *multigresv1alpha1.MultigresCluster
		wantResources []client.Object
	}{
		"full cluster integration": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images: multigresv1alpha1.ClusterImages{
						Multigateway:     "gateway:latest",
						Multiorch:        "orch:latest",
						Multipooler:      "pooler:latest",
						Multiadmin:       "admin:latest",
						Postgres:         "postgres:15",
						ImagePullPolicy:  corev1.PullAlways,
						ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
					},
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:latest"},
					},
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a", ZoneID: "use1-az1", Spec: &multigresv1alpha1.CellInlineSpec{
							Multigateway: multigresv1alpha1.MultigatewaySpec{StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))}},
						}},
					},
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name:    "postgres",
							Default: true,
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name:    "default",
									Default: true,
									Shards: []multigresv1alpha1.ShardConfig{{
										Name: "0-inf",
										Spec: &multigresv1alpha1.ShardInlineSpec{
											Multiorch: multigresv1alpha1.MultiorchSpec{StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))}},
											Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
												"primary": {
													ReplicasPerCell: ptr.To(int32(3)),
													Type:            "readWrite",
													Cells:           []multigresv1alpha1.CellName{"zone-a"},
												},
											},
										},
									}},
								},
							},
						},
					},
				},
			},
			wantResources: []client.Object{
				// 1. Global TopoServer
				&multigresv1alpha1.TopoServer{
					ObjectMeta: metav1.ObjectMeta{
						Name:            clusterName + "-global-topo",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, clusterName, "", ""),
						OwnerReferences: clusterOwnerRefs(t, clusterName),
					},
					Spec: multigresv1alpha1.TopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{
							Image:     "etcd:latest",
							Replicas:  ptr.To(resolver.DefaultEtcdReplicas),
							RootPath:  "/multigres/default/test-cluster/global",
							Storage:   multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize},
							Resources: resolver.DefaultResourcesEtcd(),
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
					},
				},
				// 2. Multiadmin Deployment
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            clusterName + "-multiadmin",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, clusterName, "multiadmin", ""),
						OwnerReferences: clusterOwnerRefs(t, clusterName),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultAdminReplicas), // Matches default in test input
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, clusterName, "multiadmin", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, clusterName, "multiadmin", ""),
								Annotations: map[string]string{
									"multigres.com/project-ref": clusterName,
								},
							},
							Spec: corev1.PodSpec{
								ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
								Containers: []corev1.Container{
									{
										Name:  "multiadmin",
										Image: "admin:latest",
										Command: []string{
											"/multigres/bin/multiadmin",
										},
										Args: []string{
											"--http-port=18000",
											"--grpc-port=18070",
											"--topo-global-server-addresses=" + clusterName + "-global-topo." + testNamespace + ".svc:2379",
											"--topo-global-root=/multigres/default/test-cluster/global",
											"--service-map=grpc-multiadmin",
											"--pprof-http=true",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											{
												Name:          "http",
												ContainerPort: 18000,
												Protocol:      corev1.ProtocolTCP,
											},
											{
												Name:          "grpc",
												ContainerPort: 18070,
												Protocol:      corev1.ProtocolTCP,
											},
										},
										Resources: resolver.DefaultResourcesAdmin(),
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path:   "/ready",
													Port:   intstr.FromInt(18000),
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
													Path:   "/live",
													Port:   intstr.FromInt(18000),
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
													Path:   "/ready",
													Port:   intstr.FromInt(18000),
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
					},
				},
				// 3. MultiadminWeb Deployment
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            clusterName + "-multiadmin-web",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, clusterName, "multiadmin-web", ""),
						OwnerReferences: clusterOwnerRefs(t, clusterName),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultMultiadminWebReplicas), // Defaults
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, clusterName, "multiadmin-web", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, clusterName, "multiadmin-web", ""),
								Annotations: map[string]string{
									"multigres.com/project-ref": clusterName,
								},
							},
							Spec: corev1.PodSpec{
								ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
								Containers: []corev1.Container{
									{
										Name:  "multiadmin-web",
										Image: resolver.DefaultMultiadminWebImage, // Default
										Ports: []corev1.ContainerPort{
											{
												Name:          "http",
												ContainerPort: 18100,
												Protocol:      corev1.ProtocolTCP,
											},
										},
										Resources: resolver.DefaultResourcesAdminWeb(),
										Env: []corev1.EnvVar{
											{Name: "HOSTNAME", Value: "::"},
											{Name: "MULTIADMIN_API_URL", Value: "http://" + clusterName + "-multiadmin:18000"},
											{Name: "POSTGRES_HOST", Value: clusterName + "-multigateway"},
											{Name: "POSTGRES_PORT", Value: "5432"},
											{Name: "POSTGRES_DATABASE", Value: "postgres"},
											{Name: "POSTGRES_USER", Value: "postgres"},
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
					},
				},
				// 4. Global Multigateway Service (ClusterIP by default, no externalGateway)
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            clusterName + "-multigateway",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, clusterName, "multigateway", ""),
						OwnerReferences: clusterOwnerRefs(t, clusterName),
					},
					Spec: corev1.ServiceSpec{
						Selector: map[string]string{
							metadata.LabelAppComponent: metadata.ComponentMultigateway,
							metadata.LabelAppInstance:  clusterName,
						},
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							{
								Name:       "postgres",
								Port:       5432,
								TargetPort: intstr.FromString("postgres"),
								Protocol:   corev1.ProtocolTCP,
							},
						},
					},
				},
				// 5. Cell
				&multigresv1alpha1.Cell{
					ObjectMeta: metav1.ObjectMeta{
						Name:            clusterName + "-zone-a",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, clusterName, "", "zone-a"),
						OwnerReferences: clusterOwnerRefs(t, clusterName),
					},
					Spec: multigresv1alpha1.CellSpec{
						Name:   "zone-a",
						ZoneID: "use1-az1",
						Images: multigresv1alpha1.CellImages{
							Multigateway:     "gateway:latest",
							ImagePullPolicy:  corev1.PullAlways,
							ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
						},
						Multigateway: multigresv1alpha1.MultigatewaySpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas:  ptr.To(int32(1)),
								Resources: resolver.DefaultResourcesGateway(), // Expected default
							},
							Buffer:       &multigresv1alpha1.GatewayBufferConfig{Enabled: ptr.To(true)},
							ClusterViews: &multigresv1alpha1.GatewayClusterViewsConfig{Enabled: ptr.To(false)},
						},
						AllCells: []multigresv1alpha1.CellName{"zone-a"},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        clusterName + "-global-topo." + testNamespace + ".svc:2379",
							RootPath:       canonicalGlobalRoot(clusterName),
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
				},
				// 6. TableGroup
				&multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:            clusterName + "-8b65dfba",
						Namespace:       testNamespace,
						Labels:          tableGroupLabels(clusterName, "postgres", "default"),
						OwnerReferences: clusterOwnerRefs(t, clusterName),
					},
					Spec: multigresv1alpha1.TableGroupSpec{
						CellTopologyLabels: map[multigresv1alpha1.CellName]map[string]string{
							"zone-a": {"topology.k8s.aws/zone-id": "use1-az1"},
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
							Multiorch:        "orch:latest",
							Multipooler:      "pooler:latest",
							Postgres:         "postgres:15",
							ImagePullPolicy:  corev1.PullAlways,
							ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
						},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        clusterName + "-global-topo." + testNamespace + ".svc:2379",
							RootPath:       canonicalGlobalRoot(clusterName),
							Implementation: "etcd",
						},
						Shards: []multigresv1alpha1.ShardResolvedSpec{
							{
								Name: "0-inf",
								Multiorch: multigresv1alpha1.MultiorchSpec{
									Cells: []multigresv1alpha1.CellName{"zone-a"},
									StatelessSpec: multigresv1alpha1.StatelessSpec{
										Replicas:  ptr.To(int32(1)),
										Resources: resolver.DefaultResourcesOrch(), // FIX: Expect defaults
									},
								},
								Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
									"primary": {
										ReplicasPerCell: ptr.To(int32(3)),
										Type:            "readWrite",
										Cells:           []multigresv1alpha1.CellName{"zone-a"},
										// FIX: Expect defaults for pool resources
										Storage:     multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize},
										Postgres:    multigresv1alpha1.ContainerConfig{Resources: resolver.DefaultResourcesPostgres()},
										Multipooler: multigresv1alpha1.ContainerConfig{Resources: resolver.DefaultResourcesPooler()},
									},
								},
								PVCDeletionPolicy: nil, // Shard-level policy is nil (inherited)
								Backup: &multigresv1alpha1.BackupConfig{
									Type:       multigresv1alpha1.BackupTypeFilesystem,
									Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
								},
							},
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
						Backup: &multigresv1alpha1.BackupConfig{
							Type:       multigresv1alpha1.BackupTypeFilesystem,
							Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
						},
						TopologyPruning:   &multigresv1alpha1.TopologyPruningConfig{Enabled: ptr.To(true)},
						DurabilityPolicy:  "AT_LEAST_2",
						PostgresSuperuser: "postgres",
					},
				},
			},
		},
		"minimal cluster with injection": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "minimal-cluster",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "default",
						CellTemplate:  "default",
						ShardTemplate: "default",
					},
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						TemplateRef: "default",
					},
					Multiadmin: &multigresv1alpha1.MultiadminConfig{
						TemplateRef: "default",
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a", ZoneID: "use1-az1"},
					},
				},
			},
			wantResources: []client.Object{
				// 1. Global TopoServer
				&multigresv1alpha1.TopoServer{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "minimal-cluster-global-topo",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "minimal-cluster", "", ""),
						OwnerReferences: clusterOwnerRefs(t, "minimal-cluster"),
					},
					Spec: multigresv1alpha1.TopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{
							Image:     "etcd:default",
							Replicas:  ptr.To(resolver.DefaultEtcdReplicas),
							RootPath:  "/multigres/default/minimal-cluster/global",
							Storage:   multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize},
							Resources: resolver.DefaultResourcesEtcd(),
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
					},
				},
				// 2. Multiadmin
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "minimal-cluster-multiadmin",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "minimal-cluster", "multiadmin", ""),
						OwnerReferences: clusterOwnerRefs(t, "minimal-cluster"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultAdminReplicas),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "minimal-cluster", "multiadmin", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, "minimal-cluster", "multiadmin", ""),
								Annotations: map[string]string{
									"multigres.com/project-ref": "minimal-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiadmin",
										Image: resolver.DefaultMultiadminImage,
										Command: []string{
											"/multigres/bin/multiadmin",
										},
										Args: []string{
											"--http-port=18000",
											"--grpc-port=18070",
											"--topo-global-server-addresses=minimal-cluster-global-topo." + testNamespace + ".svc:2379",
											"--topo-global-root=/multigres/default/minimal-cluster/global",
											"--service-map=grpc-multiadmin",
											"--pprof-http=true",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											{
												Name:          "http",
												ContainerPort: 18000,
												Protocol:      corev1.ProtocolTCP,
											},
											{
												Name:          "grpc",
												ContainerPort: 18070,
												Protocol:      corev1.ProtocolTCP,
											},
										},
										Resources: resolver.DefaultResourcesAdmin(),
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path:   "/ready",
													Port:   intstr.FromInt(18000),
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
													Path:   "/live",
													Port:   intstr.FromInt(18000),
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
													Path:   "/ready",
													Port:   intstr.FromInt(18000),
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
					},
				},
				// 2b. MultiadminWeb Deployment
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "minimal-cluster-multiadmin-web",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "minimal-cluster", "multiadmin-web", ""),
						OwnerReferences: clusterOwnerRefs(t, "minimal-cluster"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultMultiadminWebReplicas),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "minimal-cluster", "multiadmin-web", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, "minimal-cluster", "multiadmin-web", ""),
								Annotations: map[string]string{
									"multigres.com/project-ref": "minimal-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiadmin-web",
										Image: resolver.DefaultMultiadminWebImage,
										Ports: []corev1.ContainerPort{
											{
												Name:          "http",
												ContainerPort: 18100,
												Protocol:      corev1.ProtocolTCP,
											},
										},
										Resources: resolver.DefaultResourcesAdminWeb(),
										Env: []corev1.EnvVar{
											{Name: "HOSTNAME", Value: "::"},
											{Name: "MULTIADMIN_API_URL", Value: "http://minimal-cluster-multiadmin:18000"},
											{Name: "POSTGRES_HOST", Value: "minimal-cluster-multigateway"},
											{Name: "POSTGRES_PORT", Value: "5432"},
											{Name: "POSTGRES_DATABASE", Value: "postgres"},
											{Name: "POSTGRES_USER", Value: "postgres"},
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
					},
				},
				// 4. Global Multigateway Service (ClusterIP by default, no externalGateway)
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "minimal-cluster-multigateway",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "minimal-cluster", "multigateway", ""),
						OwnerReferences: clusterOwnerRefs(t, "minimal-cluster"),
					},
					Spec: corev1.ServiceSpec{
						Selector: map[string]string{
							metadata.LabelAppComponent: metadata.ComponentMultigateway,
							metadata.LabelAppInstance:  "minimal-cluster",
						},
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							{
								Name:       "postgres",
								Port:       5432,
								TargetPort: intstr.FromString("postgres"),
								Protocol:   corev1.ProtocolTCP,
							},
						},
					},
				},
				// 5. Cell
				&multigresv1alpha1.Cell{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "minimal-cluster-zone-a",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "minimal-cluster", "", "zone-a"),
						OwnerReferences: clusterOwnerRefs(t, "minimal-cluster"),
					},
					Spec: multigresv1alpha1.CellSpec{
						Name:   "zone-a",
						ZoneID: "use1-az1",
						Images: multigresv1alpha1.CellImages{
							Multigateway:    resolver.DefaultMultigatewayImage,
							ImagePullPolicy: resolver.DefaultImagePullPolicy,
						},
						Multigateway: multigresv1alpha1.MultigatewaySpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas:  ptr.To(int32(1)), // From default template
								Resources: resolver.DefaultResourcesGateway(),
							},
							Buffer:       &multigresv1alpha1.GatewayBufferConfig{Enabled: ptr.To(true)},
							ClusterViews: &multigresv1alpha1.GatewayClusterViewsConfig{Enabled: ptr.To(false)},
						},
						AllCells: []multigresv1alpha1.CellName{"zone-a"},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        "minimal-cluster-global-topo." + testNamespace + ".svc:2379",
							RootPath:       canonicalGlobalRoot("minimal-cluster"),
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
				},
				// 6. Injected TableGroup
				&multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "minimal-cluster-8b65dfba",
						Namespace:       testNamespace,
						Labels:          tableGroupLabels("minimal-cluster", "postgres", "default"),
						OwnerReferences: clusterOwnerRefs(t, "minimal-cluster"),
					},
					Spec: multigresv1alpha1.TableGroupSpec{
						CellTopologyLabels: map[multigresv1alpha1.CellName]map[string]string{
							"zone-a": {"topology.k8s.aws/zone-id": "use1-az1"},
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
							Multiorch:       resolver.DefaultMultiorchImage,
							Multipooler:     resolver.DefaultMultipoolerImage,
							Postgres:        resolver.DefaultPostgresImage,
							ImagePullPolicy: resolver.DefaultImagePullPolicy,
						},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        "minimal-cluster-global-topo." + testNamespace + ".svc:2379",
							RootPath:       canonicalGlobalRoot("minimal-cluster"),
							Implementation: "etcd",
						},
						Shards: []multigresv1alpha1.ShardResolvedSpec{
							{
								Name: "0-inf",
								Multiorch: multigresv1alpha1.MultiorchSpec{
									Cells: []multigresv1alpha1.CellName{"zone-a"},
									StatelessSpec: multigresv1alpha1.StatelessSpec{
										Replicas:  ptr.To(int32(1)),
										Resources: resolver.DefaultResourcesOrch(),
									},
								},
								// FIX: Expect the injected default pool
								Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
									"default": {
										Type:  "readWrite",
										Cells: []multigresv1alpha1.CellName{"zone-a"},
										// Single-cell pool defaults to 2 (AT_LEAST_2 minimum).
										ReplicasPerCell: ptr.To(int32(2)),
										Storage:         multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize}, // "1Gi"
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
								},
							},
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
						Backup: &multigresv1alpha1.BackupConfig{
							Type:       multigresv1alpha1.BackupTypeFilesystem,
							Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
						},
						TopologyPruning:   &multigresv1alpha1.TopologyPruningConfig{Enabled: ptr.To(true)},
						DurabilityPolicy:  "AT_LEAST_2",
						PostgresSuperuser: "postgres",
					},
				},
			},
		},

		"minimal cluster (lazy user) - regression": {
			cluster: &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lazy-cluster",
					Namespace: testNamespace,
				},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					TemplateDefaults: multigresv1alpha1.TemplateDefaults{
						CoreTemplate:  "default",
						CellTemplate:  "default",
						ShardTemplate: "default",
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a", ZoneID: "use1-az1"},
					},
				},
			},
			wantResources: []client.Object{
				// 1. Global TopoServer (Resolved from default template)
				&multigresv1alpha1.TopoServer{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "lazy-cluster-global-topo",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "lazy-cluster", "", ""),
						OwnerReferences: clusterOwnerRefs(t, "lazy-cluster"),
					},
					Spec: multigresv1alpha1.TopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{
							Image:     "etcd:default",
							Replicas:  ptr.To(resolver.DefaultEtcdReplicas),
							RootPath:  "/multigres/default/lazy-cluster/global",
							Storage:   multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize},
							Resources: resolver.DefaultResourcesEtcd(),
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
					},
				},
				// 2. Multiadmin (Resolved from default template)
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "lazy-cluster-multiadmin",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "lazy-cluster", "multiadmin", ""),
						OwnerReferences: clusterOwnerRefs(t, "lazy-cluster"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultAdminReplicas),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "lazy-cluster", "multiadmin", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, "lazy-cluster", "multiadmin", ""),
								Annotations: map[string]string{
									"multigres.com/project-ref": "lazy-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiadmin",
										Image: resolver.DefaultMultiadminImage,
										Command: []string{
											"/multigres/bin/multiadmin",
										},
										Args: []string{
											"--http-port=18000",
											"--grpc-port=18070",
											"--topo-global-server-addresses=lazy-cluster-global-topo." + testNamespace + ".svc:2379",
											"--topo-global-root=/multigres/default/lazy-cluster/global",
											"--service-map=grpc-multiadmin",
											"--pprof-http=true",
											"--log-level=info",
										},
										Ports: []corev1.ContainerPort{
											{
												Name:          "http",
												ContainerPort: 18000,
												Protocol:      corev1.ProtocolTCP,
											},
											{
												Name:          "grpc",
												ContainerPort: 18070,
												Protocol:      corev1.ProtocolTCP,
											},
										},
										Resources: resolver.DefaultResourcesAdmin(),
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path:   "/ready",
													Port:   intstr.FromInt(18000),
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
													Path:   "/live",
													Port:   intstr.FromInt(18000),
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
													Path:   "/ready",
													Port:   intstr.FromInt(18000),
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
					},
				},
				// 2b. MultiadminWeb Deployment
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "lazy-cluster-multiadmin-web",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "lazy-cluster", "multiadmin-web", ""),
						OwnerReferences: clusterOwnerRefs(t, "lazy-cluster"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultMultiadminWebReplicas),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "lazy-cluster", "multiadmin-web", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, "lazy-cluster", "multiadmin-web", ""),
								Annotations: map[string]string{
									"multigres.com/project-ref": "lazy-cluster",
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiadmin-web",
										Image: resolver.DefaultMultiadminWebImage,
										Ports: []corev1.ContainerPort{
											{
												Name:          "http",
												ContainerPort: 18100,
												Protocol:      corev1.ProtocolTCP,
											},
										},
										Resources: resolver.DefaultResourcesAdminWeb(),
										Env: []corev1.EnvVar{
											{Name: "HOSTNAME", Value: "::"},
											{Name: "MULTIADMIN_API_URL", Value: "http://lazy-cluster-multiadmin:18000"},
											{Name: "POSTGRES_HOST", Value: "lazy-cluster-multigateway"},
											{Name: "POSTGRES_PORT", Value: "5432"},
											{Name: "POSTGRES_DATABASE", Value: "postgres"},
											{Name: "POSTGRES_USER", Value: "postgres"},
										},
										StartupProbe: &corev1.Probe{
											ProbeHandler: corev1.ProbeHandler{
												HTTPGet: &corev1.HTTPGetAction{
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
													Path:   "/",
													Port:   intstr.FromInt(18100),
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
					},
				},
				// 4. Global Multigateway Service (ClusterIP by default, no externalGateway)
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "lazy-cluster-multigateway",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "lazy-cluster", "multigateway", ""),
						OwnerReferences: clusterOwnerRefs(t, "lazy-cluster"),
					},
					Spec: corev1.ServiceSpec{
						Selector: map[string]string{
							metadata.LabelAppComponent: metadata.ComponentMultigateway,
							metadata.LabelAppInstance:  "lazy-cluster",
						},
						Type: corev1.ServiceTypeClusterIP,
						Ports: []corev1.ServicePort{
							{
								Name:       "postgres",
								Port:       5432,
								TargetPort: intstr.FromString("postgres"),
								Protocol:   corev1.ProtocolTCP,
							},
						},
					},
				},
				// 5. Cell
				&multigresv1alpha1.Cell{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "lazy-cluster-zone-a",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "lazy-cluster", "", "zone-a"),
						OwnerReferences: clusterOwnerRefs(t, "lazy-cluster"),
					},
					Spec: multigresv1alpha1.CellSpec{
						Name:   "zone-a",
						ZoneID: "use1-az1",
						Images: multigresv1alpha1.CellImages{
							Multigateway:    resolver.DefaultMultigatewayImage,
							ImagePullPolicy: resolver.DefaultImagePullPolicy,
						},
						Multigateway: multigresv1alpha1.MultigatewaySpec{
							StatelessSpec: multigresv1alpha1.StatelessSpec{
								Replicas:  ptr.To(int32(1)),
								Resources: resolver.DefaultResourcesGateway(), // FIX: Expect defaults
							},
							Buffer:       &multigresv1alpha1.GatewayBufferConfig{Enabled: ptr.To(true)},
							ClusterViews: &multigresv1alpha1.GatewayClusterViewsConfig{Enabled: ptr.To(false)},
						},
						AllCells: []multigresv1alpha1.CellName{"zone-a"},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        "lazy-cluster-global-topo." + testNamespace + ".svc:2379",
							RootPath:       canonicalGlobalRoot("lazy-cluster"),
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
				},
				// 6. Injected TableGroup
				&multigresv1alpha1.TableGroup{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "lazy-cluster-8b65dfba",
						Namespace:       testNamespace,
						Labels:          tableGroupLabels("lazy-cluster", "postgres", "default"),
						OwnerReferences: clusterOwnerRefs(t, "lazy-cluster"),
					},
					Spec: multigresv1alpha1.TableGroupSpec{
						CellTopologyLabels: map[multigresv1alpha1.CellName]map[string]string{
							"zone-a": {"topology.k8s.aws/zone-id": "use1-az1"},
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
							Multiorch:       resolver.DefaultMultiorchImage,
							Multipooler:     resolver.DefaultMultipoolerImage,
							Postgres:        resolver.DefaultPostgresImage,
							ImagePullPolicy: resolver.DefaultImagePullPolicy,
						},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        "lazy-cluster-global-topo." + testNamespace + ".svc:2379",
							RootPath:       canonicalGlobalRoot("lazy-cluster"),
							Implementation: "etcd",
						},
						Shards: []multigresv1alpha1.ShardResolvedSpec{
							{
								Name: "0-inf",
								Multiorch: multigresv1alpha1.MultiorchSpec{
									Cells: []multigresv1alpha1.CellName{"zone-a"},
									StatelessSpec: multigresv1alpha1.StatelessSpec{
										Replicas:  ptr.To(int32(1)),
										Resources: resolver.DefaultResourcesOrch(), // FIX: Expect defaults
									},
								},
								// FIX: Expect the injected default pool
								Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
									"default": {
										Type:  "readWrite",
										Cells: []multigresv1alpha1.CellName{"zone-a"},
										// Single-cell pool defaults to 2 (AT_LEAST_2 minimum).
										ReplicasPerCell: ptr.To(int32(2)),
										Storage:         multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize}, // "1Gi"
										Postgres: multigresv1alpha1.ContainerConfig{
											Resources: resolver.DefaultResourcesPostgres(),
										},
										Multipooler: multigresv1alpha1.ContainerConfig{
											Resources: resolver.DefaultResourcesPooler(),
										},
									},
								},
								PVCDeletionPolicy: nil,
								Backup: &multigresv1alpha1.BackupConfig{
									Type:       multigresv1alpha1.BackupTypeFilesystem,
									Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
								},
							},
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
						Backup: &multigresv1alpha1.BackupConfig{
							Type:       multigresv1alpha1.BackupTypeFilesystem,
							Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: resolver.DefaultBackupPath, Storage: multigresv1alpha1.StorageSpec{Size: resolver.DefaultBackupStorageSize}},
						},
						TopologyPruning:   &multigresv1alpha1.TopologyPruningConfig{Enabled: ptr.To(true)},
						DurabilityPolicy:  "AT_LEAST_2",
						PostgresSuperuser: "postgres",
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			k8sClient, watcher := setupIntegration(t)

			// Patch wantResources with hashed names
			for _, obj := range tc.wantResources {
				if cell, ok := obj.(*multigresv1alpha1.Cell); ok {
					hashedName := nameutil.JoinWithConstraints(nameutil.DefaultConstraints, tc.cluster.Name, string(cell.Spec.Name))
					cell.Name = hashedName
				}
				if tg, ok := obj.(*multigresv1alpha1.TableGroup); ok {
					hashedName := nameutil.JoinWithConstraints(nameutil.DefaultConstraints, tc.cluster.Name, string(tg.Spec.DatabaseName), string(tg.Spec.TableGroupName))
					tg.Name = hashedName
					setTestTableGroupPostgresPasswordSecretRef(tg)
				}
			}

			// Create Cluster
			setTestPostgresPasswordSecretRef(tc.cluster)
			if err := k8sClient.Create(t.Context(), tc.cluster); err != nil {
				t.Fatalf("Failed to create the initial cluster, %v", err)
			}

			// Assert Resources
			if err := watcher.WaitForMatch(tc.wantResources...); err != nil {
				t.Errorf("Resources mismatch:\n%v", err)
			}

		})
	}
}

// ============================================================================
// Reconcile Gating (namespace label) Tests
// ============================================================================

// poolReplicas returns the configured ReplicasPerCell for the named pool in a
// single-shard TableGroup. ok is false when the shard or pool is absent, so it
// is safe to call from an Eventually condition (no t.Fatal off the test
// goroutine).
func poolReplicas(
	tg *multigresv1alpha1.TableGroup,
	pool multigresv1alpha1.PoolName,
) (replicas int32, ok bool) {
	if len(tg.Spec.Shards) == 0 {
		return 0, false
	}
	p, found := tg.Spec.Shards[0].Pools[pool]
	if !found {
		return 0, false
	}
	return ptr.Deref(p.ReplicasPerCell, 0), true
}

// TestMultigresCluster_ReconcileGating exercises the namespace-label reconcile
// gate end to end: gating pauses child reconciliation, the pause transition
// gets no write at all, deletion of a gated
// cluster hangs on its finalizer, removing the label resumes reconciliation
// through the namespace watch without re-touching the cluster spec, and
// un-gating a gated delete releases the finalizer so the object disappears.
func TestMultigresCluster_ReconcileGating(t *testing.T) {
	const (
		clusterName = "gated-cluster"
		gatingLabel = "multigres.com/reconcile"
	)

	var counter *testutil.CountingReconciler
	k8sClient, _, apiReader := setupIntegrationCore(
		t,
		func(inner reconcile.Reconciler) reconcile.Reconciler {
			counter = testutil.NewCountingReconciler(inner)
			return counter
		},
		nil,
	)

	ctx := t.Context()
	clusterKey := client.ObjectKey{Name: clusterName, Namespace: testNamespace}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default",
				CellTemplate:  "default",
				ShardTemplate: "default",
			},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{TemplateRef: "default"},
			Multiadmin:       &multigresv1alpha1.MultiadminConfig{TemplateRef: "default"},
			Cells:            []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name:    "postgres",
				Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name:    "default",
					Default: true,
					Shards: []multigresv1alpha1.ShardConfig{{
						Name: "0-inf",
						Spec: &multigresv1alpha1.ShardInlineSpec{
							Multiorch: multigresv1alpha1.MultiorchSpec{
								StatelessSpec: multigresv1alpha1.StatelessSpec{
									Replicas: ptr.To(int32(1)),
								},
							},
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"primary": {
									ReplicasPerCell: ptr.To(int32(3)),
									Type:            "readWrite",
									Cells:           []multigresv1alpha1.CellName{"zone-a"},
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

	// 1. The cluster reconciles normally; children appear.
	var tgName string
	require.Eventually(t, func() bool {
		tgs := &multigresv1alpha1.TableGroupList{}
		if err := k8sClient.List(ctx, tgs,
			client.InNamespace(testNamespace),
			client.MatchingLabels{metadata.LabelMultigresCluster: clusterName},
		); err != nil {
			return false
		}
		if len(tgs.Items) != 1 {
			return false
		}
		replicas, ok := poolReplicas(&tgs.Items[0], "primary")
		if !ok || replicas != 3 {
			return false
		}
		tgName = tgs.Items[0].Name
		return true
	}, testTimeout, 100*time.Millisecond, "initial TableGroup did not converge")

	tgKey := client.ObjectKey{Name: tgName, Namespace: testNamespace}

	// 2. Gate the namespace. The namespace watch enqueues the cluster and the
	// backstop must return without a write. Let any in-flight pass finish first.
	counter.WaitQuiescent(t, clusterKey)
	testutil.PatchLabel(t, k8sClient, ns, gatingLabel, "disabled")
	testutil.WaitForCachedLabel(
		ctx, t, k8sClient, ns, gatingLabel, "disabled",
	)
	counter.WaitQuiescent(t, clusterKey)

	// 3. A spec mutation while gated must not reach the child TableGroup, and
	// the cluster itself gets no write from the operator.
	var beforeTG multigresv1alpha1.TableGroup
	require.NoError(t, k8sClient.Get(ctx, tgKey, &beforeTG))

	require.NoError(t, k8sClient.Get(ctx, clusterKey, cluster))
	pool := cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["primary"]
	pool.ReplicasPerCell = ptr.To(int32(5))
	cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["primary"] = pool
	require.NoError(t, k8sClient.Update(ctx, cluster))

	require.Never(t, func() bool {
		var tg multigresv1alpha1.TableGroup
		if err := k8sClient.Get(ctx, tgKey, &tg); err != nil {
			return false
		}
		replicas, ok := poolReplicas(&tg, "primary")
		return !ok || replicas != 3 || tg.ResourceVersion != beforeTG.ResourceVersion
	}, 1500*time.Millisecond, 100*time.Millisecond, "gated cluster mutated a child")
	var gatedNow multigresv1alpha1.MultigresCluster
	require.NoError(t, apiReader.Get(ctx, clusterKey, &gatedNow))
	testutil.RequireNoWrites(ctx, t, apiReader, &gatedNow, 2*time.Second)

	// 4. Requeue-storm guard: an unrelated namespace label edit must enqueue no
	// reconcile at all.
	counter.WaitQuiescent(t, clusterKey)
	before := counter.CountFor(clusterKey)
	testutil.PatchLabel(t, k8sClient, ns, "example.com/unrelated", "x")
	require.Never(t, func() bool {
		return counter.CountFor(clusterKey) != before
	}, 1500*time.Millisecond, 100*time.Millisecond,
		"unrelated namespace label edit enqueued a reconcile")

	// 5. Watch-driven resume: removing the label must converge the pending spec
	// change without re-touching the cluster spec.
	testutil.PatchLabel(t, k8sClient, ns, gatingLabel, "")
	require.Eventually(t, func() bool {
		var tg multigresv1alpha1.TableGroup
		if err := k8sClient.Get(ctx, tgKey, &tg); err != nil {
			return false
		}
		replicas, ok := poolReplicas(&tg, "primary")
		return ok && replicas == 5
	}, testTimeout, 100*time.Millisecond, "watch-driven resume did not converge")

	// 6. A gated cluster must not be torn down: deletion hangs on the finalizer.
	testutil.PatchLabel(t, k8sClient, ns, gatingLabel, "disabled")
	testutil.WaitForCachedLabel(
		ctx, t, k8sClient, ns, gatingLabel, "disabled",
	)

	require.NoError(t, k8sClient.Delete(ctx, cluster))
	require.Never(t, func() bool {
		var c multigresv1alpha1.MultigresCluster
		err := k8sClient.Get(ctx, clusterKey, &c)
		return apierrors.IsNotFound(err)
	}, 2*time.Second, 100*time.Millisecond,
		"gated cluster was deleted despite the gate")

	var gated multigresv1alpha1.MultigresCluster
	require.NoError(t, k8sClient.Get(ctx, clusterKey, &gated))
	require.Contains(t, gated.Finalizers, multigresv1alpha1.FinalizerClusterCleanup)

	// 7. Un-gate and confirm the finalizer is actually released: the hang in
	// step 6 must be the gate and nothing else, so removing the label resumes
	// deletion and the object disappears. No assertion is made about children
	// (Cells/TableGroups) "surviving" the gated delete: envtest runs no
	// garbage-collection controller, so owner-reference cascade deletion never
	// fires here whether or not the cluster is gated, which would make that
	// assertion vacuous. The meaningful, observable signal is the released
	// finalizer.
	testutil.PatchLabel(t, k8sClient, ns, gatingLabel, "")
	require.Eventually(t, func() bool {
		var c multigresv1alpha1.MultigresCluster
		return apierrors.IsNotFound(k8sClient.Get(ctx, clusterKey, &c))
	}, testTimeout, 100*time.Millisecond,
		"un-gated cluster deletion never completed: finalizer was not released")
}

// TestMultigresCluster_LaptopModeRefusesUnclaimedNamespace pins that a laptop
// process refuses any MultigresCluster in a namespace it did not claim, even
// though that namespace carries no multigres.com/reconcile=disabled label: no
// finalizer, no child, no write. The branch lives in the shared Gate, wired
// identically into all five controllers, so this one envtest covers them all.
func TestMultigresCluster_LaptopModeRefusesUnclaimedNamespace(t *testing.T) {
	const clusterName = "unclaimed-cluster"

	k8sClient, _, apiReader := setupIntegrationCore(t, nil,
		func(r *multigrescluster.MultigresClusterReconciler) {
			r.Gate = features.NewGate("some-other-claimed-ns")
		})

	ctx := t.Context()
	clusterKey := client.ObjectKey{Name: clusterName, Namespace: testNamespace}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default",
				CellTemplate:  "default",
				ShardTemplate: "default",
			},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{TemplateRef: "default"},
			Multiadmin:       &multigresv1alpha1.MultiadminConfig{TemplateRef: "default"},
			Cells:            []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name:    "postgres",
				Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name:    "default",
					Default: true,
					Shards: []multigresv1alpha1.ShardConfig{{
						Name: "0-inf",
						Spec: &multigresv1alpha1.ShardInlineSpec{
							Multiorch: multigresv1alpha1.MultiorchSpec{
								StatelessSpec: multigresv1alpha1.StatelessSpec{
									Replicas: ptr.To(int32(1)),
								},
							},
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"primary": {
									ReplicasPerCell: ptr.To(int32(3)),
									Type:            "readWrite",
									Cells:           []multigresv1alpha1.CellName{"zone-a"},
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

	// Nothing may touch it: no write, no finalizer, no child.
	require.Never(t, func() bool {
		var c multigresv1alpha1.MultigresCluster
		if err := k8sClient.Get(ctx, clusterKey, &c); err != nil {
			return false
		}
		if slices.Contains(c.Finalizers, multigresv1alpha1.FinalizerClusterCleanup) {
			return true
		}
		tgs := &multigresv1alpha1.TableGroupList{}
		if err := k8sClient.List(ctx, tgs,
			client.InNamespace(testNamespace),
			client.MatchingLabels{metadata.LabelMultigresCluster: clusterName},
		); err != nil {
			return false
		}
		return len(tgs.Items) != 0
	}, 3*time.Second, 100*time.Millisecond,
		"laptop mode reconciled a MultigresCluster in an unclaimed namespace")
	testutil.RequireNoWrites(ctx, t, apiReader, cluster, 2*time.Second)

	// Delete must not be blocked: no finalizer was ever added, so the object
	// vanishes immediately rather than hanging for a cleanup that never comes.
	require.NoError(t, k8sClient.Delete(ctx, cluster))
	require.Eventually(t, func() bool {
		var c multigresv1alpha1.MultigresCluster
		return apierrors.IsNotFound(k8sClient.Get(ctx, clusterKey, &c))
	}, 5*time.Second, 100*time.Millisecond,
		"unclaimed cluster lingered, implying a finalizer was added")
}

// TestMultigresCluster_ReconcileGatingObjectLevel pins that the reconcile-disabled
// label, set on the MultigresCluster object itself rather than its namespace,
// pauses and resumes reconciliation. The resume half is driven by the label
// removal alone - the namespace and the spec are never touched again - so it
// fails against a For() predicate that omits features.LabelsChanged():
// projectRefOrGenerationChangedPredicate ignores metadata-only updates, so
// without that arm the removal event never reaches Reconcile.
func TestMultigresCluster_ReconcileGatingObjectLevel(t *testing.T) {
	const (
		clusterName = "object-gated-cluster"
		gatingLabel = "multigres.com/reconcile"
	)

	var counter *testutil.CountingReconciler
	k8sClient, _, apiReader := setupIntegrationCore(
		t,
		func(inner reconcile.Reconciler) reconcile.Reconciler {
			counter = testutil.NewCountingReconciler(inner)
			return counter
		},
		nil,
	)

	ctx := t.Context()
	clusterKey := client.ObjectKey{Name: clusterName, Namespace: testNamespace}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default",
				CellTemplate:  "default",
				ShardTemplate: "default",
			},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{TemplateRef: "default"},
			Multiadmin:       &multigresv1alpha1.MultiadminConfig{TemplateRef: "default"},
			Cells:            []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name:    "postgres",
				Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name:    "default",
					Default: true,
					Shards: []multigresv1alpha1.ShardConfig{{
						Name: "0-inf",
						Spec: &multigresv1alpha1.ShardInlineSpec{
							Multiorch: multigresv1alpha1.MultiorchSpec{
								StatelessSpec: multigresv1alpha1.StatelessSpec{
									Replicas: ptr.To(int32(1)),
								},
							},
							Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
								"primary": {
									ReplicasPerCell: ptr.To(int32(3)),
									Type:            "readWrite",
									Cells:           []multigresv1alpha1.CellName{"zone-a"},
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

	// 1. Reconcile normally so a child exists whose spec we can compare.
	var tgName string
	require.Eventually(t, func() bool {
		tgs := &multigresv1alpha1.TableGroupList{}
		if err := k8sClient.List(ctx, tgs,
			client.InNamespace(testNamespace),
			client.MatchingLabels{metadata.LabelMultigresCluster: clusterName},
		); err != nil {
			return false
		}
		if len(tgs.Items) != 1 {
			return false
		}
		replicas, ok := poolReplicas(&tgs.Items[0], "primary")
		if !ok || replicas != 3 {
			return false
		}
		tgName = tgs.Items[0].Name
		return true
	}, testTimeout, 100*time.Millisecond, "initial TableGroup did not converge")

	tgKey := client.ObjectKey{Name: tgName, Namespace: testNamespace}

	// 2. Gate the object itself. The namespace is left untouched, so the pause
	// transition must arrive through the For() predicate on the cluster object.
	time.Sleep(time.Second)
	testutil.PatchLabel(t, k8sClient, cluster, gatingLabel, "disabled")
	testutil.WaitForCachedLabel(ctx, t, k8sClient, cluster, gatingLabel, "disabled")
	time.Sleep(time.Second)

	// 3. A spec mutation while gated must not reach the child, and the cluster
	// gets no write from the operator.
	var beforeTG multigresv1alpha1.TableGroup
	require.NoError(t, k8sClient.Get(ctx, tgKey, &beforeTG))

	require.NoError(t, k8sClient.Get(ctx, clusterKey, cluster))
	pool := cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["primary"]
	pool.ReplicasPerCell = ptr.To(int32(5))
	cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["primary"] = pool
	require.NoError(t, k8sClient.Update(ctx, cluster))

	require.Never(t, func() bool {
		var tg multigresv1alpha1.TableGroup
		if err := k8sClient.Get(ctx, tgKey, &tg); err != nil {
			return false
		}
		replicas, ok := poolReplicas(&tg, "primary")
		return !ok || replicas != 3 || tg.ResourceVersion != beforeTG.ResourceVersion
	}, 1500*time.Millisecond, 100*time.Millisecond, "gated cluster mutated a child")
	var gatedNow multigresv1alpha1.MultigresCluster
	require.NoError(t, apiReader.Get(ctx, clusterKey, &gatedNow))
	testutil.RequireNoWrites(ctx, t, apiReader, &gatedNow, 2*time.Second)

	// 4. Remove the object's own label - spec and namespace untouched. Only the
	// LabelsChanged arm of the For() predicate can deliver this event.
	time.Sleep(time.Second)
	before := counter.CountFor(clusterKey)
	testutil.PatchLabel(t, k8sClient, cluster, gatingLabel, "")

	require.Eventually(t, func() bool {
		return counter.CountFor(clusterKey) > before
	}, 10*time.Second, 100*time.Millisecond,
		"removing the object's own label should re-enqueue the MultigresCluster")

	require.Eventually(t, func() bool {
		var tg multigresv1alpha1.TableGroup
		if err := k8sClient.Get(ctx, tgKey, &tg); err != nil {
			return false
		}
		replicas, ok := poolReplicas(&tg, "primary")
		return ok && replicas == 5
	}, testTimeout, 100*time.Millisecond,
		"removing the object's own label did not converge the pending spec change")
}
