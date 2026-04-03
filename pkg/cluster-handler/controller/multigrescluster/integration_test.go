//go:build integration
// +build integration

package multigrescluster_test

import (
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
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

// setupIntegration bootstraps the test environment, controller, and default templates.
// It returns a ready-to-use K8s Client and a ResourceWatcher.
func setupIntegration(t *testing.T) (client.Client, *testutil.ResourceWatcher) {
	t.Helper()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

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

	if err := reconciler.SetupWithManager(mgr, controller.Options{
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
				MultiGateway: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
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

	return k8sClient, watcher
}

func clusterLabels(t testing.TB, clusterName, oldApp, cell string) map[string]string {
	t.Helper()
	var component string
	if cell != "" {
		component = metadata.ComponentCell
	} else if oldApp == "multiadmin" {
		component = metadata.ComponentMultiAdmin
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
						MultiGateway:     "gateway:latest",
						MultiOrch:        "orch:latest",
						MultiPooler:      "pooler:latest",
						MultiAdmin:       "admin:latest",
						Postgres:         "postgres:15",
						ImagePullPolicy:  corev1.PullAlways,
						ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
					},
					GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
						Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:latest"},
					},
					MultiAdmin: &multigresv1alpha1.MultiAdminConfig{
						Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a", Zone: "us-east-1a", Spec: &multigresv1alpha1.CellInlineSpec{
							MultiGateway: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
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
											MultiOrch: multigresv1alpha1.MultiOrchSpec{StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))}},
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
							RootPath:  resolver.DefaultTopoRootPath,
							Storage:   multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize},
							Resources: resolver.DefaultResourcesEtcd(),
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
					},
				},
				// 2. MultiAdmin Deployment
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
											"--topo-global-root=/multigres/global",
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
				// 3. MultiAdminWeb Deployment
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            clusterName + "-multiadmin-web",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, clusterName, "multiadmin-web", ""),
						OwnerReferences: clusterOwnerRefs(t, clusterName),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultMultiAdminWebReplicas), // Defaults
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, clusterName, "multiadmin-web", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, clusterName, "multiadmin-web", ""),
							},
							Spec: corev1.PodSpec{
								ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
								Containers: []corev1.Container{
									{
										Name:  "multiadmin-web",
										Image: resolver.DefaultMultiAdminWebImage, // Default
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
							metadata.LabelAppComponent: metadata.ComponentMultiGateway,
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
						Name: "zone-a",
						Zone: "us-east-1a",
						Images: multigresv1alpha1.CellImages{
							MultiGateway:     "gateway:latest",
							ImagePullPolicy:  corev1.PullAlways,
							ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
						},
						MultiGateway: multigresv1alpha1.StatelessSpec{
							Replicas:  ptr.To(int32(1)),
							Resources: resolver.DefaultResourcesGateway(), // Expected default
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
							MultiOrch:        "orch:latest",
							MultiPooler:      "pooler:latest",
							Postgres:         "postgres:15",
							ImagePullPolicy:  corev1.PullAlways,
							ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
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
					MultiAdmin: &multigresv1alpha1.MultiAdminConfig{
						TemplateRef: "default",
					},
					Cells: []multigresv1alpha1.CellConfig{
						{Name: "zone-a", Zone: "us-east-1a"},
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
							RootPath:  resolver.DefaultTopoRootPath,
							Storage:   multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize},
							Resources: resolver.DefaultResourcesEtcd(),
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
					},
				},
				// 2. MultiAdmin
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
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiadmin",
										Image: resolver.DefaultMultiAdminImage,
										Command: []string{
											"/multigres/bin/multiadmin",
										},
										Args: []string{
											"--http-port=18000",
											"--grpc-port=18070",
											"--topo-global-server-addresses=minimal-cluster-global-topo." + testNamespace + ".svc:2379",
											"--topo-global-root=/multigres/global",
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
				// 2b. MultiAdminWeb Deployment
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "minimal-cluster-multiadmin-web",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "minimal-cluster", "multiadmin-web", ""),
						OwnerReferences: clusterOwnerRefs(t, "minimal-cluster"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultMultiAdminWebReplicas),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "minimal-cluster", "multiadmin-web", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, "minimal-cluster", "multiadmin-web", ""),
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiadmin-web",
										Image: resolver.DefaultMultiAdminWebImage,
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
							metadata.LabelAppComponent: metadata.ComponentMultiGateway,
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
						Name: "zone-a",
						Zone: "us-east-1a",
						Images: multigresv1alpha1.CellImages{
							MultiGateway:    resolver.DefaultMultiGatewayImage,
							ImagePullPolicy: resolver.DefaultImagePullPolicy,
						},
						MultiGateway: multigresv1alpha1.StatelessSpec{
							Replicas:  ptr.To(int32(1)), // From default template
							Resources: resolver.DefaultResourcesGateway(),
						},
						AllCells: []multigresv1alpha1.CellName{"zone-a"},
						GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
							Address:        "minimal-cluster-global-topo." + testNamespace + ".svc:2379",
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
							Address:        "minimal-cluster-global-topo." + testNamespace + ".svc:2379",
							RootPath:       "/multigres/global",
							Implementation: "etcd",
						},
						Shards: []multigresv1alpha1.ShardResolvedSpec{
							{
								Name: "0-inf",
								MultiOrch: multigresv1alpha1.MultiOrchSpec{
									Cells: []multigresv1alpha1.CellName{"zone-a"},
									StatelessSpec: multigresv1alpha1.StatelessSpec{
										Replicas:  ptr.To(int32(1)),
										Resources: resolver.DefaultResourcesOrch(),
									},
								},
								// FIX: Expect the injected default pool
								Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
									"default": {
										Type:            "readWrite",
										Cells:           []multigresv1alpha1.CellName{"zone-a"},
										ReplicasPerCell: ptr.To(int32(3)),
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
						{Name: "zone-a", Zone: "us-east-1a"},
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
							RootPath:  resolver.DefaultTopoRootPath,
							Storage:   multigresv1alpha1.StorageSpec{Size: resolver.DefaultEtcdStorageSize},
							Resources: resolver.DefaultResourcesEtcd(),
						},
						PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
							WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
						},
					},
				},
				// 2. MultiAdmin (Resolved from default template)
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
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiadmin",
										Image: resolver.DefaultMultiAdminImage,
										Command: []string{
											"/multigres/bin/multiadmin",
										},
										Args: []string{
											"--http-port=18000",
											"--grpc-port=18070",
											"--topo-global-server-addresses=lazy-cluster-global-topo." + testNamespace + ".svc:2379",
											"--topo-global-root=/multigres/global",
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
				// 2b. MultiAdminWeb Deployment
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:            "lazy-cluster-multiadmin-web",
						Namespace:       testNamespace,
						Labels:          clusterLabels(t, "lazy-cluster", "multiadmin-web", ""),
						OwnerReferences: clusterOwnerRefs(t, "lazy-cluster"),
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(resolver.DefaultMultiAdminWebReplicas),
						Selector: &metav1.LabelSelector{
							MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "lazy-cluster", "multiadmin-web", "")),
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: clusterLabels(t, "lazy-cluster", "multiadmin-web", ""),
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "multiadmin-web",
										Image: resolver.DefaultMultiAdminWebImage,
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
							metadata.LabelAppComponent: metadata.ComponentMultiGateway,
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
							Address:        "lazy-cluster-global-topo." + testNamespace + ".svc:2379",
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
							Address:        "lazy-cluster-global-topo." + testNamespace + ".svc:2379",
							RootPath:       "/multigres/global",
							Implementation: "etcd",
						},
						Shards: []multigresv1alpha1.ShardResolvedSpec{
							{
								Name: "0-inf",
								MultiOrch: multigresv1alpha1.MultiOrchSpec{
									Cells: []multigresv1alpha1.CellName{"zone-a"},
									StatelessSpec: multigresv1alpha1.StatelessSpec{
										Replicas:  ptr.To(int32(1)),
										Resources: resolver.DefaultResourcesOrch(), // FIX: Expect defaults
									},
								},
								// FIX: Expect the injected default pool
								Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
									"default": {
										Type:            "readWrite",
										Cells:           []multigresv1alpha1.CellName{"zone-a"},
										ReplicasPerCell: ptr.To(int32(3)),
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
				}
			}

			// Create Cluster
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
