//go:build integration
// +build integration

package multigrescluster_test

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

func TestMultigresCluster_Lifecycle(t *testing.T) {
	t.Parallel()

	t.Run("TableGroup Long Name Hashing", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		// Name length math: 25 (cluster) + 8 (db) + 25 (tg) + 2 (hyphens) = 60 chars.
		longClusterName := "valid-cluster-name-123456" // 25 chars
		longTGName := "valid-tg-name-12345678901"      // 25 chars
		// Wait, we need > 63 chars to trigger hashing for labels.
		// Let's use max allowed: 25 (cluster) + 30 (db) + 25 (tg) + 2 = 82 chars.

		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: longClusterName, Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Databases: []multigresv1alpha1.DatabaseConfig{
					{
						Name:    "postgres",
						Default: true,
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							// Must provide the mandatory default one to pass "system catalog" validation
							{Name: "default", Default: true},
							// And the one that triggers length limit (Default: false to avoid "must be named default" rule)
							{Name: multigresv1alpha1.TableGroupName(longTGName), Default: false},
						},
					},
				},
				// We need a dummy cell for tablegroup validation to pass if it tries to resolve?
				// Actually, TableGroups don't need cells if not defined inline?
				// The error message might help: "unknown field Spec".
				// Ah, DatabaseConfig has fields directly embedded or defined.
				Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		// Verify the TableGroup DOES exist (hashed)
		tgName := nameutil.JoinWithConstraints(
			nameutil.DefaultConstraints,
			longClusterName,
			"postgres",
			longTGName,
		)

		expectedTG := &multigresv1alpha1.TableGroup{}
		// We just want to wait for it to exist
		found := false
		for i := 0; i < 20; i++ {
			err := k8sClient.Get(t.Context(), client.ObjectKey{Name: tgName, Namespace: testNamespace}, expectedTG)
			if err == nil {
				found = true
				break
			} else if !apierrors.IsNotFound(err) {
				t.Fatalf("Unexpected error getting TableGroup: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
		}

		if !found {
			t.Errorf("Expected TableGroup %s to be created using hashing, but it was not found after timeout", tgName)
		}

		// Ensure Cluster exists
		fetchedCluster := &multigresv1alpha1.MultigresCluster{}
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), fetchedCluster); err != nil {
			t.Error("Cluster should exist")
		}
	})

	t.Run("Annotation Limit (Bombing)", func(t *testing.T) {
		t.Parallel()
		k8sClient, watcher := setupIntegration(t)
		// 250 chars is near limit (256). If controller appends to this value, it might fail.
		longAnnotation := strings.Repeat("a", 250)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "short-annot-bomb", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Multiadmin: &multigresv1alpha1.MultiadminConfig{
					Spec: &multigresv1alpha1.StatelessSpec{
						PodAnnotations: map[string]string{"heavy-annotation": longAnnotation},
					},
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		// Verify Multiadmin Deployment created successfully WITH annotation
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "short-annot-bomb-multiadmin",
				Namespace:       testNamespace,
				Labels:          clusterLabels(t, "short-annot-bomb", "multiadmin", ""),
				OwnerReferences: clusterOwnerRefs(t, "short-annot-bomb"),
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(resolver.DefaultAdminReplicas),
				Selector: &metav1.LabelSelector{
					MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "short-annot-bomb", "multiadmin", "")),
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels:      clusterLabels(t, "short-annot-bomb", "multiadmin", ""),
						Annotations: map[string]string{"heavy-annotation": longAnnotation},
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
									"--topo-global-server-addresses=short-annot-bomb-global-topo." + testNamespace + ".svc:2379",
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
		}
		if err := watcher.WaitForMatch(deploy); err != nil {
			t.Errorf("Multiadmin deployment failed to create with massive annotation: %v", err)
		}
	})

	t.Run("Mutability (Image Update)", func(t *testing.T) {
		t.Parallel()
		k8sClient, watcher := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "mut-test", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Images: multigresv1alpha1.ClusterImages{Multiadmin: "admin:v1"},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		// Wait for v1
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{

				Name:            "mut-test-multiadmin",
				Namespace:       testNamespace,
				Labels:          clusterLabels(t, "mut-test", "multiadmin", ""),
				OwnerReferences: clusterOwnerRefs(t, "mut-test"),
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(resolver.DefaultAdminReplicas),
				Selector: &metav1.LabelSelector{
					MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "mut-test", "multiadmin", "")),
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: clusterLabels(t, "mut-test", "multiadmin", ""),
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "multiadmin",
							Image: "admin:v1",
							Command: []string{
								"/multigres/bin/multiadmin",
							},
							Args: []string{
								"--http-port=18000",
								"--grpc-port=18070",
								"--topo-global-server-addresses=mut-test-global-topo." + testNamespace + ".svc:2379",
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
						}},
					},
				},
			},
		}
		if err := watcher.WaitForMatch(deploy); err != nil {
			t.Fatalf("Failed to wait for initial deployment v1: %v", err)
		}

		// Update Image
		if err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster); err != nil {
			t.Fatal(err)
		}
		cluster.Spec.Images.Multiadmin = "admin:v2"
		if err := k8sClient.Update(t.Context(), cluster); err != nil {
			t.Fatalf("Failed to update cluster: %v", err)
		}

		// Verify v2
		deployV2 := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "mut-test-multiadmin",
				Namespace:       testNamespace,
				Labels:          clusterLabels(t, "mut-test", "multiadmin", ""),
				OwnerReferences: clusterOwnerRefs(t, "mut-test"),
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(resolver.DefaultAdminReplicas),
				Selector: &metav1.LabelSelector{
					MatchLabels: metadata.GetSelectorLabels(clusterLabels(t, "mut-test", "multiadmin", "")),
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: clusterLabels(t, "mut-test", "multiadmin", ""),
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "multiadmin",
							Image: "admin:v2",
							Command: []string{
								"/multigres/bin/multiadmin",
							},
							Args: []string{
								"--http-port=18000",
								"--grpc-port=18070",
								"--topo-global-server-addresses=mut-test-global-topo." + testNamespace + ".svc:2379",
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
						}},
					},
				},
			},
		}
		if err := watcher.WaitForMatch(deployV2); err != nil {
			t.Errorf("Deployment failed to update to v2: %v", err)
		}
	})

}
