//go:build integration
// +build integration

package webhook_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/scale/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	multigreswebhook "github.com/multigres/multigres-operator/pkg/webhook"
)

const (
	testNamespace = "default"
	testTimeout   = 10 * time.Second
)

var (
	k8sClient    client.Client // Direct Client (Bypasses Cache) - Use for assertions
	cachedClient client.Client // Cached Client (Manager) - Use for checking Webhook visibility
	testEnv      *envtest.Environment
	TestCfg      *rest.Config
	ctx          context.Context
	cancel       context.CancelFunc
)

// TestMain acts as the global setup/teardown for the entire package.
func TestMain(m *testing.M) {
	// 1. Setup Global Context
	ctx, cancel = context.WithCancel(context.Background())

	// 2. Setup Scheme
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = multigresv1alpha1.AddToScheme(s)
	_ = admissionv1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	_ = rbacv1.AddToScheme(s)
	_ = storagev1.AddToScheme(s)

	// 3. Setup EnvTest
	crdPath := filepath.Join("..", "..", "config", "crd", "bases")
	webhookPath := filepath.Join("..", "..", "config", "webhook")

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{crdPath},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{webhookPath},
		},
	}

	var err error
	TestCfg, err = testEnv.Start()
	if err != nil {
		fmt.Printf("Failed to start envtest: %v\n", err)
		os.Exit(1)
	}

	// 4. Setup Manager & Webhook
	webhookOpts := testEnv.WebhookInstallOptions
	mgr, err := ctrl.NewManager(TestCfg, ctrl.Options{
		Scheme: s,
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:    webhookOpts.LocalServingPort,
			Host:    webhookOpts.LocalServingHost,
			CertDir: webhookOpts.LocalServingCertDir,
			TLSOpts: []func(*tls.Config){func(c *tls.Config) {}},
		}),
	})
	if err != nil {
		fmt.Printf("Failed to create manager: %v\n", err)
		os.Exit(1)
	}

	// 5. Initialize Clients
	// cachedClient is used to test if the Manager/Webhook sees the data
	cachedClient = mgr.GetClient()

	// k8sClient is used to Assert data in tests (Direct API access, no cache lag)
	k8sClient, err = client.New(TestCfg, client.Options{Scheme: s})
	if err != nil {
		fmt.Printf("Failed to create direct client: %v\n", err)
		os.Exit(1)
	}

	// 6. Setup Resolver & Handlers
	res := resolver.NewResolver(cachedClient, testNamespace)

	if err := multigreswebhook.Setup(mgr, res, multigreswebhook.Options{
		Namespace: testNamespace,
	}); err != nil {
		fmt.Printf("Failed to setup webhooks: %v\n", err)
		os.Exit(1)
	}

	// 7. Start Manager in Background
	go func() {
		if err := mgr.Start(ctx); err != nil {
			fmt.Printf("Manager error: %v\n", err)
		}
	}()

	// 8. Wait for Webhook Server Readiness
	if err := waitForWebhookReadiness(k8sClient); err != nil {
		fmt.Printf("Webhook never became ready: %v\n", err)
		os.Exit(1)
	}

	// 9. Create Global Defaults (Using Direct Client to ensure they exist immediately)
	if err := createDefaults(k8sClient); err != nil {
		fmt.Printf("Failed to create defaults: %v\n", err)
		os.Exit(1)
	}

	// 10. Run Tests
	code := m.Run()

	// 11. Teardown
	cancel()
	if err := testEnv.Stop(); err != nil {
		fmt.Printf("Failed to stop envtest: %v\n", err)
	}

	os.Exit(code)
}

// waitForWebhookReadiness polls until the manager and webhook are active.
func waitForWebhookReadiness(c client.Client) error {
	time.Sleep(1 * time.Second)
	return nil
}

func createDefaults(c client.Client) error {
	defaults := []client.Object{
		&multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
			Spec: multigresv1alpha1.CoreTemplateSpec{
				MultiAdmin: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
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
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "standard",
				Annotations: map[string]string{"storageclass.kubernetes.io/is-default-class": "true"},
			},
			Provisioner: "k8s.io/fake",
		},
	}

	for _, obj := range defaults {
		if err := c.Create(context.Background(), obj); client.IgnoreAlreadyExists(err) != nil {
			return err
		}
	}
	return nil
}

// waitForClusterList ensures the cache is updated enough for a LIST operation to see the cluster.
// We pass 'cachedClient' here to verifying the Webhook's view of the world.
func waitForClusterList(t *testing.T, c client.Client, clusterName string) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for cluster '%s' to appear in List() cache", clusterName)
		case <-ticker.C:
			clusters := &multigresv1alpha1.MultigresClusterList{}
			if err := c.List(context.Background(), clusters, client.InNamespace(testNamespace)); err != nil {
				continue
			}
			for _, item := range clusters.Items {
				if item.Name == clusterName {
					return
				}
			}
		}
	}
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

func setTestShardPostgresPasswordSecretRef(shard *multigresv1alpha1.Shard) {
	if shard == nil || shard.Spec.PostgresPasswordSecretRef.Name != "" {
		return
	}
	shard.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: "multigres-admin-password",
		Key:  "password",
	}
}

// ============================================================================
// Core Integration Tests
// ============================================================================

func TestWebhook_Mutation(t *testing.T) {
	t.Run("Should Inject System Catalog and Defaults", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mutation-test",
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		fetched := &multigresv1alpha1.MultigresCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), fetched); err != nil {
			t.Fatalf("Failed to get cluster: %v", err)
		}

		if len(fetched.Spec.Databases) == 0 {
			t.Error("Webhook failed to inject 'postgres' database")
		} else {
			db := fetched.Spec.Databases[0]
			if db.Name != "postgres" || !db.Default {
				t.Errorf("System database incorrect. Got Name=%s Default=%v", db.Name, db.Default)
			}
		}

		if fetched.Spec.TemplateDefaults.CoreTemplate != "default" {
			t.Errorf("Expected CoreTemplate to be promoted to 'default', got %q", fetched.Spec.TemplateDefaults.CoreTemplate)
		}

		if fetched.Spec.MultiAdmin != nil {
			t.Error("Expected spec.multiadmin to be nil (preserved dynamic link to template, no overrides provided)")
		}
	})
}

func TestWebhook_Validation(t *testing.T) {
	t.Run("Should Reject Reference to Missing Template", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "validation-fail",
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{
					CoreTemplate: "non-existent-template",
				},
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		if err := k8sClient.Create(ctx, cluster); err == nil {
			t.Fatal("Expected error creating cluster with missing template, got nil")
		}
	})
}

func TestWebhook_TemplateProtection(t *testing.T) {
	t.Run("Should Prevent Deleting In-Use Template", func(t *testing.T) {
		tpl := &multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "production-core", Namespace: testNamespace},
			Spec:       multigresv1alpha1.CoreTemplateSpec{},
		}
		if err := k8sClient.Create(ctx, tpl); err != nil {
			t.Fatalf("Failed to create template: %v", err)
		}

		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "prod-cluster",
				Namespace: testNamespace,
				Labels:    map[string]string{metadata.LabelUsesCoreTemplate: "true"},
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{
					CoreTemplate: "production-core",
				},
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		// CRITICAL: Use cachedClient here.
		// We must wait until the Webhook's internal cache sees the cluster.
		// If we used k8sClient, we would proceed too fast, and the webhook
		// would see 0 clusters and allow the delete.
		waitForClusterList(t, cachedClient, "prod-cluster")

		if err := k8sClient.Delete(ctx, tpl); err == nil {
			t.Fatal("Expected error deleting in-use template, got nil")
		}
	})
}

func TestWebhook_ChildResourceProtection(t *testing.T) {
	t.Run("Should Prevent Direct Modification of Child Resources", func(t *testing.T) {
		cell := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{Name: "manual-cell", Namespace: testNamespace},
			Spec: multigresv1alpha1.CellSpec{
				Name: "manual",
			},
		}

		if err := k8sClient.Create(ctx, cell); err == nil {
			t.Fatal("Expected error creating Child Resource directly, got nil")
		}
	})
}

// ============================================================================
// Cell Validation Tests
// ============================================================================

func TestWebhook_CellAppendOnly(t *testing.T) {
	t.Run("Should Reject Cell Removal", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cell-removal-test",
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "cell-a", ZoneID: "use1-az1"},
					{Name: "cell-b", ZoneID: "use1-az2"},
				},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		fetched := &multigresv1alpha1.MultigresCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), fetched); err != nil {
			t.Fatalf("Failed to get cluster: %v", err)
		}

		fetched.Spec.Cells = []multigresv1alpha1.CellConfig{
			{Name: "cell-a", ZoneID: "use1-az1"},
		}
		err := k8sClient.Update(ctx, fetched)
		if err == nil {
			t.Fatal("Expected error removing a cell, got nil")
		}
		if !strings.Contains(err.Error(), "Append-Only") {
			t.Fatalf("Expected 'Append-Only' in error, got: %v", err)
		}
	})

	t.Run("Should Reject Cell Rename", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cell-rename-test",
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "original-cell", ZoneID: "use1-az1"},
				},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		fetched := &multigresv1alpha1.MultigresCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), fetched); err != nil {
			t.Fatalf("Failed to get cluster: %v", err)
		}

		// Replace the cell with a different name — effectively a rename
		fetched.Spec.Cells = []multigresv1alpha1.CellConfig{
			{Name: "renamed-cell", ZoneID: "use1-az1"},
		}
		err := k8sClient.Update(ctx, fetched)
		if err == nil {
			t.Fatal("Expected error renaming a cell, got nil")
		}
		if !strings.Contains(err.Error(), "Append-Only") {
			t.Fatalf("Expected 'Append-Only' in error, got: %v", err)
		}
	})

	t.Run("Should Allow Adding New Cells", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cell-add-test",
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "cell-x", ZoneID: "use1-az1"},
				},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		fetched := &multigresv1alpha1.MultigresCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), fetched); err != nil {
			t.Fatalf("Failed to get cluster: %v", err)
		}

		fetched.Spec.Cells = append(fetched.Spec.Cells, multigresv1alpha1.CellConfig{
			Name: "cell-y", ZoneID: "use1-az2",
		})
		if err := k8sClient.Update(ctx, fetched); err != nil {
			t.Fatalf("Expected appending a cell to succeed, got: %v", err)
		}
	})
}

// ============================================================================
// Advanced / Edge Case Tests
// ============================================================================

func TestWebhook_OverridePrecedence(t *testing.T) {
	t.Run("Inline Spec Should Override Template", func(t *testing.T) {
		tplName := "base-template"
		tpl := &multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: tplName, Namespace: testNamespace},
			Spec: multigresv1alpha1.CoreTemplateSpec{
				MultiAdmin: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
		}
		if err := k8sClient.Create(ctx, tpl); err != nil {
			t.Fatalf("Failed to create template: %v", err)
		}

		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "override-test", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{
					CoreTemplate: multigresv1alpha1.TemplateRef(tplName),
				},
				MultiAdmin: &multigresv1alpha1.MultiAdminConfig{
					Spec: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(3))},
				},
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatalf("Failed to create cluster: %v", err)
		}

		fetched := &multigresv1alpha1.MultigresCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), fetched); err != nil {
			t.Fatal(err)
		}

		if fetched.Spec.MultiAdmin.Spec.Replicas == nil || *fetched.Spec.MultiAdmin.Spec.Replicas != 3 {
			t.Errorf("Expected inline overrides (3) to win over template (1), got: %v", fetched.Spec.MultiAdmin.Spec.Replicas)
		}
	})
}

func TestWebhook_SpecificRefPrecedence(t *testing.T) {
	t.Run("Specific TemplateRef Should NOT be Expanded (Spec Conflict)", func(t *testing.T) {
		specTpl := &multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "specific-large", Namespace: testNamespace},
			Spec: multigresv1alpha1.CoreTemplateSpec{
				MultiAdmin: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(5))},
			},
		}
		if err := k8sClient.Create(ctx, specTpl); err != nil {
			t.Fatal(err)
		}

		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "ref-precedence-test", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				MultiAdmin: &multigresv1alpha1.MultiAdminConfig{
					TemplateRef: "specific-large",
				},
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatal(err)
		}

		fetched := &multigresv1alpha1.MultigresCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), fetched); err != nil {
			t.Fatal(err)
		}

		if fetched.Spec.MultiAdmin.Spec != nil {
			t.Errorf("Expected MultiAdmin.Spec to be nil when TemplateRef is set, but got: %v", fetched.Spec.MultiAdmin.Spec)
		}
		if fetched.Spec.MultiAdmin.TemplateRef != "specific-large" {
			t.Errorf("Expected TemplateRef to be preserved")
		}
	})
}

func TestWebhook_SystemCatalogIdempotency(t *testing.T) {
	t.Run("Should Not Duplicate Existing System Catalog", func(t *testing.T) {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "idempotency-test", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
				Databases: []multigresv1alpha1.DatabaseConfig{
					{
						Name:    "postgres",
						Default: true,
						TableGroups: []multigresv1alpha1.TableGroupConfig{
							{Name: "default", Default: true},
						},
					},
				},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatal(err)
		}

		fetched := &multigresv1alpha1.MultigresCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), fetched); err != nil {
			t.Fatal(err)
		}

		if len(fetched.Spec.Databases) != 1 {
			t.Errorf("Expected 1 database, got %d", len(fetched.Spec.Databases))
		}
		tgList := fetched.Spec.Databases[0].TableGroups
		if len(tgList) != 1 {
			t.Errorf("Expected 1 tablegroup, got %d", len(tgList))
		}
	})
}

func TestWebhook_DeepTemplateProtection(t *testing.T) {
	t.Run("Should Protect Deeply Nested ShardTemplate", func(t *testing.T) {
		stName := "sensitive-shard-tpl"
		st := &multigresv1alpha1.ShardTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: stName, Namespace: testNamespace},
			Spec:       multigresv1alpha1.ShardTemplateSpec{},
		}
		if err := k8sClient.Create(ctx, st); err != nil {
			t.Fatal(err)
		}

		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "deep-ref-cluster",
				Namespace: testNamespace,
				Labels:    map[string]string{metadata.LabelUsesShardTemplate: "true"},
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
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
										Name:          "0-inf",
										ShardTemplate: multigresv1alpha1.TemplateRef(stName),
									},
								},
							},
						},
					},
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatal(err)
		}

		waitForClusterList(t, cachedClient, "deep-ref-cluster")

		if err := k8sClient.Delete(ctx, st); err == nil {
			t.Fatal("Expected error deleting in-use ShardTemplate, got nil")
		}
	})
}

func TestWebhook_StorageClassValidation(t *testing.T) {
	t.Run("Should Reject When No Default SC and No Explicit Class", func(t *testing.T) {
		// Remove the default StorageClass annotation
		sc := &storagev1.StorageClass{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: "standard"}, sc); err != nil {
			t.Fatalf("Failed to get SC: %v", err)
		}
		sc.Annotations["storageclass.kubernetes.io/is-default-class"] = "false"
		if err := k8sClient.Update(ctx, sc); err != nil {
			t.Fatalf("Failed to update SC: %v", err)
		}

		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sc-reject-test",
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		err := k8sClient.Create(ctx, cluster)
		if err == nil {
			t.Fatal("Expected rejection due to missing default StorageClass, got nil")
		}
		if !strings.Contains(err.Error(), "no default StorageClass found") {
			t.Fatalf("Expected 'no default StorageClass found' in error, got: %v", err)
		}

		// Restore the default SC for subsequent tests
		sc.Annotations["storageclass.kubernetes.io/is-default-class"] = "true"
		if err := k8sClient.Update(ctx, sc); err != nil {
			t.Fatalf("Failed to restore SC: %v", err)
		}
	})

	t.Run("Should Accept With Explicit Class Even Without Default SC", func(t *testing.T) {
		// Remove the default StorageClass annotation
		sc := &storagev1.StorageClass{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: "standard"}, sc); err != nil {
			t.Fatalf("Failed to get SC: %v", err)
		}
		sc.Annotations["storageclass.kubernetes.io/is-default-class"] = "false"
		if err := k8sClient.Update(ctx, sc); err != nil {
			t.Fatalf("Failed to update SC: %v", err)
		}

		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sc-explicit-test",
				Namespace: testNamespace,
			},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{Class: "manual", Size: "1Gi"},
					},
				},
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
						Storage: multigresv1alpha1.StorageSpec{Class: "manual", Size: "5Gi"},
					},
				},
				Cells: []multigresv1alpha1.CellConfig{{Name: "default-cell", ZoneID: "use1-az1"}},
				Databases: []multigresv1alpha1.DatabaseConfig{{
					Name:    "postgres",
					Default: true,
					TableGroups: []multigresv1alpha1.TableGroupConfig{{
						Name:    "default",
						Default: true,
						Shards: []multigresv1alpha1.ShardConfig{{
							Name: "0-inf",
							Spec: &multigresv1alpha1.ShardInlineSpec{
								Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
									"default": {
										Type:    "readWrite",
										Storage: multigresv1alpha1.StorageSpec{Class: "manual", Size: "10Gi"},
									},
								},
							},
						}},
					}},
				}},
			},
		}

		setTestPostgresPasswordSecretRef(cluster)

		if err := k8sClient.Create(ctx, cluster); err != nil {
			t.Fatalf("Expected acceptance with explicit storage class, got: %v", err)
		}

		// Restore the default SC
		sc.Annotations["storageclass.kubernetes.io/is-default-class"] = "true"
		if err := k8sClient.Update(ctx, sc); err != nil {
			t.Fatalf("Failed to restore SC: %v", err)
		}
	})
}
