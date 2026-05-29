package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// EnvtestOption is a functional option for configuring envtest setup.
type EnvtestOption func(*envtestConfig)

type envtestConfig struct {
	generateKubeconfig bool
	crdPaths           []string
}

// WithKubeconfig generates a kubeconfig file for debugging and keeps envtest running.
func WithKubeconfig() EnvtestOption {
	return func(cfg *envtestConfig) {
		cfg.generateKubeconfig = true
	}
}

// WithCRDPaths sets the CRD directory paths for envtest.
func WithCRDPaths(paths ...string) EnvtestOption {
	return func(cfg *envtestConfig) {
		cfg.crdPaths = paths
	}
}

// KubeConfigProvider abstracts the KubeConfig generation.
type KubeConfigProvider interface {
	KubeConfig() ([]byte, error)
}

// UserAdder abstracts adding a user to the control plane for testing.
type UserAdder interface {
	AddUser(user envtest.User, opts *rest.Config) (KubeConfigProvider, error)
}

// realUserAdder wraps envtest.ControlPlane to implement UserAdder.
type realUserAdder struct {
	cp *envtest.ControlPlane
}

func (r *realUserAdder) AddUser(user envtest.User, opts *rest.Config) (KubeConfigProvider, error) {
	return r.cp.AddUser(user, opts)
}

// getKubeconfigFromUserAdder generates a kubeconfig using the provided UserAdder.
func getKubeconfigFromUserAdder(adder UserAdder) ([]byte, error) {
	user, err := adder.AddUser(envtest.User{
		Name:   "envtest-admin",
		Groups: []string{"system:masters"},
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to add envtest user: %w", err)
	}
	kc, err := user.KubeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig from user: %w", err)
	}
	return kc, nil
}

// SetUpEnvtest starts Kubernetes API server for testing.
//
// This requires the envtest binary to be available. Kubebuilder setup helps
// downloading the relevant binaries into bin directory using the make script.
//
// By default, envtest is automatically cleaned up when the test finishes.
//
// Options:
//   - WithKubeconfig(): Generates kubeconfig for debugging and keeps envtest running
//   - WithCRDPaths(paths...): Sets CRD directory paths (required)
func SetUpEnvtest(t testing.TB, opts ...EnvtestOption) *rest.Config {
	t.Helper()

	cfg := &envtestConfig{}

	// Apply options
	for _, opt := range opts {
		opt(cfg)
	}

	testEnv := createEnvtestEnvironment(t, cfg.crdPaths)
	restCfg := startEnvtest(t, testEnv)

	// If kubeconfig generation is enabled, do not set up cleanup.
	if cfg.generateKubeconfig {
		// Use the real control plane for main logic.
		adder := &realUserAdder{cp: &testEnv.ControlPlane}
		kubeconfigPath := generateKubeconfigFile(t, func() ([]byte, error) {
			return getKubeconfigFromUserAdder(adder)
		})
		t.Cleanup(func() {
			fmt.Printf("Kubeconfig written to: %s\n", kubeconfigPath)
			fmt.Printf("Connect with: export KUBECONFIG=%s\n", kubeconfigPath)
		})
		return restCfg
	}

	// Default: cleanup envtest when test finishes
	t.Cleanup(cleanEnvtest(t, testEnv))

	return restCfg
}

// SetUpClient creates a direct Kubernetes client (non-cached).
//
// IMPORTANT: This creates a client that bypasses the manager's cache and reads
// directly from the API server. This is different from mgr.GetClient() which
// uses cached reads (same as controllers).
//
// For most tests, use mgr.GetClient() instead - it tests what controllers
// actually see.
//
// Note about when to use SetUpClient:
//
//  1. Testing cache synchronization:
//     - Verify what's actually in the API server vs what the cache sees
//     - Useful when debugging "why doesn't my controller see this resource?"
//
//  2. Strong consistency requirements:
//     - Need immediate reads after writes (no cache lag)
//     - Testing race conditions or timing-sensitive behavior
//
//  3. Comparing cached vs direct reads:
//     directClient := SetUpClient(t, cfg, scheme)
//     cachedClient := mgr.GetClient()
//     // Create resource
//     cachedClient.Create(ctx, obj)
//     // Direct read (guaranteed to see it)
//     directClient.Get(ctx, key, &actual)
//     // Cached read (might lag slightly)
//     cachedClient.Get(ctx, key, &fromCache)
func SetUpClient(t testing.TB, cfg *rest.Config, scheme *runtime.Scheme) client.Client {
	t.Helper()

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("Failed to setup a Kubernetes client: %v", err)
	}

	return k8sClient
}

// SetUpManager creates a controller-runtime manager for testing.
//
// The manager is created but NOT started - you must call StartManager separately.
// This separation allows you to register controllers and configure the manager
// before starting it.
//
// Typical usage:
//
//	mgr := testutil.SetUpManager(t, cfg, scheme)
//	// Register your controllers
//	reconciler := &YourReconciler{Client: mgr.GetClient(), Scheme: scheme}
//	reconciler.SetupWithManager(mgr)
//	// Start the manager
//	testutil.StartManager(t, mgr)
//
// Or use SetUpEnvtestManager for a combined setup.
func SetUpManager(t testing.TB, cfg *rest.Config, scheme *runtime.Scheme) manager.Manager {
	t.Helper()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,

		// WebhookServer:  webhook.NewServer(webhook.Options{
		// 	// Host:    webhookInstallOptions.LocalServingHost,
		// 	// Port:    webhookInstallOptions.LocalServingPort,
		// 	// CertDir: webhookInstallOptions.LocalServingCertDir,
		// }),
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		t.Fatalf("Failed to set up manager: %v", err)
	}

	return mgr
}

// StartManager starts the manager using t.Context().
//
// The manager runs in a background goroutine. If the manager fails to start
// (e.g., controller registration errors), the error will be reported via
// t.Errorf and is visible in test output.
//
// Note: t.Context() gets cancelled when the test finishes, which cleanly stops
// the manager.
func StartManager(t testing.TB, mgr manager.Manager) {
	t.Helper()

	_ = startManager(t, t.Context(), mgr)
}

// startManager is the internal implementation of StartManager that takes a context
// and returns a done channel for testing purposes. The channel is closed when the
// manager goroutine completes, allowing tests to wait for manager errors to be
// fully processed.
func startManager(t testing.TB, ctx context.Context, mgr manager.Manager) <-chan struct{} {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("Manager failed: %v", err)
		}
	}()

	// Wait for cache to sync. WaitForCacheSync returns true immediately when
	// there are no informers to wait on (e.g. an empty scheme), so also check
	// the context: a cancelled/expired context means startup did not complete.
	if !mgr.GetCache().WaitForCacheSync(ctx) || ctx.Err() != nil {
		t.Fatal("Cache failed to sync")
	}

	return done
}

// SetUpEnvtestManager is a convenience function that combines SetUpEnvtest,
// SetUpManager, and StartManager into a single call.
//
// This is the recommended way to set up integration tests:
//
//	mgr := testutil.SetUpEnvtestManager(t, scheme)
//	c := mgr.GetClient()
//
//	// Setup your controller
//	reconciler := &YourReconciler{Client: c, Scheme: scheme}
//	reconciler.SetupWithManager(mgr)
//
// Note: envtest does not support garbage collection (cascading deletion via
// owner references), because it only runs kube-apiserver and etcd, not
// kube-controller-manager where the garbage collector controller runs. To test
// cascading deletion, use kind based testing instead.
//
// Also, for more control, you can use the individual functions separately.
func SetUpEnvtestManager(
	t testing.TB,
	scheme *runtime.Scheme,
	opts ...EnvtestOption,
) manager.Manager {
	t.Helper()

	cfg := SetUpEnvtest(t, opts...)
	mgr := SetUpManager(t, cfg, scheme)
	StartManager(t, mgr)

	return mgr
}

// createEnvtestEnvironment creates an envtest.Environment with CRD paths.
// Extracted for testability.
func createEnvtestEnvironment(t testing.TB, crdPaths []string) *envtest.Environment {
	t.Helper()

	// Only error on missing CRD paths if paths are actually specified
	errorIfMissing := len(crdPaths) > 0

	return &envtest.Environment{
		CRDDirectoryPaths:     crdPaths,
		ErrorIfCRDPathMissing: errorIfMissing,
		// Increase timeout to handle resource contention when many tests run in parallel
		ControlPlaneStartTimeout: 60 * time.Second,
		ControlPlaneStopTimeout:  60 * time.Second,
	}
}

func startEnvtest(t testing.TB, testEnv *envtest.Environment) *rest.Config {
	t.Helper()

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("Setting up with envtest failed, %v", err)
	}

	return cfg
}

type envtestStopper interface {
	Stop() error
}

func cleanEnvtest(t testing.TB, testEnv envtestStopper) func() {
	t.Helper()

	return func() {
		if err := testEnv.Stop(); err != nil {
			t.Fatalf("Failed to stop envtest, %v", err)
		}
	}
}

// createEnvtestDir creates a unique temporary subdirectory under the given base directory
// for hosting envtest kubeconfigs. The directory is NOT automatically cleaned up,
// allowing the kubeconfig to persist for debugging purposes when the test environment
// is kept running.
//
// This function uses testing.TB and t.Fatal for easy corner case testing.
// Returns the path to the created directory.
func createEnvtestDir(t testing.TB, baseDir string) string {
	t.Helper()

	// Create a unique temporary directory with "envtest-" prefix
	dir, err := os.MkdirTemp(baseDir, "envtest-")
	if err != nil {
		t.Fatalf("Failed to create envtest directory: %v", err)
	}

	return dir
}

// writeKubeconfigFile writes kubeconfig content to a file at the specified path.
// This function uses testing.TB and t.Fatal for easy corner case testing.
func writeKubeconfigFile(t testing.TB, kubeconfigPath string, kubeconfig []byte) {
	t.Helper()

	if err := os.WriteFile(kubeconfigPath, kubeconfig, 0o600); err != nil {
		t.Fatalf("Failed to write kubeconfig file: %v", err)
	}
}

// generateKubeconfigFile creates a kubeconfig file for the envtest environment.
// Extracted for testability. Returns the path to the kubeconfig file.
func generateKubeconfigFile(t testing.TB, getKubeconfig func() ([]byte, error)) string {
	t.Helper()

	kubeconfig, err := getKubeconfig()
	if err != nil {
		t.Fatalf("Failed to generate kubeconfig: %v", err)
	}

	// Use a unique subdirectory for each test to avoid conflicts
	tempDir := createEnvtestDir(t, os.TempDir())
	kubeconfigPath := filepath.Join(tempDir, "kubeconfig")
	writeKubeconfigFile(t, kubeconfigPath, kubeconfig)

	return kubeconfigPath
}
