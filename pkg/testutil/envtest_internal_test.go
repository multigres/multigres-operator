//go:build integration
// +build integration

package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/multigres/testkit/assert"
)

// TestCreateEnvtestEnvironment tests environment creation.
func TestCreateEnvtestEnvironment(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	testPaths := []string{"/test/path1", "/test/path2"}
	env := createEnvtestEnvironment(t, testPaths)

	c.Require().NotNil(env, "createEnvtestEnvironment() returned nil")

	c.Len(
		env.CRDDirectoryPaths,
		2,
		"CRDDirectoryPaths length = %d, want 2",
		len(env.CRDDirectoryPaths),
	)

	c.Eq(testPaths[0], env.CRDDirectoryPaths[0], "CRDDirectoryPaths[0]")

	c.Eq(testPaths[1], env.CRDDirectoryPaths[1], "CRDDirectoryPaths[1]")

	c.True(env.ErrorIfCRDPathMissing, "ErrorIfCRDPathMissing should be true")
}

// TestStartEnvtest tests both success and error paths.
func TestStartEnvtest(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupFunc func(t testing.TB) *envtest.Environment
		wantFatal bool
	}{
		"success - valid environment": {
			setupFunc: func(t testing.TB) *envtest.Environment {
				crdPath := filepath.Join("../../", "config", "crd", "bases")
				return createEnvtestEnvironment(t, []string{crdPath})
			},
			wantFatal: false,
		},
		"error - invalid CRD path": {
			setupFunc: func(t testing.TB) *envtest.Environment {
				return &envtest.Environment{
					CRDDirectoryPaths:     []string{"/nonexistent/invalid/path"},
					ErrorIfCRDPathMissing: true,
				}
			},
			wantFatal: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			mock := &mockTB{TB: t}
			env := tc.setupFunc(mock)
			defer env.Stop() // Safe even if Start failed

			cfg := startEnvtest(mock, env)

			c.Eq(tc.wantFatal, mock.fatalCalled, "fatalCalled")

			if !tc.wantFatal {
				c.NotNil(cfg, "Config should not be nil on success")
				c.NotEq("", cfg.Host, "Config.Host should not be empty on success")
			}
		})
	}
}

// TestCreateEnvtestDir tests that t.Fatal is called correctly.
func TestCreateEnvtestDir(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		baseDir   string
		wantFatal bool
	}{
		"success - valid base directory": {
			baseDir:   os.TempDir(),
			wantFatal: false,
		},
		"error - invalid base directory": {
			baseDir:   "/nonexistent/invalid/path",
			wantFatal: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			mock := &mockTB{TB: t}

			dir := createEnvtestDir(mock, tc.baseDir)
			t.Cleanup(func() {
				os.RemoveAll(dir)
			})

			c.Eq(tc.wantFatal, mock.fatalCalled, "fatalCalled")

			c.False(!tc.wantFatal && dir == "", "createEnvtestDir() returned empty string")
		})
	}
}

// TestWriteKubeconfigFile tests kubeconfig file writing.
func TestWriteKubeconfigFile(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupPath func(t *testing.T) string
		content   []byte
		wantFatal bool
	}{
		"success - writes to valid path": {
			setupPath: func(t *testing.T) string {
				dir := t.TempDir()
				return filepath.Join(dir, "kubeconfig")
			},
			content:   []byte("test kubeconfig content"),
			wantFatal: false,
		},
		"success - writes empty content": {
			setupPath: func(t *testing.T) string {
				dir := t.TempDir()
				return filepath.Join(dir, "kubeconfig")
			},
			content:   []byte{},
			wantFatal: false,
		},
		"success - overwrites existing file": {
			setupPath: func(t *testing.T) string {
				dir := t.TempDir()
				path := filepath.Join(dir, "kubeconfig")
				// Pre-create the file
				os.WriteFile(path, []byte("old content"), 0o600)
				return path
			},
			content:   []byte("new content"),
			wantFatal: false,
		},
		"error - invalid path (directory doesn't exist)": {
			setupPath: func(t *testing.T) string {
				return filepath.Join("/nonexistent", "invalid", "path", "kubeconfig")
			},
			content:   []byte("test content"),
			wantFatal: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			mock := &mockTB{TB: t}
			path := tc.setupPath(t)

			writeKubeconfigFile(mock, path, tc.content)

			c.Eq(tc.wantFatal, mock.fatalCalled, "fatalCalled")

			if !tc.wantFatal {
				// Verify file was written
				content, err := os.ReadFile(path)
				c.Require().NoError(err, "Failed to read written file")

				// Verify content matches
				c.Eq(string(tc.content), string(content), "File content")

				// Verify file permissions
				info, err := os.Stat(path)
				c.Require().NoError(err, "Failed to stat file")

				expectedMode := os.FileMode(0o600)
				c.Eq(expectedMode, info.Mode().Perm(), "File permissions")
			}
		})
	}
}

// TestSetUpClient tests that t.Fatal is called correctly.
func TestSetUpClient(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupFunc func(t testing.TB) (*rest.Config, *runtime.Scheme)
		wantFatal bool
	}{
		"success - valid config and scheme": {
			setupFunc: func(t testing.TB) (*rest.Config, *runtime.Scheme) {
				cfg := SetUpEnvtest(t)
				scheme := runtime.NewScheme()
				return cfg, scheme
			},
			wantFatal: false,
		},
		"error - nil config": {
			setupFunc: func(t testing.TB) (*rest.Config, *runtime.Scheme) {
				scheme := runtime.NewScheme()
				return nil, scheme
			},
			wantFatal: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mock := &mockTB{TB: t}
			cfg, scheme := tc.setupFunc(mock)

			SetUpClient(mock, cfg, scheme)

			assert.NewCollecting(t).Eq(tc.wantFatal, mock.fatalCalled, "fatalCalled")
		})
	}
}

// failingRunnable is a test runnable that fails immediately on Start.
type failingRunnable struct{}

func (f *failingRunnable) Start(ctx context.Context) error {
	return fmt.Errorf("failing runnable: intentional test failure")
}

// runCleanups executes all registered cleanup functions in reverse order.
func (m *mockTB) runCleanups() {
	for i := len(m.cleanupFuncs) - 1; i >= 0; i-- {
		m.cleanupFuncs[i]()
	}
}

// TestStartManager_Internal tests the internal startManager function.
func TestStartManager_Internal(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupFunc func(t testing.TB) (context.Context, manager.Manager)
		wantFatal bool
		wantError bool
	}{
		"success - manager starts and cache syncs": {
			setupFunc: func(t testing.TB) (context.Context, manager.Manager) {
				cfg := SetUpEnvtest(t)
				scheme := runtime.NewScheme()
				mgr := SetUpManager(t, cfg, scheme)
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				return ctx, mgr
			},
			wantFatal: false,
			wantError: false,
		},
		"error - cache sync fails": {
			setupFunc: func(t testing.TB) (context.Context, manager.Manager) {
				cfg := SetUpEnvtest(t)
				scheme := runtime.NewScheme()
				mgr := SetUpManager(t, cfg, scheme)
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately - WaitForCacheSync will fail
				return ctx, mgr
			},
			wantFatal: true,
			wantError: false,
		},
		"error - manager Start fails": {
			setupFunc: func(t testing.TB) (context.Context, manager.Manager) {
				cfg := SetUpEnvtest(t)
				scheme := runtime.NewScheme()
				mgr := SetUpManager(t, cfg, scheme)
				// Add a runnable that will fail on Start
				assert.NewAborting(t).
					NoError(mgr.Add(&failingRunnable{}), "Failed to add failing runnable")
				return t.Context(), mgr
			},
			wantFatal: false,
			wantError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			mock := &mockTB{TB: t}
			ctx, mgr := tc.setupFunc(mock)

			done := startManager(mock, ctx, mgr)

			// For success case, cancel context and wait for manager to stop
			// For error case with cancelled context, done won't be waited on
			if !mock.fatalCalled {
				mock.runCleanups()
				<-done
			}

			c.Eq(tc.wantFatal, mock.fatalCalled, "fatalCalled")

			c.Eq(tc.wantError, mock.errorCalled, "errorCalled")
		})
	}
}

// TestCleanEnvtest tests that t.Fatal is called correctly.
func TestCleanEnvtest(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupFunc func() envtestStopper
		wantFatal bool
	}{
		"success - environment stops cleanly": {
			setupFunc: func() envtestStopper {
				return &mockEnvtestStopper{err: nil}
			},
			wantFatal: false,
		},
		"error - Stop fails": {
			setupFunc: func() envtestStopper {
				return &mockEnvtestStopper{err: fmt.Errorf("stop failed")}
			},
			wantFatal: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mock := &mockTB{TB: t}
			env := tc.setupFunc()

			cleanup := cleanEnvtest(mock, env)
			cleanup()

			assert.NewCollecting(t).Eq(tc.wantFatal, mock.fatalCalled, "fatalCalled")
		})
	}
}

type mockEnvtestStopper struct {
	err error
}

func (m *mockEnvtestStopper) Stop() error {
	return m.err
}

// TestSetUpManager tests that t.Fatal is called correctly.
func TestSetUpManager(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupFunc func(t testing.TB) (*rest.Config, *runtime.Scheme)
		wantFatal bool
	}{
		"success - valid config and scheme": {
			setupFunc: func(t testing.TB) (*rest.Config, *runtime.Scheme) {
				cfg := SetUpEnvtest(t)
				scheme := runtime.NewScheme()
				return cfg, scheme
			},
			wantFatal: false,
		},
		"error - nil config": {
			setupFunc: func(t testing.TB) (*rest.Config, *runtime.Scheme) {
				scheme := runtime.NewScheme()
				return nil, scheme
			},
			wantFatal: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mock := &mockTB{TB: t}
			cfg, scheme := tc.setupFunc(mock)

			SetUpManager(mock, cfg, scheme)

			assert.NewCollecting(t).Eq(tc.wantFatal, mock.fatalCalled, "fatalCalled")
		})
	}
}

// TestGenerateKubeconfigFile tests that t.Fatal is called correctly.
func TestGenerateKubeconfigFile(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		getKubeconfig func() ([]byte, error)
		wantFatal     bool
	}{
		"success - no fatal on valid kubeconfig": {
			getKubeconfig: func() ([]byte, error) {
				return []byte("valid kubeconfig"), nil
			},
			wantFatal: false,
		},
		"error - getKubeconfig fails": {
			getKubeconfig: func() ([]byte, error) {
				return nil, fmt.Errorf("failed to get kubeconfig")
			},
			wantFatal: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			mock := &mockTB{TB: t}

			kubeconfigPath := generateKubeconfigFile(mock, tc.getKubeconfig)
			t.Cleanup(func() {
				if kubeconfigPath != "" {
					os.RemoveAll(filepath.Dir(kubeconfigPath))
				}
			})

			c.Eq(tc.wantFatal, mock.fatalCalled, "fatalCalled")

			c.False(
				!tc.wantFatal && kubeconfigPath == "",
				"kubeconfigPath should not be empty on success",
			)
		})
	}
}

// mockKubeConfigProvider implements KubeConfigProvider for testing.
type mockKubeConfigProvider struct {
	kubeConfigFunc func() ([]byte, error)
}

func (m *mockKubeConfigProvider) KubeConfig() ([]byte, error) {
	if m.kubeConfigFunc != nil {
		return m.kubeConfigFunc()
	}
	return []byte("mock-kubeconfig"), nil
}

// mockUserAdder implements UserAdder for testing.
type mockUserAdder struct {
	addUserFunc func(user envtest.User, opts *rest.Config) (KubeConfigProvider, error)
}

func (m *mockUserAdder) AddUser(user envtest.User, opts *rest.Config) (KubeConfigProvider, error) {
	if m.addUserFunc != nil {
		return m.addUserFunc(user, opts)
	}
	return &mockKubeConfigProvider{}, nil
}

// TestGetKubeconfigFromUserAdder tests the helper function directly.
func TestGetKubeconfigFromUserAdder(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		adder     UserAdder
		wantError bool
	}{
		"success": {
			adder:     &mockUserAdder{},
			wantError: false,
		},
		"error - AddUser fails": {
			adder: &mockUserAdder{
				addUserFunc: func(user envtest.User, opts *rest.Config) (KubeConfigProvider, error) {
					return nil, fmt.Errorf("AddUser failed")
				},
			},
			wantError: true,
		},
		"error - KubeConfig fails": {
			adder: &mockUserAdder{
				addUserFunc: func(user envtest.User, opts *rest.Config) (KubeConfigProvider, error) {
					return &mockKubeConfigProvider{
						kubeConfigFunc: func() ([]byte, error) {
							return nil, fmt.Errorf("KubeConfig failed")
						},
					}, nil
				},
			},
			wantError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := getKubeconfigFromUserAdder(tc.adder)
			assert.NewCollecting(t).
				ErrorWhen(tc.wantError, err, "getKubeconfigFromUserAdder() error")
		})
	}
}
