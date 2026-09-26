package webhook

import (
	"net/http"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/multigres/testkit/assert"
)

// mockManager implements manager.Manager for testing.
// It allows dynamic scheme retrieval to simulate partial failures during setup steps.
type mockManager struct {
	manager.Manager
	client     client.Client
	server     webhook.Server
	schemeFunc func() *runtime.Scheme
}

func (m *mockManager) GetScheme() *runtime.Scheme {
	if m.schemeFunc != nil {
		return m.schemeFunc()
	}
	// Fallback to a default empty scheme if none provided
	return runtime.NewScheme()
}

func (m *mockManager) GetClient() client.Client {
	return m.client
}

func (m *mockManager) GetWebhookServer() webhook.Server {
	return m.server
}

func (m *mockManager) GetLogger() logr.Logger {
	return logr.Discard()
}

func (m *mockManager) GetConfig() *rest.Config {
	return &rest.Config{}
}

func (m *mockManager) Add(r manager.Runnable) error {
	return nil
}

type mockServer struct {
	webhook.Server
}

func (s *mockServer) Register(path string, handler http.Handler) {}
func (s *mockServer) WebhookMux() *http.ServeMux                 { return http.NewServeMux() }

// setupTestDeps helps initialize common dependencies for the test.
// It returns a valid scheme and a fake client.
func setupTestDeps(tb testing.TB) (*runtime.Scheme, client.Client) {
	tb.Helper()
	s := runtime.NewScheme()
	assert.NewAborting(tb).NoError(multigresv1alpha1.AddToScheme(s), "Failed to add scheme")
	c := fake.NewClientBuilder().WithScheme(s).Build()
	return s, c
}

func TestSetup(t *testing.T) {
	t.Parallel()

	// 1. Setup Base Fixtures
	baseScheme, baseClient := setupTestDeps(t)
	baseResolver := resolver.NewResolver(
		baseClient,
		"default",
	)

	tests := map[string]struct {
		mgrFunc     func(t *testing.T) *mockManager
		resolver    *resolver.Resolver
		opts        Options
		expectError string
	}{
		"Happy Path: Standard Configuration": {
			mgrFunc: func(t *testing.T) *mockManager {
				return &mockManager{
					schemeFunc: func() *runtime.Scheme { return baseScheme },
					client:     baseClient,
					server:     &mockServer{},
				}
			},
			resolver: baseResolver,
			opts: Options{
				Namespace:          "default",
				ServiceAccountName: "operator",
			},
		},
		"Happy Path: Generic Principal (Empty ServiceAccount)": {
			mgrFunc: func(t *testing.T) *mockManager {
				return &mockManager{
					schemeFunc: func() *runtime.Scheme { return baseScheme },
					client:     baseClient,
					server:     &mockServer{},
				}
			},
			resolver: baseResolver,
			opts:     Options{Namespace: "default"}, // Empty ServiceAccountName
		},
		"Error: Nil Resolver": {
			mgrFunc: func(t *testing.T) *mockManager {
				return &mockManager{server: &mockServer{}}
			},
			resolver:    nil,
			opts:        Options{Namespace: "default"},
			expectError: "webhook setup failed: resolver cannot be nil",
		},
		"Error: MultigresCluster Defaulter Registration Failure": {
			mgrFunc: func(t *testing.T) *mockManager {
				return &mockManager{
					// Empty scheme causes failure immediately at first registration (Defaulter)
					schemeFunc: func() *runtime.Scheme { return runtime.NewScheme() },
					client:     baseClient,
					server:     &mockServer{},
				}
			},
			resolver:    baseResolver,
			opts:        Options{Namespace: "default"},
			expectError: "failed to register MultigresCluster defaulter",
		},
		"Error: MultigresCluster Validator Registration Failure": {
			mgrFunc: func(t *testing.T) *mockManager {
				callCount := 0
				return &mockManager{
					schemeFunc: func() *runtime.Scheme {
						callCount++
						t.Logf("GetScheme called %d times (Validator Test)", callCount)
						// Previous failure showed Defaulter needs > 1 calls.
						// Setting to 5 should satisfy Defaulter (approx 2 calls).
						if callCount <= 5 {
							return baseScheme
						}
						return runtime.NewScheme()
					},
					client: baseClient,
					server: &mockServer{},
				}
			},
			resolver:    baseResolver,
			opts:        Options{Namespace: "default"},
			expectError: "failed to register MultigresCluster validator",
		},
		"Error: Template Validator Registration Failure": {
			mgrFunc: func(t *testing.T) *mockManager {
				callCount := 0
				return &mockManager{
					schemeFunc: func() *runtime.Scheme {
						callCount++
						t.Logf("GetScheme called %d times (Template Test)", callCount)
						// Pass Defaulter (2) + Validator (2) = 4 calls?
						// Let's give it 8 calls to be safe.
						if callCount <= 8 {
							return baseScheme
						}
						return runtime.NewScheme()
					},
					client: baseClient,
					server: &mockServer{},
				}
			},
			resolver:    baseResolver,
			opts:        Options{Namespace: "default"},
			expectError: "failed to register validator for",
		},
		"Error: Child Resource Validator Registration Failure": {
			mgrFunc: func(t *testing.T) *mockManager {
				callCount := 0
				return &mockManager{
					schemeFunc: func() *runtime.Scheme {
						callCount++
						t.Logf("GetScheme called %d times (Child Test)", callCount)
						// Pass Defaulter (2) + Validator (2) + Templates (3x2=6?) = ~10 calls.
						// Giving 15 as safe buffer.
						if callCount <= 15 {
							return baseScheme
						}
						// Then fail for Child Loop
						return runtime.NewScheme()
					},
					client: baseClient,
					server: &mockServer{},
				}
			},
			resolver:    baseResolver,
			opts:        Options{Namespace: "default"},
			expectError: "failed to register validator for",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			mgr := tc.mgrFunc(t)
			err := Setup(mgr, tc.resolver, tc.opts)

			if tc.expectError != "" {
				c.Require().Error(err, "Expected error containing %q, got nil", tc.expectError)
				diff := cmp.Diff(
					true,
					strings.Contains(err.Error(), tc.expectError),
				)
				c.Eq(
					"",
					diff,
					"Error message mismatch (-got +want matching check):\n%s\nGot error: %v",
					diff,
					err,
				)
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}
