//go:build integration
// +build integration

package testutil_test

import (
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	"github.com/multigres/multigres-operator/pkg/testutil"

	"github.com/multigres/testkit/assert"
)

func TestSetUpEnvTest(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		opts []testutil.EnvtestOption
	}{
		"default - with cleanup": {
			opts: nil,
		},
		"with kubeconfig": {
			opts: []testutil.EnvtestOption{testutil.WithKubeconfig()},
		},
		"with CRD path": {
			opts: []testutil.EnvtestOption{testutil.WithCRDPaths(
				filepath.Join("../../", "config", "crd", "bases"),
			)},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewAborting(t)

			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)

			cfg := testutil.SetUpEnvtest(t, tc.opts...)
			mgr := testutil.SetUpManager(t, cfg, scheme)

			c.NoError(mgr.AddHealthzCheck("healthz", healthz.Ping), "Failed to set up health check")
			c.NoError(mgr.AddReadyzCheck("readyz", healthz.Ping), "Failed to set up ready check")

			testutil.StartManager(t, mgr)
		})
	}
}

func TestSetUpClient(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	cfg := testutil.SetUpEnvtest(t)
	client := testutil.SetUpClient(t, cfg, scheme)

	c.Require().NotNil(client, "SetUpClient() returned nil")

	// Verify client works by listing services
	svcList := &corev1.ServiceList{}
	c.NoError(client.List(t.Context(), svcList), "Client.List() failed")

	// Create and retrieve a service (tests direct API server access without cache)
	svc := &corev1.Service{
		ObjectMeta: testutil.Obj[corev1.Service]("test-svc", "default").ObjectMeta,
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}
	c.Require().NoError(client.Create(t.Context(), svc), "Client.Create() failed")

	retrieved := &corev1.Service{}
	objKey := types.NamespacedName{Name: "test-svc", Namespace: "default"}
	c.NoError(client.Get(t.Context(), objKey, retrieved), "Client.Get() failed")

	c.Eq("test-svc", retrieved.Name, "Retrieved service Name")
}
