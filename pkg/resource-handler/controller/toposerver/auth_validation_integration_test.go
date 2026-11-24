//go:build integration

package toposerver

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

func TestTopologyAuthCannotBeDisabledAfterRequested(t *testing.T) {
	scheme := certScheme()
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../..", "config", "crd", "bases")),
	)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ts := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "auth-transition", Namespace: "default"},
		Spec:       api.TopoServerSpec{Etcd: &api.EtcdSpec{}},
	}
	require.NoError(t, c.Create(t.Context(), ts))
	ts.Spec.Auth = &api.TopoServerAuthSpec{Enabled: true}
	ts.Spec.TLS = &api.TopoTLSConfig{Enabled: ptr.To(true), IssuerName: "topology-ca"}
	require.NoError(t, c.Update(t.Context(), ts), "authorization can be enabled after creation")
	for _, scenario := range []string{"false", "remove auth", "remove enabled"} {
		t.Run(scenario, func(t *testing.T) {
			fresh := &api.TopoServer{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh))
			if scenario == "remove enabled" {
				// JSON Merge Patch exercises omission independently of typed defaults.
				err = c.Patch(
					t.Context(),
					fresh,
					client.RawPatch(
						types.MergePatchType,
						[]byte(`{"spec":{"auth":{"enabled":null}}}`),
					),
				)
			} else {
				fresh.Spec.Auth.Enabled = false
				if scenario == "remove auth" {
					fresh.Spec.Auth = nil
				}
				err = c.Update(t.Context(), fresh)
			}
			require.True(
				t,
				apierrors.IsInvalid(err),
				"expected transition validation error, got %v",
				err,
			)
			if scenario == "remove enabled" {
				require.ErrorContains(t, err, "spec.auth.enabled: Required value")
			} else {
				require.ErrorContains(t, err, "managed authorization cannot be disabled")
			}
		})
	}
}

func TestTopologyAuthRequiresTLSOnManagedServer(t *testing.T) {
	scheme := certScheme()
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../..", "config", "crd", "bases")),
	)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	ts := &api.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "auth-without-tls", Namespace: "default"},
		Spec: api.TopoServerSpec{
			Auth: &api.TopoServerAuthSpec{Enabled: true},
			Etcd: &api.EtcdSpec{},
		},
	}
	err = c.Create(t.Context(), ts)
	require.True(
		t,
		apierrors.IsInvalid(err),
		"expected admission to reject authorization without TLS, got %v",
		err,
	)
	require.ErrorContains(t, err, "managed authorization requires topology TLS")
}

func TestTopologyAuthParentValidationAndTemplateDefaults(t *testing.T) {
	scheme := certScheme()
	cfg := testutil.SetUpEnvtest(
		t,
		testutil.WithCRDPaths(filepath.Join("../../../..", "config", "crd", "bases")),
	)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	// A CoreTemplate may default authorization independently of a particular
	// cluster's TLS choice. The resolved child TopoServer enforces the final
	// auth/TLS combination.
	template := &api.CoreTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "auth-default", Namespace: "default"},
		Spec: api.CoreTemplateSpec{
			GlobalTopoServer: &api.TopoServerSpec{
				Auth: &api.TopoServerAuthSpec{Enabled: true},
				Etcd: &api.EtcdSpec{},
			},
		},
	}
	require.NoError(t, c.Create(t.Context(), template))

	cluster := &api.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "inline-auth", Namespace: "default"},
		Spec: api.MultigresClusterSpec{
			GlobalTopoServer: &api.GlobalTopoServerSpec{
				Auth: &api.TopoServerAuthSpec{Enabled: true},
				Etcd: &api.EtcdSpec{},
			},
			PostgresPasswordSecretRef: api.PostgresPasswordSecretRef{
				Name: "postgres-password",
				Key:  "password",
			},
		},
	}
	err = c.Create(t.Context(), cluster)
	require.True(
		t,
		apierrors.IsInvalid(err),
		"expected inline authorization without TLS to be rejected, got %v",
		err,
	)
	require.ErrorContains(t, err, "managed authorization requires topology TLS")

	cluster.Name = "inline-auth-tls"
	cluster.ResourceVersion = ""
	cluster.Spec.TopoTLS = &api.TopoTLSConfig{Enabled: ptr.To(true), IssuerName: "topology-ca"}
	require.NoError(t, c.Create(t.Context(), cluster))

	for _, scenario := range []string{"false", "remove auth", "remove global topology"} {
		t.Run(scenario, func(t *testing.T) {
			fresh := &api.MultigresCluster{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), fresh))
			switch scenario {
			case "false":
				fresh.Spec.GlobalTopoServer.Auth.Enabled = false
			case "remove auth":
				fresh.Spec.GlobalTopoServer.Auth = nil
			case "remove global topology":
				fresh.Spec.GlobalTopoServer = nil
			}
			err = c.Update(t.Context(), fresh)
			require.True(
				t,
				apierrors.IsInvalid(err),
				"expected the parent authorization request to be irreversible, got %v",
				err,
			)
			require.ErrorContains(t, err, "managed authorization cannot be disabled")
		})
	}

	migrating := &api.MultigresCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), migrating))
	migrating.Spec.GlobalTopoServer = &api.GlobalTopoServerSpec{
		External: &api.ExternalTopoServerSpec{
			Endpoints: []api.EndpointUrl{"https://external-etcd.default.svc:2379"},
		},
	}
	require.NoError(
		t,
		c.Update(t.Context(), migrating),
		"moving to external topology may remove managed authorization",
	)

	external := &api.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "external-auth", Namespace: "default"},
		Spec: api.MultigresClusterSpec{
			GlobalTopoServer: &api.GlobalTopoServerSpec{
				Auth: &api.TopoServerAuthSpec{Enabled: true},
				External: &api.ExternalTopoServerSpec{
					Endpoints: []api.EndpointUrl{"https://external-etcd.default.svc:2379"},
				},
			},
			TopoTLS: &api.TopoTLSConfig{
				Enabled:    ptr.To(true),
				IssuerName: "topology-ca",
			},
			PostgresPasswordSecretRef: api.PostgresPasswordSecretRef{
				Name: "postgres-password",
				Key:  "password",
			},
		},
	}
	err = c.Create(t.Context(), external)
	require.True(
		t,
		apierrors.IsInvalid(err),
		"expected authorization on an external topology server to be rejected, got %v",
		err,
	)
	require.ErrorContains(t, err, "managed authorization requires a managed topology server")

	emptyAuth := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": api.GroupVersion.String(),
		"kind":       "MultigresCluster",
		"metadata": map[string]any{
			"name":      "empty-auth",
			"namespace": "default",
		},
		"spec": map[string]any{
			"topoTLS": map[string]any{"enabled": true, "issuerName": "topology-ca"},
			"globalTopoServer": map[string]any{
				"etcd": map[string]any{},
				"auth": map[string]any{},
			},
			"postgresPasswordSecretRef": map[string]any{
				"name": "postgres-password",
				"key":  "password",
			},
		},
	}}
	err = c.Create(t.Context(), emptyAuth)
	require.True(
		t,
		apierrors.IsInvalid(err),
		"expected auth without enabled to be rejected, got %v",
		err,
	)
	require.ErrorContains(t, err, "spec.globalTopoServer.auth.enabled: Required value")
}
