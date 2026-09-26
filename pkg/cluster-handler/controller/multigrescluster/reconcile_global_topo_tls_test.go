package multigrescluster

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"

	"github.com/multigres/testkit/assert"
)

func topoRefCluster(
	topoTLS *multigresv1alpha1.TopoTLSConfig,
	globalTopo *multigresv1alpha1.GlobalTopoServerSpec,
) *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "multigres.com/v1alpha1",
			Kind:       "MultigresCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "supabase",
			UID:       "cluster-uid",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TopoTLS:          topoTLS,
			GlobalTopoServer: globalTopo,
		},
	}
}

func resolveTopoRef(
	t *testing.T,
	cluster *multigresv1alpha1.MultigresCluster,
) multigresv1alpha1.GlobalTopoServerRef {
	t.Helper()
	scheme := setupScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	ref, err := r.globalTopoRef(
		context.Background(), cluster, resolver.NewResolver(c, cluster.Namespace),
	)
	assert.NewAborting(t).NoError(err, "globalTopoRef() error =")
	return ref
}

func TestGlobalTopoRefResolvesManagedSecrets(t *testing.T) {
	managed := &multigresv1alpha1.GlobalTopoServerSpec{
		Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(3))},
	}

	t.Run("topology TLS enabled resolves the operator-issued secret", func(t *testing.T) {
		c := assert.NewCollecting(t)
		ref := resolveTopoRef(t, topoRefCluster(
			&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)}, managed,
		))

		want := "test-cluster-topo-client-tls"
		c.Eq(want, ref.ClientCertSecret, "ClientCertSecret")
		// cert-manager writes ca.crt into the same Secret as the keypair.
		c.Eq(want, ref.CASecret, "CASecret")
	})

	t.Run("topology TLS unset leaves both references empty", func(t *testing.T) {
		c := assert.NewCollecting(t)
		ref := resolveTopoRef(t, topoRefCluster(nil, managed))

		c.Eq("", ref.ClientCertSecret, "ClientCertSecret")
		c.Eq("", ref.CASecret, "CASecret")
	})

	t.Run("topology TLS disabled leaves both references empty", func(t *testing.T) {
		ref := resolveTopoRef(t, topoRefCluster(
			&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(false)}, managed,
		))

		assert.NewCollecting(t).
			False(ref.ClientCertSecret != "" || ref.CASecret != "", "want empty references, got CASecret=%q ClientCertSecret=%q", ref.CASecret, ref.ClientCertSecret)
	})
}

// An externally managed topology server carries its own credentials, so the
// spec's secret names are what reach the ref the cell and shard controllers
// read.
func TestGlobalTopoRefResolvesExternalSecrets(t *testing.T) {
	c := assert.NewCollecting(t)
	ref := resolveTopoRef(t, topoRefCluster(nil, &multigresv1alpha1.GlobalTopoServerSpec{
		//nolint:gosec // K8s resource names, not credentials
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints:        []multigresv1alpha1.EndpointUrl{"https://etcd.infra.svc:2379"},
			Implementation:   "etcd",
			RootPath:         "/multigres/proj_123/global",
			CASecret:         "infra-etcd-ca",
			ClientCertSecret: "proj-123-topo-client",
		},
	}))

	c.Eq("infra-etcd-ca", ref.CASecret, "CASecret")
	c.Eq("proj-123-topo-client", ref.ClientCertSecret, "ClientCertSecret")
}

func TestBuildGlobalTopoServerPropagatesTopoTLS(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := setupScheme()
	tls := &multigresv1alpha1.TopoTLSConfig{
		Enabled:    ptr.To(true),
		IssuerName: "multigres-infra-issuer",
	}
	cluster := topoRefCluster(tls, nil)
	spec := &multigresv1alpha1.GlobalTopoServerSpec{
		Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(3))},
	}

	ts, err := BuildGlobalTopoServer(cluster, spec, scheme)
	c.Require().NoError(err, "BuildGlobalTopoServer() error =")
	c.Require().
		NotNil(ts.Spec.TLS, "TopoServer.Spec.TLS = nil, want the cluster's topology TLS config")
	c.True(ts.Spec.TLS.IsEnabled(), "TopoServer.Spec.TLS is not enabled")
	c.Eq("multigres-infra-issuer", ts.Spec.TLS.IssuerName, "IssuerName")
	// A deep copy, so mutating the child spec cannot reach back into the cluster.
	ts.Spec.TLS.IssuerName = "mutated"
	c.Eq(
		"multigres-infra-issuer",
		cluster.Spec.TopoTLS.IssuerName,
		"mutating the child TLS config changed the cluster spec",
	)
}
