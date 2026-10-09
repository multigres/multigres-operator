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
	if err != nil {
		t.Fatalf("globalTopoRef() error = %v", err)
	}
	return ref
}

func TestGlobalTopoRefResolvesManagedSecrets(t *testing.T) {
	managed := &multigresv1alpha1.GlobalTopoServerSpec{
		Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(3))},
	}

	t.Run("topology TLS enabled resolves the operator-issued secret", func(t *testing.T) {
		ref := resolveTopoRef(t, topoRefCluster(
			&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)}, managed,
		))

		want := "test-cluster-topo-client-tls"
		if ref.ClientCertSecret != want {
			t.Errorf("ClientCertSecret = %q, want %q", ref.ClientCertSecret, want)
		}
		// cert-manager writes ca.crt into the same Secret as the keypair.
		if ref.CASecret != want {
			t.Errorf("CASecret = %q, want %q", ref.CASecret, want)
		}
	})

	t.Run("topology TLS unset leaves both references empty", func(t *testing.T) {
		ref := resolveTopoRef(t, topoRefCluster(nil, managed))

		if ref.ClientCertSecret != "" {
			t.Errorf("ClientCertSecret = %q, want empty", ref.ClientCertSecret)
		}
		if ref.CASecret != "" {
			t.Errorf("CASecret = %q, want empty", ref.CASecret)
		}
	})

	t.Run("topology TLS disabled leaves both references empty", func(t *testing.T) {
		ref := resolveTopoRef(t, topoRefCluster(
			&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(false)}, managed,
		))

		if ref.ClientCertSecret != "" || ref.CASecret != "" {
			t.Errorf(
				"want empty references, got CASecret=%q ClientCertSecret=%q",
				ref.CASecret, ref.ClientCertSecret,
			)
		}
	})
}

// An externally managed topology server carries its own credentials, so the
// spec's secret names are what reach the ref the cell and shard controllers
// read.
func TestGlobalTopoRefResolvesExternalSecrets(t *testing.T) {
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

	if ref.CASecret != "infra-etcd-ca" {
		t.Errorf("CASecret = %q, want infra-etcd-ca", ref.CASecret)
	}
	if ref.ClientCertSecret != "proj-123-topo-client" {
		t.Errorf("ClientCertSecret = %q, want proj-123-topo-client", ref.ClientCertSecret)
	}
}

func TestGlobalTopoRefExternalWithTopoTLS(t *testing.T) {
	enabled := &multigresv1alpha1.TopoTLSConfig{
		Enabled:    ptr.To(true),
		IssuerName: "multigres-infra-issuer",
	}

	t.Run("no secrets resolves the operator-issued secret", func(t *testing.T) {
		ref := resolveTopoRef(t, topoRefCluster(enabled, &multigresv1alpha1.GlobalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{
					"https://topo.infra.svc:2379",
				},
			},
		}))

		want := "test-cluster-topo-client-tls"
		if ref.ClientCertSecret != want || ref.CASecret != want {
			t.Errorf(
				"CASecret=%q ClientCertSecret=%q, want both %q",
				ref.CASecret, ref.ClientCertSecret, want,
			)
		}
		if ref.Address != "https://topo.infra.svc:2379" {
			t.Errorf("Address = %q, want the external endpoint", ref.Address)
		}
	})

	t.Run("named secrets take precedence", func(t *testing.T) {
		ref := resolveTopoRef(t, topoRefCluster(enabled, &multigresv1alpha1.GlobalTopoServerSpec{
			//nolint:gosec // K8s resource names, not credentials
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints:        []multigresv1alpha1.EndpointUrl{"https://etcd.infra.svc:2379"},
				CASecret:         "infra-etcd-ca",
				ClientCertSecret: "proj-123-topo-client",
			},
		}))

		if ref.CASecret != "infra-etcd-ca" {
			t.Errorf("CASecret = %q, want infra-etcd-ca", ref.CASecret)
		}
		if ref.ClientCertSecret != "proj-123-topo-client" {
			t.Errorf("ClientCertSecret = %q, want proj-123-topo-client", ref.ClientCertSecret)
		}
	})
}

func TestBuildGlobalTopoServerPropagatesTopoTLS(t *testing.T) {
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
	if err != nil {
		t.Fatalf("BuildGlobalTopoServer() error = %v", err)
	}
	if ts.Spec.TLS == nil {
		t.Fatal("TopoServer.Spec.TLS = nil, want the cluster's topology TLS config")
	}
	if !ts.Spec.TLS.IsEnabled() {
		t.Error("TopoServer.Spec.TLS is not enabled")
	}
	if ts.Spec.TLS.IssuerName != "multigres-infra-issuer" {
		t.Errorf("IssuerName = %q, want multigres-infra-issuer", ts.Spec.TLS.IssuerName)
	}
	// A deep copy, so mutating the child spec cannot reach back into the cluster.
	ts.Spec.TLS.IssuerName = "mutated"
	if cluster.Spec.TopoTLS.IssuerName != "multigres-infra-issuer" {
		t.Error("mutating the child TLS config changed the cluster spec")
	}
}
