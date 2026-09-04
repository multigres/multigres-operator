package multigrescluster

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func topoTLSCluster(
	name, namespace string,
	annotations map[string]string,
) *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "multigres.com/v1alpha1",
			Kind:       "MultigresCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			UID:         "cluster-uid",
			Annotations: annotations,
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			// A cluster issuer distinct from the topology issuer, so a
			// credential signed by the wrong CA is visible in tests.
			IssuerName: "cluster-issuer",
			TopoTLS: &multigresv1alpha1.TopoTLSConfig{
				Enabled:    ptr.To(true),
				IssuerName: "multigres-infra-issuer",
			},
		},
	}
}

func TestBuildTopoClientCertificate(t *testing.T) {
	tests := map[string]struct {
		cluster     *multigresv1alpha1.MultigresCluster
		wantSubject string
	}{
		"project ref annotation is the cluster identity": {
			cluster: topoTLSCluster("test-cluster", "supabase", map[string]string{
				metadata.AnnotationProjectRef: "proj_123",
			}),
			wantSubject: "/multigres/proj_123",
		},
		"empty project ref falls back to namespace and cluster name": {
			cluster: topoTLSCluster("test-cluster", "supabase", map[string]string{
				metadata.AnnotationProjectRef: "",
			}),
			wantSubject: "/multigres/supabase/test-cluster",
		},
		"absent annotation falls back to namespace and cluster name": {
			cluster:     topoTLSCluster("test-cluster", "supabase", nil),
			wantSubject: "/multigres/supabase/test-cluster",
		},
		"unsafe path characters are percent-encoded": {
			cluster: topoTLSCluster("test-cluster", "supabase", map[string]string{
				metadata.AnnotationProjectRef: "proj/123",
			}),
			wantSubject: "/multigres/proj%2F123",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := setupScheme()
			got, err := buildTopoClientCertificate(tc.cluster, scheme)
			if err != nil {
				t.Fatalf("buildTopoClientCertificate() error = %v", err)
			}

			wantName := tc.cluster.Name + "-topo-client-tls"
			if got.GetName() != wantName {
				t.Errorf("name = %q, want %q", got.GetName(), wantName)
			}
			if got.GetNamespace() != tc.cluster.Namespace {
				t.Errorf("namespace = %q, want %q", got.GetNamespace(), tc.cluster.Namespace)
			}

			ownerRefs := got.GetOwnerReferences()
			if len(ownerRefs) != 1 || ownerRefs[0].Kind != "MultigresCluster" {
				t.Fatalf("ownerReferences = %+v, want one MultigresCluster ref", ownerRefs)
			}

			spec, ok := got.Object["spec"].(map[string]any)
			if !ok {
				t.Fatal("spec is not a map")
			}
			wantSubject := fmt.Sprintf(CertLiteralSubjectTemplate, tc.wantSubject)
			if diff := cmp.Diff(wantSubject, spec["literalSubject"]); diff != "" {
				t.Errorf("literalSubject mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(wantName, spec["secretName"]); diff != "" {
				t.Errorf("secretName mismatch (-want +got):\n%s", diff)
			}
			// A client credential is verified by subject, so it carries no SANs.
			if diff := cmp.Diff([]any{}, spec["dnsNames"]); diff != "" {
				t.Errorf("dnsNames mismatch (-want +got):\n%s", diff)
			}
			wantUsages := []any{
				"digital signature",
				"key encipherment",
				"client auth",
			}
			if diff := cmp.Diff(wantUsages, spec["usages"]); diff != "" {
				t.Errorf("usages mismatch (-want +got):\n%s", diff)
			}
			// The credential is only useful if it chains to the CA the
			// topology server trusts, never to the cluster's own issuer.
			wantIssuerRef := map[string]any{
				"name":  "multigres-infra-issuer",
				"kind":  "ClusterIssuer",
				"group": "cert-manager.io",
			}
			if diff := cmp.Diff(wantIssuerRef, spec["issuerRef"]); diff != "" {
				t.Errorf("issuerRef mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// The CN is the identity a topology server authorizes against, so it has to
// be the string ClusterRoot() produces and not a re-derivation of it.
func TestBuildTopoClientCertificateCommonNameMatchesTopologyRoot(t *testing.T) {
	scheme := setupScheme()
	cluster := topoTLSCluster("test-cluster", "supabase", map[string]string{
		metadata.AnnotationProjectRef: "proj_123",
	})

	cert, err := buildTopoClientCertificate(cluster, scheme)
	if err != nil {
		t.Fatalf("buildTopoClientCertificate() error = %v", err)
	}
	subject, _, _ := unstructured.NestedString(cert.Object, "spec", "literalSubject")

	globalRoot := "/multigres/proj_123/global"
	cn, ok := parseCommonName(subject)
	if !ok {
		t.Fatalf("no CN in literalSubject %q", subject)
	}
	if got := cn + "/global"; got != globalRoot {
		t.Errorf("CN %q does not prefix the global root: got %q, want %q", cn, got, globalRoot)
	}
}

func parseCommonName(subject string) (string, bool) {
	const marker = "CN="
	i := strings.LastIndex(subject, marker)
	if i < 0 {
		return "", false
	}
	return subject[i+len(marker):], true
}

func TestReconcileCertificateTopoTLS(t *testing.T) {
	certName := "test-cluster-topo-client-tls"

	t.Run("issues the client certificate when topology TLS is enabled", func(t *testing.T) {
		scheme := setupScheme()
		cluster := topoTLSCluster("test-cluster", "supabase", nil)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), cluster); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		if err := c.Get(context.Background(), key, got); err != nil {
			t.Fatalf("expected topology client Certificate, got error %v", err)
		}
	})

	t.Run("prunes the client certificate when topology TLS is disabled", func(t *testing.T) {
		scheme := setupScheme()
		cluster := topoTLSCluster("test-cluster", "supabase", nil)
		cluster.Spec.TopoTLS.Enabled = ptr.To(false)

		existing, err := buildTopoClientCertificate(
			topoTLSCluster("test-cluster", "supabase", nil), scheme,
		)
		if err != nil {
			t.Fatalf("buildTopoClientCertificate() error = %v", err)
		}
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster, existing).
			Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), cluster); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		err = c.Get(context.Background(), key, got)
		if err == nil {
			t.Fatal("expected topology client Certificate to be deleted")
		}
	})

	t.Run("issues nothing when the topology server is external", func(t *testing.T) {
		scheme := setupScheme()
		cluster := topoTLSCluster("test-cluster", "supabase", nil)
		// An external topology server brings its own CA and client Secrets, so
		// the operator does not issue a credential from the cluster's issuer.
		cluster.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{"https://etcd.example.com:2379"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), cluster); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		if err := c.Get(context.Background(), key, got); err == nil {
			t.Fatal("expected no topology client Certificate for an external topology server")
		}
	})

	t.Run("issues nothing when topology TLS is unset", func(t *testing.T) {
		scheme := setupScheme()
		cluster := topoTLSCluster("test-cluster", "supabase", nil)
		cluster.Spec.TopoTLS = nil
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), cluster); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(certGVK)
		if err := c.List(context.Background(), list); err != nil {
			t.Fatalf("List() error = %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("got %d Certificates, want 0", len(list.Items))
		}
	})
}

// A truncated common name would no longer name the prefix it is meant to
// authorize, so an over-long topology root fails loudly instead.
func TestBuildTopoClientCertificateRejectsOverLongRoot(t *testing.T) {
	scheme := setupScheme()
	cluster := topoTLSCluster("test-cluster", "supabase", map[string]string{
		metadata.AnnotationProjectRef: strings.Repeat("p", 64),
	})

	if _, err := buildTopoClientCertificate(cluster, scheme); err == nil {
		t.Fatal("buildTopoClientCertificate() = nil error, want a common name limit error")
	}
}

// Internal component certificates keep using the cluster's own issuer; only
// the topology credential follows the topology CA.
func TestInternalCertificatesKeepClusterIssuer(t *testing.T) {
	scheme := setupScheme()
	cluster := topoTLSCluster("test-cluster", "supabase", nil)
	cluster.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}

	built, err := buildInternalCertificates(cluster, scheme)
	if err != nil {
		t.Fatalf("buildInternalCertificates() error = %v", err)
	}
	for _, cert := range built {
		issuer, _, _ := unstructured.NestedString(cert.Object, "spec", "issuerRef", "name")
		if issuer != "cluster-issuer" {
			t.Errorf("%s issuerRef.name = %q, want cluster-issuer", cert.GetName(), issuer)
		}
	}
}
