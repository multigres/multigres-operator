package multigrescluster

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/multigres/multigres/go/common/topoclient"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestTopologyTLSLongFallbackUsesSameRoot(t *testing.T) {
	ck := assert.NewCollecting(t)
	cluster := topoTLSCluster("cluster-abcdefghijklmnop", "namespace-abcdefghijklmnopqrstu", nil)
	cluster.Spec.Cells = []multigresv1alpha1.CellConfig{{Name: "zone-a"}}
	scheme := setupScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	store := newClusterTopologyMemoryStore(t)
	r := &MultigresClusterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(ref multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			ck.EqDeep(
				"/multigres-fallback/b-Tmo_r9oWzDWEuz_6f6LFOAmYO7ve1i4ksIy7qa9ac/global",
				ref.RootPath,
			)
			return noCloseStore{Store: store}, nil
		},
	}
	ck.Require().NoError(r.reconcileCertificate(t.Context(), cluster))
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certGVK)
	ck.Require().NoError(c.Get(t.Context(), client.ObjectKey{
		Namespace: cluster.Namespace, Name: multigresv1alpha1.TopoClientCertName(cluster.Name),
	}, cert))
	subject, _, err := unstructured.NestedString(cert.Object, "spec", "literalSubject")
	ck.Require().NoError(err)
	cn, ok := parseCommonName(subject)
	ck.Require().True(ok)
	ck.EqDeep("/multigres-fallback/b-Tmo_r9oWzDWEuz_6f6LFOAmYO7ve1i4ksIy7qa9ac", cn)

	res := resolver.NewResolver(c, cluster.Namespace)
	result, err := r.reconcileTopology(t.Context(), cluster, res)
	ck.Require().NoError(err)
	ck.Zero(result.RequeueAfter)
	cell, err := store.GetCell(t.Context(), "zone-a")
	ck.Require().NoError(err)
	ck.EqDeep(cn+"/global", cell.Root)

	cluster.Spec.Cells[0].Spec = &multigresv1alpha1.CellInlineSpec{
		LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{},
		},
	}
	_, _, local, err := res.ResolveCell(t.Context(), cluster, &cluster.Spec.Cells[0])
	ck.Require().NoError(err)
	ck.EqDeep(cn+"/zone-a", local.Etcd.RootPath)
}

func TestReconcileOversizedProjectRefStatusAndRecovery(t *testing.T) {
	for _, ref := range []string{strings.Repeat("p", 55), strings.Repeat("/", 19)} {
		t.Run(ref, func(t *testing.T) {
			ck := assert.NewCollecting(t)
			cluster := topoTLSCluster(
				"cluster",
				"default",
				map[string]string{metadata.AnnotationProjectRef: ref},
			)
			cluster.Generation = 3
			scheme := setupScheme()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
				WithStatusSubresource(cluster).Build()
			r := &MultigresClusterReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(100),
				CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
					return noCloseStore{Store: newClusterTopologyMemoryStore(t)}, nil
				},
			}
			key := client.ObjectKeyFromObject(cluster)
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			ck.Require().ErrorContains(err, "64 byte certificate common name limit")
			ck.Require().NoError(c.Get(t.Context(), key, cluster))
			ck.EqDeep(multigresv1alpha1.PhaseDegraded, cluster.Status.Phase)
			condition := meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
			ck.Require().NotNil(condition)
			ck.EqDeep(metav1.ConditionFalse, condition.Status)
			ck.EqDeep("TopoCertificateFailed", condition.Reason)
			ck.EqDeep(cluster.Generation, condition.ObservedGeneration)
			ck.StrContains(
				condition.Message,
				"shorten annotation multigres.com/project-ref to at most 53 bytes after path escaping",
			)

			cluster.Annotations[metadata.AnnotationProjectRef] = strings.Repeat("p", 53)
			ck.Require().NoError(c.Update(t.Context(), cluster))
			_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
			ck.Require().NoError(err)
			ck.Require().NoError(c.Get(t.Context(), key, cluster))
			condition = meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
			ck.Require().NotNil(condition)
			ck.EqDeep(metav1.ConditionTrue, condition.Status)
			ck.EqDeep("TopoConnected", condition.Reason)
			ck.NotStrContains(cluster.Status.Message, "certificate common name limit")
		})
	}
}

func TestReconcileCertificatePreservesExplicitLongRoot(t *testing.T) {
	ck := assert.NewCollecting(t)
	cluster := topoTLSCluster("cluster-abcdefghijklmnop", "namespace-abcdefghijklmnopqrstu", nil)
	const explicitRoot = "/multigres/namespace-abcdefghijklmnopqrstu/cluster-abcdefghijklmnop/global"
	cluster.Spec.GlobalTopoServer = &multigresv1alpha1.GlobalTopoServerSpec{
		Etcd: &multigresv1alpha1.EtcdSpec{RootPath: explicitRoot},
	}
	scheme := setupScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	ck.Require().
		ErrorContains(r.reconcileCertificate(t.Context(), cluster), "outside certificate identity")
	ck.Require().NoError(c.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	ck.EqDeep(explicitRoot, cluster.Spec.GlobalTopoServer.Etcd.RootPath)
	condition := meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
	ck.Require().NotNil(condition)
	ck.EqDeep("TopoCertificateFailed", condition.Reason)
	ck.StrContains(condition.Message, "migrate any existing topology data")

	cluster.Spec.GlobalTopoServer.Etcd.RootPath = ""
	cluster.Spec.Cells = []multigresv1alpha1.CellConfig{{
		Name: "zone-a",
		Spec: &multigresv1alpha1.CellInlineSpec{
			LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{RootPath: explicitRoot + "/zone-a"},
			},
		},
	}}
	ck.Require().NoError(c.Update(t.Context(), cluster))
	ck.Require().
		ErrorContains(r.reconcileCertificate(t.Context(), cluster), `cell "zone-a" topology root`)
}

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
			c := assert.NewCollecting(t)
			scheme := setupScheme()
			got, err := buildTopoClientCertificate(tc.cluster, scheme)
			c.Require().NoError(err, "buildTopoClientCertificate() error =")

			wantName := tc.cluster.Name + "-topo-client-tls"
			c.Eq(wantName, got.GetName(), "name")
			c.Eq(tc.cluster.Namespace, got.GetNamespace(), "namespace")

			ownerRefs := got.GetOwnerReferences()
			c.Require().
				False(len(ownerRefs) != 1 || ownerRefs[0].Kind != "MultigresCluster", "ownerReferences = %+v, want one MultigresCluster ref", ownerRefs)

			spec, ok := got.Object["spec"].(map[string]any)
			c.Require().True(ok, "spec is not a map")
			wantSubject := fmt.Sprintf(CertLiteralSubjectTemplate, tc.wantSubject)
			c.Eq(
				"",
				cmp.Diff(wantSubject, spec["literalSubject"]),
				"literalSubject mismatch (-want +got):\n",
			)
			c.Eq("", cmp.Diff(wantName, spec["secretName"]), "secretName mismatch (-want +got):\n")
			// A client credential is verified by subject, so it carries no SANs.
			c.Eq("", cmp.Diff([]any{}, spec["dnsNames"]), "dnsNames mismatch (-want +got):\n")
			wantUsages := []any{
				"digital signature",
				"key encipherment",
				"client auth",
			}
			c.Eq("", cmp.Diff(wantUsages, spec["usages"]), "usages mismatch (-want +got):\n")
			// The credential is only useful if it chains to the CA the
			// topology server trusts, never to the cluster's own issuer.
			wantIssuerRef := map[string]any{
				"name":  "multigres-infra-issuer",
				"kind":  "ClusterIssuer",
				"group": "cert-manager.io",
			}
			c.Eq(
				"",
				cmp.Diff(wantIssuerRef, spec["issuerRef"]),
				"issuerRef mismatch (-want +got):\n",
			)
		})
	}
}

// The CN is the identity a topology server authorizes against, so it has to
// be the string ClusterRoot() produces and not a re-derivation of it.
func TestBuildTopoClientCertificateCommonNameMatchesTopologyRoot(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := setupScheme()
	cluster := topoTLSCluster("test-cluster", "supabase", map[string]string{
		metadata.AnnotationProjectRef: "proj_123",
	})

	cert, err := buildTopoClientCertificate(cluster, scheme)
	c.Require().NoError(err, "buildTopoClientCertificate() error =")
	subject, _, _ := unstructured.NestedString(cert.Object, "spec", "literalSubject")

	globalRoot := "/multigres/proj_123/global"
	cn, ok := parseCommonName(subject)
	c.Require().True(ok, "no CN in literalSubject %q", subject)
	got := cn + "/global"
	c.Eq(globalRoot, got, "CN %q does not prefix the global root: got %q, want", cn, got)
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
		ck := assert.NewAborting(t)
		scheme := setupScheme()
		cluster := topoTLSCluster("test-cluster", "supabase", nil)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		ck.NoError(
			r.reconcileCertificate(context.Background(), cluster),
			"reconcileCertificate() error =",
		)

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		ck.NoError(
			c.Get(context.Background(), key, got),
			"expected topology client Certificate, got error",
		)
	})

	t.Run("prunes the client certificate when topology TLS is disabled", func(t *testing.T) {
		ck := assert.NewAborting(t)
		scheme := setupScheme()
		cluster := topoTLSCluster("test-cluster", "supabase", nil)
		cluster.Spec.TopoTLS.Enabled = ptr.To(false)

		existing, err := buildTopoClientCertificate(
			topoTLSCluster("test-cluster", "supabase", nil), scheme,
		)
		ck.NoError(err, "buildTopoClientCertificate() error =")
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster, existing).
			Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		ck.NoError(
			r.reconcileCertificate(context.Background(), cluster),
			"reconcileCertificate() error =",
		)

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		err = c.Get(context.Background(), key, got)
		ck.Error(err, "expected topology client Certificate to be deleted")
	})

	t.Run("issues nothing when the topology server is external", func(t *testing.T) {
		ck := assert.NewAborting(t)
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

		ck.NoError(
			r.reconcileCertificate(context.Background(), cluster),
			"reconcileCertificate() error =",
		)

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certGVK)
		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		ck.Error(
			c.Get(context.Background(), key, got),
			"expected no topology client Certificate for an external topology server",
		)
	})

	t.Run("issues nothing when topology TLS is unset", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		scheme := setupScheme()
		cluster := topoTLSCluster("test-cluster", "supabase", nil)
		cluster.Spec.TopoTLS = nil
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		ck.Require().
			NoError(r.reconcileCertificate(context.Background(), cluster), "reconcileCertificate() error =")

		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(certGVK)
		ck.Require().NoError(c.List(context.Background(), list), "List() error =")
		ck.Empty(list.Items, "got %d Certificates, want 0", len(list.Items))
	})
}

// A truncated common name would no longer name the prefix it is meant to
// authorize, so an over-long topology root fails loudly instead.
func TestBuildTopoClientCertificateRejectsOverLongRoot(t *testing.T) {
	scheme := setupScheme()
	cluster := topoTLSCluster("test-cluster", "supabase", map[string]string{
		metadata.AnnotationProjectRef: strings.Repeat("p", 64),
	})

	_, err := buildTopoClientCertificate(cluster, scheme)
	assert.NewAborting(t).
		Error(err, "buildTopoClientCertificate() = nil error, want a common name limit error")
}

// Internal component certificates keep using the cluster's own issuer; only
// the topology credential follows the topology CA.
func TestInternalCertificatesKeepClusterIssuer(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := setupScheme()
	cluster := topoTLSCluster("test-cluster", "supabase", nil)
	cluster.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}

	built, err := buildInternalCertificates(cluster, scheme)
	c.Require().NoError(err, "buildInternalCertificates() error =")
	for _, cert := range built {
		issuer, _, _ := unstructured.NestedString(cert.Object, "spec", "issuerRef", "name")
		c.Eq(
			"cluster-issuer",
			issuer,
			"%s issuerRef.name = %q, want cluster-issuer",
			cert.GetName(),
			issuer,
		)
	}
}
