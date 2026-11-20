package toposerver

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func certScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	scheme.AddKnownTypeWithName(certs.GVK, &unstructured.Unstructured{})
	listGVK := certs.GVK
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return scheme
}

func certTestTopoServer(tls *multigresv1alpha1.TopoTLSConfig) *multigresv1alpha1.TopoServer {
	return &multigresv1alpha1.TopoServer{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "multigres.com/v1alpha1",
			Kind:       "TopoServer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-global-topo",
			Namespace: "supabase",
			UID:       "toposerver-uid",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{Replicas: ptr.To(int32(3))},
			TLS:  tls,
		},
	}
}

func TestBuildServingCertificate(t *testing.T) {
	scheme := certScheme()
	toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{
		Enabled:    ptr.To(true),
		IssuerName: "multigres-infra-issuer",
	})

	got, err := BuildServingCertificate(toposerver, scheme)
	if err != nil {
		t.Fatalf("BuildServingCertificate() error = %v", err)
	}
	if got == nil {
		t.Fatal("BuildServingCertificate() = nil, want a Certificate")
	}

	wantName := "test-cluster-global-topo-topo-server-tls"
	if got.GetName() != wantName {
		t.Errorf("name = %q, want %q", got.GetName(), wantName)
	}
	if got.GetNamespace() != "supabase" {
		t.Errorf("namespace = %q, want supabase", got.GetNamespace())
	}
	ownerRefs := got.GetOwnerReferences()
	if len(ownerRefs) != 1 || ownerRefs[0].Kind != "TopoServer" {
		t.Fatalf("ownerReferences = %+v, want one TopoServer ref", ownerRefs)
	}

	spec, ok := got.Object["spec"].(map[string]any)
	if !ok {
		t.Fatal("spec is not a map")
	}

	// Both Services the controller creates have to verify: the client Service
	// (BuildClientService) and the headless peer Service (BuildHeadlessService).
	wantDNSNames := []any{
		"test-cluster-global-topo",
		"test-cluster-global-topo.supabase",
		"test-cluster-global-topo.supabase.svc",
		"test-cluster-global-topo.supabase.svc.cluster.local",
		"test-cluster-global-topo-headless",
		"test-cluster-global-topo-headless.supabase",
		"test-cluster-global-topo-headless.supabase.svc",
		"test-cluster-global-topo-headless.supabase.svc.cluster.local",
		"*.test-cluster-global-topo-headless.supabase.svc.cluster.local",
	}
	if diff := cmp.Diff(wantDNSNames, spec["dnsNames"]); diff != "" {
		t.Errorf("dnsNames mismatch (-want +got):\n%s", diff)
	}

	wantSubject := fmt.Sprintf(
		certs.LiteralSubjectTemplate,
		"test-cluster-global-topo.supabase.svc.cluster.local",
	)
	if diff := cmp.Diff(wantSubject, spec["literalSubject"]); diff != "" {
		t.Errorf("literalSubject mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantName, spec["secretName"]); diff != "" {
		t.Errorf("secretName mismatch (-want +got):\n%s", diff)
	}

	// The topology server is shared infrastructure, so it takes the issuer from
	// the topology TLS config rather than any single cluster's issuer.
	wantIssuerRef := map[string]any{
		"name":  "multigres-infra-issuer",
		"kind":  "ClusterIssuer",
		"group": "cert-manager.io",
	}
	if diff := cmp.Diff(wantIssuerRef, spec["issuerRef"]); diff != "" {
		t.Errorf("issuerRef mismatch (-want +got):\n%s", diff)
	}
}

const testOperatorNamespace = "multigres-operator-system"

func authManagedTopoServer(
	name, namespace, issuer string,
) *multigresv1alpha1.TopoServer {
	ts := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{
		Enabled: ptr.To(true), IssuerName: issuer,
	})
	ts.Name, ts.Namespace, ts.UID = name, namespace, types.UID(name+"-uid")
	ts.Spec.Auth = &multigresv1alpha1.TopoServerAuthSpec{Enabled: true}
	return ts
}

func TestBuildAdminCertificate(t *testing.T) {
	certificate := BuildAdminCertificate(testOperatorNamespace, "infra-issuer", "example.test")
	wantName := multigresv1alpha1.TopoAdminCertName("ClusterIssuer", "infra-issuer")
	if certificate.GetName() != wantName {
		t.Fatalf("admin certificate name = %q", certificate.GetName())
	}
	if certificate.GetNamespace() != testOperatorNamespace {
		t.Fatalf("admin certificate namespace = %q", certificate.GetNamespace())
	}
	if refs := certificate.GetOwnerReferences(); len(refs) != 0 {
		t.Fatalf("shared admin certificate must have no owner: %v", refs)
	}
	for path, want := range map[string]string{
		"secretName":     wantName,
		"literalSubject": fmt.Sprintf(certs.LiteralSubjectTemplate, operatorEtcdUsername),
	} {
		got, _, _ := unstructured.NestedString(certificate.Object, "spec", path)
		if got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	uris, _, _ := unstructured.NestedSlice(certificate.Object, "spec", "uris")
	if diff := cmp.Diff([]any{"spiffe://example.test/cp/multigres-operator"}, uris); diff != "" {
		t.Fatalf("uris mismatch (-want +got):\n%s", diff)
	}
	usages, _, _ := unstructured.NestedSlice(certificate.Object, "spec", "usages")
	if diff := cmp.Diff(
		[]any{"digital signature", "key encipherment", "client auth"},
		usages,
	); diff != "" {
		t.Fatal(diff)
	}
	issuer, _, _ := unstructured.NestedString(certificate.Object, "spec", "issuerRef", "name")
	if issuer != "infra-issuer" {
		t.Fatalf("issuer = %q", issuer)
	}
}

func TestBuildAdminCertificateDefaultTrustDomain(t *testing.T) {
	certificate := BuildAdminCertificate(testOperatorNamespace, "infra-issuer", "")
	uris, _, _ := unstructured.NestedSlice(certificate.Object, "spec", "uris")
	want := []any{"spiffe://" + certs.DefaultTrustDomain + "/cp/multigres-operator"}
	if diff := cmp.Diff(want, uris); diff != "" {
		t.Fatal(diff)
	}
}

func TestReconcileAdminCertificateNotIssuedWithoutAuth(t *testing.T) {
	for name, mutate := range map[string]func(*multigresv1alpha1.TopoServer){
		"auth unset":   func(ts *multigresv1alpha1.TopoServer) { ts.Spec.Auth = nil },
		"tls disabled": func(ts *multigresv1alpha1.TopoServer) { ts.Spec.TLS = nil },
	} {
		t.Run(name, func(t *testing.T) {
			ts := authManagedTopoServer("ts", "project-a", "infra-issuer")
			mutate(ts)
			scheme := certScheme()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ts).Build()
			r := &TopoServerReconciler{
				Client: c, Scheme: scheme, OperatorNamespace: testOperatorNamespace,
			}
			if err := r.reconcileCertificate(t.Context(), ts); err != nil {
				t.Fatal(err)
			}
			for _, namespace := range []string{testOperatorNamespace, ts.Namespace} {
				list, err := certs.List(t.Context(), c, namespace)
				if err != nil {
					t.Fatal(err)
				}
				for _, item := range list.Items {
					if item.GetName() == multigresv1alpha1.TopoAdminCertName(
						"ClusterIssuer",
						"infra-issuer",
					) ||
						item.GetName() == multigresv1alpha1.TopoServerAdminCertName(ts.Name) {
						t.Errorf(
							"administrator certificate %s/%s issued",
							namespace,
							item.GetName(),
						)
					}
				}
			}
		})
	}
}

func TestReconcileAdminCertificateSharedPerIssuer(t *testing.T) {
	a := authManagedTopoServer("topo-a", "project-a", "infra-issuer")
	b := authManagedTopoServer("topo-b", "project-b", "infra-issuer")
	other := authManagedTopoServer("topo-c", "project-c", "other-issuer")
	scheme := certScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(a, b, other).Build()
	r := &TopoServerReconciler{Client: c, Scheme: scheme, OperatorNamespace: testOperatorNamespace}
	for _, ts := range []*multigresv1alpha1.TopoServer{a, b, other} {
		if err := r.reconcileCertificate(t.Context(), ts); err != nil {
			t.Fatal(err)
		}
	}
	list, err := certs.List(t.Context(), c, testOperatorNamespace)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("operator namespace has %d certificates, want 2 (one per issuer)", len(list.Items))
	}
	shared := certs.FindByName(
		list,
		multigresv1alpha1.TopoAdminCertName("ClusterIssuer", "infra-issuer"),
	)
	if shared == nil {
		t.Fatal("shared administrator certificate missing")
	}
	secretName, _, _ := unstructured.NestedString(shared.Object, "spec", "secretName")
	if secretName != multigresv1alpha1.TopoAdminCertSecretName("ClusterIssuer", "infra-issuer") {
		t.Fatalf("secretName = %q", secretName)
	}
	version := shared.GetResourceVersion()
	if err := r.reconcileCertificate(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	again, err := certs.Get(t.Context(), c, testOperatorNamespace, shared.GetName())
	if err != nil || again.GetResourceVersion() != version {
		t.Fatalf("unchanged certificate was modified: %v", err)
	}
	for _, ts := range []*multigresv1alpha1.TopoServer{a, b, other} {
		legacy, err := certs.Get(
			t.Context(), c, ts.Namespace, multigresv1alpha1.TopoServerAdminCertName(ts.Name),
		)
		if err != nil || legacy != nil {
			t.Fatalf("administrator certificate issued into %s: %v", ts.Namespace, err)
		}
	}
}

func TestReconcileAdminCertificateRequiresOperatorNamespace(t *testing.T) {
	ts := authManagedTopoServer("ts", "project-a", "infra-issuer")
	scheme := certScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ts).Build()
	r := &TopoServerReconciler{Client: c, Scheme: scheme}
	if err := r.reconcileCertificate(t.Context(), ts); err == nil {
		t.Fatal("expected an error without an operator namespace")
	}
}

func TestRemoveLegacyAdminCertificate(t *testing.T) {
	ts := authManagedTopoServer("ts", "project-a", "infra-issuer")
	scheme := certScheme()
	legacyName := multigresv1alpha1.TopoServerAdminCertName(ts.Name)
	legacy := &unstructured.Unstructured{}
	legacy.SetGroupVersionKind(certs.GVK)
	legacy.SetName(legacyName)
	legacy.SetNamespace(ts.Namespace)
	legacy.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "multigres.com/v1alpha1", Kind: "TopoServer", Name: ts.Name, UID: ts.UID,
	}})
	legacy.Object["spec"] = map[string]any{"secretName": legacyName}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: legacyName, Namespace: ts.Namespace,
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ts, legacy, secret).Build()
	r := &TopoServerReconciler{Client: c, Scheme: scheme, OperatorNamespace: testOperatorNamespace}
	if err := r.reconcileCertificate(t.Context(), ts); err != nil {
		t.Fatal(err)
	}
	got, err := certs.Get(t.Context(), c, ts.Namespace, legacyName)
	if err != nil || got != nil {
		t.Fatalf("legacy certificate not removed: %v", err)
	}
	key := client.ObjectKey{Namespace: ts.Namespace, Name: legacyName}
	if err := c.Get(t.Context(), key, &corev1.Secret{}); err == nil {
		t.Fatal("legacy administrator Secret not removed")
	}
}

func TestRemoveLegacyAdminCertificateLeavesOtherOwners(t *testing.T) {
	ts := authManagedTopoServer("ts", "project-a", "infra-issuer")
	scheme := certScheme()
	legacyName := multigresv1alpha1.TopoServerAdminCertName(ts.Name)
	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(certs.GVK)
	foreign.SetName(legacyName)
	foreign.SetNamespace(ts.Namespace)
	foreign.Object["spec"] = map[string]any{"secretName": legacyName}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ts, foreign).Build()
	r := &TopoServerReconciler{Client: c, Scheme: scheme, OperatorNamespace: testOperatorNamespace}
	r.removeLegacyAdminCertificate(t.Context(), ts)
	got, err := certs.Get(t.Context(), c, ts.Namespace, legacyName)
	if err != nil || got == nil {
		t.Fatalf("certificate owned by someone else was removed: %v", err)
	}
}

func TestBuildServingCertificateSANsCoverBothServices(t *testing.T) {
	scheme := certScheme()
	toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})

	clientSvc, err := BuildClientService(toposerver, scheme)
	if err != nil {
		t.Fatalf("BuildClientService() error = %v", err)
	}
	headlessSvc, err := BuildHeadlessService(toposerver, scheme)
	if err != nil {
		t.Fatalf("BuildHeadlessService() error = %v", err)
	}

	cert, err := BuildServingCertificate(toposerver, scheme)
	if err != nil {
		t.Fatalf("BuildServingCertificate() error = %v", err)
	}
	sans, _, err := unstructured.NestedSlice(cert.Object, "spec", "dnsNames")
	if err != nil {
		t.Fatalf("NestedSlice(dnsNames) error = %v", err)
	}
	covered := make(map[string]struct{}, len(sans))
	for _, s := range sans {
		covered[s.(string)] = struct{}{}
	}

	for _, svc := range []*corev1.Service{clientSvc, headlessSvc} {
		fqdn := fmt.Sprintf("%s.%s.svc.cluster.local", svc.Name, svc.Namespace)
		if _, ok := covered[fqdn]; !ok {
			t.Errorf("Service %q FQDN %q is not a SAN; SANs = %v", svc.Name, fqdn, sans)
		}
	}
}

func TestBuildServingCertificateDefaultIssuer(t *testing.T) {
	scheme := certScheme()
	toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})

	cert, err := BuildServingCertificate(toposerver, scheme)
	if err != nil {
		t.Fatalf("BuildServingCertificate() error = %v", err)
	}
	issuer, _, _ := unstructured.NestedString(cert.Object, "spec", "issuerRef", "name")
	if issuer != certs.DefaultIssuerName {
		t.Errorf("issuerRef.name = %q, want %q", issuer, certs.DefaultIssuerName)
	}
}

func TestBuildServingCertificateDisabled(t *testing.T) {
	scheme := certScheme()

	for name, tls := range map[string]*multigresv1alpha1.TopoTLSConfig{
		"unset":    nil,
		"disabled": {Enabled: ptr.To(false)},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := BuildServingCertificate(certTestTopoServer(tls), scheme)
			if err != nil {
				t.Fatalf("BuildServingCertificate() error = %v", err)
			}
			if got != nil {
				t.Errorf("BuildServingCertificate() = %v, want nil", got)
			}
		})
	}
}

func TestReconcileCertificate(t *testing.T) {
	certName := "test-cluster-global-topo-topo-server-tls"

	t.Run("applies the certificate when enabled", func(t *testing.T) {
		scheme := certScheme()
		toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(toposerver).Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), toposerver); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		if err := c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: certName},
			got,
		); err != nil {
			t.Fatalf("expected serving Certificate, got error %v", err)
		}
	})

	t.Run("reports a foreign certificate collision when enabled", func(t *testing.T) {
		scheme := certScheme()
		toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		foreign, err := BuildServingCertificate(toposerver, scheme)
		if err != nil {
			t.Fatalf("BuildServingCertificate() error = %v", err)
		}
		foreign.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: "example.com/v1",
			Kind:       "Other",
			Name:       "other",
			UID:        "other-uid",
			Controller: ptr.To(true),
		}})

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(toposerver, foreign).Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		before := &unstructured.Unstructured{}
		before.SetGroupVersionKind(certs.GVK)
		if err := c.Get(context.Background(), key, before); err != nil {
			t.Fatalf("Get() error = %v", err)
		}

		if err := r.reconcileCertificate(context.Background(), toposerver); err == nil {
			t.Fatal("reconcileCertificate() error = nil, want collision error")
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		if err := c.Get(context.Background(), key, got); err != nil {
			t.Fatalf("foreign Certificate was modified or deleted: %v", err)
		}
		if diff := cmp.Diff(before.Object, got.Object); diff != "" {
			t.Errorf("foreign Certificate changed (-want +got):\n%s", diff)
		}
	})

	t.Run("prunes the certificate and its secret when disabled", func(t *testing.T) {
		scheme := certScheme()
		enabled := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		existing, err := BuildServingCertificate(enabled, scheme)
		if err != nil {
			t.Fatalf("BuildServingCertificate() error = %v", err)
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: certName, Namespace: "supabase"},
		}

		toposerver := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(false)})
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, existing, secret).
			Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), toposerver); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		if err := c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: certName},
			got,
		); err == nil {
			t.Error("expected serving Certificate to be deleted")
		}
		if err := c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: certName},
			&corev1.Secret{},
		); err == nil {
			t.Error("expected generated Secret to be deleted")
		}
	})

	t.Run("issues nothing when TLS is unset", func(t *testing.T) {
		scheme := certScheme()
		toposerver := certTestTopoServer(nil)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(toposerver).Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), toposerver); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(certs.GVK)
		if err := c.List(context.Background(), list); err != nil {
			t.Fatalf("List() error = %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("got %d Certificates, want 0", len(list.Items))
		}
	})

	// Removing the TLS block is as common as setting enabled to false, and it
	// must not leave private key material behind either.
	t.Run("cleans up when the TLS block is removed", func(t *testing.T) {
		scheme := certScheme()
		enabled := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		existing, err := BuildServingCertificate(enabled, scheme)
		if err != nil {
			t.Fatalf("BuildServingCertificate() error = %v", err)
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: certName, Namespace: "supabase"},
		}

		toposerver := certTestTopoServer(nil)
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, existing, secret).
			Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), toposerver); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		key := client.ObjectKey{Namespace: "supabase", Name: certName}
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		if err := c.Get(context.Background(), key, got); err == nil {
			t.Error("expected orphaned Certificate to be deleted")
		}
		if err := c.Get(context.Background(), key, &corev1.Secret{}); err == nil {
			t.Error("expected orphaned Secret to be deleted")
		}
	})

	// A same-named Certificate owned by something else is not ours to delete.
	t.Run("leaves an unowned certificate alone", func(t *testing.T) {
		scheme := certScheme()
		enabled := certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)})
		foreign, err := BuildServingCertificate(enabled, scheme)
		if err != nil {
			t.Fatalf("BuildServingCertificate() error = %v", err)
		}
		foreign.SetOwnerReferences(nil)

		toposerver := certTestTopoServer(nil)
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, foreign).
			Build()
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.reconcileCertificate(context.Background(), toposerver); err != nil {
			t.Fatalf("reconcileCertificate() error = %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(certs.GVK)
		if err := c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: certName},
			got,
		); err != nil {
			t.Errorf("unowned Certificate was deleted: %v", err)
		}
	})
}

// With topology TLS off the etcd StatefulSet renders exactly as it did before
// enforcement existed: plaintext listeners and no serving certificate mount.
// This is the invariant that keeps the change safe to merge with the gate off.
func TestTopoTLSOffRendersPlaintext(t *testing.T) {
	scheme := certScheme()

	sts, err := BuildStatefulSet(certTestTopoServer(nil), scheme)
	if err != nil {
		t.Fatalf("BuildStatefulSet() error = %v", err)
	}

	for _, vol := range sts.Spec.Template.Spec.Volumes {
		if vol.Name == TopoServerTLSVolumeName {
			t.Fatalf("serving certificate volume present with topology TLS off")
		}
	}
	env := etcdEnvMap(t, sts)
	if got := env["ETCD_LISTEN_CLIENT_URLS"]; got != "http://[::]:2379" {
		t.Errorf("ETCD_LISTEN_CLIENT_URLS = %q, want plaintext", got)
	}
	if _, ok := env["ETCD_CLIENT_CERT_AUTH"]; ok {
		t.Errorf("ETCD_CLIENT_CERT_AUTH set with topology TLS off")
	}
}

// With topology TLS on, etcd serves its client and peer listeners over TLS,
// requires a client certificate on each, and mounts the issued serving
// certificate. The metrics listener stays plaintext so the probes keep working.
func TestTopoTLSOnRequiresClientCerts(t *testing.T) {
	scheme := certScheme()

	sts, err := BuildStatefulSet(certTestTopoServer(&multigresv1alpha1.TopoTLSConfig{
		Enabled:    ptr.To(true),
		IssuerName: "multigres-infra-issuer",
	}), scheme)
	if err != nil {
		t.Fatalf("BuildStatefulSet() error = %v", err)
	}

	var servingVol *corev1.Volume
	for i := range sts.Spec.Template.Spec.Volumes {
		if sts.Spec.Template.Spec.Volumes[i].Name == TopoServerTLSVolumeName {
			servingVol = &sts.Spec.Template.Spec.Volumes[i]
		}
	}
	if servingVol == nil {
		t.Fatal("serving certificate volume missing with topology TLS on")
	}
	if servingVol.Secret == nil ||
		servingVol.Secret.SecretName != multigresv1alpha1.TopoServerCertSecretName(
			"test-cluster-global-topo",
		) {
		t.Errorf("serving certificate volume references the wrong Secret: %+v", servingVol.Secret)
	}

	var mounted bool
	for _, m := range sts.Spec.Template.Spec.Containers[0].VolumeMounts {
		if m.Name == TopoServerTLSVolumeName {
			mounted = true
			if !m.ReadOnly || m.MountPath != TopoServerTLSMountPath {
				t.Errorf(
					"serving certificate mount = %+v, want read-only at %s",
					m,
					TopoServerTLSMountPath,
				)
			}
		}
	}
	if !mounted {
		t.Error("serving certificate is not mounted into the etcd container")
	}

	env := etcdEnvMap(t, sts)
	wantEnv := map[string]string{
		"ETCD_LISTEN_CLIENT_URLS":    "https://[::]:2379",
		"ETCD_LISTEN_PEER_URLS":      "https://[::]:2380",
		"ETCD_LISTEN_METRICS_URLS":   "http://[::]:2381",
		"ETCD_CLIENT_CERT_AUTH":      "true",
		"ETCD_PEER_CLIENT_CERT_AUTH": "true",
		"ETCD_CERT_FILE":             TopoServerTLSCertFile,
		"ETCD_KEY_FILE":              TopoServerTLSKeyFile,
		"ETCD_TRUSTED_CA_FILE":       TopoServerTLSCAFile,
		"ETCD_PEER_CERT_FILE":        TopoServerTLSCertFile,
		"ETCD_PEER_KEY_FILE":         TopoServerTLSKeyFile,
		"ETCD_PEER_TRUSTED_CA_FILE":  TopoServerTLSCAFile,
	}
	for k, want := range wantEnv {
		if got := env[k]; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
}

// etcdEnvMap collects the etcd container's environment variables that carry a
// literal value into a lookup keyed by name.
func etcdEnvMap(t *testing.T, sts *appsv1.StatefulSet) map[string]string {
	t.Helper()
	env := map[string]string{}
	for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
		if e.ValueFrom == nil {
			env[e.Name] = e.Value
		}
	}
	return env
}
