package certs

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	return scheme
}

func TestBuildDefaultsIssuer(t *testing.T) {
	owner := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "owner",
			Namespace: "supabase",
			UID:       "owner-uid",
		},
	}

	cert, err := Build(owner, testScheme(t), Spec{
		Name:       "example",
		SecretName: "example",
		CommonName: "example.supabase.svc.cluster.local",
		DNSNames:   []any{"example"},
		Usages:     []any{"server auth"},
	})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if cert.GetNamespace() != "supabase" {
		t.Errorf("namespace = %q, want supabase", cert.GetNamespace())
	}
	spec, ok := cert.Object["spec"].(map[string]any)
	if !ok {
		t.Fatal("spec is not a map")
	}
	wantIssuerRef := map[string]any{
		"name":  DefaultIssuerName,
		"kind":  "ClusterIssuer",
		"group": "cert-manager.io",
	}
	if diff := cmp.Diff(wantIssuerRef, spec["issuerRef"]); diff != "" {
		t.Errorf("issuerRef mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(Duration, spec["duration"]); diff != "" {
		t.Errorf("duration mismatch (-want +got):\n%s", diff)
	}
}

func TestTruncateCommonName(t *testing.T) {
	short := strings.Repeat("a", MaxCommonNameBytes)
	if got := TruncateCommonName(short); got != short {
		t.Errorf("TruncateCommonName() shortened a name that fits: %q", got)
	}

	long := strings.Repeat("a", MaxCommonNameBytes+40)
	got := TruncateCommonName(long)
	if len(got) > MaxCommonNameBytes {
		t.Errorf("TruncateCommonName() = %d bytes, want <= %d", len(got), MaxCommonNameBytes)
	}
	if got != TruncateCommonName(long) {
		t.Error("TruncateCommonName() is not deterministic")
	}
	if got == TruncateCommonName(long+"b") {
		t.Error("TruncateCommonName() collided for different inputs")
	}
}

func certFixture(t *testing.T, name, secretName string) *unstructured.Unstructured {
	t.Helper()
	owner := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "owner",
			Namespace: "supabase",
			UID:       "owner-uid",
		},
	}
	cert, err := Build(owner, testScheme(t), Spec{
		Name:       name,
		SecretName: secretName,
		CommonName: name,
		DNSNames:   []any{name},
		Usages:     []any{"server auth"},
	})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return cert
}

func fakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := testScheme(t)
	_ = corev1.AddToScheme(scheme)
	scheme.AddKnownTypeWithName(GVK, &unstructured.Unstructured{})
	listGVK := GVK
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func TestKeepSets(t *testing.T) {
	desired := []*unstructured.Unstructured{
		certFixture(t, "a", "a-secret"),
		certFixture(t, "b", "b-secret"),
	}

	keepNames, keepSecretNames := KeepSets(desired)
	if diff := cmp.Diff(
		map[string]struct{}{"a": {}, "b": {}}, keepNames,
	); diff != "" {
		t.Errorf("keepNames mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(
		map[string]struct{}{"a-secret": {}, "b-secret": {}}, keepSecretNames,
	); diff != "" {
		t.Errorf("keepSecretNames mismatch (-want +got):\n%s", diff)
	}
}

func TestListTolerantOfMissingCRD(t *testing.T) {
	// A scheme without the Certificate type makes the client report no mapping
	// for the GVK, which is what a cluster without cert-manager looks like.
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	got, err := List(context.Background(), c, "supabase")
	if err != nil {
		t.Fatalf("List() error = %v, want nil when cert-manager is absent", err)
	}
	if len(got.Items) != 0 {
		t.Errorf("got %d Certificates, want 0", len(got.Items))
	}
}

func TestFindByNameAndOwnedBy(t *testing.T) {
	certList := &unstructured.UnstructuredList{}
	certList.SetGroupVersionKind(GVK)
	certList.Items = []unstructured.Unstructured{*certFixture(t, "a", "a-secret")}

	if got := FindByName(certList, "a"); got == nil {
		t.Error("FindByName(a) = nil, want the Certificate")
	}
	if got := FindByName(certList, "missing"); got != nil {
		t.Errorf("FindByName(missing) = %v, want nil", got)
	}
	if !OwnedBy(&certList.Items[0], "owner-uid") {
		t.Error("OwnedBy(owner-uid) = false, want true")
	}
	if OwnedBy(&certList.Items[0], "other-uid") {
		t.Error("OwnedBy(other-uid) = true, want false")
	}
}

func TestPruneDeletesUnwantedCertificatesAndSecrets(t *testing.T) {
	stale := certFixture(t, "stale", "stale-secret")
	kept := certFixture(t, "kept", "kept-secret")
	unowned := certFixture(t, "unowned", "unowned-secret")
	unowned.SetOwnerReferences(nil)

	staleSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "stale-secret", Namespace: "supabase"},
	}
	c := fakeClient(t, stale, kept, unowned, staleSecret)

	certList, err := List(context.Background(), c, "supabase")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	keepNames, keepSecretNames := KeepSets([]*unstructured.Unstructured{kept})
	if err := Prune(
		context.Background(), c, "supabase", "owner-uid",
		certList, keepNames, keepSecretNames,
	); err != nil {
		t.Fatalf("Prune() error = %v", err)
	}

	for name, wantGone := range map[string]bool{
		"stale":   true,
		"kept":    false,
		"unowned": false,
	} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(GVK)
		err := c.Get(
			context.Background(),
			client.ObjectKey{Namespace: "supabase", Name: name},
			got,
		)
		if wantGone && err == nil {
			t.Errorf("Certificate %q still exists, want deleted", name)
		}
		if !wantGone && err != nil {
			t.Errorf("Certificate %q was deleted, want kept: %v", name, err)
		}
	}

	if err := c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "stale-secret"},
		&corev1.Secret{},
	); err == nil {
		t.Error("stale Secret still exists, want deleted")
	}
}

func TestApplySkipsUnchangedCertificates(t *testing.T) {
	existing := certFixture(t, "a", "a-secret")
	c := fakeClient(t, existing)

	certList, err := List(context.Background(), c, "supabase")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	before := certList.Items[0].GetResourceVersion()

	desired := certFixture(t, "a", "a-secret")
	if err := Apply(
		context.Background(), c, certList, "owner-uid",
		[]*unstructured.Unstructured{desired},
	); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	if err := c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.GetResourceVersion() != before {
		t.Errorf(
			"resourceVersion changed on a no-op apply: %q -> %q",
			before, got.GetResourceVersion(),
		)
	}
}

func TestApplyRejectsForeignCertificate(t *testing.T) {
	existing := certFixture(t, "a", "a-secret")
	existing.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "example.com/v1",
		Kind:       "Other",
		Name:       "other",
		UID:        "other-uid",
	}})
	c := fakeClient(t, existing)

	certList, err := List(context.Background(), c, "supabase")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	before := certList.Items[0].DeepCopy()

	desired := certFixture(t, "a", "a-secret")
	err = Apply(
		context.Background(), c, certList, "owner-uid",
		[]*unstructured.Unstructured{desired},
	)
	if err == nil {
		t.Fatal("Apply() error = nil, want collision error")
	}
	if !strings.Contains(err.Error(), "a") || !strings.Contains(err.Error(), "supabase") {
		t.Errorf("Apply() error = %q, want namespace and name", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	if err := c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	); err != nil {
		t.Fatalf("foreign Certificate was modified or deleted: %v", err)
	}
	if diff := cmp.Diff(before.Object, got.Object); diff != "" {
		t.Errorf("foreign Certificate changed (-want +got):\n%s", diff)
	}
}

func TestApplyCreatesMissingCertificates(t *testing.T) {
	c := fakeClient(t)

	certList, err := List(context.Background(), c, "supabase")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	desired := certFixture(t, "a", "a-secret")
	if err := Apply(
		context.Background(), c, certList, "owner-uid",
		[]*unstructured.Unstructured{desired},
	); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	if err := c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	); err != nil {
		t.Fatalf("expected Certificate to be created, got error %v", err)
	}
}

func TestGetAbsentAndMissingCRD(t *testing.T) {
	t.Run("absent certificate", func(t *testing.T) {
		c := fakeClient(t)
		got, err := Get(context.Background(), c, "supabase", "a")
		if err != nil {
			t.Fatalf("Get() error = %v, want nil", err)
		}
		if got != nil {
			t.Errorf("Get() = %v, want nil", got)
		}
	})

	t.Run("cert-manager absent", func(t *testing.T) {
		// A scheme without the Certificate type is what a cluster with no
		// cert-manager CRD looks like to the client.
		c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
		got, err := Get(context.Background(), c, "supabase", "a")
		if err != nil {
			t.Fatalf("Get() error = %v, want nil when cert-manager is absent", err)
		}
		if got != nil {
			t.Errorf("Get() = %v, want nil", got)
		}
	})

	t.Run("present certificate", func(t *testing.T) {
		c := fakeClient(t, certFixture(t, "a", "a-secret"))
		got, err := Get(context.Background(), c, "supabase", "a")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got == nil || got.GetName() != "a" {
			t.Fatalf("Get() = %v, want the Certificate named a", got)
		}
	})
}

func TestDeleteRemovesCertificateAndSecret(t *testing.T) {
	cert := certFixture(t, "a", "a-secret")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "a-secret", Namespace: "supabase"},
	}
	c := fakeClient(t, cert, secret)

	if err := Delete(context.Background(), c, cert); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	if err := c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	); err == nil {
		t.Error("Certificate still exists, want deleted")
	}
	if err := c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a-secret"},
		&corev1.Secret{},
	); err == nil {
		t.Error("Secret still exists, want deleted")
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	cert := certFixture(t, "a", "a-secret")
	c := fakeClient(t)

	if err := Delete(context.Background(), c, cert); err != nil {
		t.Errorf("Delete() on absent objects error = %v, want nil", err)
	}
}

func TestSpecEqual(t *testing.T) {
	a := certFixture(t, "a", "a-secret")
	same := certFixture(t, "a", "a-secret")
	other := certFixture(t, "a", "b-secret")

	if !SpecEqual(a, same) {
		t.Error("SpecEqual() = false for identical specs")
	}
	if SpecEqual(a, other) {
		t.Error("SpecEqual() = true for differing specs")
	}
}

func TestApplyOneCreates(t *testing.T) {
	c := fakeClient(t)
	if err := ApplyOne(context.Background(), c, certFixture(t, "a", "a-secret")); err != nil {
		t.Fatalf("ApplyOne() error = %v", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	if err := c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	); err != nil {
		t.Fatalf("expected Certificate to be created, got error %v", err)
	}
}

func TestBuildSharedHasNoOwnerAndOmitsEmptyURIs(t *testing.T) {
	cert := BuildShared("operator-ns", Spec{Name: "c", SecretName: "s", CommonName: "cn"})
	if cert.GetNamespace() != "operator-ns" || len(cert.GetOwnerReferences()) != 0 {
		t.Fatalf("namespace=%q owners=%v", cert.GetNamespace(), cert.GetOwnerReferences())
	}
	if _, found, _ := unstructured.NestedSlice(cert.Object, "spec", "uris"); found {
		t.Fatal("uris present although none were requested")
	}
	cert = BuildShared("operator-ns", Spec{
		Name: "c", SecretName: "s", CommonName: "cn", URIs: []any{SPIFFEURI("", "cp/x")},
	})
	uris, _, _ := unstructured.NestedSlice(cert.Object, "spec", "uris")
	if len(uris) != 1 || uris[0] != "spiffe://"+DefaultTrustDomain+"/cp/x" {
		t.Fatalf("uris = %v", uris)
	}
}
