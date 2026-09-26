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

	"github.com/multigres/testkit/assert"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	assert.NewAborting(t).NoError(multigresv1alpha1.AddToScheme(scheme), "AddToScheme() error =")
	return scheme
}

func TestBuildDefaultsIssuer(t *testing.T) {
	c := assert.NewCollecting(t)
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
	c.Require().NoError(err, "Build() error =")

	c.Eq("supabase", cert.GetNamespace(), "namespace")
	spec, ok := cert.Object["spec"].(map[string]any)
	c.Require().True(ok, "spec is not a map")
	wantIssuerRef := map[string]any{
		"name":  DefaultIssuerName,
		"kind":  "ClusterIssuer",
		"group": "cert-manager.io",
	}
	c.Eq("", cmp.Diff(wantIssuerRef, spec["issuerRef"]), "issuerRef mismatch (-want +got):\n")
	c.Eq("", cmp.Diff(Duration, spec["duration"]), "duration mismatch (-want +got):\n")
}

func TestTruncateCommonName(t *testing.T) {
	c := assert.NewCollecting(t)
	short := strings.Repeat("a", MaxCommonNameBytes)
	c.Eq(short, TruncateCommonName(short), "TruncateCommonName() shortened a name that fits")

	long := strings.Repeat("a", MaxCommonNameBytes+40)
	got := TruncateCommonName(long)
	c.LessOrEqual(MaxCommonNameBytes, len(got), "TruncateCommonName()")
	c.Eq(TruncateCommonName(long), got, "TruncateCommonName() is not deterministic")
	c.NotEq(TruncateCommonName(long+"b"), got, "TruncateCommonName() collided for different inputs")
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
	assert.NewAborting(t).NoError(err, "Build() error =")
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
	c := assert.NewCollecting(t)
	desired := []*unstructured.Unstructured{
		certFixture(t, "a", "a-secret"),
		certFixture(t, "b", "b-secret"),
	}

	keepNames, keepSecretNames := KeepSets(desired)
	c.EqDiff(map[string]struct{}{"a": {}, "b": {}}, keepNames, "keepNames mismatch")
	c.EqDiff(
		map[string]struct{}{"a-secret": {}, "b-secret": {}},
		keepSecretNames,
		"keepSecretNames mismatch",
	)
}

func TestListTolerantOfMissingCRD(t *testing.T) {
	ck := assert.NewCollecting(t)
	// A scheme without the Certificate type makes the client report no mapping
	// for the GVK, which is what a cluster without cert-manager looks like.
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	got, err := List(context.Background(), c, "supabase")
	ck.Require().NoError(err, "List() error")
	ck.Empty(got.Items, "got %d Certificates, want 0", len(got.Items))
}

func TestFindByNameAndOwnedBy(t *testing.T) {
	c := assert.NewCollecting(t)
	certList := &unstructured.UnstructuredList{}
	certList.SetGroupVersionKind(GVK)
	certList.Items = []unstructured.Unstructured{*certFixture(t, "a", "a-secret")}

	c.NotNil(FindByName(certList, "a"), "FindByName(a) = nil, want the Certificate")
	c.Nil(FindByName(certList, "missing"), "FindByName(missing)")
	c.True(OwnedBy(&certList.Items[0], "owner-uid"), "OwnedBy(owner-uid) = false, want true")
	c.False(OwnedBy(&certList.Items[0], "other-uid"), "OwnedBy(other-uid) = true, want false")
}

func TestPruneDeletesUnwantedCertificatesAndSecrets(t *testing.T) {
	ck := assert.NewCollecting(t)
	stale := certFixture(t, "stale", "stale-secret")
	kept := certFixture(t, "kept", "kept-secret")
	unowned := certFixture(t, "unowned", "unowned-secret")
	unowned.SetOwnerReferences(nil)

	staleSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "stale-secret", Namespace: "supabase"},
	}
	c := fakeClient(t, stale, kept, unowned, staleSecret)

	certList, err := List(context.Background(), c, "supabase")
	ck.Require().NoError(err, "List() error =")
	keepNames, keepSecretNames := KeepSets([]*unstructured.Unstructured{kept})
	ck.Require().NoError(Prune(
		context.Background(), c, "supabase", "owner-uid",
		certList, keepNames, keepSecretNames,
	), "Prune() error =")

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
		ck.False(wantGone && err == nil, "Certificate %q still exists, want deleted", name)
		ck.False(!wantGone && err != nil, "Certificate %q was deleted, want kept: %v", name, err)
	}

	ck.Error(c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "stale-secret"},
		&corev1.Secret{},
	), "stale Secret still exists, want deleted")
}

func TestApplySkipsUnchangedCertificates(t *testing.T) {
	ck := assert.NewCollecting(t)
	existing := certFixture(t, "a", "a-secret")
	c := fakeClient(t, existing)

	certList, err := List(context.Background(), c, "supabase")
	ck.Require().NoError(err, "List() error =")
	before := certList.Items[0].GetResourceVersion()

	desired := certFixture(t, "a", "a-secret")
	ck.Require().NoError(Apply(
		context.Background(), c, certList, "owner-uid",
		[]*unstructured.Unstructured{desired},
	), "Apply() error =")

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	ck.Require().NoError(c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	), "Get() error =")
	ck.Eq(before, got.GetResourceVersion(), "resourceVersion changed on a no-op apply")
}

func TestApplyRejectsForeignCertificate(t *testing.T) {
	ck := assert.NewCollecting(t)
	existing := certFixture(t, "a", "a-secret")
	existing.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "example.com/v1",
		Kind:       "Other",
		Name:       "other",
		UID:        "other-uid",
	}})
	c := fakeClient(t, existing)

	certList, err := List(context.Background(), c, "supabase")
	ck.Require().NoError(err, "List() error =")
	before := certList.Items[0].DeepCopy()

	desired := certFixture(t, "a", "a-secret")
	err = Apply(
		context.Background(), c, certList, "owner-uid",
		[]*unstructured.Unstructured{desired},
	)
	ck.Require().Error(err, "Apply() error = nil, want collision error")
	ck.False(
		!strings.Contains(err.Error(), "a") || !strings.Contains(err.Error(), "supabase"),
		"Apply() error = %q, want namespace and name",
		err,
	)

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	ck.Require().NoError(c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	), "foreign Certificate was modified or deleted")
	ck.EqDiff(before.Object, got.Object, "foreign Certificate changed")
}

func TestApplyCreatesMissingCertificates(t *testing.T) {
	ck := assert.NewAborting(t)
	c := fakeClient(t)

	certList, err := List(context.Background(), c, "supabase")
	ck.NoError(err, "List() error =")
	desired := certFixture(t, "a", "a-secret")
	ck.NoError(Apply(
		context.Background(), c, certList, "owner-uid",
		[]*unstructured.Unstructured{desired},
	), "Apply() error =")

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	ck.NoError(c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	), "expected Certificate to be created, got error")
}

func TestGetAbsentAndMissingCRD(t *testing.T) {
	t.Run("absent certificate", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		c := fakeClient(t)
		got, err := Get(context.Background(), c, "supabase", "a")
		ck.Require().NoError(err, "Get() error")
		ck.Nil(got, "Get()")
	})

	t.Run("cert-manager absent", func(t *testing.T) {
		ck := assert.NewCollecting(t)
		// A scheme without the Certificate type is what a cluster with no
		// cert-manager CRD looks like to the client.
		c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
		got, err := Get(context.Background(), c, "supabase", "a")
		ck.Require().NoError(err, "Get() error")
		ck.Nil(got, "Get()")
	})

	t.Run("present certificate", func(t *testing.T) {
		ck := assert.NewAborting(t)
		c := fakeClient(t, certFixture(t, "a", "a-secret"))
		got, err := Get(context.Background(), c, "supabase", "a")
		ck.NoError(err, "Get() error =")
		ck.False(
			got == nil || got.GetName() != "a",
			"Get() = %v, want the Certificate named a",
			got,
		)
	})
}

func TestDeleteRemovesCertificateAndSecret(t *testing.T) {
	ck := assert.NewCollecting(t)
	cert := certFixture(t, "a", "a-secret")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "a-secret", Namespace: "supabase"},
	}
	c := fakeClient(t, cert, secret)

	ck.Require().NoError(Delete(context.Background(), c, cert), "Delete() error =")

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	ck.Error(c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	), "Certificate still exists, want deleted")
	ck.Error(c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a-secret"},
		&corev1.Secret{},
	), "Secret still exists, want deleted")
}

func TestDeleteIsIdempotent(t *testing.T) {
	cert := certFixture(t, "a", "a-secret")
	c := fakeClient(t)

	assert.NewCollecting(t).
		NoError(Delete(context.Background(), c, cert), "Delete() on absent objects error")
}

func TestSpecEqual(t *testing.T) {
	c := assert.NewCollecting(t)
	a := certFixture(t, "a", "a-secret")
	same := certFixture(t, "a", "a-secret")
	other := certFixture(t, "a", "b-secret")

	c.True(SpecEqual(a, same), "SpecEqual() = false for identical specs")
	c.False(SpecEqual(a, other), "SpecEqual() = true for differing specs")
}

func TestApplyOneCreates(t *testing.T) {
	ck := assert.NewAborting(t)
	c := fakeClient(t)
	ck.NoError(
		ApplyOne(context.Background(), c, certFixture(t, "a", "a-secret")),
		"ApplyOne() error =",
	)

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVK)
	ck.NoError(c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "a"},
		got,
	), "expected Certificate to be created, got error")
}
