package topology

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestManagedProbeTLSSecretUsesOperatorNamespaceWhenAuthManaged(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "ts", Namespace: "project"},
		Spec: multigresv1alpha1.TopoServerSpec{
			TLS:  &multigresv1alpha1.TopoTLSConfig{IssuerName: "issuer"},
			Auth: &multigresv1alpha1.TopoServerAuthSpec{Enabled: true},
		},
	}
	key := AdminSecretKey("operator", ts)
	if key.Namespace != "operator" ||
		key.Name != multigresv1alpha1.TopoAdminCertSecretName("ClusterIssuer", "issuer") {
		t.Fatalf("AdminSecretKey = %v", key)
	}
	// A same-named Secret in the project namespace must never be used.
	decoy := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: "project"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(decoy).Build()
	if _, err := ManagedProbeTLSSecret(t.Context(), c, "operator", ts); err == nil {
		t.Fatal("expected an error while the operator-namespace Secret is missing")
	}
	real := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: "operator"}}
	c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(decoy, real).Build()
	got, err := ManagedProbeTLSSecret(t.Context(), c, "operator", ts)
	if err != nil || got.Namespace != "operator" {
		t.Fatalf("secret = %v, err = %v", got, err)
	}
	if _, err := ManagedProbeTLSSecret(t.Context(), c, "", ts); err == nil {
		t.Fatal("expected an error without an operator namespace")
	}
}

func TestManagedProbeTLSSecretUsesServingSecretWithoutAuth(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: "ts", Namespace: "project"},
	}
	serving := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: multigresv1alpha1.TopoServerCertSecretName("ts"), Namespace: "project",
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(serving).Build()
	got, err := ManagedProbeTLSSecret(t.Context(), c, "operator", ts)
	if err != nil || got.Name != serving.Name {
		t.Fatalf("secret = %v, err = %v", got, err)
	}
}
