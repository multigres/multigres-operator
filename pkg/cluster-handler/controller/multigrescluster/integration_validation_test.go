//go:build integration
// +build integration

package multigrescluster_test

import (
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestMultigresCluster_Validation(t *testing.T) {
	t.Parallel()

	t.Run("Explicit Empty GlobalTopoServer (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-empty-struct", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{}, // Not nil, but empty
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with empty GlobalTopoServer struct, got nil")
		}
		// We expect CEL validation error here
		if !strings.Contains(err.Error(), "must specify exactly one of") {
			t.Errorf("Expected CEL validation error, got: %v", err)
		}
	})

	t.Run("Multiadmin XOR Violation (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-xor-admin", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Multiadmin: &multigresv1alpha1.MultiadminConfig{
					Spec:        &multigresv1alpha1.StatelessSpec{},
					TemplateRef: "some-template",
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with Multiadmin XOR violation, got nil")
		}
		if !strings.Contains(err.Error(), "cannot specify both") {
			t.Errorf("Expected CEL validation error, got: %v", err)
		}
	})

	t.Run("Multiple Databases (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-multi-db", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Databases: []multigresv1alpha1.DatabaseConfig{
					{Name: "postgres", Default: true},
					{Name: "analytics"},
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with multiple databases, got nil")
		}
		// Expect MaxItems=1 or system database rule violation
		if !strings.Contains(err.Error(), "Invalid value") && !strings.Contains(err.Error(), "only the single system database") {
			t.Errorf("Expected validation error regarding DB count/rules, got: %v", err)
		}
	})

	// Topology access is only as narrow as the CA that backs it, so an enabled
	// configuration has to name its issuer rather than inherit one.
	t.Run("Topology TLS Without Issuer (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "fail-topo-tls", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(true)},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		err := k8sClient.Create(t.Context(), cluster)
		if err == nil {
			t.Fatal("Expected error creating cluster with topology TLS and no issuer, got nil")
		}
		if !strings.Contains(err.Error(), "issuerName is required when topology TLS is enabled") {
			t.Errorf("Expected CEL validation error, got: %v", err)
		}
	})

	t.Run("Topology TLS With Issuer (Should Pass)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pass-topo-tls", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{
					Enabled:    ptr.To(true),
					IssuerName: "multigres-infra-issuer",
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected cluster with topology TLS and an issuer to be accepted, got: %v", err)
		}
	})

	// Disabled and absent configurations stay valid without an issuer, so the
	// rule cannot break clusters that never opt in.
	t.Run("Topology TLS Disabled Without Issuer (Should Pass)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pass-topo-tls-off", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(false)},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected cluster with topology TLS disabled to be accepted, got: %v", err)
		}
	})

	// Enablement is fixed at creation: flipping it on a running cluster cannot be
	// rolled out safely, so the API rejects the transition.
	t.Run("Enabling Topology TLS After Creation (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "mutate-topo-tls", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{Enabled: ptr.To(false)},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected initial cluster to be accepted, got: %v", err)
		}

		cluster.Spec.TopoTLS = &multigresv1alpha1.TopoTLSConfig{
			Enabled:    ptr.To(true),
			IssuerName: "multigres-infra-issuer",
		}
		if err := k8sClient.Update(t.Context(), cluster); err == nil {
			t.Fatal("Expected enabling topology TLS after creation to be rejected")
		}
	})

	// Rotating the issuer rotates the CA every certificate chains to, which the
	// running clients cannot follow, so changing it after creation is rejected
	// too.
	t.Run("Changing Topology TLS Issuer After Creation (Should Fail)", func(t *testing.T) {
		t.Parallel()
		k8sClient, _ := setupIntegration(t)
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "rotate-topo-issuer", Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TopoTLS: &multigresv1alpha1.TopoTLSConfig{
					Enabled:    ptr.To(true),
					IssuerName: "issuer-a",
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		if err := k8sClient.Create(t.Context(), cluster); err != nil {
			t.Fatalf("Expected initial cluster to be accepted, got: %v", err)
		}

		cluster.Spec.TopoTLS.IssuerName = "issuer-b"
		if err := k8sClient.Update(t.Context(), cluster); err == nil {
			t.Fatal("Expected changing the topology TLS issuer after creation to be rejected")
		}
	})
}
