//go:build integration
// +build integration

package multigrescluster_test

import (
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
}
