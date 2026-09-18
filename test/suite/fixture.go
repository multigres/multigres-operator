package suite

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// The name of a Secret object, not a credential. gosec matches on the string
// value rather than on how it is used, so the suppression has to be explicit.
//
//nolint:gosec // G101: this is a Secret's name; the password itself is set below
const adminSecretName = "multigres-admin-password"

// MinimalCluster creates the equivalent of config/samples/minimal.yaml in ns,
// along with the password Secret it references, and returns it.
//
// Built in Go rather than read from the sample, because the e2e loader
// (framework.MustLoadCluster) is behind //go:build e2e and this package
// deliberately has no build tag.
//
// The Secret is intentionally unlabelled, matching what a user would create.
// Under the production cache config that makes it invisible to the cached
// client, so this fixture also exercises why the reconcilers hold an APIReader.
func MinimalCluster(t *testing.T, ns, name string) *multigresv1alpha1.MultigresCluster {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: adminSecretName, Namespace: ns},
		StringData: map[string]string{"password": "postgres"},
	}
	if err := Suite.Client.Create(t.Context(), secret); err != nil {
		t.Fatalf("create password secret: %v", err)
	}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: adminSecretName,
				Key:  "password",
			},
			PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			Cells: []multigresv1alpha1.CellConfig{
				{Name: defaultSimCell, ZoneID: "us-central1-a"},
			},
		},
	}
	if err := Suite.Client.Create(t.Context(), cluster); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	return cluster
}
