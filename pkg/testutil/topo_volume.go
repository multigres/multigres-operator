package testutil

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// AssertTopoVolumeSecret fails unless the topology client TLS volume exists and
// every Secret it projects is named want.
func AssertTopoVolumeSecret(t *testing.T, volumes []corev1.Volume, want string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name != multigresv1alpha1.TopoClientTLSVolumeName {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.Secret.Name != want {
				t.Errorf("topology Secret = %q, want %q", source.Secret.Name, want)
			}
		}
		return
	}
	t.Fatal("topology client volume missing")
}
