package ctrltest

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PVCOf returns the PVC bound by the named pod's data volume, read from the
// pod's volumes rather than derived by rebuilding the operator's naming
// scheme. Recomputing the name would prove only that the operator's naming
// scheme is consistent with itself, not that this pod is bound to the PVC it
// should be.
//
// A pod may carry several PVC-backed volumes, which is why selection is by
// name.
func PVCOf(ctx context.Context, c client.Client, ns, pod, volumeName string) (string, error) {
	key := client.ObjectKey{Namespace: ns, Name: pod}
	p := &corev1.Pod{}
	if err := c.Get(ctx, key, p); err != nil {
		return "", fmt.Errorf("get pod %s: %w", key, err)
	}

	var claims []string
	for _, vol := range p.Spec.Volumes {
		if vol.Name == volumeName && vol.PersistentVolumeClaim != nil {
			claims = append(claims, vol.PersistentVolumeClaim.ClaimName)
		}
	}

	switch len(claims) {
	case 0:
		return "", fmt.Errorf(
			"pod %s: no volume named %s is bound to a PersistentVolumeClaim",
			key, volumeName,
		)
	case 1:
		return claims[0], nil
	default:
		return "", fmt.Errorf(
			"pod %s: more than one volume named %s claims a PVC: %s",
			key, volumeName, strings.Join(claims, ", "),
		)
	}
}
