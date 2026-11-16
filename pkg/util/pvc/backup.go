package pvc

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
)

// RecordBackupPrefix backfills an allocation on existing storage. Checking
// resourceVersion prevents a concurrent adoption or replacement from being overwritten.
func RecordBackupPrefix(
	ctx context.Context,
	c client.Client,
	claim *corev1.PersistentVolumeClaim,
	prefix string,
) error {
	key := api.AnnotationDefaultBackupKeyPrefix
	saved := claim.Annotations[key]
	if saved == prefix {
		return nil
	}
	if saved != "" {
		return fmt.Errorf(
			"PVC %s records backup allocation %q, but the cluster uses %q; verify the original repository before reusing the volume",
			claim.Name,
			saved,
			prefix,
		)
	}
	updated := claim.DeepCopy()
	metav1.SetMetaDataAnnotation(&updated.ObjectMeta, key, prefix)
	patch := client.MergeFromWithOptions(claim, client.MergeFromWithOptimisticLock{})
	if err := c.Patch(ctx, updated, patch); err != nil {
		return fmt.Errorf("recording backup allocation on PVC %s: %w", claim.Name, err)
	}
	*claim = *updated
	return nil
}
