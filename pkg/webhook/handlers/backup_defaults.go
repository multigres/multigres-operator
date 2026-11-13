package handlers

import (
	"fmt"
	"slices"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// validateBackupDefaultUpdate protects the durable default, including the
// absence of a default on legacy clusters. Only the controller's initial patch
// (before the cleanup finalizer exists) may allocate a prefix.
func validateBackupDefaultUpdate(oldCluster, newCluster *multigresv1alpha1.MultigresCluster) error {
	key := multigresv1alpha1.AnnotationDefaultBackupKeyPrefix
	oldPrefix, newPrefix := oldCluster.Annotations[key], newCluster.Annotations[key]
	if oldPrefix == newPrefix {
		return nil
	}
	if oldPrefix != "" ||
		slices.Contains(oldCluster.Finalizers, multigresv1alpha1.FinalizerClusterCleanup) {
		return fmt.Errorf("annotation %s is immutable after the cluster is initialized", key)
	}
	if oldCluster.UID == "" || newPrefix != "clusters/"+string(oldCluster.UID) {
		return fmt.Errorf("annotation %s must be allocated from the cluster UID", key)
	}
	return nil
}
