package handlers

import (
	"context"
	"fmt"
	"slices"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/backup"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

// validateBackupDefaultUpdate protects the durable default, including the
// absence of a default on legacy clusters. Initial allocation must be part of
// the update that adds the cleanup finalizer.
func (v *MultigresClusterValidator) validateBackupDefaultUpdate(
	ctx context.Context,
	oldCluster, newCluster *multigresv1alpha1.MultigresCluster,
) error {
	key := multigresv1alpha1.AnnotationDefaultBackupKeyPrefix
	oldPrefix, newPrefix := oldCluster.Annotations[key], newCluster.Annotations[key]
	if oldPrefix == newPrefix {
		return nil
	}
	if oldPrefix != "" ||
		slices.Contains(oldCluster.Finalizers, multigresv1alpha1.FinalizerClusterCleanup) {
		return fmt.Errorf("annotation %s is immutable after the cluster is initialized", key)
	}
	if !slices.Contains(newCluster.Finalizers, multigresv1alpha1.FinalizerClusterCleanup) {
		return fmt.Errorf(
			"annotation %s must be allocated together with the cluster cleanup finalizer",
			key,
		)
	}
	reader := v.APIReader
	if reader == nil {
		reader = v.Client
	}
	claim, retained, err := backup.RetainedDefault(
		ctx,
		reader,
		resolver.NewResolver(v.Client, newCluster.Namespace),
		newCluster,
	)
	if err != nil {
		return err
	}
	if claim == "" && oldCluster.UID != "" {
		retained = "clusters/" + string(oldCluster.UID)
	}
	if retained == "" || newPrefix != retained {
		return fmt.Errorf(
			"annotation %s must match the cluster UID or the allocation on retained storage",
			key,
		)
	}
	return nil
}
