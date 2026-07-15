package shard

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// reconcileMultiorchDeployment creates or updates the Multiorch Deployment for a specific cell.
func (r *ShardReconciler) reconcileMultiorchDeployment(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellName string,
) error {
	desired, err := BuildMultiorchDeployment(shard, cellName, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build Multiorch Deployment: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply Multiorch Deployment: %w", err)
	}

	r.Recorder.Eventf(
		shard,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}

// reconcileMultiorchService creates or updates the Multiorch Service for a specific cell.
func (r *ShardReconciler) reconcileMultiorchService(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellName string,
) error {
	desired, err := BuildMultiorchService(shard, cellName, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build Multiorch Service: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply Multiorch Service: %w", err)
	}

	r.Recorder.Eventf(
		shard,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}
