package toposerver

import (
	"context"
	"errors"
	"fmt"
	"time"

	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

const (
	conditionStorageClassValid    = "StorageClassValid"
	storageClassDependencyRequeue = 10 * time.Second

	storageClassNotFoundReason     = "StorageClassNotFound"
	storageClassReadyReason        = "StorageClassReady"
	storageClassNotSpecifiedReason = "StorageClassNotSpecified"
	storageClassBindingModeReason  = "StorageClassBindingModeInvalid"
)

// missingStorageClassDependencyError signals a missing StorageClass (requeue calmly,
// not exponential backoff).
type missingStorageClassDependencyError struct {
	className string
}

// invalidStorageClassBindingModeError signals that a StorageClass can bind a
// volume before the scheduler selects a zone for its pod.
type invalidStorageClassBindingModeError struct {
	className string
	mode      storagev1.VolumeBindingMode
}

func (e *invalidStorageClassBindingModeError) Error() string {
	return fmt.Sprintf(
		"referenced StorageClass %q uses volumeBindingMode %q; topology-server storage requires %q",
		e.className,
		e.mode,
		storagev1.VolumeBindingWaitForFirstConsumer,
	)
}

func (e *missingStorageClassDependencyError) Error() string {
	return fmt.Sprintf("referenced StorageClass %q was not found", e.className)
}

// isMissingStorageClassDependency checks whether err is a missing StorageClass dependency.
func isMissingStorageClassDependency(err error) bool {
	var depErr *missingStorageClassDependencyError
	return errors.As(err, &depErr)
}

func isStorageClassDependencyError(err error) bool {
	if isMissingStorageClassDependency(err) {
		return true
	}
	var bindingErr *invalidStorageClassBindingModeError
	return errors.As(err, &bindingErr)
}

// getStorageClass returns the named StorageClass when it exists.
func (r *TopoServerReconciler) getStorageClass(
	ctx context.Context,
	className string,
) (*storagev1.StorageClass, error) {
	sc := &storagev1.StorageClass{}
	err := r.Get(ctx, client.ObjectKey{Name: className}, sc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get StorageClass %q: %w", className, err)
	}
	return sc, nil
}

// validateEtcdStorageClassDependency runs before StatefulSet apply.
// Empty class passes through. An explicit class must exist and delay volume
// binding until the scheduler has selected a node and availability zone.
func (r *TopoServerReconciler) validateEtcdStorageClassDependency(
	ctx context.Context,
	toposerver *multigresv1alpha1.TopoServer,
) error {
	var etcdClass string
	if toposerver.Spec.Etcd != nil {
		etcdClass = toposerver.Spec.Etcd.Storage.Class
	}

	if etcdClass == "" {
		return r.setStorageClassCondition(
			ctx,
			toposerver,
			metav1.ConditionTrue,
			storageClassNotSpecifiedReason,
			"No explicit etcd StorageClass configured; using cluster default",
		)
	}

	sc, err := r.getStorageClass(ctx, etcdClass)
	if err != nil {
		return fmt.Errorf("failed to validate etcd StorageClass %q: %w", etcdClass, err)
	}
	if sc == nil {
		msg := fmt.Sprintf("StorageClass %q not found for etcd PVCs", etcdClass)
		if setErr := r.setStorageClassCondition(
			ctx,
			toposerver,
			metav1.ConditionFalse,
			storageClassNotFoundReason,
			msg,
		); setErr != nil {
			return setErr
		}
		r.Recorder.Event(toposerver, "Warning", storageClassNotFoundReason, msg)
		return &missingStorageClassDependencyError{className: etcdClass}
	}

	mode := storagev1.VolumeBindingImmediate
	if sc.VolumeBindingMode != nil {
		mode = *sc.VolumeBindingMode
	}
	if mode != storagev1.VolumeBindingWaitForFirstConsumer {
		msg := fmt.Sprintf(
			"StorageClass %q uses volumeBindingMode %q; topology-server storage requires %q",
			etcdClass,
			mode,
			storagev1.VolumeBindingWaitForFirstConsumer,
		)
		if setErr := r.setStorageClassCondition(
			ctx,
			toposerver,
			metav1.ConditionFalse,
			storageClassBindingModeReason,
			msg,
		); setErr != nil {
			return setErr
		}
		r.Recorder.Event(toposerver, "Warning", storageClassBindingModeReason, msg)
		return &invalidStorageClassBindingModeError{className: etcdClass, mode: mode}
	}

	return r.setStorageClassCondition(
		ctx,
		toposerver,
		metav1.ConditionTrue,
		storageClassReadyReason,
		fmt.Sprintf(
			"StorageClass %q uses volumeBindingMode %q",
			etcdClass,
			storagev1.VolumeBindingWaitForFirstConsumer,
		),
	)
}

// setStorageClassCondition patches the StorageClassValid condition using SSA.
// Uses FieldOwner("multigres-operator-guard") to avoid ownership conflicts with
// updateStatus which uses FieldOwner("multigres-operator").

// TODO: This stale-safe condition skip logic is mirrored in the Shard guard;
// extract a shared helper to reduce duplication.
func (r *TopoServerReconciler) setStorageClassCondition(
	ctx context.Context,
	toposerver *multigresv1alpha1.TopoServer,
	condStatus metav1.ConditionStatus,
	reason string,
	message string,
) error {
	// Read the latest from the API server so the skip-if-unchanged check
	// compares against the real persisted state, not a potentially stale
	// in-memory copy.
	latest := &multigresv1alpha1.TopoServer{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(toposerver), latest); err != nil {
		return fmt.Errorf("failed to get TopoServer for StorageClass condition check: %w", err)
	}

	existing := meta.FindStatusCondition(latest.Status.Conditions, conditionStorageClassValid)
	if existing != nil &&
		existing.Status == condStatus &&
		existing.Reason == reason &&
		existing.Message == message &&
		existing.ObservedGeneration == latest.Generation {
		return nil
	}

	// Preserve LastTransitionTime when the status hasn't transitioned,
	// matching the behavior of meta.SetStatusCondition.
	now := metav1.Now()
	if existing != nil && existing.Status == condStatus {
		now = existing.LastTransitionTime
	}

	patchObj := &multigresv1alpha1.TopoServer{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "TopoServer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      toposerver.Name,
			Namespace: toposerver.Namespace,
		},
		Status: multigresv1alpha1.TopoServerStatus{
			Conditions: []metav1.Condition{
				{
					Type:               conditionStorageClassValid,
					Status:             condStatus,
					Reason:             reason,
					Message:            message,
					ObservedGeneration: latest.Generation,
					LastTransitionTime: now,
				},
			},
		},
	}

	if err := r.Status().Patch(
		ctx,
		patchObj,
		client.Apply,
		client.FieldOwner("multigres-operator-guard"),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to patch TopoServer StorageClass condition: %w", err)
	}

	return nil
}
