package shard

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

const (
	conditionStorageClassValid    = "StorageClassValid"
	storageClassDependencyRequeue = 10 * time.Second

	storageClassNotFoundReason     = "StorageClassNotFound"
	storageClassFoundReason        = "StorageClassFound"
	storageClassNotSpecifiedReason = "StorageClassNotSpecified"
)

type missingStorageClassDependencyError struct {
	className string
}

func (e *missingStorageClassDependencyError) Error() string {
	return fmt.Sprintf("referenced StorageClass %q was not found", e.className)
}

func isMissingStorageClassDependency(err error) bool {
	var depErr *missingStorageClassDependencyError
	return errors.As(err, &depErr)
}

// storageClassCheck is the whole StorageClassValid verdict for one reconcile:
// the condition to publish, plus the dependency errors that gate the reconcile.
//
// It exists so the condition has exactly one writer per reconcile. The backup
// and pool checks used to write it independently with different messages, and
// since setStorageClassCondition's skip-if-unchanged test compares the
// persisted message, each call saw the other's message and rewrote it, forever.
//
// backupDependency and poolDependency are held apart because they gate
// different points in Reconcile: a missing backup class must stop before the
// shared backup PVC is created, a missing pool class before the pool
// workloads. Each is a *missingStorageClassDependencyError when set.
type storageClassCheck struct {
	status  metav1.ConditionStatus
	reason  string
	message string

	backupDependency error
	poolDependency   error
}

func backupFilesystemStorageClassName(shard *multigresv1alpha1.Shard) string {
	if shard.Spec.Backup == nil ||
		shard.Spec.Backup.Type != multigresv1alpha1.BackupTypeFilesystem {
		return ""
	}
	if shard.Spec.Backup.Filesystem == nil {
		return ""
	}
	return shard.Spec.Backup.Filesystem.Storage.Class
}

func (r *ShardReconciler) validateStorageClassExists(
	ctx context.Context,
	className string,
) (bool, error) {
	if className == "" {
		return true, nil
	}

	sc := &storagev1.StorageClass{}
	err := r.Get(ctx, client.ObjectKey{Name: className}, sc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get StorageClass %q: %w", className, err)
	}
	return true, nil
}

// validateStorageClassDependencies resolves every StorageClass the shard
// references and reduces them to a single verdict. It writes nothing: the
// caller publishes the condition once and then gates on the missing-class
// fields, which is what keeps the condition single-writer.
//
// A missing backup class short-circuits before the pools are looked at, so the
// reported message matches the order in which Reconcile gates on them.
func (r *ShardReconciler) validateStorageClassDependencies(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (storageClassCheck, error) {
	backupClass := backupFilesystemStorageClassName(shard)
	if backupClass != "" {
		exists, err := r.validateStorageClassExists(ctx, backupClass)
		if err != nil {
			return storageClassCheck{}, fmt.Errorf(
				"failed to validate backup StorageClass %q: %w",
				backupClass,
				err,
			)
		}
		if !exists {
			return storageClassCheck{
				status: metav1.ConditionFalse,
				reason: storageClassNotFoundReason,
				message: fmt.Sprintf(
					"StorageClass %q not found for shared backup PVCs",
					backupClass,
				),
				backupDependency: &missingStorageClassDependencyError{className: backupClass},
			}, nil
		}
	}

	// Pools are visited in name order because Spec.Pools is a map: reporting
	// whichever missing class Go's randomised map iteration reached first would
	// flap the condition message between reconciles.
	hasExplicitPoolClass := false
	for _, poolName := range slices.Sorted(maps.Keys(shard.Spec.Pools)) {
		poolClass := shard.Spec.Pools[poolName].Storage.Class
		if poolClass == "" {
			continue
		}
		hasExplicitPoolClass = true

		exists, err := r.validateStorageClassExists(ctx, poolClass)
		if err != nil {
			return storageClassCheck{}, fmt.Errorf(
				"failed to validate StorageClass %q for pool %s: %w",
				poolClass,
				poolName,
				err,
			)
		}
		if !exists {
			return storageClassCheck{
				status: metav1.ConditionFalse,
				reason: storageClassNotFoundReason,
				message: fmt.Sprintf(
					"StorageClass %q not found for pool %s",
					poolClass,
					poolName,
				),
				poolDependency: &missingStorageClassDependencyError{className: poolClass},
			}, nil
		}
	}

	if backupClass == "" && !hasExplicitPoolClass {
		return storageClassCheck{
			status:  metav1.ConditionTrue,
			reason:  storageClassNotSpecifiedReason,
			message: "No explicit backup filesystem or pool StorageClass configured; using cluster default",
		}, nil
	}

	return storageClassCheck{
		status:  metav1.ConditionTrue,
		reason:  storageClassFoundReason,
		message: "All explicitly configured StorageClasses are present",
	}, nil
}

// setStorageClassCondition patches the StorageClassValid condition using SSA.
// Uses FieldOwner("multigres-resource-handler-guard") to avoid ownership conflicts
// with updateStatus which uses FieldOwner("multigres-resource-handler").
//
// The payload is unstructured and carries status.conditions and nothing else.
// An SSA apply payload is a complete statement of what its field manager owns,
// so a payload built from a partially-populated typed struct silently asserts
// the zero value of every non-omitempty field in it: a ShardStatus literal that
// sets only Conditions still serialises orchReady:false and poolsReady:false,
// and with ForceOwnership it seizes both fields from updateStatus, which forces
// them straight back on its next write. That is a permanent ping-pong, one
// round trip per reconcile, each one scheduling the next reconcile.
//
// Reads the latest condition from the API server (not the in-memory shard) to
// avoid false skips when the in-memory object is stale.
// TODO: This stale-safe condition skip logic is mirrored in the TopoServer
// guard; extract a shared helper to reduce duplication.
func (r *ShardReconciler) setStorageClassCondition(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	check storageClassCheck,
) error {
	// Read the latest from the API server so the skip-if-unchanged check
	// compares against the real persisted state, not a potentially stale
	// in-memory copy.
	latest := &multigresv1alpha1.Shard{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(shard), latest); err != nil {
		return fmt.Errorf("failed to get Shard for StorageClass condition check: %w", err)
	}

	existing := meta.FindStatusCondition(latest.Status.Conditions, conditionStorageClassValid)
	if existing != nil &&
		existing.Status == check.status &&
		existing.Reason == check.reason &&
		existing.Message == check.message &&
		existing.ObservedGeneration == latest.Generation {
		return nil
	}

	// Preserve LastTransitionTime when the status hasn't transitioned,
	// matching the behaviour of meta.SetStatusCondition.
	now := metav1.Now()
	if existing != nil && existing.Status == check.status {
		now = existing.LastTransitionTime
	}

	// Converted from the typed condition rather than hand-built so the wire
	// encoding, notably the metav1.Time format, cannot drift from the API's.
	cond, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&metav1.Condition{
		Type:               conditionStorageClassValid,
		Status:             check.status,
		Reason:             check.reason,
		Message:            check.message,
		ObservedGeneration: latest.Generation,
		LastTransitionTime: now,
	})
	if err != nil {
		return fmt.Errorf("failed to encode Shard StorageClass condition: %w", err)
	}

	patchObj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": multigresv1alpha1.GroupVersion.String(),
		"kind":       "Shard",
		"metadata": map[string]any{
			"name":      shard.Name,
			"namespace": shard.Namespace,
		},
		"status": map[string]any{
			"conditions": []any{cond},
		},
	}}

	if err := r.Status().Patch(
		ctx,
		patchObj,
		client.Apply,
		client.FieldOwner("multigres-resource-handler-guard"),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to patch Shard StorageClass condition: %w", err)
	}

	return nil
}
