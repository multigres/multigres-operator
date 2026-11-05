package pvc

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/go-logr/logr"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// MarkOrphan stamps a PVC with multigres.com/orphan-since=<now in
// OrphanTimestampFormat> and removes any controller ownerRef whose UID matches
// ownerUID. The downstream multigres-gc CronJob deletes such PVCs once the
// configured retention has elapsed.
//
// Idempotent: a PVC already carrying the label is left untouched (preserves
// the original orphan timestamp so retention is computed from when the PVC
// first became orphan, not from the most recent reconciliation).
//
// ownerUID may be the zero value, in that case no ownerRef is stripped. The
// label patch still runs.
func MarkOrphan(
	ctx context.Context,
	c client.Client,
	pvc *corev1.PersistentVolumeClaim,
	ownerUID types.UID,
	now time.Time,
) error {
	if pvc == nil {
		return fmt.Errorf("MarkOrphan: nil PVC")
	}

	// Build a JSON-merge patch from a deep copy so we don't mutate the caller's
	// object until the API call succeeds.
	base := pvc.DeepCopy()
	patch := client.MergeFrom(base)

	mutated := false

	if _, ok := pvc.Labels[metadata.LabelOrphan]; !ok {
		if pvc.Labels == nil {
			pvc.Labels = map[string]string{}
		}
		pvc.Labels[metadata.LabelOrphan] = now.UTC().Format(metadata.OrphanTimestampFormat)
		mutated = true
	}

	if ownerUID != "" {
		filtered := pvc.OwnerReferences[:0:0]
		for _, ref := range pvc.OwnerReferences {
			if ref.UID == ownerUID {
				continue
			}
			filtered = append(filtered, ref)
		}
		if len(filtered) != len(pvc.OwnerReferences) {
			pvc.OwnerReferences = filtered
			mutated = true
		}
	}

	if !mutated {
		return nil
	}

	return c.Patch(ctx, pvc, patch)
}

// ErrTerminating is returned by ClearOrphan for a PVC that already has a
// deletionTimestamp. Such a PVC cannot be put back into service: it disappears,
// together with its data, as soon as the last pod mounting it stops, and an
// apply of the same name afterwards creates a new, empty claim.
var ErrTerminating = errors.New("PVC is being deleted")

// IsTerminating reports whether a PVC has been deleted and is only held in
// place by finalizers, normally kubernetes.io/pvc-protection while a pod still
// mounts it.
func IsTerminating(pvc *corev1.PersistentVolumeClaim) bool {
	return pvc != nil && !pvc.DeletionTimestamp.IsZero()
}

// ClearOrphan removes the multigres.com/orphan-since label from a PVC, marking
// it live again. We call this before reusing a deterministically-named PVC (e.g.
// when a scaled-down pool scales back up onto the same PVC) so the multigres-gc
// CronJob does not delete storage that has been put back into service.
//
// The patch carries pvc's resourceVersion as a precondition. multigres-gc
// deletes with the resourceVersion it listed, so whichever write commits first
// makes the other fail with a Conflict: either the label is cleared and the
// deletion is refused, or the deletion stands and this returns an error rather
// than reporting a reclaim of a PVC that is already on its way out. A caller
// holding a stale copy gets a Conflict too and retries from a fresh read.
//
// A PVC that is already being deleted is refused with ErrTerminating, label or
// not, because the caller is about to depend on storage that will not survive.
func ClearOrphan(
	ctx context.Context,
	logger logr.Logger,
	c client.Client,
	pvc *corev1.PersistentVolumeClaim,
) error {
	if pvc == nil {
		return fmt.Errorf("ClearOrphan: nil PVC")
	}
	if IsTerminating(pvc) {
		return fmt.Errorf("%w: %s/%s", ErrTerminating, pvc.Namespace, pvc.Name)
	}
	since, ok := pvc.Labels[metadata.LabelOrphan]
	if !ok {
		return nil
	}

	patch := client.MergeFromWithOptions(pvc.DeepCopy(), client.MergeFromWithOptimisticLock{})
	delete(pvc.Labels, metadata.LabelOrphan)
	if err := c.Patch(ctx, pvc, patch); err != nil {
		// Leave the caller's copy describing the object as it still is.
		pvc.Labels[metadata.LabelOrphan] = since
		return err
	}
	logger.Info("Cleared orphan label on PVC", "pvc", pvc.Name)
	return nil
}

// HasOrphanLabel reports whether a PVC has already been marked orphan.
func HasOrphanLabel(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc == nil {
		return false
	}
	_, ok := pvc.Labels[metadata.LabelOrphan]
	return ok
}
