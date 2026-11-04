package multigrescluster

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

func (r *MultigresClusterReconciler) reconcileCells(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) (bool, error) {
	existingCells := &multigresv1alpha1.CellList{}
	if err := r.List(
		ctx,
		existingCells,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"multigres.com/cluster": cluster.Name},
	); err != nil {
		return false, fmt.Errorf("failed to list existing cells: %w", err)
	}

	globalTopoRef, err := r.globalTopoRef(ctx, cluster, res)
	if err != nil {
		return false, fmt.Errorf("failed to get global topo ref: %w", err)
	}

	activeCellNames := make(map[multigresv1alpha1.CellName]bool, len(cluster.Spec.Cells))
	existingVersions := make(map[string]string, len(existingCells.Items))
	for i := range existingCells.Items {
		existingVersions[existingCells.Items[i].Name] = existingCells.Items[i].ResourceVersion
	}

	allCellNames := []multigresv1alpha1.CellName{}
	for _, cellCfg := range cluster.Spec.Cells {
		allCellNames = append(allCellNames, cellCfg.Name)
	}

	for _, cellCfg := range cluster.Spec.Cells {
		activeCellNames[cellCfg.Name] = true

		// Apply global CellTemplate default (Level 2 in the 4-level override chain)
		cellCfg.CellTemplate = cluster.Spec.EffectiveCellTemplate(cellCfg.CellTemplate)

		gatewaySpec, gatewayPlacement, localTopoSpec, err := res.ResolveCell(ctx, cluster, &cellCfg)
		if err != nil {
			r.Recorder.Event(cluster, "Warning", "TemplateMissing", err.Error())
			return false, fmt.Errorf("failed to resolve cell '%s': %w", cellCfg.Name, err)
		}

		desired, err := BuildCell(
			cluster,
			&cellCfg,
			gatewaySpec,
			gatewayPlacement,
			localTopoSpec,
			globalTopoRef,
			allCellNames,
			r.Scheme,
		)
		if err != nil {
			return false, fmt.Errorf("failed to build cell '%s': %w", cellCfg.Name, err)
		}

		// Server Side Apply
		desired.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("Cell"))
		if err := r.Patch(
			ctx,
			desired,
			client.Apply,
			client.ForceOwnership,
			client.FieldOwner(ownership.ManagerChildWriter),
		); err != nil {
			return false, fmt.Errorf("failed to apply cell '%s': %w", cellCfg.Name, err)
		}
		if before, found := existingVersions[desired.Name]; !found ||
			before != desired.ResourceVersion {
			r.Recorder.Eventf(cluster, "Normal", "Applied", "Applied Cell %s", desired.Name)
		}
	}

	var pendingDeletion bool

	for i := range existingCells.Items {
		item := &existingCells.Items[i]
		if activeCellNames[item.Spec.Name] {
			if err := r.cancelCellPendingDeletion(ctx, cluster, item); err != nil {
				return false, err
			}
			continue
		}

		// Step 1: Set PendingDeletion annotation if not already set.
		if item.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			patch := client.MergeFrom(item.DeepCopy())
			if item.Annotations == nil {
				item.Annotations = make(map[string]string)
			}
			item.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = metav1.Now().
				UTC().Format(time.RFC3339)
			if err := r.Patch(ctx, item, patch); err != nil {
				return false, fmt.Errorf("failed to set PendingDeletion on cell '%s': %w",
					item.Name, err)
			}
			r.Recorder.Eventf(cluster, "Normal", "PendingDeletion",
				"Marked Cell %s for graceful deletion", item.Name)
			pendingDeletion = true
			continue
		}

		// Step 2: Wait for ReadyForDeletion condition.
		if !meta.IsStatusConditionTrue(
			item.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion,
		) {
			pendingDeletion = true
			continue
		}

		// Step 3: Drain complete — safe to delete. The read above may be a
		// cached one, and a Cell can be cancelled and stamped again, so the
		// delete is conditioned on the exact version that showed the stamp
		// and the condition together. A Cell that changed since (a cancel, a
		// cleared condition, a new stamp) is left for the next pass to judge.
		err := r.Delete(ctx, item, client.Preconditions{
			UID:             &item.UID,
			ResourceVersion: &item.ResourceVersion,
		})
		switch {
		case err == nil:
			r.Recorder.Eventf(cluster, "Normal", "Deleted",
				"Deleted orphaned Cell %s", item.Name)
		case errors.IsConflict(err):
			log.FromContext(ctx).V(1).Info("Orphaned Cell changed since it was judged ready; "+
				"re-judging next pass", "cell", item.Name)
			pendingDeletion = true
		case !errors.IsNotFound(err):
			return false, fmt.Errorf("failed to delete orphaned cell '%s': %w", item.Name, err)
		}
	}

	return pendingDeletion, nil
}

// cancelCellPendingDeletion removes AnnotationPendingDeletion from a Cell the
// cluster spec names again. A Cell keeps that annotation until something
// removes it, and while it is set the Cell controller does nothing but drain
// it, so a Cell put back into spec after the stamp, and before the delete,
// would otherwise stay drained for good: gateway no longer reconciled, managed
// local TopoServer deleted and never recreated, ReadyForDeletion left True.
//
// Only the annotation is touched. ReadyForDeletion is the Cell controller's
// own status, and it drops the condition on its next normal reconcile, which
// the annotation's removal enqueues. A terminating Cell is left alone: its
// delete is already committed, and the next pass recreates it from spec once
// it is gone.
func (r *MultigresClusterReconciler) cancelCellPendingDeletion(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	cell *multigresv1alpha1.Cell,
) error {
	if cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" ||
		!cell.DeletionTimestamp.IsZero() {
		return nil
	}
	// Conditioned on the version read, so a stale cached Cell (one deleted and
	// recreated, or already cancelled) is re-judged on the next pass rather
	// than patched blind.
	patch := client.MergeFromWithOptions(cell.DeepCopy(), client.MergeFromWithOptimisticLock{})
	delete(cell.Annotations, multigresv1alpha1.AnnotationPendingDeletion)
	if err := r.Patch(ctx, cell, patch); errors.IsConflict(err) || errors.IsNotFound(err) {
		log.FromContext(ctx).V(1).Info("Cell changed before its pending deletion was cancelled; "+
			"re-judging next pass", "cell", cell.Name, "error", err.Error())
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to clear PendingDeletion on cell '%s': %w", cell.Name, err)
	}
	r.Recorder.Eventf(cluster, "Normal", "PendingDeletionCancelled",
		"Cell %s is back in spec; cancelled its pending deletion", cell.Name)
	return nil
}
