package multigrescluster

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
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

	allCellNames := []multigresv1alpha1.CellName{}
	for _, cellCfg := range cluster.Spec.Cells {
		allCellNames = append(allCellNames, cellCfg.Name)
	}

	for _, cellCfg := range cluster.Spec.Cells {
		activeCellNames[cellCfg.Name] = true

		// Apply global CellTemplate default (Level 2 in the 4-level override chain)
		cellCfg.CellTemplate = cluster.Spec.EffectiveCellTemplate(cellCfg.CellTemplate)

		gatewaySpec, gatewayPlacement, localTopoSpec, err := res.ResolveCell(ctx, &cellCfg)
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
			client.FieldOwner("multigres-operator"),
		); err != nil {
			return false, fmt.Errorf("failed to apply cell '%s': %w", cellCfg.Name, err)
		}
		r.Recorder.Eventf(cluster, "Normal", "Applied", "Applied Cell %s", desired.Name)
	}

	var pendingDeletion bool

	for i := range existingCells.Items {
		item := &existingCells.Items[i]
		if activeCellNames[item.Spec.Name] {
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

		// Step 3: Drain complete — safe to delete.
		if err := r.Delete(ctx, item); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("failed to delete orphaned cell '%s': %w", item.Name, err)
		} else if err == nil {
			r.Recorder.Eventf(cluster, "Normal", "Deleted",
				"Deleted orphaned Cell %s", item.Name)
		}
	}

	return pendingDeletion, nil
}
