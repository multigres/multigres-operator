package tablegroup

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// stepListChildShards reads the children once for the normal reconcile path.
// Later steps apply desired children and prune orphans against this same
// snapshot, preserving each child's observed .Status for status aggregation.
func stepListChildShards(ctx context.Context, rc *reconcileContext) (stepResult, error) {
	observed := &multigresv1alpha1.ShardList{}
	if err := rc.r.List(ctx, observed, childShardSelector(rc.tg)...); err != nil {
		return stepResult{}, newStepError(
			fmt.Errorf("failed to list shards for pruning: %w", err),
			"Warning",
			"CleanUpError",
			fmt.Sprintf("Failed to list shards for pruning: %v", err),
		)
	}

	rc.observedShards = observed
	return continueStep(), nil
}

// stepApplyDesiredShards applies each desired Shard that is not already being
// cleaned up. If the same name still carries PendingDeletion or is terminating,
// the old child must leave before a replacement is created.
func stepApplyDesiredShards(ctx context.Context, rc *reconcileContext) (stepResult, error) {
	rc.activeShardNames = make(map[string]bool, len(rc.tg.Spec.Shards))
	rc.appliedGeneration = make(map[string]int64, len(rc.tg.Spec.Shards))
	for i := range rc.tg.Spec.Shards {
		shardSpec := rc.tg.Spec.Shards[i]
		desired, err := BuildShard(rc.tg, &shardSpec, rc.r.Scheme)
		if err != nil {
			return stepResult{}, newStepError(
				fmt.Errorf("failed to build shard: %w", err),
				"Warning",
				"FailedApply",
				fmt.Sprintf("Failed to build shard %s: %v", shardSpec.Name, err),
			)
		}

		if observedCleanupInProgress(rc.observedShards, desired.Name) {
			rc.pendingDeletion = true
			continue
		}

		// Track the name BuildShard produced, not a manually computed one.
		rc.activeShardNames[desired.Name] = true

		desired.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("Shard"))
		if err := rc.r.Patch(
			ctx,
			desired,
			client.Apply,
			client.ForceOwnership,
			client.FieldOwner("multigres-operator"),
		); err != nil {
			return stepResult{}, newStepError(
				fmt.Errorf("failed to apply shard: %w", err),
				"Warning",
				"FailedApply",
				fmt.Sprintf("Failed to apply shard %s: %v", desired.Name, err),
			)
		}

		// Record the generation the apply settled on. If this apply changed the
		// child's spec the server bumps it past the child's observed generation,
		// which stepComputeStatus uses to avoid reporting a just-changed child as
		// ready before its own controller has reconciled.
		rc.appliedGeneration[desired.Name] = desired.Generation

		rc.r.Recorder.Eventf(rc.tg, "Normal", "Applied", "Applied Shard %s", desired.Name)
	}

	return continueStep(), nil
}

func observedCleanupInProgress(shards *multigresv1alpha1.ShardList, name string) bool {
	if shards == nil {
		return false
	}
	for i := range shards.Items {
		s := &shards.Items[i]
		if s.Name == name {
			return s.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" ||
				s.DeletionTimestamp != nil
		}
	}
	return false
}

// stepReconcileUndesired advances cleanup for observed children that were not
// applied this reconcile. Children drain through PendingDeletion and
// ReadyForDeletion before deletion; same-name replacements wait here too.
func stepReconcileUndesired(ctx context.Context, rc *reconcileContext) (stepResult, error) {
	l := log.FromContext(ctx)

	for i := range rc.observedShards.Items {
		s := &rc.observedShards.Items[i]
		if rc.activeShardNames[s.Name] {
			continue
		}

		if s.DeletionTimestamp != nil {
			l.V(1).Info("Shard already terminating, waiting for deletion", "shard", s.Name)
			rc.pendingDeletion = true
			continue
		}

		// The annotation is the handoff to the Shard controller: keep the child
		// object around, but ask it to drain before the parent deletes it.
		if s.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			if err := rc.r.setShardPendingDeletion(ctx, s); err != nil {
				return stepResult{}, newStepError(
					fmt.Errorf("failed to set PendingDeletion on shard '%s': %w", s.Name, err),
					"Warning",
					"CleanUpError",
					fmt.Sprintf("Failed to set PendingDeletion on shard %s: %v", s.Name, err),
				)
			}
			rc.r.Recorder.Eventf(rc.tg, "Normal", "PendingDeletion",
				"Marked Shard %s for graceful deletion", s.Name)
			rc.pendingDeletion = true
			continue
		}

		// Until the child reports ReadyForDeletion, parent status must remain
		// Progressing and the orphan must stay in place.
		if !meta.IsStatusConditionTrue(
			s.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion,
		) {
			l.V(1).Info("Shard pending deletion, waiting for drain", "shard", s.Name)
			rc.pendingDeletion = true
			continue
		}

		// The Shard has drained, so it is safe to delete.
		if err := rc.r.Delete(ctx, s); err != nil && !apierrors.IsNotFound(err) {
			return stepResult{}, newStepError(
				fmt.Errorf("failed to delete shard '%s' after cleanup: %w", s.Name, err),
				"Warning",
				"CleanUpError",
				fmt.Sprintf("Failed to delete shard %s after cleanup: %v", s.Name, err),
			)
		} else if err == nil {
			rc.r.Recorder.Eventf(rc.tg, "Normal", "Deleted",
				"Deleted Shard %s after cleanup", s.Name)
		}
	}

	return continueStep(), nil
}

func stepRequeueIfPending(_ context.Context, rc *reconcileContext) (stepResult, error) {
	if rc.pendingDeletion {
		return doneStep(ctrl.Result{RequeueAfter: 5 * time.Second}), nil
	}
	return continueStep(), nil
}
