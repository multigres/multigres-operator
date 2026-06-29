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

// stepApplyDesiredShards applies each desired Shard and records the names and
// generations produced by the apiserver. Later status aggregation uses those
// generations to avoid treating a just-updated child as ready before the child
// controller observes its new spec.
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

// stepReconcileUndesired retires observed children that are no longer desired.
// Orphans are not deleted on sight: the parent first marks PendingDeletion, waits
// for the Shard controller to report ReadyForDeletion, and only then deletes.
// The desired objects built above are never written back into rc.observedShards,
// so status continues to use the children's observed status.
func stepReconcileUndesired(ctx context.Context, rc *reconcileContext) (stepResult, error) {
	l := log.FromContext(ctx)

	for i := range rc.observedShards.Items {
		s := &rc.observedShards.Items[i]
		if rc.activeShardNames[s.Name] {
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
				fmt.Errorf("failed to delete orphan shard '%s': %w", s.Name, err),
				"Warning",
				"CleanUpError",
				fmt.Sprintf("Failed to delete orphan shard %s: %v", s.Name, err),
			)
		} else if err == nil {
			rc.r.Recorder.Eventf(rc.tg, "Normal", "Deleted", "Deleted orphaned Shard %s", s.Name)
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
