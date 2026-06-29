package tablegroup

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// stepComputeStatus derives TableGroup status from desired children whose
// observed status has caught up to the generation applied in this reconcile.
// Pending cleanup forces Progressing so stale child status cannot report ready.
func stepComputeStatus(_ context.Context, rc *reconcileContext) (stepResult, error) {
	observed := rc.observedShards
	if observed == nil {
		observed = &multigresv1alpha1.ShardList{}
	}

	total := int32(len(rc.tg.Spec.Shards)) //nolint:gosec // bounded by K8s object size limits
	ready := int32(0)

	var anyDegraded bool

	for i := range observed.Items {
		s := &observed.Items[i]

		// Orphans being pruned (or already deleted this pass) are no longer
		// desired, so they must not influence the parent's status.
		if !rc.activeShardNames[s.Name] {
			continue
		}

		// Count the child only after it has observed the generation applied above.
		if s.Status.ObservedGeneration != rc.appliedGeneration[s.Name] {
			continue
		}

		switch s.Status.Phase {
		case multigresv1alpha1.PhaseHealthy:
			ready++
		case multigresv1alpha1.PhaseDegraded:
			anyDegraded = true
		default:
			// Initializing, Progressing, and Unknown all count as progressing.
		}
	}

	rc.tg.Status.TotalShards = total
	rc.tg.Status.ReadyShards = ready

	switch {
	case anyDegraded:
		rc.tg.Status.Phase = multigresv1alpha1.PhaseDegraded
		rc.tg.Status.Message = "At least one shard is degraded"
	case rc.pendingDeletion:
		rc.tg.Status.Phase = multigresv1alpha1.PhaseProgressing
		rc.tg.Status.Message = "Waiting for removed shards to drain"
	case total == 0:
		rc.tg.Status.Phase = multigresv1alpha1.PhaseInitializing
		rc.tg.Status.Message = "No Shards"
	case ready == total:
		rc.tg.Status.Phase = multigresv1alpha1.PhaseHealthy
		rc.tg.Status.Message = "Ready"
	default:
		rc.tg.Status.Phase = multigresv1alpha1.PhaseProgressing
		rc.tg.Status.Message = fmt.Sprintf("%d/%d shards ready", ready, total)
	}

	condStatus := metav1.ConditionFalse
	condMessage := fmt.Sprintf("%d/%d shards ready", ready, total)
	condReason := "ShardsNotReady"

	switch rc.tg.Status.Phase {
	case multigresv1alpha1.PhaseHealthy:
		condStatus = metav1.ConditionTrue
		condReason = "ShardsReady"
	case multigresv1alpha1.PhaseDegraded:
		condReason = "ShardDegraded"
		condMessage = rc.tg.Status.Message
	}
	if total == 0 && !rc.pendingDeletion {
		condStatus = metav1.ConditionTrue
		condReason = "NoShards"
		condMessage = rc.tg.Status.Message
	}
	if rc.pendingDeletion && rc.tg.Status.Phase == multigresv1alpha1.PhaseProgressing {
		condReason = "CleanupPending"
		condMessage = rc.tg.Status.Message
	}

	// Let meta.SetStatusCondition manage LastTransitionTime.
	meta.SetStatusCondition(&rc.tg.Status.Conditions, metav1.Condition{
		Type:               "Available",
		Status:             condStatus,
		Reason:             condReason,
		Message:            condMessage,
		ObservedGeneration: rc.tg.Generation,
	})

	rc.tg.Status.ObservedGeneration = rc.tg.Generation

	return continueStep(), nil
}

// stepPatchStatus patches the computed status and emits lifecycle events.
func stepPatchStatus(ctx context.Context, rc *reconcileContext) (stepResult, error) {
	l := log.FromContext(ctx)

	patchObj := &multigresv1alpha1.TableGroup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "TableGroup",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      rc.tg.Name,
			Namespace: rc.tg.Namespace,
		},
		Status: rc.tg.Status,
	}

	if rc.oldPhase != rc.tg.Status.Phase {
		rc.r.Recorder.Eventf(
			rc.tg,
			"Normal",
			"PhaseChange",
			"Transitioned from '%s' to '%s'",
			rc.oldPhase,
			rc.tg.Status.Phase,
		)
	}

	// Server-Side Apply makes unchanged status patches no-ops.
	if err := rc.r.Status().Patch(
		ctx,
		patchObj,
		client.Apply,
		client.FieldOwner("multigres-operator"),
		client.ForceOwnership,
	); err != nil {
		return stepResult{}, newStepError(
			fmt.Errorf("failed to patch status: %w", err),
			"Warning",
			"StatusError",
			fmt.Sprintf("Failed to patch status: %v", err),
		)
	}

	l.V(1).Info("reconcile complete", "duration", time.Since(rc.start).String())
	if !rc.pendingDeletion {
		rc.r.Recorder.Event(rc.tg, "Normal", "Synced", "Successfully reconciled TableGroup")
	}
	return continueStep(), nil
}
