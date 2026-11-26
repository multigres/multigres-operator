package shard

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

// boundPodGoneRequeue is how often a bound shard that waits for its pod to be
// gone is looked at again, when a pod event does not do it first.
const boundPodGoneRequeue = 2 * time.Second

// reconcileBound reconciles a bound shard after the prelude every shard runs.
// A bound shard fronts an external PostgreSQL with one pooler pod; see
// BuildBoundPoolerPod. Nothing else of Reconcile runs for it: no secrets or
// ConfigMaps of a PostgreSQL, multiorch, PodDisruptionBudget, headless
// Service, rollout, drain, quarantine or data-plane phase. The pooler
// registers itself in topology, and the database record already exists from
// the default tablegroup.
func (r *ShardReconciler) reconcileBound(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (ctrl.Result, error) {
	ctx, span := monitoring.StartChildSpan(ctx, "Shard.ReconcileBound")
	defer span.End()

	if shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" {
		return r.handleBoundPendingDeletion(ctx, shard)
	}

	poolName, cellName, poolSpec, err := boundPool(shard)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(shard, "Warning", "ConfigError", "%v", err)
		return ctrl.Result{}, err
	}

	if err := r.ensureBoundPod(ctx, shard, poolName, cellName, poolSpec); err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(
			shard,
			"Warning",
			"FailedApply",
			"Failed to reconcile the pooler pod: %v",
			err,
		)
		return ctrl.Result{}, err
	}
	if err := r.updateBoundStatus(ctx, shard, cellName); err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(shard, "Warning", "StatusError", "Failed to update status: %v", err)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// ensureBoundPod keeps exactly one pooler pod for the shard, and never two at
// once. Multigres does not enforce that: a second pooler for the same external
// database would register beside the first, and both would serve. So the pod
// is replaced, never surged. A pod whose spec differs, that is in a terminal
// phase, or that is not the one the shard names now is deleted, and the next
// reconcile creates the replacement only when no pod of the shard is left,
// terminating ones included. A pod stuck terminating on a lost node therefore
// holds the replacement back, which is the safe side of that failure.
//
// There is no drain or disruption check: the pod is the shard's only pooler,
// so there is no cohort to protect.
func (r *ShardReconciler) ensureBoundPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	logger := log.FromContext(ctx)

	desired, err := BuildBoundPoolerPod(shard, poolName, cellName, poolSpec, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build the pooler pod: %w", err)
	}

	// An uncached read: a stale cache must not show an empty list while a pod
	// of another name is still running.
	pods := &corev1.PodList{}
	if err := r.disruptionReader().List(ctx, pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(boundPoolerSelector(shard)),
	); err != nil {
		return fmt.Errorf("failed to list the shard's pooler pods: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if !pod.DeletionTimestamp.IsZero() {
			// Replaced once it is gone; its deletion requeues the shard.
			continue
		}
		reason := ""
		switch {
		case pod.Name != desired.Name:
			reason = "replace a pod the shard no longer names"
		case pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded:
			reason = "replace a terminal pod"
		case pod.Annotations[metadata.AnnotationSpecHash] !=
			desired.Annotations[metadata.AnnotationSpecHash]:
			reason = "apply a spec change"
		}
		if reason == "" {
			continue
		}
		logger.Info("Deleting the bound pooler pod",
			"reason", reason, "pod", pod.Name,
			"currentHash", pod.Annotations[metadata.AnnotationSpecHash],
			"desiredHash", desired.Annotations[metadata.AnnotationSpecHash])
		if err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil &&
			!apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete pod %s: %w", pod.Name, err)
		}
		r.Recorder.Eventf(shard, "Normal", "PodReplaced", "Deleted pod %s to %s", pod.Name, reason)
	}

	if len(pods.Items) > 0 {
		return nil
	}
	if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create pod %s: %w", desired.Name, err)
	}
	logger.Info("Created the bound pooler pod", "pod", desired.Name)
	r.Recorder.Eventf(shard, "Normal", "PodCreated", "Created pod %s", desired.Name)
	return nil
}

// handleBoundPendingDeletion deletes the pod of a shard that left the spec and
// reports it ready for deletion once the pod is gone. There is nothing to
// drain: the pooler holds no data, and it sets itself not serving when it gets
// SIGTERM.
func (r *ShardReconciler) handleBoundPendingDeletion(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pods := &corev1.PodList{}
	if err := r.disruptionReader().List(ctx, pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(boundPoolerSelector(shard)),
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list pods for pending deletion: %w", err)
	}

	waiting := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("failed to delete pod %s: %w", pod.Name, err)
			}
			waiting = true
			continue
		}
		// A pod stuck terminating (a dead kubelet) must not hold the shard,
		// and the database record it belongs to, forever.
		if time.Since(pod.DeletionTimestamp.Time) < terminatingTimeout {
			waiting = true
			continue
		}
		logger.Info("Pod stuck terminating, treating as gone",
			"pod", pod.Name, "terminatingSince", pod.DeletionTimestamp.Time)
		r.Recorder.Eventf(shard, "Warning", "StuckTerminating",
			"Pod %s has been terminating for >%s, treating as gone", pod.Name, terminatingTimeout)
	}
	if waiting {
		return ctrl.Result{RequeueAfter: boundPodGoneRequeue}, nil
	}
	return r.markReadyForDeletion(ctx, shard)
}

// updateBoundStatus reports the pod as a pool of one, so PoolsReady,
// ReadyReplicas and Phase roll up through the TableGroup as they do for any
// other shard. Ready means the pooler process is up and accepting calls, which
// is all the pod's readiness says: whether the external database is reachable
// is the pooler's own report, in topology, and is what CREATE TABLEGROUP
// waits on.
func (r *ShardReconciler) updateBoundStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellName string,
) error {
	oldPhase := shard.Status.Phase

	var pod *corev1.Pod
	existing := &corev1.Pod{}
	name := BuildBoundPoolerPodName(shard, cellName)
	if err := r.Get(
		ctx,
		client.ObjectKey{Namespace: shard.Namespace, Name: name},
		existing,
	); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get pod %s for status: %w", name, err)
		}
	} else if existing.DeletionTimestamp.IsZero() &&
		existing.Status.Phase != corev1.PodFailed &&
		existing.Status.Phase != corev1.PodSucceeded {
		pod = existing
	}

	var readyPods int32
	if isPodReady(pod) {
		readyPods = 1
	}
	shard.Status.Cells = []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cellName)}
	shard.Status.PoolsReady = readyPods == 1
	shard.Status.ReadyReplicas = readyPods
	shard.Status.OrchReady = false
	r.setConditions(shard, 1, readyPods)

	switch {
	case pod != nil && status.IsCrashLooping(pod):
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		shard.Status.Message = "The pooler pod is crash-looping"
	case shard.Status.PoolsReady:
		shard.Status.Phase = multigresv1alpha1.PhaseHealthy
		shard.Status.Message = "Ready"
	default:
		shard.Status.Phase = multigresv1alpha1.PhaseProgressing
		shard.Status.Message = "Waiting for the pooler pod to become ready"
	}
	shard.Status.ObservedGeneration = shard.Generation
	return r.patchStatus(ctx, shard, oldPhase)
}
