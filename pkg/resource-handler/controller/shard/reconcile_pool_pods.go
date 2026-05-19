package shard

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

// reconcilePoolPods ensures all pods and PVCs for a pool in a specific cell
// match the desired state. It handles creation of missing resources, scale-down,
// and rolling updates.
func (r *ShardReconciler) reconcilePoolPods(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("reconcilePoolPods started", "pool", poolName, "cell", cellName)

	replicas := DefaultPoolReplicas
	if poolSpec.ReplicasPerCell != nil {
		replicas = *poolSpec.ReplicasPerCell
	}

	// List existing pods and PVCs for this pool
	labels := buildPoolLabelsWithCell(shard, poolName, cellName)
	selector := metadata.GetSelectorLabels(labels)

	podList := &corev1.PodList{}
	if err := r.List(
		ctx,
		podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return fmt.Errorf("failed to list pods for pool %s cell %s: %w", poolName, cellName, err)
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(
		ctx,
		pvcList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return fmt.Errorf("failed to list PVCs for pool %s cell %s: %w", poolName, cellName, err)
	}

	existingPods := make(map[string]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		existingPods[pod.Name] = pod
	}

	existingPVCs := make(map[string]*corev1.PersistentVolumeClaim, len(pvcList.Items))
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		existingPVCs[pvc.Name] = pvc
	}

	// Phase 0: Sync DRAINED labels and compute effective replicas.
	// DRAINED pods stay alive for investigation; stand-in replicas compensate.
	drainedCount := countDrainedPods(shard, existingPods)
	effectiveReplicas := replicas + drainedCount

	if err := r.syncDrainedLabels(ctx, shard, existingPods); err != nil {
		return err
	}

	// Phase 1: Create missing resources and handle terminal/deleted pods
	driftedCount, actionTaken, err := r.createMissingResources(
		ctx, shard, poolName, cellName, poolSpec, existingPods, existingPVCs, effectiveReplicas,
	)
	if err != nil {
		return err
	}

	// Phase 2: Handle scale-down (extra pod draining, ready-for-deletion cleanup)
	actionTaken, inProgress, err := r.handleScaleDown(
		ctx, shard, poolName, poolSpec, existingPods, replicas, effectiveReplicas, actionTaken,
	)
	if err != nil {
		return err
	}

	// Phase 3: Handle rolling updates
	if err := r.handleRollingUpdates(
		ctx,
		shard,
		poolName,
		cellName,
		poolSpec,
		existingPods,
		driftedCount,
		actionTaken,
		inProgress,
	); err != nil {
		return err
	}

	// Record drift metric
	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	monitoring.SetPoolPodsDrifted(
		clusterName,
		shard.Name,
		poolName,
		cellName,
		shard.Namespace,
		driftedCount,
	)

	return nil
}

// createMissingResources creates PVCs and Pods that should exist but don't.
// It also handles terminal pods (Failed/Succeeded) and externally-deleted pods.
// effectiveReplicas includes stand-in pods for DRAINED pods (replicas + drainedCount).
// Returns the number of drifted pods and whether an action was taken this reconcile.
func (r *ShardReconciler) createMissingResources(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	existingPods map[string]*corev1.Pod,
	existingPVCs map[string]*corev1.PersistentVolumeClaim,
	effectiveReplicas int32,
) (driftedCount int, actionTaken bool, err error) {
	logger := log.FromContext(ctx)

	for i := int32(0); i < effectiveReplicas; i++ {
		podName := BuildPoolPodName(shard, poolName, cellName, int(i))
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, int(i))

		// Create PVC if missing, or expand if storage.size increased
		if _, exists := existingPVCs[pvcName]; !exists {
			desiredPVC, buildErr := BuildPoolDataPVC(
				shard,
				poolName,
				cellName,
				poolSpec,
				int(i),
				ShouldDeletePVCOnShardRemoval(shard, poolSpec),
				r.Scheme,
			)
			if buildErr != nil {
				return 0, false, fmt.Errorf("failed to build PVC %s: %w", pvcName, buildErr)
			}
			if createErr := r.Create(
				ctx,
				desiredPVC,
			); createErr != nil &&
				!errors.IsAlreadyExists(createErr) {
				return 0, false, fmt.Errorf("failed to create PVC %s: %w", pvcName, createErr)
			}
			logger.Info("Created missing pool PVC", "pvc", pvcName)
		} else {
			// This deterministically-named PVC is desired again. If a prior
			// scale-down / drain marked it orphan, clear the label so the
			// multigres-gc cronjob does not delete storage we are putting back
			// into service.
			if err := pvcutil.ClearOrphan(
				ctx,
				logger,
				r.Client,
				existingPVCs[pvcName],
			); err != nil {
				return 0, false, fmt.Errorf(
					"failed to clear orphan label on PVC %s: %w",
					pvcName,
					err,
				)
			}
			if err := r.expandPVCIfNeeded(ctx, shard, existingPVCs[pvcName], poolSpec); err != nil {
				return 0, false, err
			}
		}

		// Create Pod if missing
		pod, exists := existingPods[podName]
		if !exists {
			desiredPod, buildErr := BuildPoolPod(
				shard,
				poolName,
				cellName,
				poolSpec,
				int(i),
				r.Scheme,
			)
			if buildErr != nil {
				return 0, false, fmt.Errorf("failed to build pod %s: %w", podName, buildErr)
			}
			if createErr := r.Create(
				ctx,
				desiredPod,
			); createErr != nil &&
				!errors.IsAlreadyExists(createErr) {
				return 0, false, fmt.Errorf("failed to create pod %s: %w", podName, createErr)
			}
			logger.Info("Created missing pool pod", "pod", podName)
			r.Recorder.Eventf(
				shard,
				"Normal",
				"PodCreated",
				"Created pod %s for pool %s",
				podName,
				poolName,
			)
			actionTaken = true
			continue
		}

		// Handle terminal pods (Failed/Succeeded) — delete for recreation
		if (pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded) &&
			pod.DeletionTimestamp.IsZero() {
			if !actionTaken {
				logger.Info(
					"Deleting terminal pool pod for recreation",
					"pod",
					pod.Name,
					"phase",
					pod.Status.Phase,
				)
				if delErr := r.Delete(ctx, pod); delErr != nil && !errors.IsNotFound(delErr) {
					return 0, false, fmt.Errorf(
						"failed to delete terminal pod %s: %w",
						pod.Name,
						delErr,
					)
				}
				actionTaken = true
			}
			continue
		}

		// Handle pods being deleted externally
		if !pod.DeletionTimestamp.IsZero() {
			if !actionTaken {
				if handleErr := r.handleExternalDeletion(ctx, shard, pod); handleErr != nil {
					return 0, false, handleErr
				}
				actionTaken = true
			}
			continue
		}

		// Check if it's drifted
		isDrifted := podNeedsUpdate(pod, shard, poolName, cellName, poolSpec, int(i), r.Scheme)
		if isDrifted {
			driftedCount++
		}

		// Check if the data PVC needs a pod restart for filesystem expansion.
		// Some CSI drivers require the pod to be restarted after the block device
		// has been expanded so the filesystem can grow.
		if !actionTaken && pvcNeedsFilesystemResize(existingPVCs, pvcName) {
			if pod.Annotations[metadata.AnnotationDrainState] == "" {
				if err := r.initiateDrain(ctx, pod); err != nil {
					return 0, false, fmt.Errorf(
						"failed to drain pod %s for filesystem resize: %w",
						pod.Name, err,
					)
				}
				logger.Info(
					"Draining pod for PVC filesystem expansion",
					"pod",
					pod.Name,
					"pvc",
					pvcName,
				)
				r.Recorder.Eventf(
					shard, "Normal", "FilesystemResize",
					"Draining pod %s to expand filesystem on PVC %s", pod.Name, pvcName,
				)
				actionTaken = true
			}
		}

		// Drift detection above already counted this pod. No creation blocking
		// needed here — unready pods do not prevent creation of other replicas.
		// Phases 2 (scale-down) and 3 (rolling updates) are still gated by
		// actionTaken set during any creation or deletion above.
	}

	return driftedCount, actionTaken, nil
}

// isPodReady returns true if a pod is fully ready
func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// isPoolHealthy returns true if all non-draining, non-terminating, non-DRAINED
// pods that will remain after scale-down are Ready. Extra pods (index >=
// effectiveReplicas) are excluded so an unhealthy extra pod does not block its
// own removal. DRAINED pods are excluded because they are expected to be
// unhealthy and should not block scale-down of stand-in pods.
func isPoolHealthy(
	existingPods map[string]*corev1.Pod,
	effectiveReplicas int32,
	shard *multigresv1alpha1.Shard,
) bool {
	for _, pod := range existingPods {
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		if idx, ok := resolvePodIndex(pod.Name); !ok || idx >= int(effectiveReplicas) {
			continue
		}
		if resolvePodRole(shard, pod.Name) == "DRAINED" {
			continue
		}
		if !isPodReady(pod) {
			return false
		}
	}
	return true
}

// handleExternalDeletion handles a pod that has been deleted externally (e.g. kubectl delete).
// Unscheduled pods are allowed to terminate; scheduled pods enter the drain state machine.
func (r *ShardReconciler) handleExternalDeletion(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
) error {
	logger := log.FromContext(ctx)

	isScheduled := false
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionTrue {
			isScheduled = true
			break
		}
	}

	if !isScheduled {
		logger.Info("Ignoring externally-deleted unscheduled pod", "pod", pod.Name)
		return nil
	}

	if pod.Annotations[metadata.AnnotationDrainState] == "" {
		logger.Info("Initiating drain for externally-deleted pod", "pod", pod.Name)
		if err := r.initiateDrain(ctx, pod); err != nil {
			return err
		}
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ExternalDeletion",
			"Initiating drain for externally-deleted pod %s",
			pod.Name,
		)
	}

	return nil
}

// handleScaleDown processes pods that need removal: ready-for-deletion cleanup
// and draining extra pods beyond the effective replica count.
// replicas is the user-desired count; effectiveReplicas = replicas + drainedCount.
// Returns whether an action was taken and whether any drain is in progress.
func (r *ShardReconciler) handleScaleDown(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	poolSpec multigresv1alpha1.PoolSpec,
	existingPods map[string]*corev1.Pod,
	replicas int32,
	effectiveReplicas int32,
	actionTaken bool,
) (bool, bool, error) {
	logger := log.FromContext(ctx)

	var extraPods []*corev1.Pod
	var readyForDeletion []*corev1.Pod
	inProgress := false

	// Sort pods by name for deterministic processing
	podNames := make([]string, 0, len(existingPods))
	for k := range existingPods {
		podNames = append(podNames, k)
	}
	slices.Sort(podNames)

	for _, podName := range podNames {
		pod := existingPods[podName]
		drainState := pod.Annotations[metadata.AnnotationDrainState]

		if drainState == metadata.DrainStateReadyForDeletion {
			readyForDeletion = append(readyForDeletion, pod)
			continue
		}

		if drainState != "" || !pod.DeletionTimestamp.IsZero() {
			inProgress = true
		}

		index, ok := resolvePodIndex(pod.Name)
		if !ok || index >= int(effectiveReplicas) {
			extraPods = append(extraPods, pod)
		}
	}

	// Process external deletions for extra pods to avoid deadlocks
	for _, pod := range extraPods {
		if !pod.DeletionTimestamp.IsZero() &&
			pod.Annotations[metadata.AnnotationDrainState] == "" {
			if !actionTaken {
				if err := r.handleExternalDeletion(ctx, shard, pod); err != nil {
					return actionTaken, inProgress, err
				}
				actionTaken = true
			}
		}
	}

	// Cleanup pods ready for deletion
	for _, pod := range readyForDeletion {
		logger.Info("Cleaning up pod in ready-for-deletion state", "pod", pod.Name)
		if err := r.cleanupDrainedPod(ctx, shard, pod, poolName, poolSpec, replicas); err != nil {
			return actionTaken, inProgress, fmt.Errorf(
				"failed to cleanup drained pod %s: %w",
				pod.Name,
				err,
			)
		}
		if err := r.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
			return actionTaken, inProgress, fmt.Errorf(
				"failed to delete ready-for-deletion pod %s: %w",
				pod.Name,
				err,
			)
		}
		actionTaken = true
	}

	// Drain extra pods (scale-down, skip if another drain is already in progress).
	// Health gate: refuse to start a new drain if the pool is already degraded.
	// This prevents cascading failures where removing pods from an unhealthy pool
	// could cause an outage.
	logger.V(1).
		Info("Scale-down check", "extraPods", len(extraPods), "actionTaken", actionTaken, "inProgress", inProgress, "desiredReplicas", replicas, "effectiveReplicas", effectiveReplicas)
	if !actionTaken && !inProgress && len(extraPods) > 0 {
		if !isPoolHealthy(existingPods, effectiveReplicas, shard) {
			logger.Info(
				"Deferring scale-down: pool has non-ready pods",
				"extraPods",
				len(extraPods),
			)
			r.Recorder.Eventf(
				shard,
				"Warning",
				"ScaleDownBlocked",
				"Deferring scale-down of %d extra pod(s): pool has non-ready pods",
				len(extraPods),
			)
			return actionTaken, inProgress, nil
		}
		podToDrain := r.selectPodToDrain(ctx, extraPods, shard)
		if podToDrain != nil && podToDrain.Annotations[metadata.AnnotationDrainState] == "" {
			if err := r.initiateDrain(ctx, podToDrain); err != nil {
				return actionTaken, inProgress, fmt.Errorf(
					"failed to initiate drain for extra pod %s: %w",
					podToDrain.Name,
					err,
				)
			}
			logger.Info("Requested drain for extra pod", "pod", podToDrain.Name)
			r.Recorder.Eventf(
				shard,
				"Normal",
				"DrainStarted",
				"Initiated drain for extra pod %s",
				podToDrain.Name,
			)
			actionTaken = true
		}
	}

	return actionTaken, inProgress, nil
}

// handleRollingUpdates drains drifted pods one at a time (replicas first, primary last).
func (r *ShardReconciler) handleRollingUpdates(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	existingPods map[string]*corev1.Pod,
	driftedCount int,
	actionTaken, isAnyPodDraining bool,
) error {
	logger := log.FromContext(ctx)

	inProgress := isAnyPodDraining || driftedCount > 0
	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	monitoring.SetRollingUpdateInProgress(
		clusterName,
		shard.Name,
		poolName,
		cellName,
		shard.Namespace,
		inProgress,
	)

	if inProgress {
		msg := fmt.Sprintf("%d pods need update in pool %s", driftedCount, poolName)
		meta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
			Type:    "RollingUpdate",
			Status:  metav1.ConditionTrue,
			Reason:  "PodsDrifted",
			Message: msg,
		})
	} else {
		msg := fmt.Sprintf("All pods up to date in pool %s", poolName)
		meta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
			Type:    "RollingUpdate",
			Status:  metav1.ConditionFalse,
			Reason:  "PodsUpToDate",
			Message: msg,
		})
	}

	if actionTaken || isAnyPodDraining || driftedCount == 0 {
		return nil
	}

	// Sort pods for deterministic ordering
	podNames := make([]string, 0, len(existingPods))
	for k := range existingPods {
		podNames = append(podNames, k)
	}
	slices.Sort(podNames)

	var waitPrimary *corev1.Pod
	for _, name := range podNames {
		pod := existingPods[name]
		podIdx, _ := resolvePodIndex(pod.Name)
		if !podNeedsUpdate(
			pod,
			shard,
			poolName,
			cellName,
			poolSpec,
			podIdx,
			r.Scheme,
		) {
			continue
		}

		isPrimary := resolvePodRole(shard, pod.Name) == "PRIMARY"
		if !isPrimary {
			if err := r.initiateDrain(ctx, pod); err != nil {
				return fmt.Errorf("failed to initiate drain for drifted pod %s: %w", pod.Name, err)
			}
			logger.Info(
				"Initiated drain for drifted replica pod during rolling update",
				"pod",
				pod.Name,
			)
			r.Recorder.Eventf(
				shard,
				"Normal",
				"PodUpdated",
				"Initiated drain for drifted replica pod %s",
				pod.Name,
			)
			return nil // Only one per reconcile loop
		}
		waitPrimary = pod
	}

	// If the only pod that needs updating is the PRIMARY, initiate a switchover.
	if waitPrimary != nil && waitPrimary.Annotations[metadata.AnnotationDrainState] == "" {
		if err := r.initiateDrain(ctx, waitPrimary); err != nil {
			return fmt.Errorf(
				"failed to request drain for primary pod %s: %w",
				waitPrimary.Name,
				err,
			)
		}
		logger.Info("Requested switchover for primary pod rolling update", "pod", waitPrimary.Name)
		r.Recorder.Eventf(
			shard,
			"Normal",
			"RollingUpdateStarted",
			"Initiating primary switchover for rolling update of pod %s",
			waitPrimary.Name,
		)
	}

	return nil
}

// selectPodToDrain chooses the best pod to drain during scale-down.
// Preference: non-ready, non-primary, highest index.
func (r *ShardReconciler) selectPodToDrain(
	ctx context.Context,
	extraPods []*corev1.Pod,
	shard *multigresv1alpha1.Shard,
) *corev1.Pod {
	logger := log.FromContext(ctx)
	if len(extraPods) == 0 {
		return nil
	}

	var bestPod *corev1.Pod
	var bestScore int

	for _, pod := range extraPods {
		if pod == nil {
			continue
		}
		score, _ := resolvePodIndex(pod.Name) // higher index gets higher score

		role := resolvePodRole(shard, pod.Name)
		if role == "PRIMARY" {
			score -= 1000
		}

		// Prefer non-ready pods
		isReady := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				isReady = true
				break
			}
		}
		if !isReady {
			score += 500
		}

		logger.V(1).
			Info("Pod score for drain", "pod", pod.Name, "score", score, "isReady", isReady, "role", role)
		if bestPod == nil || score > bestScore {
			bestPod = pod
			bestScore = score
		}
	}

	if bestPod != nil {
		logger.Info("Selected pod to drain", "pod", bestPod.Name, "score", bestScore)
	} else {
		logger.Info("No pod selected to drain despite extraPods being non-empty")
	}

	return bestPod
}

// syncDrainedLabels ensures pods with topology role DRAINED have the
// multigres.com/role=DRAINED label, and pods no longer DRAINED have it removed.
// The label is the durable signal for DRAINED PVC cleanup — PodRoles may be
// cleared by the data-handler during drain before cleanup runs.
func (r *ShardReconciler) syncDrainedLabels(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	existingPods map[string]*corev1.Pod,
) error {
	for _, pod := range existingPods {
		role := resolvePodRole(shard, pod.Name)
		currentLabel := pod.Labels[metadata.LabelPodRole]

		if role == "DRAINED" && currentLabel != "DRAINED" {
			patch := client.MergeFrom(pod.DeepCopy())
			if pod.Labels == nil {
				pod.Labels = make(map[string]string)
			}
			pod.Labels[metadata.LabelPodRole] = "DRAINED"
			if err := r.Patch(ctx, pod, patch); err != nil {
				return fmt.Errorf("failed to set DRAINED label on pod %s: %w", pod.Name, err)
			}
			r.Recorder.Eventf(shard, "Warning", "PodDrained",
				"Pod %s detected as DRAINED — provisioning stand-in replica", pod.Name)
		} else if role != "DRAINED" && currentLabel == "DRAINED" {
			patch := client.MergeFrom(pod.DeepCopy())
			delete(pod.Labels, metadata.LabelPodRole)
			if err := r.Patch(ctx, pod, patch); err != nil {
				return fmt.Errorf("failed to remove DRAINED label from pod %s: %w", pod.Name, err)
			}
			r.Recorder.Eventf(shard, "Normal", "PodRecovered",
				"Pod %s is no longer DRAINED", pod.Name)
		}
	}
	return nil
}

func (r *ShardReconciler) cleanupDrainedPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	poolName string,
	poolSpec multigresv1alpha1.PoolSpec,
	replicas int32,
) error {
	logger := log.FromContext(ctx)

	// DRAINED pods always get their PVC marked orphan — data is known-bad.
	// The multigres-gc CronJob deletes the PVC after the retention window.
	// We check the pod label (not PodRoles) because the data-handler clears
	// the topology entry during drain before this cleanup point.
	if pod.Labels[metadata.LabelPodRole] == "DRAINED" {
		if err := r.cleanupPodPVC(
			ctx,
			shard,
			pod,
			poolName,
			"DRAINED (data known-bad)",
		); err != nil {
			return err
		}
		logger.Info("Drained pod cleanup complete", "pod", pod.Name)
		r.Recorder.Eventf(shard, "Normal", "DrainCompleted",
			"Completed drain for DRAINED pod %s — PVC cleanup queued", pod.Name)
		return nil
	}

	// For non-DRAINED pods, respect WhenScaled policy
	mergedPolicy := multigresv1alpha1.MergePVCDeletionPolicy(
		poolSpec.PVCDeletionPolicy,
		shard.Spec.PVCDeletionPolicy,
	)
	policy := multigresv1alpha1.PVCDeletionPolicy{
		WhenScaled: multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	if mergedPolicy != nil && mergedPolicy.WhenScaled != "" {
		policy = *mergedPolicy
	}

	if policy.WhenScaled == multigresv1alpha1.DeletePVCRetentionPolicy {
		idx, idxOK := resolvePodIndex(pod.Name)
		if !idxOK {
			logger.Info("Skipping PVC deletion for pod with unparseable index", "pod", pod.Name)
		} else if idx >= int(replicas) {
			if err := r.cleanupPodPVC(ctx, shard, pod, poolName, "scaled down"); err != nil {
				return err
			}
		} else {
			logger.Info(
				"Retaining PVC for pod during rolling update",
				"pod",
				pod.Name,
				"index",
				idx,
				"replicas",
				replicas,
			)
		}
	}

	// Record event for completed drain.
	logger.Info("Drained pod cleanup complete", "pod", pod.Name)
	r.Recorder.Eventf(shard, "Normal", "DrainCompleted", "Completed drain for pod %s", pod.Name)

	return nil
}

// cleanupPodPVC removes a pod's data PVC from the operator's care. The choice
// between orphaning (deferred deletion via multigres-gc) and in-line deletion
// is based on how many sibling PVCs remain in the same pool+cell: if removing
// this one still leaves >= pvcOrphanReplicasThreshold volumes, it is excess and
// is deleted, otherwise it is orphaned so the data can be recovered. See
// orphanByRemainingCount.
func (r *ShardReconciler) cleanupPodPVC(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	poolName, reason string,
) error {
	logger := log.FromContext(ctx)

	idx, ok := resolvePodIndex(pod.Name)
	if !ok {
		logger.Info("Skipping PVC cleanup for pod with unparseable index", "pod", pod.Name)
		return nil
	}

	cellName := pod.Labels[metadata.LabelMultigresCell]
	pvcName := BuildPoolDataPVCName(shard, poolName, cellName, idx)
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(
		ctx,
		client.ObjectKey{Namespace: pod.Namespace, Name: pvcName},
		pvc,
	); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		logger.Error(err, "Failed to fetch PVC for cleanup", "pvc", pvcName)
		return fmt.Errorf("failed to fetch PVC %s for cleanup: %w", pvcName, err)
	}

	liveCount, err := r.countPoolCellPVCs(ctx, shard, poolName, cellName)
	if err != nil {
		return err
	}

	if orphanByRemainingCount(liveCount) {
		if err := pvcutil.MarkOrphan(ctx, r.Client, pvc, shard.GetUID(), time.Now()); err != nil {
			logger.Error(err, "Failed to mark PVC orphan for "+reason+" pod", "pvc", pvcName)
			return fmt.Errorf("failed to mark PVC %s orphan: %w", pvcName, err)
		}
		logger.Info("Marked PVC orphan for "+reason+" pod", "pvc", pvcName, "liveCount", liveCount)
		return nil
	}

	if err := r.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
		logger.Error(err, "Failed to delete PVC for "+reason+" pod", "pvc", pvcName)
		return fmt.Errorf("failed to delete PVC %s: %w", pvcName, err)
	}
	logger.Info("Deleted PVC for "+reason+" pod", "pvc", pvcName, "liveCount", liveCount)
	return nil
}

// countPoolCellPVCs returns the number of PVCs currently present for the given
// pool+cell, used to decide orphan-vs-delete. Already-orphaned PVCs are
// excluded: they are no longer part of the live serving set, so they must not
// inflate the count and cause a still-needed volume to be hard-deleted.
func (r *ShardReconciler) countPoolCellPVCs(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
) (int, error) {
	labels := buildPoolLabelsWithCell(shard, poolName, cellName)
	selector := metadata.GetSelectorLabels(labels)

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(
		ctx,
		pvcList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return 0, fmt.Errorf(
			"failed to list PVCs for pool %s cell %s: %w",
			poolName,
			cellName,
			err,
		)
	}

	count := 0
	for i := range pvcList.Items {
		if pvcutil.HasOrphanLabel(&pvcList.Items[i]) {
			continue
		}
		count++
	}
	return count, nil
}

// podNeedsUpdate checks if a pod requires recreation due to spec changes.
// Since most pod fields are immutable, we rely on the pre-computed spec-hash annotation.
func podNeedsUpdate(
	existing *corev1.Pod,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	index int,
	scheme *runtime.Scheme,
) bool {
	// If it has a deletion timestamp, let it die
	if !existing.DeletionTimestamp.IsZero() {
		return false
	}

	// If missing the annotation entirely, it needs an update
	existingHash, ok := existing.Annotations[metadata.AnnotationSpecHash]
	if !ok {
		return true
	}

	// Compute desired hash by building the ideal pod spec
	// NOTE: BuildPoolPod doesn't make API calls, it's safe to call frequently.
	desired, err := BuildPoolPod(shard, poolName, cellName, poolSpec, index, scheme)
	if err != nil {
		return false // Assume no update needed if we can't build it
	}

	desiredHash := ComputeSpecHash(desired)
	return existingHash != desiredHash
}

// expandPVCIfNeeded patches an existing PVC's storage request when the desired
// size (from poolSpec.Storage.Size) exceeds the current PVC spec. This enables
// in-place volume expansion without pod deletion.
func (r *ShardReconciler) expandPVCIfNeeded(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pvc *corev1.PersistentVolumeClaim,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	logger := log.FromContext(ctx)

	storageSize := DefaultDataVolumeSize
	if poolSpec.Storage.Size != "" {
		storageSize = poolSpec.Storage.Size
	}

	desired, err := resource.ParseQuantity(storageSize)
	if err != nil {
		return fmt.Errorf("invalid storage size %q: %w", storageSize, err)
	}

	var current resource.Quantity
	if pvc.Spec.Resources.Requests != nil {
		current = pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	}
	if desired.Cmp(current) <= 0 {
		return nil
	}

	patch := client.MergeFrom(pvc.DeepCopy())
	if pvc.Spec.Resources.Requests == nil {
		pvc.Spec.Resources.Requests = corev1.ResourceList{}
	}
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desired
	if err := r.Patch(ctx, pvc, patch); err != nil {
		r.Recorder.Eventf(shard, "Warning", "ExpandPVCFailed",
			"Failed to expand PVC %s from %s to %s: %v",
			pvc.Name, current.String(), desired.String(), err)
		return fmt.Errorf("failed to expand PVC %s from %s to %s: %w",
			pvc.Name, current.String(), desired.String(), err)
	}

	logger.Info("Expanded PVC storage",
		"pvc", pvc.Name, "from", current.String(), "to", desired.String())
	r.Recorder.Eventf(shard, "Normal", "PVCExpanded",
		"Expanded PVC %s from %s to %s", pvc.Name, current.String(), desired.String())

	return nil
}

// pvcNeedsFilesystemResize returns true if the named PVC has the
// FileSystemResizePending condition, meaning the block device has been expanded
// but the filesystem needs a pod restart to grow.
func pvcNeedsFilesystemResize(pvcs map[string]*corev1.PersistentVolumeClaim, pvcName string) bool {
	pvc, ok := pvcs[pvcName]
	if !ok {
		return false
	}
	for _, cond := range pvc.Status.Conditions {
		if cond.Type == corev1.PersistentVolumeClaimFileSystemResizePending &&
			cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// resolvePodIndex parses the index from the pod name (the number after the last dash).
func resolvePodIndex(podName string) (int, bool) {
	lastDash := strings.LastIndex(podName, "-")
	if lastDash == -1 {
		return 0, false
	}
	index, err := strconv.Atoi(podName[lastDash+1:])
	if err != nil {
		return 0, false
	}
	return index, true
}
