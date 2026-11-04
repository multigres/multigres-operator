package shard

import (
	"context"
	stderrors "errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
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
	rollout *shardRolloutTracker,
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

	// Phase 0: Reconcile temporary maintenance capacity.
	maintenanceSurges, surgeAction, err := r.reconcileCellMaintenanceSurge(
		ctx,
		shard,
		poolName,
		cellName,
		poolSpec,
		existingPods,
		existingPVCs,
		replicas,
		rollout,
	)
	if err != nil {
		return err
	}
	if surgeAction {
		return nil
	}

	// Active maintenance surges remain desired until the cell has settled.
	effectiveReplicas := replicas + maintenanceSurges

	// Phase 1: Create missing resources and handle terminal/deleted pods
	driftedCount, actionTaken, err := r.createMissingResources(
		ctx, shard, poolName, cellName, poolSpec, existingPods, existingPVCs, effectiveReplicas,
		rollout,
	)
	if err != nil {
		return err
	}

	// Phase 2: Handle scale-down (extra pod draining, ready-for-deletion cleanup)
	actionTaken, inProgress, err := r.handleScaleDown(
		ctx, shard, poolName, poolSpec, existingPods, replicas, effectiveReplicas, actionTaken,
		rollout,
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
		rollout,
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
// effectiveReplicas includes temporary maintenance-surge pods (replicas + maintenanceSurges).
// Returns the number of drifted pods and whether an action was taken this reconcile.
func (r *ShardReconciler) createMissingResources(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	existingPods map[string]*corev1.Pod,
	existingPVCs map[string]*corev1.PersistentVolumeClaim,
	effectiveReplicas int32,
	disruptionTrackers ...*shardRolloutTracker,
) (driftedCount int, actionTaken bool, err error) {
	logger := log.FromContext(ctx)
	disruptions := firstShardRolloutTracker(disruptionTrackers)

	for i := int32(0); i < effectiveReplicas; i++ {
		podName := BuildPoolPodName(shard, poolName, cellName, int(i))
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, int(i))
		dataTerminating := pvcutil.IsTerminating(existingPVCs[pvcName])

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
			if !dataTerminating {
				terminating, err := r.reclaimPVC(ctx, existingPVCs[pvcName])
				if err != nil {
					return 0, false, fmt.Errorf(
						"failed to clear orphan label on PVC %s: %w",
						pvcName,
						err,
					)
				}
				dataTerminating = terminating
			}
			if dataTerminating {
				// Neither reclaimed nor expanded: it is gone once no pod uses
				// it, and the branch above then creates its replacement.
				logger.Info("Pool PVC is being deleted; leaving it alone", "pvc", pvcName)
			} else if err := r.expandPVCIfNeeded(ctx, shard, existingPVCs[pvcName], poolSpec); err != nil {
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
				r.podOptions()...,
			)
			if buildErr != nil {
				return 0, false, fmt.Errorf("failed to build pod %s: %w", podName, buildErr)
			}
			// A pod started on a PVC being deleted either cannot schedule or,
			// once scheduled, holds that PVC and its doomed data in service.
			if claim, err := r.claimPodVolumes(ctx, desiredPod); err != nil {
				return 0, false, err
			} else if claim != "" {
				logger.Info("Not creating pool pod while a PVC it mounts is being deleted",
					"pod", podName, "pvc", claim)
				continue
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
			// The pod object still holds its PVCs whatever its phase.
			claim, err := podTerminatingClaim(ctx, r.disruptionReader(), pod)
			if err != nil {
				return 0, false, err
			}
			if claim != "" {
				logger.Info(
					"Not replacing terminal pool pod while a PVC it mounts is being deleted",
					"pod",
					pod.Name,
					"pvc",
					claim,
				)
				continue
			}
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
			if !actionTaken && !disruptions.HasStarted() {
				if handleErr := r.handleExternalDeletion(ctx, shard, pod); handleErr != nil {
					return 0, false, handleErr
				}
				actionTaken = true
				disruptions.SetStarted()
			}
			continue
		}

		// Check if it's drifted
		isDrifted := podNeedsUpdate(
			pod,
			shard,
			poolName,
			cellName,
			poolSpec,
			int(i),
			r.Scheme,
			r.podOptions()...)
		if isDrifted {
			driftedCount++
		}

		// Check if the data PVC needs a pod restart for filesystem expansion.
		// Some CSI drivers require the pod to be restarted after the block device
		// has been expanded so the filesystem can grow.
		if !actionTaken &&
			!disruptions.HasStarted() &&
			pvcNeedsFilesystemResize(existingPVCs, pvcName) {
			if pod.Annotations[metadata.AnnotationDrainState] == "" {
				healthy, healthErr := r.isShardHealthy(ctx, shard)
				if healthErr != nil {
					return 0, false, healthErr
				}
				if !healthy {
					continue
				}
				capacity, capacityErr := r.hasMaintenanceCapacityForPod(
					ctx,
					shard,
					cellName,
					pod.Name,
				)
				if capacityErr != nil {
					return 0, false, capacityErr
				}
				if !capacity {
					continue
				}
				if allowed, err := r.canStartDisruption(ctx, shard, pod, disruptions); err != nil {
					return 0, false, err
				} else if !allowed {
					continue
				}
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
				disruptions.SetStarted()
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

// isPoolHealthy returns true if the pool/cell has at least effectiveReplicas
// pods, and all of them — except extras (index >= effectiveReplicas) and
// QUARANTINED pods — are Ready, with none draining or terminating.
// Extra pods are excluded so an unhealthy extra pod does not block its own
// removal. QUARANTINED pods are excluded because they are expected to be
// unhealthy and are being replaced by quarantine remediation.
//
// The count check matters because a pod drained all the way to deletion
// disappears from existingPods entirely — there is nothing left for the
// per-pod checks below to flag as unhealthy. Without it, a pool/cell sitting
// at zero pods for a slot (deleted, replacement not yet created) would look
// indistinguishable from a pool that was never touched.
func isPoolHealthy(
	existingPods map[string]*corev1.Pod,
	effectiveReplicas int32,
	shard *multigresv1alpha1.Shard,
) bool {
	//nolint:gosec // pod count will never exceed int32 capacity
	if int32(len(existingPods)) < effectiveReplicas {
		return false
	}
	for _, pod := range existingPods {
		if idx, ok := resolvePodIndex(pod.Name); !ok || idx >= int(effectiveReplicas) {
			continue
		}
		if resolvePodRole(shard, pod.Name) == "QUARANTINED" {
			continue
		}
		if !pod.DeletionTimestamp.IsZero() {
			return false
		}
		if pod.Annotations[metadata.AnnotationDrainState] != "" {
			return false
		}
		if !isPodReady(pod) {
			return false
		}
	}
	return true
}

// isShardHealthy reports whether every declared pool/cell across the whole
// shard is healthy per isPoolHealthy and no pod, including extras, is draining.
func (r *ShardReconciler) isShardHealthy(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (bool, error) {
	podList, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return false, fmt.Errorf("listing pool pods for shard health check: %w", err)
	}

	// Group every pod by pool/cell
	podsByPoolCell := make(map[string]map[string]*corev1.Pod, len(shard.Spec.Pools))
	for i := range podList.Items {
		pod := &podList.Items[i]
		// Extras and surge pods still belong to the shard while they drain.
		// Check persisted operation state before filtering to desired ordinals.
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			return false, nil
		}
		key := pod.Labels[metadata.LabelMultigresPool] + "/" + pod.Labels[metadata.LabelMultigresCell]
		if podsByPoolCell[key] == nil {
			podsByPoolCell[key] = map[string]*corev1.Pod{}
		}
		podsByPoolCell[key][pod.Name] = pod
	}

	for poolName, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			replicas := DefaultPoolReplicas
			if pool.ReplicasPerCell != nil {
				replicas = *pool.ReplicasPerCell
			}
			group := podsByPoolCell[string(poolName)+"/"+string(cell)]
			if !isPoolHealthy(group, replicas, shard) {
				return false, nil
			}
		}
	}

	return true, nil
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
// replicas is the user-desired count; effectiveReplicas = replicas + maintenanceSurges.
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
	disruptionTrackers ...*shardRolloutTracker,
) (bool, bool, error) {
	logger := log.FromContext(ctx)
	disruptions := firstShardRolloutTracker(disruptionTrackers)

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
			if !actionTaken && !disruptions.HasStarted() {
				if err := r.handleExternalDeletion(ctx, shard, pod); err != nil {
					return actionTaken, inProgress, err
				}
				actionTaken = true
				disruptions.SetStarted()
			}
		}
	}

	// Cleanup pods ready for deletion
	for _, pod := range readyForDeletion {
		// A drain that started before a PVC it mounts began deleting stops
		// short of deleting the pod; see terminating_pvc.go.
		claim, err := podTerminatingClaim(ctx, r.disruptionReader(), pod)
		if err != nil {
			return actionTaken, inProgress, err
		}
		if claim != "" {
			// Its own PVC is still released by policy now, so a scale-down
			// whose pod a person then deletes does not strand it.
			if err := r.releaseDrainedPodPVC(
				ctx,
				shard,
				pod,
				poolName,
				poolSpec,
				replicas,
			); err != nil {
				return actionTaken, inProgress, fmt.Errorf(
					"failed to release PVC of drained pod %s: %w", pod.Name, err)
			}
			logger.Info("Not deleting drained pod while a PVC it mounts is being deleted",
				"pod", pod.Name, "pvc", claim)
			inProgress = true
			continue
		}
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
		disruptions.SetStarted()
	}

	// Drain extra pods (scale-down, skip if another drain is already in progress).
	// Health gate: refuse to start a new drain if the pool is already degraded.
	// This prevents cascading failures where removing pods from an unhealthy pool
	// could cause an outage.
	logger.V(1).
		Info("Scale-down check", "extraPods", len(extraPods), "actionTaken", actionTaken, "inProgress", inProgress, "desiredReplicas", replicas, "effectiveReplicas", effectiveReplicas)
	if !actionTaken && !inProgress && !disruptions.HasStarted() && len(extraPods) > 0 {
		if !isPoolHealthy(existingPods, effectiveReplicas, shard) {
			logger.Info(
				"Deferring scale-down: current pool has non-ready pods",
				"extraPods",
				len(extraPods),
			)
			r.Recorder.Eventf(
				shard,
				"Warning",
				"ScaleDownBlocked",
				"Deferring scale-down of %d extra pod(s): current pool has non-ready pods",
				len(extraPods),
			)
			return actionTaken, inProgress, nil
		}
		healthy, err := r.isShardHealthy(ctx, shard)
		if err != nil {
			return actionTaken, inProgress, err
		}
		if !healthy {
			logger.Info(
				"Deferring scale-down: shard has non-ready pods",
				"extraPods",
				len(extraPods),
			)
			r.Recorder.Eventf(
				shard,
				"Warning",
				"ScaleDownBlocked",
				"Deferring scale-down of %d extra pod(s): shard has non-ready pods",
				len(extraPods),
			)
			return actionTaken, inProgress, nil
		}
		podToDrain, err := r.selectShardScaleDownPod(ctx, shard, extraPods)
		if err != nil {
			return actionTaken, inProgress, err
		}
		if podToDrain != nil && existingPods[podToDrain.Name] == nil {
			// The selected replica belongs to another pool/cell. Let its normal
			// reconciliation own the drain and PVC lifecycle.
			return actionTaken, inProgress, nil
		}
		if podToDrain != nil && podToDrain.Annotations[metadata.AnnotationDrainState] == "" {
			if allowed, err := r.canStartDisruption(
				ctx,
				shard,
				podToDrain,
				disruptions,
			); err != nil ||
				!allowed {
				return actionTaken, inProgress, err
			}
			capacity, err := r.hasMaintenanceCapacityForPod(
				ctx,
				shard,
				podToDrain.Labels[metadata.LabelMultigresCell],
				podToDrain.Name,
			)
			if err != nil {
				return actionTaken, inProgress, err
			}
			if !capacity {
				return actionTaken, inProgress, nil
			}
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
			disruptions.SetStarted()
		}
	}

	return actionTaken, inProgress, nil
}

// shardRolloutTracker records, within a single Shard reconcile pass, whether
// any pool has already initiated a disruption. Scale-down, filesystem resize,
// external-deletion handling, and rolling updates all consult it so a pool
// reconciled later in the same pass cannot start another drain while the
// controller cache is catching up.
type shardRolloutTracker struct {
	started            bool
	waitingForRecovery bool
	surgeStartedCells  map[string]struct{}
	// blocked is why a drifted Pod the rollout must replace could not be,
	// reported once every pool has been reconciled so no later pool's
	// RollingUpdate condition hides it. blockedAgain records that the same
	// refusal was already reported, so the recovery requeue backs off.
	blocked      string
	blockedAgain bool
	// unreadyDecided and unreadyPending carry replaceUnreadyDriftedPod's
	// shard-wide decision to the remaining pool/cells of the pass.
	// unreadyDeferred records that it decided nothing because another
	// disruption path had already asked to wait.
	unreadyDecided  bool
	unreadyPending  bool
	unreadyDeferred bool
}

// block records the first reason a required replacement was refused this
// pass.
func (t *shardRolloutTracker) block(reason string) {
	if t.blocked == "" {
		t.blocked = reason
	}
}

func firstShardRolloutTracker(trackers []*shardRolloutTracker) *shardRolloutTracker {
	if len(trackers) > 0 && trackers[0] != nil {
		return trackers[0]
	}
	return &shardRolloutTracker{}
}

func (t *shardRolloutTracker) HasStarted() bool {
	return t.started
}

func (t *shardRolloutTracker) SetStarted() {
	t.started = true
}

func (t *shardRolloutTracker) HasSurgeStarted(cellName string) bool {
	_, ok := t.surgeStartedCells[cellName]
	return ok
}

func (t *shardRolloutTracker) SetSurgeStarted(cellName string) {
	if t.surgeStartedCells == nil {
		t.surgeStartedCells = make(map[string]struct{})
	}
	t.surgeStartedCells[cellName] = struct{}{}
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
	rollout *shardRolloutTracker,
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

	// Prevent a new drain if some other pool in this shard already
	// started one earlier in this same reconcile pass. Checked before
	// isShardHealthy so completion/cleanup in this pass cannot immediately
	// trigger another operation, even if the deleted pod is already absent.
	if rollout.HasStarted() {
		return nil
	}

	// A pod still configured for a global topology the shard has moved off
	// cannot register where multiorch and the readiness gate now look, so it
	// never turns Ready again. The health, capacity and posture gates below
	// all wait on Ready poolers, so they would hold its replacement forever.
	// Draining it costs nothing: it is already invisible to the rest of the
	// cluster. This stays one pod per pass but does not wait for the previous
	// replacement to turn Ready, because the first replacement alone cannot
	// form a quorum and so cannot turn Ready either.
	if pod := r.topoStrandedPod(ctx, shard, existingPods); pod != nil {
		// The health gates may be skipped here; keeping a PVC being deleted
		// in place may not. See terminating_pvc.go.
		terminating, err := mountedTerminatingPVCs(ctx, r.disruptionReader(), shard)
		if err != nil {
			return fmt.Errorf("check for mounted PVCs being deleted: %w", err)
		}
		if len(terminating) > 0 {
			return nil
		}
		if err := r.initiateDrain(ctx, pod); err != nil {
			return fmt.Errorf("failed to initiate drain for topology-stranded pod %s: %w",
				pod.Name, err)
		}
		rollout.SetStarted()
		logger.Info("Initiated drain for pod stranded on a previous global topology",
			"pod", pod.Name)
		r.Recorder.Eventf(
			shard,
			"Normal",
			"PodUpdated",
			"Initiated drain for pod %s, stranded on a previous global topology",
			pod.Name,
		)
		return nil
	}

	// A pod whose pgbackrest sidecar never started cannot turn Ready, so the
	// health gates below would hold its replacement forever. It never started
	// postgres either, so draining it costs no availability. One pod per pass,
	// like the stranded-topology path above.
	if pod := r.sidecarStuckPod(ctx, shard, existingPods); pod != nil {
		// This recovery path must retain the same PVC deletion barrier as
		// topology-stranded and RPC-observed replacements.
		terminating, err := mountedTerminatingPVCs(ctx, r.disruptionReader(), shard)
		if err != nil {
			return fmt.Errorf("check for mounted PVCs being deleted: %w", err)
		}
		if len(terminating) > 0 {
			return nil
		}
		if err := r.initiateDrain(ctx, pod); err != nil {
			return fmt.Errorf(
				"failed to initiate drain for pod %s with a stuck pgbackrest sidecar: %w",
				pod.Name,
				err,
			)
		}
		rollout.SetStarted()
		logger.Info("Initiated drain for pod whose pgbackrest sidecar never started",
			"pod", pod.Name)
		r.Recorder.Eventf(
			shard,
			"Normal",
			"PodUpdated",
			"Initiated drain for pod %s, whose pgbackrest sidecar never started",
			pod.Name,
		)
		return nil
	}

	// A drifted Pod that is not Ready may be the one the new spec repairs, so
	// it cannot wait for the shard to be healthy: that wait would last
	// forever. It is replaced through the RPC-observed preflight instead.
	// Ready drifted Pods still wait below, and any not-Ready Pod here also
	// keeps isShardHealthy false, so the two paths never both act.
	if handled, err := r.replaceUnreadyDriftedPod(ctx, shard, rollout); err != nil || handled {
		return err
	}

	// Prevent a new drain if some other pool/cell in this shard is
	// already mid-rollout or has not yet come back Ready. This reads uncached
	// state and includes extra pods left over from scale-down.
	if healthy, err := r.isShardHealthy(ctx, shard); err != nil {
		return err
	} else if !healthy {
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
			r.podOptions()...,
		) {
			continue
		}

		isPrimary := resolvePodRole(shard, pod.Name) == "PRIMARY"
		if !isPrimary {
			capacity, err := r.hasMaintenanceCapacityForPod(
				ctx,
				shard,
				cellName,
				pod.Name,
			)
			if err != nil {
				return err
			}
			if !capacity {
				return nil
			}
			if allowed, err := r.canStartDisruption(
				ctx,
				shard,
				pod,
				rollout,
			); err != nil ||
				!allowed {
				return err
			}
			if err := r.initiateDrain(ctx, pod); err != nil {
				return fmt.Errorf("failed to initiate drain for drifted pod %s: %w", pod.Name, err)
			}
			rollout.SetStarted()
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
		capacity, err := r.hasMaintenanceCapacityForPod(
			ctx,
			shard,
			cellName,
			waitPrimary.Name,
		)
		if err != nil {
			return err
		}
		if !capacity {
			return nil
		}
		if allowed, err := r.canStartDisruption(
			ctx,
			shard,
			waitPrimary,
			rollout,
		); err != nil ||
			!allowed {
			return err
		}
		if err := r.initiateDrain(ctx, waitPrimary); err != nil {
			return fmt.Errorf(
				"failed to request drain for primary pod %s: %w",
				waitPrimary.Name,
				err,
			)
		}
		rollout.SetStarted()
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

// rolloutCandidates is how replaceUnreadyDriftedPod sees the shard's desired
// pool Pods, each list in name order.
type rolloutCandidates struct {
	// unready are drifted, not Ready, and not the primary.
	unready []*corev1.Pod
	// ready are drifted and Ready, and not the primary.
	ready []*corev1.Pod
	// primary is the primary when it is drifted and not Ready.
	primary *corev1.Pod
	// current are already on the desired spec.
	current []*corev1.Pod
}

// classifyDesiredPods sorts the shard's desired pool Pods. Pods draining or
// terminating, quarantined Pods (quarantine remediation owns them), and Pods
// beyond their pool/cell's desired count other than an active maintenance
// surge (scale-down owns them) are left out.
func (r *ShardReconciler) classifyDesiredPods(
	shard *multigresv1alpha1.Shard,
	pods []corev1.Pod,
) rolloutCandidates {
	sorted := make([]*corev1.Pod, len(pods))
	for i := range pods {
		sorted[i] = &pods[i]
	}
	slices.SortFunc(sorted, func(a, b *corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	var c rolloutCandidates
	for _, pod := range sorted {
		poolName := pod.Labels[metadata.LabelMultigresPool]
		cellName := pod.Labels[metadata.LabelMultigresCell]
		poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
		if !ok || !poolUsesCell(poolSpec, cellName) {
			continue
		}
		index, ok := resolvePodIndex(pod.Name)
		// An active maintenance surge sits past the desired count (further
		// past it after a scale-down) and is replaced like a desired Pod: one
		// broken on an earlier spec would otherwise hold its cell's capacity,
		// and so the base Pod it exists to cover, forever.
		desired := ok &&
			(index < int(poolReplicas(poolSpec)) || isActiveMaintenanceSurge(shard, pod))
		if !desired ||
			!pod.DeletionTimestamp.IsZero() ||
			pod.Annotations[metadata.AnnotationDrainState] != "" {
			continue
		}
		role := resolvePodRole(shard, pod.Name)
		if role == "QUARANTINED" {
			continue
		}
		switch {
		case !podNeedsUpdate(pod, shard, poolName, cellName, poolSpec, index, r.Scheme, r.podOptions()...):
			c.current = append(c.current, pod)
		case role == "PRIMARY":
			if !isPodReady(pod) {
				c.primary = pod
			}
		case isPodReady(pod):
			c.ready = append(c.ready, pod)
		default:
			c.unready = append(c.unready, pod)
		}
	}
	return c
}

// missingDesiredPods returns, in name order, the desired pool Pods of the
// shard that do not exist: a replacement not yet created, or a new replica.
// Pool/cells are reconciled one after another, so another cell's rolling
// update can run before this cell creates its missing Pod.
func missingDesiredPods(shard *multigresv1alpha1.Shard, pods []corev1.Pod) []string {
	existing := make(map[string]bool, len(pods))
	for i := range pods {
		existing[pods[i].Name] = true
	}
	var missing []string
	for poolName, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			for index := range int(poolReplicas(pool)) {
				name := BuildPoolPodName(shard, string(poolName), string(cell), index)
				if !existing[name] {
					missing = append(missing, name)
				}
			}
		}
	}
	slices.Sort(missing)
	return missing
}

// unreadyExtras returns, in name order, the Pods of the shard's pool/cells
// beyond their desired count that are not Ready and that scale-down would
// remove: not a maintenance surge, not the primary, not quarantined, not
// already draining or terminating. Scale-down waits for every desired Pod to
// be Ready, so while desired Pods are being repaired these are removed here.
func unreadyExtras(shard *multigresv1alpha1.Shard, pods []corev1.Pod) []*corev1.Pod {
	var extras []*corev1.Pod
	for i := range pods {
		pod := &pods[i]
		// Only pool/cells still in the spec: handleScaleDown, which deletes a
		// drained extra, runs for nothing else, and a drain left on a Pod
		// nobody deletes would block every later disruption in the shard.
		poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(pod.Labels[metadata.LabelMultigresPool])]
		index, indexed := resolvePodIndex(pod.Name)
		if !ok || !poolUsesCell(poolSpec, pod.Labels[metadata.LabelMultigresCell]) || !indexed ||
			index < int(poolReplicas(poolSpec)) {
			continue
		}
		role := resolvePodRole(shard, pod.Name)
		if isPodReady(pod) || isMaintenanceSurge(pod) || role == "PRIMARY" ||
			role == "QUARANTINED" || !pod.DeletionTimestamp.IsZero() ||
			pod.Annotations[metadata.AnnotationDrainState] != "" {
			continue
		}
		extras = append(extras, pod)
	}
	slices.SortFunc(extras, func(a, b *corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	return extras
}

// replaceUnreadyDriftedPod starts the replacement of at most one drifted,
// not-Ready Pod anywhere in the shard. It reports handled when such Pods
// exist, whether or not one could be replaced, because isShardHealthy cannot
// pass while any of them does. It decides once per reconcile pass, over every
// pool and cell together: a candidate refused in one cell must not stop a
// replaceable candidate in another from being tried, and the shard-wide
// rollout tracker would stop it if each pool/cell decided alone.
//
// Readiness is not consulted for safety. Every pool Pod is observed by RPC,
// and the target must pass the same posture check every other disruption
// does: a committed primary that is serving, every other committed member
// observed and settled on the committed rule, and a remainder that satisfies
// durability. So a not-Ready Pod that is still a serving member is protected
// exactly as a Ready one would be, and a Pod that is not serving costs the
// shard nothing to restart. One disruption runs at a time: the preflight
// refuses while any pool Pod in the shard is draining or terminating.
//
// Nothing is replaced while a desired Pod does not exist yet or a Pod already
// on the desired spec is not both Ready and serving. The desired spec has then
// not shown that it works, and replacing more Pods onto it would let a bad
// spec advance every time an old Pod briefly turned not Ready; it also paces
// the roll forward, so each replacement is up before the next starts.
//
// Candidates are tried in order until one passes: not-Ready extras (the
// scale-down already asked for, which scale-down itself defers while desired
// Pods are not Ready), not-Ready drifted replicas, and, when the primary is
// itself drifted and not Ready, the Ready drifted replicas and then the
// primary, so the primary is replaced once, last, rather than failed over
// onto a replica that still has to be replaced. A target restoring a backup
// or creating the first backup is left alone so that work is not thrown away.
func (r *ShardReconciler) replaceUnreadyDriftedPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
) (bool, error) {
	if rollout.unreadyDecided {
		return rollout.unreadyPending, nil
	}
	rollout.unreadyDecided = true
	pods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return false, fmt.Errorf("listing pool pods for not-Ready replacement: %w", err)
	}
	desired := r.classifyDesiredPods(shard, pods.Items)
	if len(desired.unready) == 0 && desired.primary == nil {
		return false, nil
	}
	extras := unreadyExtras(shard, pods.Items)
	isExtra := make(map[string]bool, len(extras))
	for _, pod := range extras {
		isExtra[pod.Name] = true
	}
	candidates := append(slices.Clone(extras), desired.unready...)
	if desired.primary != nil {
		candidates = append(append(candidates, desired.ready...), desired.primary)
	}
	rollout.unreadyPending = true
	if rollout.waitingForRecovery {
		rollout.unreadyDeferred = true
		return true, nil
	}
	// The two waits below end on Pod writes the controller watches (the Pod
	// being created, the Pod turning Ready), so they need no recovery
	// requeue.
	if missing := missingDesiredPods(shard, pods.Items); len(missing) > 0 {
		rollout.block(fmt.Sprintf(
			"desired pod %s does not exist yet; not replacing more pods until it is up",
			missing[0]))
		return true, nil
	}
	for _, pod := range desired.current {
		if !isPodReady(pod) {
			rollout.block(fmt.Sprintf(
				"pod %s on the desired spec is not Ready; not replacing more pods onto it",
				pod.Name))
			return true, nil
		}
	}
	preflight, unobservable, err := r.observeDisruption(ctx, shard, pods)
	if err != nil {
		return true, err
	}
	if preflight == nil {
		rollout.waitingForRecovery = true
		if unobservable != "" {
			rollout.block("cannot observe the data plane: " + unobservable)
		}
		return true, nil
	}
	for _, pod := range desired.current {
		if !preflight.observation.Serving(pod.Name) {
			rollout.waitingForRecovery = true
			rollout.block(fmt.Sprintf(
				"pod %s on the desired spec is not serving; not replacing more pods onto it",
				pod.Name))
			return true, nil
		}
	}

	logger := log.FromContext(ctx)
	var firstBlock string
	var surgeFor *corev1.Pod
	for _, pod := range candidates {
		blocked, needsSurge, err := r.unreadyReplacementBlocked(
			ctx, shard, preflight, pod, isExtra[pod.Name])
		if err != nil {
			return true, err
		}
		if needsSurge && surgeFor == nil {
			surgeFor = pod
		}
		if blocked != "" {
			logger.V(1).Info("Not replacing drifted pod", "pod", pod.Name, "reason", blocked)
			if firstBlock == "" {
				firstBlock = fmt.Sprintf("not replacing pod %s: %s", pod.Name, blocked)
			}
			continue
		}
		if err := r.initiateDrain(ctx, pod); err != nil {
			return true, fmt.Errorf("failed to initiate drain for pod %s: %w", pod.Name, err)
		}
		rollout.SetStarted()
		logger.Info("Initiated drain while drifted pods are not Ready", "pod", pod.Name)
		r.Recorder.Eventf(
			shard,
			"Normal",
			"PodUpdated",
			"Initiated drain for pod %s while drifted pods are not Ready",
			pod.Name,
		)
		return true, nil
	}
	// Only when nothing could be replaced: a surge is temporary capacity the
	// shard pays for until the cell settles.
	if surgeFor != nil {
		if err := r.requestMaintenanceSurge(ctx, shard, surgeFor, rollout); err != nil {
			return true, err
		}
	}
	rollout.waitingForRecovery = true
	rollout.block(firstBlock)
	return true, nil
}

// unreadyReplacementBlocked returns why pod may not be replaced now, or "",
// and whether replacing it would leave its cell of a two-cell shard without a
// serving pooler, which a maintenance surge supplies. An extra is being
// removed, not replaced, so work in flight on it is not worth waiting for.
func (r *ShardReconciler) unreadyReplacementBlocked(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	preflight *disruptionPreflight,
	pod *corev1.Pod,
	extra bool,
) (string, bool, error) {
	observation := preflight.observation
	// Only work a restart would throw away holds a replacement back. A start
	// that fails because of the spec retries on every monitor tick and would
	// otherwise be reported in flight at almost every sample.
	if action, ok := observation.PostgresAction(pod.Name); !extra && ok &&
		(action == multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP ||
			action == multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_CREATING_FIRST_BACKUP) {
		return fmt.Sprintf("pooler reports in-flight postgres action %s", action), false, nil
	}
	// Capacity guards what a disruption takes away. A target that is not
	// serving takes nothing away, and counting it against a floor the shard
	// is already below would hold back the one change that can restore it.
	// It is checked before the posture check, as on the Ready path, because
	// the surge it asks for is what can make the posture check pass: a
	// two-cell shard with one pooler per cell has no remainder to lose one
	// from until the surge joins.
	cellName := pod.Labels[metadata.LabelMultigresCell]
	serving := func(p *corev1.Pod) bool {
		return p.DeletionTimestamp.IsZero() &&
			p.Annotations[metadata.AnnotationDrainState] == "" &&
			observation.Serving(p.Name)
	}
	// A cell left with no serving pooler needs a surge whether or not the
	// target serves: when it does not, the remainder of a one-pooler cell
	// cannot satisfy multi-cell durability until the surge joins.
	needsSurge := false
	if requiresTwoCellMaintenanceSurge(shard) {
		cellServing := false
		for i := range preflight.pods.Items {
			p := &preflight.pods.Items[i]
			if p.Name != pod.Name && p.Labels[metadata.LabelMultigresCell] == cellName &&
				serving(p) {
				cellServing = true
			}
		}
		needsSurge = !cellServing
	}
	if observation.Serving(pod.Name) {
		capacity, err := r.maintenanceCapacityWithout(ctx, shard, cellName, pod.Name, serving)
		if err != nil {
			return "", false, err
		}
		if !capacity {
			return "too few serving poolers would remain in the shard or in cell " + cellName,
				needsSurge, nil
		}
	}
	if err := r.disruptionCheck(shard, preflight, pod); err != nil {
		// Only a remainder too small to stay durable is something more
		// poolers resolve; a surge cannot fix a missing primary or a member
		// that has not recovered.
		return err.Error(), needsSurge && stderrors.Is(err, posture.ErrInsufficientRemainder), nil
	}
	return "", false, nil
}

// requestMaintenanceSurge creates the temporary pooler that lets pod's cell
// keep a serving member while pod is replaced. reconcileCellMaintenanceSurge
// creates one ahead of a Ready drifted Pod from Pod readiness alone; a
// not-Ready Pod needs one only when the RPC observation shows it is still its
// cell's last serving pooler, so it is asked for here, once that is known.
// The surge is then kept and released by reconcileCellMaintenanceSurge like
// any other.
func (r *ShardReconciler) requestMaintenanceSurge(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	rollout *shardRolloutTracker,
) error {
	if !requiresTwoCellMaintenanceSurge(shard) {
		return nil
	}
	poolName := pod.Labels[metadata.LabelMultigresPool]
	cellName := pod.Labels[metadata.LabelMultigresCell]
	poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	if !ok || rollout.HasSurgeStarted(cellName) {
		return nil
	}
	cellPods, err := r.listCellPoolers(ctx, shard, cellName)
	if err != nil {
		return err
	}
	existingPods := map[string]*corev1.Pod{}
	for i := range cellPods.Items {
		p := &cellPods.Items[i]
		if isActiveMaintenanceSurge(shard, p) {
			return nil
		}
		if p.Labels[metadata.LabelMultigresPool] == poolName {
			existingPods[p.Name] = p
		}
	}
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList, client.InNamespace(shard.Namespace),
		client.MatchingLabels(metadata.GetSelectorLabels(
			buildPoolLabelsWithCell(shard, poolName, cellName)))); err != nil {
		return fmt.Errorf("list PVCs for maintenance surge in cell %s: %w", cellName, err)
	}
	existingPVCs := map[string]*corev1.PersistentVolumeClaim{}
	for i := range pvcList.Items {
		existingPVCs[pvcList.Items[i].Name] = &pvcList.Items[i]
	}
	if err := r.createOrAdoptMaintenanceSurge(ctx, shard, poolName, cellName, poolSpec,
		existingPods, existingPVCs, poolReplicas(poolSpec)); err != nil {
		return err
	}
	rollout.SetSurgeStarted(cellName)
	return nil
}

// topoStrandedPod returns the lowest-named pool pod stranded on a previous
// global topology, or nil.
//
// podTopoDrifted only nominates candidates. The readiness gate it reads also
// goes False when observation fails for reasons unrelated to topology, such
// as the operator being unable to load its RPC credentials, and a pod whose
// old address still reaches the shard's topology is registered there and
// must keep the gated path. So a candidate is stranded only once the shard's
// current topology has been read and holds no live pooler for it. Failing to
// read it counts as the pooler being present, leaving the gated path in
// charge.
func (r *ShardReconciler) topoStrandedPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pods map[string]*corev1.Pod,
) *corev1.Pod {
	var candidates []*corev1.Pod
	for _, name := range slices.Sorted(maps.Keys(pods)) {
		if podTopoDrifted(pods[name], shard) {
			candidates = append(candidates, pods[name])
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	logger := log.FromContext(ctx)
	// isAnyPodDraining and the rollout tracker only serialize within one
	// pool/cell and one pass, and the gates this path skips are what
	// serialize the rest of the shard. Without this, each pool/cell would
	// start its own drain on the next pass, and if the old topology is still
	// up, several poolers it is still serving could go at once. Read
	// uncached, like canStartDisruption, so a drain annotation written by the
	// previous pass is not missed.
	shardPods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		logger.Info("Not replacing topology-drifted pods: cannot list the shard's pods",
			"error", err)
		return nil
	}
	for i := range shardPods.Items {
		pod := &shardPods.Items[i]
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			return nil
		}
	}

	store, err := r.topoStore(ctx, shard)
	if err != nil || store == nil {
		logger.Info("Not replacing topology-drifted pods: cannot open the shard's topology",
			"error", err)
		return nil
	}
	defer func() { _ = store.Close() }()

	names := make([]string, len(candidates))
	for i, pod := range candidates {
		names[i] = pod.Name
	}
	live, err := topo.LivePoolerPods(ctx, store, shard, names)
	if err != nil {
		logger.Info("Not replacing topology-drifted pods: cannot read the shard's topology",
			"error", err)
		return nil
	}
	for _, pod := range candidates {
		if !live[pod.Name] {
			return pod
		}
	}
	return nil
}

// podTopoDrifted reports whether pod's multipooler points at a different
// global topology (address or root) than the shard's spec while its pooler
// readiness gate is not True. That makes it a candidate for topoStrandedPod,
// which decides.
func podTopoDrifted(pod *corev1.Pod, shard *multigresv1alpha1.Shard) bool {
	if pod == nil || !pod.DeletionTimestamp.IsZero() ||
		pod.Annotations[metadata.AnnotationDrainState] != "" || poolerDataReady(pod) {
		return false
	}
	for _, c := range pod.Spec.Containers {
		if c.Name != multipoolerContainerName {
			continue
		}
		var addr, root string
		var haveAddr, haveRoot bool
		for _, arg := range c.Args {
			if v, ok := strings.CutPrefix(arg, topoGlobalAddressFlag); ok {
				addr, haveAddr = v, true
			} else if v, ok := strings.CutPrefix(arg, topoGlobalRootFlag); ok {
				root, haveRoot = v, true
			}
		}
		// A pod missing either flag was not built by this operator's
		// multipooler template, so there is no topology to compare.
		if !haveAddr || !haveRoot {
			return false
		}
		return addr != shard.Spec.GlobalTopoServer.Address ||
			root != shard.Spec.GlobalTopoServer.RootPath
	}
	return false
}

func poolerDataReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == PoolerDataReadyCondition {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
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

func (r *ShardReconciler) cleanupDrainedPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	poolName string,
	poolSpec multigresv1alpha1.PoolSpec,
	replicas int32,
) error {
	if err := r.releaseDrainedPodPVC(ctx, shard, pod, poolName, poolSpec, replicas); err != nil {
		return err
	}
	log.FromContext(ctx).Info("Drained pod cleanup complete", "pod", pod.Name)
	r.Recorder.Eventf(shard, "Normal", "DrainCompleted", "Completed drain for pod %s", pod.Name)
	return nil
}

// releaseDrainedPodPVC applies the WhenScaled PVC policy to a drained pod's
// data PVC: one past the desired replicas is marked orphan, one within them is
// kept for the pod's replacement. Idempotent.
func (r *ShardReconciler) releaseDrainedPodPVC(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	poolName string,
	poolSpec multigresv1alpha1.PoolSpec,
	replicas int32,
) error {
	logger := log.FromContext(ctx)

	// Respect the WhenScaled PVC-deletion policy.
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
	return nil
}

// cleanupPodPVC removes a pod's data PVC from the operator's care by marking
// it orphan. The multigres-gc CronJob deletes it once the retention window
// elapses, giving an accidental scale down or replace a window to be rolled
// back. This only runs while the Shard itself is not being deleted, see
// cleanupShardPVCs for the Shard and cluster teardown path.
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

	if err := pvcutil.MarkOrphan(ctx, r.Client, pvc, shard.GetUID(), time.Now()); err != nil {
		logger.Error(err, "Failed to mark PVC orphan for "+reason+" pod", "pvc", pvcName)
		return fmt.Errorf("failed to mark PVC %s orphan: %w", pvcName, err)
	}
	logger.Info("Marked PVC orphan for "+reason+" pod", "pvc", pvcName)
	return nil
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
	opts ...PodOption,
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
	desired, err := BuildPoolPod(shard, poolName, cellName, poolSpec, index, scheme, opts...)
	if err != nil {
		return false // Assume no update needed if we can't build it
	}

	desiredHash := ComputeSpecHash(desired)
	return existingHash != desiredHash
}

// expandPVCIfNeeded patches an existing PVC's storage request when the desired
// size (from poolSpec.Storage.Size) exceeds the current PVC spec and the PVC
// can be expanded (see storageRequestFor). This enables in-place volume
// expansion without pod deletion.
func (r *ShardReconciler) expandPVCIfNeeded(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pvc *corev1.PersistentVolumeClaim,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	logger := log.FromContext(ctx)

	want, err := poolStorageRequest(poolSpec)
	if err != nil {
		return err
	}
	desired, _, err := r.storageRequestFor(ctx, pvc, want)
	if err != nil {
		return err
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
