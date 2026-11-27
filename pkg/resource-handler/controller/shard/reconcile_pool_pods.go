package shard

import (
	"cmp"
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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	listed := podList.Items
	podList.Items = r.managedPoolPods(ctx, shard, listed)
	if len(podList.Items) != len(listed) {
		// Pods that carry this pool/cell's labels and that the shard does
		// not manage. They are reported in the RollingUpdate condition. One
		// that holds a desired pod's name keeps maintenance from creating
		// that pod; see shardRolloutTracker.held.
		managed := make(map[string]bool, len(podList.Items))
		for i := range podList.Items {
			managed[podList.Items[i].Name] = true
		}
		for i := range listed {
			name := listed[i].Name
			if managed[name] {
				continue
			}
			rollout.noteIgnored(name)
		}
		r.logIgnoredPods(ctx, shard, poolName, cellName, listed, managed)
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
	// Keep desired surge capacity separate from disruption requests: a cell
	// that acted here may not nominate anything, but its drift still counts.
	if maintenanceSurges > 0 {
		if rollout.keptSurges == nil {
			rollout.keptSurges = map[poolCell]int32{}
		}
		rollout.keptSurges[poolCell{poolName, cellName}] = maintenanceSurges
	}
	if surgeAction {
		// The surge pod or PVC this pass created or waits on ends in a watch
		// event.
		rollout.pending = true
		// The pass ends here for this pool/cell, but its drift still belongs
		// in the shard's RollingUpdate condition: the surge exists to replace
		// a drifted Pod.
		drifted, draining := r.poolCellRollout(
			shard, poolName, cellName, poolSpec, existingPods, replicas)
		rollout.notePool(poolName, drifted, draining || drifted > 0)
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

	// Phase 3: Record drift and ask for rolling updates
	r.handleRollingUpdates(
		shard,
		poolName,
		cellName,
		driftedCount,
		actionTaken,
		inProgress,
		rollout,
	)

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

// poolCellRollout counts the pool/cell's live desired Pods on an old spec,
// the Pods createMissingResources counts, and reports whether any of its Pods is
// draining or terminating.
func (r *ShardReconciler) poolCellRollout(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	existingPods map[string]*corev1.Pod,
	replicas int32,
) (drifted int, draining bool) {
	for _, pod := range existingPods {
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			draining = true
		}
	}
	for i := range replicas {
		pod := existingPods[BuildPoolPodName(shard, poolName, cellName, int(i))]
		if pod == nil || !pod.DeletionTimestamp.IsZero() ||
			pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		if podNeedsUpdate(pod, shard, poolName, cellName, poolSpec, int(i), r.Scheme) {
			drifted++
		}
	}
	return drifted, draining
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
			} else if err := r.reconcilePoolDataPVC(ctx, shard, existingPVCs[pvcName], poolSpec); err != nil {
				return 0, false, err
			}
		}

		// Create Pod if missing
		pod, exists := existingPods[podName]
		if _, ignored := disruptions.ignored[podName]; !exists && ignored {
			// The name is taken by a pod the shard does not manage, so the
			// create cannot succeed. Nothing watches that pod; the backstop
			// requeue finds it gone.
			disruptions.noteHeld(podName)
			continue
		}
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
			// A pod started on a PVC being deleted either cannot schedule or,
			// once scheduled, holds that PVC and its doomed data in service.
			if claim, err := r.claimPodVolumes(ctx, shard, desiredPod); err != nil {
				return 0, false, err
			} else if claim != "" {
				logger.Info("Not creating pool pod while a PVC it mounts is being deleted",
					"pod", podName, "pvc", claim)
				disruptions.pending = true
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
			disruptions.pending = true
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
			disruptions.pending = true
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
			disruptions.pending = true
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
		isDrifted := podNeedsUpdate(pod, shard, poolName, cellName, poolSpec, int(i), r.Scheme)
		if isDrifted {
			driftedCount++
		}

		// A data PVC whose block device has grown may need its pod restarted
		// before the filesystem can grow. Whether the pod may be removed now
		// is the disruption walk's decision.
		if !actionTaken &&
			!disruptions.HasStarted() &&
			pvcNeedsFilesystemResize(existingPVCs, pvcName) &&
			pod.Annotations[metadata.AnnotationDrainState] == "" {
			disruptions.cell(poolName, cellName).noteResize(pod.UID, pvcName)
		}

		// Drift detection above already counted this pod. No creation blocking
		// needed here — unready pods do not prevent creation of other replicas.
		// The scale-down and rolling-update requests are still gated by
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

// shardPodsHealthy reports whether every declared pool/cell across the whole
// shard is healthy per isPoolHealthy and no pod among pods, the shard's pool
// Pods, is draining or terminating, extras included.
func shardPodsHealthy(shard *multigresv1alpha1.Shard, pods []corev1.Pod) bool {
	podsByPoolCell := make(map[poolCell]map[string]*corev1.Pod, len(shard.Spec.Pools))
	for i := range pods {
		pod := &pods[i]
		// Extras and surge pods still belong to the shard while they drain.
		// Check persisted operation state before filtering to desired ordinals.
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			return false
		}
		key := podPoolCell(pod)
		if podsByPoolCell[key] == nil {
			podsByPoolCell[key] = map[string]*corev1.Pod{}
		}
		podsByPoolCell[key][pod.Name] = pod
	}

	for poolName, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			group := podsByPoolCell[poolCell{string(poolName), string(cell)}]
			if !isPoolHealthy(group, poolReplicas(pool), shard) {
				return false
			}
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
		if drainState != "" || !pod.DeletionTimestamp.IsZero() || !isPodReady(pod) {
			disruptions.pending = true
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
			disruptions.pending = true
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

	// Extra pods are drained by the disruption walk, one at a time across the
	// shard. Ask for it unless something is already happening here.
	logger.V(1).
		Info("Scale-down check", "extraPods", len(extraPods), "actionTaken", actionTaken, "inProgress", inProgress, "desiredReplicas", replicas, "effectiveReplicas", effectiveReplicas)
	if !actionTaken && !inProgress && !disruptions.HasStarted() && len(extraPods) > 0 {
		cellName := extraPods[0].Labels[metadata.LabelMultigresCell]
		disruptions.cell(poolName, cellName).noteScaleDown(effectiveReplicas)
	}

	return actionTaken, inProgress, nil
}

// shardRolloutTracker carries one Shard reconcile pass's rollout state from
// pool maintenance to the disruption walk and the RollingUpdate condition.
type shardRolloutTracker struct {
	// started records that the pass already disrupted or removed a pool pod
	// (cleanup of a drained pod, handling an external deletion, the walk's
	// one drain), so nothing else starts in the same pass while the
	// controller cache is catching up.
	started bool
	// waitingForRecovery records that the walk waits on something only a
	// pooler RPC shows, such as a posture refusal or a data plane that
	// cannot be observed. No watch event announces its end, so the shard is
	// polled, on a backoff.
	waitingForRecovery bool
	// pending records that the pass is waiting on an operation whose end a
	// Pod or PVC watch event announces: a pod created and not yet Ready, a
	// pod or PVC being deleted, a drain requested or in progress. The event
	// starts the next pass; pending only asks for the backstop requeue.
	pending           bool
	surgeStartedCells map[string]struct{}
	// blocked is why the walk could start no disruption this pass, reported
	// in the RollingUpdate condition. blockedAgain records that the same
	// refusal was already reported, so its Event is not repeated.
	blocked      string
	blockedAgain bool
	// cells records, per pool/cell, the disruptions its maintenance left for
	// the walk to decide. A pool/cell that acted or has a drain in progress
	// asks for none.
	cells      map[poolCell]*cellDisruptions
	keptSurges map[poolCell]int32
	// generation is the Shard generation pool maintenance read its spec
	// from. The walk acts on cells only while the shard is still on it.
	generation int64
	// ignored are the pods pool maintenance listed by a pool/cell's labels
	// and left out because the shard does not manage them.
	ignored map[string]struct{}
	// held are the desired pods maintenance could not create because an
	// ignored pod holds their name. The rollout cannot finish until that
	// pod is deleted, and the RollingUpdate condition says so.
	held map[string]struct{}
	// pools records, per pool, what handleRollingUpdates saw this pass,
	// summed over the pool's cells, so the final RollingUpdate condition
	// describes every pool rather than whichever was reconciled last.
	pools map[string]*poolRolloutState
}

// poolRolloutState is one pool's rollout progress in the current pass.
type poolRolloutState struct {
	drifted    int
	inProgress bool
}

// notePool adds one pool/cell's rollout progress to its pool's total.
func (t *shardRolloutTracker) notePool(poolName string, drifted int, inProgress bool) {
	if t.pools == nil {
		t.pools = map[string]*poolRolloutState{}
	}
	p := t.pools[poolName]
	if p == nil {
		p = &poolRolloutState{}
		t.pools[poolName] = p
	}
	p.drifted += drifted
	p.inProgress = p.inProgress || inProgress
}

// poolsRollingUpdate builds the RollingUpdate condition for every pool noted
// this pass, naming pools in sorted order so the message depends only on the
// state. It reports false when no pool was noted.
func (t *shardRolloutTracker) poolsRollingUpdate() (metav1.Condition, bool) {
	if len(t.pools) == 0 {
		return metav1.Condition{}, false
	}
	names := slices.Sorted(maps.Keys(t.pools))
	var drifting []string
	for _, name := range names {
		if p := t.pools[name]; p.inProgress {
			noun, verb := "pods", "need"
			if p.drifted == 1 {
				noun, verb = "pod", "needs"
			}
			drifting = append(drifting,
				fmt.Sprintf("%d %s %s update in pool %s", p.drifted, noun, verb, name))
		}
	}
	if len(drifting) > 0 {
		return metav1.Condition{
			Type:    "RollingUpdate",
			Status:  metav1.ConditionTrue,
			Reason:  "PodsDrifted",
			Message: strings.Join(drifting, "; "),
		}, true
	}
	message := fmt.Sprintf("All pods up to date in pool %s", names[0])
	if len(names) > 1 {
		message = "All pods up to date in pools " + strings.Join(names, ", ")
	}
	return metav1.Condition{
		Type:    "RollingUpdate",
		Status:  metav1.ConditionFalse,
		Reason:  "PodsUpToDate",
		Message: message,
	}, true
}

// poolCell names one cell of one pool.
type poolCell struct{ pool, cell string }

func podPoolCell(pod *corev1.Pod) poolCell {
	return poolCell{
		pod.Labels[metadata.LabelMultigresPool],
		pod.Labels[metadata.LabelMultigresCell],
	}
}

// cellDisruptions is what one pool/cell's maintenance asks the disruption
// walk to consider.
type cellDisruptions struct {
	// resize are the pods, by UID, whose data PVC needs a restart to grow
	// its filesystem, in ordinal order.
	resize []resizeRequest
	// scaleDown is set when the pool/cell has pods beyond effectiveReplicas,
	// its replica count plus its active maintenance surges.
	scaleDown         bool
	effectiveReplicas int32
	// rolling is set when the pool/cell has drifted pods to replace.
	rolling bool
}

type resizeRequest struct {
	uid types.UID
	pvc string
}

func (c *cellDisruptions) noteResize(pod types.UID, pvc string) {
	c.resize = append(c.resize, resizeRequest{uid: pod, pvc: pvc})
}

func (c *cellDisruptions) noteScaleDown(effectiveReplicas int32) {
	c.scaleDown = true
	c.effectiveReplicas = effectiveReplicas
}

// cell returns the pool/cell's requests, creating the record.
func (t *shardRolloutTracker) cell(poolName, cellName string) *cellDisruptions {
	key := poolCell{poolName, cellName}
	if t.cells == nil {
		t.cells = map[poolCell]*cellDisruptions{}
	}
	if t.cells[key] == nil {
		t.cells[key] = &cellDisruptions{}
	}
	return t.cells[key]
}

// noteIgnored records a pod pool maintenance listed and does not manage.
func (t *shardRolloutTracker) noteIgnored(podName string) {
	if t.ignored == nil {
		t.ignored = map[string]struct{}{}
	}
	t.ignored[podName] = struct{}{}
}

// ignoredPodsShown is how many ignored pods ignoredSummary names.
const ignoredPodsShown = 3

// ignoredSummary names the ignored pods for the RollingUpdate condition
// message, or is empty when there are none. The names are sorted and capped
// so the message depends only on which pods exist, not on the pass.
func (t *shardRolloutTracker) ignoredSummary() string {
	if len(t.ignored) == 0 {
		return ""
	}
	names := slices.Sorted(maps.Keys(t.ignored))
	more := ""
	if len(names) > ignoredPodsShown {
		more = fmt.Sprintf(" and %d more", len(names)-ignoredPodsShown)
		names = names[:ignoredPodsShown]
	}
	return fmt.Sprintf("; ignoring pods the shard does not manage: %s%s",
		strings.Join(names, ", "), more)
}

// noteHeld records a desired pod whose name an ignored pod holds.
func (t *shardRolloutTracker) noteHeld(podName string) {
	if t.held == nil {
		t.held = map[string]struct{}{}
	}
	t.held[podName] = struct{}{}
	t.pending = true
}

// heldName returns the first, by name, of the desired pods whose name an
// ignored pod holds, or "" when there is none.
func (t *shardRolloutTracker) heldName() string {
	if len(t.held) == 0 {
		return ""
	}
	return slices.Min(slices.Collect(maps.Keys(t.held)))
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

// isPending reports whether the pass waits on an operation that ends in a
// watch event: one it noted, one it started, or whatever a pool/cell asked
// the disruption walk for and the walk did not refuse over RPC.
func (t *shardRolloutTracker) isPending() bool {
	return t.pending || t.started || len(t.cells) > 0
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

// handleRollingUpdates records the pool/cell's drift for the RollingUpdate
// condition and asks the disruption walk to replace its drifted pods.
func (r *ShardReconciler) handleRollingUpdates(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	driftedCount int,
	actionTaken, isAnyPodDraining bool,
	rollout *shardRolloutTracker,
) {
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

	rollout.notePool(poolName, driftedCount, inProgress)

	// Nothing new starts for this pool/cell in a pass that created, deleted
	// or cleaned up one of its pods, or while one of them drains.
	if actionTaken || isAnyPodDraining || driftedCount == 0 || rollout.HasStarted() {
		return
	}
	rollout.cell(poolName, cellName).rolling = true
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
		case !podNeedsUpdate(pod, shard, poolName, cellName, poolSpec, index, r.Scheme):
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

// unreadyReplacementBlocked returns why pod may not be replaced now, or "",
// and whether replacing it would leave its cell of a two-cell shard without a
// serving pooler, which a maintenance surge supplies. An extra is being
// removed, not replaced, so work in flight on it is not worth waiting for.
func (r *ShardReconciler) unreadyReplacementBlocked(
	shard *multigresv1alpha1.Shard,
	preflight *disruptionPreflight,
	pod *corev1.Pod,
	extra bool,
) (string, bool) {
	observation := preflight.observation
	// Only work a restart would throw away holds a replacement back. A start
	// that fails because of the spec retries on every monitor tick and would
	// otherwise be reported in flight at almost every sample.
	if action, ok := observation.PostgresAction(pod.Name); !extra && ok &&
		(action == multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP ||
			action == multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_CREATING_FIRST_BACKUP) {
		return fmt.Sprintf("pooler reports in-flight postgres action %s", action), false
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
	poolers := preflight.pods.Items
	if requiresTwoCellMaintenanceSurge(shard) {
		cellServing := false
		for i := range poolers {
			p := &poolers[i]
			if p.Name != pod.Name && p.Labels[metadata.LabelMultigresCell] == cellName &&
				serving(p) {
				cellServing = true
			}
		}
		needsSurge = !cellServing
	}
	if observation.Serving(pod.Name) {
		if !maintenanceCapacityAmong(shard, poolers, cellName, pod.Name, serving) {
			return "too few serving poolers would remain in the shard or in cell " + cellName,
				needsSurge
		}
	}
	if err := r.disruptionCheck(shard, preflight, pod); err != nil {
		// Only a remainder too small to stay durable is something more
		// poolers resolve; a surge cannot fix a missing primary or a member
		// that has not recovered.
		return err.Error(), needsSurge && stderrors.Is(err, posture.ErrInsufficientRemainder)
	}
	return "", false
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

// topoStrandedPod returns the first of pods, taken in order, that is stranded
// on a previous global topology, or nil.
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
	pods []*corev1.Pod,
) *corev1.Pod {
	var candidates []*corev1.Pod
	for _, pod := range pods {
		if podTopoDrifted(pod, shard) {
			candidates = append(candidates, pod)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	logger := log.FromContext(ctx)
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

// rankPodsToDrain orders scale-down candidates best first: not-Ready before
// Ready, the primary last, then highest ordinal. Equal scores keep the input
// order.
func rankPodsToDrain(extraPods []*corev1.Pod, shard *multigresv1alpha1.Shard) []*corev1.Pod {
	ranked := make([]*corev1.Pod, 0, len(extraPods))
	scores := make(map[*corev1.Pod]int, len(extraPods))
	for _, pod := range extraPods {
		if pod == nil {
			continue
		}
		score, _ := resolvePodIndex(pod.Name)
		if resolvePodRole(shard, pod.Name) == "PRIMARY" {
			score -= 1000
		}
		if !isPodReady(pod) {
			score += 500
		}
		scores[pod] = score
		ranked = append(ranked, pod)
	}
	slices.SortStableFunc(
		ranked,
		func(a, b *corev1.Pod) int { return cmp.Compare(scores[b], scores[a]) },
	)
	return ranked
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

// reconcilePoolDataPVC also backfills the allocation on a data PVC created
// before the operator recorded it at creation. Never relabel a volume from a
// different allocation when a later scale-up or catalog change brings it back.
func (r *ShardReconciler) reconcilePoolDataPVC(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pvc *corev1.PersistentVolumeClaim,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	if err := pvcutil.RecordBackupPrefix(
		ctx,
		r.Client,
		pvc,
		shard.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix],
	); err != nil {
		return err
	}
	return r.expandPVCIfNeeded(ctx, shard, pvc, poolSpec)
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
