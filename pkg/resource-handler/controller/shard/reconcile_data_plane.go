package shard

import (
	"context"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
	"github.com/multigres/multigres-operator/pkg/data-handler/drain"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

// reconcileDataPlane opens a topo connection and runs all data-plane phases:
// PodRoles update, drain state machine, and backup health evaluation.
// Database registration is handled by the MultigresCluster controller.
func (r *ShardReconciler) reconcileDataPlane(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rendered renderedConfig,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Open a single topo connection for PodRoles, drain, and backup health.
	store, err := r.topoStore(shard)
	if err != nil {
		if !topo.IsTopoUnavailable(err) {
			r.Recorder.Eventf(shard, "Warning", "TopologyError",
				"Failed to connect to topology store: %v", err)
		}
		logger.Error(err, "Failed to get topo store, cannot update roles or execute drain")
		return ctrl.Result{RequeueAfter: topoUnavailableRequeueDelay}, nil
	}
	defer func() { _ = store.Close() }()

	// Phase: Update PodRoles from topology
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcilePodRoles")
		r.reconcilePodRoles(ctx, store, shard)
		childSpan.End()
	}

	postureRequeue := false
	if r.RPCClient != nil {
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcilePosture")
		var err error
		postureRequeue, err = r.reconcilePosture(ctx, store, shard)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			return ctrl.Result{}, err
		}
		childSpan.End()

		// Publish posture, condition, and phase in one status update.
		if err := r.updateStatus(ctx, shard, rendered); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status after posture observation: %w", err)
		}
	}

	// Requeue when the shard is Healthy but topology hasn't elected a primary
	// yet. This closes a race where the reconciliation burst settles before
	// multiorch writes the primary type to etcd.
	if shard.Status.Phase == multigresv1alpha1.PhaseHealthy && !hasPrimary(shard.Status.PodRoles) {
		logger.Info("No primary in podRoles, requeueing to re-read topology")
		return withPostureRequeue(ctrl.Result{RequeueAfter: 10 * time.Second}, postureRequeue), nil
	}

	// Phase: Prune stale pooler entries from topology
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcilePoolerPrune")
		r.reconcilePoolerPrune(ctx, store, shard)
		childSpan.End()
	}

	// Phase: Remediate quarantined (unrecoverable) poolers by replacing the pod
	// and wiping its data PVC so it re-bootstraps from backup. Runs before the
	// drain state machine: a quarantined node is already down, so replacing it is
	// the priority disruptive action this cycle.
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileQuarantineRemediation")
		acted, err := r.reconcileQuarantineRemediation(ctx, store, shard)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			return ctrl.Result{}, err
		}
		childSpan.End()
		if acted {
			return withPostureRequeue(
				ctrl.Result{RequeueAfter: quarantineRemediationRequeue},
				postureRequeue,
			), nil
		}
	}

	// Phase: Execute drain state machine for pods with drain annotations
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileDrainState")
		requeue, err := r.reconcileDrainState(ctx, store, shard)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			return ctrl.Result{}, err
		}
		childSpan.End()
		if requeue {
			return withPostureRequeue(
				ctrl.Result{RequeueAfter: 2 * time.Second},
				postureRequeue,
			), nil
		}
	}

	// Phase: Evaluate backup health
	if r.RPCClient != nil {
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileBackupHealth")
		result, err := backuphealth.Evaluate(ctx, store, r.RPCClient, shard)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to evaluate backup health")
			r.Recorder.Eventf(
				shard,
				"Warning",
				"BackupCheckFailed",
				"Failed to check backup health: %v",
				err,
			)
		} else if result != nil {
			backupBase := shard.DeepCopy()
			prevHealthy := status.IsConditionTrue(
				shard.Status.Conditions,
				backuphealth.ConditionHealthy,
			)
			backuphealth.Apply(shard, result)

			if result.Healthy && !prevHealthy {
				r.Recorder.Event(shard, "Normal", "BackupHealthy", result.Message)
			} else if !result.Healthy && prevHealthy {
				r.Recorder.Eventf(shard, "Warning", "BackupStale", result.Message)
			}

			if err := r.Status().Patch(ctx, shard, client.MergeFrom(backupBase)); err != nil {
				monitoring.RecordSpanError(childSpan, err)
				childSpan.End()
				logger.Error(err, "Failed to update shard backup status")
				return ctrl.Result{}, err
			}
			childSpan.End()
		} else {
			childSpan.End()
		}
	}

	// Phase: Apply reload-safe config changes in place (SIGHUP via ReloadConfig),
	// so a reload-only postgresql.conf change converges without recreating pods.
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileReloadState")
		wait, err := r.reconcileReloadState(ctx, store, shard, rendered)
		childSpan.End()
		if err != nil {
			logger.Error(err, "Failed to reconcile config reload state")
			return ctrl.Result{}, err
		}
		if wait > 0 {
			return withPostureRequeue(ctrl.Result{RequeueAfter: wait}, postureRequeue), nil
		}
	}

	return withPostureRequeue(ctrl.Result{}, postureRequeue), nil
}

// reconcilePodRoles queries the topology for pooler status and updates
// shard.Status.PodRoles. Pod names are resolved by matching topology entries
// to actual managed Kubernetes pods via PodMatchesPooler.
func (r *ShardReconciler) reconcilePodRoles(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
) {
	logger := log.FromContext(ctx)

	// List managed pods for this shard (same pattern as reconcilePoolerPrune).
	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(lbls),
	); err != nil {
		logger.Error(err, "Failed to list pods for role reconciliation")
		return
	}

	podNames := make([]string, len(podList.Items))
	for i := range podList.Items {
		podNames[i] = podList.Items[i].Name
	}

	statusBase := shard.DeepCopy()
	poolerStatus := topo.GetPoolerStatus(ctx, store, shard, podNames)

	if shard.Status.PodRoles == nil {
		shard.Status.PodRoles = make(map[string]string)
	}
	rolesChanged := false

	for podName, role := range poolerStatus.Roles {
		if shard.Status.PodRoles[podName] != role {
			shard.Status.PodRoles[podName] = role
			rolesChanged = true
		}
	}

	// Prune entries for poolers that no longer exist in the topology.
	if poolerStatus.QuerySuccess {
		for podName := range shard.Status.PodRoles {
			if _, exists := poolerStatus.Roles[podName]; !exists {
				delete(shard.Status.PodRoles, podName)
				rolesChanged = true
			}
		}
	}

	if rolesChanged {
		if err := r.Status().Patch(ctx, shard, client.MergeFrom(statusBase)); err != nil {
			logger.Error(err, "Failed to update shard pod roles")
		}
	}
}

const (
	postureStrikeThreshold      = 2
	postureDebounceRequeueDelay = 5 * time.Second
)

func (r *ShardReconciler) reconcilePosture(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
) (bool, error) {
	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(lbls),
	); err != nil {
		return false, fmt.Errorf("list pods for posture reconciliation: %w", err)
	}
	podNames := make([]string, len(podList.Items))
	for i := range podList.Items {
		podNames[i] = podList.Items[i].Name
	}

	result, err := posture.Evaluate(ctx, store, r.RPCClient, shard, podNames)
	if err != nil {
		r.Recorder.Eventf(shard, "Warning", "PostureCheckFailed",
			"Failed to check postgres posture consistency: %v", err)
		return false, fmt.Errorf("evaluate posture consistency: %w", err)
	}
	if result == nil {
		return false, nil
	}

	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	inconsistent := result.MultiplePrimaries || len(result.Mismatches) > 0
	if inconsistent || !result.Incomplete {
		monitoring.SetShardPostureInconsistent(
			clusterName,
			shard.Name,
			shard.Namespace,
			inconsistent,
		)
	}

	strikes := r.recordPostureObservation(shard, inconsistent)

	prevFalse := status.IsConditionFalse(shard.Status.Conditions, posture.ConditionConsistent)

	if !inconsistent || strikes >= postureStrikeThreshold {
		posture.Apply(shard, result)
	} else {
		shard.Status.PodPostures = result.Postures
	}

	if !prevFalse && status.IsConditionFalse(shard.Status.Conditions, posture.ConditionConsistent) {
		reason := "RolePostureMismatch"
		if result.MultiplePrimaries {
			reason = "MultiplePrimariesDetected"
		}
		r.Recorder.Eventf(shard, "Warning", reason, result.Message)
	}

	return inconsistent && strikes < postureStrikeThreshold, nil
}

func withPostureRequeue(result ctrl.Result, pending bool) ctrl.Result {
	if pending && (result.RequeueAfter == 0 || result.RequeueAfter > postureDebounceRequeueDelay) {
		result.RequeueAfter = postureDebounceRequeueDelay
	}
	return result
}

func (r *ShardReconciler) recordPostureObservation(
	shard *multigresv1alpha1.Shard,
	inconsistent bool,
) int {
	key := fmt.Sprintf("%s/%s", shard.Namespace, shard.Name)

	r.postureStrikesMu.Lock()
	defer r.postureStrikesMu.Unlock()
	if r.postureStrikes == nil {
		r.postureStrikes = make(map[string]int)
	}
	if inconsistent {
		r.postureStrikes[key]++
	} else {
		r.postureStrikes[key] = 0
	}
	return r.postureStrikes[key]
}

// reconcileDrainState iterates pods with drain annotations and runs the
// drain state machine for each one.
func (r *ShardReconciler) reconcileDrainState(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
) (bool, error) {
	logger := log.FromContext(ctx)

	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}
	podList := &corev1.PodList{}
	if err := r.List(
		ctx,
		podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(lbls),
	); err != nil {
		logger.Error(err, "Failed to list pods for drain state machine")
		return false, err
	}

	requeue := false
	for i := range podList.Items {
		pod := &podList.Items[i]
		state := pod.Annotations[metadata.AnnotationDrainState]
		if state == "" {
			continue
		}

		if r.isDrainStale(shard, pod, state) {
			logger.Info("Cancelling stale drain: pod is within desired replicas and spec matches",
				"pod", pod.Name, "state", state)
			if err := clearDrainAnnotations(ctx, r.Client, pod); err != nil {
				logger.Error(err, "Failed to clear drain annotations", "pod", pod.Name)
			}
			r.Recorder.Eventf(shard, "Normal", "DrainCancelled",
				"Cancelled stale drain on pod %s (now within desired state)", pod.Name)
			continue
		}

		shouldRequeue, derr := drain.ExecuteDrainStateMachine(
			ctx, r.Client, r.Recorder, shard, pod,
		)
		if derr != nil {
			logger.Error(derr, "Failed to execute drain state machine", "pod", pod.Name)
		}
		if shouldRequeue {
			requeue = true
		}
	}

	return requeue, nil
}

// isDrainStale returns true when a pod's drain is no longer needed because the
// desired state has changed (e.g. scale-down reversed or rolling-update reverted).
// Only the requested state is cancellable.
func (r *ShardReconciler) isDrainStale(
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	state string,
) bool {
	// Only cancel Requested — nothing has happened yet at this point.
	if state != metadata.DrainStateRequested {
		return false
	}

	// Pods being deleted need the drain to complete.
	if !pod.DeletionTimestamp.IsZero() {
		return false
	}

	// A drain on a DRAINED pod comes from external deletion (kubectl delete),
	// which also sets DeletionTimestamp (handled above). If we somehow reach
	// here with a DRAINED pod in requested state without a DeletionTimestamp,
	// the drain should still complete — it should never be cancelled.
	if resolvePodRole(shard, pod.Name) == "DRAINED" {
		return false
	}

	poolName := pod.Labels[metadata.LabelMultigresPool]
	cellName := pod.Labels[metadata.LabelMultigresCell]
	if poolName == "" || cellName == "" {
		return false
	}

	poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	if !ok {
		return false
	}

	replicas := DefaultPoolReplicas
	if poolSpec.ReplicasPerCell != nil {
		replicas = *poolSpec.ReplicasPerCell
	}

	index, ok := resolvePodIndex(pod.Name)
	if !ok || index >= int(replicas) {
		return false // Pod is still an extra pod for scale-down
	}

	// Pod is within replica range — check if its spec still matches desired.
	return !podNeedsUpdate(pod, shard, poolName, cellName, poolSpec, index, r.Scheme)
}

// topoStore returns a topology store, using the custom factory if set, otherwise the default.
func (r *ShardReconciler) topoStore(shard *multigresv1alpha1.Shard) (topoclient.Store, error) {
	if r.CreateTopoStore != nil {
		return r.CreateTopoStore(shard)
	}
	return topo.NewStoreFromShard(shard)
}

// reconcilePoolerPrune lists active pods for the shard and marks topology
// entries for poolers that no longer have a running pod as LIFECYCLE_SHUTDOWN,
// so the orchestrator clears them from the cohort. This is skipped when the
// parent cluster has disabled topology pruning (propagated via the Cell's
// TopologyReconciliation.PrunePoolers field).
func (r *ShardReconciler) reconcilePoolerPrune(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
) {
	logger := log.FromContext(ctx)

	if !isPoolerPruningEnabled(shard) {
		return
	}

	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}
	podList := &corev1.PodList{}
	if err := r.List(
		ctx,
		podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(lbls),
	); err != nil {
		logger.Error(err, "Failed to list pods for pooler pruning")
		return
	}

	activePodNames := make(map[string]bool, len(podList.Items))
	for _, pod := range podList.Items {
		activePodNames[pod.Name] = true
	}

	marked, err := topo.MarkDeadPoolers(ctx, store, shard, activePodNames)
	if err != nil {
		logger.Error(err, "Failed to mark dead poolers shut down")
	}
	if marked > 0 {
		r.Recorder.Eventf(shard, "Normal", "DeadPoolersMarked",
			"Marked %d dead pooler(s) LIFECYCLE_SHUTDOWN in topology", marked)
	}
}

// hasPrimary returns true if at least one pod in podRoles has the PRIMARY role.
func hasPrimary(podRoles map[string]string) bool {
	for _, role := range podRoles {
		if role == "PRIMARY" {
			return true
		}
	}
	return false
}

// isPoolerPruningEnabled returns true when topology pruning is enabled for
// the shard. The setting is inherited from MultigresCluster via the
// TableGroup builder. Defaults to true when unset.
func isPoolerPruningEnabled(shard *multigresv1alpha1.Shard) bool {
	if shard.Spec.TopologyPruning == nil || shard.Spec.TopologyPruning.Enabled == nil {
		return true
	}
	return *shard.Spec.TopologyPruning.Enabled
}
