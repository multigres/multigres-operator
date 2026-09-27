package shard

import (
	"context"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
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
	store, err := r.topoStore(ctx, shard)
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

	// Resolve the pooler RPC client once for every RPC phase. A
	// resolution failure (e.g. operator client cert not issued yet) is not a
	// reconcile error: skip RPC phases, continue topology maintenance, and retry
	// shortly.
	var rpcClient rpcclient.MultipoolerClient
	poolerClientUnavailable := false
	if r.PoolerClients != nil {
		var err error
		rpcClient, err = r.PoolerClients.ClientFor(ctx, shard)
		if err != nil {
			previous := meta.FindStatusCondition(
				shard.Status.Conditions,
				posture.ConditionConsistent,
			)
			enteringUnavailable := previous == nil ||
				previous.Status != metav1.ConditionUnknown ||
				previous.Reason != reasonPoolerClientUnavailable

			// A resolver error breaks the sequence of posture observations. It must
			// not let a strike from before the transport outage combine with the
			// first unsettled observation after recovery. notConvergedSince needs no
			// matching reset here: this path (like the nil-topology one below)
			// returns its own fixed delay independent of that map, so a stale entry
			// left over from before the outage only shortens the path to the
			// backoff's ceiling once posture resumes, it never suppresses a requeue
			// that is actually owed. That is intentional, not a gap.
			r.recordPostureObservation(shard, false)

			// A partial observation cannot clear a confirmed posture failure. Keep a
			// split-brain shard Degraded while the RPC client is unavailable; for all
			// other prior states, report that posture cannot currently be observed.
			rpcClient = nil
			poolerClientUnavailable = true
			if readinessErr := r.reconcilePoolerReadiness(ctx, shard, nil); readinessErr != nil {
				return ctrl.Result{}, readinessErr
			}
			setPostureUnknownUnlessFalse(
				shard,
				reasonPoolerClientUnavailable,
				fmt.Sprintf("Failed to build multipooler RPC client: %v", err),
			)
			setBackupUnknownUnlessFalse(
				shard,
				fmt.Sprintf("Failed to build multipooler RPC client: %v", err),
			)
			if enteringUnavailable {
				logger.Error(err, "Failed to resolve multipooler RPC client")
				r.Recorder.Eventf(shard, "Warning", reasonPoolerClientUnavailable,
					"Failed to build multipooler RPC client: %v", err)
			} else {
				logger.V(1).Info("Multipooler RPC client is still unavailable", "error", err)
			}
		}
	}

	postureRetryAfter := time.Duration(0)
	if rpcClient != nil {
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcilePosture")
		var err error
		postureRetryAfter, err = r.reconcilePosture(ctx, store, shard, rpcClient)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			return ctrl.Result{}, err
		}
		childSpan.End()

	}

	// Publish the topology-derived pod roles even when the RPC client is not
	// available yet. When posture ran, this also publishes its observation,
	// condition, and resulting phase in the same status update.
	if err := r.updateStatus(ctx, shard, rendered); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status after data-plane observation: %w", err)
	}

	// Requeue when the shard is Healthy but topology hasn't elected a primary
	// yet. This closes a race where the reconciliation burst settles before
	// multiorch writes the primary type to etcd.
	if shard.Status.Phase == multigresv1alpha1.PhaseHealthy &&
		!hasPrimary(shard.Status.PodRoles) {
		logger.Info("No primary in podRoles, requeueing to re-read topology")
		return withDataPlaneRequeue(
			ctrl.Result{RequeueAfter: 10 * time.Second},
			postureRetryAfter,
			poolerClientUnavailable,
		), nil
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
			return withDataPlaneRequeue(
				ctrl.Result{RequeueAfter: quarantineRemediationRequeue},
				postureRetryAfter,
				poolerClientUnavailable,
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
			return withDataPlaneRequeue(
				ctrl.Result{RequeueAfter: 2 * time.Second},
				postureRetryAfter,
				poolerClientUnavailable,
			), nil
		}
	}

	// Phase: Evaluate backup health
	if rpcClient != nil {
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileBackupHealth")
		backupBase := shard.DeepCopy()
		result, err := backuphealth.Evaluate(ctx, store, rpcClient, shard)
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
			setBackupUnknownUnlessFalse(
				shard,
				fmt.Sprintf("Failed to check backup health: %v", err),
			)
			if patchErr := r.Status().Patch(
				ctx,
				shard,
				client.MergeFrom(backupBase),
				client.FieldOwner("multigres-resource-handler"),
			); patchErr != nil {
				return ctrl.Result{}, fmt.Errorf("update unavailable backup status: %w", patchErr)
			}
		} else if result != nil {
			prevHealthy := status.IsConditionTrue(
				shard.Status.Conditions,
				backuphealth.ConditionHealthy,
			)
			backuphealth.Apply(shard, result)

			if result.Healthy && !prevHealthy {
				r.Recorder.Event(shard, "Normal", "BackupHealthy", result.Message)
			} else if !result.Healthy && prevHealthy {
				r.Recorder.Event(shard, "Warning", "BackupStale", result.Message)
			}

			if err := r.Status().Patch(
				ctx,
				shard,
				client.MergeFrom(backupBase),
				client.FieldOwner("multigres-resource-handler"),
			); err != nil {
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
		wait, err := r.reconcileReloadState(ctx, store, shard, rendered, rpcClient)
		childSpan.End()
		if err != nil {
			logger.Error(err, "Failed to reconcile config reload state")
			return ctrl.Result{}, err
		}
		if wait > 0 {
			return withDataPlaneRequeue(
				ctrl.Result{RequeueAfter: wait},
				postureRetryAfter,
				poolerClientUnavailable,
			), nil
		}
	}

	return withDataPlaneRequeue(
		ctrl.Result{},
		postureRetryAfter,
		poolerClientUnavailable,
	), nil
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

	// List managed pool pods for this shard (same pattern as
	// reconcilePoolerPrune). Scoped to pool pods: only they can ever match a
	// topology pooler entry, and a shard's multiorch pod carries the same four
	// identity labels but is never one.
	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
		metadata.LabelAppComponent:        PoolComponentName,
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
		if err := r.Status().Patch(
			ctx,
			shard,
			client.MergeFrom(statusBase),
			client.FieldOwner("multigres-resource-handler"),
		); err != nil {
			logger.Error(err, "Failed to update shard pod roles")
		}
	}
}

const (
	postureStrikeThreshold       = 2
	postureDebounceRequeueDelay  = 5 * time.Second
	poolerRegistrationRetryDelay = time.Minute
	// poolerClientRetryDelay is the requeue delay when the multipooler RPC
	// client cannot be built yet (e.g. operator client cert not issued).
	poolerClientRetryDelay = 10 * time.Second

	// A shard that is not converged, whatever the reason, is requeued on a
	// backoff rather than left to the 10h resync. The delay is clamped elapsed
	// time since the shard was first observed not-converged, not a per-pass
	// count: a burst of unrelated pod/status events must not itself advance
	// the backoff. The floor is the multipooler container's own readiness
	// probe period, so the operator never polls topology more aggressively
	// than the kubelet polls the pod, and the ceiling matches the
	// empty-topology path.
	readinessBackoffMinDelay = 5 * time.Second
	readinessBackoffMaxDelay = time.Minute
	// readinessBackoffJitter adds upward jitter. controller-runtime does not
	// jitter RequeueAfter, and a fleet that lost convergence together
	// (topology restart, mass scale-up) would otherwise retry in lockstep
	// against the topology server that just came back.
	readinessBackoffJitter = 0.2

	reasonPoolerClientUnavailable    = "PoolerClientUnavailable"
	reasonAwaitingPoolerRegistration = "AwaitingPoolerRegistration"
	reasonObservationPending         = "ObservationPending"
	reasonBackupCheckUnavailable     = "BackupCheckUnavailable"
)

func (r *ShardReconciler) reconcilePosture(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
	rpcClient rpcclient.MultipoolerClient,
) (time.Duration, error) {
	// Pool pods only: a shard's multiorch pod carries the same four identity
	// labels (buildMultiorchLabelsWithCell merges them in) but is never a
	// pooler, so an unfiltered list seeds it AwaitingRegistration forever and
	// anyPodNotReady never clears. reconcilePoolerReadiness already scopes
	// this way; posture.Evaluate must match it.
	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
		metadata.LabelAppComponent:        PoolComponentName,
	}
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(lbls),
	); err != nil {
		return 0, fmt.Errorf("list pods for posture reconciliation: %w", err)
	}
	podNames := make([]string, len(podList.Items))
	for i := range podList.Items {
		podNames[i] = podList.Items[i].Name
	}

	result, err := posture.Evaluate(ctx, store, rpcClient, shard, podNames)
	if err != nil {
		r.Recorder.Eventf(shard, "Warning", "PostureCheckFailed",
			"Failed to check postgres posture consistency: %v", err)
		return 0, fmt.Errorf("evaluate posture consistency: %w", err)
	}
	if result == nil {
		if err := r.reconcilePoolerReadiness(ctx, shard, nil); err != nil {
			return 0, err
		}
		// An empty topology is expected during bootstrap, but it is not a settled
		// posture observation. Keep polling until poolers register rather than
		// leaving a previous transport condition stuck until the periodic resync.
		// This path always returns a fixed 1-minute delay of its own regardless of
		// notConvergedSince, so it needs no matching reset there either.
		r.recordPostureObservation(shard, false)
		setPostureUnknownUnlessFalse(
			shard,
			reasonAwaitingPoolerRegistration,
			"Waiting for multipoolers to register in topology",
		)
		return poolerRegistrationRetryDelay, nil
	}
	if err := r.reconcilePoolerReadiness(ctx, shard, result.Readiness); err != nil {
		return 0, err
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

	// A single flaky RPC (dial error, EOF, stale pod) must not be enough to
	// drop a shard out of Healthy any more than a single role mismatch is.
	// Both inconsistent and merely-incomplete observations share the same
	// strike counter and debounce window.
	unsettled := inconsistent || result.Incomplete
	strikes := r.recordPostureObservation(shard, unsettled)

	prevFalse := status.IsConditionFalse(shard.Status.Conditions, posture.ConditionConsistent)

	if !unsettled || strikes >= postureStrikeThreshold {
		posture.Apply(shard, result)
	} else {
		shard.Status.PodPostures = result.Postures
		// Once RPCs recover, do not retain a transport-specific condition reason
		// during the debounce cycle. The observation is available but still needs
		// confirmation before it may change the shard's health.
		if condition := meta.FindStatusCondition(
			shard.Status.Conditions,
			posture.ConditionConsistent,
		); condition != nil && condition.Status == metav1.ConditionUnknown {
			status.SetCondition(&shard.Status.Conditions, metav1.Condition{
				Type:               posture.ConditionConsistent,
				Status:             metav1.ConditionUnknown,
				ObservedGeneration: shard.Generation,
				LastTransitionTime: metav1.Now(),
				Reason:             reasonObservationPending,
				Message:            result.Message,
			})
		}
	}

	if !prevFalse && status.IsConditionFalse(shard.Status.Conditions, posture.ConditionConsistent) {
		reason := "RolePostureMismatch"
		if result.MultiplePrimaries {
			reason = "MultiplePrimariesDetected"
		}
		r.Recorder.Event(shard, "Warning", reason, result.Message)
	}

	// A shard is converged only once posture is settled AND every managed pod
	// has reached posture readiness. Neither half implies the other: an
	// accepted mismatch or accepted Incomplete observation (strikes past
	// threshold) is settled but still has a pod sitting NotReady, and a shard
	// mid-bootstrap with every pooler registered but no primary elected yet is
	// neither inconsistent nor incomplete but has nothing actually ready.
	// Registration itself, and any change multiorch makes in topology, are
	// writes to the topology store with no Kubernetes event behind them, so
	// nothing but this controller's own requeue will ever look again. Without
	// it, a settled-but-not-ready shard sits until controller-runtime's 10h
	// resync; a shard with a genuinely stuck pod (Pending, crash-looping,
	// quarantined) sits forever, since a lost watch never recovers it.
	//
	// The debounce above stays authoritative for whether an unsettled
	// observation is accepted into status: this never fires while
	// `unsettled && strikes < postureStrikeThreshold`, so it cannot preempt or
	// shorten that window, only pick up once it ends.
	notConverged := unsettled || anyPodNotReady(result.Readiness)
	elapsed := r.recordNotConverged(shard, notConverged)

	if unsettled && strikes < postureStrikeThreshold {
		return postureDebounceRequeueDelay, nil
	}
	if notConverged {
		return readinessBackoffDelay(elapsed), nil
	}
	return 0, nil
}

// anyPodNotReady reports whether any managed pod has not yet reached posture
// readiness.
//
// Deliberately broader than a check for the seeded "AwaitingRegistration"
// reason alone. A pod whose pooler has never registered carries that reason,
// but every other reason poolerReadiness can return (NotInitialized,
// PostgresNotReady, CohortIneligible, NotCohortMember) means Ready is false
// too, and all of them are reachable by a shard that Evaluate reports as
// neither inconsistent nor incomplete, since that result only compares
// postures against topology roles and does not require a primary to exist. A
// shard mid-bootstrap with every pooler registered but no primary elected
// settles there, looking "consistent" while nothing is actually ready.
func anyPodNotReady(readiness map[string]posture.Readiness) bool {
	for _, r := range readiness {
		if !r.Ready {
			return true
		}
	}
	return false
}

// readinessBackoffDelay clamps elapsed (time since the shard was first
// observed not-converged) to [readinessBackoffMinDelay,
// readinessBackoffMaxDelay], with upward jitter.
//
// Elapsed-time-based rather than a per-pass count: Owns(&corev1.Pod{}) and
// For(&Shard{}) carry no predicates, so a pod's Pending -> ContainerCreating ->
// per-container-Ready transitions, an operator gate patch, and a drain's 2s
// requeues are each their own reconcile. A scale-up alone is at least five of
// those before there is anything to register, and a wall-clock burst of
// events must not by itself run the delay up to the ceiling: two reconciles a
// second apart are five seconds not-converged either way, whether they were
// one pass or five.
func readinessBackoffDelay(elapsed time.Duration) time.Duration {
	d := elapsed
	if d < readinessBackoffMinDelay {
		d = readinessBackoffMinDelay
	} else if d > readinessBackoffMaxDelay {
		d = readinessBackoffMaxDelay
	}
	return wait.Jitter(d, readinessBackoffJitter)
}

func setPostureUnknownUnlessFalse(
	shard *multigresv1alpha1.Shard,
	reason string,
	message string,
) {
	if status.IsConditionFalse(shard.Status.Conditions, posture.ConditionConsistent) {
		return
	}
	status.SetCondition(&shard.Status.Conditions, metav1.Condition{
		Type:               posture.ConditionConsistent,
		Status:             metav1.ConditionUnknown,
		ObservedGeneration: shard.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})
}

func setBackupUnknownUnlessFalse(shard *multigresv1alpha1.Shard, message string) {
	if status.IsConditionFalse(shard.Status.Conditions, backuphealth.ConditionHealthy) {
		return
	}
	status.SetCondition(&shard.Status.Conditions, metav1.Condition{
		Type:               backuphealth.ConditionHealthy,
		Status:             metav1.ConditionUnknown,
		ObservedGeneration: shard.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reasonBackupCheckUnavailable,
		Message:            message,
	})
}

func withDataPlaneRequeue(
	result ctrl.Result,
	postureRetryAfter time.Duration,
	poolerClientUnavailable bool,
) ctrl.Result {
	if postureRetryAfter > 0 &&
		(result.RequeueAfter == 0 || result.RequeueAfter > postureRetryAfter) {
		result.RequeueAfter = postureRetryAfter
	}
	if poolerClientUnavailable &&
		(result.RequeueAfter == 0 || result.RequeueAfter > poolerClientRetryDelay) {
		result.RequeueAfter = poolerClientRetryDelay
	}
	return result
}

// recordPostureObservation counts consecutive unsettled observations for one
// shard and returns the running total.
//
// A settled observation deletes the entry rather than writing zero. The two
// mean the same thing to every reader, since a missing key reads as zero, but
// they differ in what the map holds: writing zero keeps an entry for every
// shard this process has ever reconciled, while deleting keeps only the
// shards currently accumulating strikes, which in a healthy cluster is none.
// A Go map's table is sized by its peak simultaneous entries and does not
// shrink on delete, so this is what keeps that peak bounded by currently
// unsettled shards rather than by cumulative shards over the operator's
// lifetime.
func (r *ShardReconciler) recordPostureObservation(
	shard *multigresv1alpha1.Shard,
	unsettled bool,
) int {
	key := fmt.Sprintf("%s/%s", shard.Namespace, shard.Name)

	r.postureStrikesMu.Lock()
	defer r.postureStrikesMu.Unlock()
	if !unsettled {
		delete(r.postureStrikes, key)
		return 0
	}
	if r.postureStrikes == nil {
		r.postureStrikes = make(map[string]int)
	}
	r.postureStrikes[key]++
	return r.postureStrikes[key]
}

// recordNotConverged tracks, per shard, the time a shard was first observed
// not converged (unsettled, or some managed pod not posture-ready), and
// returns how long that has been true. Same delete-on-settle pattern as
// recordPostureObservation, and a separate map for the same reason that one
// is not reused here: this one picks the backoff, and posture strikes must
// keep gating only posture.Apply.
//
// Storing the first-seen time rather than a per-pass count is what makes
// readinessBackoffDelay a function of elapsed wall-clock time instead of
// event count; see readinessBackoffDelay's own doc for why that matters.
func (r *ShardReconciler) recordNotConverged(
	shard *multigresv1alpha1.Shard,
	notConverged bool,
) time.Duration {
	key := fmt.Sprintf("%s/%s", shard.Namespace, shard.Name)
	now := r.now()

	r.notConvergedMu.Lock()
	defer r.notConvergedMu.Unlock()
	if !notConverged {
		delete(r.notConvergedSince, key)
		return 0
	}
	if r.notConvergedSince == nil {
		r.notConvergedSince = make(map[string]time.Time)
	}
	first, ok := r.notConvergedSince[key]
	if !ok {
		first = now
		r.notConvergedSince[key] = first
	}
	return now.Sub(first)
}

// now returns the reconciler's clock, defaulting to time.Now so production
// code never has to set Clock. Tests inject Clock to drive
// recordNotConverged's elapsed-time math deterministically.
func (r *ShardReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

// forgetStrikes drops namespace/name's entry from both strike maps. Called on
// a Shard's deletion and not-found paths so a shard deleted mid-backoff, in
// either counter, does not leak its entry for the life of the process.
func (r *ShardReconciler) forgetStrikes(namespace, name string) {
	key := fmt.Sprintf("%s/%s", namespace, name)

	r.postureStrikesMu.Lock()
	delete(r.postureStrikes, key)
	r.postureStrikesMu.Unlock()

	r.notConvergedMu.Lock()
	delete(r.notConvergedSince, key)
	r.notConvergedMu.Unlock()
}

// reconcileDrainState iterates pods with drain annotations and runs the
// drain state machine for each one.
func (r *ShardReconciler) reconcileDrainState(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
) (bool, error) {
	logger := log.FromContext(ctx)

	// Pool pods only: the drain-requested annotation this loop acts on is only
	// ever set by the pool scale-down/rolling-update path (reconcile_pool_pods.go),
	// never on a shard's multiorch pod, so this is currently a no-op filter.
	// Scoped anyway for the same reason reconcilePosture now is: relying on an
	// annotation nothing else sets is a coincidence, not a guarantee.
	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
		metadata.LabelAppComponent:        PoolComponentName,
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
func (r *ShardReconciler) topoStore(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (topoclient.Store, error) {
	if r.CreateTopoStore != nil {
		return r.CreateTopoStore(shard)
	}
	// Read the client credential with the uncached reader: the manager's cache
	// only stores tenant-namespace Secrets carrying the operator's managed-by
	// label, and a cert-manager or user-provided topology Secret does not.
	return topo.NewStoreFromShard(ctx, r.APIReader, shard)
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

	// Pool pods only: topo.MarkDeadPoolers matches this set's names against
	// topology pooler entries, and a multiorch pod's name never matches one
	// (it is not a pooler), so including it here is currently a no-op. Scoped
	// anyway to keep this list's meaning ("pods that can be poolers") aligned
	// with what it is actually used for.
	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
		metadata.LabelAppComponent:        PoolComponentName,
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
