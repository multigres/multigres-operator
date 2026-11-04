package shard

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

const (
	// topoUnavailableRequeueDelay is the delay before retrying when the topology
	// server is unavailable during the grace period.
	topoUnavailableRequeueDelay = 5 * time.Second

	// shardFinalizer gates Shard deletion until the operator has stripped its
	// ownerRef from associated PVCs and labelled them orphan, so the downstream
	// multigres-gc cronjob can clean them up instead of k8s cascade-GC
	// nuking them the moment the Shard CR is deleted.
	shardFinalizer = "multigres.com/shard-pvc-orphan"
)

// clusterIsChurning reports whether the owning MultigresCluster is being
// deleted. Pod/pool scale down and shard removal always orphan their PVCs so
// an accidental change can be rolled back within the retention window, but
// that protection is pointless once the whole cluster is being torn down, so
// PVCs are hard deleted immediately in that case.
func (r *ShardReconciler) clusterIsChurning(
	ctx context.Context,
	namespace, clusterName string,
) (bool, error) {
	if clusterName == "" {
		return false, nil
	}
	cluster := &multigresv1alpha1.MultigresCluster{}
	err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: clusterName}, cluster)
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get MultigresCluster %s: %w", clusterName, err)
	}
	return !cluster.DeletionTimestamp.IsZero(), nil
}

// ShardReconciler reconciles a Shard object.
type ShardReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// APIReader is an uncached client that reads directly from the API server.
	// The default cached client (r.Get) only sees Secrets labeled with
	// "app.kubernetes.io/managed-by: multigres-operator" due to the informer
	// cache's label filter. External Secrets (e.g., cert-manager) lack this
	// label, so we need APIReader to validate user-provided pgBackRest TLS Secrets
	// and external postgres password Secrets.
	APIReader client.Reader
	// LitePodPriorityClassName applies only to single-pod Lite shards.
	LitePodPriorityClassName string
	// PgBackRestSandbox is the --sandbox mode every pgbackrestd container is
	// started with: required, auto or off. Empty means required. It is an
	// operator-wide deployment property, not part of the Shard spec.
	PgBackRestSandbox string
	PoolerClients     poolerclient.Resolver
	CreateTopoStore   func(*multigresv1alpha1.Shard) (topoclient.Store, error)
	// Clock overrides time.Now for everything the reconciler times against
	// its own records: recordNotConverged's elapsed-time math, the wait
	// between two creates of an object a quota turned down, and the schedule on
	// which a Lite shard asks again about an unanswered replacement. Nil in
	// production; tests inject it to drive those deterministically.
	Clock func() time.Time
	// SidecarStartupDeadline, when positive, replaces the age a pod must reach
	// before sidecarStuckPod may nominate it (otherwise the sidecar's startup
	// probe budget plus an image pull allowance, about seven minutes). Nil
	// effect in production; the suite sets it because envtest cannot backdate a
	// pod's creationTimestamp.
	SidecarStartupDeadline time.Duration

	// Gate pauses objects labelled multigres.com/reconcile=disabled, see
	// docs/development/reconcile-gating.md. The zero value is safe.
	Gate features.Gate

	// DialContext carries the topology store's connections. Nil (direct
	// dialing) outside laptop mode.
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)

	// UnregisteredPodGrace is how long a scheduled pool Pod that is not Ready
	// may go without registering in topology before a disruption may remove
	// it unregistered. Zero means defaultUnregisteredPodGrace.
	UnregisteredPodGrace time.Duration

	postureStrikesMu sync.Mutex
	postureStrikes   map[string]int

	// notConvergedMu and notConvergedSince record, per shard, the time it was
	// first observed not converged: unsettled (posture debounce past
	// threshold, i.e. an accepted mismatch or Incomplete observation), or some
	// managed pod not yet posture-ready. Kept separate from postureStrikes,
	// which gates posture.Apply: folding this into that counter would change
	// when Apply fires.
	notConvergedMu    sync.Mutex
	notConvergedSince map[string]time.Time

	synced ownership.SyncedEvents

	// disruptionLoggedAt records, per target pod, when its blocked disruption
	// was last logged, so the log repeats at a bounded cadence.
	disruptionLogMu    sync.Mutex
	disruptionLoggedAt map[string]time.Time

	// backupCheckWarned holds, per shard ("namespace/name"), the message of
	// the last backup check failure reported with an event; see
	// reconcileBackupHealth.
	backupCheckMu     sync.Mutex
	backupCheckWarned map[string]string

	// quotaWaiting is the Shards with a create a ResourceQuota turned
	// down, for status.
	quotaWaiting quotaWaiters
	// liteUnanswered spaces out the dry runs of Lite shards that got no
	// answer.
	liteUnanswered unansweredAsks
}

// deferredCreateResult is how a pass ends whose create of a desired pod or
// claim failed and was carried to its end (see deferCreateFailure), after
// everything else succeeded.
//
// A requeue the pass asked for is when it has to look again: at a drain in
// progress, or at a disruption preflight that refused (waitingForRecovery,
// which Reconcile turns into a requeue), and no event reports that
// preflight passing. Failing the pass would replace that requeue with the
// rate limiter's delay, which after a long stall is many minutes, in exactly
// the pass that is trying to make room. The create is tried again on the
// requeued pass. With no requeue pending the pass fails with the create's
// error, so that the rate limiter retries it.
func deferredCreateResult(
	result ctrl.Result,
	waitingForRecovery bool,
	deferred error,
) (ctrl.Result, error) {
	if result.RequeueAfter > 0 || waitingForRecovery {
		return result, nil
	}
	return ctrl.Result{}, deferred
}

// endPass files what the pass learned in the reconciler's per-Shard
// records. A Shard with a create turned down by a ResourceQuota, or not sent
// because its last one was, stays in the waiter set whatever the pass
// returned; a pass that ended without an error and without such a create
// takes it out. A pass that ended without an error
// and had no question about a replacement has no unanswered question left to
// space out.
//
// waitEnded is whether the pass took the Shard out of the waiter set. The
// pass wrote status before that, with the wait in it (see setConditions),
// and a wait can end without anything that reconciles the Shard again: the
// create is no longer wanted, as when a maintenance surge is withdrawn, and
// every pod is Ready.
func (r *ShardReconciler) endPass(
	ctx context.Context,
	shard types.NamespacedName,
	err error,
) (waitEnded bool) {
	pass, _ := ctx.Value(replacementPassKey{}).(*replacementPass)
	if pass == nil {
		return false
	}
	if err == nil && !pass.quotaWait {
		waitEnded = r.quotaWaiting.clear(shard)
	} else {
		r.quotaWaiting.prune(shard, pass.quotaReached, r.now())
	}
	if err == nil && !pass.asked {
		r.liteUnanswered.note(shard, "", time.Time{}, nil)
	}
	return waitEnded
}

// quotaWaitEndedRequeue is how soon a Shard is reconciled again after the
// pass that ended its quota wait, so that status stops naming the wait.
const quotaWaitEndedRequeue = time.Second

// afterQuotaWait returns result with a requeue added when the pass ended
// the Shard's quota wait and asked for none itself. One more pass follows,
// which finds the Shard not waiting and asks for nothing.
func afterQuotaWait(result ctrl.Result, waitEnded bool) ctrl.Result {
	if waitEnded && result.RequeueAfter == 0 {
		result.RequeueAfter = quotaWaitEndedRequeue
	}
	return result
}

// Reconcile manages pool pods, PVCs, services, and data-plane topology for a Shard.
func (r *ShardReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (result ctrl.Result, err error) {
	start := time.Now()
	ctx, span := monitoring.StartReconcileSpan(
		ctx,
		"Shard.Reconcile",
		req.Name,
		req.Namespace,
		"Shard",
	)
	defer span.End()
	ctx = withReplacementPass(ctx)
	defer func() { result = afterQuotaWait(result, r.endPass(ctx, req.NamespacedName, err)) }()
	defer func() { monitoring.RecordReconcileError(err, "shard", req.Name, req.Namespace) }()
	ctx = monitoring.EnrichLoggerWithTrace(ctx)

	logger := log.FromContext(ctx)
	logger.V(1).Info("Reconcile started for shard", "shard", req.Name)

	// Fetch the Shard instance
	shard := &multigresv1alpha1.Shard{}
	if err := r.Get(ctx, req.NamespacedName, shard); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Shard resource not found, ignoring")
			r.synced.Forget(req.NamespacedName)
			r.forgetStrikes(req.Namespace, req.Name)
			monitoring.ForgetShard(req.Name, req.Namespace)
			return ctrl.Result{}, nil
		}
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to get Shard")
		return ctrl.Result{}, err
	}
	r.synced.Observe(shard, shard.Status.ObservedGeneration)

	// Backstop for the event filter: a RequeueAfter scheduled before the label
	// went on still reaches here.
	if gated, err := r.Gate.Gated(ctx, r.Client, shard); gated || err != nil {
		return ctrl.Result{}, err
	}

	// Finalizer gates deletion so PVCs can be detached + labelled orphan
	// before Kubernetes cascade-GC fires.
	if !shard.DeletionTimestamp.IsZero() {
		monitoring.ForgetShardPoolPodsStuck(shard.Name, shard.Namespace)
		return r.handleDeletion(ctx, shard)
	}
	if !slices.Contains(shard.Finalizers, shardFinalizer) {
		patch := client.MergeFrom(shard.DeepCopy())
		shard.Finalizers = append(shard.Finalizers, shardFinalizer)
		if err := r.Patch(ctx, shard, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add shard finalizer: %w", err)
		}
	}

	// Handle graceful deletion via PendingDeletion annotation.
	// The TableGroup controller sets this annotation when a shard is removed from
	// spec. The shard controller drains all pods and sets ReadyForDeletion
	// condition, at which point the TableGroup controller calls Delete.
	if shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" {
		monitoring.ForgetShardPoolPodsStuck(shard.Name, shard.Namespace)
		return r.handlePendingDeletion(ctx, shard)
	}

	// Registered here, after the returns of a gated Shard and of one being
	// deleted or drained: none of those passes asks the API server about the
	// refused pod, so a recheck would only come back to the same return. A
	// gated Shard is reconciled when its gate is lifted.
	defer func() {
		if err == nil {
			result = afterRefusal(result, shard)
		}
	}()

	// Render the effective postgresql.conf once per reconcile. Both the status
	// path (drift detection) and the ConfigMap-delivery path below consume this
	// single result, so the ref ConfigMap is fetched and the template rendered
	// exactly once. A render/read error is reported softly in status here and
	// then fails the reconcile at reconcilePostgresConfig below.
	renderedCfg := r.renderEffectiveConfig(ctx, shard)

	// Update status early so observedGeneration and pod-health phase are
	// always current. Later steps (especially reconcileDataPlane) may block
	// on the topo connection for an extended period; placing updateStatus
	// here guarantees the status subresource is written every reconcile.
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.UpdateStatus")
		if err := r.updateStatus(ctx, shard, renderedCfg); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to update status")
			r.Recorder.Eventf(shard, "Warning", "StatusError", "Failed to update status: %v", err)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Reconcile pg_hba ConfigMap first (required by all pools before starting)
	if err := r.reconcilePgHbaConfigMap(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile pg_hba ConfigMap")
		r.Recorder.Eventf(shard, "Warning", "ConfigError", "Failed to generate pg_hba: %v", err)
		return ctrl.Result{}, err
	}

	// Reconcile the postgres_exporter custom queries ConfigMap (mounted by the
	// exporter sidecar in every pool pod).
	if err := r.reconcilePostgresExporterQueriesConfigMap(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile postgres_exporter queries ConfigMap")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ConfigError",
			"Failed to generate postgres_exporter queries: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Reconcile postgres password Secret (required by pgctld and multipooler)
	if err := r.reconcilePostgresPasswordSecret(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile postgres password Secret")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ConfigError",
			"Failed to reconcile postgres password Secret: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	if err := r.reconcilePostgresInitSecretsSecret(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile postgres init-secrets Secret")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ConfigError",
			"Failed to reconcile postgres init-secrets Secret: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// A shard whose sidecar cannot serve the configured repository gets no
	// multiorch, claims or pods, and none of its pods is replaced.
	// updateStatus above has already set BackupRepositoryValid=False with
	// the reason and emitted the event. Only a change to the Shard clears
	// the refusal, and that change starts the next reconcile, so the refusal
	// itself is not retried. What still runs is what the pods already
	// running depend on, or what the spec says must not be lost.
	if err := validateSidecarLocation(shard); err != nil {
		logger.Info("Not reconciling shard resources: pgBackRest sidecar is refused",
			"reason", err.Error())
		// The one thing a refused shard still does with its spec is let go
		// of PVCs the spec says to keep. Its claims are garbage-collected
		// with it for as long as it owns them, and an owner who cannot
		// repair the shard sets Retain before deleting it. The other
		// direction, taking ownership for a Delete policy, waits until the
		// shard is accepted: it is what makes a claim deletable.
		if releaseErr := r.syncPVCOwnerRefs(ctx, shard, false); releaseErr != nil {
			monitoring.RecordSpanError(span, releaseErr)
			logger.Error(releaseErr, "Failed to release the PVCs of a refused shard")
			r.Recorder.Eventf(shard, "Warning", "PVCOwnerRefError",
				"Failed to reconcile PVC ownerRefs: %v", releaseErr)
			return ctrl.Result{}, releaseErr
		}
		// The pods that are running mount the pgBackRest certificate, and
		// it expires on its own schedule whatever the spec says.
		if certErr := r.reconcilePgBackRestCerts(ctx, shard); certErr != nil {
			monitoring.RecordSpanError(span, certErr)
			logger.Error(certErr, "Failed to reconcile pgBackRest TLS certificates")
			r.Recorder.Eventf(shard, "Warning", "CertError",
				"Failed to reconcile pgBackRest TLS certificates: %v", certErr)
			return ctrl.Result{}, certErr
		}
		observeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return r.observeRefusedShard(observeCtx, shard, renderedCfg)
	}

	// Reconcile pgBackRest TLS certificates (required for inter-node backup communication)
	if err := r.reconcilePgBackRestCerts(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile pgBackRest TLS certificates")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"CertError",
			"Failed to reconcile pgBackRest TLS certificates: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Reconcile pgBackRest backup encryption cipher key Secret (required when
	// client-side backup encryption is enabled)
	if err := r.reconcileBackupCipherSecret(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile pgBackRest cipher key Secret")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"CipherKeyError",
			"Failed to reconcile pgBackRest cipher key Secret: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Compute Multiorch cells
	multiorchCells, err := getMultiorchCells(shard)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to determine Multiorch cells")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ConfigError",
			"Failed to determine Multiorch cells: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Every StorageClass the shard references is resolved here, in one pass, and
	// the StorageClassValid condition is published once. Two call sites used to
	// write that condition with their own message and overwrite each other on
	// every reconcile. The gates stay where they were: this one stops before
	// the shared backup PVC, the pool one further down stops before the
	// workloads that consume pool storage.
	storageClasses, err := r.validateStorageClassDependencies(ctx, shard)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to validate StorageClass dependencies")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"FailedApply",
			"Failed to validate StorageClass dependencies: %v",
			err,
		)
		return ctrl.Result{}, err
	}
	if err := r.setStorageClassCondition(ctx, shard, storageClasses); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to set StorageClass condition")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"FailedApply",
			"Failed to set StorageClass condition: %v",
			err,
		)
		return ctrl.Result{}, err
	}
	backupGated, err := r.backupStorageClassGated(ctx, shard, storageClasses)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to check the shared backup PVC")
		return ctrl.Result{}, err
	}
	if backupGated {
		r.Recorder.Event(shard, "Warning", storageClassNotFoundReason, storageClasses.message)
		logger.Info(
			"StorageClass dependency missing for shared backup PVC; requeueing",
			"after",
			storageClassDependencyRequeue,
		)
		return ctrl.Result{RequeueAfter: storageClassDependencyRequeue}, nil
	}

	// A Lite shard skips everything below: multiorch, the PDB, pool rollouts
	// and the data-plane phase.
	if shard.Spec.Mode == multigresv1alpha1.ModeLite {
		return r.reconcileLite(ctx, shard, renderedCfg, storageClasses)
	}

	// Reconcile Multiorch - one Deployment and Service per cell
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileMultiorch")
		for _, cell := range multiorchCells {
			cellName := string(cell)

			// Reconcile Multiorch Deployment for this cell
			if err := r.reconcileMultiorchDeployment(ctx, shard, cellName); err != nil {
				monitoring.RecordSpanError(childSpan, err)
				childSpan.End()
				logger.Error(err, "Failed to reconcile Multiorch Deployment", "cell", cellName)
				r.Recorder.Eventf(
					shard,
					"Warning",
					"FailedApply",
					"Failed to supply Multiorch Deployment for cell %s: %v",
					cellName,
					err,
				)
				return ctrl.Result{}, err
			}

			// Reconcile Multiorch Service for this cell
			if err := r.reconcileMultiorchService(ctx, shard, cellName); err != nil {
				monitoring.RecordSpanError(childSpan, err)
				childSpan.End()
				logger.Error(err, "Failed to reconcile Multiorch Service", "cell", cellName)
				r.Recorder.Eventf(
					shard,
					"Warning",
					"FailedApply",
					"Failed to supply Multiorch Service for cell %s: %v",
					cellName,
					err,
				)
				return ctrl.Result{}, err
			}
		}
		childSpan.End()
	}

	// Shared across every pool in this reconcile pass so that once one
	// pool initiates a drain, no other pool starts one too in the same
	// pass
	rollout := &shardRolloutTracker{}

	// Reconcile the shard-wide shared backup PVC.
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileBackupPVCs")
		if err := r.reconcileSharedBackupPVC(ctx, shard); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile shared backup PVC")
			r.Recorder.Eventf(
				shard,
				"Warning",
				"FailedApply",
				"Failed to reconcile shared backup PVC: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	if isMissingStorageClassDependency(storageClasses.poolDependency) {
		r.Recorder.Event(shard, "Warning", storageClassNotFoundReason, storageClasses.message)
		logger.Info(
			"StorageClass dependency missing for pool resources; requeueing",
			"after",
			storageClassDependencyRequeue,
		)
		return ctrl.Result{RequeueAfter: storageClassDependencyRequeue}, nil
	}

	// Render the effective postgres config into the operator-owned ConfigMap and
	// stamp a content hash for rolling update detection. The hash is set as an
	// in-memory annotation on the shard so that BuildPoolPod can propagate it to
	// each pod without extra parameters.
	if err := r.reconcilePostgresConfig(ctx, shard, renderedCfg); err != nil {
		logger.Error(err, "Failed to reconcile postgres config")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ConfigError",
			"Failed to reconcile postgres config: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// deferredCreate is a failed create of a desired pod in a pool/cell that
	// has a pod to scale down. The data-plane phases still run, because they
	// are what carries that scale-down's drain to its end, and the pass then
	// fails with this error so that it is retried.
	var deferredCreate error
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcilePools")
		if err := r.reconcileShardPDB(ctx, shard); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile shard PDB")
			r.Recorder.Eventf(
				shard,
				"Warning",
				"FailedApply",
				"Failed to reconcile shard PDB: %v",
				err,
			)
			return ctrl.Result{}, err
		}

		defer func() {
			// Consensus changes do not generate Kubernetes watch events.
			// A refusal that has not changed since the last pass is waiting
			// on something slower, such as multiorch evicting a dead member
			// or a human, and re-observing every pooler every few seconds
			// would only add load. Pod changes still trigger a reconcile.
			requeue := disruptionRecoveryRequeue
			if rollout.blockedAgain {
				requeue = repeatedRefusalRequeue
			}
			if err == nil && rollout.waitingForRecovery &&
				(result.RequeueAfter == 0 || result.RequeueAfter > requeue) {
				result.RequeueAfter = requeue
			}
		}()
		// A status that changes from pass to pass without the state changing
		// makes the Shard's own watch requeue it at once. Pools are walked in
		// Go map order, so the RollingUpdate condition is written once, by
		// finishRollingUpdateCondition, from what every pool/cell noted, and
		// its transition time follows only a change in the final status.
		var rollingBefore *metav1.Condition
		if c := meta.FindStatusCondition(shard.Status.Conditions, "RollingUpdate"); c != nil {
			rollingBefore = c.DeepCopy()
		}
		for poolName, pool := range shard.Spec.Pools {
			if err := r.reconcilePool(ctx, shard, string(poolName), pool, rollout); err != nil {
				monitoring.RecordSpanError(childSpan, err)
				childSpan.End()
				logger.Error(err, "Failed to reconcile pool", "poolName", poolName)
				r.Recorder.Eventf(
					shard,
					"Warning",
					"FailedApply",
					"Failed to reconcile pool %s: %v",
					poolName,
					err,
				)
				return ctrl.Result{}, err
			}
		}
		finishRollingUpdateCondition(&shard.Status.Conditions, rollout, rollingBefore)
		deferredCreate = rollout.createFailure
		childSpan.End()
	}

	// Ensure PVC ownerRefs match the current PVCDeletionPolicy.
	// This handles mid-lifecycle policy changes (e.g., Retain → Delete).
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcilePVCOwnerRefs")
		if err := r.reconcilePVCOwnerRefs(ctx, shard); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile PVC ownerRefs")
			r.Recorder.Eventf(
				shard,
				"Warning",
				"PVCOwnerRefError",
				"Failed to reconcile PVC ownerRefs: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Data-plane phases: open a single topo connection shared across all phases.
	// Use a deadline so a hanging topo dial cannot block the reconcile
	// goroutine indefinitely, which would prevent future reconciles for
	// this shard and leave observedGeneration stale.
	dataPlaneCtx, dataPlaneCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dataPlaneCancel()
	result, err = r.reconcileDataPlane(dataPlaneCtx, shard, renderedCfg)
	if err == nil && deferredCreate != nil {
		monitoring.RecordSpanError(span, deferredCreate)
		logger.Error(deferredCreate, "Failed to create a pool pod or claim")
		r.Recorder.Eventf(shard, "Warning", "FailedApply",
			"Failed to reconcile pool: %v", deferredCreate)
		return deferredCreateResult(result, rollout.waitingForRecovery, deferredCreate)
	}
	if err == nil && !shard.Status.PoolsReady &&
		(result.RequeueAfter == 0 || result.RequeueAfter > stuckPodsRefreshInterval) {
		result.RequeueAfter = stuckPodsRefreshInterval
	}
	if err != nil || result.RequeueAfter > 0 {
		return result, err
	}

	logger.V(1).Info("reconcile complete", "duration", time.Since(start).String())
	if r.synced.Completed(shard) {
		r.Recorder.Event(shard, "Normal", "Synced", "Successfully reconciled Shard")
	}
	return ctrl.Result{}, nil
}

// reconcilePool creates or updates the Pods, PVCs and headless Service for a pool.
// For pools spanning multiple cells, this creates resources per cell.
func (r *ShardReconciler) reconcilePool(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	poolSpec multigresv1alpha1.PoolSpec,
	rollout *shardRolloutTracker,
) error {
	// Pools must have cells specified
	if len(poolSpec.Cells) == 0 {
		return fmt.Errorf(
			"pool %s has no cells specified - cannot deploy without cell information",
			poolName,
		)
	}

	// Create Pods and PVCs per cell
	for _, cell := range poolSpec.Cells {
		cellName := string(cell)

		// Reconcile pool Pods and PVCs for this cell
		if err := r.reconcilePoolPods(
			ctx,
			shard,
			poolName,
			cellName,
			poolSpec,
			rollout,
		); err != nil {
			return fmt.Errorf("failed to reconcile pool pods for cell %s: %w", cellName, err)
		}

		// Reconcile pool headless Service for this cell
		if err := r.reconcilePoolHeadlessService(
			ctx,
			shard,
			poolName,
			cellName,
			poolSpec,
		); err != nil {
			return fmt.Errorf(
				"failed to reconcile pool headless Service for cell %s: %w",
				cellName,
				err,
			)
		}
	}

	return nil
}

// getMultiorchCells returns the list of cells where Multiorch should be deployed.
// If Multiorch.Cells is specified, it uses that.
// Otherwise, it infers cells from all pools (union of pool cells).
func getMultiorchCells(shard *multigresv1alpha1.Shard) ([]multigresv1alpha1.CellName, error) {
	cells := shard.Spec.Multiorch.Cells

	// If Multiorch specifies cells explicitly, use them
	if len(cells) > 0 {
		return cells, nil
	}

	// Otherwise, collect unique cells from all pools
	cellSet := make(map[multigresv1alpha1.CellName]bool)
	for _, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			cellSet[cell] = true
		}
	}

	// Convert set to slice
	cells = make([]multigresv1alpha1.CellName, 0, len(cellSet))
	for cell := range cellSet {
		cells = append(cells, cell)
	}

	// If still no cells found, error
	if len(cells) == 0 {
		return nil, fmt.Errorf(
			"multiorch has no cells specified and no cells found in pools - cannot deploy without cell information",
		)
	}

	slices.Sort(cells)
	return cells, nil
}

// ShouldDeletePVCOnShardRemoval returns true when the effective PVCDeletionPolicy
// for a pool resolves to Delete. Used by PVC builders to conditionally set
// a controller ownerRef so Kubernetes GC cascade-deletes the PVC with the Shard.
func ShouldDeletePVCOnShardRemoval(
	shard *multigresv1alpha1.Shard,
	poolSpec multigresv1alpha1.PoolSpec,
) bool {
	policy := multigresv1alpha1.MergePVCDeletionPolicy(
		poolSpec.PVCDeletionPolicy,
		shard.Spec.PVCDeletionPolicy,
	)
	return policy != nil && policy.WhenDeleted == multigresv1alpha1.DeletePVCRetentionPolicy
}

// ShouldDeleteShardLevelPVCOnRemoval returns true when the shard-level
// PVCDeletionPolicy resolves to Delete. Used for shared infrastructure PVCs
// (e.g., backup PVCs) that are not pool-specific.
func ShouldDeleteShardLevelPVCOnRemoval(shard *multigresv1alpha1.Shard) bool {
	p := shard.Spec.PVCDeletionPolicy
	return p != nil && p.WhenDeleted == multigresv1alpha1.DeletePVCRetentionPolicy
}

// reconcilePVCOwnerRefs ensures ownerRefs on existing PVCs match the current
// PVCDeletionPolicy. When policy is Delete, a controller ownerRef is added so
// Kubernetes GC cascade-deletes PVCs with the Shard. When policy is Retain,
// any existing controller ownerRef is removed so PVCs survive Shard deletion.
// Only PVCs the shard's builders name get or keep the ownerRef; see
// isShardBuiltPVC.
func (r *ShardReconciler) reconcilePVCOwnerRefs(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	return r.syncPVCOwnerRefs(ctx, shard, true)
}

// syncPVCOwnerRefs is reconcilePVCOwnerRefs when adopt is set. Without it
// only the ownerRefs the policy no longer asks for are removed, and none is
// added.
func (r *ShardReconciler) syncPVCOwnerRefs(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	adopt bool,
) error {
	logger := log.FromContext(ctx)

	selector := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(
		ctx,
		pvcList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return fmt.Errorf("failed to list PVCs for ownerRef reconciliation: %w", err)
	}

	shardUID := shard.GetUID()

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		// Skip PVCs already marked orphan. Their Shard ownerRef was deliberately
		// stripped so the multigres-gc cronjob owns the delayed deletion.
		if pvcutil.HasOrphanLabel(pvc) {
			continue
		}

		hasOwnerRef := false
		for _, ref := range pvc.OwnerReferences {
			if ref.UID == shardUID {
				hasOwnerRef = true
				break
			}
		}

		// SetControllerReference only refuses PVCs that already have a
		// controller, so it is shardPVCShouldBeCleaned's name check that keeps
		// a labelled stranger from being adopted. A PVC without such a name
		// that carries this Shard's ref anyway is released whatever the
		// policy: releasing can only leave a volume behind, never delete one.
		wantOwnerRef := shardPVCShouldBeCleaned(shard, pvc)

		if wantOwnerRef && !hasOwnerRef && adopt {
			patch := client.MergeFrom(pvc.DeepCopy())
			if err := ctrl.SetControllerReference(shard, pvc, r.Scheme); err != nil {
				return fmt.Errorf("failed to add ownerRef to PVC %s: %w", pvc.Name, err)
			}
			if err := r.Patch(ctx, pvc, patch); err != nil {
				return fmt.Errorf("failed to patch PVC %s ownerRef: %w", pvc.Name, err)
			}
			logger.Info("Added ownerRef to PVC for Delete policy", "pvc", pvc.Name)
		} else if !wantOwnerRef && hasOwnerRef {
			if err := r.releasePVCFromShard(ctx, shard, pvc); err != nil {
				return err
			}
		}
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ShardReconciler) SetupWithManager(mgr ctrl.Manager, opts ...controller.Options) error {
	return r.SetupWithManagerReconciler(mgr, r, opts...)
}

// SetupWithManagerReconciler wires this controller's watch configuration to an
// arbitrary reconcile.Reconciler instead of the receiver. A method-bound
// reconciler cannot be substituted from outside its package, and rebuilding the
// builder below in a test would duplicate its watch configuration and let that
// copy drift from production. Production always goes through SetupWithManager,
// which passes the receiver here unchanged; nothing outside a test harness
// should call this directly.
//
// Substitution swaps the reconcile boundary and nothing else. The receiver
// stays live on the enqueue path, since map functions and predicates are bound
// to it when the builder runs and use its client. So the receiver has to be
// fully constructed, and it should be the same object the supplied reconciler
// ultimately delegates to, or the two will disagree about any state kept on
// it.
func (r *ShardReconciler) SetupWithManagerReconciler(
	mgr ctrl.Manager,
	reconciler reconcile.Reconciler,
	opts ...controller.Options,
) error {
	r.Client = ownership.SkipNoopApplies(r.Client)
	controllerOpts := controller.Options{
		MaxConcurrentReconciles: 20,
	}
	if len(opts) > 0 {
		controllerOpts = opts[0]
	}

	b := ctrl.NewControllerManagedBy(mgr).
		For(&multigresv1alpha1.Shard{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Watches(
			&corev1.PersistentVolumeClaim{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueShardForPVC),
		).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueFromPostgresConfigMap),
		).
		Watches(
			&storagev1.StorageClass{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueFromStorageClass),
		).
		// What the API server says about a pod changes with the namespace's
		// LimitRanges. A Shard holding a pod for a replacement the API
		// server rejected does not hear of that otherwise.
		Watches(
			&corev1.LimitRange{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueRefusedShards),
		).
		// A second handler on the pod informer Owns() already runs: someone
		// who made room in a quota asks for the creates to be sent now.
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.retryCreatesNow),
			builder.WithPredicates(retryCreatesRequested),
		)

	return r.Gate.Wire(b, mgr, func() client.ObjectList {
		return &multigresv1alpha1.ShardList{}
	}).
		WithOptions(controllerOpts).
		Complete(reconciler)
}

// finishRollingUpdateCondition sets the RollingUpdate condition from what
// every pool/cell noted this pass. before is the condition as the pass began.
func finishRollingUpdateCondition(
	conditions *[]metav1.Condition,
	rollout *shardRolloutTracker,
	before *metav1.Condition,
) {
	if c, ok := rollout.poolsRollingUpdate(); ok {
		meta.SetStatusCondition(conditions, c)
	}
	if rollout.blocked != "" {
		rollout.blockedAgain = before != nil && before.Reason == "RolloutBlocked" &&
			before.Message == rollout.blocked
		// Events about a Shard are rate limited per object and are lost once
		// its early lifecycle has used the budget, so a rollout that is
		// waiting on a safety check says so in status.
		meta.SetStatusCondition(conditions, metav1.Condition{
			Type:    "RollingUpdate",
			Status:  metav1.ConditionTrue,
			Reason:  "RolloutBlocked",
			Message: rollout.blocked,
		})
	}
	// A pass that reached no decision on a pending not-Ready replacement
	// (another disruption already asked to wait) changes nothing about why
	// the rollout is blocked, so the last reason stands.
	if rollout.blocked == "" && rollout.unreadyDeferred &&
		before != nil && before.Reason == "RolloutBlocked" {
		meta.SetStatusCondition(conditions, *before)
	}
	keepTransitionTime(*conditions, before)
}

// keepTransitionTime restores before's LastTransitionTime on the condition of
// the same type in conditions when its status is unchanged, whatever states
// it passed through in between.
func keepTransitionTime(conditions []metav1.Condition, before *metav1.Condition) {
	if before == nil {
		return
	}
	after := meta.FindStatusCondition(conditions, before.Type)
	if after != nil && after.Status == before.Status {
		after.LastTransitionTime = before.LastTransitionTime
	}
}
