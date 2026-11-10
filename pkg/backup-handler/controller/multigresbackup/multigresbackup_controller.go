// Package multigresbackup implements the attempt controller: it starts one
// backup through multiadmin per MultigresBackup and follows it to the end.
package multigresbackup

import (
	"context"
	"fmt"
	"time"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	backuphandler "github.com/multigres/multigres-operator/pkg/backup-handler"
	"github.com/multigres/multigres-operator/pkg/backup-handler/multiadmin"
	"github.com/multigres/multigres-operator/pkg/features"
)

const (
	// PendingRequeue is how often a queued attempt checks whether it is first
	// in line.
	PendingRequeue = 15 * time.Second
	// RunningRequeue is how often a running attempt polls multiadmin.
	RunningRequeue = 30 * time.Second
	// LostGrace is how long GetBackupJobStatus must keep answering NotFound
	// before the attempt is marked Lost. Multiadmin's NotFound also covers
	// transient pooler-selection and RPC failures, so one NotFound is not
	// proof that the backup is gone.
	LostGrace = 10 * time.Minute
	// rpcTimeout bounds each multiadmin call.
	rpcTimeout = 30 * time.Second
	// maxErrorBackoff caps the backoff for temporary errors.
	maxErrorBackoff = 5 * time.Minute
)

// Reconciler drives MultigresBackup through Pending → Triggering → Running →
// Succeeded/Failed.
type Reconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Recorder   record.EventRecorder
	Multiadmin multiadmin.Factory
	Gate       features.Gate
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	backup := &multigresv1alpha1.MultigresBackup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if gated, err := r.Gate.Gated(ctx, r.Client, backup); gated || err != nil {
		return ctrl.Result{}, err
	}
	// I10: a finished attempt is never changed again.
	if backup.Status.Phase.Finished() || !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	cluster := &multigresv1alpha1.MultigresCluster{}
	err := r.Get(ctx, client.ObjectKey{
		Namespace: backup.Namespace,
		Name:      backup.Spec.ClusterRef.Name,
	}, cluster)
	switch {
	case apierrors.IsNotFound(err):
		cluster = nil
	case err != nil:
		return ctrl.Result{}, err
	case !cluster.DeletionTimestamp.IsZero():
		cluster = nil
	}

	switch backup.Status.Phase {
	case "":
		return r.initialize(ctx, backup, cluster)
	case multigresv1alpha1.BackupPhasePending:
		if cluster == nil {
			return r.fail(ctx, backup, multigresv1alpha1.BackupReasonClusterNotFound,
				fmt.Sprintf("MultigresCluster %q is missing or being deleted",
					backup.Spec.ClusterRef.Name))
		}
		return r.reconcilePending(ctx, backup, cluster)
	case multigresv1alpha1.BackupPhaseTriggering:
		// A previous reconcile saved Triggering and then made, or tried to
		// make, the Backup call without saving its result. Calling Backup
		// again could start a second backup that cancels the first (C1), so
		// the outcome is unknown and the schedule retries with a new object.
		return r.fail(ctx, backup, multigresv1alpha1.BackupReasonTriggerOutcomeUnknown,
			"found in Triggering without a job ID; the Backup call's outcome is unknown")
	case multigresv1alpha1.BackupPhaseRunning:
		if cluster == nil {
			return r.fail(ctx, backup, multigresv1alpha1.BackupReasonClusterNotFound,
				fmt.Sprintf("MultigresCluster %q is missing or being deleted",
					backup.Spec.ClusterRef.Name))
		}
		return r.reconcileRunning(ctx, backup, cluster)
	default:
		l.Info("unknown phase; ignoring", "phase", backup.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// initialize labels the attempt for the per-shard queue, gives a manual
// backup an owner reference to its cluster, and moves it to Pending.
func (r *Reconciler) initialize(
	ctx context.Context,
	backup *multigresv1alpha1.MultigresBackup,
	cluster *multigresv1alpha1.MultigresCluster,
) (ctrl.Result, error) {
	changed := false
	if backup.Labels == nil {
		backup.Labels = map[string]string{}
	}
	for k, v := range backuphandler.BackupShardLabels(backup) {
		if backup.Labels[k] != v {
			backup.Labels[k] = v
			changed = true
		}
	}
	if _, ok := backup.Labels[multigresv1alpha1.LabelBackupTrigger]; !ok {
		trigger := multigresv1alpha1.BackupTriggerManual
		if backup.Spec.ScheduledTime != nil {
			trigger = multigresv1alpha1.BackupTriggerScheduled
		}
		backup.Labels[multigresv1alpha1.LabelBackupTrigger] = trigger
		changed = true
	}
	if cluster != nil && metav1.GetControllerOf(backup) == nil {
		// Manual backups are garbage-collected with their cluster.
		if err := controllerutil.SetControllerReference(cluster, backup, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		changed = true
	}
	if changed {
		if err := r.Update(ctx, backup); err != nil {
			return ctrl.Result{}, err
		}
	}

	backup.Status.Phase = multigresv1alpha1.BackupPhasePending
	if err := r.Status().Update(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Millisecond}, nil
}

// reconcilePending enforces the per-shard queue (I1, D6) and triggers the
// backup once this attempt is first in line.
func (r *Reconciler) reconcilePending(
	ctx context.Context,
	backup *multigresv1alpha1.MultigresBackup,
	cluster *multigresv1alpha1.MultigresCluster,
) (ctrl.Result, error) {
	others, err := backuphandler.ListShardBackups(ctx, r.Client, backup.Namespace,
		backup.Spec.ClusterRef.Name, backup.Spec.Database, backup.Spec.TableGroup,
		backup.Spec.Shard)
	if err != nil {
		return ctrl.Result{}, err
	}
	for i := range others {
		other := &others[i]
		if other.UID == backup.UID || other.Status.Phase.Finished() {
			continue
		}
		inFlight := other.Status.Phase == multigresv1alpha1.BackupPhaseTriggering ||
			other.Status.Phase == multigresv1alpha1.BackupPhaseRunning
		if inFlight || backuphandler.QueuedBefore(other, backup) {
			msg := fmt.Sprintf("waiting for %s", other.Name)
			if backup.Status.Message != msg {
				backup.Status.Message = msg
				if err := r.Status().Update(ctx, backup); err != nil {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{RequeueAfter: PendingRequeue}, nil
		}
	}

	// I3: save Triggering before the RPC. The optimistic-concurrency check on
	// this write also ensures only one reconcile goes on to call Backup (I2).
	now := metav1.NewTime(r.now())
	backup.Status.Phase = multigresv1alpha1.BackupPhaseTriggering
	backup.Status.TriggerTime = &now
	backup.Status.Message = ""
	if err := r.Status().Update(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}
	backuphandler.PendingDuration.Observe(now.Sub(backup.CreationTimestamp.Time).Seconds())

	return r.trigger(ctx, backup, cluster)
}

// trigger makes the one Backup call for this attempt.
func (r *Reconciler) trigger(
	ctx context.Context,
	backup *multigresv1alpha1.MultigresBackup,
	cluster *multigresv1alpha1.MultigresCluster,
) (ctrl.Result, error) {
	ma, err := r.Multiadmin.ClientFor(ctx, cluster)
	if err != nil {
		// No call was made, so nothing was started.
		return r.fail(ctx, backup, multigresv1alpha1.BackupReasonTriggerRejected,
			fmt.Sprintf("connecting to multiadmin: %v", err))
	}
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	resp, err := ma.Backup(rpcCtx, &multiadminpb.BackupRequest{
		Database:     backup.Spec.Database,
		TableGroup:   backup.Spec.TableGroup,
		Shard:        backup.Spec.Shard,
		Type:         string(backup.Spec.Type),
		ForcePrimary: backup.Spec.ForcePrimary,
	})
	if err != nil {
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.InvalidArgument, codes.PermissionDenied,
			codes.Unauthenticated, codes.Unimplemented:
			return r.fail(ctx, backup, multigresv1alpha1.BackupReasonTriggerRejected,
				err.Error())
		default:
			return r.fail(ctx, backup, multigresv1alpha1.BackupReasonTriggerOutcomeUnknown,
				err.Error())
		}
	}
	if resp.GetJobId() == "" {
		return r.fail(ctx, backup, multigresv1alpha1.BackupReasonTriggerOutcomeUnknown,
			"multiadmin accepted the backup but returned no job ID")
	}

	now := metav1.NewTime(r.now())
	backup.Status.Phase = multigresv1alpha1.BackupPhaseRunning
	backup.Status.JobID = resp.GetJobId()
	backup.Status.StartTime = &now
	if err := r.Status().Update(ctx, backup); err != nil {
		// The job is running but we could not record it. The next reconcile
		// finds Triggering without a job ID and fails the attempt.
		return ctrl.Result{}, err
	}
	r.Recorder.Eventf(backup, "Normal", "Triggered",
		"Started %s backup of %s/%s/%s (job %s)", backup.Spec.Type,
		backup.Spec.Database, backup.Spec.TableGroup, backup.Spec.Shard, resp.GetJobId())
	return ctrl.Result{RequeueAfter: RunningRequeue}, nil
}

// reconcileRunning polls the job until it finishes, times out, or is lost.
func (r *Reconciler) reconcileRunning(
	ctx context.Context,
	backup *multigresv1alpha1.MultigresBackup,
	cluster *multigresv1alpha1.MultigresCluster,
) (ctrl.Result, error) {
	now := r.now()
	if backup.Status.StartTime != nil {
		deadline := backup.Status.StartTime.Add(
			time.Duration(backup.Spec.Timeout()) * time.Second,
		)
		if now.After(deadline) {
			return r.fail(ctx, backup, multigresv1alpha1.BackupReasonTimeout,
				fmt.Sprintf("still running after %ds", backup.Spec.Timeout()))
		}
	}

	ma, err := r.Multiadmin.ClientFor(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err // I5: temporary, retried with backoff.
	}
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	// I6: include the shard so multiadmin can ask a pooler after a restart.
	resp, err := ma.GetBackupJobStatus(rpcCtx, &multiadminpb.GetBackupJobStatusRequest{
		JobId:      backup.Status.JobID,
		Database:   backup.Spec.Database,
		TableGroup: backup.Spec.TableGroup,
		Shard:      backup.Spec.Shard,
	})
	if status.Code(err) == codes.NotFound {
		if backup.Status.NotFoundSince == nil {
			t := metav1.NewTime(now)
			backup.Status.NotFoundSince = &t
			if err := r.Status().Update(ctx, backup); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: RunningRequeue}, nil
		}
		if now.Sub(backup.Status.NotFoundSince.Time) >= LostGrace {
			return r.fail(ctx, backup, multigresv1alpha1.BackupReasonLost,
				fmt.Sprintf("multiadmin has not known job %s for %s: %v",
					backup.Status.JobID, LostGrace, err))
		}
		return ctrl.Result{RequeueAfter: RunningRequeue}, nil
	}
	if err != nil {
		// I5: one failed poll never fails the attempt.
		return ctrl.Result{}, fmt.Errorf("polling backup job %s: %w", backup.Status.JobID, err)
	}

	if backup.Status.NotFoundSince != nil {
		backup.Status.NotFoundSince = nil
		if err := r.Status().Update(ctx, backup); err != nil {
			return ctrl.Result{}, err
		}
	}

	switch resp.GetStatus() {
	case multiadminpb.JobStatus_JOB_STATUS_COMPLETED:
		return r.succeed(ctx, backup, resp.GetBackupId())
	case multiadminpb.JobStatus_JOB_STATUS_FAILED:
		msg := resp.GetErrorMessage()
		if msg == "" {
			msg = "multiadmin reported the backup job failed"
		}
		return r.fail(ctx, backup, multigresv1alpha1.BackupReasonBackupFailed, msg)
	default:
		return ctrl.Result{RequeueAfter: RunningRequeue}, nil
	}
}

func (r *Reconciler) succeed(
	ctx context.Context,
	backup *multigresv1alpha1.MultigresBackup,
	backupID string,
) (ctrl.Result, error) {
	now := metav1.NewTime(r.now())
	backup.Status.Phase = multigresv1alpha1.BackupPhaseSucceeded
	backup.Status.Reason = ""
	backup.Status.Message = ""
	backup.Status.BackupID = backupID
	backup.Status.CompletionTime = &now
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               multigresv1alpha1.BackupConditionComplete,
		Status:             metav1.ConditionTrue,
		Reason:             string(multigresv1alpha1.BackupPhaseSucceeded),
		Message:            fmt.Sprintf("backup %s completed", backupID),
		ObservedGeneration: backup.Generation,
	})
	if err := r.Status().Update(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}
	r.observeFinished(backup)
	r.Recorder.Eventf(backup, "Normal", string(multigresv1alpha1.BackupPhaseSucceeded),
		"Backup %s completed", backupID)
	return ctrl.Result{}, nil
}

func (r *Reconciler) fail(
	ctx context.Context,
	backup *multigresv1alpha1.MultigresBackup,
	reason, message string,
) (ctrl.Result, error) {
	now := metav1.NewTime(r.now())
	backup.Status.Phase = multigresv1alpha1.BackupPhaseFailed
	backup.Status.Reason = reason
	backup.Status.Message = message
	backup.Status.CompletionTime = &now
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               multigresv1alpha1.BackupConditionComplete,
		Status:             metav1.ConditionTrue,
		Reason:             string(multigresv1alpha1.BackupPhaseFailed),
		Message:            reason + ": " + message,
		ObservedGeneration: backup.Generation,
	})
	if err := r.Status().Update(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}
	r.observeFinished(backup)
	r.Recorder.Event(backup, "Warning", reason, message)
	return ctrl.Result{}, nil
}

func (r *Reconciler) observeFinished(backup *multigresv1alpha1.MultigresBackup) {
	backuphandler.AttemptsTotal.WithLabelValues(
		string(backup.Status.Phase),
		backup.Status.Reason,
		string(backup.Spec.Type),
		backup.Labels[multigresv1alpha1.LabelBackupTrigger],
	).Inc()
	if backup.Status.StartTime != nil && backup.Status.CompletionTime != nil {
		backuphandler.AttemptDuration.Observe(
			backup.Status.CompletionTime.Sub(backup.Status.StartTime.Time).Seconds(),
		)
	}
}

// SetupWithManager registers the controller.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts ...controller.Options) error {
	controllerOpts := controller.Options{
		MaxConcurrentReconciles: 10,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			time.Second, maxErrorBackoff,
		),
	}
	if len(opts) > 0 {
		controllerOpts = opts[0]
	}
	b := ctrl.NewControllerManagedBy(mgr).
		Named("multigresbackup").
		For(&multigresv1alpha1.MultigresBackup{})
	return r.Gate.Wire(b, mgr, func() client.ObjectList {
		return &multigresv1alpha1.MultigresBackupList{}
	}).
		WithOptions(controllerOpts).
		Complete(r)
}
