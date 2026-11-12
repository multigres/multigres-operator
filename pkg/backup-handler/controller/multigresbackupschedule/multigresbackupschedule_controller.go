// Package multigresbackupschedule implements the schedule controller: it
// creates MultigresBackup attempts when runs are due, retries failed runs,
// and deletes old attempts.
package multigresbackupschedule

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	backuphandler "github.com/multigres/multigres-operator/pkg/backup-handler"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// maxRequeue is the longest the controller waits between reconciles.
const maxRequeue = 5 * time.Minute

// Reconciler reconciles MultigresBackupSchedule objects.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Gate     features.Gate
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

// AttemptName returns the deterministic name of a scheduled attempt (I4):
// <schedule>-<hash8(database/tableGroup/shard)>-<scheduledTime unix minutes>-<attempt>.
func AttemptName(
	schedule string,
	target multigresv1alpha1.BackupTarget,
	scheduled time.Time,
	attempt int32,
) string {
	return fmt.Sprintf("%s-%s-%d-%d",
		schedule,
		name.Hash([]string{target.Database, target.TableGroup, target.Shard}),
		scheduled.Unix()/60,
		attempt,
	)
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	sched := &multigresv1alpha1.MultigresBackupSchedule{}
	if err := r.Get(ctx, req.NamespacedName, sched); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if gated, err := r.Gate.Gated(ctx, r.Client, sched); gated || err != nil {
		return ctrl.Result{}, err
	}
	if !sched.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	orig := sched.DeepCopy()
	now := r.now()

	// 1. Validate the schedule.
	cronSched, err := parseSchedule(sched.Spec.Schedule)
	if err != nil {
		r.setReady(sched, metav1.ConditionFalse,
			multigresv1alpha1.BackupScheduleReasonInvalidSchedule, err.Error())
		return ctrl.Result{}, r.writeStatus(ctx, orig, sched)
	}

	cluster := &multigresv1alpha1.MultigresCluster{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: sched.Namespace, Name: sched.Spec.ClusterRef.Name,
	}, cluster); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		r.setReady(sched, metav1.ConditionFalse,
			multigresv1alpha1.BackupScheduleReasonClusterNotFound,
			fmt.Sprintf("MultigresCluster %q not found", sched.Spec.ClusterRef.Name))
		return ctrl.Result{RequeueAfter: maxRequeue}, r.writeStatus(ctx, orig, sched)
	}

	// 2. Find the target shards.
	shards, missing, err := r.targetShards(ctx, sched)
	if err != nil {
		return ctrl.Result{}, err
	}

	// List by namespace, not labels: unlabeled manual backups still count for
	// the skip rule.
	backups := &multigresv1alpha1.MultigresBackupList{}
	if err := r.List(ctx, backups, client.InNamespace(sched.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing MultigresBackups: %w", err)
	}

	prev := map[multigresv1alpha1.BackupTarget]multigresv1alpha1.BackupScheduleTargetStatus{}
	for _, ts := range sched.Status.Targets {
		prev[ts.BackupTarget] = ts
	}

	requeue := maxRequeue
	wake := func(at time.Time) {
		if d := at.Sub(now); d > 0 && d < requeue {
			requeue = d
		}
	}

	targets := make([]multigresv1alpha1.BackupScheduleTargetStatus, 0, len(shards))
	for i := range shards {
		shard := &shards[i]
		target := multigresv1alpha1.BackupTarget{
			Database:   string(shard.Spec.DatabaseName),
			TableGroup: string(shard.Spec.TableGroupName),
			Shard:      string(shard.Spec.ShardName),
		}
		ts := prev[target]
		ts.BackupTarget = target

		shardBackups := filterShard(backups.Items, sched.Spec.ClusterRef.Name, target)
		owned := filterOwned(shardBackups, sched)

		next := cronSched.Next(now)
		ts.NextScheduleTime = &metav1.Time{Time: next}
		wake(next)

		retryAt, err := r.reconcileTarget(
			ctx, sched, cronSched, shard, target, &ts, shardBackups, owned, now,
		)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !retryAt.IsZero() {
			wake(retryAt)
		}

		// 6. History.
		if err := r.pruneHistory(ctx, sched, owned, ts.LastScheduleTime); err != nil {
			l.Error(err, "pruning backup history", "shard", target)
		}
		targets = append(targets, ts)
	}
	sched.Status.Targets = targets
	r.pruneUntargeted(ctx, sched, backups.Items, targets)

	if len(missing) > 0 {
		r.setReady(sched, metav1.ConditionFalse,
			multigresv1alpha1.BackupScheduleReasonTargetNotFound,
			fmt.Sprintf("no Shard matches targets %v", missing))
	} else {
		r.setReady(sched, metav1.ConditionTrue,
			multigresv1alpha1.BackupScheduleReasonScheduled,
			fmt.Sprintf("scheduling %d shard(s)", len(shards)))
	}

	if err := r.writeStatus(ctx, orig, sched); err != nil {
		return ctrl.Result{}, err
	}
	// Requeue a little after the due time so the cron boundary has passed.
	return ctrl.Result{RequeueAfter: requeue + time.Second}, nil
}

// reconcileTarget runs steps 3.1–3.5 for one shard. It returns when the
// shard next needs attention for a retry, or the zero time.
func (r *Reconciler) reconcileTarget(
	ctx context.Context,
	sched *multigresv1alpha1.MultigresBackupSchedule,
	cronSched cron.Schedule,
	shard *multigresv1alpha1.Shard,
	target multigresv1alpha1.BackupTarget,
	ts *multigresv1alpha1.BackupScheduleTargetStatus,
	shardBackups, owned []multigresv1alpha1.MultigresBackup,
	now time.Time,
) (time.Time, error) {
	// Observed state, independent of whether a run is due.
	ts.Active = nil
	for i := range owned {
		b := &owned[i]
		if !b.Status.Phase.Finished() {
			ts.Active = &corev1.LocalObjectReference{Name: b.Name}
		}
		if b.Status.Phase == multigresv1alpha1.BackupPhaseSucceeded &&
			b.Status.CompletionTime != nil &&
			(ts.LastSuccessfulTime == nil || b.Status.CompletionTime.After(ts.LastSuccessfulTime.Time)) {
			ts.LastSuccessfulTime = b.Status.CompletionTime.DeepCopy()
		}
	}
	if ts.LastSuccessfulTime != nil {
		backuphandler.ScheduleLastSuccess.WithLabelValues(
			sched.Namespace, sched.Spec.ClusterRef.Name,
			target.Database, target.TableGroup, target.Shard, sched.Name,
		).Set(float64(ts.LastSuccessfulTime.Unix()))
	}

	// 3.1 Bootstrap check (I7, D9).
	if ok, why := backuphandler.ShardBootstrapped(shard); !ok {
		ts.Waiting = why
		return time.Time{}, nil
	}
	ts.Waiting = ""

	// 3.3 Suspend: status only.
	if sched.Spec.Suspend {
		ts.Waiting = "schedule is suspended"
		return time.Time{}, nil
	}

	// 3.2 Timing. Runs before the schedule existed are never due, as with a
	// CronJob.
	last := mostRecent(cronSched, sched.CreationTimestamp.Time, now)
	if last.IsZero() {
		return time.Time{}, nil
	}
	deadline := time.Duration(sched.Spec.StartingDeadline()) * time.Second
	withinDeadline := now.Sub(last) <= deadline
	alreadyActed := ts.LastScheduleTime != nil && ts.LastScheduleTime.Time.Equal(last)

	var attempts []*multigresv1alpha1.MultigresBackup
	for i := range owned {
		b := &owned[i]
		if b.Spec.ScheduledTime != nil && b.Spec.ScheduledTime.Time.Equal(last) {
			attempts = append(attempts, b)
		}
	}

	// 3.4 No attempt yet for last.
	if len(attempts) == 0 {
		if alreadyActed {
			// Skipped or missed already, or its attempts were pruned.
			return time.Time{}, nil
		}
		ts.LastScheduleTime = &metav1.Time{Time: last}
		if !withinDeadline {
			ts.LastScheduleOutcome = multigresv1alpha1.BackupScheduleOutcomeMissed
			backuphandler.ScheduleRunsMissed.WithLabelValues(sched.Namespace, sched.Name).Inc()
			r.Recorder.Eventf(sched, "Warning", "Missed",
				"Run at %s for %s/%s/%s missed: more than %ds late",
				last.Format(time.RFC3339), target.Database, target.TableGroup, target.Shard,
				sched.Spec.StartingDeadline())
			return time.Time{}, nil
		}
		return time.Time{}, r.createOrSkip(ctx, sched, target, ts, shardBackups, last, 1)
	}

	// 3.5 Retry the latest attempt for last if it failed.
	sort.Slice(attempts, func(i, j int) bool {
		return attempts[i].Spec.AttemptNumber() < attempts[j].Spec.AttemptNumber()
	})
	latest := attempts[len(attempts)-1]
	if !alreadyActed {
		// The status write after creating the attempt was lost.
		ts.LastScheduleTime = &metav1.Time{Time: last}
		ts.LastScheduleOutcome = multigresv1alpha1.BackupScheduleOutcomeCreated
	}
	if latest.Status.Phase != multigresv1alpha1.BackupPhaseFailed ||
		ts.LastScheduleOutcome == multigresv1alpha1.BackupScheduleOutcomeSkipped ||
		latest.Spec.AttemptNumber() >= sched.Spec.MaxAttempts() ||
		!withinDeadline {
		return time.Time{}, nil
	}
	completed := latest.CreationTimestamp.Time
	if latest.Status.CompletionTime != nil {
		completed = latest.Status.CompletionTime.Time
	}
	retryAt := completed.Add(time.Duration(sched.Spec.BackoffSeconds()) * time.Second)
	if now.Before(retryAt) {
		return retryAt, nil
	}
	return time.Time{}, r.createOrSkip(
		ctx, sched, target, ts, shardBackups, last, latest.Spec.AttemptNumber()+1,
	)
}

// createOrSkip applies the type-aware skip rule (D7) and otherwise creates
// the attempt, which queues behind any unfinished backup for the shard (D6).
func (r *Reconciler) createOrSkip(
	ctx context.Context,
	sched *multigresv1alpha1.MultigresBackupSchedule,
	target multigresv1alpha1.BackupTarget,
	ts *multigresv1alpha1.BackupScheduleTargetStatus,
	shardBackups []multigresv1alpha1.MultigresBackup,
	last time.Time,
	attempt int32,
) error {
	if covering := backuphandler.CoveringBackup(shardBackups, sched.Spec.Type); covering != nil {
		ts.LastScheduleOutcome = multigresv1alpha1.BackupScheduleOutcomeSkipped
		backuphandler.ScheduleRunsSkipped.WithLabelValues(sched.Namespace, sched.Name).Inc()
		r.Recorder.Eventf(sched, "Normal", "Skipped",
			"Run at %s for %s/%s/%s skipped: %s backup %s is unfinished",
			last.Format(time.RFC3339), target.Database, target.TableGroup, target.Shard,
			covering.Spec.Type, covering.Name)
		return nil
	}

	scheduled := metav1.NewTime(last)
	backup := &multigresv1alpha1.MultigresBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AttemptName(sched.Name, target, last, attempt),
			Namespace: sched.Namespace,
			Labels: backuphandler.ShardLabels(
				sched.Spec.ClusterRef.Name, target.Database, target.TableGroup, target.Shard,
			),
		},
		Spec: multigresv1alpha1.MultigresBackupSpec{
			ClusterRef:     sched.Spec.ClusterRef,
			Database:       target.Database,
			TableGroup:     target.TableGroup,
			Shard:          target.Shard,
			Type:           sched.Spec.Type,
			ForcePrimary:   sched.Spec.ForcePrimary,
			TimeoutSeconds: sched.Spec.TimeoutSeconds(),
			ScheduledTime:  &scheduled,
			Attempt:        attempt,
		},
	}
	backup.Labels[multigresv1alpha1.LabelBackupSchedule] = backuphandler.ScheduleLabelValue(
		sched.Name,
	)
	backup.Labels[multigresv1alpha1.LabelBackupTrigger] = multigresv1alpha1.BackupTriggerScheduled
	if err := controllerutil.SetControllerReference(sched, backup, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, backup); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating MultigresBackup %s: %w", backup.Name, err)
	}
	ts.LastScheduleOutcome = multigresv1alpha1.BackupScheduleOutcomeCreated
	ts.Active = &corev1.LocalObjectReference{Name: backup.Name}
	return nil
}

// pruneHistory deletes this schedule's finished attempts for a shard beyond
// the history limits, oldest first. Attempts of the current run are kept so
// retry counting stays correct.
func (r *Reconciler) pruneHistory(
	ctx context.Context,
	sched *multigresv1alpha1.MultigresBackupSchedule,
	owned []multigresv1alpha1.MultigresBackup,
	current *metav1.Time,
) error {
	var succeeded, failed []*multigresv1alpha1.MultigresBackup
	for i := range owned {
		b := &owned[i]
		if current != nil && b.Spec.ScheduledTime != nil &&
			b.Spec.ScheduledTime.Time.Equal(current.Time) &&
			b.Status.Phase == multigresv1alpha1.BackupPhaseFailed {
			continue
		}
		switch b.Status.Phase {
		case multigresv1alpha1.BackupPhaseSucceeded:
			succeeded = append(succeeded, b)
		case multigresv1alpha1.BackupPhaseFailed:
			failed = append(failed, b)
		}
	}
	prune := func(list []*multigresv1alpha1.MultigresBackup, limit32 int32) error {
		limit := int(limit32)
		if len(list) <= limit {
			return nil
		}
		sort.Slice(
			list,
			func(i, j int) bool { return finishedAt(list[i]).Before(finishedAt(list[j])) },
		)
		for _, b := range list[:len(list)-limit] {
			if err := r.Delete(ctx, b); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
		return nil
	}
	if err := prune(succeeded, sched.Spec.SuccessfulHistoryLimit()); err != nil {
		return err
	}
	return prune(failed, sched.Spec.FailedHistoryLimit())
}

// pruneUntargeted applies the history limits to this schedule's attempts for
// shards it no longer targets, because the Shard was deleted or dropped from
// spec.targets. No new attempts are created for those shards, so without this
// their finished attempts would stay until the schedule is deleted. Unfinished
// attempts are left to fail on their own and are pruned on a later reconcile.
func (r *Reconciler) pruneUntargeted(
	ctx context.Context,
	sched *multigresv1alpha1.MultigresBackupSchedule,
	all []multigresv1alpha1.MultigresBackup,
	targeted []multigresv1alpha1.BackupScheduleTargetStatus,
) {
	seen := make(map[multigresv1alpha1.BackupTarget]bool, len(targeted))
	for _, ts := range targeted {
		seen[ts.BackupTarget] = true
	}
	byTarget := map[multigresv1alpha1.BackupTarget][]multigresv1alpha1.MultigresBackup{}
	for _, b := range filterOwned(all, sched) {
		t := multigresv1alpha1.BackupTarget{
			Database:   b.Spec.Database,
			TableGroup: b.Spec.TableGroup,
			Shard:      b.Spec.Shard,
		}
		if !seen[t] {
			byTarget[t] = append(byTarget[t], b)
		}
	}
	for t, owned := range byTarget {
		if err := r.pruneHistory(ctx, sched, owned, nil); err != nil {
			log.FromContext(ctx).Error(err,
				"pruning backup history for untargeted shard", "shard", t)
		}
	}
}

func finishedAt(b *multigresv1alpha1.MultigresBackup) time.Time {
	if b.Status.CompletionTime != nil {
		return b.Status.CompletionTime.Time
	}
	return b.CreationTimestamp.Time
}

// targetShards lists the cluster's live Shards that match spec.targets (all of
// them when targets is empty), sorted, and the targets that matched nothing.
func (r *Reconciler) targetShards(
	ctx context.Context,
	sched *multigresv1alpha1.MultigresBackupSchedule,
) ([]multigresv1alpha1.Shard, []multigresv1alpha1.BackupTarget, error) {
	list := &multigresv1alpha1.ShardList{}
	if err := r.List(ctx, list,
		client.InNamespace(sched.Namespace),
		client.MatchingLabels{metadata.LabelMultigresCluster: sched.Spec.ClusterRef.Name},
	); err != nil {
		return nil, nil, fmt.Errorf("listing Shards: %w", err)
	}
	key := func(s *multigresv1alpha1.Shard) multigresv1alpha1.BackupTarget {
		return multigresv1alpha1.BackupTarget{
			Database:   string(s.Spec.DatabaseName),
			TableGroup: string(s.Spec.TableGroupName),
			Shard:      string(s.Spec.ShardName),
		}
	}
	live := map[multigresv1alpha1.BackupTarget]multigresv1alpha1.Shard{}
	for _, s := range list.Items {
		if s.DeletionTimestamp.IsZero() {
			live[key(&s)] = s
		}
	}

	var out []multigresv1alpha1.Shard
	var missing []multigresv1alpha1.BackupTarget
	if len(sched.Spec.Targets) == 0 {
		for _, s := range live {
			out = append(out, s)
		}
	} else {
		for _, t := range sched.Spec.Targets {
			if s, ok := live[t]; ok {
				out = append(out, s)
			} else {
				missing = append(missing, t)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, missing, nil
}

func filterShard(
	all []multigresv1alpha1.MultigresBackup,
	cluster string,
	t multigresv1alpha1.BackupTarget,
) []multigresv1alpha1.MultigresBackup {
	var out []multigresv1alpha1.MultigresBackup
	for _, b := range all {
		if b.Spec.ClusterRef.Name == cluster &&
			b.Spec.Database == t.Database && b.Spec.TableGroup == t.TableGroup &&
			b.Spec.Shard == t.Shard {
			out = append(out, b)
		}
	}
	return out
}

func filterOwned(
	all []multigresv1alpha1.MultigresBackup,
	sched *multigresv1alpha1.MultigresBackupSchedule,
) []multigresv1alpha1.MultigresBackup {
	var out []multigresv1alpha1.MultigresBackup
	for _, b := range all {
		if owner := metav1.GetControllerOf(&b); owner != nil && owner.UID == sched.UID {
			out = append(out, b)
		}
	}
	return out
}

func (r *Reconciler) setReady(
	sched *multigresv1alpha1.MultigresBackupSchedule,
	status metav1.ConditionStatus,
	reason, message string,
) {
	sched.Status.ObservedGeneration = sched.Generation
	meta.SetStatusCondition(&sched.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: sched.Generation,
	})
}

func (r *Reconciler) writeStatus(
	ctx context.Context,
	orig, sched *multigresv1alpha1.MultigresBackupSchedule,
) error {
	if equalStatus(orig, sched) {
		return nil
	}
	return r.Status().Patch(ctx, sched, client.MergeFrom(orig))
}

func equalStatus(a, b *multigresv1alpha1.MultigresBackupSchedule) bool {
	return equality.Semantic.DeepEqual(a.Status, b.Status)
}

// SetupWithManager registers the controller.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts ...controller.Options) error {
	controllerOpts := controller.Options{MaxConcurrentReconciles: 5}
	if len(opts) > 0 {
		controllerOpts = opts[0]
	}
	b := ctrl.NewControllerManagedBy(mgr).
		Named("multigresbackupschedule").
		For(&multigresv1alpha1.MultigresBackupSchedule{}).
		Owns(&multigresv1alpha1.MultigresBackup{}).
		Watches(
			&multigresv1alpha1.Shard{},
			handler.EnqueueRequestsFromMapFunc(r.schedulesForShard),
			builder.WithPredicates(shardSchedulingChanged()),
		)
	return r.Gate.Wire(b, mgr, func() client.ObjectList {
		return &multigresv1alpha1.MultigresBackupScheduleList{}
	}).
		WithOptions(controllerOpts).
		Complete(r)
}

// shardSchedulingChanged passes Shard events that can change what a schedule
// does: creation, deletion, and a change in bootstrap state. Shard status is
// updated often, so other updates are dropped.
func shardSchedulingChanged() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldShard, ok1 := e.ObjectOld.(*multigresv1alpha1.Shard)
			newShard, ok2 := e.ObjectNew.(*multigresv1alpha1.Shard)
			if !ok1 || !ok2 {
				return false
			}
			oldReady, _ := backuphandler.ShardBootstrapped(oldShard)
			newReady, _ := backuphandler.ShardBootstrapped(newShard)
			return oldReady != newReady ||
				oldShard.DeletionTimestamp.IsZero() != newShard.DeletionTimestamp.IsZero()
		},
	}
}

// schedulesForShard maps a Shard to the schedules of its cluster.
func (r *Reconciler) schedulesForShard(ctx context.Context, o client.Object) []reconcile.Request {
	cluster := o.GetLabels()[metadata.LabelMultigresCluster]
	if cluster == "" {
		return nil
	}
	list := &multigresv1alpha1.MultigresBackupScheduleList{}
	if err := r.List(ctx, list, client.InNamespace(o.GetNamespace())); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, s := range list.Items {
		if s.Spec.ClusterRef.Name == cluster {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&s)})
		}
	}
	return reqs
}
