package multigresbackupschedule

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	ns    = "proj"
	sname = "mgc-daily"
)

var (
	created = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	due     = time.Date(2026, 11, 7, 4, 0, 0, 0, time.UTC) // "0 4 * * *" after created
	target  = multigresv1alpha1.BackupTarget{
		Database:   "postgres",
		TableGroup: "default",
		Shard:      "0-inf",
	}
)

type harness struct {
	t   *testing.T
	c   client.Client
	now time.Time
	r   *Reconciler
}

func newHarness(
	t *testing.T,
	mutate func(*multigresv1alpha1.MultigresBackupSchedule),
	objs ...client.Object,
) *harness {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	sched := &multigresv1alpha1.MultigresBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name: sname, Namespace: ns, UID: "sched-uid",
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: multigresv1alpha1.MultigresBackupScheduleSpec{
			ClusterRef: multigresv1alpha1.BackupClusterReference{Name: "mgc"},
			BackupScheduleTemplate: multigresv1alpha1.BackupScheduleTemplate{
				Schedule: "0 4 * * *",
				Type:     multigresv1alpha1.BackupKindDifferential,
			},
		},
	}
	if mutate != nil {
		mutate(sched)
	}
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mgc", Namespace: ns, UID: "cluster-uid"},
	}
	all := append([]client.Object{sched, cluster}, objs...)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(all...).
		WithStatusSubresource(
			&multigresv1alpha1.MultigresBackupSchedule{},
			&multigresv1alpha1.MultigresBackup{},
		).
		Build()
	h := &harness{t: t, c: c, now: due.Add(time.Minute)}
	h.r = &Reconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
		Now:      func() time.Time { return h.now },
	}
	return h
}

func bootstrappedShard(name, shard string) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns,
			Labels: map[string]string{metadata.LabelMultigresCluster: "mgc"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      multigresv1alpha1.ShardName(shard),
		},
		Status: multigresv1alpha1.ShardStatus{
			LastBackupTime: ptr.To(metav1.NewTime(created)),
			PodRoles:       map[string]string{"pod-0": "PRIMARY", "pod-1": "REPLICA"},
		},
	}
}

func (h *harness) reconcile() ctrl.Result {
	h.t.Helper()
	res, err := h.r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: sname},
	})
	if err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (h *harness) backups() []multigresv1alpha1.MultigresBackup {
	h.t.Helper()
	list := &multigresv1alpha1.MultigresBackupList{}
	if err := h.c.List(context.Background(), list, client.InNamespace(ns)); err != nil {
		h.t.Fatal(err)
	}
	return list.Items
}

func (h *harness) schedule() *multigresv1alpha1.MultigresBackupSchedule {
	h.t.Helper()
	s := &multigresv1alpha1.MultigresBackupSchedule{}
	if err := h.c.Get(
		context.Background(),
		client.ObjectKey{Namespace: ns, Name: sname},
		s,
	); err != nil {
		h.t.Fatal(err)
	}
	return s
}

func (h *harness) targetStatus() multigresv1alpha1.BackupScheduleTargetStatus {
	h.t.Helper()
	s := h.schedule()
	if len(s.Status.Targets) != 1 {
		h.t.Fatalf("targets = %d, want 1", len(s.Status.Targets))
	}
	return s.Status.Targets[0]
}

// finish sets an attempt's phase and completion time.
func (h *harness) finish(name string, phase multigresv1alpha1.BackupPhase, at time.Time) {
	h.t.Helper()
	b := &multigresv1alpha1.MultigresBackup{}
	if err := h.c.Get(
		context.Background(),
		client.ObjectKey{Namespace: ns, Name: name},
		b,
	); err != nil {
		h.t.Fatal(err)
	}
	b.Status.Phase = phase
	b.Status.CompletionTime = ptr.To(metav1.NewTime(at))
	if err := h.c.Status().Update(context.Background(), b); err != nil {
		h.t.Fatal(err)
	}
}

func unfinished(name string, kind multigresv1alpha1.BackupKind) *multigresv1alpha1.MultigresBackup {
	return &multigresv1alpha1.MultigresBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: multigresv1alpha1.MultigresBackupSpec{
			ClusterRef: multigresv1alpha1.BackupClusterReference{Name: "mgc"},
			Database:   target.Database, TableGroup: target.TableGroup, Shard: target.Shard,
			Type: kind,
		},
		Status: multigresv1alpha1.MultigresBackupStatus{
			Phase: multigresv1alpha1.BackupPhaseRunning,
		},
	}
}

// T1: a due run creates exactly one attempt with the deterministic name.
func TestDueRunCreatesOneAttempt(t *testing.T) {
	h := newHarness(t, nil, bootstrappedShard("s0", "0-inf"))
	for range 5 {
		h.reconcile()
	}
	backups := h.backups()
	if len(backups) != 1 {
		t.Fatalf("attempts = %d, want 1", len(backups))
	}
	b := backups[0]
	if want := AttemptName(sname, target, due, 1); b.Name != want {
		t.Fatalf("name = %s, want %s", b.Name, want)
	}
	if !b.Spec.ScheduledTime.Time.Equal(due) || b.Spec.Attempt != 1 ||
		b.Spec.Type != multigresv1alpha1.BackupKindDifferential {
		t.Fatalf("spec = %+v", b.Spec)
	}
	if owner := metav1.GetControllerOf(&b); owner == nil || owner.UID != "sched-uid" {
		t.Fatalf("owner = %v", b.OwnerReferences)
	}
	ts := h.targetStatus()
	if ts.LastScheduleOutcome != multigresv1alpha1.BackupScheduleOutcomeCreated ||
		ts.Active != b.Name || !ts.NextScheduleTime.Time.Equal(due.Add(24*time.Hour)) {
		t.Fatalf("target status = %+v", ts)
	}
	if !meta.IsStatusConditionTrue(h.schedule().Status.Conditions, "Ready") {
		t.Fatal("Ready is not True")
	}
}

// No run is due before the first cron time after the schedule was created.
func TestNothingDueBeforeFirstRun(t *testing.T) {
	h := newHarness(t, nil, bootstrappedShard("s0", "0-inf"))
	h.now = due.Add(-time.Minute)
	res := h.reconcile()
	if n := len(h.backups()); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}
	if res.RequeueAfter > 2*time.Minute {
		t.Fatalf("requeue = %s, want about a minute", res.RequeueAfter)
	}
}

// T9: a shard without a completed backup gets no attempt (I7).
func TestBootstrapGate(t *testing.T) {
	shard := bootstrappedShard("s0", "0-inf")
	shard.Status.LastBackupTime = nil
	h := newHarness(t, nil, shard)
	h.reconcile()
	if n := len(h.backups()); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}
	if ts := h.targetStatus(); ts.Waiting == "" {
		t.Fatal("Waiting not set")
	}

	shard = bootstrappedShard("s0", "0-inf")
	shard.Status.PodRoles = nil
	h = newHarness(t, nil, shard)
	h.reconcile()
	if n := len(h.backups()); n != 0 {
		t.Fatalf("attempts without a primary = %d, want 0", n)
	}
}

// T8: a run more than startingDeadlineSeconds late is missed, counted once.
func TestMissedRun(t *testing.T) {
	h := newHarness(t, nil, bootstrappedShard("s0", "0-inf"))
	h.now = due.Add(
		time.Duration(multigresv1alpha1.DefaultBackupStartingDeadlineSeconds+60) * time.Second,
	)
	h.reconcile()
	h.reconcile()
	if n := len(h.backups()); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}
	ts := h.targetStatus()
	if ts.MissedRuns != 1 ||
		ts.LastScheduleOutcome != multigresv1alpha1.BackupScheduleOutcomeMissed {
		t.Fatalf("target status = %+v", ts)
	}
}

// T6: failed attempts are retried after the backoff, up to maxAttempts.
func TestRetries(t *testing.T) {
	h := newHarness(t, nil, bootstrappedShard("s0", "0-inf"))
	backoff := time.Duration(multigresv1alpha1.DefaultBackupBackoffSeconds) * time.Second

	for attempt := int32(1); attempt <= multigresv1alpha1.DefaultBackupMaxAttempts; attempt++ {
		h.reconcile()
		if n := len(h.backups()); n != int(attempt) {
			t.Fatalf("after attempt %d: attempts = %d", attempt, n)
		}
		h.finish(
			AttemptName(sname, target, due, attempt),
			multigresv1alpha1.BackupPhaseFailed,
			h.now,
		)

		// Not before the backoff.
		h.now = h.now.Add(backoff - time.Second)
		res := h.reconcile()
		if n := len(h.backups()); n != int(attempt) {
			t.Fatalf("attempt %d retried before backoff", attempt)
		}
		if attempt < multigresv1alpha1.DefaultBackupMaxAttempts &&
			res.RequeueAfter > 2*time.Second {
			t.Fatalf("requeue = %s, want the retry time", res.RequeueAfter)
		}
		h.now = h.now.Add(2 * time.Second)
	}
	h.reconcile()
	if n := len(h.backups()); n != int(multigresv1alpha1.DefaultBackupMaxAttempts) {
		t.Fatalf(
			"attempts = %d after the limit, want %d",
			n,
			multigresv1alpha1.DefaultBackupMaxAttempts,
		)
	}
}

// A succeeded attempt is not retried.
func TestNoRetryAfterSuccess(t *testing.T) {
	h := newHarness(t, nil, bootstrappedShard("s0", "0-inf"))
	h.reconcile()
	h.finish(AttemptName(sname, target, due, 1), multigresv1alpha1.BackupPhaseSucceeded, h.now)
	h.now = h.now.Add(time.Hour)
	h.reconcile()
	if n := len(h.backups()); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
	if ts := h.targetStatus(); ts.LastSuccessfulTime == nil || ts.Active != "" {
		t.Fatalf("target status = %+v", ts)
	}
}

// T13: a full run queues behind an unfinished differential; a differential
// run is skipped while a full is unfinished, and counted once.
func TestTypeAwareSkip(t *testing.T) {
	full := func(s *multigresv1alpha1.MultigresBackupSchedule) {
		s.Spec.Type = multigresv1alpha1.BackupKindFull
	}
	h := newHarness(t, full, bootstrappedShard("s0", "0-inf"),
		unfinished("manual-diff", multigresv1alpha1.BackupKindDifferential))
	h.reconcile()
	if n := len(h.backups()); n != 2 {
		t.Fatalf("full behind differential: attempts = %d, want 2", n)
	}

	h = newHarness(t, nil, bootstrappedShard("s0", "0-inf"),
		unfinished("manual-full", multigresv1alpha1.BackupKindFull))
	h.reconcile()
	h.reconcile()
	if n := len(h.backups()); n != 1 {
		t.Fatalf("differential while full runs: attempts = %d, want 1", n)
	}
	ts := h.targetStatus()
	if ts.SkippedRuns != 1 ||
		ts.LastScheduleOutcome != multigresv1alpha1.BackupScheduleOutcomeSkipped {
		t.Fatalf("target status = %+v", ts)
	}

	// A same-type backup also covers the run.
	h = newHarness(t, nil, bootstrappedShard("s0", "0-inf"),
		unfinished("manual-diff", multigresv1alpha1.BackupKindDifferential))
	h.reconcile()
	if n := len(h.backups()); n != 1 {
		t.Fatalf("differential while differential runs: attempts = %d, want 1", n)
	}
}

// T12: finished attempts beyond the history limit are deleted, oldest first.
func TestHistoryLimit(t *testing.T) {
	var objs []client.Object
	objs = append(objs, bootstrappedShard("s0", "0-inf"))
	for i := range 4 {
		scheduled := due.Add(-time.Duration(4-i) * 24 * time.Hour)
		b := &multigresv1alpha1.MultigresBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      AttemptName(sname, target, scheduled, 1),
				Namespace: ns,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: multigresv1alpha1.GroupVersion.String(),
					Kind:       "MultigresBackupSchedule", Name: sname, UID: types.UID("sched-uid"),
					Controller: ptr.To(true),
				}},
			},
			Spec: multigresv1alpha1.MultigresBackupSpec{
				ClusterRef:    multigresv1alpha1.BackupClusterReference{Name: "mgc"},
				Database:      target.Database,
				TableGroup:    target.TableGroup,
				Shard:         target.Shard,
				Type:          multigresv1alpha1.BackupKindDifferential,
				ScheduledTime: ptr.To(metav1.NewTime(scheduled)),
				Attempt:       1,
			},
			Status: multigresv1alpha1.MultigresBackupStatus{
				Phase:          multigresv1alpha1.BackupPhaseSucceeded,
				CompletionTime: ptr.To(metav1.NewTime(scheduled.Add(time.Hour))),
			},
		}
		objs = append(objs, b)
	}
	h := newHarness(t, func(s *multigresv1alpha1.MultigresBackupSchedule) {
		s.Spec.Suspend = true // only prune
	}, objs...)
	h.reconcile()

	names := map[string]bool{}
	for _, b := range h.backups() {
		names[b.Name] = true
	}
	if len(names) != 3 {
		t.Fatalf("kept %d attempts, want 3: %v", len(names), names)
	}
	oldest := AttemptName(sname, target, due.Add(-4*24*time.Hour), 1)
	if names[oldest] {
		t.Fatalf("oldest attempt %s was kept", oldest)
	}
}

func TestSuspend(t *testing.T) {
	h := newHarness(t, func(s *multigresv1alpha1.MultigresBackupSchedule) {
		s.Spec.Suspend = true
	}, bootstrappedShard("s0", "0-inf"))
	h.reconcile()
	if n := len(h.backups()); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}
}

func TestAllShardsAndTargets(t *testing.T) {
	h := newHarness(t, nil, bootstrappedShard("s0", "0-inf"), bootstrappedShard("s1", "1"))
	h.reconcile()
	if n := len(h.backups()); n != 2 {
		t.Fatalf("all shards: attempts = %d, want 2", n)
	}

	h = newHarness(t, func(s *multigresv1alpha1.MultigresBackupSchedule) {
		s.Spec.Targets = []multigresv1alpha1.BackupTarget{
			target,
			{Database: "postgres", TableGroup: "default", Shard: "missing"},
		}
	}, bootstrappedShard("s0", "0-inf"), bootstrappedShard("s1", "1"))
	h.reconcile()
	if n := len(h.backups()); n != 1 {
		t.Fatalf("targeted: attempts = %d, want 1", n)
	}
	cond := meta.FindStatusCondition(h.schedule().Status.Conditions, "Ready")
	if cond == nil || cond.Reason != multigresv1alpha1.BackupScheduleReasonTargetNotFound {
		t.Fatalf("Ready = %+v, want TargetNotFound", cond)
	}
}

func TestInvalidSchedule(t *testing.T) {
	for _, spec := range []string{"not cron", "CRON_TZ=Asia/Singapore 0 4 * * *", "0 0 4 * * *"} {
		t.Run(spec, func(t *testing.T) {
			h := newHarness(t, func(s *multigresv1alpha1.MultigresBackupSchedule) {
				s.Spec.Schedule = spec
			}, bootstrappedShard("s0", "0-inf"))
			h.reconcile()
			cond := meta.FindStatusCondition(h.schedule().Status.Conditions, "Ready")
			if cond == nil || cond.Reason != multigresv1alpha1.BackupScheduleReasonInvalidSchedule {
				t.Fatalf("Ready = %+v", cond)
			}
		})
	}
}

func TestMostRecent(t *testing.T) {
	daily, _ := parseSchedule("0 4 * * *")
	minutely, _ := parseSchedule("* * * * *")
	monthly, _ := parseSchedule("0 4 1 * *")
	now := time.Date(2026, 11, 7, 10, 30, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		since time.Time
		want  time.Time
		s     interface{ Next(time.Time) time.Time }
	}{
		{"daily", now.Add(-72 * time.Hour), time.Date(2026, 11, 7, 4, 0, 0, 0, time.UTC), daily},
		{"daily since after last", time.Date(2026, 11, 7, 5, 0, 0, 0, time.UTC), time.Time{}, daily},
		{"minutely, long history", now.Add(-365 * 24 * time.Hour), now, minutely},
		{"monthly", now.Add(-90 * 24 * time.Hour), time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC), monthly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := mostRecent(tc.s, tc.since, now)
			if !got.Equal(tc.want) {
				t.Fatalf("mostRecent = %v, want %v", got, tc.want)
			}
		})
	}
}
