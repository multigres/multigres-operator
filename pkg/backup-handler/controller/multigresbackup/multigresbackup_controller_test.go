package multigresbackup

import (
	"context"
	"testing"
	"time"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/backup-handler/multiadmin"
)

const ns = "proj"

type harness struct {
	t     *testing.T
	c     client.Client
	fake  *multiadmin.Fake
	now   time.Time
	r     *Reconciler
	scope *runtime.Scheme
}

func newHarness(t *testing.T, objs ...client.Object) *harness {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mgc", Namespace: ns, UID: "cluster-uid"},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(append([]client.Object{cluster}, objs...)...).
		WithStatusSubresource(&multigresv1alpha1.MultigresBackup{}).
		Build()
	h := &harness{
		t:     t,
		c:     c,
		fake:  multiadmin.NewFake(),
		now:   time.Date(2026, 11, 7, 4, 0, 0, 0, time.UTC),
		scope: scheme,
	}
	h.r = h.newReconciler()
	return h
}

// newReconciler returns a fresh reconciler sharing the client and fake, as
// after an operator restart.
func (h *harness) newReconciler() *Reconciler {
	return &Reconciler{
		Client:     h.c,
		Scheme:     h.scope,
		Recorder:   record.NewFakeRecorder(100),
		Multiadmin: multiadmin.FakeFactory{Fake: h.fake},
		Now:        func() time.Time { return h.now },
	}
}

func (h *harness) reconcile(name string) (ctrl.Result, error) {
	h.t.Helper()
	return h.r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: ns, Name: name},
	})
}

func (h *harness) get(name string) *multigresv1alpha1.MultigresBackup {
	h.t.Helper()
	b := &multigresv1alpha1.MultigresBackup{}
	if err := h.c.Get(
		context.Background(),
		client.ObjectKey{Namespace: ns, Name: name},
		b,
	); err != nil {
		h.t.Fatal(err)
	}
	return b
}

func (h *harness) setStatus(name string, mutate func(*multigresv1alpha1.MultigresBackupStatus)) {
	h.t.Helper()
	b := h.get(name)
	mutate(&b.Status)
	if err := h.c.Status().Update(context.Background(), b); err != nil {
		h.t.Fatal(err)
	}
}

// reconcileUntil reconciles until the attempt reaches phase or 10 rounds pass.
func (h *harness) reconcileUntil(name string, phase multigresv1alpha1.BackupPhase) {
	h.t.Helper()
	for range 10 {
		if _, err := h.reconcile(name); err != nil {
			h.t.Fatalf("reconcile %s: %v", name, err)
		}
		if h.get(name).Status.Phase == phase {
			return
		}
	}
	h.t.Fatalf(
		"%s: phase %q, want %q (message %q, created %v)",
		name,
		h.get(name).Status.Phase,
		phase,
		h.get(name).Status.Message,
		h.get(name).CreationTimestamp,
	)
}

func newBackup(name string, created time.Time) *multigresv1alpha1.MultigresBackup {
	return &multigresv1alpha1.MultigresBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         ns,
			UID:               types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: multigresv1alpha1.MultigresBackupSpec{
			ClusterRef: multigresv1alpha1.BackupClusterReference{Name: "mgc"},
			Database:   "postgres",
			TableGroup: "default",
			Shard:      "0-inf",
			Type:       multigresv1alpha1.BackupKindFull,
		},
	}
}

// T2: a Pending attempt reaches Running with one Backup call.
func TestTriggerOnce(t *testing.T) {
	h := newHarness(t, newBackup("b1", time.Now()))
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseRunning)
	for range 3 {
		if _, err := h.reconcile("b1"); err != nil {
			t.Fatal(err)
		}
	}

	b := h.get("b1")
	if b.Status.JobID == "" || b.Status.TriggerTime == nil || b.Status.StartTime == nil {
		t.Fatalf("status not filled in: %+v", b.Status)
	}
	calls := h.fake.BackupCalls()
	if len(calls) != 1 {
		t.Fatalf("Backup calls = %d, want 1", len(calls))
	}
	if calls[0].Shard != "0-inf" || calls[0].Type != "full" {
		t.Fatalf("unexpected request %v", calls[0])
	}
	// Manual backups are labeled for the queue and owned by the cluster.
	if b.Labels["multigres.com/shard"] != "0-inf" ||
		b.Labels[multigresv1alpha1.LabelBackupTrigger] != "manual" {
		t.Fatalf("labels = %v", b.Labels)
	}
	if owner := metav1.GetControllerOf(b); owner == nil || owner.Name != "mgc" {
		t.Fatalf("owner = %v", b.OwnerReferences)
	}
}

// T3: found in Triggering without a job ID → TriggerOutcomeUnknown, no call.
func TestTriggeringWithoutJobID(t *testing.T) {
	h := newHarness(t, newBackup("b1", time.Now()))
	h.setStatus("b1", func(s *multigresv1alpha1.MultigresBackupStatus) {
		s.Phase = multigresv1alpha1.BackupPhaseTriggering
	})
	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	b := h.get("b1")
	if b.Status.Phase != multigresv1alpha1.BackupPhaseFailed ||
		b.Status.Reason != multigresv1alpha1.BackupReasonTriggerOutcomeUnknown {
		t.Fatalf("status = %s/%s", b.Status.Phase, b.Status.Reason)
	}
	if n := len(h.fake.BackupCalls()); n != 0 {
		t.Fatalf("Backup calls = %d, want 0", n)
	}
}

// T4: after a restart, a Running attempt polls the same job and never calls
// Backup again; it ends Succeeded with the backup ID.
func TestRestartResumesPolling(t *testing.T) {
	h := newHarness(t, newBackup("b1", time.Now()))
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseRunning)
	jobID := h.get("b1").Status.JobID

	h.r = h.newReconciler()
	h.fake.SetJob(jobID, multiadminpb.JobStatus_JOB_STATUS_RUNNING, "", "")
	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	h.fake.SetJob(jobID, multiadminpb.JobStatus_JOB_STATUS_COMPLETED, "20261107-040002F", "")
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseSucceeded)

	b := h.get("b1")
	if b.Status.BackupID != "20261107-040002F" || b.Status.CompletionTime == nil {
		t.Fatalf("status = %+v", b.Status)
	}
	if n := len(h.fake.BackupCalls()); n != 1 {
		t.Fatalf("Backup calls = %d, want 1", n)
	}
	for _, call := range h.fake.StatusCalls() {
		if call.JobId != jobID {
			t.Fatalf("polled %s, want %s", call.JobId, jobID)
		}
	}

	// I10: nothing changes after the attempt finished.
	before := h.get("b1").ResourceVersion
	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	if after := h.get("b1").ResourceVersion; after != before {
		t.Fatal("finished attempt was modified")
	}
}

// T5: a lost job is polled with shard fields (I6), survives NotFound within
// the grace period, and is marked Lost after it.
func TestLostJob(t *testing.T) {
	h := newHarness(t, newBackup("b1", time.Now()))
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseRunning)
	jobID := h.get("b1").Status.JobID
	h.fake.Lose(jobID)

	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	h.now = h.now.Add(LostGrace / 2)
	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	if p := h.get("b1").Status.Phase; p != multigresv1alpha1.BackupPhaseRunning {
		t.Fatalf("phase = %s within grace, want Running", p)
	}
	h.now = h.now.Add(LostGrace)
	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	b := h.get("b1")
	if b.Status.Phase != multigresv1alpha1.BackupPhaseFailed ||
		b.Status.Reason != multigresv1alpha1.BackupReasonLost {
		t.Fatalf("status = %s/%s", b.Status.Phase, b.Status.Reason)
	}
	for _, call := range h.fake.StatusCalls() {
		if call.Database != "postgres" || call.TableGroup != "default" || call.Shard != "0-inf" {
			t.Fatalf("status call without shard fields: %v", call)
		}
	}
}

// NotFound followed by a successful poll clears the grace timer.
func TestNotFoundRecovers(t *testing.T) {
	h := newHarness(t, newBackup("b1", time.Now()))
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseRunning)
	h.fake.StatusErr = status.Error(codes.NotFound, "pooler not found")
	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	if h.get("b1").Status.NotFoundSince == nil {
		t.Fatal("NotFoundSince not set")
	}
	h.fake.StatusErr = nil
	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	if h.get("b1").Status.NotFoundSince != nil {
		t.Fatal("NotFoundSince not cleared")
	}
}

// I5: an Unavailable poll returns an error for backoff and does not fail
// the attempt.
func TestPollErrorIsTemporary(t *testing.T) {
	h := newHarness(t, newBackup("b1", time.Now()))
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseRunning)
	h.fake.StatusErr = status.Error(codes.Unavailable, "connection refused")
	if _, err := h.reconcile("b1"); err == nil {
		t.Fatal("want error for backoff")
	}
	if p := h.get("b1").Status.Phase; p != multigresv1alpha1.BackupPhaseRunning {
		t.Fatalf("phase = %s, want Running", p)
	}
}

func TestBackupFailed(t *testing.T) {
	h := newHarness(t, newBackup("b1", time.Now()))
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseRunning)
	h.fake.SetJob(
		h.get("b1").Status.JobID,
		multiadminpb.JobStatus_JOB_STATUS_FAILED,
		"",
		"pgbackrest exited 82",
	)
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseFailed)
	b := h.get("b1")
	if b.Status.Reason != multigresv1alpha1.BackupReasonBackupFailed ||
		b.Status.Message != "pgbackrest exited 82" {
		t.Fatalf("status = %s/%s", b.Status.Reason, b.Status.Message)
	}
}

func TestTriggerRejectedAndUnknown(t *testing.T) {
	for _, tc := range []struct {
		code   codes.Code
		reason string
	}{
		{codes.FailedPrecondition, multigresv1alpha1.BackupReasonTriggerRejected},
		{codes.InvalidArgument, multigresv1alpha1.BackupReasonTriggerRejected},
		{codes.Unavailable, multigresv1alpha1.BackupReasonTriggerOutcomeUnknown},
		{codes.DeadlineExceeded, multigresv1alpha1.BackupReasonTriggerOutcomeUnknown},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			h := newHarness(t, newBackup("b1", time.Now()))
			h.fake.BackupErr = status.Error(tc.code, "nope")
			h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseFailed)
			if r := h.get("b1").Status.Reason; r != tc.reason {
				t.Fatalf("reason = %s, want %s", r, tc.reason)
			}
		})
	}
}

// T7: only the attempt created first leaves Pending until it finishes (I1).
func TestPerShardQueue(t *testing.T) {
	t0 := time.Now().Add(-time.Minute)
	h := newHarness(t, newBackup("first", t0), newBackup("second", t0.Add(time.Second)))

	// Reconcile the later one first: it must wait.
	for range 3 {
		if _, err := h.reconcile("second"); err != nil {
			t.Fatal(err)
		}
	}
	h.reconcileUntil("first", multigresv1alpha1.BackupPhaseRunning)
	for range 3 {
		if _, err := h.reconcile("second"); err != nil {
			t.Fatal(err)
		}
	}
	if p := h.get("second").Status.Phase; p != multigresv1alpha1.BackupPhasePending {
		t.Fatalf("second phase = %s while first runs, want Pending", p)
	}
	if n := len(h.fake.BackupCalls()); n != 1 {
		t.Fatalf("Backup calls = %d, want 1", n)
	}

	h.fake.SetJob(
		h.get("first").Status.JobID,
		multiadminpb.JobStatus_JOB_STATUS_COMPLETED,
		"F1",
		"",
	)
	h.reconcileUntil("first", multigresv1alpha1.BackupPhaseSucceeded)
	h.reconcileUntil("second", multigresv1alpha1.BackupPhaseRunning)
	if n := len(h.fake.BackupCalls()); n != 2 {
		t.Fatalf("Backup calls = %d, want 2", n)
	}
}

// A backup for another shard does not wait.
func TestOtherShardDoesNotWait(t *testing.T) {
	t0 := time.Now().Add(-time.Minute)
	other := newBackup("other", t0)
	other.Spec.Shard = "1"
	h := newHarness(t, other, newBackup("b1", t0.Add(time.Second)))
	h.reconcileUntil("other", multigresv1alpha1.BackupPhaseRunning)
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseRunning)
}

// T10: still running after timeoutSeconds → Timeout.
func TestTimeout(t *testing.T) {
	b := newBackup("b1", time.Now())
	b.Spec.TimeoutSeconds = 600
	h := newHarness(t, b)
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseRunning)
	h.fake.SetJob(h.get("b1").Status.JobID, multiadminpb.JobStatus_JOB_STATUS_RUNNING, "", "")
	h.now = h.now.Add(601 * time.Second)
	if _, err := h.reconcile("b1"); err != nil {
		t.Fatal(err)
	}
	if r := h.get("b1").Status.Reason; r != multigresv1alpha1.BackupReasonTimeout {
		t.Fatalf("reason = %s, want Timeout", r)
	}
}

func TestClusterNotFound(t *testing.T) {
	b := newBackup("b1", time.Now())
	b.Spec.ClusterRef.Name = "gone"
	h := newHarness(t, b)
	h.reconcileUntil("b1", multigresv1alpha1.BackupPhaseFailed)
	if r := h.get("b1").Status.Reason; r != multigresv1alpha1.BackupReasonClusterNotFound {
		t.Fatalf("reason = %s", r)
	}
}
