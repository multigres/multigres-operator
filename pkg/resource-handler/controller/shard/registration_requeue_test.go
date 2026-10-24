package shard

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// fakeClock lets a test drive recordNotConverged's elapsed-time math without
// sleeping. Set r.Clock = clk.now.
type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// wantDelayRange mirrors readinessBackoffDelay's own clamp so a change to
// that clamp is caught here rather than only against itself.
func wantDelayRange(elapsed time.Duration) (min, max time.Duration) {
	min = elapsed
	if min < readinessBackoffMinDelay {
		min = readinessBackoffMinDelay
	} else if min > readinessBackoffMaxDelay {
		min = readinessBackoffMaxDelay
	}
	max = time.Duration(float64(min) * 1.2)
	return min, max
}

// TestReadinessBackoffDelayClampsElapsedTime pins the clamp. The lower bound
// matters as much as the upper one: a delay that could return zero would
// reinstate the defect this exists to fix, since a zero RequeueAfter means no
// requeue at all rather than an immediate one.
func TestReadinessBackoffDelayClampsElapsedTime(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		elapsed  time.Duration
		min, max time.Duration
	}{
		{elapsed: 0, min: 5 * time.Second, max: 6 * time.Second},
		{elapsed: 3 * time.Second, min: 5 * time.Second, max: 6 * time.Second},
		{elapsed: 5 * time.Second, min: 5 * time.Second, max: 6 * time.Second},
		{elapsed: 30 * time.Second, min: 30 * time.Second, max: 36 * time.Second},
		{elapsed: 59 * time.Second, min: 59 * time.Second, max: 70800 * time.Millisecond},
		{elapsed: time.Minute, min: time.Minute, max: 72 * time.Second},
		{elapsed: 70 * time.Second, min: time.Minute, max: 72 * time.Second},
		{elapsed: time.Hour, min: time.Minute, max: 72 * time.Second},
	} {
		// Jitter is random, so the bound has to hold across repeats rather
		// than on one draw.
		for range 200 {
			got := readinessBackoffDelay(tc.elapsed)
			if got < tc.min || got > tc.max {
				t.Fatalf("elapsed=%v: delay %v outside [%v, %v]",
					tc.elapsed, got, tc.min, tc.max)
			}
		}
	}
}

// TestReadinessBackoffDelayIsNeverZero is the one that dies if the clamp is
// removed. A zero duration is not "retry immediately", it is "do not
// requeue", which is exactly how a shard ends up stranded not-converged.
func TestReadinessBackoffDelayIsNeverZero(t *testing.T) {
	t.Parallel()

	for _, elapsed := range []time.Duration{
		-5 * time.Second, -1, 0, time.Second, 5 * time.Second,
		30 * time.Second, time.Minute, time.Hour,
	} {
		for range 50 {
			if got := readinessBackoffDelay(elapsed); got <= 0 {
				t.Fatalf("elapsed=%v produced a non-positive delay %v, "+
					"which controller-runtime reads as no requeue", elapsed, got)
			}
		}
	}
}

// TestReadinessBackoffDelayJitters guards the fleet-lockstep property: a
// constant delay would have every shard that lost convergence together retry
// in lockstep against the topology server that just came back.
func TestReadinessBackoffDelayJitters(t *testing.T) {
	t.Parallel()

	seen := map[time.Duration]bool{}
	for range 200 {
		seen[readinessBackoffDelay(30*time.Second)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("only %d distinct delays across 200 draws; jitter is not applied", len(seen))
	}
}

// shardNamed is the minimum a strike counter reads.
func shardNamed(ns, name string) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
	}
}

// TestPostureStrikesLeaveNoEntryOnceSettled is what deleting on settle
// actually buys: a map's table is sized by its peak simultaneous entries and
// does not shrink on delete, so writing zero instead kept an entry for every
// shard the process had ever reconciled.
func TestPostureStrikesLeaveNoEntryOnceSettled(t *testing.T) {
	t.Parallel()

	r := &ShardReconciler{}
	for i := range 1000 {
		s := shardNamed("ns", fmt.Sprintf("shard-%d", i))
		r.recordPostureObservation(s, true)
		r.recordPostureObservation(s, false)
	}

	if got := len(r.postureStrikes); got != 0 {
		t.Fatalf("a thousand shards seen and settled left %d entries, want 0", got)
	}
}

// TestPostureStrikesDoNotSurviveRecreation pins the intended semantic: a
// recreated Shard (a different object at the same namespace/name) opens at
// strike 1, not at whatever count its predecessor left. Posture strikes gate
// posture.Apply (when an unsettled observation is accepted into status); they
// do not pick a requeue delay, that is notConvergedSince's job.
func TestPostureStrikesDoNotSurviveRecreation(t *testing.T) {
	t.Parallel()

	r := &ShardReconciler{}
	s := shardNamed("ns", "shard-0")

	for range 5 {
		r.recordPostureObservation(s, true)
	}
	if got := r.recordPostureObservation(s, false); got != 0 {
		t.Fatalf("a settled observation reported %d strikes, want 0", got)
	}

	// The replacement is a different object at the same key, which is what
	// the tablegroup controller creates after a Shard is deleted.
	if got := r.recordPostureObservation(shardNamed("ns", "shard-0"), true); got != 1 {
		t.Fatalf("a recreated shard opened at %d strikes, want 1", got)
	}
}

// TestPostureStrikesCountConsecutiveUnsettled pins what the counter is for,
// so settling on delete cannot be "fixed" into never counting at all.
func TestPostureStrikesCountConsecutiveUnsettled(t *testing.T) {
	t.Parallel()

	r := &ShardReconciler{}
	s := shardNamed("ns", "shard-0")
	for want := 1; want <= 3; want++ {
		if got := r.recordPostureObservation(s, true); got != want {
			t.Fatalf("consecutive unsettled observation %d reported %d strikes", want, got)
		}
	}
	// Shards are counted independently, which is the only reason the map has
	// keys at all.
	if got := r.recordPostureObservation(shardNamed("ns", "other"), true); got != 1 {
		t.Fatalf("a second shard opened at %d strikes, want 1", got)
	}
	if got := r.recordPostureObservation(s, true); got != 4 {
		t.Fatalf("the first shard reported %d strikes after a second shard, want 4", got)
	}
}

// TestNotConvergedSinceLeavesNoEntryOnceSettled mirrors
// TestPostureStrikesLeaveNoEntryOnceSettled for the not-converged-since map: a
// shard deleted mid-backoff must not leak its entry, and neither must one
// that simply converges.
func TestNotConvergedSinceLeavesNoEntryOnceSettled(t *testing.T) {
	t.Parallel()

	r := &ShardReconciler{}
	for i := range 1000 {
		s := shardNamed("ns", fmt.Sprintf("shard-%d", i))
		r.recordNotConverged(s, true)
		r.recordNotConverged(s, false)
	}

	if got := len(r.notConvergedSince); got != 0 {
		t.Fatalf("a thousand shards seen and settled left %d entries, want 0", got)
	}
}

// TestNotConvergedSinceTracksElapsedTime pins the elapsed-time semantics: the
// first not-converged observation opens the clock, later ones read the time
// since then rather than a per-call count, and a settled observation clears
// it so a later not-converged spell starts over rather than resuming.
func TestNotConvergedSinceTracksElapsedTime(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	r := &ShardReconciler{Clock: clk.now}
	s := shardNamed("ns", "shard-0")

	if got := r.recordNotConverged(s, true); got != 0 {
		t.Fatalf("first not-converged observation reported elapsed %v, want 0", got)
	}
	clk.advance(37 * time.Second)
	if got := r.recordNotConverged(s, true); got != 37*time.Second {
		t.Fatalf("second not-converged observation reported elapsed %v, want 37s", got)
	}
	// A burst of same-instant calls (a wave of unrelated pod events) must not
	// itself advance the elapsed time.
	if got := r.recordNotConverged(s, true); got != 37*time.Second {
		t.Fatalf(
			"third not-converged observation (no time passed) reported elapsed %v, want 37s", got,
		)
	}

	if got := r.recordNotConverged(s, false); got != 0 {
		t.Fatalf("a settled observation reported elapsed %v, want 0", got)
	}
	if got := r.recordNotConverged(s, true); got != 0 {
		t.Fatalf("a fresh not-converged spell reported elapsed %v, want 0 (not resumed)", got)
	}
}

// TestForgetStrikesDropsBothCounters pins that a Shard's strike entries do
// not survive forgetStrikes, in either counter.
func TestForgetStrikesDropsBothCounters(t *testing.T) {
	t.Parallel()

	r := &ShardReconciler{}
	s := shardNamed("ns", "shard-0")
	r.recordPostureObservation(s, true)
	r.recordNotConverged(s, true)

	r.forgetStrikes(s.Namespace, s.Name)

	if got := len(r.postureStrikes); got != 0 {
		t.Fatalf("posture strikes: %d entries survived forgetStrikes, want 0", got)
	}
	if got := len(r.notConvergedSince); got != 0 {
		t.Fatalf("not-converged-since: %d entries survived forgetStrikes, want 0", got)
	}
}

// TestHandleDeletionForgetsStrikes drives the deletion cleanup through the
// real controller path (handleDeletion) rather than calling forgetStrikes
// directly, so a regression that stops handleDeletion from reaching it is
// caught here rather than only in the helper's own unit test.
func TestHandleDeletionForgetsStrikes(t *testing.T) {
	shard := postureTestShard()
	shard.Finalizers = []string{shardFinalizer}
	now := metav1.Now()
	shard.DeletionTimestamp = &now

	scheme := postureTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()
	r := &ShardReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
	}

	key := fmt.Sprintf("%s/%s", shard.Namespace, shard.Name)
	r.postureStrikes = map[string]int{key: 2}
	r.notConvergedSince = map[string]time.Time{key: time.Now()}

	if _, err := r.handleDeletion(t.Context(), shard); err != nil {
		t.Fatalf("handleDeletion() error = %v", err)
	}

	if _, ok := r.postureStrikes[key]; ok {
		t.Errorf("posture strikes entry for %s survived handleDeletion", key)
	}
	if _, ok := r.notConvergedSince[key]; ok {
		t.Errorf("not-converged-since entry for %s survived handleDeletion", key)
	}
}

// TestReconcileForgetsStrikesOnNotFound drives the not-found cleanup through
// Reconcile itself: a Shard already gone from the API server (the common case
// once handleDeletion above has already run and removed the finalizer) must
// still have its strike entries dropped, as a backstop.
func TestReconcileForgetsStrikesOnNotFound(t *testing.T) {
	scheme := postureTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := &ShardReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
	}

	key := "default/gone-shard"
	r.postureStrikes = map[string]int{key: 3}
	r.notConvergedSince = map[string]time.Time{key: time.Now()}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "gone-shard"},
	}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	if _, ok := r.postureStrikes[key]; ok {
		t.Errorf("posture strikes entry for %s survived Reconcile on a missing Shard", key)
	}
	if _, ok := r.notConvergedSince[key]; ok {
		t.Errorf("not-converged-since entry for %s survived Reconcile on a missing Shard", key)
	}
}

// gateTestReconciler is postureTestReconciler plus a Pod status subresource,
// for tests that read back the PoolerDataReady gate condition a real
// apiserver would only apply through Status().Patch.
func gateTestReconciler(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	rpc rpcclient.MultipoolerClient,
	objects ...client.Object,
) (*ShardReconciler, client.Client) {
	t.Helper()
	scheme := postureTestScheme(t)
	allObjects := append([]client.Object{shard}, objects...)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(allObjects...).
		WithStatusSubresource(&multigresv1alpha1.Shard{}, &corev1.Pod{}).
		Build()
	return &ShardReconciler{
		Client:          c,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(20),
		PoolerClients:   poolerclient.Static(rpc),
		CreateTopoStore: newMemoryTopoFactory(),
	}, c
}

// registeredReplica registers id in store as a non-primary member of shard,
// with an RPC status that reads Ready via poolerReadiness: initialized,
// accepting connections, cohort-eligible, and a committed member of its own
// single-member rule. Standing in for "this pooler has finished bootstrapping
// and has somewhere to belong," independent of whether anyone in the shard has
// been elected primary.
func registeredReplica(
	t *testing.T,
	store topoclient.Store,
	rpc *rpcclient.FakeClient,
	shard *multigresv1alpha1.Shard,
	cell, name string,
) topoclient.ComponentID {
	t.Helper()
	id := &clustermetadata.ID{Cell: cell, Name: name}
	if err := store.RegisterMultipooler(t.Context(), &clustermetadata.Multipooler{
		Id:       id,
		Hostname: name,
		ShardKey: &clustermetadata.ShardKey{
			Database:   string(shard.Spec.DatabaseName),
			TableGroup: string(shard.Spec.TableGroupName),
			Shard:      string(shard.Spec.ShardName),
		},
		RoutingState: &clustermetadata.RoutingState{
			Role: clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
		},
	}, false); err != nil {
		t.Fatalf("register pooler %s: %v", name, err)
	}

	componentID := topoclient.ComponentIDString(id)
	rpc.SetStatusResponse(componentID, readyStatusResponse(id))
	return componentID
}

// readyStatusResponse is a fully posture-ready StatusResponse for id: a
// replica, initialized, accepting connections, cohort-eligible, and a
// committed member of its own single-member rule.
func readyStatusResponse(id *clustermetadata.ID) *multipoolermanagerdatapb.StatusResponse {
	return &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized:  true,
			PostgresReady:  true,
			PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY,
		},
		AvailabilityStatus: &clustermetadata.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadata.CohortEligibilityStatus{
				Signal: clustermetadata.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
			},
		},
		ConsensusStatus: &clustermetadata.ConsensusStatus{
			Id: id,
			CurrentPosition: &clustermetadata.PoolerPosition{
				Position: &clustermetadata.RulePosition{
					Decision: &clustermetadata.ShardRule{
						RuleNumber:       &clustermetadata.RuleNumber{CoordinatorTerm: 1},
						LeaderId:         id,
						CohortMembers:    []*clustermetadata.ID{id},
						DurabilityPolicy: topoclient.AtLeastN(1),
					},
				},
			},
		},
	}
}

// notYetSettledReplica registers id in store as a non-primary member of
// shard, with an RPC status that is initialized, accepting connections, and
// cohort-eligible, but has not yet committed a rule naming any cohort
// members. Standing in for a shard mid-bootstrap where every pooler has
// registered but multiorch has not yet elected a primary or committed a
// durability rule: nobody is a "postgres primary" (so nothing looks
// inconsistent) and nobody has an unmatched topology entry or an unreadable
// RPC (so nothing looks incomplete), but nobody is ready either.
func notYetSettledReplica(
	t *testing.T,
	store topoclient.Store,
	rpc *rpcclient.FakeClient,
	shard *multigresv1alpha1.Shard,
	cell, name string,
) *clustermetadata.ID {
	t.Helper()
	id := &clustermetadata.ID{Cell: cell, Name: name}
	if err := store.RegisterMultipooler(t.Context(), &clustermetadata.Multipooler{
		Id:       id,
		Hostname: name,
		ShardKey: &clustermetadata.ShardKey{
			Database:   string(shard.Spec.DatabaseName),
			TableGroup: string(shard.Spec.TableGroupName),
			Shard:      string(shard.Spec.ShardName),
		},
		RoutingState: &clustermetadata.RoutingState{
			Role: clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
		},
	}, false); err != nil {
		t.Fatalf("register pooler %s: %v", name, err)
	}

	componentID := topoclient.ComponentIDString(id)
	rpc.SetStatusResponse(componentID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			IsInitialized:  true,
			PostgresReady:  true,
			PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY,
		},
		AvailabilityStatus: &clustermetadata.AvailabilityStatus{
			CohortEligibilityStatus: &clustermetadata.CohortEligibilityStatus{
				Signal: clustermetadata.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
			},
		},
		// No ConsensusStatus: nothing has been committed yet, so
		// committedCohortContains is false for everyone.
	})
	return id
}

// TestReconcilePostureConvergedShardReturnsZero pins the hot-loop fix's own
// invariant: a converged shard returns 0 and leaves no map entry. It includes
// a multiorch pod built from BuildMultiorchDeployment, since a shard's
// multiorch pod carries the same four identity labels as its pool pods, so a
// selector that forgets to scope to pool pods seeds it AwaitingRegistration
// forever and this shard never returns 0.
func TestReconcilePostureConvergedShardReturnsZero(t *testing.T) {
	shard := postureTestShard()
	shard.Labels[metadata.LabelMultigresDatabase] = "database"
	shard.Labels[metadata.LabelMultigresTableGroup] = "table-group"
	shard.Labels[metadata.LabelMultigresShard] = "0"

	dep, err := BuildMultiorchDeployment(shard, "cell1", postureTestScheme(t))
	if err != nil {
		t.Fatalf("BuildMultiorchDeployment() error = %v", err)
	}
	orch := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "multiorch-abc", Namespace: shard.Namespace, Labels: dep.Spec.Template.Labels,
	}}

	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()
	rpc := rpcclient.NewFakeClient()
	registeredReplica(t, store, rpc, shard, "cell1", "pooler-0")

	pool := postureTestPod()
	r, _ := postureTestReconciler(t, shard, rpc, pool, orch)

	delay, err := r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("reconcilePosture() error = %v", err)
	}
	if delay != 0 {
		t.Errorf("delay = %v, want 0 for a converged shard", delay)
	}
	key := fmt.Sprintf("%s/%s", shard.Namespace, shard.Name)
	if _, ok := r.postureStrikes[key]; ok {
		t.Errorf("posture strikes entry left for a converged shard")
	}
	if _, ok := r.notConvergedSince[key]; ok {
		t.Errorf("not-converged-since entry left for a converged shard")
	}
	if conditionIsFalse(shard.Status.Conditions, "PostureConsistent") {
		t.Errorf("conditions = %#v, want no failure for a converged shard", shard.Status.Conditions)
	}
}

// TestReconcilePostureAcceptedIncompleteObservationStillRequeues covers an
// accepted-but-not-ready observation: a Status RPC failure makes a pod
// UNKNOWN, which makes it Incomplete, which makes the shard unsettled. After
// the strike threshold the observation is accepted into status, but the
// failing pod has also never reached posture readiness, so this must keep
// requesting a requeue rather than stranding it until the 10h resync.
func TestReconcilePostureAcceptedIncompleteObservationStillRequeues(t *testing.T) {
	shard := postureTestShard()
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()
	rpc := rpcclient.NewFakeClient()
	registeredReplica(t, store, rpc, shard, "cell1", "pooler-0")
	bad := registeredReplica(t, store, rpc, shard, "cell1", "pooler-1")
	rpc.Errors[bad] = errors.New("dial: connection refused")

	p0 := postureTestPod()
	p1 := postureTestPod()
	p1.Name = "pooler-1"
	r, _ := postureTestReconciler(t, shard, rpc, p0, p1)

	first, err := r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("first reconcilePosture() error = %v", err)
	}
	if first != postureDebounceRequeueDelay {
		t.Errorf("first delay = %v, want the %v debounce", first, postureDebounceRequeueDelay)
	}

	for pass := 2; pass <= 4; pass++ {
		delay, err := r.reconcilePosture(t.Context(), store, shard, rpc)
		if err != nil {
			t.Fatalf("pass %d reconcilePosture() error = %v", pass, err)
		}
		if delay <= 0 {
			t.Errorf(
				"pass %d: delay = %v, want non-zero (RPC failure still unresolved)", pass, delay,
			)
		}
	}
}

// TestReconcilePostureAcceptedMismatchAndNotReadyStillRequeues covers a
// mismatch compounded with a pod that has never reached posture readiness at
// all (this mock pooler never reports IsInitialized/PostgresReady): a
// replica reporting postgres PRIMARY is a role mismatch. After the strike
// threshold it is accepted as PostureConsistent=False, and this must keep
// requesting a requeue.
func TestReconcilePostureAcceptedMismatchAndNotReadyStillRequeues(t *testing.T) {
	shard := postureTestShard()
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()
	rpc := rpcclient.NewFakeClient()
	id := &clustermetadata.ID{Cell: "cell1", Name: "pooler-0"}
	if err := store.RegisterMultipooler(t.Context(), &clustermetadata.Multipooler{
		Id:       id,
		Hostname: "pooler-0",
		ShardKey: &clustermetadata.ShardKey{
			Database:   string(shard.Spec.DatabaseName),
			TableGroup: string(shard.Spec.TableGroupName),
			Shard:      string(shard.Spec.ShardName),
		},
		RoutingState: &clustermetadata.RoutingState{
			Role: clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
		},
	}, false); err != nil {
		t.Fatalf("register pooler: %v", err)
	}
	componentID := topoclient.ComponentIDString(id)
	rpc.SetStatusResponse(componentID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
		},
	})
	r, _ := postureTestReconciler(t, shard, rpc, postureTestPod())

	first, err := r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("first reconcilePosture() error = %v", err)
	}
	if first != postureDebounceRequeueDelay {
		t.Errorf("first delay = %v, want the %v debounce", first, postureDebounceRequeueDelay)
	}

	for pass := 2; pass <= 4; pass++ {
		delay, err := r.reconcilePosture(t.Context(), store, shard, rpc)
		if err != nil {
			t.Fatalf("pass %d reconcilePosture() error = %v", pass, err)
		}
		if delay <= 0 {
			t.Errorf("pass %d: delay = %v, want non-zero (mismatch still unresolved)", pass, delay)
		}
		if !conditionIsFalse(shard.Status.Conditions, "PostureConsistent") {
			t.Errorf("pass %d: conditions = %#v, want PostureConsistent=False once accepted",
				pass, shard.Status.Conditions)
		}
	}
}

// TestReconcilePostureAcceptedMismatchWithReadyPodsStillRequeues is the other
// half of the accepted-mismatch case: the pod itself is fully ready
// (registeredReplica: initialized, accepting connections, cohort-eligible,
// a committed cohort member), and the only thing wrong is that it reports
// postgres PRIMARY while topology still has it as REPLICA. anyPodNotReady is
// false throughout, so unsettled is the only thing driving this requeue: a
// shard where every pod is ready but multiorch and postgres disagree about
// who is primary must not be left Degraded until the 10h resync once that
// disagreement is accepted into status.
func TestReconcilePostureAcceptedMismatchWithReadyPodsStillRequeues(t *testing.T) {
	shard := postureTestShard()
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()
	rpc := rpcclient.NewFakeClient()
	id := registeredReplica(t, store, rpc, shard, "cell1", "pooler-0")
	rpc.StatusResponses[id].Response.Status.PostgresStatus = multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY
	r, _ := postureTestReconciler(t, shard, rpc, postureTestPod())

	first, err := r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("first reconcilePosture() error = %v", err)
	}
	if first != postureDebounceRequeueDelay {
		t.Errorf("first delay = %v, want the %v debounce", first, postureDebounceRequeueDelay)
	}

	for pass := 2; pass <= 4; pass++ {
		delay, err := r.reconcilePosture(t.Context(), store, shard, rpc)
		if err != nil {
			t.Fatalf("pass %d reconcilePosture() error = %v", pass, err)
		}
		if delay <= 0 {
			t.Errorf(
				"pass %d: delay = %v, want non-zero (mismatch still unresolved, though the pod is ready)",
				pass,
				delay,
			)
		}
		if !conditionIsFalse(shard.Status.Conditions, "PostureConsistent") {
			t.Errorf("pass %d: conditions = %#v, want PostureConsistent=False once accepted",
				pass, shard.Status.Conditions)
		}
	}
}

// TestReconcilePostureRequeuesWhileAPodAwaitsItsPooler covers a shard with one
// settled, registered pooler and one managed pod that never registers: it
// must keep requesting a requeue, and the requested delay must grow with
// elapsed wall-clock time rather than sit at a fixed floor forever.
//
// Drives a fake clock directly so the growth assertion is exact rather than
// "second draw happened to be bigger": elapsed time is measured directly
// regardless of how many reconcile passes it took to get there, so a mutation
// that turns the backoff back into a per-pass count cannot pass by chance.
func TestReconcilePostureRequeuesWhileAPodAwaitsItsPooler(t *testing.T) {
	shard := postureTestShard()
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	registeredReplica(t, store, rpc, shard, "cell1", "pooler-0")

	settledPod := postureTestPod()
	awaitingPod := postureTestPod()
	awaitingPod.Name = "pooler-1"

	r, _ := postureTestReconciler(t, shard, rpc, settledPod, awaitingPod)
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	r.Clock = clk.now

	for _, tc := range []struct {
		advance time.Duration
		elapsed time.Duration
	}{
		{advance: 0, elapsed: 0},
		{advance: 20 * time.Second, elapsed: 20 * time.Second},
		{advance: 50 * time.Second, elapsed: 70 * time.Second},
	} {
		clk.advance(tc.advance)
		delay, err := r.reconcilePosture(t.Context(), store, shard, rpc)
		if err != nil {
			t.Fatalf("reconcilePosture() error = %v", err)
		}
		min, max := wantDelayRange(tc.elapsed)
		if delay < min || delay > max {
			t.Errorf("at elapsed=%v: delay = %v, want in [%v, %v]", tc.elapsed, delay, min, max)
		}
	}
}

// TestReconcilePostureBacksOffThenClearsOnceAPrimaryIsElected covers the
// bring-up state a minimal cluster passes through before a primary is
// elected: every managed pod has a topology entry (nothing is "awaiting
// registration" in the topology-match sense), and nothing is inconsistent or
// incomplete (Evaluate only compares observed postgres primaries against
// topology roles and finds none of either), but nobody has committed a cohort
// membership because no primary has been elected yet.
//
// Drives a fake clock through several passes to pin the actual delay range,
// checks the PoolerDataReady gate stays False while waiting, then flips the
// fixture to a committed primary and checks the requeue stops, the gate goes
// True, and the not-converged-since entry is gone.
func TestReconcilePostureBacksOffThenClearsOnceAPrimaryIsElected(t *testing.T) {
	shard := postureTestShard()
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	id0 := notYetSettledReplica(t, store, rpc, shard, "cell1", "pooler-0")
	id1 := notYetSettledReplica(t, store, rpc, shard, "cell1", "pooler-1")

	pod0 := postureTestPod()
	pod1 := postureTestPod()
	pod1.Name = "pooler-1"

	r, c := gateTestReconciler(t, shard, rpc, pod0, pod1)
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	r.Clock = clk.now

	for _, tc := range []struct {
		advance time.Duration
		elapsed time.Duration
	}{
		{advance: 0, elapsed: 0},
		{advance: 20 * time.Second, elapsed: 20 * time.Second},
		{advance: 50 * time.Second, elapsed: 70 * time.Second},
	} {
		clk.advance(tc.advance)
		delay, err := r.reconcilePosture(t.Context(), store, shard, rpc)
		if err != nil {
			t.Fatalf("reconcilePosture() error = %v", err)
		}
		min, max := wantDelayRange(tc.elapsed)
		if delay < min || delay > max {
			t.Errorf("at elapsed=%v: delay = %v, want in [%v, %v]", tc.elapsed, delay, min, max)
		}
	}

	for _, pod := range []*corev1.Pod{pod0, pod1} {
		got := &corev1.Pod{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), got); err != nil {
			t.Fatalf("get pod %s: %v", pod.Name, err)
		}
		condition := findPoolerReadinessCondition(got.Status.Conditions)
		if condition == nil || condition.Status != corev1.ConditionFalse {
			t.Errorf("pod %s readiness condition = %#v, want False while waiting for a primary",
				pod.Name, condition)
		}
	}

	// The fixture reaches a settled state: both poolers commit a rule naming
	// pooler-0 as leader, so both are cohort-eligible members of the same
	// durability rule and pooler-0's postgres reports PRIMARY, matching the
	// topology role a leader-designate needs.
	if err := store.RegisterMultipooler(t.Context(), &clustermetadata.Multipooler{
		Id:       id0,
		Hostname: "pooler-0",
		ShardKey: &clustermetadata.ShardKey{
			Database:   string(shard.Spec.DatabaseName),
			TableGroup: string(shard.Spec.TableGroupName),
			Shard:      string(shard.Spec.ShardName),
		},
		RoutingState: &clustermetadata.RoutingState{
			Role: clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
		},
	}, true); err != nil {
		t.Fatalf("promote pooler-0 in topology: %v", err)
	}
	rule := &clustermetadata.ShardRule{
		RuleNumber:       &clustermetadata.RuleNumber{CoordinatorTerm: 1},
		LeaderId:         id0,
		CohortMembers:    []*clustermetadata.ID{id0, id1},
		DurabilityPolicy: topoclient.AtLeastN(1),
	}
	for _, elected := range []struct {
		id      *clustermetadata.ID
		primary bool
	}{{id0, true}, {id1, false}} {
		status := multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY
		if elected.primary {
			status = multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY
		}
		componentID := topoclient.ComponentIDString(elected.id)
		rpc.SetStatusResponse(componentID, &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized:  true,
				PostgresReady:  true,
				PostgresStatus: status,
			},
			AvailabilityStatus: &clustermetadata.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadata.CohortEligibilityStatus{
					Signal: clustermetadata.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
			ConsensusStatus: &clustermetadata.ConsensusStatus{
				Id: elected.id,
				CurrentPosition: &clustermetadata.PoolerPosition{
					Position: &clustermetadata.RulePosition{Decision: rule},
				},
			},
		})
	}

	clk.advance(time.Second)
	delay, err := r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("reconcilePosture() after election error = %v", err)
	}
	if delay != 0 {
		t.Errorf("delay after a primary is elected = %v, want 0", delay)
	}
	key := fmt.Sprintf("%s/%s", shard.Namespace, shard.Name)
	if _, ok := r.notConvergedSince[key]; ok {
		t.Errorf("not-converged-since entry left after a primary is elected")
	}
	if conditionIsFalse(shard.Status.Conditions, "PostureConsistent") {
		t.Errorf(
			"conditions = %#v, want no failure once a primary is elected",
			shard.Status.Conditions,
		)
	}

	for _, pod := range []*corev1.Pod{pod0, pod1} {
		got := &corev1.Pod{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), got); err != nil {
			t.Fatalf("get pod %s: %v", pod.Name, err)
		}
		condition := findPoolerReadinessCondition(got.Status.Conditions)
		if condition == nil || condition.Status != corev1.ConditionTrue {
			t.Errorf("pod %s readiness condition = %#v, want True once a primary is elected",
				pod.Name, condition)
		}
	}
}
