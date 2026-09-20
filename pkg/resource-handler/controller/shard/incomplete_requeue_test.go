package shard

import (
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestIncompleteRequeueDelayBacksOffAndCaps pins the backoff curve. The lower
// bound matters as much as the upper one: a delay that could return zero would
// reinstate the defect this exists to fix, since a zero RequeueAfter means no
// requeue at all rather than an immediate one.
func TestIncompleteRequeueDelayBacksOffAndCaps(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		strikes  int
		min, max time.Duration
	}{
		{strikes: 0, min: 5 * time.Second, max: 6 * time.Second},
		{strikes: 1, min: 5 * time.Second, max: 6 * time.Second},
		{strikes: 2, min: 10 * time.Second, max: 12 * time.Second},
		{strikes: 3, min: 20 * time.Second, max: 24 * time.Second},
		{strikes: 4, min: 40 * time.Second, max: 48 * time.Second},
		{strikes: 5, min: time.Minute, max: 72 * time.Second},
		{strikes: 500, min: time.Minute, max: 72 * time.Second},
	} {
		// Jitter is random, so the bound has to hold across repeats rather
		// than on one draw.
		for range 200 {
			got := incompleteRequeueDelay(tc.strikes)
			if got < tc.min || got > tc.max {
				t.Fatalf("strikes=%d: delay %v outside [%v, %v]",
					tc.strikes, got, tc.min, tc.max)
			}
		}
	}
}

// TestIncompleteRequeueDelayIsNeverZero is the one that dies if the guard is
// removed. A zero duration is not "retry immediately", it is "do not requeue",
// which is exactly how a shard ends up stranded with a short membership list.
func TestIncompleteRequeueDelayIsNeverZero(t *testing.T) {
	t.Parallel()

	for strikes := -5; strikes < 100; strikes++ {
		for range 50 {
			if got := incompleteRequeueDelay(strikes); got <= 0 {
				t.Fatalf("strikes=%d produced a non-positive delay %v, "+
					"which controller-runtime reads as no requeue", strikes, got)
			}
		}
	}
}

// TestIncompleteRequeueDelayJitters guards the fleet-lockstep property: a
// constant delay would have every shard that went incomplete together retry
// in lockstep against the topology server that just came back.
func TestIncompleteRequeueDelayJitters(t *testing.T) {
	t.Parallel()

	seen := map[time.Duration]bool{}
	for range 200 {
		seen[incompleteRequeueDelay(3)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("only %d distinct delays across 200 draws; jitter is not applied", len(seen))
	}
}

// shardNamed is the minimum a posture observation reads.
func shardNamed(ns, name string) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
	}
}

// TestPostureStrikesLeaveNoEntryOnceSettled is what the deletion change
// actually buys, and the one of these three that a mutation check confirms
// goes red against the pre-fix code.
//
// Writing zero on a settled observation kept an entry for every shard the
// process had ever reconciled, so the map's size tracked cumulative shards
// rather than currently-unhealthy ones. A map's table is sized by its peak
// simultaneous entries, so that made the peak unbounded over an operator's
// lifetime. Deleting makes the peak "shards unsettled right now", which a
// healthy cluster holds none of.
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

// TestPostureStrikesDoNotSurviveRecreation pins the intended semantic, and is
// deliberately labelled as a regression guard rather than as evidence for the
// deletion change.
//
// A mutation check caught this claiming more than it proves: it passes
// against the pre-fix code too, because a shard that settles before deletion
// left a zero, and zero and absent behave identically here. The deletion
// change is a memory fix, not a behavioural one, and a test whose name
// suggests otherwise is worse than no test.
//
// Kept because it still fences a real future mistake. Strikes now select the
// requeue backoff, so if anyone ever keys this map by something that outlives
// a shard, or persists it, a recreation would open at up to a minute on its
// first incomplete observation instead of five seconds. This fails if that
// happens.
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
// so the deletion above cannot be "fixed" into never counting at all.
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
