package shard

import (
	"testing"
	"time"
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
