package ctrltest

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// quiescenceSlice is how long one pass waits on the event stream before coming
// up for air to consume the recorder. The write half has nothing to block on,
// so it is polled, and this is the granularity at which a write that produced
// no event is noticed.
const quiescenceSlice = 100 * time.Millisecond

// quiescenceReportLimit caps each histogram in the failure report. A hot loop
// names the same handful of paths thousands of times, so the head of a
// count-sorted list is the diagnosis and the tail is noise.
const quiescenceReportLimit = 20

// churnMonitor accumulates everything that kept a namespace from going quiet,
// from the two signals quiescence is measured on.
//
// Neither signal subsumes the other. The event stream sees a state change
// whoever caused it, including the data plane fakes, which write through a
// client the recorder never wraps. The recorder sees an attempted write even
// when nothing changed, and there are two ways for a write to change nothing.
// A write whose result is identical bumps no resourceVersion and so produces no
// watch event at all: the tablegroup controller once issued 3,641 such patches
// against one Shard in three minutes, every one of them a hot loop and every
// one of them invisible to a watch. And a write the API server refuses changes
// nothing by definition, so it is equally invisible to a watch, which is why
// attempts are counted here and not just accepted writes. A controller wedged
// on a rejected write is the case this half exists to catch, and counting only
// successes read it as perfectly quiet.
type churnMonitor struct {
	events int
	ops    int
	// rejected counts how many of ops the API server refused. A namespace whose
	// churn is all rejections is a wedged controller rather than a busy one,
	// and that is the first thing a reader of the report needs to know.
	rejected int

	// changes counts stream events per object, paths counts how often each
	// projected field path moved, and writes counts recorded writes per
	// controller/verb/object.
	changes map[string]int
	paths   map[string]int
	writes  map[string]int
}

func newChurnMonitor() *churnMonitor {
	return &churnMonitor{
		changes: map[string]int{},
		paths:   map[string]int{},
		writes:  map[string]int{},
	}
}

func (m *churnMonitor) observeEvent(ev Event) {
	m.events++
	id := ev.Kind + " " + ev.Key.String()
	m.changes[id]++
	for _, p := range ev.Changed {
		// Keyed on the transition and not just the path, so a field flapping
		// between two values reports as two entries of equal count. That shape
		// is the diagnosis: it is how the shard status loop was read as two
		// server-side-apply defects overwriting each other rather than as one
		// controller drifting.
		m.paths[fmt.Sprintf("%s %s: %s", id, p, ev.Transitions[p])]++
	}
}

// observeWrites consumes every write the cursor can see and reports whether
// there was one. The cursor is an attempt cursor, so a rejected write counts as
// activity exactly like an accepted one.
func (m *churnMonitor) observeWrites(c *Cursor) bool {
	var seen bool
	for {
		op, ok := c.take()
		if !ok {
			return seen
		}
		seen = true
		m.ops++
		if !op.accepted() {
			m.rejected++
		}
		m.writes[op.String()]++
	}
}

func (m *churnMonitor) report(ns string, quiet, timeout time.Duration) error {
	var b strings.Builder
	fmt.Fprintf(&b,
		"namespace %s never went quiet for %s within %s: "+
			"%d projected state changes, %d attempted writes (%d rejected)",
		ns, quiet, timeout, m.events, m.ops, m.rejected)
	for _, section := range []struct {
		label  string
		counts map[string]int
	}{
		{"changed", m.changes},
		{"churn", m.paths},
		{"wrote", m.writes},
	} {
		for _, line := range topCounts(section.counts, quiescenceReportLimit) {
			fmt.Fprintf(&b, "\n  %-8s %s", section.label, line)
		}
	}
	return errors.New(b.String())
}

// topCounts renders a histogram highest count first, capped at limit. Ties
// break on the key so that two runs of the same failure read the same.
func topCounts(counts map[string]int, limit int) []string {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, fmt.Sprintf("x%-6d %s", counts[k], k))
	}
	return out
}

// Unconverged returns one line per (controller, request key) in ns whose most
// recent reconcile pass ended in an error, or nil if every key's last pass
// succeeded.
//
// Unconverged is deliberately a level check rather than a rate one. A
// controller that fails every pass is retried with exponential backoff, so
// the gap between its failures grows without bound and any fixed window
// eventually sees silence. Asking "what did this controller last say about
// this object" cannot decay that way, so it holds however long the backoff
// has stretched.
//
// Grouping is by controller and request key together, because two controllers
// can be reconciling the same object and only one of them wedged, and one
// controller can be converged on every object but one.
func (s *Suite) Unconverged(ns string) []string {
	type group struct {
		controller string
		key        client.ObjectKey
	}

	last := map[group]Reconcile{}
	for _, r := range s.Reconciles.InNamespace(ns) {
		// A gated request is not a pass of the code under test, and it always
		// carries a nil error, so counting one would let the gate closing
		// erase a wedge that is still there. No such record can exist while
		// the test that owns ns is running, since Namespace deactivates only
		// its own namespace and only in its own cleanup; this guards a caller
		// outside that window instead, a failure dump or a later diagnostic,
		// which is reachable because this is exported.
		if r.Gated {
			continue
		}
		g := group{controller: r.Controller, key: r.Request.NamespacedName}
		// Ordered on Start rather than on Seq: Seq counts a controller's
		// passes across every object it reconciles, so it does not order a
		// group. Ties go to the later log entry, which for one controller and
		// one key is the later pass, because the log is append-ordered and two
		// passes over one key cannot overlap: client-go's workqueue never
		// hands the same item to two workers at once, and controller-runtime's
		// queue keeps that invariant. So a group is a sequence rather than a
		// set, at any MaxConcurrentReconciles the consumer chooses, and this
		// does not rest on how this suite happens to configure its
		// controllers.
		if prev, ok := last[g]; ok && r.Start.Before(prev.Start) {
			continue
		}
		last[g] = r
	}

	var out []string
	for g, r := range last {
		if r.Err == nil {
			continue
		}
		out = append(out, fmt.Sprintf("%s %s: %v", g.controller, g.key, r.Err))
	}
	// Sorted so that the report reads the same across runs, and so a test can
	// assert on the whole slice rather than on a set.
	sort.Strings(out)
	return out
}

// quiescenceError renders both halves of the measurement as a single error, or
// nil when both are clean.
//
// Unconverged keys come first because they name a cause, while the churn
// histograms only describe a symptom: "the shard controller's last word on
// this object was an error" is the diagnosis, and "nothing has happened for
// five seconds" is the thing that made it look fine. churn is nil when the
// window half was satisfied, and its absence from the message is how a reader
// tells that the namespace did go quiet and was still not converged.
func quiescenceError(ns string, unconverged []string, churn error) error {
	if len(unconverged) == 0 {
		return churn
	}
	var b strings.Builder
	fmt.Fprintf(&b,
		"namespace %s has not converged: %d controller/object pair(s) "+
			"whose most recent reconcile pass errored",
		ns, len(unconverged))
	for _, line := range unconverged {
		fmt.Fprintf(&b, "\n  %-8s %s", "errored", line)
	}
	if churn != nil {
		fmt.Fprintf(&b, "\n%v", churn)
	}
	return errors.New(b.String())
}

// RequireQuiescent fails unless the namespace goes quiet: no projected state
// change on any watched object and no attempted write for quiet, reached within
// timeout, and no controller whose last word on an object was an error.
//
// That last condition is not a refinement of the other two, it is the one that
// makes them safe to believe. Quiet is a rate measure and convergence is a
// level one, and exponential backoff pulls them apart: see Unconverged.
//
// This is the assertion a conventional "did it reach Healthy?" check cannot
// make. A controller that rewrites the same status forever still reports
// Healthy, so a suite without this passes cleanly against a hot loop. A
// controller wedged on a write the API server refuses forever is worse still,
// since it changes no object and so is silent on the stream; it is caught here
// because the write half counts attempts and not just accepted writes. On
// failure this names the fields that kept moving and marks the writes that were
// rejected, which is usually the diagnosis.
//
// Both halves of the window measure are required, and dropping either one
// makes this weaker than the poll loop it replaced. See churnMonitor for what
// each half can see that the other cannot.
//
// A quiet verdict has one seam on the write path, and it is worth knowing
// rather than trusting past. The recorder appends an op only after the API
// server has answered, so a writer preempted in that gap can have its write
// land after the last drain and still be counted quiet. The exposure is a
// scheduling gap rather than the length of a pass, and it is not a regression:
// the poller this replaced could not see a no-op write at all. Closing it would
// need a barrier between this and every writer, which the suite does not have.
//
// Bounding the level half just as honestly: it keys on a pass returning an
// error and on nothing else, so a controller stuck in a nil-error requeue
// loop is not caught by it. A pass that writes nothing and returns
// (Result{RequeueAfter: x}, nil) forever is silent on both window halves and
// converged on this one, and so reads as quiescent. That shape is a real way
// for a controller to be wedged, and catching it would mean judging whether a
// requeue is progress, which is the controller's own business rather than the
// harness's. A test that suspects one should assert on the object's
// conditions, or count passes via Suite.Reconciles.
//
// Bounding the write half honestly: it sees writes through a recorder-wrapped
// client, which is every controller under test and is not the data plane fakes.
// A fake looping on writes that change nothing would be invisible to both
// halves. That is a statement about the harness rather than about the operator,
// so a quiet verdict is still a claim about the code under test.
//
// It costs at least quiet seconds of wall clock even on success, so it belongs
// in tests about convergence rather than in every test.
func (s *Suite) RequireQuiescent(t *testing.T, ns string, quiet, timeout time.Duration) {
	t.Helper()
	if err := s.TryQuiescent(t, ns, quiet, timeout); err != nil {
		t.Error(err)
	}
}

// TryQuiescent is RequireQuiescent's measurement, returning the report rather
// than failing, so that the report itself can be asserted on. That is what a
// KnownDefect body needs: a pin on a controller that never goes quiet has to
// read the report as a value, since a t.Error from inside the check would fail
// the test rather than record the defect.
func (s *Suite) TryQuiescent(t *testing.T, ns string, quiet, timeout time.Duration) error {
	t.Helper()

	m := newChurnMonitor()
	writes := s.Ops.attemptCursor(ns)
	st := s.Watch(t, ns, s.watchedKinds...)
	// Closed here rather than left to the cleanup Watch registers, so a test
	// calling this several times runs one stream at a time instead of
	// accumulating a watcher per kind per call for the rest of the test. The
	// cleanup still runs, and shutdown is idempotent.
	defer st.shutdown()

	// The watch opens over a namespace that already holds objects, so the API
	// server replays those as adds before it reports anything new. They are not
	// filtered out and do not need a baseline pass of their own: they count as
	// activity, which delays the start of the quiet window by however long the
	// replay takes and no longer.
	deadline := time.Now().Add(timeout)
	quietSince := time.Now()
	for time.Now().Before(deadline) {
		ev, err := st.Next(quiescenceSlice)
		if err != nil {
			// Next reports a timeout and a lost stream the same way, and only
			// the second one voids the measurement.
			if lost := st.Terminal(); lost != nil {
				return fmt.Errorf("quiescence over %s is void: %w", ns, lost)
			}
		} else {
			m.observeEvent(ev)
			quietSince = time.Now()
		}

		if m.observeWrites(writes) {
			quietSince = time.Now()
		}
		if time.Since(quietSince) >= quiet {
			// The window going quiet is necessary and not sufficient. A
			// controller whose last pass errored is still trying, however
			// slowly, so the namespace has not converged no matter how long
			// the window saw nothing.
			return quiescenceError(ns, s.Unconverged(ns), nil)
		}
	}
	return quiescenceError(ns, s.Unconverged(ns), m.report(ns, quiet, timeout))
}
