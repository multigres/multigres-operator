package ctrltest

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// Expect is one expected write: the recorder's verb string, the kind's short
// name, and the object's name. It is what WaitForAll and WaitForMatching take
// in place of the three loose strings WaitForNext takes, because a multiset
// assertion needs to name several ops in one call.
type Expect struct {
	Verb string
	Kind string
	Name string
}

func (e Expect) String() string {
	return fmt.Sprintf("%s %s %q", e.Verb, e.Kind, e.Name)
}

// key is the multiset key. Kind is normalised through KindSuffix so that a
// caller-supplied "Shard" and a recorded "*v1alpha1.Shard" are the same
// expectation.
func (e Expect) key() string {
	return e.Verb + "|" + KindSuffix(e.Kind) + "|" + e.Name
}

func opKey(op Op) string {
	return op.Verb + "|" + KindSuffix(op.Kind) + "|" + op.Key.Name
}

// Which matcher belongs where is decided by the structure of the code under
// test, not by which one makes a failing test pass. This operator's shard
// reconciler runs fourteen write-capable steps in a single pass, so the
// boundary is:
//
//   - Across steps, order is deterministic. They are sequential statements, so
//     the pg_hba ConfigMap always precedes the shared backup PVC. WaitForNext
//     is sound here and this is where the protocol assertions belong.
//   - Within a step, fan-out over ShardSpec.Pools is undefined, because that
//     field is a Go map and nine sites iterate it with a plain range. A
//     two-pool shard emits its pods, PVCs and PDB entries in randomised order.
//     WaitForNext flakes there; WaitForAll expresses it exactly.
//   - WaitForMatching, ideally never. See its own comment.
//
// One scope rule covers all three: the order-exact and multiset matchers are
// for controller-scoped cursors only. Nothing orders writes across
// controllers, so an ordering assertion on an unscoped cursor is unsound
// whichever matcher it uses.
//
// And the framing that makes any of this survive requeue compression: the unit
// of ordering is one reconcile pass, not wall-clock time. Compression means
// far more reconciles and far more interleaving, so an assertion anchored to
// elapsed time degrades as the suite gets faster, while one anchored to a
// controller's next op stays exact however many other controllers' timers fire
// around it.

// WaitForAll asserts that the next len(want) ops in the cursor's scope are
// exactly want, in any order, and nothing else. It consumes all of them and
// returns them in the order they were recorded.
//
// This relaxes ORDER and nothing else. "And nothing else happened" still
// holds, because the assertion is over a fixed count of ops: an extra write
// inside the window fails it, rather than being skipped over.
//
// want is a multiset, not a set, and that is the point. Two identical ops in
// one pass, two status patches of the same Shard say, need two entries here to
// be satisfied. A set-based matcher would accept one, leave the second
// unconsumed and invisible, and the next assertion in the test would trip over
// it somewhere that looks unrelated. The op log is an append-only slice
// precisely so duplicates survive; a matcher that deduped would throw that
// away at the last step.
func (c *Cursor) WaitForAll(t *testing.T, want []Expect, timeout time.Duration) []Op {
	t.Helper()
	ops, err := waitForAll(c, want, timeout)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return ops
}

func waitForAll(c *Cursor, want []Expect, timeout time.Duration) ([]Op, error) {
	if len(want) == 0 {
		return nil, fmt.Errorf(
			"cursor(%s): WaitForAll needs at least one expectation; "+
				"for \"nothing happens\" use RequireNoActionTaken",
			c.scope(),
		)
	}

	deadline := time.Now().Add(timeout)
	for {
		ops, idx, floor := c.rec.scanInScope(
			c.position(), len(want), c.ns, c.controller, c.attempts,
		)
		if len(ops) == len(want) {
			// Consumed whether or not the multiset matches, for the same
			// reason WaitForNext consumes a mismatch: the ops have been
			// looked at, and leaving them for a later assertion to trip
			// over would move the failure away from its cause.
			c.advanceTo(idx[len(idx)-1] + 1)
			if err := compareMultiset(c, want, ops); err != nil {
				return ops, err
			}
			return ops, nil
		}
		c.advanceTo(floor)
		if time.Now().After(deadline) {
			return ops, fmt.Errorf(
				"cursor(%s): timed out after %s waiting for %d ops in any order [%s]; "+
					"only %d arrived [%s]",
				c.scope(), timeout, len(want), joinExpect(want), len(ops), joinOps(ops),
			)
		}
		time.Sleep(cursorPollInterval)
	}
}

// compareMultiset reports how got differs from want as multisets, naming both
// the expectations left unsatisfied and the ops nothing expected. Reporting
// only one side of that is what makes a fan-out failure hard to read: the
// interesting case is usually a substitution, where each list has one entry.
func compareMultiset(c *Cursor, want []Expect, got []Op) error {
	counts := map[string]int{}
	for _, e := range want {
		counts[e.key()]++
	}
	var unexpected []Op
	for _, op := range got {
		k := opKey(op)
		if counts[k] > 0 {
			counts[k]--
			continue
		}
		unexpected = append(unexpected, op)
	}
	var missing []string
	for k, n := range counts {
		for range n {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf(
		"cursor(%s): the next %d ops were not the expected multiset [%s];\n"+
			"  got:        %s\n"+
			"  missing:    %s\n"+
			"  unexpected: %s",
		c.scope(), len(want), joinExpect(want),
		joinOps(got), strings.Join(missing, ", "), joinOps(unexpected),
	)
}

// WaitForMatching scans the ops already buffered in the cursor's scope for one
// matching verb, kind and name and consumes that one. It is the escape hatch,
// it relaxes both axes the other two matchers hold, and reaching for it to
// silence a failing strict assertion is how an ordering test quietly becomes an
// eventually-assertion.
//
// Three properties to know before using it:
//
// It cannot express "and nothing else". It is tolerant of extra ops by
// construction, so it can never fail because something unexpected also
// happened.
//
// It has a duplicate hazard by construction. With two identical ops pending it
// consumes the first and leaves the second in place, unremarked, and the next
// assertion in the test meets that leftover. That is a plausible mechanism for
// the original "two actions in one reconcile, waiter caught the wrong one" bug
// this whole pattern was added to fix, so it is worth knowing rather than
// rediscovering.
//
// It drops what it scans past. The ops it skipped are gone from this cursor,
// not merely stepped over, so no later assertion on this cursor can see them.
// They remain in the op log, which is where the failure dump reads from, so the
// evidence survives even though the cursor's view of it does not.
//
// A case that genuinely needs this is usually a case where the operator could
// be deterministic and is not. Prefer fixing that. Do not use it in an existing
// test to make it pass.
func (c *Cursor) WaitForMatching(t *testing.T, want Expect, timeout time.Duration) Op {
	t.Helper()
	op, err := waitForMatching(c, want, timeout)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return op
}

func waitForMatching(c *Cursor, want Expect, timeout time.Duration) (Op, error) {
	deadline := time.Now().Add(timeout)
	for {
		op, idx, ok := c.rec.findInScope(c.position(), c.ns, c.controller, want, c.attempts)
		if ok {
			c.advanceTo(idx + 1)
			return op, nil
		}
		if time.Now().After(deadline) {
			pending, _, _ := c.rec.scanInScope(c.position(), 20, c.ns, c.controller, c.attempts)
			return Op{}, fmt.Errorf(
				"cursor(%s): timed out after %s scanning for %s; in scope and unconsumed: [%s]",
				c.scope(), timeout, want, joinOps(pending),
			)
		}
		time.Sleep(cursorPollInterval)
	}
}

// inScope is the cursor scope predicate: one namespace, one controller when the
// cursor names one, and accepted writes only unless attempts widens it. The
// outcome filter lives here rather than at each call site so that the matchers,
// the cursor peek and the dump cannot drift apart on what they can see.
func inScope(op Op, ns, controller string, attempts bool) bool {
	if !attempts && !op.accepted() {
		return false
	}
	if op.Key.Namespace != ns {
		return false
	}
	return controller == "" || op.Controller == controller
}

// scanInScope returns up to n in-scope ops at or after from with their log
// indices, plus a floor the caller can park a cursor on. The floor is the index
// of the first in-scope op found, or the current log length when none was, so
// that a poll loop rules out each out-of-scope op once instead of refiltering
// the namespace's whole history on every tick.
func (r *Recorder) scanInScope(
	from, n int,
	ns, controller string,
	attempts bool,
) ([]Op, []int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var (
		ops   []Op
		idx   []int
		floor = len(r.ops)
	)
	for i := from; i < len(r.ops) && len(ops) < n; i++ {
		op := r.ops[i]
		if !inScope(op, ns, controller, attempts) {
			continue
		}
		if len(ops) == 0 {
			floor = i
		}
		ops = append(ops, op)
		idx = append(idx, i)
	}
	return ops, idx, floor
}

// findInScope returns the first in-scope op at or after from that matches want,
// with its log index.
func (r *Recorder) findInScope(
	from int,
	ns, controller string,
	want Expect,
	attempts bool,
) (Op, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := from; i < len(r.ops); i++ {
		op := r.ops[i]
		if !inScope(op, ns, controller, attempts) {
			continue
		}
		if opKey(op) == want.key() {
			return op, i, true
		}
	}
	return Op{}, 0, false
}

// opsBetween returns one controller's accepted ops in the log range [from, to).
// See Interceptor.Ops for why that identifies a single reconcile pass.
//
// Accepted only, so that "the writes this pass made" keeps meaning writes that
// landed. A pass whose writes were all refused reports none here, which is the
// truth about its effect; what it attempted is in the op log the failure dump
// renders, marked as rejected.
func (r *Recorder) opsBetween(from, to int, controller string) []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	to = min(to, len(r.ops))
	var out []Op
	for i := max(from, 0); i < to; i++ {
		if r.ops[i].accepted() && r.ops[i].Controller == controller {
			out = append(out, r.ops[i])
		}
	}
	return out
}

// position is the cursor's current scan index.
func (c *Cursor) position() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scan
}

// advanceTo moves the cursor forward to idx, never backwards. Monotonic
// because two waiters sharing a cursor is already a mistake, and a cursor that
// could move backwards would turn that mistake into ops being consumed twice.
func (c *Cursor) advanceTo(idx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx > c.scan {
		c.scan = idx
	}
}

func joinExpect(want []Expect) string {
	parts := make([]string, 0, len(want))
	for _, e := range want {
		parts = append(parts, e.String())
	}
	return strings.Join(parts, ", ")
}

func joinOps(ops []Op) string {
	if len(ops) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(ops))
	for _, op := range ops {
		parts = append(parts, op.String())
	}
	return strings.Join(parts, ", ")
}

// ExpectCreate, ExpectPatch, ExpectStatusPatch, ExpectUpdate and ExpectDelete
// are sugar for the verbs the named WaitFor* helpers cover, so a multiset reads
// as a list of actions rather than a list of structs. Any other verb, and
// "patch" is this operator's most common one, is written out as an Expect
// literal.
func ExpectCreate(kind, name string) Expect {
	return Expect{Verb: "create", Kind: kind, Name: name}
}

func ExpectPatch(kind, name string) Expect {
	return Expect{Verb: "patch", Kind: kind, Name: name}
}

func ExpectStatusPatch(kind, name string) Expect {
	return Expect{Verb: "status-patch", Kind: kind, Name: name}
}

func ExpectUpdate(kind, name string) Expect {
	return Expect{Verb: "update", Kind: kind, Name: name}
}

func ExpectDelete(kind, name string) Expect {
	return Expect{Verb: "delete", Kind: kind, Name: name}
}

// ExpectOf builds an Expect from a recorded op, for a test that wants to state
// an expectation in terms of an op it already holds.
func ExpectOf(op Op) Expect {
	return Expect{Verb: op.Verb, Kind: KindSuffix(op.Kind), Name: op.Key.Name}
}
