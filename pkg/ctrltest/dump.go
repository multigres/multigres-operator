package ctrltest

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A failure dump is bounded on purpose. The whole point is a race made visible,
// and a thousand lines of op log hides a race as thoroughly as none at all.
// These are indices into the append-only log rather than a time window, which
// comes to the same thing because the log is chronological, and unlike a time
// window it cannot accidentally be empty.
const (
	dumpOpsBefore    = 24
	dumpOpsAfter     = 8
	dumpMaxReconcile = 12
)

// All returns every recorded op, in order. This is the explicit-reporting
// escape hatch: OpsInNamespace answers the common question, and this one
// answers everything else, including writes against cluster-scoped objects
// that no namespace-scoped cursor can see.
func (r *Recorder) All() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Op, len(r.ops))
	copy(out, r.ops)
	return out
}

// Dump logs the whole op log with reconcile attribution. Unbounded, for a test
// author driving an investigation by hand; the automatic dumps on failure are
// the bounded ones.
func (r *Recorder) Dump(t *testing.T) {
	t.Helper()
	r.dump(t, dumpScope{label: "all ops", all: true}, 0, -1, -1)
}

// DumpNamespace logs every op recorded against one namespace, by any
// controller.
func (r *Recorder) DumpNamespace(t *testing.T, ns string) {
	t.Helper()
	r.dump(t, dumpScope{label: ns, ns: ns}, 0, -1, -1)
}

// Dump logs the op log around this cursor: the window's in-scope ops marked,
// every other controller's ops in the same window interleaved, and the
// reconciles that produced them.
//
// The interleaving is the whole point. A race between two controllers shows up
// as one op the assertion caught and another it did not, and reading only the
// cursor's own scope leaves the second one invisible, so the reader has to
// infer it. Here it is on the next line.
func (c *Cursor) Dump(t *testing.T) {
	t.Helper()
	pos := c.position()
	c.rec.dump(
		t,
		dumpScope{label: c.scope(), ns: c.ns, controller: c.controller},
		max(pos-dumpOpsBefore, 0),
		pos+dumpOpsAfter,
		pos,
	)
}

// DumpOnFailure arranges for Dump to run if the test fails, and returns the
// cursor so it can be chained onto a constructor.
//
// This is how the recorder's own WaitFor* helpers and RequireNoActionTaken get
// a dump without knowing anything about one: they report through t.Fatalf, and
// a cleanup registered here runs afterwards with the cursor parked exactly
// where the assertion left it. The timeout path benefits most, since a timeout
// is precisely the case where the failure message can name nothing that did
// happen and the log is the only evidence there is.
//
// One consequence of running at cleanup rather than at the moment of failure:
// in a parallel package, ops recorded by other tests after the failure can
// appear in the window's tail. They are timestamped and out of scope, so they
// read as what they are.
func (c *Cursor) DumpOnFailure(t *testing.T) *Cursor {
	t.Cleanup(func() {
		if t.Failed() {
			c.Dump(t)
		}
	})
	return c
}

// CursorT is Cursor with a failure dump already attached.
func (r *Recorder) CursorT(t *testing.T, ns string) *Cursor {
	return r.Cursor(ns).DumpOnFailure(t)
}

// CursorForT is CursorFor with a failure dump already attached, and is the one
// to reach for by default: a controller-scoped cursor is what makes an
// intolerant assertion usable in this suite, and a dump is what makes the
// intolerance diagnosable when it fires.
func (r *Recorder) CursorForT(t *testing.T, ns, controller string) *Cursor {
	return r.CursorFor(ns, controller).DumpOnFailure(t)
}

// dumpScope is what a dump considers in scope, so the same renderer serves a
// whole-log dump and a cursor's window.
type dumpScope struct {
	label      string
	all        bool
	ns         string
	controller string
}

// A dump is the one reader that wants rejected writes in scope: a controller
// wedged on a refused write is exactly the situation a dump is being read in,
// and Op.String marks the rejection inline.
func (s dumpScope) matches(op Op) bool {
	return s.all || inScope(op, s.ns, s.controller, true)
}

// dump logs what render produced. Splitting the two is what lets the renderer
// be asserted on directly, since a dump that silently stopped interleaving
// out-of-scope ops would still look fine from the call site.
func (r *Recorder) dump(t *testing.T, scope dumpScope, from, to, pos int) {
	t.Helper()
	t.Logf("\n%s", r.render(r.reconcileLog(), scope, from, to, pos))
}

// render draws ops in [from, to) with pos marked as the cursor position. to of
// -1 means the end of the log, pos of -1 means no cursor. ic may be nil, in
// which case ops get no reconcile attribution.
func (r *Recorder) render(ic *Interceptor, scope dumpScope, from, to, pos int) string {
	all := r.All()
	if to < 0 || to > len(all) {
		to = len(all)
	}
	from = max(from, 0)
	if from > len(all) {
		from = len(all)
	}

	var b strings.Builder
	inScopeCount := 0
	for _, op := range all[from:to] {
		if scope.matches(op) {
			inScopeCount++
		}
	}
	fmt.Fprintf(&b, "op log for %s: showing ops %d..%d of %d, %d in scope",
		scope.label, from, to, len(all), inScopeCount)
	if pos >= 0 {
		fmt.Fprintf(&b, ", cursor at %d", pos)
	}
	b.WriteString("\n")

	if from == to {
		b.WriteString("  (no ops recorded in this window)\n")
		return b.String()
	}

	b.WriteString(
		"  '>' is in scope for this cursor; the rest are other writes in the same window\n",
	)
	t0 := all[from].At
	for i := from; i < to; i++ {
		op := all[i]
		marker := " "
		if scope.matches(op) {
			marker = ">"
		}
		fmt.Fprintf(&b, "  %s %8s  %-5d %-16s %-14s %-24s %s\n",
			marker,
			since(t0, op.At),
			i,
			attribution(ic, op, i),
			op.Verb,
			KindSuffix(op.Kind),
			op.Key,
		)
	}

	renderReconciles(&b, ic, from, to, t0)
	return b.String()
}

// attribution names the controller and, when the op can be tied to one, the
// reconcile pass that produced it. An op with no pass is either a write from
// outside any reconcile or one by a controller this suite did not wrap, and
// both are worth seeing as such rather than as a blank column.
func attribution(ic *Interceptor, op Op, idx int) string {
	if ic == nil {
		return op.Controller
	}
	if r, ok := ic.reconcileOf(op.Controller, idx); ok {
		return fmt.Sprintf("%s#%d", op.Controller, r.Seq)
	}
	return op.Controller + "#?"
}

// renderReconciles is the grouping half of the dump: which pass produced which
// writes, and what each pass returned.
//
// This is what makes "more than one action happened in one reconcile" legible
// instead of something a reader reconstructs from timestamps, and that shape of
// bug is exactly why this pattern exists. It also shows the passes that wrote
// nothing at all, which the op log cannot, and a controller spinning without
// writing is a common enough answer to "why is this test slow" to be worth the
// lines.
func renderReconciles(b *strings.Builder, ic *Interceptor, from, to int, t0 time.Time) {
	if ic == nil {
		return
	}
	rs := ic.overlapping(from, to)
	if len(rs) == 0 {
		return
	}
	elided := 0
	if len(rs) > dumpMaxReconcile {
		elided = len(rs) - dumpMaxReconcile
		rs = rs[elided:]
	}
	fmt.Fprintf(b, "  reconciles overlapping this window (%d shown", len(rs))
	if elided > 0 {
		fmt.Fprintf(b, ", %d earlier elided", elided)
	}
	b.WriteString("):\n")
	for _, r := range rs {
		writes := "none"
		if ops := ic.Ops(r); len(ops) > 0 {
			parts := make([]string, 0, len(ops))
			for _, op := range ops {
				parts = append(parts, fmt.Sprintf("%s %s/%s",
					op.Verb, KindSuffix(op.Kind), op.Key.Name))
			}
			writes = strings.Join(parts, ", ")
		}
		fmt.Fprintf(b, "    %-16s %8s..%-8s %-28s writes: %s\n",
			fmt.Sprintf("%s#%d", r.Controller, r.Seq),
			since(t0, r.Start), since(t0, r.End),
			r.outcome(),
			writes,
		)
	}
}

// since renders a timestamp relative to the window's first op, since absolute
// times in a dump are unreadable and the interesting quantity is always a gap.
func since(t0, t time.Time) string {
	return fmt.Sprintf("%+.3fs", t.Sub(t0).Seconds())
}

// reconcileLog returns the interceptor that wraps this recorder, when there is
// one. The recorder works standalone against a fake client, with no manager and
// so no reconcile boundary to attribute anything to, and a dump in that setting
// still has to render.
//
// The link is structural rather than a runtime comparison: op log indices are
// only meaningful against the log they came from, so borrowing another
// recorder's interceptor would invent a reconcile number for each of them.
func (r *Recorder) reconcileLog() *Interceptor {
	return r.il
}
