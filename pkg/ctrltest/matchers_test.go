package ctrltest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestWaitForAllToleratesOrder is the case the suite actually needs and did
// not have: a fan-out over ShardSpec.Pools, which is a Go map, emits its
// writes in randomised order, so the expectation has to be a multiset while
// still refusing anything extra.
func TestWaitForAllToleratesOrder(t *testing.T) {
	rec, wrapped := newRecordedClient("shard")
	ctx := t.Context()

	cur := rec.CursorForT(t, "ns1", "shard")
	for _, name := range []string{"pool-b", "pool-a", "pool-c"} {
		if err := wrapped.Create(ctx, configMap("ns1", name)); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	got := cur.WaitForAll(t, []Expect{
		ExpectCreate("ConfigMap", "pool-a"),
		ExpectCreate("ConfigMap", "pool-c"),
		ExpectCreate("ConfigMap", "pool-b"),
	}, time.Second)

	if len(got) != 3 {
		t.Fatalf("returned %d ops, want 3", len(got))
	}
	// Returned in recorded order, not in the order they were expected: the
	// matcher relaxes the assertion, not the record.
	if got[0].Key.Name != "pool-b" {
		t.Errorf("first op is %s, want the first one recorded, pool-b", got[0].Key.Name)
	}
	if _, ok := cur.next(); ok {
		t.Error("cursor did not consume all three ops")
	}
}

// TestWaitForAllIsAMultisetNotASet is the trap the multiset requirement exists
// for: two identical ops need two entries, and a matcher that deduped would
// accept one, leave the second invisible, and break some later assertion
// instead.
func TestWaitForAllIsAMultisetNotASet(t *testing.T) {
	t.Run("two entries consume two identical ops", func(t *testing.T) {
		rec, wrapped := newRecordedClient("shard")
		cur := rec.CursorForT(t, "ns1", "shard")

		p := pod("ns1", "p")
		if err := wrapped.Create(t.Context(), p); err != nil {
			t.Fatalf("create: %v", err)
		}
		for range 2 {
			if err := wrapped.Status().Patch(t.Context(), p, client.Merge); err != nil {
				t.Fatalf("status patch: %v", err)
			}
		}

		cur.WaitForAll(t, []Expect{
			ExpectCreate("Pod", "p"),
			ExpectStatusPatch("Pod", "p"),
			ExpectStatusPatch("Pod", "p"),
		}, time.Second)

		if op, ok := cur.next(); ok {
			t.Errorf("op %s left unconsumed; a duplicate was dropped", op)
		}
	})

	t.Run("one entry leaves the duplicate", func(t *testing.T) {
		rec, wrapped := newRecordedClient("shard")
		cur := rec.CursorForT(t, "ns1", "shard")

		p := pod("ns1", "p")
		if err := wrapped.Create(t.Context(), p); err != nil {
			t.Fatalf("create: %v", err)
		}
		for range 2 {
			if err := wrapped.Status().Patch(t.Context(), p, client.Merge); err != nil {
				t.Fatalf("status patch: %v", err)
			}
		}

		cur.WaitForAll(t, []Expect{
			ExpectCreate("Pod", "p"),
			ExpectStatusPatch("Pod", "p"),
		}, time.Second)

		// The window is exactly as wide as the expectation, so the second
		// identical op is still there to be asserted on rather than having
		// been silently absorbed.
		if _, ok := cur.next(); !ok {
			t.Error("the second identical op was consumed by a one-entry expectation")
		}
	})
}

// TestWaitForAllRefusesExtraOps pins the axis WaitForAll does not relax. It
// tolerates order; it still means "and nothing else".
func TestWaitForAllRefusesExtraOps(t *testing.T) {
	rec, wrapped := newRecordedClient("shard")
	ctx := t.Context()
	cur := rec.CursorFor("ns1", "shard")

	for _, name := range []string{"pool-a", "unexpected", "pool-b"} {
		if err := wrapped.Create(ctx, configMap("ns1", name)); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	_, err := waitForAll(cur, []Expect{
		ExpectCreate("ConfigMap", "pool-a"),
		ExpectCreate("ConfigMap", "pool-b"),
	}, 50*time.Millisecond)
	if err == nil {
		t.Fatal("waitForAll accepted a window containing an unexpected op")
	}
	// Both sides of the difference have to be named. A failure that reports
	// only the missing op sends the reader looking for something that never
	// happened, when the answer is the op that took its place.
	for _, want := range []string{"missing", "pool-b", "unexpected", "unexpected"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
	if strings.Contains(err.Error(), "timed out") {
		t.Errorf("error %q reports a timeout, want a mismatch on the window", err.Error())
	}

	// The window is consumed even though it did not match, for the same
	// reason WaitForNext consumes a mismatch. A matcher that consumed only on
	// success would leave pool-a and unexpected behind for some later
	// assertion to trip over, which moves the failure away from its cause and
	// is the drift towards eventually-assertions this family exists to
	// prevent.
	left, ok := cur.next()
	if !ok {
		t.Fatal("the cursor sees nothing after the mismatch; the window should be two ops wide")
	}
	if left.Key.Name != "pool-b" {
		t.Errorf(
			"cursor is parked on %s, want pool-b: the mismatched window was not consumed",
			left,
		)
	}
}

func TestWaitForAllTimeout(t *testing.T) {
	rec, wrapped := newRecordedClient("shard")
	cur := rec.CursorFor("ns1", "shard")
	if err := wrapped.Create(t.Context(), configMap("ns1", "only")); err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err := waitForAll(cur, []Expect{
		ExpectCreate("ConfigMap", "only"),
		ExpectCreate("ConfigMap", "never"),
	}, 50*time.Millisecond)
	if err == nil {
		t.Fatal("waitForAll returned nil with one of two ops recorded")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error %q, want a timeout naming what did arrive", err.Error())
	}
	if !strings.Contains(err.Error(), "only") {
		t.Errorf("error %q does not name the op that did arrive", err.Error())
	}
}

func TestWaitForAllRejectsAnEmptyExpectation(t *testing.T) {
	rec := NewRecorder()
	_, err := waitForAll(rec.CursorFor("ns1", "shard"), nil, time.Millisecond)
	if err == nil {
		t.Fatal("waitForAll accepted an empty multiset")
	}
	if !strings.Contains(err.Error(), "RequireNoActionTaken") {
		t.Errorf("error %q does not point at the assertion that means this", err.Error())
	}
}

// TestWaitForMatchingScansPastOps documents the escape hatch honestly: it skips
// what it does not match, and what it skipped is gone from the cursor.
func TestWaitForMatchingScansPastOps(t *testing.T) {
	rec, wrapped := newRecordedClient("shard")
	ctx := t.Context()
	cur := rec.CursorForT(t, "ns1", "shard")

	for _, name := range []string{"skipped", "wanted"} {
		if err := wrapped.Create(ctx, configMap("ns1", name)); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	op := cur.WaitForMatching(t, ExpectCreate("ConfigMap", "wanted"), time.Second)
	if op.Key.Name != "wanted" {
		t.Errorf("matched %s, want wanted", op.Key.Name)
	}
	if left, ok := cur.next(); ok {
		t.Errorf("op %s still visible; the scan is documented to drop what it passes", left)
	}
}

// TestWaitForMatchingLeavesADuplicate is the hazard by construction, and a
// plausible mechanism for the bug this whole pattern was added to fix: the
// second identical op survives, unremarked, for some later assertion to meet.
func TestWaitForMatchingLeavesADuplicate(t *testing.T) {
	rec, wrapped := newRecordedClient("shard")
	ctx := t.Context()
	cur := rec.CursorForT(t, "ns1", "shard")

	p := pod("ns1", "p")
	if err := wrapped.Create(ctx, p); err != nil {
		t.Fatalf("create: %v", err)
	}
	for range 2 {
		if err := wrapped.Status().Patch(ctx, p, client.Merge); err != nil {
			t.Fatalf("status patch: %v", err)
		}
	}

	cur.WaitForMatching(t, ExpectStatusPatch("Pod", "p"), time.Second)

	left, ok := cur.next()
	if !ok {
		t.Fatal("the duplicate was consumed; this matcher consumes one op")
	}
	if left.Verb != "status-patch" {
		t.Errorf("leftover op is %s, want the second status-patch", left)
	}
}

func TestWaitForMatchingTimeout(t *testing.T) {
	rec, wrapped := newRecordedClient("shard")
	cur := rec.CursorFor("ns1", "shard")
	if err := wrapped.Create(t.Context(), configMap("ns1", "something-else")); err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err := waitForMatching(cur, ExpectCreate("ConfigMap", "absent"), 50*time.Millisecond)
	if err == nil {
		t.Fatal("waitForMatching matched an op that was never recorded")
	}
	if !strings.Contains(err.Error(), "something-else") {
		t.Errorf("error %q does not name the ops it scanned", err.Error())
	}
}

// TestWaitForMatchingRespectsControllerScope guards the one property the escape
// hatch keeps: it scans, but only within the cursor's scope.
func TestWaitForMatchingRespectsControllerScope(t *testing.T) {
	rec := NewRecorder()
	base := fake.NewClientBuilder().Build()
	cur := rec.CursorFor("ns1", "shard")

	if err := rec.For("cell", base).Create(t.Context(), configMap("ns1", "cm")); err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err := waitForMatching(cur, ExpectCreate("ConfigMap", "cm"), 50*time.Millisecond)
	if err == nil {
		t.Fatal("a shard-scoped cursor matched the cell controller's write")
	}
}

// TestRecorderAllSeesWhatNoCursorCan is why explicit reporting is worth having:
// a cluster-scoped write files under namespace "", where no namespace-scoped
// cursor can reach it.
func TestRecorderAllSeesWhatNoCursorCan(t *testing.T) {
	rec, wrapped := newRecordedClient("shard")
	ctx := t.Context()

	if err := wrapped.Create(ctx, configMap("ns1", "cm")); err != nil {
		t.Fatalf("create configmap: %v", err)
	}
	if err := wrapped.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-scoped"},
	}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	all := rec.All()
	if len(all) != 2 {
		t.Fatalf("All() returned %d ops, want 2", len(all))
	}
	if got := len(rec.OpsInNamespace("ns1")); got != 1 {
		t.Errorf("OpsInNamespace(ns1) = %d, want 1", got)
	}

	// A copy, so a caller cannot rewrite the log it was handed.
	all[0].Verb = "mutated"
	if rec.All()[0].Verb == "mutated" {
		t.Error("All() handed out the recorder's own slice")
	}
}

// TestCursorDumpInterleavesOutOfScopeOps is the reporting job's whole point: a
// race is visible when the op that raced the assertion is on the next line,
// and merely inferable when it is not.
func TestCursorDumpInterleavesOutOfScopeOps(t *testing.T) {
	rec := NewRecorder()
	base := fake.NewClientBuilder().Build()
	shardClient := rec.For("shard", base)
	cellClient := rec.For("cell", base)
	ic := NewInterceptor(rec, RequeueClamp)
	ic.Activate("ns1")

	stub := &stubReconciler{
		fn: func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
			if err := shardClient.Create(ctx, configMap("ns1", "in-scope")); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: time.Minute},
				shardClient.Create(ctx, configMap("ns1", "also-in-scope"))
		},
	}

	if err := cellClient.Create(t.Context(), configMap("ns1", "raced-it")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := ic.Wrap("shard", stub).Reconcile(t.Context(), request("ns1", "a")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	cur := rec.CursorFor("ns1", "shard")
	out := rec.render(
		ic,
		dumpScope{label: cur.scope(), ns: cur.ns, controller: cur.controller},
		0, -1, cur.position(),
	)

	for _, want := range []string{
		"in-scope",     // the cursor's own scope
		"raced-it",     // and the write that raced it, interleaved
		"shard#1",      // tied to the reconcile that produced it
		"requeue 50ms", // what the reconcile returned
		"asked 1m0s",   // and what it had asked for
		"writes: create ConfigMap/in-scope, create ConfigMap/also-in-scope",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dump does not mention %q:\n%s", want, out)
		}
	}

	// Attribution is per controller, so the cell controller's op must not be
	// credited to the shard's reconcile.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "raced-it") && strings.Contains(line, "shard") {
			t.Errorf("out-of-scope op credited to the shard controller: %q", line)
		}
	}
}

// TestCursorDumpBoundsItsWindow keeps a failure readable: the log can be
// thousands of ops long and a dump that printed all of them would hide the race
// as effectively as printing none.
func TestCursorDumpBoundsItsWindow(t *testing.T) {
	rec, wrapped := newRecordedClient("shard")
	ctx := t.Context()
	cur := rec.CursorFor("ns1", "shard")
	const total = dumpOpsBefore * 3
	for i := range total {
		if err := wrapped.Create(ctx, configMap("ns1", fmt.Sprintf("cm-%d", i))); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	if _, ok := cur.take(); !ok {
		t.Fatal("cursor is empty")
	}
	windowed := rec.render(
		nil,
		dumpScope{label: cur.scope(), ns: cur.ns, controller: cur.controller},
		max(cur.position()-dumpOpsBefore, 0),
		cur.position()+dumpOpsAfter,
		cur.position(),
	)
	if strings.Contains(windowed, fmt.Sprintf("cm-%d", total-1)) {
		t.Errorf("the bounded window reaches the end of a %d op log:\n%s", total, windowed)
	}
	if !strings.Contains(windowed, "of "+fmt.Sprint(total)) {
		t.Errorf("the dump does not say how much of the log it is showing:\n%s", windowed)
	}
}

func TestDumpOnFailureReturnsTheCursor(t *testing.T) {
	rec := NewRecorder()
	cur := rec.CursorFor("ns1", "shard")
	if got := cur.DumpOnFailure(t); got != cur {
		t.Error("DumpOnFailure returned a different cursor, so it cannot be chained")
	}
}

// TestReconcileLogRefusesAnotherRecordersOps pins the identity check no green
// run ever reaches, since it only executes while a failure dump renders.
//
// Op log indices are only meaningful against the log they came from, so a
// standalone recorder must not borrow the suite's interceptor to attribute its
// ops: every index would land in some unrelated pass's range and the dump
// would quote a reconcile number that has nothing to do with the write beside
// it. Wrong attribution in a failure dump is worse than none, because a reader
// acts on it.
func TestReconcileLogRefusesAnotherRecordersOps(t *testing.T) {
	if shared.Ops.reconcileLog() != shared.Reconciles {
		t.Error("the suite's own recorder is not attributed to the suite's interceptor")
	}

	other, wrapped := newRecordedClient("shard")
	if err := wrapped.Create(t.Context(), configMap("ns1", "cm")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if ic := other.reconcileLog(); ic != nil {
		t.Error("a standalone recorder was handed the suite's interceptor")
	}

	// The consequence at the level a reader meets it: the attribution column
	// names the controller and stops, rather than citing a pass.
	out := other.render(other.reconcileLog(), dumpScope{label: "ns1", ns: "ns1"}, 0, -1, -1)
	if !strings.Contains(out, "shard") {
		t.Fatalf("dump does not name the controller that wrote:\n%s", out)
	}
	if strings.Contains(out, "shard#") {
		t.Errorf("dump credits the op to a reconcile pass from another log:\n%s", out)
	}
}
