package ctrltest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// newRecordedClient wires a fresh Recorder to a fake client, driven by hand,
// with no envtest involved.
func newRecordedClient(controller string) (*Recorder, client.Client) {
	rec := NewRecorder()
	return rec, rec.For(controller, fake.NewClientBuilder().Build())
}

func configMap(ns, name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
}

func pod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
		},
	}
}

func TestRecorderCursorWaitForCreate(t *testing.T) {
	rec, wrapped := newRecordedClient("ctrl-a")
	ctx := t.Context()

	cur := rec.Cursor("ns1")
	if err := wrapped.Create(ctx, configMap("ns1", "cm1")); err != nil {
		t.Fatalf("create: %v", err)
	}

	op := cur.WaitForCreate(t, "ConfigMap", "cm1", time.Second)

	if op.Controller != "ctrl-a" {
		t.Errorf("Controller = %q, want ctrl-a", op.Controller)
	}
	if op.Verb != "create" {
		t.Errorf("Verb = %q, want create", op.Verb)
	}
	if op.Key.Name != "cm1" || op.Key.Namespace != "ns1" {
		t.Errorf("Key = %v, want ns1/cm1", op.Key)
	}

	if _, ok := cur.next(); ok {
		t.Fatalf("cursor did not advance past the consumed op")
	}
}

// TestRecorderCursorWaitForVerbs checks that each WaitFor* wraps the right
// verb string, since a typo here would silently make one helper match
// another's ops. patch and status-update have no named helper and go through
// WaitForNext, which is the only way to assert this operator's most common
// write.
func TestRecorderCursorWaitForVerbs(t *testing.T) {
	cases := []struct {
		name   string
		kind   string
		newObj func(ns, name string) client.Object
		do     func(ctx context.Context, c client.Client, obj client.Object) error
		wait   func(t *testing.T, cur *Cursor, kind, name string, timeout time.Duration) Op
	}{
		{
			name:   "update",
			kind:   "ConfigMap",
			newObj: func(ns, name string) client.Object { return configMap(ns, name) },
			do:     func(ctx context.Context, c client.Client, obj client.Object) error { return c.Update(ctx, obj) },
			wait: func(t *testing.T, cur *Cursor, kind, name string, timeout time.Duration) Op {
				return cur.WaitForUpdate(t, kind, name, timeout)
			},
		},
		{
			name:   "delete",
			kind:   "ConfigMap",
			newObj: func(ns, name string) client.Object { return configMap(ns, name) },
			do:     func(ctx context.Context, c client.Client, obj client.Object) error { return c.Delete(ctx, obj) },
			wait: func(t *testing.T, cur *Cursor, kind, name string, timeout time.Duration) Op {
				return cur.WaitForDelete(t, kind, name, timeout)
			},
		},
		{
			name:   "patch",
			kind:   "ConfigMap",
			newObj: func(ns, name string) client.Object { return configMap(ns, name) },
			do: func(ctx context.Context, c client.Client, obj client.Object) error {
				return c.Patch(ctx, obj, client.Merge)
			},
			wait: func(t *testing.T, cur *Cursor, kind, name string, timeout time.Duration) Op {
				return cur.WaitForNext(t, "patch", kind, name, timeout)
			},
		},
		{
			// Status() writes need an object with a status subresource;
			// ConfigMap has none, so these cases use a Pod instead.
			name:   "status-patch",
			kind:   "Pod",
			newObj: func(ns, name string) client.Object { return pod(ns, name) },
			do: func(ctx context.Context, c client.Client, obj client.Object) error {
				return c.Status().Patch(ctx, obj, client.Merge)
			},
			wait: func(t *testing.T, cur *Cursor, kind, name string, timeout time.Duration) Op {
				return cur.WaitForStatusPatch(t, kind, name, timeout)
			},
		},
		{
			name:   "status-update",
			kind:   "Pod",
			newObj: func(ns, name string) client.Object { return pod(ns, name) },
			do: func(ctx context.Context, c client.Client, obj client.Object) error {
				return c.Status().Update(ctx, obj)
			},
			wait: func(t *testing.T, cur *Cursor, kind, name string, timeout time.Duration) Op {
				return cur.WaitForNext(t, "status-update", kind, name, timeout)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, wrapped := newRecordedClient("ctrl-a")
			ctx := t.Context()
			obj := tc.newObj("ns1", "obj1")

			if err := wrapped.Create(ctx, obj); err != nil {
				t.Fatalf("seed create: %v", err)
			}

			cur := rec.Cursor("ns1")
			if err := tc.do(ctx, wrapped, obj); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}

			op := tc.wait(t, cur, tc.kind, "obj1", time.Second)
			if op.Verb != tc.name {
				t.Errorf("Verb = %q, want %q", op.Verb, tc.name)
			}
		})
	}
}

// TestRecorderWaitForNextMismatch drives waitForNext directly rather than
// the *testing.T-calling wrappers, per the brief: asserting a helper fails
// means inspecting the returned error, not faking a *testing.T.
func TestRecorderWaitForNextMismatch(t *testing.T) {
	cases := []struct {
		name                 string
		verb, kind, wantName string
	}{
		{name: "wrong kind", verb: "create", kind: "Secret", wantName: "cm1"},
		{name: "wrong verb", verb: "update", kind: "ConfigMap", wantName: "cm1"},
		{name: "wrong name", verb: "create", kind: "ConfigMap", wantName: "other"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, wrapped := newRecordedClient("ctrl-a")
			ctx := t.Context()

			// The cursor has to precede the write, or every case here
			// times out on an empty log instead of rejecting the op the
			// cursor is parked on, and the three clauses go untested.
			cur := rec.Cursor("ns1")
			if err := wrapped.Create(ctx, configMap("ns1", "cm1")); err != nil {
				t.Fatalf("create: %v", err)
			}

			_, err := waitForNext(cur, tc.verb, tc.kind, tc.wantName, 50*time.Millisecond)
			if err == nil {
				t.Fatalf(
					"waitForNext(%s, %s, %s) = nil error, want a mismatch",
					tc.verb, tc.kind, tc.wantName,
				)
			}
			// Distinguishing "failed fast because the next op was wrong"
			// from "failed slowly because it never found one" is the whole
			// point: the second is what a forward-scanning matcher does.
			if !strings.Contains(err.Error(), "next op was") {
				t.Fatalf(
					"error %q reports a timeout, want a mismatch on the op at the cursor",
					err.Error(),
				)
			}
		})
	}
}

// TestRecorderWaitForNextRefusesToScanForward is the property this whole type
// exists for. Every other mismatch case above has a one-op log with no
// matching op anywhere in it, which a forward-scanning matcher also fails, by
// running out the timeout. Here the expected op sits directly behind an
// unexpected one, so a matcher that scans finds it and reports success.
func TestRecorderWaitForNextRefusesToScanForward(t *testing.T) {
	rec, wrapped := newRecordedClient("ctrl-a")
	ctx := t.Context()

	cur := rec.Cursor("ns1")
	for _, name := range []string{"cm1", "cm2"} {
		if err := wrapped.Create(ctx, configMap("ns1", name)); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	op, err := waitForNext(cur, "create", "ConfigMap", "cm2", time.Second)
	if err == nil {
		t.Fatalf("waitForNext skipped cm1 to match %s; the cursor must not scan forward", op)
	}
	if !strings.Contains(err.Error(), "cm1") {
		t.Fatalf("error %q does not name cm1, the op that was actually next", err.Error())
	}
	if op.Key.Name != "cm1" {
		t.Errorf("returned op = %s, want the intervening create of cm1", op)
	}
}

// TestRecorderCursorConsumesOpsInSequence covers the API's actual use case:
// several waits on one cursor, each resuming where the last stopped. A matcher
// that restarted from the head of the log, or one that scanned forward, would
// pass a single-wait test and fail here.
func TestRecorderCursorConsumesOpsInSequence(t *testing.T) {
	rec, wrapped := newRecordedClient("ctrl-a")
	ctx := t.Context()

	cur := rec.Cursor("ns1")
	for _, name := range []string{"cm1", "cm2"} {
		if err := wrapped.Create(ctx, configMap("ns1", name)); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	first := cur.WaitForCreate(t, "ConfigMap", "cm1", time.Second)
	second := cur.WaitForCreate(t, "ConfigMap", "cm2", time.Second)

	if first.Key.Name != "cm1" || second.Key.Name != "cm2" {
		t.Fatalf("consumed %s then %s, want cm1 then cm2", first, second)
	}
	if _, ok := cur.next(); ok {
		t.Errorf("cursor still holds an op after consuming both creates")
	}

	// Asking again for cm2 must fail rather than re-serve the op the second
	// wait already consumed.
	if _, err := waitForNext(cur, "create", "ConfigMap", "cm2", 20*time.Millisecond); err == nil {
		t.Errorf("the cursor served cm2 twice")
	}
}

// TestRecorderCursorForController is the scoping that makes an intolerant
// assertion usable while several controllers write into one namespace, which is
// the suite's actual shape.
func TestRecorderCursorForController(t *testing.T) {
	rec := NewRecorder()
	base := fake.NewClientBuilder().Build()
	shard := rec.For("shard", base)
	cell := rec.For("cell", base)
	ctx := t.Context()

	shardCur := rec.CursorFor("ns1", "shard")
	cellCur := rec.CursorFor("ns1", "cell")
	toposerverCur := rec.CursorFor("ns1", "toposerver")
	nsCur := rec.Cursor("ns1")

	for _, w := range []struct {
		c    client.Client
		name string
	}{
		{cell, "cell-1"},
		{shard, "shard-1"},
		{cell, "cell-2"},
		{shard, "shard-2"},
	} {
		if err := w.c.Create(ctx, configMap("ns1", w.name)); err != nil {
			t.Fatalf("create %s: %v", w.name, err)
		}
	}

	// The scoped cursor sees only the shard controller's writes, in order.
	// The cell writes in between are out of scope rather than a mismatch,
	// which is the difference between an assertion that can be written and
	// one that fails on a run where the protocol worked.
	op := shardCur.WaitForCreate(t, "ConfigMap", "shard-1", time.Second)
	if op.Controller != "shard" {
		t.Errorf("Controller = %q, want shard", op.Controller)
	}
	shardCur.WaitForCreate(t, "ConfigMap", "shard-2", time.Second)

	// The namespace-scoped cursor still sees every controller, in order.
	for _, name := range []string{"cell-1", "shard-1", "cell-2", "shard-2"} {
		nsCur.WaitForCreate(t, "ConfigMap", name, time.Second)
	}

	// Scoping narrows what a cursor can see; it does not relax the
	// intolerance within that scope.
	if _, err := waitForNext(
		cellCur,
		"create",
		"ConfigMap",
		"cell-2",
		50*time.Millisecond,
	); err == nil {
		t.Errorf("a controller-scoped cursor skipped cell-1 to reach cell-2")
	}

	// A controller that wrote nothing is quiet even though the namespace was
	// busy throughout.
	toposerverCur.RequireNoActionTaken(t, 20*time.Millisecond)
}

func TestRecorderWaitForNextTimeout(t *testing.T) {
	rec := NewRecorder()
	cur := rec.Cursor("ns1")

	_, err := waitForNext(cur, "create", "ConfigMap", "cm1", 20*time.Millisecond)
	if err == nil {
		t.Fatalf("waitForNext = nil error, want a timeout")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error %q does not mention a timeout", err.Error())
	}
}

func TestRecorderCursorRequireNoActionTaken(t *testing.T) {
	t.Run("quiet cursor passes", func(t *testing.T) {
		rec := NewRecorder()
		cur := rec.Cursor("ns1")
		cur.RequireNoActionTaken(t, 20*time.Millisecond)
	})

	t.Run("an op fails it", func(t *testing.T) {
		rec, wrapped := newRecordedClient("ctrl-a")
		ctx := t.Context()

		cur := rec.Cursor("ns1")
		if err := wrapped.Create(ctx, configMap("ns1", "cm1")); err != nil {
			t.Fatalf("create: %v", err)
		}

		op, ok := waitWithinQuiet(cur, 50*time.Millisecond)
		if !ok {
			t.Fatalf("waitWithinQuiet observed nothing, want the create op")
		}
		if op.Key.Name != "cm1" {
			t.Errorf("Key.Name = %q, want cm1", op.Key.Name)
		}
	})

	t.Run("observing does not consume", func(t *testing.T) {
		rec, wrapped := newRecordedClient("ctrl-a")
		ctx := t.Context()

		cur := rec.Cursor("ns1")
		if err := wrapped.Create(ctx, configMap("ns1", "cm1")); err != nil {
			t.Fatalf("create: %v", err)
		}

		if _, ok := waitWithinQuiet(cur, 50*time.Millisecond); !ok {
			t.Fatalf("waitWithinQuiet observed nothing, want the create op")
		}
		// A negative assertion that ate the op would leave a later positive
		// assertion on the same cursor waiting for something already gone.
		if op := cur.WaitForCreate(t, "ConfigMap", "cm1", time.Second); op.Key.Name != "cm1" {
			t.Errorf("Key.Name = %q, want cm1", op.Key.Name)
		}
	})
}

// TestRecorderRejectedWriteIsNotVisibleToACursor pins the difference between a
// write and an attempted write, from both sides.
//
// A cursor must not see a rejection. RequireNoActionTaken has to mean "nothing
// was written", and those two readings diverge exactly when a controller is
// losing a conflict race, which is when the assertion matters most. So must
// Counts, which is a total of writes that landed.
//
// The attempt still has to be recorded, because quiescence is the one consumer
// asking "is anything happening" rather than "what changed", and a controller
// retrying a refused write forever is the most important possible answer. Both
// halves are asserted here because holding one without the other is the bug:
// recording nothing left an erroring controller reading as quiescent, and
// recording into the cursor's view would let a refused write satisfy an
// assertion that the operator did something.
func TestRecorderRejectedWriteIsNotVisibleToACursor(t *testing.T) {
	rec, wrapped := newRecordedClient("ctrl-a")
	ctx := t.Context()

	if err := wrapped.Create(ctx, configMap("ns1", "cm1")); err != nil {
		t.Fatalf("first create: %v", err)
	}

	cur := rec.Cursor("ns1")
	attempts := rec.attemptCursor("ns1")

	if err := wrapped.Create(ctx, configMap("ns1", "cm1")); !apierrors.IsAlreadyExists(err) {
		t.Fatalf("second create = %v, want AlreadyExists", err)
	}
	if err := wrapped.Update(ctx, configMap("ns1", "missing")); !apierrors.IsNotFound(err) {
		t.Fatalf("update of a missing object = %v, want NotFound", err)
	}
	if err := wrapped.Delete(ctx, configMap("ns1", "missing")); !apierrors.IsNotFound(err) {
		t.Fatalf("delete of a missing object = %v, want NotFound", err)
	}

	cur.RequireNoActionTaken(t, 20*time.Millisecond)

	if got := rec.Counts()["ctrl-a/create/*v1.ConfigMap"]; got != 1 {
		t.Errorf("create count = %d, want 1; a rejected create was recorded", got)
	}

	// The same three writes, through the attempt cursor: all present, all
	// carrying the error the API server answered with.
	for _, want := range []string{"create", "update", "delete"} {
		op, ok := attempts.take()
		if !ok {
			t.Fatalf("attempt cursor: no op for the rejected %s", want)
		}
		if op.Verb != want {
			t.Errorf("attempt cursor: Verb = %q, want %q", op.Verb, want)
		}
		if op.accepted() {
			t.Errorf("attempt cursor: %s op records no error, want the rejection", want)
		}
		if !strings.Contains(op.String(), "rejected") {
			t.Errorf("attempt cursor: %s renders as %q, want the rejection named", want, op)
		}
	}
	if op, ok := attempts.take(); ok {
		t.Errorf("attempt cursor: unexpected fourth op %s", op)
	}
}

// TestRecorderRecordsEveryWritePath covers the write methods a client.Client
// carries beyond the obvious four. An unrecorded write is one
// RequireNoActionTaken cannot refuse, so a whole class of invisible write is
// worse than no negative assertion at all.
func TestRecorderRecordsEveryWritePath(t *testing.T) {
	t.Run("apply", func(t *testing.T) {
		rec, wrapped := newRecordedClient("ctrl-a")
		ctx := t.Context()

		cur := rec.Cursor("ns1")
		ac := corev1ac.ConfigMap("cm1", "ns1").WithData(map[string]string{"k": "v"})
		if err := wrapped.Apply(ctx, ac, client.FieldOwner("ctrl-a")); err != nil {
			t.Fatalf("apply: %v", err)
		}

		op := cur.WaitForNext(t, "apply", "ConfigMap", "cm1", time.Second)
		if op.Key.Namespace != "ns1" {
			t.Errorf("Key = %v, want ns1/cm1", op.Key)
		}
	})

	t.Run("delete all of", func(t *testing.T) {
		rec, wrapped := newRecordedClient("ctrl-a")
		ctx := t.Context()

		if err := wrapped.Create(ctx, configMap("ns1", "cm1")); err != nil {
			t.Fatalf("seed create: %v", err)
		}

		cur := rec.Cursor("ns1")
		if err := wrapped.DeleteAllOf(
			ctx,
			&corev1.ConfigMap{},
			client.InNamespace("ns1"),
		); err != nil {
			t.Fatalf("delete all of: %v", err)
		}

		// The op names no object, so the namespace from the options is the
		// only thing that puts it in reach of a cursor at all.
		op := cur.WaitForNext(t, "delete-all-of", "ConfigMap", "", time.Second)
		if op.Key.Namespace != "ns1" {
			t.Errorf("Key = %v, want namespace ns1", op.Key)
		}
	})

	t.Run("subresource writer", func(t *testing.T) {
		rec, wrapped := newRecordedClient("ctrl-a")
		ctx := t.Context()

		p := pod("ns1", "p1")
		if err := wrapped.Create(ctx, p); err != nil {
			t.Fatalf("seed create: %v", err)
		}

		cur := rec.Cursor("ns1")
		if err := wrapped.SubResource("status").Patch(ctx, p, client.Merge); err != nil {
			t.Fatalf("subresource patch: %v", err)
		}

		// Reached through SubResource rather than Status, the write still has
		// to record the same verb, since a controller may use either.
		cur.WaitForStatusPatch(t, "Pod", "p1", time.Second)
	})

	t.Run("reads pass through unrecorded", func(t *testing.T) {
		rec, wrapped := newRecordedClient("ctrl-a")
		ctx := t.Context()

		cm := configMap("ns1", "cm1")
		if err := wrapped.Create(ctx, cm); err != nil {
			t.Fatalf("seed create: %v", err)
		}

		// Wrapping every method by hand rather than embedding the interface
		// means the reads had to be forwarded by hand too, so a slip there
		// would put a read in the op log and break every negative assertion.
		cur := rec.Cursor("ns1")
		if err := wrapped.Get(
			ctx,
			client.ObjectKeyFromObject(cm),
			&corev1.ConfigMap{},
		); err != nil {
			t.Fatalf("get: %v", err)
		}
		if err := wrapped.List(
			ctx,
			&corev1.ConfigMapList{},
			client.InNamespace("ns1"),
		); err != nil {
			t.Fatalf("list: %v", err)
		}
		cur.RequireNoActionTaken(t, 20*time.Millisecond)
	})
}

func TestRecorderCursorNamespaceIsolation(t *testing.T) {
	rec, wrapped := newRecordedClient("ctrl-a")
	ctx := t.Context()

	curA := rec.Cursor("ns-a")
	curB := rec.Cursor("ns-b")

	if err := wrapped.Create(ctx, configMap("ns-a", "cm-a")); err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, ok := curB.next(); ok {
		t.Fatalf("cursor for ns-b observed an op recorded against ns-a")
	}

	op := curA.WaitForCreate(t, "ConfigMap", "cm-a", time.Second)
	if op.Key.Namespace != "ns-a" {
		t.Errorf("Key.Namespace = %q, want ns-a", op.Key.Namespace)
	}
}

func TestRecorderKindSuffix(t *testing.T) {
	cases := []struct{ in, want string }{
		// The case the function exists for, and the one no other test feeds
		// it: the operator's own CRDs arrive %T-formatted like this.
		{in: "*v1alpha1.Shard", want: "Shard"},
		{in: "*v1.ConfigMap", want: "ConfigMap"},
		{in: "Shard", want: "Shard"},
	}
	for _, tc := range cases {
		if got := KindSuffix(tc.in); got != tc.want {
			t.Errorf("KindSuffix(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestRecorderConcurrentWriters gives -race something to find. The Recorder is
// built to be written by five reconcilers at once while a test reads it, and
// every other test here writes and reads from the test goroutine only.
func TestRecorderConcurrentWriters(t *testing.T) {
	const perWriter = 20
	controllers := []string{"multigrescluster", "tablegroup", "cell", "toposerver", "shard"}

	rec := NewRecorder()
	base := fake.NewClientBuilder().Build()
	// Not t.Context(): it is cancelled before cleanups run, and the cleanup
	// below is what stops the writers logging into a finished test.
	ctx := context.Background()

	// Established before the fan-out, so the waits below race the writes
	// rather than reading a log that is already complete.
	cur := rec.CursorFor("ns1", "shard")

	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	for _, name := range controllers {
		wrapped := rec.For(name, base)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				if err := wrapped.Create(
					ctx,
					configMap("ns1", fmt.Sprintf("%s-%d", name, i)),
				); err != nil {
					t.Errorf("%s create %d: %v", name, i, err)
					return
				}
			}
		}()
	}

	// Consume the shard controller's writes while the other four write into
	// the same namespace. Each of these has to be the next shard op, which is
	// the controller-scoping guarantee under real concurrency rather than
	// against a hand-built log.
	for i := range perWriter {
		op := cur.WaitForCreate(t, "ConfigMap", fmt.Sprintf("shard-%d", i), 30*time.Second)
		if op.Controller != "shard" {
			t.Fatalf("op %d: Controller = %q, want shard", i, op.Controller)
		}
	}

	wg.Wait()

	if got := len(rec.OpsInNamespace("ns1")); got != len(controllers)*perWriter {
		t.Errorf("OpsInNamespace = %d ops, want %d", got, len(controllers)*perWriter)
	}
	if got := rec.Counts()["shard/create/*v1.ConfigMap"]; got != perWriter {
		t.Errorf("shard create count = %d, want %d", got, perWriter)
	}
}
