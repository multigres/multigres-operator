package ctrltest

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// eventConfigMap builds a ConfigMap carrying a chosen resourceVersion, for the
// tests that drive the reorder buffer through a fake watcher rather than through
// the API server.
func eventConfigMap(ns, name, rv string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			ResourceVersion: rv,
		},
		Data: map[string]string{"a": "1"},
	}
}

// TestEventsOrdersChangesAcrossKinds is the reason this stream reorders at all.
// Two kinds means two watcher goroutines feeding one channel, so with no buffer
// the order these emerge in is whichever goroutine the scheduler ran.
//
// The two creates are back to back on purpose. A test that slept between them
// would pass against an arrival-ordered merge and prove nothing.
func TestEventsOrdersChangesAcrossKinds(t *testing.T) {
	ns := shared.Namespace(t)
	st := shared.Watch(t, ns, &corev1.ConfigMapList{}, &corev1.SecretList{})

	if err := shared.Client.Create(t.Context(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: ns},
		Data:       map[string]string{"a": "1"},
	}); err != nil {
		t.Fatalf("create ConfigMap: %v", err)
	}
	if err := shared.Client.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "second", Namespace: ns},
		StringData: map[string]string{"b": "2"},
	}); err != nil {
		t.Fatalf("create Secret: %v", err)
	}

	want := []Event{
		{Type: "added", Kind: "ConfigMap", Key: client.ObjectKey{Namespace: ns, Name: "first"}},
		{Type: "added", Kind: "Secret", Key: client.ObjectKey{Namespace: ns, Name: "second"}},
	}
	for i, w := range want {
		got, err := st.Next(30 * time.Second)
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if got.Type != w.Type || got.Kind != w.Kind || got.Key != w.Key {
			t.Fatalf("event %d = %s, want %s %s %s", i, got, w.Type, w.Kind, w.Key)
		}
	}

	if extra, err := st.Next(time.Second); err == nil {
		t.Fatalf("a third event arrived: %s", extra)
	}
}

// TestEventsDropsUnprojectedChanges covers the filter's whole purpose: a write
// that bumps resourceVersion without moving anything a test cares about is a
// real watch event that must not reach a test.
//
// Re-applying identical content under a second field owner is the cheapest
// honest way to produce one. Patching an annotation would not do: annotations
// are not excluded, and must not be, since tests use them as probes.
func TestEventsDropsUnprojectedChanges(t *testing.T) {
	ns := shared.Namespace(t)
	st := shared.Watch(t, ns, &corev1.ConfigMapList{})

	apply := func(owner string) {
		t.Helper()
		ac := corev1ac.ConfigMap("shared", ns).WithData(map[string]string{"a": "1"})
		if err := shared.Client.Apply(t.Context(), ac, client.FieldOwner(owner)); err != nil {
			t.Fatalf("apply as %s: %v", owner, err)
		}
	}
	key := client.ObjectKey{Namespace: ns, Name: "shared"}
	resourceVersion := func() string {
		t.Helper()
		got := &corev1.ConfigMap{}
		if err := shared.Client.Get(t.Context(), key, got); err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		return got.GetResourceVersion()
	}

	apply("first-owner")
	if _, err := st.Next(30 * time.Second); err != nil {
		t.Fatalf("waiting for the create: %v", err)
	}
	before := resourceVersion()

	apply("second-owner")

	// Without this the test could pass vacuously: if the second apply were a
	// server-side no-op there would be no event to drop, and the projection
	// would never be exercised.
	after := resourceVersion()
	if after == before {
		t.Fatalf("the second apply did not bump resourceVersion (still %s), "+
			"so there was no event for the projection to drop", before)
	}

	if ev, err := st.Next(2 * time.Second); err == nil {
		t.Fatalf("a managedFields-only update surfaced as %s", ev)
	}
}

// TestEventsOrdersOutOfOrderArrivals holds the buffer itself honest. Every other
// test here feeds the stream through the API server, which hands out ascending
// resourceVersions in roughly arrival order, so they all pass against a merge
// that is a pass-through. This one chooses the arrival order.
func TestEventsOrdersOutOfOrderArrivals(t *testing.T) {
	ns := shared.Namespace(t)
	st := shared.Watch(t, ns)

	fake := watch.NewFakeWithOptions(watch.FakeOptions{ChannelSize: 2})
	st.addSource("ConfigMap", fake)

	fake.Add(eventConfigMap(ns, "higher", "200"))
	fake.Add(eventConfigMap(ns, "lower", "100"))

	for i, want := range []string{"lower", "higher"} {
		got, err := st.Next(5 * time.Second)
		if err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
		if got.Key.Name != want {
			t.Fatalf("event %d = %s, want the %s-resourceVersion object %q",
				i, got, want, want)
		}
	}
}

// TestEventsReportsChangedPaths is what makes an event usable as an assertion
// rather than just a wakeup: the paths that moved, and nothing else.
func TestEventsReportsChangedPaths(t *testing.T) {
	ns := shared.Namespace(t)
	st := shared.Watch(t, ns, &corev1.ConfigMapList{})

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "paths", Namespace: ns},
		Data:       map[string]string{"a": "1"},
	}
	if err := shared.Client.Create(t.Context(), cm); err != nil {
		t.Fatalf("create ConfigMap: %v", err)
	}
	if _, err := st.Next(30 * time.Second); err != nil {
		t.Fatalf("waiting for the create: %v", err)
	}

	cm.Data["a"] = "2"
	if err := shared.Client.Update(t.Context(), cm); err != nil {
		t.Fatalf("update ConfigMap: %v", err)
	}

	got, err := st.Next(30 * time.Second)
	if err != nil {
		t.Fatalf("waiting for the update: %v", err)
	}
	if len(got.Changed) != 1 || got.Changed[0] != "data.a" {
		t.Fatalf("Changed = %v, want exactly [data.a]", got.Changed)
	}
}

// TestEventsIgnoresOtherNamespaces guards the isolation boundary the whole suite
// rests on: tests run concurrently in separate namespaces off one API server, so
// a stream that leaked a neighbour's writes would break every closed-world
// assertion in the package at once.
func TestEventsIgnoresOtherNamespaces(t *testing.T) {
	mine := shared.Namespace(t)
	other := shared.Namespace(t)
	st := shared.Watch(t, mine, &corev1.ConfigMapList{})

	for _, ns := range []string{other, mine} {
		if err := shared.Client.Create(t.Context(), &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: ns},
			Data:       map[string]string{"a": "1"},
		}); err != nil {
			t.Fatalf("create ConfigMap in %s: %v", ns, err)
		}
	}

	got, err := st.Next(30 * time.Second)
	if err != nil {
		t.Fatalf("waiting for the create in %s: %v", mine, err)
	}
	if got.Key.Namespace != mine {
		t.Fatalf("event = %s, want one in %s", got, mine)
	}
	if extra, err := st.Next(2 * time.Second); err == nil {
		t.Fatalf("an event from another namespace arrived: %s", extra)
	}
}

// TestEventsRelistIsTerminal pins the rule that makes this stream worth building
// on. A 410 Gone yields current state rather than the events that were missed,
// so a stream that relisted has lost history and every assertion over it is
// void. Recovering silently would be worse than failing.
func TestEventsRelistIsTerminal(t *testing.T) {
	ns := shared.Namespace(t)
	st := shared.Watch(t, ns)

	fake := watch.NewFake()
	st.addSource("ConfigMap", fake)
	fake.Stop()

	first, err := st.Next(5 * time.Second)
	if err == nil {
		t.Fatalf("a closed watcher yielded %s, want an error", first)
	}
	if !strings.Contains(err.Error(), "lost history") {
		t.Fatalf("error %q does not name the lost history", err)
	}

	// The second call is the load-bearing half: a terminal error has to be
	// returned rather than waited out, or every downstream assertion pays the
	// full timeout once the stream is dead.
	started := time.Now()
	second, again := st.Next(30 * time.Second)
	if again == nil {
		t.Fatalf("a second Next yielded %s, want the same error", second)
	}
	if again.Error() != err.Error() {
		t.Fatalf("second error = %q, want the first one %q", again, err)
	}
	if waited := time.Since(started); waited > 2*time.Second {
		t.Fatalf("second Next blocked for %s; a terminal error must return at once", waited)
	}
}

// TestEventsNoLeak asserts nothing itself. The subtest opens a stream and lets
// its cleanup close it, and TestMain's goleak check with an empty ignore list
// does the asserting after the package finishes: a watcher or merge goroutine
// that outlived the subtest fails the whole run.
func TestEventsNoLeak(t *testing.T) {
	ns := shared.Namespace(t)

	t.Run("open and close", func(t *testing.T) {
		st := shared.Watch(t, ns, &corev1.ConfigMapList{}, &corev1.SecretList{})
		if err := shared.Client.Create(t.Context(), &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "leak", Namespace: ns},
			Data:       map[string]string{"a": "1"},
		}); err != nil {
			t.Fatalf("create ConfigMap: %v", err)
		}
		if _, err := st.Next(30 * time.Second); err != nil {
			t.Fatalf("waiting for the create: %v", err)
		}
	})
}

// TestEventsDrainsQueuedArrivalsBeforeEmitting pins the invariant that stops the
// merge inverting two events that have both already arrived: it must consult the
// arrival channel before deciding the head of its buffer is ripe.
//
// Driven synchronously, with no merge goroutine running and both events stamped
// as long since arrived, because the scenario that makes the invariant matter is
// a 50ms stall of the merge goroutine and a test that simulated one would be a
// flake generator. See the comment on emitRipe.
func TestEventsDrainsQueuedArrivalsBeforeEmitting(t *testing.T) {
	ns := shared.Namespace(t)
	st := newStream(ns)

	ripe := time.Now().Add(-time.Hour)
	for _, q := range []struct {
		name string
		rv   uint64
	}{{"higher", 200}, {"lower", 100}} {
		st.raw <- rawEvent{
			kind: "ConfigMap",
			typ:  watch.Added,
			obj:  eventConfigMap(ns, q.name, strconv.FormatUint(q.rv, 10)),
			rv:   q.rv,
			at:   ripe,
		}
	}

	held, running := st.emitRipe(nil)
	if !running {
		t.Fatal("emitRipe reported the merge should stop")
	}
	if len(held) != 0 {
		t.Fatalf("%d events still held, want every ripe event emitted", len(held))
	}

	for i, want := range []string{"lower", "higher"} {
		select {
		case got := <-st.out:
			if got.Key.Name != want {
				t.Fatalf("event %d = %s, want %q", i, got, want)
			}
		default:
			t.Fatalf("event %d never emitted; the head was judged ripe before "+
				"the queued lower-resourceVersion arrival was drained", i)
		}
	}
}

// TestEventsIgnoresForeignNamespaceArrivals pins the in-process namespace
// filter, which the server-side InNamespace scope on a real watch hides:
// TestEventsIgnoresOtherNamespaces passes with the filter deleted, because the
// foreign event never reaches the client in the first place.
func TestEventsIgnoresForeignNamespaceArrivals(t *testing.T) {
	ns := shared.Namespace(t)
	st := shared.Watch(t, ns)

	fake := watch.NewFakeWithOptions(watch.FakeOptions{ChannelSize: 2})
	st.addSource("ConfigMap", fake)

	// Lower resourceVersion than the local one, so an unfiltered stream emits
	// the intruder first rather than merely emitting it eventually.
	fake.Add(eventConfigMap("someone-elses-namespace", "intruder", "100"))
	fake.Add(eventConfigMap(ns, "mine", "200"))

	got, err := st.Next(5 * time.Second)
	if err != nil {
		t.Fatalf("waiting for the event in %s: %v", ns, err)
	}
	if got.Key.Namespace != ns || got.Key.Name != "mine" {
		t.Fatalf("event = %s, want mine in %s", got, ns)
	}
}

// TestEventsUnparseableResourceVersionIsTerminal pins the refusal to guess. A
// stream that swallowed the parse error and used zero, or fell back to arrival
// order, would pass every other test in this file, since they all feed
// parseable values, and would quietly return the whole suite's ordering
// assertions to the coin flip this primitive exists to remove.
func TestEventsUnparseableResourceVersionIsTerminal(t *testing.T) {
	ns := shared.Namespace(t)
	st := shared.Watch(t, ns)

	fake := watch.NewFakeWithOptions(watch.FakeOptions{ChannelSize: 1})
	st.addSource("ConfigMap", fake)
	fake.Add(eventConfigMap(ns, "garbage", "not-a-number"))

	ev, err := st.Next(5 * time.Second)
	if err == nil {
		t.Fatalf("a garbage resourceVersion yielded %s, want an error", ev)
	}
	if !strings.Contains(err.Error(), "unparseable resourceVersion") {
		t.Fatalf("error %q does not name the unparseable resourceVersion", err)
	}
}

// TestEventsDrainReportsTerminalError covers the second return value. Drain is
// the only way to consume the stream without calling Next, so without this a
// relist would leave a Drain-only caller asserting over a truncated history with
// no signal that it was truncated.
func TestEventsDrainReportsTerminalError(t *testing.T) {
	ns := shared.Namespace(t)
	st := shared.Watch(t, ns, &corev1.ConfigMapList{})

	for _, name := range []string{"one", "two"} {
		if err := shared.Client.Create(t.Context(), &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Data:       map[string]string{"a": "1"},
		}); err != nil {
			t.Fatalf("create ConfigMap %s: %v", name, err)
		}
	}

	// Next for the first one, because the visibility lag Drain waits out is
	// measured from when a pump accepts an event and promises nothing about how
	// long the API server takes to deliver it. Asserting on one lag's worth of
	// sleep would be betting on watch latency, which on a loaded runner is
	// exactly the bet this suite exists to stop other tests making.
	first, err := st.Next(30 * time.Second)
	if err != nil {
		t.Fatalf("waiting for the first create: %v", err)
	}
	if first.Key.Name != "one" {
		t.Fatalf("first event = %s, want one", first)
	}

	// Drain advances past what it returns, so it cannot be polled in place;
	// accumulate across calls until the second create shows up instead. This is
	// the documented usage, and it waits on delivery rather than on a timer.
	var got []Event
	Eventually(t, 30*time.Second, "the second create to drain", func() error {
		batch, err := st.Drain()
		if err != nil {
			// Terminal and so never going to clear: no point retrying.
			t.Fatalf("Drain on a healthy stream: %v", err)
		}
		got = append(got, batch...)
		if len(got) != 1 {
			return fmt.Errorf("drained %d events, want 1", len(got))
		}
		return nil
	})
	if got[0].Key.Name != "two" {
		t.Fatalf("drained %s, want two", got[0])
	}
	if again, err := st.Drain(); err != nil || len(again) != 0 {
		t.Fatalf("a further Drain = %v, %v; want nothing left and no error", again, err)
	}

	fake := watch.NewFake()
	st.addSource("ConfigMap", fake)
	fake.Stop()

	// Wait for the pump to observe the close. Drain reports a terminal error the
	// stream already has; it does not wait for one to arrive, so asserting
	// straight after Stop would be betting on the pump being scheduled inside
	// Drain's own sleep.
	select {
	case <-st.failed:
	case <-time.After(30 * time.Second):
		t.Fatal("the closed watcher never failed the stream")
	}

	if _, err := st.Drain(); err == nil {
		t.Fatal("Drain on a relisted stream returned no error")
	} else if !strings.Contains(err.Error(), "lost history") {
		t.Fatalf("error %q does not name the lost history", err)
	}
}
