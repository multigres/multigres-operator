package ctrltest

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// reorderWindow bounds how long an arriving event is held before it may be
// emitted, so that the merge can emit in ascending resourceVersion rather than
// in arrival order.
//
// One goroutine per watched kind feeds one channel, so arrival order is
// goroutine scheduling order: two events microseconds apart on different kinds
// arrive in either order. The operator writes a PVC and then the pod that binds
// it inside a single reconcile, so without this an assertion about that
// ordering is a coin flip.
//
// This works only because resourceVersion is one monotonic counter across every
// resource here: one etcd behind one envtest API server. The Kubernetes API
// contract does not guarantee that and treats resourceVersion as an opaque
// token, so this stream is an envtest-only construct. Lifting it to a real
// cluster means replacing the ordering key, not tuning the window.
const reorderWindow = 50 * time.Millisecond

// reorderPoll is how often the merge re-checks whether the oldest held event
// has ripened. Polling rather than arming a timer per event keeps the merge loop
// a single select.
const reorderPoll = reorderWindow / 10

// visibilityLag is the longest an accepted event can take to become visible
// through Next or Drain: the hold itself, plus the polling granularity the merge
// notices the hold expiring at, plus one poll of slack.
//
// Next adds it to the caller's timeout and Drain waits it out, so both speak in
// terms of when an event arrived rather than when the merge got round to
// publishing it. They have to agree: a caller reaching for whichever of the two
// is convenient should not get a different answer about whether an event exists.
const visibilityLag = reorderWindow + 2*reorderPoll

// Transition is the two values one projected path moved between, each rendered
// as JSON and truncated. An absent side reads "null", which is how a leaf that
// appeared or went away renders.
type Transition struct {
	From string
	To   string
}

func (t Transition) String() string { return t.From + " -> " + t.To }

// Event is one observed change to one object, after projection.
type Event struct {
	Type    string // "added", "modified", "deleted"
	Kind    string // "Pod", "PersistentVolumeClaim", "Shard", ...
	Key     client.ObjectKey
	Changed []string // projected field paths that differ from the previous state
	At      time.Time

	// Transitions gives, for each path in Changed, the values it moved
	// between. Carried alongside Changed rather than folded into it because a
	// consumer that only wants to know which fields moved should not have to
	// parse them back out of a formatted string.
	//
	// It is what makes a hot loop diagnosable rather than merely visible: the
	// shard status loop was identified as two server-side-apply defects
	// fighting because the output named the two strings a condition message
	// alternated between, not just the path it alternated on.
	Transitions map[string]Transition
}

func (e Event) String() string {
	const shown = 4
	if len(e.Changed) > shown {
		return fmt.Sprintf("%s %s %s [%d paths: %s, ...]",
			e.Type, e.Kind, e.Key, len(e.Changed),
			strings.Join(e.Changed[:shown], " "))
	}
	return fmt.Sprintf("%s %s %s [%s]",
		e.Type, e.Kind, e.Key, strings.Join(e.Changed, " "))
}

// Stream is an ordered, filtered view of changes in one namespace.
type Stream struct {
	ns string

	// raw carries arrivals from the per-kind pumps to the merge. Buffered so a
	// momentarily busy merge does not stall a watcher and risk the API server
	// dropping the connection, which would read as lost history.
	raw chan rawEvent

	// out carries projected events from the merge to Next and Drain. Buffered
	// deeply because a test is entitled to create a fixture and only read the
	// stream afterwards; a full buffer is backpressure onto the merge, never
	// a discarded event.
	out chan Event

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	mu       sync.Mutex
	watchers []watch.Interface

	errMu  sync.Mutex
	err    error
	failed chan struct{}

	// prev is the last projected document seen per object, the baseline each
	// Changed list is computed against. Owned by the merge goroutine alone,
	// which is why it carries no lock.
	prev map[string]map[string]any
}

// rawEvent is one arrival, before projection and before ordering.
type rawEvent struct {
	kind string
	typ  watch.EventType
	obj  client.Object
	rv   uint64
	at   time.Time
}

func newStream(ns string) *Stream {
	return &Stream{
		ns:     ns,
		raw:    make(chan rawEvent, 1024),
		out:    make(chan Event, 4096),
		stop:   make(chan struct{}),
		failed: make(chan struct{}),
		prev:   map[string]map[string]any{},
	}
}

// Watch opens a stream over the given kinds in ns. It must be called before the
// objects being watched are created. Closing is registered on t.Cleanup.
//
// The watch carries no starting resourceVersion, so the API server replays the
// namespace's current contents as "added" before streaming changes. On a fresh
// namespace that is nothing, and on a populated one it is a baseline rather
// than a gap.
func (s *Suite) Watch(t *testing.T, ns string, kinds ...client.ObjectList) *Stream {
	t.Helper()

	// Deliberately not t.Context(): that is cancelled just before cleanups run,
	// which would close every watcher underneath the stream and have it report
	// lost history on the way out.
	ctx, cancel := context.WithCancel(context.Background())

	st := newStream(ns)

	for _, proto := range kinds {
		gvk, err := apiutil.GVKForObject(proto, s.Scheme)
		if err != nil {
			st.shutdown()
			cancel()
			t.Fatalf("resolve kind for %T: %v", proto, err)
		}
		list, ok := proto.DeepCopyObject().(client.ObjectList)
		if !ok {
			st.shutdown()
			cancel()
			t.Fatalf("%T does not deep-copy to a client.ObjectList", proto)
		}
		w, err := s.Client.Watch(ctx, list, client.InNamespace(ns))
		if err != nil {
			st.shutdown()
			cancel()
			t.Fatalf("watch %s in %s: %v", gvk.Kind, ns, err)
		}
		st.addSource(strings.TrimSuffix(gvk.Kind, "List"), w)
	}

	st.wg.Add(1)
	go st.merge()

	t.Cleanup(func() {
		st.shutdown()
		cancel()
	})
	return st
}

// addSource attaches one watcher and the goroutine that pumps it.
//
// It is also the seam the stream's own tests drive: a fake watcher is the only
// way to choose arrival order, to deliver an event for a foreign namespace or a
// malformed resourceVersion, or to close a watcher out from under a live stream.
func (st *Stream) addSource(kind string, w watch.Interface) {
	st.mu.Lock()
	st.watchers = append(st.watchers, w)
	st.mu.Unlock()

	st.wg.Add(1)
	go st.pump(kind, w)
}

// shutdown stops every watcher and joins every goroutine. TestMain leak-checks
// with an empty ignore list, so a watcher left running fails the package rather
// than going unnoticed.
func (st *Stream) shutdown() {
	// Before Stop, so a pump that sees its channel close during teardown can
	// tell that apart from a relist.
	st.stopOnce.Do(func() { close(st.stop) })

	st.mu.Lock()
	watchers := append([]watch.Interface(nil), st.watchers...)
	st.mu.Unlock()
	for _, w := range watchers {
		w.Stop()
	}

	st.wg.Wait()
}

// pump forwards one watcher's events onto the merge channel.
func (st *Stream) pump(kind string, w watch.Interface) {
	defer st.wg.Done()
	for {
		select {
		case <-st.stop:
			return
		case ev, ok := <-w.ResultChan():
			if !ok {
				select {
				case <-st.stop:
				default:
					st.fail(fmt.Errorf(
						"%s watch closed: the stream relisted and so lost history, "+
							"and every assertion over it from here on is void", kind))
				}
				return
			}
			if !st.forward(kind, ev) {
				return
			}
		}
	}
}

// forward classifies one arrival and hands it to the merge, reporting whether
// the pump should keep running.
func (st *Stream) forward(kind string, ev watch.Event) bool {
	switch ev.Type {
	case watch.Added, watch.Modified, watch.Deleted:
	case watch.Bookmark:
		// Carries no object change, and this stream never asks for bookmarks.
		return true
	case watch.Error:
		st.fail(fmt.Errorf(
			"%s watch returned an error event (%v): the stream has lost history, "+
				"and every assertion over it from here on is void", kind, ev.Object))
		return false
	default:
		st.fail(fmt.Errorf("%s watch returned unhandled event type %q", kind, ev.Type))
		return false
	}

	obj, ok := ev.Object.(client.Object)
	if !ok {
		st.fail(fmt.Errorf("%s watch delivered a %T, not a client.Object", kind, ev.Object))
		return false
	}
	// The server-side watch is already scoped to one namespace, but the suite
	// runs tests concurrently in separate namespaces off one API server, so the
	// filter is asserted here too rather than assumed.
	if obj.GetNamespace() != st.ns {
		return true
	}

	rv, err := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
	if err != nil {
		// Falling back to arrival order here would quietly turn every ordering
		// assertion built on this stream back into a coin flip.
		st.fail(fmt.Errorf("%s/%s carries unparseable resourceVersion %q: %w",
			kind, obj.GetName(), obj.GetResourceVersion(), err))
		return false
	}

	select {
	case st.raw <- rawEvent{kind: kind, typ: ev.Type, obj: obj, rv: rv, at: time.Now()}:
		return true
	case <-st.stop:
		return false
	}
}

// merge is the reorder buffer: it holds arrivals for reorderWindow and emits
// them in ascending resourceVersion. See the constant for why that is the
// ordering key and what it costs.
func (st *Stream) merge() {
	defer st.wg.Done()

	poll := time.NewTicker(reorderPoll)
	defer poll.Stop()

	// Sorted ascending by resourceVersion.
	var held []rawEvent

	for {
		select {
		case <-st.stop:
			return
		case ev := <-st.raw:
			held = insertByResourceVersion(held, ev)
		case <-poll.C:
		}

		var running bool
		if held, running = st.emitRipe(held); !running {
			return
		}
	}
}

// emitRipe publishes every held event whose hold has expired, lowest
// resourceVersion first, and reports whether the merge should keep running.
//
// The drainQueued call is load-bearing rather than an optimisation, and it has
// to run before every head check. An event's hold is measured from when a pump
// accepted it, so an event ages while it sits unread in raw. If the merge
// goroutine is starved for longer than the window, which a CI box running
// envtest plus five reconcilers can easily manage, it wakes to find the first
// event it reads already ripe. Emitting there would publish a higher
// resourceVersion ahead of a lower one sitting one slot behind it in raw, and
// descending adjacent pairs in raw are the cross-kind race this whole buffer
// exists for, so that is the common case rather than an exotic one.
//
// TestEventsDrainsQueuedArrivalsBeforeEmitting pins the invariant by queueing
// already-ripe events out of order and calling this directly. What no test
// simulates is the stall that makes the invariant matter: reproducing 50ms of
// scheduler starvation would be a flake generator rather than a test. That hole
// is closed by construction instead, so do not simplify the drain away.
func (st *Stream) emitRipe(held []rawEvent) ([]rawEvent, bool) {
	for {
		held = st.drainQueued(held)
		if len(held) == 0 || time.Since(held[0].at) < reorderWindow {
			return held, true
		}
		// Only ever emits from the head, so a lower-resourceVersion event that
		// genuinely arrives after a higher one has already ripened is the one
		// inversion this cannot fix. That is what the window is sized to
		// prevent, and it is the limit the design accepts.
		ev := held[0]
		held = held[1:]
		if !st.emit(ev) {
			return held, false
		}
	}
}

// drainQueued moves every already-queued arrival into the sorted buffer.
func (st *Stream) drainQueued(held []rawEvent) []rawEvent {
	for {
		select {
		case ev := <-st.raw:
			held = insertByResourceVersion(held, ev)
		default:
			return held
		}
	}
}

func insertByResourceVersion(held []rawEvent, ev rawEvent) []rawEvent {
	i := sort.Search(len(held), func(i int) bool { return held[i].rv >= ev.rv })
	held = append(held, rawEvent{})
	copy(held[i+1:], held[i:])
	held[i] = ev
	return held
}

// emit projects one arrival and publishes it, reporting whether the merge
// should keep running.
func (st *Stream) emit(ev rawEvent) bool {
	projected, ok := st.project(ev)
	if !ok {
		return true
	}
	select {
	case st.out <- projected:
		return true
	case <-st.stop:
		return false
	}
}

// project diffs one arrival against the last state seen for that object and
// reports whether the result is a change worth publishing.
//
// An event whose projection is empty is dropped rather than returned: a write
// that moved only bookkeeping is not a change any test should have to reason
// about, and surfacing it would make every closed-world assertion in the suite
// unwritable.
func (st *Stream) project(ev rawEvent) (Event, bool) {
	doc, err := projectedDoc(ev.obj)
	if err != nil {
		st.fail(fmt.Errorf("%s/%s: %w", ev.kind, ev.obj.GetName(), err))
		return Event{}, false
	}

	key := client.ObjectKeyFromObject(ev.obj)
	id := ev.kind + " " + key.String()
	prev := st.prev[id]

	var changes []projectedChange
	if ev.typ == watch.Deleted {
		// Forgotten rather than remembered, so that a recreate under the same
		// name diffs from nothing and reports as a create.
		delete(st.prev, id)
		changes = projectedChanges("", prev, nil, streamExcluded)
	} else {
		st.prev[id] = doc
		changes = projectedChanges("", prev, doc, streamExcluded)
	}
	if len(changes) == 0 {
		return Event{}, false
	}

	changed := make([]string, 0, len(changes))
	transitions := make(map[string]Transition, len(changes))
	for _, c := range changes {
		changed = append(changed, c.path)
		transitions[c.path] = Transition{From: c.from, To: c.to}
	}

	return Event{
		Type:        strings.ToLower(string(ev.typ)),
		Kind:        ev.kind,
		Key:         key,
		Changed:     changed,
		At:          ev.at,
		Transitions: transitions,
	}, true
}

// Next returns the next event, or an error on timeout. Error-returning rather
// than t.Fatalf so KnownDefect and the script runner can consume it.
//
// The visibility lag is added to the caller's timeout, so the number passed here
// means "nothing became visible in this long" rather than "nothing arrived in
// this long, minus however much of the hold it was still serving".
func (st *Stream) Next(timeout time.Duration) (Event, error) {
	if err := st.Terminal(); err != nil {
		return Event{}, err
	}

	timer := time.NewTimer(timeout + visibilityLag)
	defer timer.Stop()

	select {
	case ev := <-st.out:
		return ev, nil
	case <-st.failed:
		return Event{}, st.Terminal()
	case <-timer.C:
		if err := st.Terminal(); err != nil {
			return Event{}, err
		}
		return Event{}, fmt.Errorf("no event within %s on the stream over %s", timeout, st.ns)
	}
}

// Drain returns every event received so far and advances past them, together
// with the stream's terminal error if it has one.
//
// A non-nil error voids every event returned alongside it. Both are returned
// rather than one or the other because the events are exactly what a caller
// would otherwise assert over: after a relist this is a truncated history, and
// handing it back with no signal is the state the relist rule exists to make
// loud.
//
// It waits out the visibility lag first. An event that arrived microseconds ago
// is legitimately still being held, and a Drain that walked past it would report
// it on a later call, out of order with respect to the caller's own timeline.
func (st *Stream) Drain() ([]Event, error) {
	time.Sleep(visibilityLag)

	var out []Event
	for {
		select {
		case ev := <-st.out:
			out = append(out, ev)
		default:
			return out, st.Terminal()
		}
	}
}

// fail records the stream's terminal error. The first one wins: it is the one
// that names where history was lost, and everything after it is a consequence.
func (st *Stream) fail(err error) {
	st.errMu.Lock()
	defer st.errMu.Unlock()
	if st.err != nil {
		return
	}
	st.err = err
	close(st.failed)
}

// Terminal reports the error that ended the stream, if one has. A non-nil
// answer voids every assertion made over the stream, which is why a caller
// that exits its read loop on something other than a timeout has to check it:
// Next selects at random when both an event and the failure are ready, so a
// loop can finish without ever having been told history was lost.
func (st *Stream) Terminal() error {
	st.errMu.Lock()
	defer st.errMu.Unlock()
	return st.err
}

func projectedDoc(obj client.Object) (map[string]any, error) {
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal for projection: %w", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal for projection: %w", err)
	}
	return doc, nil
}

// projectedChange is one leaf that moved: the path, and the values either side.
type projectedChange struct {
	path string
	from string
	to   string
}

// projectedValueLimit caps each rendered value. A whole PodSpec on one side of
// a diff is not a diagnosis, and a busy stream holds thousands of events.
const projectedValueLimit = 80

// projectedChanges names the JSON paths whose values moved between two versions
// of one object, and what they moved between, skipping any path excluded
// returns true for.
//
// A nil side is treated as an absent map or slice rather than as a scalar, so a
// create names the leaves that appeared and a delete names the leaves that went
// away, instead of both collapsing to one path.
//
// The exclusion set is a parameter rather than baked in because the stream is
// not the only thing that has to diff a projected document, and the callers
// legitimately disagree about what counts as a change: a condition restamp is
// churn to RequireQuiescent and invisible to this stream. Taking the set as an
// argument is what keeps those two answers separable.
func projectedChanges(path string, a, b any, excluded func(string) bool) []projectedChange {
	if path != "" && excluded(path) {
		return nil
	}

	am, aIsMap := a.(map[string]any)
	bm, bIsMap := b.(map[string]any)
	if (aIsMap && (bIsMap || b == nil)) || (bIsMap && a == nil) {
		seen := make(map[string]bool, len(am)+len(bm))
		for k := range am {
			seen[k] = true
		}
		for k := range bm {
			seen[k] = true
		}
		keys := make([]string, 0, len(seen))
		for k := range seen {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var out []projectedChange
		for _, k := range keys {
			out = append(out, projectedChanges(projectedJoin(path, k), am[k], bm[k], excluded)...)
		}
		return out
	}

	aa, aIsSlice := a.([]any)
	bb, bIsSlice := b.([]any)
	if (aIsSlice && (bIsSlice || b == nil)) || (bIsSlice && a == nil) {
		var out []projectedChange
		for i := range max(len(aa), len(bb)) {
			var x, y any
			if i < len(aa) {
				x = aa[i]
			}
			if i < len(bb) {
				y = bb[i]
			}
			out = append(out, projectedChanges(fmt.Sprintf("%s[%d]", path, i), x, y, excluded)...)
		}
		return out
	}

	ra, _ := json.Marshal(a)
	rb, _ := json.Marshal(b)
	if string(ra) != string(rb) {
		return []projectedChange{{
			path: path,
			from: projectedValue(ra),
			to:   projectedValue(rb),
		}}
	}
	return nil
}

func projectedValue(raw []byte) string {
	if len(raw) <= projectedValueLimit {
		return string(raw)
	}
	return string(raw[:projectedValueLimit]) + "..."
}

// streamExcluded is the event stream's exclusion set: fields the API server or a
// controller rewrites without anything meaning anything different, so an update
// that moved only these is not a change at all and its event is dropped.
//
// Annotations are deliberately absent and must stay absent. Tests write
// annotations as probes precisely because no controller's apply payload mentions
// them (see shardProbeAnnotation in seam_race_test.go), so an excluded
// annotation would make those probes invisible.
func streamExcluded(path string) bool {
	switch projectedLeaf(path) {
	case "resourceVersion", "managedFields":
		// By leaf rather than as an exact top-level path: a resourceVersion
		// nested inside an embedded object is no more meaningful than the
		// outer one.
		return true
	case "lastTransitionTime", "lastUpdateTime":
		// Restamped on every rewrite of a condition, at whatever depth the
		// condition list happens to sit.
		return true
	}
	switch path {
	case "metadata.generation", "status.observedGeneration":
		return true
	}
	return false
}

func projectedLeaf(path string) string {
	return path[strings.LastIndex(path, ".")+1:]
}

func projectedJoin(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
