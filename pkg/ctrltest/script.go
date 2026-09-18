// Script is the runner every scenario test in this suite is written against: an
// ordered list of steps, each doing one thing and declaring exactly which
// changes that thing may produce.
//
// The closed world is the whole point. The failure this suite exists to prevent
// is an assertion that passed because it checked that something expected
// happened rather than that it was all that happened, so every event the stream
// delivers must be permitted by the step in flight. There is no mode, flag or
// option that relaxes that, and none may be added: a step that needs one is a
// step whose declaration is wrong.
//
// That has to hold at the end of a script too, which is why Finish is not
// optional and why forgetting it fails the test.

package ctrltest

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// defaultStepTimeout bounds how long a step waits for the changes it permits.
// It is generous because a scenario step can be waiting on a failover or on a
// pod going ready, and it can never make a step pass: it only decides how long
// a step that is going to fail takes to say so.
const defaultStepTimeout = 30 * time.Second

// settleWindow is how long a step keeps listening after the last change it
// permits has arrived.
//
// A step that permits three changes and produces four is a defect, and a runner
// that returned the moment the third arrived would sleep through it. The window
// is deliberately short: a straggler later than this is not lost, it lands in
// the next step and fails there instead, so making it too small misattributes a
// failure rather than missing one.
//
// That argument holds for every step with a successor, and the last step of a
// script has none. Finish is what closes that end, over a horizon its caller
// chooses rather than this one.
const settleWindow = 250 * time.Millisecond

// unfinishedGrace is how long the end-of-test backstop waits for a straggler
// before reporting a script that never called Finish.
//
// Longer than settleWindow, and its cost is only ever paid by a script that is
// already failing, because it is diagnostics: naming the change that was about
// to be dropped is worth a second of a broken test's time.
const unfinishedGrace = 2 * time.Second

// Script runs ordered steps against one namespace, validating every observed
// change against the step that is in flight.
type Script struct {
	t  *testing.T
	st *Stream

	// StepTimeout is how long a step waits for the changes it permits. It
	// never affects which changes are permitted, so lowering it to keep a test
	// quick is safe in a way that nothing else on this type would be.
	StepTimeout time.Duration

	// f is where a programming error in the script itself is reported, and
	// reportf is where the end-of-test backstop reports. Both are the test's
	// own *testing.T in every real use, and separable only so that this
	// package's tests can observe a failure without having to survive the
	// runtime.Goexit that a real Fatalf performs.
	f       fataller
	reportf func(format string, args ...any)

	invariants []scriptInvariant

	finished bool
}

type scriptInvariant struct {
	name  string
	check func(Event) error
}

// NewScript opens a stream over kinds and returns a script. Call it before
// creating the objects under test: five reconcilers run concurrently, so a
// stream opened one line late can start after another controller's reaction has
// already landed, and the change it missed then shows up as a step that timed
// out waiting for something that already happened.
//
// Open it on a fresh namespace, which is what Suite.Namespace hands out. The
// watch carries no starting resourceVersion, so an already-populated namespace
// is replayed as a run of "added" events, and those become the first step's
// problem: either permit that baseline explicitly or start from an empty
// namespace.
//
// Choose the kinds and the window with care, because a step's allow-list is an
// exact multiset with no "N events of this kind" wildcard, and there is no
// intention to add one. The rule that follows from that is one rule, not a list
// of awkward kinds:
//
// An exact step is sound only over writes whose number follows from the
// operator's code, and is unsound over any progression a concurrent actor
// paces. Declared against a paced progression it flakes on this harness rather
// than on the operator, which is worse than not writing the assertion, because
// it spends a reader's trust on a number nobody controls. Two instances have
// been measured, and they are instances of the same thing rather than two
// separate cautions:
//
// This operator's own CR kinds. Any spec write to a TableGroup or a Shard bumps
// its generation, which sends all five concurrent reconcilers back to re-stamp
// their own conditions' observedGeneration, and that cascade takes a different
// number of passes every run. One single field write, watching only TableGroup
// and Shard, settled after 19 events, then 13, then 9 across three otherwise
// identical runs; the measurement is in testDurabilityPolicyRoundTrip's comment
// in seam_transitions_test.go.
//
// Pod status conditions, which is the one that looks safe and is not. A Pod is
// a plain core kind and its own creation and deletion are the operator's, but
// its readiness progression is not: the data plane fake (datasim.go)
// patches pod status on its own passes while reconcilePoolerReadiness writes
// the readiness-gate condition on the same object, so the number of status
// modifications a pod takes to settle is a property of that race. A pod settles
// in three most of the time and four often enough to matter. That was measured
// twice in seam_shard_lifecycle_test.go, which is the file to read before
// declaring another one: first as a fourteen-event convergence step failing
// about one run in five, and then, after that step was replaced, as a
// three-leaf readiness settle on a single new pod failing one run in eight.
// The second measurement is the useful one, because it is what a reader who had
// just seen the first fix would have assumed was safe.
//
// Do not answer this by adding one more speculative Changed leaf. Nothing bounds
// the progression at the count you just observed, so a fourth leaf moves the
// boundary rather than removing it, and the flake comes back later and reads as
// a new defect. That file's answer, and the one to copy, is to take the kind out
// of the watch and read pods directly: a state comparison across the window
// (poolPodFingerprints there) catches every accepted write because
// resourceVersion is monotonic, and the operator's own write log
// (Recorder.OpsInNamespace, one controller at a time) carries the ordering
// claims an event stream was being used for.
//
// So the useful division is not core kinds against CR kinds. It is writes the
// operator makes because its code says to, which are countable, against
// everything another actor paces, which is not. Assert over the first,
// including over the core kinds a CR write produces; read the second, or use
// RequireQuiescent for it and for the "and then nothing else happened" half
// over the CR kinds themselves.
//
// End the script with Finish. Forgetting is a failure, not a pass.
func (s *Suite) NewScript(t *testing.T, ns string, kinds ...client.ObjectList) *Script {
	t.Helper()
	sc := &Script{
		t:           t,
		st:          s.Watch(t, ns, kinds...),
		StepTimeout: defaultStepTimeout,
		f:           t,
		reportf:     t.Errorf,
	}
	// Registered after Watch, so that it runs before it: cleanups are LIFO,
	// and a drain after the stream has shut down finds nothing, which is the
	// silence this backstop exists to refuse.
	t.Cleanup(func() {
		if msg := sc.unfinished(); msg != "" {
			sc.reportf("%s", msg)
		}
	})
	return sc
}

// unfinished is the end-of-test backstop's message, or "" when there is nothing
// to say.
//
// A script that never called Finish never closed its world: the stream shuts
// down at cleanup and discards whatever it was holding, so a change the
// operator made after the last step is dropped rather than asserted. That is
// the failure this whole suite exists to prevent, so a forgotten Finish is
// reported whether or not anything was actually left behind. Silence and "I
// forgot" must not be the same expression at the end of a script either.
//
// There are exactly two quiet cases, and the narrowness is the point. Finish
// was called, even if that Finish itself failed, because the backstop is for
// the author who never closed the script rather than for one who closed it
// badly. Or the test has already failed, for any reason, where a second
// complaint would only bury the first.
//
// A step that failed is deliberately not a third case. A KnownDefect body
// turns a failing step into a passing test by design, and pinning a defect is
// what the scenario tests are told to do, so treating "a step returned an
// error" as reason enough to stay quiet would switch the backstop off in
// precisely the scripts that most need it. The same goes for a caller who
// discards a TryStep error: that already passes the test on its own, and this
// is the only net left underneath it.
func (s *Script) unfinished() string {
	if s.finished || s.t.Failed() {
		return ""
	}
	const forgot = "the script never called Finish, so whatever the operator did after " +
		"its last step went unexamined: the last step has no successor to catch its " +
		"stragglers, and the stream is discarded at cleanup. End every script with Finish"

	found, err := s.st.Next(unfinishedGrace)
	if err == nil {
		return fmt.Sprintf("%s. %s arrived after the last step with nothing to permit it",
			forgot, found)
	}
	if terminal := s.st.Terminal(); terminal != nil {
		return fmt.Sprintf("%s. Its event stream had also failed: %v", forgot, terminal)
	}
	return forgot
}

// Invariant registers a predicate checked against every event for the rest of
// the script, including events the step in flight permits. A non-nil error
// fails the script, naming both the invariant and that step.
func (s *Script) Invariant(name string, check func(Event) error) {
	s.invariants = append(s.invariants, scriptInvariant{name: name, check: check})
}

// Step runs do, then requires that the events which follow are exactly the ones
// permitted by allow, in the order given where an order is declared.
//
// do may be nil, for a step that only waits on another actor's reaction to the
// step before it.
func (s *Script) Step(name string, do func() error, allow ...Allow) {
	s.t.Helper()
	if err := s.TryStep(name, do, allow...); err != nil {
		s.fatalf("%v", err)
	}
}

// TryStep is Step's error-returning core, so that a whole step can be placed
// inside a KnownDefect body: KnownDefect takes a func() error, and a body that
// called t.Fatalf could not be inverted, because Fatalf calls runtime.Goexit.
//
// Only observations about the system under test come back as errors. A
// malformed declaration, a do that itself fails, and a stream that has lost
// history all fail the test outright instead, because KnownDefect reads a
// returned error as confirmation that the defect it pins is still live, and
// none of those three is evidence about the operator either way.
//
// That protects against structural mistakes only. A well-formed permission
// naming an object that never existed (Added("Shard", "shrad-1")) is
// indistinguishable, from in here, from a permission the operator failed to
// satisfy: it times out, returns, and confirms the pin on every run, including
// after the real defect is fixed. Nothing at this level can tell those apart,
// so a pinned step deserves the same reading as the assertion it replaced.
func (s *Script) TryStep(name string, do func() error, allow ...Allow) error {
	s.t.Helper()

	where := fmt.Sprintf("script step %q", name)

	if msg := validateAllows(where, allow); msg != "" {
		s.fatalf("%s", msg)
		return nil
	}
	leaves, order := flattenAllows(allow)

	if do != nil {
		if err := do(); err != nil {
			s.fatalf("%s: the action itself failed: %v", where, err)
			return nil
		}
	}

	seen := make([]Event, 0, len(leaves))
	at := noAssignment(len(leaves))
	deadline := time.Now().Add(s.StepTimeout)

	for len(seen) < len(leaves) {
		ev, ok, alive := s.next(where, time.Until(deadline))
		if !alive {
			return nil
		}
		if !ok {
			return unsatisfiedStep(where, leaves, at, s.StepTimeout)
		}
		if err := s.checkInvariants(where, ev); err != nil {
			return err
		}
		seen = append(seen, ev)

		fitted := assignEvents(seen, leaves, order)
		if fitted == nil {
			if assignEvents(seen, leaves, nil) != nil {
				return orderViolated(where, leaves, order, seen)
			}
			return unexpectedEvent(where, ev, leaves, seen[:len(seen)-1])
		}
		at = fitted
	}

	ev, ok, alive := s.next(where, settleWindow)
	if !alive || !ok {
		return nil
	}
	if err := s.checkInvariants(where, ev); err != nil {
		return err
	}
	return unexpectedEvent(where, ev, leaves, seen)
}

// endOfScript labels the window Finish owns, for the messages that name where a
// failure happened.
const endOfScript = "the end of the script"

// Finish closes the script: it requires silence over within, and fails on any
// change that no step permitted.
//
// Every script must end with it, and forgetting is a failure rather than a
// pass. A step's settle window can afford to be short because a straggler
// later than it lands in the next step and fails there, but the last step of a
// script has no successor, so without Finish the end of every script is open
// world: the stream is shut down at cleanup and whatever it was holding is
// discarded.
//
// within is the caller's judgement about how long the system under test could
// still react to the last step, and is the only number in this API that says
// how hard the end of a script is being checked.
//
// Use Finish(time.Second) unless the last step waits on something slower, in
// which case use whatever that step waits on. One reconcile of a busy
// controller can take several hundred milliseconds and a requeue can take
// longer, so a horizon under half a second buys almost nothing, while a second
// costs one second per script and is the difference between the end of a
// scenario being checked and being assumed.
func (s *Script) Finish(within time.Duration) {
	s.t.Helper()
	if err := s.TryFinish(within); err != nil {
		s.fatalf("%v", err)
	}
}

// TryFinish is Finish's error-returning core, on the same terms as TryStep.
func (s *Script) TryFinish(within time.Duration) error {
	s.t.Helper()

	// Set before the check rather than after it, so that a Finish which fails
	// is still a Finish that was called: the backstop exists to catch the
	// author who never closed the script, not to complain twice about one that
	// closed badly.
	s.finished = true

	ev, ok, alive := s.next(endOfScript, within)
	if !alive || !ok {
		return nil
	}
	if err := s.checkInvariants(endOfScript, ev); err != nil {
		return err
	}
	return fmt.Errorf("after the script's last step, %s arrived within %s with nothing "+
		"to permit it: every change a script produces must be permitted by the step "+
		"that caused it", ev, within)
}

// next reads one event, distinguishing a quiet stream from a broken one. It
// reports whether an event arrived, and whether the stream is still worth
// reading at all.
//
// A stream that has gone terminal fails the test on the spot rather than
// returning an error. It goes terminal when a watcher closes or a watch.Error
// arrives, which means it relisted and lost history, so every assertion over it
// from that point on is void. Waiting it out or retrying would turn a step into
// an "eventually" matcher over a truncated history. Returning it would be
// worse: inside a KnownDefect body a returned error reads as confirmation that
// the pinned defect is live, and a harness that lost history is evidence about
// nothing.
func (s *Script) next(where string, timeout time.Duration) (ev Event, ok bool, alive bool) {
	ev, err := s.st.Next(max(timeout, 0))
	if err == nil {
		return ev, true, true
	}
	if terminal := s.st.Terminal(); terminal != nil {
		s.fatalf("%s: the event stream failed and every assertion over it is void: %v",
			where, terminal)
		return Event{}, false, false
	}
	return Event{}, false, true
}

// fatalf reports a failure that is the script's own or the harness's, never the
// operator's. Everything that must not be capable of confirming a KnownDefect
// pin goes through here.
func (s *Script) fatalf(format string, args ...any) {
	s.t.Helper()
	s.f.Helper()
	s.f.Fatalf(format, args...)
}

func (s *Script) checkInvariants(where string, ev Event) error {
	for _, inv := range s.invariants {
		if err := inv.check(ev); err != nil {
			return fmt.Errorf("invariant %q violated during %s by %s: %w",
				inv.name, where, ev, err)
		}
	}
	return nil
}

// allowForm distinguishes the three shapes an Allow can take. The zero value is
// not one of them, so a struct literal that skipped the constructors is caught
// rather than silently permitting nothing.
type allowForm int

const (
	formInvalid allowForm = iota
	formLeaf
	formSeq
	formQuiet
)

// Allow permits one expected change, or, once combined with Before, a group of
// them in a declared order.
type Allow struct {
	form  allowForm
	leaf  leafAllow
	parts []Allow
}

// leafAllow is one permitted change: a kind and name, an event type, and
// optionally the projected paths a matching change has to have touched.
type leafAllow struct {
	// typ is always one of the stream's three event types. Every constructor
	// pins one, and validation rejects the zero-value Allow that is the only
	// other way to reach here, so a type-blind leaf cannot exist: one would
	// make Changed a silent superset of Added and Deleted.
	typ     string // "added", "modified" or "deleted"
	kind    string
	name    string
	changed []string
}

// Changed permits one modification of the named object, narrowed to
// modifications whose projected diff covers every path given.
//
// It permits a modification only. A create and a delete are Added and Deleted,
// and Changed matching those too would make it a superset of both with nothing
// but its name to say so: a delete's diff names every leaf that went away, so a
// type-blind Changed("Shard", "s", "status.phase") would be satisfied by the
// Shard being deleted instead of updated. There is deliberately no
// any-lifecycle constructor. A step that cannot say whether the operator should
// create or update is a step that has not decided what it is asserting.
//
// The paths are projected JSON paths into the object as the stream reports
// them, dotted and zero-indexed: "data.a", "spec.replicas",
// "spec.template.spec.containers[0].image". The stream's exclusion set applies
// first, so resourceVersion, managedFields, generation, observedGeneration and
// condition timestamps are never reported and can never be named here. Failure
// messages print the paths an event actually carried, which is the fastest way
// to find the one wanted.
//
// Naming paths narrows which event this permits; it never widens what the step
// tolerates. An event that fails to carry them is permitted by nothing and
// fails the step, which is the same answer a closed world gives to any event
// nobody named.
func Changed(kind, name string, changed ...string) Allow {
	return Allow{form: formLeaf, leaf: leafAllow{
		typ: "modified", kind: kind, name: name, changed: changed,
	}}
}

// Added permits the creation of the named object.
func Added(kind, name string) Allow {
	return Allow{form: formLeaf, leaf: leafAllow{typ: "added", kind: kind, name: name}}
}

// Deleted permits the deletion of the named object.
func Deleted(kind, name string) Allow {
	return Allow{form: formLeaf, leaf: leafAllow{typ: "deleted", kind: kind, name: name}}
}

// Quiet declares that the step permits nothing at all. It is the only way to
// write a step that expects silence, so that silence and "I forgot to say" are
// never the same expression.
//
// It asserts silence over the settle window, currently 250ms plus the stream's
// visibility lag, and not over all future time. A reaction that lands later
// than that is caught by the next step, which it fails as an unpermitted event,
// or by Finish if there is no next step. A Quiet() step is therefore worth
// having as a statement about the immediate reaction, and Finish with a horizon
// chosen by the author is the tool for "the operator never does this at all".
func Quiet() Allow {
	return Allow{form: formQuiet}
}

// Before permits everything a and b permit, and additionally requires every
// event a permits to precede every event b permits.
//
// Ordering is opt-in because five reconcilers run concurrently, so most
// interleavings are legitimate and only an ordering that follows from the code
// should be pinned. seam_fanout_test.go documents one such genuine ordering and
// the reasoning that establishes it.
func Before(a, b Allow) Allow {
	return Allow{form: formSeq, parts: []Allow{a, b}}
}

func (l leafAllow) String() string {
	var b strings.Builder
	if l.typ != "" {
		b.WriteString(l.typ)
		b.WriteString(" ")
	}
	b.WriteString(l.kind)
	b.WriteString("/")
	b.WriteString(l.name)
	if len(l.changed) > 0 {
		fmt.Fprintf(&b, " [%s]", strings.Join(l.changed, " "))
	}
	return b.String()
}

func (l leafAllow) matches(ev Event) bool {
	if l.kind != ev.Kind || l.name != ev.Key.Name {
		return false
	}
	if l.typ != "" && l.typ != ev.Type {
		return false
	}
	for _, path := range l.changed {
		if !slices.Contains(ev.Changed, path) {
			return false
		}
	}
	return true
}

// validateAllows reports the message for a malformed step declaration, or "" if
// the declaration is well formed.
func validateAllows(where string, allow []Allow) string {
	if len(allow) == 0 {
		return fmt.Sprintf("%s declares no permitted change. Every step must "+
			"state what it may produce; if it is meant to produce nothing at all, say so "+
			"with Quiet()", where)
	}
	for _, a := range allow {
		if a.form == formQuiet && len(allow) > 1 {
			return fmt.Sprintf("%s combines Quiet() with other permissions. "+
				"Quiet() permits nothing at all, so there is nothing to combine it with", where)
		}
		if msg := validateAllow(where, a, false); msg != "" {
			return msg
		}
	}
	return ""
}

func validateAllow(where string, a Allow, nested bool) string {
	switch a.form {
	case formLeaf:
		if a.leaf.kind == "" || a.leaf.name == "" {
			return fmt.Sprintf("%s permits a change with an empty kind or "+
				"name (%s)", where, a.leaf)
		}
		return ""
	case formQuiet:
		if nested {
			return fmt.Sprintf("%s uses Quiet() inside Before(). Quiet() "+
				"permits no event, so there is nothing for an ordering to be about", where)
		}
		return ""
	case formSeq:
		for _, part := range a.parts {
			if msg := validateAllow(where, part, true); msg != "" {
				return msg
			}
		}
		return ""
	}
	return fmt.Sprintf("%s declares a zero-value Allow. Build one with "+
		"Changed, Added, Deleted or Quiet", where)
}

// flattenAllows reduces a step's declaration to the flat set of permitted
// changes and the ordering constraints declared between them, as pairs of
// indices into leaves meaning "this one's event must precede that one's".
func flattenAllows(allow []Allow) (leaves []leafAllow, order [][2]int) {
	var walk func(a Allow) []int
	walk = func(a Allow) []int {
		switch a.form {
		case formLeaf:
			leaves = append(leaves, a.leaf)
			return []int{len(leaves) - 1}
		case formSeq:
			var all, prev []int
			for _, part := range a.parts {
				cur := walk(part)
				for _, earlier := range prev {
					for _, later := range cur {
						order = append(order, [2]int{earlier, later})
					}
				}
				prev = cur
				all = append(all, cur...)
			}
			return all
		}
		return nil
	}
	for _, a := range allow {
		walk(a)
	}
	return leaves, order
}

func noAssignment(n int) []int {
	at := make([]int, n)
	for i := range at {
		at[i] = -1
	}
	return at
}

// assignEvents maps the observed events onto distinct permitted changes,
// honouring every declared ordering, and returns that mapping from permitted
// change to event index, or nil when no such mapping exists.
//
// A search rather than a greedy first match, because two permitted changes can
// both match the same event while only one assignment lets the rest of the
// events fit: permitting any change to an object alongside a change to one of
// its paths is the ordinary case, not a contrived one, and greedily spending
// the broader permission on the narrower event would report a step as failing
// that in fact did exactly what it declared.
func assignEvents(seen []Event, leaves []leafAllow, order [][2]int) []int {
	at := noAssignment(len(leaves))
	var fit func(i int) bool
	fit = func(i int) bool {
		if i == len(seen) {
			return true
		}
		for l := range leaves {
			if at[l] >= 0 || !leaves[l].matches(seen[i]) {
				continue
			}
			at[l] = i
			if orderHolds(order, at) && fit(i+1) {
				return true
			}
			at[l] = -1
		}
		return false
	}
	if fit(0) {
		return at
	}
	return nil
}

// orderHolds reports whether an assignment, which may still be partial, breaks
// no declared ordering. Pairs with an unassigned end are not yet decided and
// are checked once the other end is filled in.
func orderHolds(order [][2]int, at []int) bool {
	for _, pair := range order {
		earlier, later := at[pair[0]], at[pair[1]]
		if earlier >= 0 && later >= 0 && earlier > later {
			return false
		}
	}
	return true
}

func unexpectedEvent(where string, ev Event, leaves []leafAllow, seen []Event) error {
	if len(leaves) == 0 {
		return fmt.Errorf("%s is Quiet(), so it permits no change at all, "+
			"but %s arrived", where, ev)
	}
	return fmt.Errorf("%s produced an unexpected event %s\n"+
		"\tpermitted: %s\n"+
		"\talready seen: %s",
		where, ev, describeLeaves(leaves), describeEvents(seen))
}

func unsatisfiedStep(where string, leaves []leafAllow, at []int, timeout time.Duration) error {
	var missing []string
	for i, ev := range at {
		if ev < 0 {
			missing = append(missing, leaves[i].String())
		}
	}
	return fmt.Errorf("%s timed out after %s: %d of the %d changes it "+
		"permits never arrived: %s",
		where, timeout, len(missing), len(leaves), strings.Join(missing, "; "))
}

func orderViolated(where string, leaves []leafAllow, order [][2]int, seen []Event) error {
	// Reached only when the events fit the permitted set with the orderings
	// dropped, so some declared pair is inverted in every assignment that fits,
	// including this one. The nil guard is for a future caller that has not
	// established that.
	free := assignEvents(seen, leaves, nil)
	if free == nil {
		return fmt.Errorf("%s violated a declared ordering: %s",
			where, describeEvents(seen))
	}
	for _, pair := range order {
		earlier, later := free[pair[0]], free[pair[1]]
		if earlier >= 0 && later >= 0 && earlier > later {
			return fmt.Errorf("%s declares %s before %s, but %s arrived first",
				where, leaves[pair[0]], leaves[pair[1]], seen[later])
		}
	}
	return fmt.Errorf("%s violated a declared ordering: %s",
		where, describeEvents(seen))
}

func describeLeaves(leaves []leafAllow) string {
	out := make([]string, 0, len(leaves))
	for _, l := range leaves {
		out = append(out, l.String())
	}
	return strings.Join(out, "; ")
}

func describeEvents(seen []Event) string {
	if len(seen) == 0 {
		return "nothing"
	}
	out := make([]string, 0, len(seen))
	for _, ev := range seen {
		out = append(out, ev.String())
	}
	return strings.Join(out, "; ")
}
