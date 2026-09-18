package ctrltest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// RequeueClamp is the ceiling Boot's interceptor puts on any RequeueAfter a
// controller asks for, and it is the reason a timeout in this harness means
// something is broken rather than that the system is waiting.
//
// A controller that polls out-of-band state on a one minute timer is right in
// production and ruinous in a test. The shard controller does exactly that
// while multipoolers register in the topology store: nothing in Kubernetes
// watches that store, so no event can wake the controller early and a
// converging shard sits out the full minute with every other test in the
// package queued behind it. Clamped, the same poll comes back in 50ms.
//
// Two things this deliberately does not do:
//
// It does not touch the deprecated Result.Requeue flag. That asks for a
// rate-limited requeue whose base delay is already in the low milliseconds, so
// it can never be the thing a test waits on.
//
// It does not hide the wait it compresses. Every compressed result keeps the
// duration the controller asked for in Reconcile.RequestedAfter, so a test can
// assert "the shard asked for a minute here" as a fact about the operator.
// That is a better test than waiting a minute for the same information, and it
// keeps the defect visible in an assertion rather than only in a runtime that
// nobody reads.
//
// Exported because a consumer's canary test on compression has to be able to
// name it: asserting that the harness clamped is a claim about this value, and
// a consumer that hard-coded 50ms would go quietly stale if it ever moved.
//
// One consequence is worth stating because it looks like a problem and is not:
// RequireQuiescent still works unchanged. Compression makes a namespace busy
// with reconciles while leaving it silent in writes, and quiescence is measured
// on projected state changes plus recorded writes, never on reconcile counts. A
// controller reconciling twenty times a second and writing nothing is correctly
// quiet, and a controller writing the same status forever is still caught, now
// by the write half directly rather than by inference from resourceVersion. What compression removes from that assertion is only
// the minute it used to spend waiting to start.
const RequeueClamp = 50 * time.Millisecond

// reconcilePollInterval is how often the reconcile-log waiters re-check.
const reconcilePollInterval = 5 * time.Millisecond

// Reconcile is one pass through one controller's reconcile boundary: what was
// asked of it, what it returned, how long it took, and which writes it made.
//
// This is strictly richer than the op log, and does not replace it. The op log
// sees writes and cannot see a reconcile that decided to do nothing, which is
// most of them; this sees every pass and only names its writes indirectly.
type Reconcile struct {
	// Controller is the name the reconciler was wrapped under, matching the
	// Recorder.For name its client was tagged with.
	Controller string

	// Seq counts admitted reconciles of this controller from 1. A gated
	// request is not a reconcile of the code under test and gets 0.
	Seq int

	Request reconcile.Request

	// Result is what controller-runtime was told, so its RequeueAfter is the
	// clamped value. RequestedAfter is what the controller asked for.
	Result reconcile.Result

	// RequestedAfter is the controller's own RequeueAfter, before clamping. It
	// equals Result.RequeueAfter whenever nothing was compressed.
	RequestedAfter time.Duration

	Err error

	Start time.Time
	End   time.Time

	// Gated records that the namespace gate refused the request and the real
	// reconciler was never called. Such an entry proves the suite chose not to
	// act, which is otherwise indistinguishable from a controller that never
	// woke up.
	Gated bool

	// opFrom and opTo bracket the op log around this pass: the log length
	// before the delegate was called and after it returned. See
	// Interceptor.Ops for why that plus the controller name is exactly this
	// pass's writes.
	opFrom int
	opTo   int
}

// Duration is how long the reconcile took, which is 0 for a gated request.
func (r Reconcile) Duration() time.Duration { return r.End.Sub(r.Start) }

// Compressed reports whether this pass asked for a longer requeue than the
// suite allowed.
func (r Reconcile) Compressed() bool { return r.RequestedAfter > r.Result.RequeueAfter }

func (r Reconcile) String() string {
	name := r.Controller + "#" + fmt.Sprint(r.Seq)
	if r.Gated {
		name = r.Controller + "#gated"
	}
	return fmt.Sprintf("%s %s -> %s", name, r.Request.String(), r.outcome())
}

// outcome renders the result and error the way a dump wants them: short when
// nothing happened, explicit about a compressed requeue when one did.
func (r Reconcile) outcome() string {
	switch {
	case r.Gated:
		return "gated"
	case r.Err != nil:
		return "err=" + r.Err.Error()
	case r.Compressed():
		return fmt.Sprintf("requeue %s (asked %s)", r.Result.RequeueAfter, r.RequestedAfter)
	case r.Result.RequeueAfter > 0:
		return fmt.Sprintf("requeue %s", r.Result.RequeueAfter)
	default:
		return "ok"
	}
}

// Interceptor sits between controller-runtime and every reconciler in the
// suite. It gates by namespace, compresses requeues, and records each pass.
//
// It is on the hot path of every reconcile of every controller and is called
// concurrently by five controllers' goroutines, so its state is guarded and no
// lock is held across the call into the real reconciler. Holding one would
// serialise the whole operator behind this type and would turn any lock
// ordering mistake inside a reconciler into a suite-wide deadlock.
type Interceptor struct {
	// ops is the client-level recorder whose log this type groups by
	// reconcile. Nil is legal and disables grouping.
	ops *Recorder

	// clamp is the ceiling on a requeue. Zero disables compression.
	clamp time.Duration

	// gateMu is separate from mu because the two have opposite profiles: the
	// gate is read once per reconcile and written twice per test, while the
	// log is appended to once per reconcile. Sharing one mutex would make
	// every gate check queue behind an unrelated log append.
	gateMu sync.RWMutex
	active map[string]struct{}

	mu  sync.Mutex
	seq map[string]int
	log []Reconcile
}

// NewInterceptor returns an interceptor with no active namespaces, so nothing
// reconciles until a test asks for a namespace. See Activate.
func NewInterceptor(ops *Recorder, clamp time.Duration) *Interceptor {
	i := &Interceptor{
		ops:    ops,
		clamp:  clamp,
		active: map[string]struct{}{},
		seq:    map[string]int{},
	}
	if ops != nil {
		ops.il = i
	}
	return i
}

// Activate admits reconciles for ns. Suite.Namespace does this, so a test that
// takes its namespace from there cannot forget to; a test that invents a
// namespace name of its own gets a manager that ignores it entirely, which is
// the failure mode this trade accepts in exchange for the guarantee below.
//
// The set is empty by default rather than open by default. An open default
// would mean every namespace any test ever used stays live for the rest of the
// package, and under requeue compression that is not a small cost: a finished
// test's cluster would keep reconciling twenty times a second until the
// process exits, spending CPU and burying the namespace under investigation in
// another test's log lines.
func (i *Interceptor) Activate(ns string) {
	i.gateMu.Lock()
	defer i.gateMu.Unlock()
	i.active[ns] = struct{}{}
}

// Deactivate stops admitting reconciles for ns. A reconcile already running is
// not interrupted: it holds no lock of ours and cancelling it would only make
// teardown racy.
func (i *Interceptor) Deactivate(ns string) {
	i.gateMu.Lock()
	defer i.gateMu.Unlock()
	delete(i.active, ns)
}

// Active reports whether ns is admitted. A request whose namespace is empty,
// which is what a cluster-scoped object produces, is never admitted: no
// controller in this operator reconciles one, and admitting them by default
// would be a hole in the isolation the gate exists to provide.
func (i *Interceptor) Active(ns string) bool {
	i.gateMu.RLock()
	defer i.gateMu.RUnlock()
	_, ok := i.active[ns]
	return ok
}

// Wrap returns the reconciler to hand to a controller's
// SetupWithManagerReconciler.
//
// delegate must be the same object as the receiver that SetupWithManagerReconciler
// is called on. Substitution swaps the reconcile boundary and nothing else:
// the receiver stays live on the enqueue path, because map functions and
// predicates bind to it and use its client, and some reconcilers keep mutable
// state on themselves. Two instances means one of them holds the state and the
// other does the reconciling.
func (i *Interceptor) Wrap(controller string, delegate reconcile.Reconciler) reconcile.Reconciler {
	return &intercepted{ic: i, controller: controller, delegate: delegate}
}

type intercepted struct {
	ic         *Interceptor
	controller string
	delegate   reconcile.Reconciler
}

var _ reconcile.Reconciler = &intercepted{}

func (in *intercepted) Reconcile(
	ctx context.Context,
	req reconcile.Request,
) (reconcile.Result, error) {
	if !in.ic.Active(req.Namespace) {
		now := time.Now()
		in.ic.append(Reconcile{
			Controller: in.controller,
			Request:    req,
			Gated:      true,
			Start:      now,
			End:        now,
		})
		return reconcile.Result{}, nil
	}

	rec := Reconcile{
		Controller: in.controller,
		Request:    req,
		opFrom:     in.ic.opLogLen(),
		Start:      time.Now(),
	}

	res, err := in.delegate.Reconcile(ctx, req)

	rec.End = time.Now()
	rec.opTo = in.ic.opLogLen()
	rec.Err = err
	rec.RequestedAfter = res.RequeueAfter
	if in.ic.clamp > 0 && res.RequeueAfter > in.ic.clamp {
		res.RequeueAfter = in.ic.clamp
	}
	rec.Result = res
	in.ic.append(rec)

	return res, err
}

func (i *Interceptor) opLogLen() int {
	if i.ops == nil {
		return 0
	}
	return i.ops.logLen()
}

// append assigns the sequence number and files the record. Sequence numbers
// are per controller so that a dump can say "shard#7" and mean the seventh
// time the shard controller ran, which is a stable thing to cite while five
// controllers interleave.
func (i *Interceptor) append(r Reconcile) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if !r.Gated {
		i.seq[r.Controller]++
		r.Seq = i.seq[r.Controller]
	}
	i.log = append(i.log, r)
}

// All returns every recorded pass, in the order they were filed.
//
// As with the op log, that order is arrival at this type's mutex, which for one
// controller is program order and across controllers is not a causal order.
func (i *Interceptor) All() []Reconcile {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]Reconcile, len(i.log))
	copy(out, i.log)
	return out
}

// InNamespace returns every recorded pass against one namespace, gated ones
// included.
func (i *Interceptor) InNamespace(ns string) []Reconcile {
	return i.filter(func(r Reconcile) bool { return r.Request.Namespace == ns })
}

// Requeues returns the passes in ns that asked for a requeue, which is the
// interesting subset when a test is asking why something took as long as it
// did.
func (i *Interceptor) Requeues(ns string) []Reconcile {
	return i.filter(func(r Reconcile) bool {
		return r.Request.Namespace == ns && r.RequestedAfter > 0
	})
}

func (i *Interceptor) filter(keep func(Reconcile) bool) []Reconcile {
	i.mu.Lock()
	defer i.mu.Unlock()
	var out []Reconcile
	for _, r := range i.log {
		if keep(r) {
			out = append(out, r)
		}
	}
	return out
}

// Ops returns the writes r produced.
//
// The grouping is an index range into the op log plus the controller name,
// rather than a tag carried on each op, and its soundness rests on one fact
// stated explicitly at the registration site in suite.go: every controller in
// this suite runs at MaxConcurrentReconciles 1. One in-flight reconcile per
// controller means this controller's ops inside [opFrom, opTo) can only have
// come from this pass. Other controllers' ops land in the same range and are
// filtered out by name.
//
// Raising that concurrency silently breaks this: two of one controller's
// reconciles would overlap and their writes would be attributed to both. It is
// the same fact the ordering assertions depend on, so it is one decision rather
// than two.
func (i *Interceptor) Ops(r Reconcile) []Op {
	if i.ops == nil || r.Gated {
		return nil
	}
	return i.ops.opsBetween(r.opFrom, r.opTo, r.Controller)
}

// reconcileOf returns the pass that produced the op at log index idx, for the
// failure dump's attribution column. Ops written outside any reconcile, and
// ops of a controller this interceptor never wrapped, have none.
func (i *Interceptor) reconcileOf(controller string, idx int) (Reconcile, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, r := range i.log {
		if r.Gated || r.Controller != controller {
			continue
		}
		if idx >= r.opFrom && idx < r.opTo {
			return r, true
		}
	}
	return Reconcile{}, false
}

// overlapping returns the passes whose op range intersects [from, to), which is
// how the dump decides which reconciles to describe alongside a window of ops.
func (i *Interceptor) overlapping(from, to int) []Reconcile {
	return i.filter(func(r Reconcile) bool {
		return !r.Gated && r.opFrom < to && r.opTo > from
	})
}

// WaitForRequeue blocks until controller has asked for a requeue of at least
// atLeast against key, and returns that pass.
//
// This is the assertion that keeps requeue compression honest. A test that
// wants to pin the shard controller's minute-long poll for multipooler
// registration asserts it here, and gets the answer in milliseconds instead of
// waiting out the minute it is asserting.
//
// It scans the whole log rather than from a cursor, because the requeue a test
// cares about has usually already happened by the time the test can see the
// state that caused it. The scope is the key, which carries the namespace, so
// another test's identical requeue cannot satisfy this.
//
// Note what the reconcile boundary cannot tell you: why. The reason a
// controller asked for a requeue lives in its own conditions, not in
// ctrl.Result, and map functions run before this boundary so the watch that
// caused the request is not visible either. A test that wants "asked for a
// minute because it was awaiting pooler registration" asserts the duration
// here and the reason on the object's condition.
func (i *Interceptor) WaitForRequeue(
	t *testing.T,
	controller string,
	key client.ObjectKey,
	atLeast time.Duration,
	timeout time.Duration,
) Reconcile {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if r, ok := i.findRequeue(controller, key, atLeast); ok {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"no requeue of at least %s from the %s controller for %s within %s; "+
					"it made %d recorded passes there",
				atLeast, controller, key, timeout, len(i.passesFor(controller, key)),
			)
		}
		time.Sleep(reconcilePollInterval)
	}
}

func (i *Interceptor) findRequeue(
	controller string,
	key client.ObjectKey,
	atLeast time.Duration,
) (Reconcile, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, r := range i.log {
		if r.Controller == controller && r.Request.NamespacedName == key &&
			r.RequestedAfter >= atLeast {
			return r, true
		}
	}
	return Reconcile{}, false
}

func (i *Interceptor) passesFor(controller string, key client.ObjectKey) []Reconcile {
	return i.filter(func(r Reconcile) bool {
		return r.Controller == controller && r.Request.NamespacedName == key
	})
}
