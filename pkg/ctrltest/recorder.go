package ctrltest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// cursorPollInterval is how often a Cursor re-checks the op log while waiting.
const cursorPollInterval = 5 * time.Millisecond

// Op is one attempted write, attributed to the controller that issued it.
type Op struct {
	Controller string
	Verb       string
	Kind       string
	Key        client.ObjectKey
	At         time.Time

	// Err is what the wrapped call returned, so a nil Err means the API server
	// accepted the write and a non-nil one means it refused it.
	//
	// Rejections are recorded because a controller retrying a write the API
	// server keeps refusing is the busiest a namespace can be while looking
	// completely idle: the refusal changes no object, so no resourceVersion
	// moves and no watch event is emitted, and a recorder that only logged
	// successes had nothing to append either. That left an erroring-forever
	// controller reading as quiescent on both halves of the measurement.
	Err error
}

// accepted reports whether the API server took this write. It is the filter
// every consumer of the log applies by default, so that "an op" keeps meaning
// "something changed at the API server" everywhere except in the two places
// that deliberately ask for attempts: quiescence and the failure dump.
func (o Op) accepted() bool { return o.Err == nil }

func (o Op) String() string {
	s := fmt.Sprintf("%s %s %s %s", o.Controller, o.Verb, o.Kind, o.Key)
	if o.Err != nil {
		s += fmt.Sprintf(" (rejected: %v)", o.Err)
	}
	return s
}

// Recorder wraps the manager's client once per controller, so the suite can say
// which controller wrote what. This is the thing ResourceWatcher cannot be: it
// is informer-sourced, carries no attribution, and coalesces, so under five
// reconcilers it can neither name the writer nor prove a write did not happen.
//
// An op is appended after the wrapped call returns, whatever it returned, and
// carries the outcome in Op.Err. Readers filter: everything that asserts on
// writes sees accepted ops only, so the log still stands in for what the API
// server did rather than for what a controller tried, and a write that lost a
// conflict race and was retried still appears once rather than twice. The
// attempts are there for the two consumers that need "is anything happening"
// rather than "what changed": quiescence counts them as activity, and the
// failure dump shows them so a wedged controller is legible.
//
// The log is ordered by arrival at the recorder's mutex, which happens after
// the API server answered. That is NOT completion order: a goroutine can be
// answered and then be preempted before it records, so two writes that overlap
// in time can appear inverted. Within one controller the orders coincide, but
// only because the suite runs every controller at MaxConcurrentReconciles 1;
// see the note at its registration in suite.go. Across controllers the log is
// not a causal order and must not be read as one.
type Recorder struct {
	mu     sync.Mutex
	counts map[string]int
	ops    []Op

	// il is the interceptor wrapping this recorder, set by NewInterceptor at
	// construction. See reconcileLog in dump.go.
	il *Interceptor
}

func NewRecorder() *Recorder {
	return &Recorder{counts: map[string]int{}}
}

// For returns a client tagged with a controller name. Reads pass straight
// through; only writes are recorded.
func (r *Recorder) For(controller string, c client.Client) client.Client {
	return &recordingClient{client: c, controller: controller, rec: r}
}

func (r *Recorder) add(controller, verb string, obj client.Object, err error) {
	r.addKey(controller, verb, fmt.Sprintf("%T", obj), client.ObjectKeyFromObject(obj), err)
}

func (r *Recorder) addKey(controller, verb, kind string, key client.ObjectKey, err error) {
	op := Op{
		Controller: controller,
		Verb:       verb,
		Kind:       kind,
		Key:        key,
		Err:        err,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Sampled under the lock so that timestamp order and index order agree.
	// Sampling before the lock lets a reader who sorts by At get a third
	// order, different from both the index and the truth.
	op.At = time.Now()
	// Rejections are left out of the counts, which are totals of writes the
	// API server took. Counting attempts here would silently change what every
	// existing count means, and the attempts are reachable through the log.
	if op.accepted() {
		r.counts[fmt.Sprintf("%s/%s/%s", op.Controller, op.Verb, op.Kind)]++
	}
	r.ops = append(r.ops, op)
}

// applyConfigMeta is the accessor set controller-runtime itself uses to pull
// name, namespace and kind off an apply configuration, which is not a
// client.Object and so carries none of the usual metadata accessors.
type applyConfigMeta interface {
	GetName() *string
	GetNamespace() *string
	GetKind() *string
}

func (r *Recorder) addApply(
	controller, verb string,
	obj runtime.ApplyConfiguration,
	err error,
) {
	// %T on an apply configuration reads "*v1.ConfigMapApplyConfiguration",
	// so trim the suffix to leave the same kind string every other verb
	// records. GetKind is preferred where it is set, since an unstructured
	// apply configuration carries its kind but not a useful Go type name.
	kind := strings.TrimSuffix(fmt.Sprintf("%T", obj), "ApplyConfiguration")
	var key client.ObjectKey
	if m, ok := obj.(applyConfigMeta); ok {
		key = client.ObjectKey{
			Namespace: derefString(m.GetNamespace()),
			Name:      derefString(m.GetName()),
		}
		if k := derefString(m.GetKind()); k != "" {
			kind = k
		}
	}
	r.addKey(controller, verb, kind, key, err)
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Counts returns write totals keyed by controller/verb/kind. Rejected writes
// are not counted; see addKey.
func (r *Recorder) Counts() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.counts))
	for k, v := range r.counts {
		out[k] = v
	}
	return out
}

// OpsInNamespace returns the accepted writes against one namespace, in order.
// Rejected attempts are excluded, so a caller asking "did the operator do X"
// gets writes that actually landed. Recorder.All is the way to see attempts.
func (r *Recorder) OpsInNamespace(ns string) []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Op
	for _, op := range r.ops {
		if op.accepted() && op.Key.Namespace == ns {
			out = append(out, op)
		}
	}
	return out
}

// logLen is the length of the whole op log, which is what a new Cursor starts
// from. The log is append-only, so an index into it stays meaningful however
// many ops arrive afterwards.
func (r *Recorder) logLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// firstMatchFrom returns the first op at or after from that is in scope, along
// with its index. When nothing matches it reports the current log length, which
// the caller can safely treat as a new floor: every op below it has already
// been ruled out and the log never reorders or shrinks.
func (r *Recorder) firstMatchFrom(
	from int,
	ns, controller string,
	attempts bool,
) (Op, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := from; i < len(r.ops); i++ {
		op := r.ops[i]
		if !inScope(op, ns, controller, attempts) {
			continue
		}
		return op, i, true
	}
	return Op{}, len(r.ops), false
}

// Cursor tracks how much of one namespace's op log a test has already consumed,
// optionally narrowed to a single controller.
//
// What a cursor can prove that a watch or an informer cannot is absence: the
// recorder sits on the write path itself, so a cursor that stays empty across a
// quiet period means no write in its scope was accepted, not merely that no
// event was delivered. The claim is exactly as wide as the scope, and no wider.
// Writes in another namespace, writes against cluster-scoped objects (whose
// ObjectKey namespace is ""), and, on a controller-scoped cursor, writes by any
// other controller, are all things this cursor cannot see. So is a write the
// API server rejected: a cursor is accepted-only, because every assertion built
// on one is about what the operator did rather than what it tried. The one
// consumer that needs attempts is quiescence, through attemptCursor.
//
// Ownership: a Cursor belongs to one goroutine. The scan position is mutex
// guarded so that sharing one cannot corrupt the log index, but sharing is
// still a mistake: two goroutines racing on one cursor split the ops between
// them arbitrarily, which downgrades an ordering assertion into a partial one
// without any visible failure. Give each waiter its own cursor.
//
// Relatedly, the WaitFor* helpers take a *testing.T and call t.Fatalf, so they
// must run on the test's own goroutine. t.Fatalf elsewhere only ends the
// goroutine that called it, so a wait moved onto a background goroutine in
// order to "watch two controllers at once" reports its failure into the void
// and the test passes regardless. Two CursorFor cursors consumed in sequence
// express the same expectation without that hazard.
type Cursor struct {
	rec *Recorder
	ns  string
	// controller narrows the cursor to one Recorder.For name. Empty means
	// every controller writing in ns.
	controller string
	// attempts widens the cursor to writes the API server rejected as well as
	// the ones it accepted. False on every cursor a test can construct.
	attempts bool

	mu sync.Mutex
	// scan is the index into rec.ops of the first op this cursor has neither
	// consumed nor ruled out as out of scope.
	scan int
}

// Cursor returns a cursor positioned at the current end of the op log, scoped
// to every write in ns. Ops recorded before this call are not visible to it.
//
// So construct every cursor a test needs BEFORE creating the objects under
// test. With five reconcilers live, a cursor constructed after an earlier wait
// can start after the op it was meant to catch: another controller reacting to
// what the first one just wrote gets its write recorded in the gap. The rule is
// "before the first recorded write in this namespace", not "a line or two
// early", since a cursor opened many lines early is safe and one opened
// immediately after the first write is not.
//
// Two independent seam tests hit this before it was written down. If an
// assertion is about what a whole reconcile produced rather than about a
// specific next write, prefer Interceptor.Ops over a cursor: reconcile records
// are appended only once a pass returns, so they cannot interleave with the
// thing being asserted.
//
// Use this when the assertion really is about the namespace as a whole. When
// several controllers write into one namespace, which is the normal shape in
// this suite, their ops interleave and an intolerant next-op assertion trips
// over writes that are unexpected but entirely legitimate. CursorFor is the
// tool for that case.
func (r *Recorder) Cursor(ns string) *Cursor {
	return r.CursorFor(ns, "")
}

// CursorFor returns a cursor scoped to the writes one controller makes in ns,
// so that "the next op" means "the next op by this controller". controller is
// the name passed to For. An empty controller widens the scope to every
// controller, which is what Cursor gives.
//
// This is what makes an intolerant assertion usable while five reconcilers
// share a namespace. Asserting that the shard controller sets a condition and
// the tablegroup controller then reacts has to survive the pod patches and cell
// reconciles that legitimately interleave, and a matcher that refuses to skip
// ops cannot survive them by skipping. Scoping the cursor instead puts those
// writes out of scope rather than out of order, and within the scope the
// intolerance is untouched: the next op by this controller is asserted to be
// exactly the expected one.
func (r *Recorder) CursorFor(ns, controller string) *Cursor {
	return &Cursor{rec: r, ns: ns, controller: controller, scan: r.logLen()}
}

// attemptCursor is Cursor widened to every write attempted in ns, rejected ones
// included. It is unexported, and deliberately: the question it answers is "is
// anything happening", which is quiescence's question, and it is the wrong
// input to any WaitFor* matcher. An assertion that a controller wrote something
// must not be satisfiable by a write the API server refused, so the matchers
// only ever see accepted ops and the widening is not reachable from a test.
func (r *Recorder) attemptCursor(ns string) *Cursor {
	return &Cursor{rec: r, ns: ns, attempts: true, scan: r.logLen()}
}

// scope names what the cursor can see, for error messages.
func (c *Cursor) scope() string {
	if c.controller == "" {
		return c.ns
	}
	return c.ns + "/" + c.controller
}

// next returns the next in-scope op without consuming it.
func (c *Cursor) next() (Op, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peekLocked()
}

// take returns the next in-scope op and consumes it.
func (c *Cursor) take() (Op, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	op, ok := c.peekLocked()
	if ok {
		c.scan++
	}
	return op, ok
}

// peekLocked parks scan on the next in-scope op, or past the end of the log
// when there is none. Moving scan over out-of-scope ops during a peek is what
// keeps a poll loop cheap: each op in the log is examined at most once per
// cursor, instead of the whole namespace history being filtered and copied on
// every poll while the reconcilers queue behind the same mutex to record their
// own writes. A long RequireNoActionTaken must not slow the controllers whose
// silence it is asserting.
func (c *Cursor) peekLocked() (Op, bool) {
	op, idx, ok := c.rec.firstMatchFrom(c.scan, c.ns, c.controller, c.attempts)
	c.scan = idx
	return op, ok
}

// KindSuffix returns the part of a %T-formatted kind after the last '.', so
// "*v1alpha1.Shard" matches a caller-supplied "Shard". Exported because Op.Kind
// is, and a consumer scanning the op log for a kind needs the same comparison
// the matchers make rather than a hand-rolled suffix test of its own.
func KindSuffix(full string) string {
	if i := strings.LastIndex(full, "."); i >= 0 {
		return full[i+1:]
	}
	return full
}

// waitForNext blocks until the next in-scope op appears or timeout elapses,
// then checks it against verb/kind/name. It advances the cursor past that op as
// soon as one arrives, whether or not it matches: the op has been popped either
// way, and a mismatch is reported rather than skipped, since scanning forward
// for a match is exactly the eventually-assertion this type exists to rule out.
func waitForNext(c *Cursor, verb, kind, name string, timeout time.Duration) (Op, error) {
	deadline := time.Now().Add(timeout)
	for {
		if op, ok := c.take(); ok {
			if KindSuffix(op.Kind) != kind || op.Verb != verb || op.Key.Name != name {
				return op, fmt.Errorf(
					"cursor(%s): waiting for %s %s %q, next op was %s",
					c.scope(), verb, kind, name, op,
				)
			}
			return op, nil
		}
		if time.Now().After(deadline) {
			return Op{}, fmt.Errorf(
				"cursor(%s): timed out after %s waiting for %s %s %q",
				c.scope(), timeout, verb, kind, name,
			)
		}
		time.Sleep(cursorPollInterval)
	}
}

// WaitForNext pops the next op in the cursor's scope and fails the test if it
// is not verb/kind/name. It does not scan forward past a non-matching op: the
// assertion is "the next action is exactly this", not "this happens
// eventually".
//
// verb is the recorder's own verb string rather than a Kubernetes verb:
// "create", "update", "patch", "delete", "apply" and "delete-all-of" for the
// main resource, and "<subresource>-<action>" for a subresource write, so a
// status patch is "status-patch" and a scale update reached through SubResource
// is "scale-update". Plain patch is this operator's dominant write verb, so an
// API that could only assert the four named helpers below could reject the most
// common write without being able to expect it. The named helpers are sugar
// over this.
func (c *Cursor) WaitForNext(t *testing.T, verb, kind, name string, timeout time.Duration) Op {
	t.Helper()
	op, err := waitForNext(c, verb, kind, name, timeout)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return op
}

// WaitForCreate is WaitForNext for the create verb.
func (c *Cursor) WaitForCreate(t *testing.T, kind, name string, timeout time.Duration) Op {
	t.Helper()
	return c.WaitForNext(t, "create", kind, name, timeout)
}

// WaitForUpdate is WaitForNext for the update verb.
func (c *Cursor) WaitForUpdate(t *testing.T, kind, name string, timeout time.Duration) Op {
	t.Helper()
	return c.WaitForNext(t, "update", kind, name, timeout)
}

// WaitForDelete is WaitForNext for the delete verb.
func (c *Cursor) WaitForDelete(t *testing.T, kind, name string, timeout time.Duration) Op {
	t.Helper()
	return c.WaitForNext(t, "delete", kind, name, timeout)
}

// WaitForStatusPatch is WaitForNext for a status subresource patch.
func (c *Cursor) WaitForStatusPatch(t *testing.T, kind, name string, timeout time.Duration) Op {
	t.Helper()
	return c.WaitForNext(t, "status-patch", kind, name, timeout)
}

// RequireNoActionTaken fails if any op appears at the cursor within quiet.
// This is provable here in a way it is not against an informer-sourced watcher,
// because the recorder sits on the write path itself rather than on a cache
// that may simply not have delivered an event yet. What it proves is bounded by
// the cursor's scope, so read it as "no write this cursor can see", per the
// scope note on Cursor.
func (c *Cursor) RequireNoActionTaken(t *testing.T, quiet time.Duration) {
	t.Helper()
	if op, ok := waitWithinQuiet(c, quiet); ok {
		t.Fatalf("cursor(%s): expected no action within %s, but observed %s", c.scope(), quiet, op)
	}
}

// waitWithinQuiet reports whether an op appears at the cursor before quiet
// elapses. It does not consume the op: RequireNoActionTaken is a negative
// assertion, not a consuming one.
func waitWithinQuiet(c *Cursor, quiet time.Duration) (Op, bool) {
	deadline := time.Now().Add(quiet)
	for {
		if op, ok := c.next(); ok {
			return op, true
		}
		if time.Now().After(deadline) {
			return Op{}, false
		}
		time.Sleep(cursorPollInterval)
	}
}

// recordingClient holds the wrapped client in a named field rather than
// embedding the interface. Embedding promotes any method this type forgets to
// wrap, and a write the recorder never sees is a write RequireNoActionTaken
// cannot refuse, so the negative assertion would pass while a controller was
// writing. With a named field the compiler names every method that still needs
// a decision, which is why Apply, DeleteAllOf and SubResource are wrapped here
// rather than silently passed through.
type recordingClient struct {
	client     client.Client
	controller string
	rec        *Recorder
}

var (
	_ client.Client            = &recordingClient{}
	_ client.SubResourceWriter = &recordingSubResourceWriter{}
	_ client.SubResourceClient = &recordingSubResourceClient{}
)

func (r *recordingClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	return r.client.Get(ctx, key, obj, opts...)
}

func (r *recordingClient) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	return r.client.List(ctx, list, opts...)
}

func (r *recordingClient) Scheme() *runtime.Scheme { return r.client.Scheme() }

func (r *recordingClient) RESTMapper() meta.RESTMapper { return r.client.RESTMapper() }

func (r *recordingClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return r.client.GroupVersionKindFor(obj)
}

func (r *recordingClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return r.client.IsObjectNamespaced(obj)
}

func (r *recordingClient) Create(
	ctx context.Context,
	obj client.Object,
	opts ...client.CreateOption,
) error {
	err := r.client.Create(ctx, obj, opts...)
	r.rec.add(r.controller, "create", obj, err)
	return err
}

func (r *recordingClient) Update(
	ctx context.Context,
	obj client.Object,
	opts ...client.UpdateOption,
) error {
	err := r.client.Update(ctx, obj, opts...)
	r.rec.add(r.controller, "update", obj, err)
	return err
}

func (r *recordingClient) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	err := r.client.Patch(ctx, obj, patch, opts...)
	r.rec.add(r.controller, "patch", obj, err)
	return err
}

func (r *recordingClient) Delete(
	ctx context.Context,
	obj client.Object,
	opts ...client.DeleteOption,
) error {
	err := r.client.Delete(ctx, obj, opts...)
	r.rec.add(r.controller, "delete", obj, err)
	return err
}

func (r *recordingClient) Apply(
	ctx context.Context,
	obj runtime.ApplyConfiguration,
	opts ...client.ApplyOption,
) error {
	err := r.client.Apply(ctx, obj, opts...)
	r.rec.addApply(r.controller, "apply", obj, err)
	return err
}

func (r *recordingClient) DeleteAllOf(
	ctx context.Context,
	obj client.Object,
	opts ...client.DeleteAllOfOption,
) error {
	err := r.client.DeleteAllOf(ctx, obj, opts...)
	// A DeleteAllOf names no object and carries its namespace in the options
	// rather than on obj, so recording client.ObjectKeyFromObject(obj) would
	// file the op under namespace "" where no cursor could see it. The op has
	// an empty Key.Name, which is what an assertion on it has to expect.
	o := (&client.DeleteAllOfOptions{}).ApplyOptions(opts)
	r.rec.addKey(
		r.controller,
		"delete-all-of",
		fmt.Sprintf("%T", obj),
		client.ObjectKey{Namespace: o.Namespace},
		err,
	)
	return err
}

func (r *recordingClient) Status() client.SubResourceWriter {
	return &recordingSubResourceWriter{
		writer:      r.client.Status(),
		subResource: "status",
		controller:  r.controller,
		rec:         r.rec,
	}
}

func (r *recordingClient) SubResource(subResource string) client.SubResourceClient {
	sub := r.client.SubResource(subResource)
	return &recordingSubResourceClient{
		recordingSubResourceWriter: recordingSubResourceWriter{
			writer:      sub,
			subResource: subResource,
			controller:  r.controller,
			rec:         r.rec,
		},
		reader: sub,
	}
}

// recordingSubResourceWriter records subresource writes under a verb of
// "<subresource>-<action>", so Status() keeps recording "status-patch" and
// "status-update" while a subresource reached through SubResource is
// distinguishable from the main resource and from other subresources.
type recordingSubResourceWriter struct {
	writer      client.SubResourceWriter
	subResource string
	controller  string
	rec         *Recorder
}

func (r *recordingSubResourceWriter) verb(action string) string {
	return r.subResource + "-" + action
}

func (r *recordingSubResourceWriter) Create(
	ctx context.Context,
	obj client.Object,
	subResource client.Object,
	opts ...client.SubResourceCreateOption,
) error {
	err := r.writer.Create(ctx, obj, subResource, opts...)
	// Keyed on the parent object, since that is what the write targets; the
	// subResource argument is the request body.
	r.rec.add(r.controller, r.verb("create"), obj, err)
	return err
}

func (r *recordingSubResourceWriter) Update(
	ctx context.Context,
	obj client.Object,
	opts ...client.SubResourceUpdateOption,
) error {
	err := r.writer.Update(ctx, obj, opts...)
	r.rec.add(r.controller, r.verb("update"), obj, err)
	return err
}

func (r *recordingSubResourceWriter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.SubResourcePatchOption,
) error {
	err := r.writer.Patch(ctx, obj, patch, opts...)
	r.rec.add(r.controller, r.verb("patch"), obj, err)
	return err
}

func (r *recordingSubResourceWriter) Apply(
	ctx context.Context,
	obj runtime.ApplyConfiguration,
	opts ...client.SubResourceApplyOption,
) error {
	err := r.writer.Apply(ctx, obj, opts...)
	r.rec.addApply(r.controller, r.verb("apply"), obj, err)
	return err
}

// recordingSubResourceClient is the SubResource(name) client: the same recorded
// writes plus the subresource read, which passes through unrecorded like every
// other read.
type recordingSubResourceClient struct {
	recordingSubResourceWriter
	reader client.SubResourceReader
}

func (r *recordingSubResourceClient) Get(
	ctx context.Context,
	obj client.Object,
	subResource client.Object,
	opts ...client.SubResourceGetOption,
) error {
	return r.reader.Get(ctx, obj, subResource, opts...)
}
