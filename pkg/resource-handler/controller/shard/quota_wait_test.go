package shard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func forbidden(message string) error {
	return fmt.Errorf("failed to create pod p: %w", apierrors.NewForbidden(
		corev1.Resource("pods"), "p", errors.New(message)))
}

var (
	errOverQuota = forbidden(
		"exceeded quota: q, requested: pods=1, used: pods=4, limited: pods=4")
	errNoCoveringQuota = forbidden("insufficient quota to consume: pods")
	errUsageUnknown    = forbidden("status unknown for quota: q, resources: pods")
)

// quotaPasses drives the reconciler's quota records as passes do: each pass
// creates an object of the Shard, and the API server answers with answer.
type quotaPasses struct {
	r      *ShardReconciler
	now    time.Time
	answer error
	// sent counts the creates that reached the API server.
	sent int
}

func newQuotaPasses() *quotaPasses {
	p := &quotaPasses{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = multigresv1alpha1.AddToScheme(scheme)
	p.r = &ShardReconciler{
		Clock: func() time.Time { return p.now },
		Client: fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Create: func(
				context.Context, client.WithWatch, client.Object, ...client.CreateOption,
			) error {
				p.sent++
				return p.answer
			},
		}).Build(),
	}
	return p
}

// pass is one pass of the Shard that tries a create, unless create is false,
// and ends with passErr, or with what the create returned when passErr is
// errFromCreate. It reports whether the create reached the API server.
func (p *quotaPasses) pass(
	t *testing.T,
	shard types.NamespacedName,
	create bool,
	passErr error,
) bool {
	t.Helper()
	ctx := withReplacementPass(t.Context())
	sent := p.sent
	if create {
		err := p.r.createUnderQuota(ctx, &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Namespace: shard.Namespace, Name: shard.Name},
		}, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: shard.Namespace, Name: "p"}})
		if passErr == errFromCreate {
			passErr = err
		}
	}
	if passErr == errFromCreate {
		passErr = nil
	}
	p.r.endPass(ctx, shard, passErr)
	return p.sent > sent
}

var errFromCreate = errors.New("the pass ends with what its create returned")

func shardKey(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

// What puts a Shard in the waiter set is a quota's answer to a create,
// whatever the pass returned; what takes it out is a pass that ends without
// an error and without a create turned down.
func TestQuotaWaiters(t *testing.T) {
	p := newQuotaPasses()
	other := errors.New("another failure")
	waiting := shardKey("a", "waiting")

	p.answer = errOverQuota
	p.pass(t, waiting, true, errFromCreate)
	p.answer = other
	p.pass(t, shardKey("a", "failing"), true, errFromCreate)
	p.answer = forbidden("maximum memory usage per Pod is 1Gi")
	p.pass(t, shardKey("a", "limit range"), true, errFromCreate)
	p.answer = nil
	p.pass(t, shardKey("a", "settled"), true, errFromCreate)
	p.answer = errUsageUnknown
	p.pass(t, shardKey("b", "waiting"), true, errFromCreate)
	p.answer = errNoCoveringQuota
	p.pass(t, shardKey("b", "uncovered"), true, errFromCreate)
	assert.True(t, p.r.quotaWaiting.waiting(waiting))
	assert.True(t, p.r.quotaWaiting.waiting(shardKey("b", "waiting")))
	assert.True(t, p.r.quotaWaiting.waiting(shardKey("b", "uncovered")))
	for _, name := range []string{"failing", "limit range", "settled"} {
		assert.False(t, p.r.quotaWaiting.waiting(shardKey("a", name)), name)
	}

	// The create was turned down and the pass ended with another error, or
	// with none because the failure was carried to its end behind a requeue.
	p.answer = errOverQuota
	p.pass(t, waiting, true, other)
	assert.True(t, p.r.quotaWaiting.waiting(waiting))
	p.pass(t, waiting, true, nil)
	assert.True(t, p.r.quotaWaiting.waiting(waiting))

	// A pass that failed before it reached its creates says nothing.
	p.pass(t, waiting, false, other)
	assert.True(t, p.r.quotaWaiting.waiting(waiting))

	// The pass whose create fits takes the Shard out again.
	p.answer = nil
	p.now = p.now.Add(quotaHoldCap)
	p.pass(t, waiting, true, errFromCreate)
	assert.False(t, p.r.quotaWaiting.waiting(waiting))

	// So does a pass that ends without an error and makes no create: a
	// Shard that was deleted, or that builds no pods any more.
	p.pass(t, shardKey("b", "waiting"), false, nil)
	p.pass(t, shardKey("b", "uncovered"), false, nil)
	assert.Empty(t, p.r.quotaWaiting.byShard)
}

// create is one create of the pod called name for the Shard at generation,
// in a pass of its own that ends with what the create returned. It returns
// that error and whether the create reached the API server.
func (p *quotaPasses) create(
	t *testing.T, shard types.NamespacedName, generation int64, name string,
) (err error, sent bool) {
	t.Helper()
	ctx := withReplacementPass(t.Context())
	before := p.sent
	err = p.r.createUnderQuota(ctx, &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: shard.Namespace, Name: shard.Name, Generation: generation,
		},
	}, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: shard.Namespace, Name: name}})
	p.r.endPass(ctx, shard, err)
	return err, p.sent > before
}

// A Shard is reconciled every few seconds while it waits on a quota, and a
// create the quota turned down is not sent by each of those passes: the next
// goes out 5 seconds after the first, and the wait doubles with every create
// turned down, up to 5 minutes. A pass in between ends on the quota's answer
// like one whose create was sent, keeps the Shard waiting and leaves what
// status says as it is. The wait is the object's own and is for the
// generation of the Shard the create was built from. A create that succeeds
// ends it.
func TestQuotaHoldSpacesOutTheCreatesOfAnObject(t *testing.T) {
	p := newQuotaPasses()
	shard := shardKey("ns", "shard")
	p.answer = errOverQuota
	start := p.now

	err, sent := p.create(t, shard, 1, "pod-0")
	require.True(t, sent)
	require.ErrorIs(t, err, errOverQuota)
	text := p.r.quotaWaiting.describe(shard, 1, quotaObjectsSeen{})
	require.Len(t, text, 1)

	waits := []time.Duration{5, 10, 20, 40, 80, 160, 300, 300, 300}
	for i, wait := range waits {
		// Passes all through the wait, the last a second before its end.
		for _, after := range []time.Duration{0, time.Second, wait*time.Second - time.Second} {
			at := p.now.Add(after)
			p.r.Clock = func() time.Time { return at }
			err, sent := p.create(t, shard, 1, "pod-0")
			require.False(
				t,
				sent,
				"wait %d: a create was sent %s into a wait of %ds",
				i,
				after,
				wait,
			)
			require.True(
				t,
				quotaExceeded(err),
				"the pass must end as on the quota's answer: %v",
				err,
			)
			require.ErrorIs(t, err, errOverQuota)
			require.True(t, p.r.quotaWaiting.waiting(shard))
			require.Equal(t, text, p.r.quotaWaiting.describe(shard, 1, quotaObjectsSeen{}))
		}
		p.now = p.now.Add(wait * time.Second)
		p.r.Clock = func() time.Time { return p.now }
		_, sent := p.create(t, shard, 1, "pod-0")
		require.True(t, sent, "wait %d: no create was sent after %ds", i, wait)
	}
	require.Equal(t, 1+len(waits), p.sent)
	require.Equal(t, start.Add(1215*time.Second), p.now)

	// Another object of the Shard, another Shard, and another generation
	// of this one have no answer of their own to wait on.
	for name, send := range map[string]func() (error, bool){
		"another pod":        func() (error, bool) { return p.create(t, shard, 1, "pod-1") },
		"another shard":      func() (error, bool) { return p.create(t, shardKey("ns", "other"), 1, "pod-0") },
		"another generation": func() (error, bool) { return p.create(t, shard, 2, "pod-0") },
	} {
		_, sent := send()
		require.True(t, sent, name)
		_, sent = send()
		require.False(t, sent, name+", a second time")
	}

	// The create fits once the wait is over: the record is gone, and a
	// create turned down after that starts at 5 seconds again.
	p.now = p.now.Add(quotaHoldCap)
	p.answer = nil
	for _, name := range []string{"pod-0", "pod-1"} {
		err, sent = p.create(t, shard, 2, name)
		require.True(t, sent)
		require.NoError(t, err)
	}
	require.False(t, p.r.quotaWaiting.waiting(shard))
	require.Empty(t, p.r.quotaWaiting.holds[shard])
	p.answer = errOverQuota
	_, sent = p.create(t, shard, 2, "pod-0")
	require.True(t, sent)
	p.now = p.now.Add(quotaHoldFloor)
	_, sent = p.create(t, shard, 2, "pod-0")
	require.True(t, sent, "the wait did not start over")
}

// The wait of an object that no pass reaches any more, because the spec no
// longer asks for it or something else created it, is dropped once it is
// over, while another object keeps the Shard waiting. A wait that is still
// going on is kept: a pass can end before it reaches the object.
func TestQuotaHoldOfAnObjectNoLongerCreatedIsDropped(t *testing.T) {
	p := newQuotaPasses()
	shard := shardKey("ns", "shard")
	p.answer = errOverQuota
	for range 3 {
		_, sent := p.create(t, shard, 1, "gone")
		require.True(t, sent)
		p.now = p.now.Add(time.Minute)
	}
	gone := quotaObject{quotaKindPod, "gone"}
	require.Equal(t, 20*time.Second, p.r.quotaWaiting.holds[shard][gone].delay)

	_, _ = p.create(t, shard, 1, "gone")
	_, sent := p.create(t, shard, 1, "other")
	require.True(t, sent)
	require.Contains(t, p.r.quotaWaiting.holds[shard], gone, "a wait that is not over")

	p.now = p.now.Add(time.Minute)
	_, _ = p.create(t, shard, 1, "other")
	require.NotContains(t, p.r.quotaWaiting.holds[shard], gone)
	require.Contains(t, p.r.quotaWaiting.holds[shard], quotaObject{quotaKindPod, "other"})
}

// Someone who made room in a quota sets or changes the retry annotation on a
// pod of the shard. That update, and no other pod event, ends the waits of
// the Shard that owns the pod, so the pass it starts sends the creates. What
// status says stays until that pass has its answer.
func TestRetryAnnotationOnAPodEndsTheShardsWaits(t *testing.T) {
	f := newReplacementFixture(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	key := client.ObjectKeyFromObject(f.shard)
	sent := 0
	f.reconciler.Client = interceptor.NewClient(
		f.client.(client.WithWatch), interceptor.Funcs{
			Create: func(
				context.Context, client.WithWatch, client.Object, ...client.CreateOption,
			) error {
				sent++
				return errOverQuota
			},
		})
	missing, err := BuildPoolPod(f.shard, "main", "z1", f.pool, 1, f.scheme)
	require.NoError(t, err)
	create := func() {
		err := f.reconciler.createUnderQuota(withReplacementPass(t.Context()), f.shard, missing)
		require.ErrorIs(t, err, errOverQuota)
	}
	create()
	create()
	require.Equal(t, 1, sent)
	text := f.reconciler.quotaWaiting.describe(key, f.shard.Generation, quotaObjectsSeen{})

	// f.pod is a pod of the shard from the production builder.
	annotated := func(value string) *corev1.Pod {
		pod := f.pod.DeepCopy()
		if value != "" {
			pod.Annotations[metadata.AnnotationRetryCreates] = value
		}
		return pod
	}
	update := func(was, is *corev1.Pod) bool {
		return retryCreatesRequested.Update(event.UpdateEvent{ObjectOld: was, ObjectNew: is})
	}
	assert.True(t, update(annotated(""), annotated("1")), "set")
	assert.True(t, update(annotated("1"), annotated("2")), "changed")
	assert.False(t, update(annotated("1"), annotated("1")), "unchanged")
	assert.False(t, update(annotated("1"), annotated("")), "removed")
	other := annotated("")
	other.Annotations[metadata.AnnotationDrainState] = "requested"
	assert.False(t, update(annotated(""), other), "another annotation, as the operator writes")
	assert.False(t, retryCreatesRequested.Create(event.CreateEvent{Object: annotated("1")}))
	assert.False(t, retryCreatesRequested.Delete(event.DeleteEvent{Object: annotated("1")}))

	unowned := annotated("1")
	unowned.OwnerReferences = nil
	assert.Empty(t, f.reconciler.retryCreatesNow(t.Context(), unowned))
	create()
	require.Equal(t, 1, sent, "a pod no Shard owns ended the waits")

	assert.Equal(t, []reconcile.Request{{NamespacedName: key}},
		f.reconciler.retryCreatesNow(t.Context(), annotated("1")))
	assert.Equal(t, text,
		f.reconciler.quotaWaiting.describe(key, f.shard.Generation, quotaObjectsSeen{}))
	create()
	require.Equal(t, 2, sent, "the pass the annotation started sent no create")
	create()
	require.Equal(t, 2, sent)
}

// A create failure carried to the end of the pass does not replace a requeue
// the pass asked for, and fails the pass when it asked for none.
func TestDeferredCreateResult(t *testing.T) {
	soon := reconcile.Result{RequeueAfter: 5 * time.Second}
	for name, tc := range map[string]struct {
		result  reconcile.Result
		waiting bool
		want    reconcile.Result
		wantErr error
	}{
		"nothing pending":                 {wantErr: errOverQuota},
		"a requeue pending":               {result: soon, want: soon},
		"a preflight waiting to be asked": {waiting: true},
		"both":                            {result: soon, waiting: true, want: soon},
	} {
		got, err := deferredCreateResult(tc.result, tc.waiting, errOverQuota)
		assert.Equal(t, tc.want, got, name)
		assert.Equal(t, tc.wantErr, err, name)
	}
}

// What status says about a Shard's quota waits is the same text on every
// pass while the waits are the same: claims before pods, resources sorted,
// no usage figures, a start time that a later create does not move, and the
// first object turned down, whichever is turned down
// after it. An answer given to an earlier generation of the Shard is
// described once more and then left out, and a kind is gone once a create of
// it succeeds.
func TestQuotaWaitersDescribe(t *testing.T) {
	var w quotaWaiters
	shard := shardKey("ns", "shard")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Empty(t, w.describe(shard, 1, quotaObjectsSeen{}))

	w.turnedDown(shard, quotaKindPod, "pod-0", 1, start, forbidden(
		"exceeded quota: team, requested: pods=1,limits.memory=1Gi,cpu=1, "+
			"used: pods=4,limits.memory=7Gi,cpu=3, limited: pods=4,limits.memory=8Gi,cpu=4"))
	w.turnedDown(shard, quotaKindClaim, "data-0", 1, start.Add(time.Minute),
		errNoCoveringQuota)
	want := []string{
		"claim data-0 waiting on a ResourceQuota that covers pods since 2026-01-01T00:01:00Z",
		"pod pod-0 waiting on ResourceQuota team (cpu,limits.memory,pods) since 2026-01-01T00:00:00Z",
	}
	assert.Equal(t, want, w.describe(shard, 1, quotaObjectsSeen{}))

	// A later create with other figures.
	w.turnedDown(shard, quotaKindPod, "pod-0", 1, start.Add(time.Hour), forbidden(
		"exceeded quota: team, requested: cpu=1,limits.memory=1Gi,pods=1, "+
			"used: cpu=4,limits.memory=8Gi,pods=9, limited: cpu=4,limits.memory=8Gi,pods=9"))
	assert.Equal(t, want, w.describe(shard, 1, quotaObjectsSeen{}))

	// Another pod of the Shard is turned down, as when the pools are
	// reconciled in another order: the pod named stays, with its answer.
	other := forbidden("exceeded quota: other, requested: pods=1, used: pods=9, limited: pods=9")
	w.turnedDown(shard, quotaKindPod, "pod-9", 1, start.Add(time.Hour+10*time.Minute), other)
	assert.Equal(t, want, w.describe(shard, 1, quotaObjectsSeen{}))
	assert.Equal(t, want, w.describe(shard, 1, quotaObjectsSeen{}))

	// The status written right after a spec edit still has the waits, and
	// the next has only those the new generation's creates were answered
	// for: here the claim. The same after the next edit.
	none := quotaObjectsSeen{}
	assert.Equal(t, want, w.describe(shard, 2, none))
	assert.Empty(t, w.describe(shard, 2, none), "an earlier generation's answers described twice")
	w.turnedDown(
		shard,
		quotaKindClaim,
		"data-0",
		2,
		start.Add(2*time.Hour),
		errNoCoveringQuota,
	)
	assert.Equal(t, want[:1], w.describe(shard, 2, none))
	assert.Equal(t, want, w.describe(shard, 3, none))
	assert.Empty(t, w.describe(shard, 3, none))
	w.fitted(shard, quotaKindClaim, "data-0")

	// The pod's create of the new generation is another pod's, with
	// another answer: that is the one named from here on.
	w.turnedDown(shard, quotaKindPod, "pod-9", 3, start.Add(3*time.Hour), errUsageUnknown)
	podWait := []string{
		"pod pod-9 waiting on ResourceQuota q (pods), whose usage is not computed yet " +
			"since 2026-01-01T00:00:00Z",
	}
	assert.Equal(t, podWait, w.describe(shard, 3, none))
	assert.Equal(t, podWait, w.describe(shard, 3, none))
	w.turnedDown(shard, quotaKindPod, "pod-9", 3, start.Add(4*time.Hour),
		forbidden("exceeded quota"))
	assert.Equal(t, []string{
		"pod pod-9 waiting on a ResourceQuota since 2026-01-01T00:00:00Z",
	}, w.describe(shard, 3, none))

	w.fitted(shard, quotaKindPod, "pod-9")
	assert.Empty(t, w.describe(shard, 3, none))
	w.turnedDown(shard, quotaKindPod, "pod-9", 3, start.Add(5*time.Hour), errOverQuota)
	assert.True(t, w.clear(shard))
	assert.False(t, w.clear(shard))
	assert.Empty(t, w.describe(shard, 3, none))
}

// The pod a quota wait names is checked against what the status update
// knows. A pod that exists, or that the spec does not ask for, gives way to
// the pod turned down last, with that pod's answer and the wait's start
// time; a pod that is missing and wanted stays, whichever is turned down
// after it, with the last answer given to its own create.
func TestQuotaWaitersDescribe_NamedObjectIsChecked(t *testing.T) {
	shard := shardKey("ns", "shard")
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	small := forbidden("exceeded quota: small, requested: pods=1, used: pods=4, limited: pods=4")
	team := forbidden(
		"exceeded quota: team, requested: requests.memory=1Gi, used: requests.memory=8Gi, " +
			"limited: requests.memory=8Gi")
	seen := func(existing, wanted []string) quotaObjectsSeen {
		return quotaObjectsSeen{
			existing: map[quotaKind]sets.Set[string]{quotaKindPod: sets.New(existing...)},
			wanted:   map[quotaKind]sets.Set[string]{quotaKindPod: sets.New(wanted...)},
		}
	}
	text := func(pod, quota string) []string {
		return []string{"pod " + pod + " waiting on ResourceQuota " + quota +
			" since 2026-01-01T00:00:00Z"}
	}
	waiting := func() *quotaWaiters {
		w := &quotaWaiters{}
		w.turnedDown(shard, quotaKindPod, "pod-2", 1, start, small)
		w.turnedDown(shard, quotaKindPod, "pod-0", 1, start.Add(time.Second), team)
		return w
	}
	both := []string{"pod-0", "pod-2"}

	neither := seen(nil, nil)
	for name, tc := range map[string]struct {
		seen quotaObjectsSeen
		want []string
	}{
		"missing and wanted":        {seen(nil, both), text("pod-2", "small (pods)")},
		"nothing known":             {quotaObjectsSeen{}, text("pod-2", "small (pods)")},
		"exists":                    {seen([]string{"pod-2"}, both), text("pod-0", "team (requests.memory)")},
		"not asked for by the spec": {seen(nil, []string{"pod-0"}), text("pod-0", "team (requests.memory)")},
		"only the claims are known": {
			quotaObjectsSeen{wanted: map[quotaKind]sets.Set[string]{quotaKindClaim: {}}},
			text("pod-2", "small (pods)"),
		},
		// Two surges whose creates are turned down in turn: the first
		// named stays, or status would change with every create.
		"neither is asked for": {neither, text("pod-2", "small (pods)")},
		"exists, and the last one turned down is not asked for": {
			seen([]string{"pod-2"}, nil), text("pod-0", "team (requests.memory)"),
		},
		// Nothing of the kind is known to wait: it is left out.
		"both exist": {seen(both, both), nil},
	} {
		w := waiting()
		assert.Equal(t, tc.want, w.describe(shard, 1, tc.seen), name)
		assert.Equal(t, tc.want, w.describe(shard, 1, tc.seen), name+", and it stays")
	}

	// A named object that is the last one turned down has nothing to give
	// way to: a surge pod, which the spec's replica count does not include,
	// while its create is the one being turned down. Once it exists it is
	// not waiting, whatever keeps the record.
	w := &quotaWaiters{}
	w.turnedDown(shard, quotaKindPod, "surge", 1, start, small)
	assert.Equal(t, text("surge", "small (pods)"), w.describe(shard, 1, seen(nil, both)))
	assert.Empty(t, w.describe(shard, 1, seen([]string{"surge"}, both)))

	// pod-2 stays missing, and every create sent after it is pod-0's:
	// pod-2's own answer stands.
	w = waiting()
	for i := range 4 {
		w.turnedDown(shard, quotaKindPod, "pod-0", 1, start.Add(time.Duration(i)*time.Hour), team)
		assert.Equal(t, text("pod-2", "small (pods)"), w.describe(shard, 1, seen(nil, both)))
	}
}

// A pass that ends a Shard's quota wait and asks for no requeue gets one, so
// that the wait leaves status without waiting for an event; a pass that
// asked for one keeps it.
func TestAfterQuotaWait(t *testing.T) {
	soon := reconcile.Result{RequeueAfter: 5 * time.Second}
	later := reconcile.Result{RequeueAfter: time.Minute}
	assert.Equal(t, reconcile.Result{RequeueAfter: quotaWaitEndedRequeue},
		afterQuotaWait(reconcile.Result{}, true))
	assert.Equal(t, reconcile.Result{}, afterQuotaWait(reconcile.Result{}, false))
	assert.Equal(t, soon, afterQuotaWait(soon, true))
	assert.Equal(t, later, afterQuotaWait(later, true))
}

// The wait of a Shard can end with nothing left to reconcile it: the create
// that was turned down is no longer wanted and every pod is Ready. The pass
// that finds that out has already written status with the wait in it, so it
// asks for one more, and that one asks for nothing. Here the Shard itself is
// gone, which ends a pass the same way: without an error, a create or a
// requeue.
func TestReconcileRequeuesOnceWhenAQuotaWaitEndsWithoutACreate(t *testing.T) {
	p := newQuotaPasses()
	p.r.Recorder = record.NewFakeRecorder(10)
	p.answer = errOverQuota
	shard := shardKey("ns", "gone")
	require.True(t, p.pass(t, shard, true, errFromCreate))
	require.True(t, p.r.quotaWaiting.waiting(shard))

	request := reconcile.Request{NamespacedName: shard}
	result, err := p.r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: quotaWaitEndedRequeue}, result)
	assert.False(t, p.r.quotaWaiting.waiting(shard))
	result, err = p.r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)
}

// What a status update knows about the objects a quota wait may name: the
// pods it listed that are not being deleted, the claims it listed, and the
// pods and claims within each pool's replica count plus the shared backup
// claim. A kind it knows nothing about is never called stale.
func TestQuotaObjectsSeen(t *testing.T) {
	shard := filesystemBackups(newTestShard())
	pod := func(ordinal int, deleting bool) corev1.Pod {
		p := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: BuildPoolPodName(shard, "main", "z1", ordinal),
		}}
		if deleting {
			p.DeletionTimestamp = ptr.To(metav1.Now())
		}
		return p
	}
	claim := func(ordinal int) string { return BuildPoolDataPVCName(shard, "main", "z1", ordinal) }

	var seen quotaObjectsSeen
	for _, kind := range []quotaKind{quotaKindPod, quotaKindClaim} {
		assert.False(t, seen.exists(kind, "any"))
		assert.False(t, seen.unwanted(kind, "any"))
	}

	seen.see(shard, "main", "z1", 3, []corev1.Pod{pod(0, false), pod(1, true)})
	assert.True(t, seen.exists(quotaKindPod, pod(0, false).Name))
	assert.False(t, seen.exists(quotaKindPod, pod(1, false).Name), "a pod that is being deleted")
	assert.False(t, seen.exists(quotaKindPod, pod(2, false).Name))
	assert.False(t, seen.unwanted(quotaKindPod, pod(2, false).Name), "a pod within the count")
	assert.True(t, seen.unwanted(quotaKindPod, pod(3, false).Name), "a pod beyond the count")
	assert.False(t, seen.unwanted(quotaKindClaim, claim(2)))
	assert.True(t, seen.unwanted(quotaKindClaim, claim(3)), "a claim beyond the count")
	assert.False(t, seen.unwanted(quotaKindClaim, BuildSharedBackupPVCName(shard)))
	assert.False(t, seen.exists(quotaKindClaim, claim(0)), "claims not listed yet")

	seen.seeClaims([]string{claim(0), BuildSharedBackupPVCName(shard)})
	assert.True(t, seen.exists(quotaKindClaim, claim(0)))
	assert.True(t, seen.exists(quotaKindClaim, BuildSharedBackupPVCName(shard)))
	assert.False(t, seen.exists(quotaKindClaim, claim(1)))
}

// The operator's role grants nothing on ResourceQuotas, so no informer for
// them can run: what the operator knows about a quota is the API server's
// answer to a create.
func TestOperatorRoleGrantsNothingOnResourceQuotas(t *testing.T) {
	role, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "config", "rbac", "role.yaml"))
	require.NoError(t, err)
	require.Contains(t, string(role), "limitranges", "the role file is not the operator's")
	assert.NotContains(t, string(role), "resourcequotas")
}
