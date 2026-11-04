package shard

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func quotaOf(hard, statusHard, used map[corev1.ResourceName]string) *corev1.ResourceQuota {
	list := func(in map[corev1.ResourceName]string) corev1.ResourceList {
		if in == nil {
			return nil
		}
		out := corev1.ResourceList{}
		for name, quantity := range in {
			out[name] = resource.MustParse(quantity)
		}
		return out
	}
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "quota"},
		Spec:       corev1.ResourceQuotaSpec{Hard: list(hard)},
		Status:     corev1.ResourceQuotaStatus{Hard: list(statusHard), Used: list(used)},
	}
}

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

// Only a change after which a create that a quota turned down may succeed
// reaches the queue. The quota controller writes status.used for every pod
// and claim that comes or goes, so the common write, usage going up, must
// not.
func TestQuotaMakesRoom(t *testing.T) {
	type amounts = map[corev1.ResourceName]string
	pods, cpu, memory := corev1.ResourcePods, corev1.ResourceRequestsCPU,
		corev1.ResourceRequestsMemory
	base := quotaOf(amounts{pods: "4", cpu: "2"}, amounts{pods: "4", cpu: "2"},
		amounts{pods: "4", cpu: "1500m"})
	room := quotaMakesRoom(&quotaWaiters{})
	for name, tc := range map[string]struct {
		was, now *corev1.ResourceQuota
		want     bool
	}{
		"nothing changed": {now: base.DeepCopy()},
		"usage rose": {now: quotaOf(amounts{pods: "4", cpu: "2"}, amounts{pods: "4", cpu: "2"},
			amounts{pods: "4", cpu: "2"})},
		"usage fell": {now: quotaOf(amounts{pods: "4", cpu: "2"}, amounts{pods: "4", cpu: "2"},
			amounts{pods: "3", cpu: "1500m"}), want: true},
		"one usage fell, another rose": {now: quotaOf(amounts{pods: "4", cpu: "2"},
			amounts{pods: "4", cpu: "2"}, amounts{pods: "3", cpu: "2"}), want: true},
		"a usage is no longer counted": {now: quotaOf(amounts{pods: "4", cpu: "2"},
			amounts{pods: "4", cpu: "2"}, amounts{pods: "4"}), want: true},
		"spec limit raised": {now: quotaOf(amounts{pods: "5", cpu: "2"},
			amounts{pods: "4", cpu: "2"}, amounts{pods: "4", cpu: "1500m"}), want: true},
		"status limit raised": {now: quotaOf(amounts{pods: "4", cpu: "2"},
			amounts{pods: "5", cpu: "2"}, amounts{pods: "4", cpu: "1500m"}), want: true},
		"limit removed": {now: quotaOf(amounts{pods: "4"}, amounts{pods: "4", cpu: "2"},
			amounts{pods: "4", cpu: "1500m"}), want: true},
		"limit lowered": {now: quotaOf(amounts{pods: "3", cpu: "2"}, amounts{pods: "3", cpu: "2"},
			amounts{pods: "4", cpu: "1500m"})},
		"limit added, its usage not computed": {now: quotaOf(
			amounts{pods: "4", cpu: "2", memory: "1Gi"},
			amounts{pods: "4", cpu: "2", memory: "1Gi"}, amounts{pods: "4", cpu: "1500m"})},
		"usage computed for an added limit": {now: quotaOf(
			amounts{pods: "4", cpu: "2", memory: "1Gi"},
			amounts{pods: "4", cpu: "2", memory: "1Gi"},
			amounts{pods: "4", cpu: "1500m", memory: "1Gi"}), want: true},
		"usage computed for a new quota": {
			was: quotaOf(amounts{pods: "4"}, nil, nil),
			now: quotaOf(amounts{pods: "4"}, amounts{pods: "4"}, amounts{pods: "0"}), want: true,
		},
	} {
		was := tc.was
		if was == nil {
			was = base
		}
		got := room.Update(event.UpdateEvent{ObjectOld: was, ObjectNew: tc.now})
		assert.Equal(t, tc.want, got, name)
	}
	assert.True(t, room.Delete(event.DeleteEvent{Object: base}))
	assert.False(t, room.Generic(event.GenericEvent{Object: base}))
}

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

func (p *quotaPasses) quotaEvent(t *testing.T, namespace string) []reconcile.Request {
	t.Helper()
	return p.r.enqueueQuotaWaiters(t.Context(), &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "quota"},
	})
}

func shardKey(namespace, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: name}
}

// A new quota takes room away, except from a Shard whose create the API
// server turned down because no quota covers it: for that Shard a quota
// being created is the event that lets the create through.
func TestQuotaMakesRoom_NewQuota(t *testing.T) {
	created := event.CreateEvent{Object: quotaOf(nil, nil, nil)}
	p := newQuotaPasses()
	room := quotaMakesRoom(&p.r.quotaWaiting)
	assert.False(t, room.Create(created), "nothing waits")

	p.answer = errOverQuota
	p.pass(t, shardKey("ns", "over"), true, errFromCreate)
	p.answer = errNoCoveringQuota
	p.pass(t, shardKey("other", "uncovered"), true, errFromCreate)
	assert.False(t, room.Create(created),
		"a new quota makes no room for a Shard that is over another")

	p.pass(t, shardKey("ns", "uncovered"), true, errFromCreate)
	assert.True(t, room.Create(created))

	p.answer = nil
	p.quotaEvent(t, "ns")
	p.pass(t, shardKey("ns", "uncovered"), true, errFromCreate)
	assert.False(t, room.Create(created))
}

// A quota event reconciles the Shards of its namespace that have a create a
// quota turned down, and no others. What puts a Shard in the set is the
// create's answer, whatever the pass returned; what takes it out is a pass
// that ends without an error and without a create turned down or held back.
func TestQuotaWaiters(t *testing.T) {
	p := newQuotaPasses()
	other := errors.New("another failure")
	waiting := []reconcile.Request{{NamespacedName: shardKey("a", "waiting")}}
	assert.Empty(t, p.quotaEvent(t, "a"))

	p.answer = errOverQuota
	p.pass(t, shardKey("a", "waiting"), true, errFromCreate)
	p.answer = other
	p.pass(t, shardKey("a", "failing"), true, errFromCreate)
	p.answer = forbidden("maximum memory usage per Pod is 1Gi")
	p.pass(t, shardKey("a", "limit range"), true, errFromCreate)
	p.answer = nil
	p.pass(t, shardKey("a", "settled"), true, errFromCreate)
	p.answer = errUsageUnknown
	p.pass(t, shardKey("b", "waiting"), true, errFromCreate)
	assert.Equal(t, waiting, p.quotaEvent(t, "a"))
	assert.Equal(t, []reconcile.Request{{NamespacedName: shardKey("b", "waiting")}},
		p.quotaEvent(t, "b"))
	assert.Empty(t, p.quotaEvent(t, "c"))

	// The create was turned down and the pass ended with another error, or
	// with none because the failure was carried to its end behind a requeue.
	p.answer = errOverQuota
	p.pass(t, shardKey("a", "waiting"), true, other)
	assert.True(t, p.r.quotaWaiting.waiting(shardKey("a", "waiting")))
	p.now = p.now.Add(time.Hour)
	p.pass(t, shardKey("a", "waiting"), true, nil)
	assert.True(t, p.r.quotaWaiting.waiting(shardKey("a", "waiting")))

	// A create held back keeps the Shard waiting as one turned down does.
	require.False(t, p.pass(t, shardKey("a", "waiting"), true, nil))
	assert.True(t, p.r.quotaWaiting.waiting(shardKey("a", "waiting")))

	// A pass that failed before it reached its creates says nothing.
	p.pass(t, shardKey("a", "waiting"), false, other)
	assert.True(t, p.r.quotaWaiting.waiting(shardKey("a", "waiting")))

	// The pass after the create fits takes the Shard out again.
	p.answer = nil
	p.now = p.now.Add(2 * time.Hour)
	require.True(t, p.pass(t, shardKey("a", "waiting"), true, errFromCreate))
	assert.False(t, p.r.quotaWaiting.waiting(shardKey("a", "waiting")))
	assert.Len(t, p.r.quotaWaiting.byNamespace, 1, "an emptied namespace must not stay in the map")

	// So does a pass that ends without an error and makes no create: a
	// Shard that was deleted, or that builds no pods any more.
	p.pass(t, shardKey("b", "waiting"), false, nil)
	assert.False(t, p.r.quotaWaiting.waiting(shardKey("b", "waiting")))
	assert.Empty(t, p.r.quotaWaiting.byNamespace)
}

// A Shard that a quota turned down sends its next create after as long as
// it has been turned down so far, at least quotaRetryFloor and at most
// quotaRetryCap, however often it is reconciled in between: a pass every
// five seconds, as the disruption preflight asks for, sends creates at 0, 5,
// 10, 20, 40... seconds. A pass that ended on the quota's answer is retried
// when the next create is due; one that ended on another error, or a Shard
// that is not waiting, gets the rate limiter's own delay.
func TestQuotaWaiterSendsCreatesOnItsOwnSchedule(t *testing.T) {
	p := newQuotaPasses()
	p.answer = errOverQuota
	shard := shardKey("ns", "shard")
	request := reconcile.Request{NamespacedName: shard}
	limiter := quotaRetryLimiter{
		TypedRateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			5*time.Millisecond, 1000*time.Second),
		waiting: &p.r.quotaWaiting, now: p.r.now,
	}
	start := p.now

	var sentAt []time.Duration
	for elapsed := time.Duration(0); elapsed <= 90*time.Second; elapsed += 5 * time.Second {
		p.now = start.Add(elapsed)
		if p.pass(t, shard, true, nil) {
			sentAt = append(sentAt, elapsed)
		}
	}
	assert.Equal(t, []time.Duration{
		0, 5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second,
		80 * time.Second,
	}, sentAt)

	// Reconciled only by its own retries, the Shard sends one create per
	// pass, at 1, 2, 4... seconds, up to the cap.
	p.r.quotaWaiting.clear(shard)
	var waits []time.Duration
	for range 13 {
		require.True(t, p.pass(t, shard, true, errFromCreate), "a retry was spent without a create")
		wait := limiter.When(request)
		waits = append(waits, wait)
		p.now = p.now.Add(wait)
	}
	want := []time.Duration{time.Second}
	for wait := time.Second; len(want) < 13; wait *= 2 {
		want = append(want, min(wait, quotaRetryCap))
	}
	assert.Equal(t, want, waits)

	// Another error ends the pass, with the create held back or not
	// reached: the limiter's own delay, which starts at 5ms, because the
	// passes that ended on the quota's answer are not failures it counts.
	p.now = p.now.Add(-time.Second)
	p.pass(t, shard, false, errors.New("another failure"))
	assert.Equal(t, 5*time.Millisecond, limiter.When(request))
	require.False(t, p.pass(t, shard, true, errFromCreate))
	assert.Equal(t, time.Second, limiter.When(request))
	require.False(t, p.pass(t, shard, true, errors.New("another failure")))
	assert.Equal(t, 5*time.Millisecond, limiter.When(request))
	p.pass(t, shard, false, errors.New("another failure"))
	assert.Equal(t, 10*time.Millisecond, limiter.When(request), "failures in a row still double")
	assert.Equal(t, 5*time.Millisecond,
		limiter.When(reconcile.Request{NamespacedName: shardKey("ns", "not waiting")}))
}

// A Shard that waits on a quota is reconciled by ordinary events, and each
// of those passes ends on the held create's answer. They must not count as
// failures: one pass that then ends on another error, a status conflict or a
// client-side throttle timeout, is retried in milliseconds, also when it is
// the pass a quota event started, which is the pass that was to send the
// create.
func TestOtherErrorOfAQuotaWaiterIsRetriedAtOnce(t *testing.T) {
	transient := errors.New("the object has been modified")
	for name, quotaEvent := range map[string]bool{"after a quota event": true, "without": false} {
		t.Run(name, func(t *testing.T) {
			p := newQuotaPasses()
			p.answer = errOverQuota
			shard := shardKey("ns", "shard")
			request := reconcile.Request{NamespacedName: shard}
			limiter := quotaRetryLimiter{
				TypedRateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
					5*time.Millisecond,
					1000*time.Second,
				),
				waiting: &p.r.quotaWaiting,
				now:     p.r.now,
			}
			// Twenty event-driven passes one second apart.
			sent := 0
			for range 20 {
				if p.pass(t, shard, true, errFromCreate) {
					sent++
				}
				limiter.When(request)
				p.now = p.now.Add(time.Second)
			}
			require.Equal(t, 6, sent, "creates at 0, 1, 2, 4, 8 and 16 seconds")
			if quotaEvent {
				require.Len(t, p.quotaEvent(t, "ns"), 1)
			}

			p.pass(t, shard, false, transient)
			assert.Equal(t, 5*time.Millisecond, limiter.When(request))
			_, held := p.r.quotaWaiting.held(shard, quotaKindPod, 0, p.now)
			assert.Equal(t, quotaEvent, held == nil,
				"the create may be sent now exactly after a quota event")
		})
	}
}

// The schedule is kept per kind. A quota may count claims and not pods, so
// a claim that was turned down holds back the Shard's claims and none of its
// pods, and the answer that stands in names the kind it was given for. A
// quota event lets both kinds through. A create that fits ends its kind's
// waits, also when the pass then fails on something else: the next create
// of that kind to be turned down starts at the floor again.
func TestQuotaScheduleIsPerKind(t *testing.T) {
	p := newQuotaPasses()
	shard := shardKey("ns", "shard")
	owner := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Namespace: shard.Namespace, Name: shard.Name},
	}
	create := func(obj client.Object) (sent bool, err error) {
		before := p.sent
		err = p.r.createUnderQuota(withReplacementPass(t.Context()), owner, obj)
		return p.sent > before, err
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p"}}
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c"},
	}

	p.answer = errOverQuota
	sent, err := create(claim)
	require.True(t, sent)
	require.ErrorIs(t, err, errOverQuota)
	p.now = p.now.Add(500 * time.Second)
	sent, _ = create(claim)
	require.True(t, sent)

	// The claims wait 500s; a pod is sent at once, and its own answer
	// starts its own waits at the floor.
	sent, err = create(claim)
	require.False(t, sent)
	require.ErrorContains(t, err, "last claim create")
	sent, err = create(pod)
	require.True(t, sent, "a pod was held back for a claim's quota answer")
	require.ErrorIs(t, err, errOverQuota)
	sent, err = create(pod)
	require.False(t, sent)
	require.ErrorContains(t, err, "last pod create")
	// A pass held back at both is retried when the first of them is due.
	ctx := withReplacementPass(t.Context())
	require.Error(t, p.r.createUnderQuota(ctx, owner, claim))
	err = p.r.createUnderQuota(ctx, owner, pod)
	require.Error(t, err)
	p.r.endPass(ctx, shard, err)
	after, _ := p.r.quotaWaiting.retryAfter(shard, p.now)
	require.Equal(t, quotaRetryFloor, after)
	p.now = p.now.Add(quotaRetryFloor)
	sent, _ = create(pod)
	require.True(t, sent)
	sent, _ = create(claim)
	require.False(t, sent, "the claims' wait was shortened by a pod's")

	// A quota event lets both through.
	require.Len(t, p.quotaEvent(t, "ns"), 1)
	sent, _ = create(claim)
	require.True(t, sent)
	sent, _ = create(pod)
	require.True(t, sent)

	// A claim fits after a long stall. The Shard still waits for its pod.
	p.now = p.now.Add(300 * time.Second)
	sent, _ = create(claim)
	require.True(t, sent)
	p.now = p.now.Add(300 * time.Second)
	p.answer = nil
	sent, err = create(claim)
	require.True(t, sent)
	require.NoError(t, err)
	require.True(t, p.r.quotaWaiting.waiting(shard))
	require.Nil(t, p.r.quotaWaiting.get(shard).kinds[quotaKindClaim])
	require.NotNil(t, p.r.quotaWaiting.get(shard).kinds[quotaKindPod])

	// The next claim to be turned down waits the floor, not 300s.
	p.answer = errOverQuota
	sent, _ = create(claim)
	require.True(t, sent)
	p.now = p.now.Add(quotaRetryFloor)
	sent, _ = create(claim)
	require.True(t, sent, "a claim turned down after one fitted kept the earlier long wait")

	// Another failure says nothing about the quota and leaves the waits.
	// So does a claim of that name being there already: it is not a claim
	// the quota let in.
	for _, answer := range []error{
		errors.New("another failure"),
		apierrors.NewAlreadyExists(corev1.Resource("persistentvolumeclaims"), "c"),
	} {
		p.answer = answer
		p.now = p.now.Add(quotaRetryFloor)
		sent, _ = create(claim)
		require.True(t, sent)
		require.NotNil(t, p.r.quotaWaiting.get(shard).kinds[quotaKindClaim], answer)
	}
}

// A quota event lets each waiting Shard of its namespace send its create at
// once, on the pass the event starts. If that create is still turned down,
// the next follows after quotaRetryFloor and the waits double from there:
// not after the wait the stall had built up, and not faster than the floor,
// so a namespace whose usage falls often costs one create per event and
// Shard, and no more than the 1, 2, 4... second retries after the last one.
func TestQuotaEventLetsAWaiterTryAtOnce(t *testing.T) {
	p := newQuotaPasses()
	p.answer = errOverQuota
	shard, elsewhere := shardKey("a", "w"), shardKey("b", "w")
	request := reconcile.Request{NamespacedName: shard}
	limiter := quotaRetryLimiter{
		TypedRateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			5*time.Millisecond, 1000*time.Second),
		waiting: &p.r.quotaWaiting, now: p.r.now,
	}
	for _, key := range []types.NamespacedName{shard, elsewhere} {
		require.True(t, p.pass(t, key, true, errFromCreate))
	}
	// A long stall: the next create is far off for both.
	p.now = p.now.Add(500 * time.Second)
	for _, key := range []types.NamespacedName{shard, elsewhere} {
		require.True(t, p.pass(t, key, true, errFromCreate))
		require.False(t, p.pass(t, key, true, errFromCreate))
	}
	require.Equal(t, 500*time.Second, limiter.When(request))

	assert.Equal(t, []reconcile.Request{request}, p.quotaEvent(t, "a"))
	require.True(t, p.pass(t, shard, true, errFromCreate), "the event's pass must send the create")
	require.False(t, p.pass(t, elsewhere, true, errFromCreate), "another namespace's event")
	assert.Equal(t, quotaRetryFloor, limiter.When(request))
	require.False(t, p.pass(t, shard, true, errFromCreate), "a second create within the floor")
	p.now = p.now.Add(quotaRetryFloor)
	require.True(t, p.pass(t, shard, true, errFromCreate))
	assert.Equal(t, quotaRetryFloor, limiter.When(request))
	p.now = p.now.Add(quotaRetryFloor)
	require.True(t, p.pass(t, shard, true, errFromCreate))
	assert.Equal(t, 2*quotaRetryFloor, limiter.When(request))

	// Events faster than the floor: one create each.
	for range 5 {
		p.now = p.now.Add(100 * time.Millisecond)
		p.quotaEvent(t, "a")
		require.True(t, p.pass(t, shard, true, errFromCreate))
		require.False(t, p.pass(t, shard, true, errFromCreate))
	}
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

// The shard controller's rate limiter is per Shard, 5ms doubling to 1000s,
// and the reconciler holds it. Nothing in it spaces different Shards out:
// a thousand Shards that fail at once are each retried after 5ms.
func TestSetupKeepsAPerShardRateLimiter(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:1"}, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	require.NoError(t, err)
	r := &ShardReconciler{Client: mgr.GetClient(), Scheme: scheme}
	require.NoError(t, r.SetupWithManagerReconciler(mgr, r, controller.Options{
		SkipNameValidation: ptr.To(true), MaxConcurrentReconciles: 20,
	}))
	require.IsType(t, quotaRetryLimiter{}, r.retryLimiter)
	request := func(i int) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: "ns", Name: fmt.Sprintf("shard-%d", i),
		}}
	}
	for i := range 1000 {
		require.Equal(t, 5*time.Millisecond, r.retryLimiter.When(request(i)))
	}
	assert.Equal(t, 10*time.Millisecond, r.retryLimiter.When(request(0)))
	for range 30 {
		r.retryLimiter.When(request(0))
	}
	assert.Equal(t, 1000*time.Second, r.retryLimiter.When(request(0)))

	// A Shard whose pass ended on a quota's answer is retried when its next
	// create is due.
	waiting := request(0).NamespacedName
	r.quotaWaiting.passEnded(waiting,
		r.quotaWaiting.turnedDown(waiting, quotaKindPod, "p", 0, time.Now(), errOverQuota, false))
	assert.InDelta(t, quotaRetryFloor, r.retryLimiter.When(request(0)), float64(time.Second/2))
}

// What status says about a Shard's quota waits is the same text on every
// pass while the waits are the same: claims before pods, resources sorted,
// no usage figures, a start time that a quota event or a later create does
// not move, and the first object turned down, whichever is turned down
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
			"used: pods=4,limits.memory=7Gi,cpu=3, limited: pods=4,limits.memory=8Gi,cpu=4"),
		false)
	w.turnedDown(shard, quotaKindClaim, "data-0", 1, start.Add(time.Minute),
		errNoCoveringQuota, true)
	want := []string{
		"claim data-0 waiting on a ResourceQuota that covers pods since 2026-01-01T00:01:00Z",
		"pod pod-0 waiting on ResourceQuota team (cpu,limits.memory,pods) since 2026-01-01T00:00:00Z",
	}
	assert.Equal(t, want, w.describe(shard, 1, quotaObjectsSeen{}))

	// Later creates with other figures, and a quota event in between.
	w.roomMade("ns")
	w.turnedDown(shard, quotaKindPod, "pod-0", 1, start.Add(time.Hour), forbidden(
		"exceeded quota: team, requested: cpu=1,limits.memory=1Gi,pods=1, "+
			"used: cpu=4,limits.memory=8Gi,pods=9, limited: cpu=4,limits.memory=8Gi,pods=9"),
		false)
	assert.Equal(t, want, w.describe(shard, 1, quotaObjectsSeen{}))

	// Another pod of the Shard is turned down, as when the pools are
	// reconciled in another order: the pod named stays, with its answer.
	other := forbidden("exceeded quota: other, requested: pods=1, used: pods=9, limited: pods=9")
	w.turnedDown(shard, quotaKindPod, "pod-9", 1, start.Add(time.Hour+10*time.Minute), other, false)
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
		true,
	)
	assert.Equal(t, want[:1], w.describe(shard, 2, none))
	assert.Equal(t, want, w.describe(shard, 3, none))
	assert.Empty(t, w.describe(shard, 3, none))
	w.fitted(shard, quotaKindClaim)

	// The pod's create of the new generation is another pod's, with
	// another answer: that is the one named from here on.
	w.turnedDown(shard, quotaKindPod, "pod-9", 3, start.Add(3*time.Hour), errUsageUnknown, false)
	podWait := []string{
		"pod pod-9 waiting on ResourceQuota q (pods), whose usage is not computed yet " +
			"since 2026-01-01T00:00:00Z",
	}
	assert.Equal(t, podWait, w.describe(shard, 3, none))
	assert.Equal(t, podWait, w.describe(shard, 3, none))
	w.turnedDown(shard, quotaKindPod, "pod-9", 3, start.Add(4*time.Hour),
		forbidden("exceeded quota"), false)
	assert.Equal(t, []string{
		"pod pod-9 waiting on a ResourceQuota since 2026-01-01T00:00:00Z",
	}, w.describe(shard, 3, none))

	w.fitted(shard, quotaKindPod)
	assert.Empty(t, w.describe(shard, 3, none))
	w.turnedDown(shard, quotaKindPod, "pod-9", 3, start.Add(5*time.Hour), errOverQuota, false)
	assert.True(t, w.clear(shard))
	assert.False(t, w.clear(shard))
	assert.Empty(t, w.describe(shard, 3, none))
}

// The pod a quota wait names is checked against what the status update
// knows. A pod that exists, or that the spec does not ask for, gives way to
// the pod turned down last, with that pod's answer and the wait's start
// time; a pod that is missing and wanted stays, whichever is turned down
// after it. Its answer is its own for as long as its create is still
// reached, and the last one given to the kind once it has not been for
// longer than the kind's wait between creates.
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
		w.turnedDown(shard, quotaKindPod, "pod-2", 1, start, small, false)
		w.turnedDown(shard, quotaKindPod, "pod-0", 1, start.Add(time.Second), team, false)
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
	w.turnedDown(shard, quotaKindPod, "surge", 1, start, small, false)
	assert.Equal(t, text("surge", "small (pods)"), w.describe(shard, 1, seen(nil, both)))
	assert.Empty(t, w.describe(shard, 1, seen([]string{"surge"}, both)))

	// pod-2 stays missing, and every create sent is pod-0's. While those
	// come within the kind's wait, pod-2's own answer stands; the quota
	// event that follows a replaced quota shortens the wait, and the next
	// answer is reported.
	w = waiting()
	at := start.Add(time.Second)
	for _, step := range []time.Duration{1, 2, 4, 8} {
		at = at.Add(step * time.Second)
		w.turnedDown(shard, quotaKindPod, "pod-0", 1, at, team, false)
		assert.Equal(t, text("pod-2", "small (pods)"), w.describe(shard, 1, seen(nil, both)), at)
	}
	w.roomMade("ns")
	w.turnedDown(shard, quotaKindPod, "pod-0", 1, at.Add(16*time.Second), team, false)
	assert.Equal(t, text("pod-2", "team (requests.memory)"), w.describe(shard, 1, seen(nil, both)))
	// At the cap it follows without an event, once the named pod's own
	// answer is more than two waits old: not while the two are reached in
	// turn, a wait apart.
	w = waiting()
	at = start.Add(2000 * time.Second)
	for range 6 {
		w.turnedDown(shard, quotaKindPod, "pod-2", 1, at, small, false)
		at = at.Add(quotaRetryCap + time.Second)
		w.turnedDown(shard, quotaKindPod, "pod-0", 1, at, team, false)
		at = at.Add(quotaRetryCap + time.Second)
		assert.Equal(t, text("pod-2", "small (pods)"), w.describe(shard, 1, seen(nil, both)))
	}
	// pod-0 a second time in a row.
	w.turnedDown(shard, quotaKindPod, "pod-0", 1, at, team, false)
	assert.Equal(t, text("pod-2", "team (requests.memory)"), w.describe(shard, 1, seen(nil, both)))
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
