package shard

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// quotaWaiters is the set of Shards with a create that a ResourceQuota
// turned down, and the schedule on which each tries again.
//
// Such a Shard has nothing of its own to wait on: room appears when the
// quota is raised or when something else in the namespace is removed. So the
// ResourceQuota watch reconciles exactly these Shards when a quota in their
// namespace changes that way, and that pass sends the create at once.
// Without such an event the create is sent again after as long as it has
// been turned down so far, at least quotaRetryFloor and at most
// quotaRetryCap. Until then no create of that kind is sent for the Shard,
// whatever reconciles it: a pass requeued every few seconds for a disruption
// preflight, or started by any pod event, would otherwise send a create that
// fails each time, times the number of Shards. The quota's last answer
// stands in for the creates not sent (see held).
//
// The schedule is kept per kind of object, pods and claims apart. A quota
// may count one and not the other: with a claim turned down by a storage
// quota, a pod the quota does not look at is created when it is needed, not
// when the claim is next due.
//
// A Shard enters the set when a create of its is turned down, whatever the
// pass then returns, and leaves it when a pass ends without an error and
// without a create turned down or held back. A pass that fails before it
// reaches its creates says nothing about the quota and leaves the entry.
//
// The set is kept in memory only. A new process starts with it empty and
// reconciles every Shard once, which puts back each one that is still
// waiting.
type quotaWaiters struct {
	mu          sync.Mutex
	byNamespace map[string]map[string]*quotaWait
}

// quotaKind is a kind of object a ResourceQuota counts.
type quotaKind string

const (
	quotaKindPod   quotaKind = "pod"
	quotaKindClaim quotaKind = "claim"
)

// quotaKindOf is the kind of obj, a pod or a claim.
func quotaKindOf(obj client.Object) quotaKind {
	if _, claim := obj.(*corev1.PersistentVolumeClaim); claim {
		return quotaKindClaim
	}
	return quotaKindPod
}

type quotaWait struct {
	// kinds holds the schedule of each kind with a create turned down.
	kinds map[quotaKind]*quotaKindWait
	// retryAt is when the Shard is retried after a pass that ended on the
	// quota's answer: the earliest due time among the creates that pass
	// was held at or had turned down. It is zero after a pass that ended
	// otherwise, whose retry is the rate limiter's.
	//
	// Only the creates the pass reached count. A kind on record whose
	// create the pass did not reach, because the pass ended at another
	// create before it or the spec no longer asks for it, may be long due;
	// retrying for it would bring the Shard back at once, to end at the
	// same place, every time.
	retryAt time.Time
}

type quotaKindWait struct {
	// needsQuota is whether the create was turned down for want of a quota
	// that covers it (quotaNotCovered).
	needsQuota bool
	// first and last are when creates were turned down since the Shard
	// started waiting or a quota last made room; due is when the next may
	// be sent.
	first, last, due time.Time
	// err is the quota's answer to the last create sent, and generation
	// the Shard's metadata.generation it was built from. A create for
	// another generation may ask for something else (smaller requests, a
	// smaller claim), which the answer says nothing about, so it is due at
	// once: a spec edit is the one way to make room that no quota event
	// reports. It costs one create per kind, Shard and spec edit; the waits
	// go on from where they were when that create is turned down too.
	err        error
	generation int64
	// object, answer and since are what status says about the wait (see
	// describe): the name of the pod or claim whose create was the first of
	// the kind to be turned down for the Shard's generation, a quota's
	// answer for it, and when the kind's first create was turned down.
	//
	// The object is not replaced by each one turned down after it. Which
	// create a pass reaches first is not fixed when several objects of a
	// kind are missing (pools are reconciled in map order), and naming each
	// in turn would rewrite status on every create sent. latest is the one
	// turned down last, with err as its answer: describe names it instead
	// once the status update finds that the object exists or is not one the
	// spec asks for, which happens without a create of the kind succeeding
	// when something else creates the object or a maintenance surge is
	// withdrawn.
	//
	// answer is the answer to the object's own create while that create is
	// still reached, as of answerAt. When it has not been for longer than
	// twice the kind's wait between creates, because the passes end at
	// another object before it, answer follows err, so that a quota that
	// was replaced, or another resource running out, is reported. Twice,
	// because with two objects reached in turn each is answered every other
	// create, and once would swap the answer on each of them where the two
	// are refused for different resources. Passes in map order can still
	// reach one object several times in a row; the answer then changes at
	// most once per two waits.
	object, latest string
	answer         error
	answerAt       time.Time
	since          time.Time
	// describedStale is the generation of the Shard for which describe
	// has reported this record although its answer is for an earlier one.
	describedStale int64
}

const (
	// quotaRetryFloor is the shortest wait between two creates of one kind
	// that a quota turned down for a Shard.
	quotaRetryFloor = time.Second
	// quotaRetryCap is the longest, the same as the longest delay of the
	// controller's rate limiter.
	quotaRetryCap = 1000 * time.Second
)

func (w *quotaWaiters) get(shard types.NamespacedName) *quotaWait {
	return w.byNamespace[shard.Namespace][shard.Name]
}

// held returns the quota's last answer to a create of kind while the
// Shard's next one is not due at now, with when it is due, and nil when a
// create may be sent: the wait is over, or the answer was given for another
// generation of the Shard.
func (w *quotaWaiters) held(
	shard types.NamespacedName,
	kind quotaKind,
	generation int64,
	now time.Time,
) (due time.Time, answer error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	wait := w.get(shard)
	if wait == nil || wait.kinds[kind] == nil || !now.Before(wait.kinds[kind].due) ||
		wait.kinds[kind].generation != generation {
		return time.Time{}, nil
	}
	return wait.kinds[kind].due, fmt.Errorf(
		"not sent again before %s, a quota turned down the Shard's last %s create: %w",
		wait.kinds[kind].due.UTC().Format(time.RFC3339), kind, wait.kinds[kind].err)
}

// turnedDown records that a create of kind sent for the Shard's generation
// at now got err, a quota's answer, and returns when the next one is due.
func (w *quotaWaiters) turnedDown(
	shard types.NamespacedName,
	kind quotaKind,
	object string,
	generation int64,
	now time.Time,
	err error,
	needsQuota bool,
) (due time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.byNamespace == nil {
		w.byNamespace = map[string]map[string]*quotaWait{}
	}
	if w.byNamespace[shard.Namespace] == nil {
		w.byNamespace[shard.Namespace] = map[string]*quotaWait{}
	}
	waiter := w.get(shard)
	if waiter == nil {
		waiter = &quotaWait{kinds: map[quotaKind]*quotaKindWait{}}
		w.byNamespace[shard.Namespace][shard.Name] = waiter
	}
	of := waiter.kinds[kind]
	fresh := of == nil
	if fresh {
		of = &quotaKindWait{since: now}
		waiter.kinds[kind] = of
	}
	if fresh || of.generation != generation {
		of.object = object
	}
	if of.first.IsZero() {
		of.first = now
	}
	wait := min(max(now.Sub(of.first), quotaRetryFloor), quotaRetryCap)
	if of.object == object {
		of.answer, of.answerAt = err, now
	} else if now.Sub(of.answerAt) > 2*wait {
		of.answer = err
	}
	of.latest = object
	of.last, of.err, of.needsQuota, of.generation = now, err, needsQuota, generation
	of.due = now.Add(wait)
	return of.due
}

// fitted records that a create of kind sent for the Shard succeeded.
// Whatever waits the kind had built up are over: a create turned down after
// this starts at quotaRetryFloor.
func (w *quotaWaiters) fitted(shard types.NamespacedName, kind quotaKind) {
	w.mu.Lock()
	defer w.mu.Unlock()
	wait := w.get(shard)
	if wait == nil {
		return
	}
	delete(wait.kinds, kind)
	if len(wait.kinds) == 0 {
		w.remove(shard)
	}
}

// passEnded records how a pass of a waiting Shard ended: retryAt is the
// earliest due time among the creates it was held at or had turned down
// when it ended on the quota's answer, and zero otherwise.
func (w *quotaWaiters) passEnded(shard types.NamespacedName, retryAt time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if wait := w.get(shard); wait != nil {
		wait.retryAt = retryAt
	}
}

// retryAfter returns how long from now a waiting Shard whose pass ended on
// the quota's answer is retried. ok is false for any other Shard.
//
// The wait is to a due time the pass itself met in the future, so it is the
// rest of that wait, and a millisecond when the pass took longer than that.
// The pass that follows cannot end at the same create held back again: that
// create is due, so it is sent, and either fits or sets a due time at least
// quotaRetryFloor away. A pass that ends before it ended at another create,
// which gives its own due time.
func (w *quotaWaiters) retryAfter(
	shard types.NamespacedName,
	now time.Time,
) (after time.Duration, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	wait := w.get(shard)
	if wait == nil || wait.retryAt.IsZero() {
		return 0, false
	}
	return max(wait.retryAt.Sub(now), time.Millisecond), true
}

// clear records that the Shard is not waiting, and reports whether it was.
func (w *quotaWaiters) clear(shard types.NamespacedName) (waited bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	waited = w.get(shard) != nil
	w.remove(shard)
	return waited
}

func (w *quotaWaiters) remove(shard types.NamespacedName) {
	if names := w.byNamespace[shard.Namespace]; names != nil {
		delete(names, shard.Name)
		if len(names) == 0 {
			delete(w.byNamespace, shard.Namespace)
		}
	}
}

// describe returns one sentence per create of the Shard that waits on a
// quota, for status: which object, which quota and resources, and since
// when. The text is the same on every pass while the same create keeps
// getting the same kind of answer: it carries no usage figures, due times or
// counts, the resources are sorted and the kinds are in a fixed order.
//
// An answer given to an earlier generation of the Shard is described once
// for the current one and then left out. It may be for a create the spec no
// longer asks for, which stays on record while the Shard's other kind keeps
// it waiting, so it cannot be reported for good. Leaving it out at once
// would remove the text on every spec edit and put it back a pass later:
// status is written before the pass sends the create that renews the
// answer. "Once" is once per call, and a pass of a pool shard computes
// status twice, so the answer is left out from the second status of that
// pass on if the pass did not get the edited create answered in between.
//
// seen is what the status update that asks knows about the Shard's pods and
// claims. An object that is named although it exists, or although the spec
// does not ask for it, gives way to the one turned down last where that one
// is the better name (see quotaKindWait.object and the cases below); the
// kind's start time stays.
func (w *quotaWaiters) describe(
	shard types.NamespacedName,
	generation int64,
	seen quotaObjectsSeen,
) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	wait := w.get(shard)
	if wait == nil {
		return nil
	}
	var waits []string
	for _, kind := range []quotaKind{quotaKindClaim, quotaKindPod} {
		of := wait.kinds[kind]
		if of == nil {
			continue
		}
		if of.generation != generation {
			if of.describedStale == generation {
				continue
			}
			of.describedStale = generation
		}
		switch other := of.latest != of.object; {
		case seen.exists(kind, of.object):
			// An object that exists is not waiting. The one turned down
			// last is, unless it exists too; with none to name, the kind
			// is left out: its record lasts only while the Shard's other
			// kind keeps the Shard waiting, or until the next create of
			// this kind is turned down.
			if !other || seen.exists(kind, of.latest) {
				continue
			}
			of.object, of.answer, of.answerAt = of.latest, of.err, of.last
		case other && seen.unwanted(kind, of.object) && !seen.unwanted(kind, of.latest):
			// The spec does not ask for the object, and it does for the
			// one turned down last: a maintenance surge that was
			// withdrawn. When it asks for neither, both are surges whose
			// creates are being turned down in turn, and swapping them
			// would rewrite status on every create.
			of.object, of.answer, of.answerAt = of.latest, of.err, of.last
		}
		waits = append(waits, fmt.Sprintf("%s %s waiting on %s since %s",
			kind, of.object, quotaAnswerSubject(of.answer), of.since.UTC().Format(time.RFC3339)))
	}
	return waits
}

// quotaObjectsSeen is what a status update knows, from the lists it reads
// anyway, about the objects a quota wait may name: for each kind the names
// that exist and are not being deleted, and the names the spec asks for. A
// kind that is absent from a map is one the update does not know about.
type quotaObjectsSeen struct {
	existing, wanted map[quotaKind]sets.Set[string]
}

// exists reports whether the object of kind called name is known to exist.
func (s quotaObjectsSeen) exists(kind quotaKind, name string) bool {
	existing, known := s.existing[kind]
	return known && existing.Has(name)
}

// unwanted reports whether the object of kind called name is known not to
// be within a replica count of the spec. A maintenance surge's pod and claim
// are unwanted in that sense while the surge is active, too.
func (s quotaObjectsSeen) unwanted(kind quotaKind, name string) bool {
	wanted, known := s.wanted[kind]
	return known && !wanted.Has(name)
}

// see records the pods a status update listed for one pool and cell, and
// the pods and claims the spec asks for there.
//
// The first call makes the wanted names of both kinds "known", so every name
// that no call added reads as unwanted. The caller must therefore see every
// pool and cell of the shard or none: a status update that skipped one would
// have its pods and claims taken for ones the spec does not ask for.
func (s *quotaObjectsSeen) see(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	replicas int32,
	pods []corev1.Pod,
) {
	if s.existing == nil {
		s.existing = map[quotaKind]sets.Set[string]{quotaKindPod: {}}
		s.wanted = map[quotaKind]sets.Set[string]{
			quotaKindPod:   {},
			quotaKindClaim: sets.New(BuildSharedBackupPVCName(shard)),
		}
	}
	for i := range pods {
		if pods[i].DeletionTimestamp.IsZero() {
			s.existing[quotaKindPod].Insert(pods[i].Name)
		}
	}
	for i := range int(replicas) {
		s.wanted[quotaKindPod].Insert(BuildPoolPodName(shard, poolName, cellName, i))
		s.wanted[quotaKindClaim].Insert(BuildPoolDataPVCName(shard, poolName, cellName, i))
	}
}

// seeClaims records the claims a status update listed.
func (s *quotaObjectsSeen) seeClaims(claims []string) {
	if s.existing == nil {
		s.existing = map[quotaKind]sets.Set[string]{}
	}
	s.existing[quotaKindClaim] = sets.New(claims...)
}

var (
	quotaAnswerNoRoom  = regexp.MustCompile(`exceeded quota: ([^,]+), requested: (\S+), used: `)
	quotaAnswerUnknown = regexp.MustCompile(`status unknown for quota: ([^,]+), resources: (\S+)`)
	quotaAnswerNone    = regexp.MustCompile(`insufficient quota to consume: (\S+)`)
)

// quotaAnswerSubject names what a quota's answer to a create says the create
// waits on: the ResourceQuota and the resources it has no room for, a quota
// whose usage is not computed yet, or the resources no quota covers. The
// usage figures of the answer are left out, since they change from one
// create to the next.
func quotaAnswerSubject(answer error) string {
	message := ""
	if answer != nil {
		message = answer.Error()
	}
	if m := quotaAnswerNoRoom.FindStringSubmatch(message); m != nil {
		return fmt.Sprintf("ResourceQuota %s (%s)", m[1], quotaResourceNames(m[2]))
	}
	if m := quotaAnswerUnknown.FindStringSubmatch(message); m != nil {
		return fmt.Sprintf("ResourceQuota %s (%s), whose usage is not computed yet",
			m[1], quotaResourceNames(m[2]))
	}
	if m := quotaAnswerNone.FindStringSubmatch(message); m != nil {
		return fmt.Sprintf("a ResourceQuota that covers %s", quotaResourceNames(m[1]))
	}
	return "a ResourceQuota"
}

// quotaResourceNames returns the sorted resource names of list, which is a
// comma-separated list of names or of name=quantity pairs.
func quotaResourceNames(list string) string {
	names := strings.Split(list, ",")
	for i, name := range names {
		names[i], _, _ = strings.Cut(name, "=")
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}

// waiting reports whether the Shard is in the set.
func (w *quotaWaiters) waiting(shard types.NamespacedName) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.get(shard) != nil
}

// roomMade returns a request for every waiting Shard in the namespace and
// lets each send its next create of every kind at once. A create that is
// still turned down after that starts the waits over at quotaRetryFloor: the
// event is a reason to expect room, and one failure after it is no reason to
// wait as long as the stall before it had built up, nor to retry any faster
// than the floor.
func (w *quotaWaiters) roomMade(namespace string) []reconcile.Request {
	w.mu.Lock()
	defer w.mu.Unlock()
	names := w.byNamespace[namespace]
	if len(names) == 0 {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(names))
	for name, wait := range names {
		for _, of := range wait.kinds {
			of.first, of.due = time.Time{}, time.Time{}
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
	}
	return requests
}

// needsQuota reports whether a Shard in the namespace waits for a quota
// that covers its create.
func (w *quotaWaiters) needsQuota(namespace string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, wait := range w.byNamespace[namespace] {
		for _, of := range wait.kinds {
			if of.needsQuota {
				return true
			}
		}
	}
	return false
}

// enqueueQuotaWaiters maps a ResourceQuota event to the Shards in its
// namespace that are waiting for room, each of which sends its create on
// the pass this starts.
func (r *ShardReconciler) enqueueQuotaWaiters(
	_ context.Context,
	quota client.Object,
) []reconcile.Request {
	return r.quotaWaiting.roomMade(quota.GetNamespace())
}

// quotaRetryLimiter is the shard controller's rate limiter. A Shard whose
// pass ended on a quota's answer is retried when the first of the creates
// that pass was held at or had turned down is due (see quotaWait.retryAt
// and quotaWaiters.retryAfter); every other failed pass gets the wrapped limiter's
// delay, which is per Shard and doubles from 5ms to 1000s. Nothing in
// either limits how many Shards retry at once.
//
// A pass that ended on the quota's answer got as far as its creates, so
// whatever failed in earlier passes is over, and the wrapped limiter's count
// of failures starts again. Without that, a waiting Shard's passes, which
// every pod event starts and which all end on the quota's answer, would
// count up to the cap, and the first pass to end on another error, the one a
// quota event started included, would be retried 1000 seconds later.
type quotaRetryLimiter struct {
	workqueue.TypedRateLimiter[reconcile.Request]
	waiting *quotaWaiters
	now     func() time.Time
}

func (l quotaRetryLimiter) When(request reconcile.Request) time.Duration {
	if after, waiting := l.waiting.retryAfter(request.NamespacedName, l.now()); waiting {
		l.Forget(request)
		return after
	}
	return l.TypedRateLimiter.When(request)
}

// quotaMakesRoom passes the ResourceQuota events after which a create that
// a quota turned down may succeed: the quota is deleted, a limit is raised
// or removed, usage falls, or usage is computed for a resource for the first
// time. The quota controller rewrites status.used on every pod and claim
// that comes or goes in the namespace, and most of those writes raise usage,
// so everything else is dropped before it reaches the queue. A new quota
// takes room away, except from a Shard whose create was turned down for want
// of a quota that covers it.
func quotaMakesRoom(waiting *quotaWaiters) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return waiting.needsQuota(e.Object.GetNamespace())
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			was, okWas := e.ObjectOld.(*corev1.ResourceQuota)
			is, okIs := e.ObjectNew.(*corev1.ResourceQuota)
			if !okWas || !okIs {
				return false
			}
			// Admission compares against status.hard, which the quota
			// controller copies from spec.hard. Reading both reconciles a
			// waiting Shard when the copy arrives, whichever write this is.
			return limitLoosened(was.Spec.Hard, is.Spec.Hard) ||
				limitLoosened(was.Status.Hard, is.Status.Hard) ||
				usageFell(was.Status.Used, is.Status.Used) ||
				usageComputed(was.Status.Used, is.Status.Used)
		},
	}
}

// limitLoosened reports whether any limit in was is higher or absent in is.
func limitLoosened(was, is corev1.ResourceList) bool {
	for name, limit := range was {
		now, limited := is[name]
		if !limited || now.Cmp(limit) > 0 {
			return true
		}
	}
	return false
}

// usageFell reports whether any usage in was is lower or absent in is.
func usageFell(was, is corev1.ResourceList) bool {
	for name, used := range was {
		now, counted := is[name]
		if !counted || now.Cmp(used) < 0 {
			return true
		}
	}
	return false
}

// usageComputed reports whether is counts a resource that was does not. The
// admission plugin admits nothing a quota covers until the quota controller
// has computed its usage, so this is when a new quota, or a resource newly
// added to one, starts to admit.
func usageComputed(was, is corev1.ResourceList) bool {
	for name := range is {
		if _, counted := was[name]; !counted {
			return true
		}
	}
	return false
}
