package shard

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// quotaWaiters records, per Shard, the creates that the ResourceQuota
// admission plugin turned down, so that status can say what the Shard waits
// on (see describe). Everything in it comes from the API server's answer to
// a create: no ResourceQuota object is read or watched.
//
// The record also spaces out the creates of an object a quota turned down
// (see held): a Shard is reconciled for many reasons while it waits, a
// requeue every few seconds for a disruption preflight and every pod event
// among them, and each pass would otherwise send a create that fails. When
// the Shard is reconciled is not changed by it: a pass that ends on the
// quota's answer is retried by the controller's rate limiter.
//
// A Shard enters the set when a create of its is turned down, whatever the
// pass then returns, and leaves it when a pass ends without an error and
// without a create turned down. A pass that fails before it reaches its
// creates says nothing about the quota and leaves the entry.
//
// The set is kept in memory only. A new process starts with it empty and
// reconciles every Shard once, which puts back each one that is still
// waiting.
type quotaWaiters struct {
	mu      sync.Mutex
	byShard map[types.NamespacedName]map[quotaKind]*quotaKindWait
	holds   map[types.NamespacedName]map[quotaObject]*quotaHold
}

// quotaObject names a pod or claim of a Shard.
type quotaObject struct {
	kind quotaKind
	name string
}

// quotaHold is the wait before the next create of one object that a quota
// turned down: err is the quota's answer, generation the Shard's
// metadata.generation the create was built from, delay the wait that
// followed it and due when it ends.
type quotaHold struct {
	err        error
	generation int64
	delay      time.Duration
	due        time.Time
}

const (
	// quotaHoldFloor is the wait after the first create of an object that a
	// quota turned down. It doubles with every create turned down after it.
	quotaHoldFloor = 5 * time.Second
	// quotaHoldCap is the longest wait between two creates of an object.
	quotaHoldCap = 5 * time.Minute
)

// heldAt reports that a create of the object is not to be sent at now: the
// error is the quota's answer to the last one sent, wrapped, so that the
// pass ends or goes on exactly as it does for a create that is turned down.
// It is nil when the create may be sent: nothing is on record for the
// object, its wait is over, or the answer was given to another generation
// of the Shard, whose create may have asked for more.
func (w *quotaWaiters) heldAt(
	shard types.NamespacedName,
	object quotaObject,
	generation int64,
	now time.Time,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	hold := w.holds[shard][object]
	if hold == nil || hold.generation != generation || !now.Before(hold.due) {
		return nil
	}
	return fmt.Errorf("not sent again before %s, since the last create of %s %s got: %w",
		hold.due.UTC().Format(time.RFC3339), object.kind, object.name, hold.err)
}

// retryNow ends the waits of every object of the Shard, so that the next
// pass sends each create it reaches. What status says is not changed.
func (w *quotaWaiters) retryNow(shard types.NamespacedName) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.holds, shard)
}

// prune drops the waits of the Shard's objects that are over at now and
// that the pass did not reach, as told by reached: an object the spec no
// longer asks for, or that something else created. A wait that is still
// going on is kept, since a pass can end before it reaches the object.
func (w *quotaWaiters) prune(
	shard types.NamespacedName,
	reached map[quotaObject]bool,
	now time.Time,
) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for object, hold := range w.holds[shard] {
		if !reached[object] && !now.Before(hold.due) {
			delete(w.holds[shard], object)
		}
	}
	if len(w.holds[shard]) == 0 {
		delete(w.holds, shard)
	}
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

type quotaKindWait struct {
	// err is the quota's answer to the last create turned down, latest the
	// object that create was for, and generation the Shard's
	// metadata.generation it was built from.
	err        error
	latest     string
	generation int64
	// object, answer and since are what status says about the wait (see
	// describe): the name of the pod or claim whose create was the first of
	// the kind to be turned down for the Shard's generation, the quota's
	// last answer for that object, and when the kind's first create was
	// turned down.
	//
	// The object is not replaced by each one turned down after it. Which
	// create a pass reaches first is not fixed when several objects of a
	// kind are missing (pools are reconciled in map order), and naming each
	// in turn would rewrite status on every pass. describe names the one
	// turned down last instead once the status update finds that the object
	// exists or is not one the spec asks for, which happens without a
	// create of the kind succeeding when something else creates the object
	// or a maintenance surge is withdrawn.
	object string
	answer error
	since  time.Time
	// describedStale is the generation of the Shard for which describe
	// has reported this record although its answer is for an earlier one.
	describedStale int64
}

// turnedDown records that a create of kind for the Shard's generation got
// err, a quota's answer, at now.
func (w *quotaWaiters) turnedDown(
	shard types.NamespacedName,
	kind quotaKind,
	object string,
	generation int64,
	now time.Time,
	err error,
) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.byShard == nil {
		w.byShard = map[types.NamespacedName]map[quotaKind]*quotaKindWait{}
	}
	if w.byShard[shard] == nil {
		w.byShard[shard] = map[quotaKind]*quotaKindWait{}
	}
	of := w.byShard[shard][kind]
	fresh := of == nil
	if fresh {
		of = &quotaKindWait{since: now}
		w.byShard[shard][kind] = of
	}
	if fresh || of.generation != generation {
		of.object = object
	}
	if of.object == object {
		of.answer = err
	}
	of.latest, of.err, of.generation = object, err, generation

	if w.holds == nil {
		w.holds = map[types.NamespacedName]map[quotaObject]*quotaHold{}
	}
	if w.holds[shard] == nil {
		w.holds[shard] = map[quotaObject]*quotaHold{}
	}
	hold := w.holds[shard][quotaObject{kind, object}]
	if hold == nil {
		hold = &quotaHold{}
		w.holds[shard][quotaObject{kind, object}] = hold
	}
	hold.delay = min(max(2*hold.delay, quotaHoldFloor), quotaHoldCap)
	hold.err, hold.generation, hold.due = err, generation, now.Add(hold.delay)
}

// fitted records that the create of the object of kind sent for the Shard
// succeeded, which ends the object's wait and what status says the kind was
// waiting for: a create turned down after this starts both anew.
func (w *quotaWaiters) fitted(shard types.NamespacedName, kind quotaKind, object string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.holds[shard], quotaObject{kind, object})
	if len(w.holds[shard]) == 0 {
		delete(w.holds, shard)
	}
	delete(w.byShard[shard], kind)
	if len(w.byShard[shard]) == 0 {
		delete(w.byShard, shard)
	}
}

// clear records that the Shard is not waiting, and reports whether it was.
func (w *quotaWaiters) clear(shard types.NamespacedName) (waited bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, waited = w.byShard[shard]
	delete(w.byShard, shard)
	delete(w.holds, shard)
	return waited
}

// waiting reports whether the Shard is in the set.
func (w *quotaWaiters) waiting(shard types.NamespacedName) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, waiting := w.byShard[shard]
	return waiting
}

// describe returns one sentence per create of the Shard that waits on a
// quota, for status: which object, which quota and resources, and since
// when. The text is the same on every pass while the same create keeps
// getting the same kind of answer: it carries no usage figures, due times or
// counts, the resources are sorted and the kinds are in a fixed order.
//
// An answer given to an earlier generation of the Shard is described once
// for the current one and then left out. It may be for a create the spec no
// longer asks for, which stays on record while another create of the Shard
// keeps being turned down, so it cannot be reported for good. Leaving it out at once
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
	var waits []string
	for _, kind := range []quotaKind{quotaKindClaim, quotaKindPod} {
		of := w.byShard[shard][kind]
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
			// is left out: its record lasts until a pass ends without a
			// create turned down, or until the next create of this kind
			// is turned down.
			if !other || seen.exists(kind, of.latest) {
				continue
			}
			of.object, of.answer = of.latest, of.err
		case other && seen.unwanted(kind, of.object) && !seen.unwanted(kind, of.latest):
			// The spec does not ask for the object, and it does for the
			// one turned down last: a maintenance surge that was
			// withdrawn. When it asks for neither, both are surges whose
			// creates are being turned down in turn, and swapping them
			// would rewrite status on every create.
			of.object, of.answer = of.latest, of.err
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
