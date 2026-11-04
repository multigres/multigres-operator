package shard

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// replacementVerdict is the API server's answer to a dry-run create of the
// pod that would replace an existing one.
type replacementVerdict int

const (
	// replacementAccepted: the API server would take the pod.
	replacementAccepted replacementVerdict = iota
	// replacementRejected: the API server will not take the pod, whether or
	// not the pod it replaces is gone.
	replacementRejected
	// replacementUnanswered: the request was not judged this time, so
	// nothing is known about the pod yet: a server error, a timeout,
	// throttling, a transport error. The create itself would fail the same
	// way, and asking again later can get an answer.
	replacementUnanswered
	// replacementUnaskable: the question cannot be put to this API server.
	// An admission webhook that matches pods declares side effects it
	// cannot suppress, so the API server refuses every dry run of a pod
	// while it accepts the create. Asking again changes nothing.
	replacementUnaskable
)

// replacementProceeds reports whether the pod named pod may be given up for
// its replacement after verdict.
//
// A question that cannot be asked holds nothing back: waiting cannot change
// it, so a pod held for it would never be replaced. The exception is a pod
// whose replacement is recorded as refused, because being unable to ask
// again does not undo a rejection.
//
// An unanswered question holds the pod where waitForAnswer is set: where
// giving the pod up is an outage or loses data. Elsewhere it holds the pod
// only while a refusal is recorded for the shard. One refusal is recorded
// at a time, so the recorded one may be about another pod built from the
// same rejected template.
func replacementProceeds(
	verdict replacementVerdict,
	shard *multigresv1alpha1.Shard,
	pod string,
	waitForAnswer bool,
) bool {
	switch verdict {
	case replacementAccepted:
		return true
	case replacementUnaskable:
		return refusedPod(shard) != pod
	case replacementUnanswered:
		return !waitForAnswer && refusedPod(shard) == ""
	}
	return false
}

// unansweredAsks is the schedule on which a Lite shard whose question got no
// answer asks again. Nothing the shard watches changes when the API server
// recovers, so the question has to be asked again without an event; and a
// Lite shard is reconciled on a fixed short requeue while its pod is not
// primary, and by any event, so each pass sending the dry run would be a
// steady load on an API server that is failing, times the number of shards.
//
// The wait after an unanswered dry run is as long as the question has been
// unanswered so far, at least unansweredAskFloor and at most
// unansweredAskCap, so the asks of one shard fall at about 1, 2, 4, 8...
// times the gap between its first two, whatever brings the passes. Until
// the wait is over the last answer stands in for a new one (see heldAnswer),
// and a shard with no other reason to be reconciled is requeued for the end
// of the wait. The schedule is per shard: across shards, only the
// controller's workers and the client's request rate bound how many ask at
// once. It is held in memory only: a new process asks once and starts over.
//
// The answer that stands in is an answer to one question: whether this pod,
// in this phase, may be given up for that replacement. When the pod is
// another one, has turned terminal, or the spec asks for another
// replacement, the question is a new one and is asked at once.
type unansweredAsks struct {
	mu      sync.Mutex
	byShard map[types.NamespacedName]unansweredAsk
}

type unansweredAsk struct {
	// question identifies what was asked; see replacementQuestion.
	question    string
	first, last time.Time
	err         error
}

// replacementQuestion identifies the question whether old may be given up
// for desired: the pod by UID and phase, the replacement by its spec hash.
func replacementQuestion(old, desired *corev1.Pod) string {
	return fmt.Sprintf("%s/%s/%s", old.UID, old.Status.Phase,
		desired.Annotations[metadata.AnnotationSpecHash])
}

const (
	// unansweredAskFloor is the shortest wait between two dry runs of a
	// Lite shard that got no answer.
	unansweredAskFloor = time.Second
	// unansweredAskCap is the longest, the same as the longest delay of the
	// controller's rate limiter.
	unansweredAskCap = 1000 * time.Second
)

// heldAnswer is the error of the last unanswered dry run, returned for a
// question that is not asked again yet.
type heldAnswer struct{ error }

// wait returns how long from now the shard's question is not asked, with
// the answer that stands until then. It is zero, with no answer, for a
// shard that may ask, which includes one whose question is not the one on
// record.
func (u *unansweredAsks) wait(
	shard types.NamespacedName,
	question string,
	now time.Time,
) (time.Duration, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	ask, waiting := u.byShard[shard]
	if !waiting || ask.question != question {
		return 0, nil
	}
	if left := ask.due().Sub(now); left > 0 {
		return left, heldAnswer{ask.err}
	}
	return 0, nil
}

// due is when the question may be asked again.
func (a unansweredAsk) due() time.Time {
	return a.last.Add(min(max(a.last.Sub(a.first), unansweredAskFloor), unansweredAskCap))
}

// untilDue returns how long from now the shard's question on record may not
// be asked again, at least a millisecond.
func (u *unansweredAsks) untilDue(shard types.NamespacedName, now time.Time) time.Duration {
	u.mu.Lock()
	defer u.mu.Unlock()
	ask, waiting := u.byShard[shard]
	if !waiting {
		return time.Millisecond
	}
	return max(ask.due().Sub(now), time.Millisecond)
}

// note records the outcome of a dry run about question sent at now: err
// when it got no answer, nil when it got one.
func (u *unansweredAsks) note(
	shard types.NamespacedName,
	question string,
	now time.Time,
	err error,
) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err == nil {
		delete(u.byShard, shard)
		return
	}
	ask, waiting := u.byShard[shard]
	if !waiting || ask.question != question {
		ask.first = now
	}
	ask.question, ask.last, ask.err = question, now, err
	if u.byShard == nil {
		u.byShard = map[types.NamespacedName]unansweredAsk{}
	}
	u.byShard[shard] = ask
}

// replacementPass holds what one reconcile pass learned from its dry runs,
// so that a pod is asked about once per pass however many places want to
// know, and what it learned from its creates.
type replacementPass struct {
	verdicts map[string]replacementVerdict
	errors   map[string]error
	// asked is whether the pass had a question about a replacement, sent
	// or held back.
	asked bool
	// quotaWait is whether a create of the pass was turned down by a
	// ResourceQuota, or not sent because the last one was, and quotaReached
	// the objects those creates were for; see underQuota.
	quotaWait    bool
	quotaReached map[quotaObject]bool
}

type replacementPassKey struct{}

// withReplacementPass returns a context under which the dry runs of one
// reconcile pass are remembered.
func withReplacementPass(ctx context.Context) context.Context {
	return context.WithValue(ctx, replacementPassKey{}, &replacementPass{
		verdicts: map[string]replacementVerdict{},
		errors:   map[string]error{},
	})
}

// askAPIServer creates desired, the pod that takes the place of old, as a
// dry run and classifies the answer. A Shard's pod template is checked by
// nothing until a pod is created from it: the CRD stores resource names,
// quantities and claims that pod validation rejects, and a LimitRange, Pod
// Security or an admission policy can reject a pod the CRD has no opinion
// on. Deleting or draining a pod toward such a template leaves the shard one
// pod short until someone edits the spec, and a Lite shard has only the one
// pod.
//
// The probe is the pod the create would send, under the same name, so that
// admission judges exactly that object. The API server validates and admits
// a pod before it looks the name up, so a rejected pod is rejected and an
// acceptable one comes back as AlreadyExists while the pod it replaces is
// still there.
//
// A Lite shard whose last dry run got no answer is not asked about again
// before its wait is over (see unansweredAsks); the unanswered answer stands
// until then, which keeps the pod.
func (r *ShardReconciler) askAPIServer(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	old, desired *corev1.Pod,
) (replacementVerdict, error) {
	pass, _ := ctx.Value(replacementPassKey{}).(*replacementPass)
	if pass != nil {
		pass.asked = true
		if verdict, asked := pass.verdicts[old.Name]; asked {
			return verdict, pass.errors[old.Name]
		}
	}
	lite, key := shard.Spec.Mode == multigresv1alpha1.ModeLite, client.ObjectKeyFromObject(shard)
	var verdict replacementVerdict
	var err error
	question := replacementQuestion(old, desired)
	if _, held := r.liteUnanswered.wait(key, question, r.now()); lite && held != nil {
		verdict, err = replacementUnanswered, held
	} else {
		probe := desired.DeepCopy()
		probe.ResourceVersion = ""
		err = r.Create(ctx, probe, client.DryRunAll)
		verdict = classifyDryRun(err, probe)
		if lite && verdict == replacementAccepted && quotaExceeded(err) && !podIsTerminal(old) {
			// A pool shard deletes one pod of several and is one short
			// until the quota has room. A Lite shard has only this pod, and
			// its replacement is created after it is deleted: an answer
			// that the namespace has no room for the probe does not say
			// whether it has room once the pod is gone, and a create
			// turned down then leaves the shard with no pod at all. So the
			// pod is kept, as for a rejection, until a dry run fits.
			//
			// A pod that has failed or completed serves nothing, so keeping
			// it protects nothing, and under a count/pods quota it holds
			// the object its replacement needs.
			verdict = replacementRejected
		}
		if lite && verdict == replacementAccepted && !podIsTerminal(old) {
			// The API server takes a pod whose volumes name objects that do
			// not exist: the kubelet finds out. The Lite pod is the shard's
			// only one, so it is not given up for a successor that cannot
			// mount its CA bundle or whose sidecar would not start with it.
			switch problem, readErr := r.s3CAProblem(ctx, shard); {
			case readErr != nil:
				verdict, err = replacementUnanswered, readErr
			case problem != nil:
				verdict, err = replacementRejected, problem
			}
		}
		if lite {
			var unanswered error
			if verdict == replacementUnanswered {
				unanswered = err
			}
			r.liteUnanswered.note(key, question, r.now(), unanswered)
		}
	}
	if pass != nil {
		pass.verdicts[old.Name], pass.errors[old.Name] = verdict, err
	}
	return verdict, err
}

// unusableCA is why the pod built from a shard's spec could not run with the
// CA bundle the spec's S3 location names.
type unusableCA struct{ problem string }

func (e unusableCA) Error() string { return e.problem }

// s3CAProblem reads the CA bundle the shard's S3 location references and
// returns what is wrong with it for a pod that would mount it: the Secret or
// ConfigMap does not exist, it has no such key, or the key holds no PEM
// certificate. It returns nil for a location without a CA reference. err is
// a failure to read, which says nothing about the bundle.
//
// A ConfigMap is read from the cache. A Secret a user wrote has no label of
// the operator's and is not in the cache, so it is read from the API server.
// The bundle's content is used for this check only.
func (r *ShardReconciler) s3CAProblem(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (problem error, err error) {
	s3 := sidecarS3(sidecarLocation(shard))
	if s3 == nil || s3.CA == nil || s3.SkipsTLSVerification() {
		return nil, nil
	}
	var kind string
	var ref *multigresv1alpha1.S3CAKeyReference
	var bundle []byte
	var found bool
	switch {
	case s3.CA.Secret != nil:
		kind, ref = "Secret", s3.CA.Secret
		reader := r.APIReader
		if reader == nil {
			reader = r.Client
		}
		secret := &corev1.Secret{}
		err = reader.Get(ctx, client.ObjectKey{Namespace: shard.Namespace, Name: ref.Name}, secret)
		if err == nil {
			bundle, found = secret.Data[ref.KeyOrDefault()]
		}
	case s3.CA.ConfigMap != nil:
		kind, ref = "ConfigMap", s3.CA.ConfigMap
		configMap := &corev1.ConfigMap{}
		err = r.Get(ctx, client.ObjectKey{Namespace: shard.Namespace, Name: ref.Name}, configMap)
		if err == nil {
			var text string
			if text, found = configMap.Data[ref.KeyOrDefault()]; found {
				bundle = []byte(text)
			} else {
				bundle, found = configMap.BinaryData[ref.KeyOrDefault()]
			}
		}
	default:
		return nil, nil
	}
	const from = "s3.ca of the shard's backup location: "
	switch {
	case apierrors.IsNotFound(err):
		return unusableCA{fmt.Sprintf(from+"%s %q does not exist", kind, ref.Name)}, nil
	case err != nil:
		return nil, fmt.Errorf("read %s %s for the S3 CA bundle: %w", kind, ref.Name, err)
	case !found:
		return unusableCA{fmt.Sprintf(
			from+"%s %q has no key %q", kind, ref.Name, ref.KeyOrDefault())}, nil
	}
	for rest := bundle; ; {
		var block *pem.Block
		if block, rest = pem.Decode(rest); block == nil {
			return unusableCA{fmt.Sprintf(
				from+"key %q of %s %q holds no PEM certificate, and the pgBackRest "+
					"sidecar does not start without one",
				ref.KeyOrDefault(), kind, ref.Name)}, nil
		}
		if block.Type == "CERTIFICATE" {
			return nil, nil
		}
	}
}

// podIsTerminal reports whether every container of the pod has stopped for
// good.
func podIsTerminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}

// classifyDryRun reads err, the answer to a dry-run create of probe.
//
// Every client error (4xx) is a rejection, with these exceptions:
//
//   - 409 AlreadyExists for this very pod is the answer for a pod that passed
//     validation and admission: only its name is taken, by the pod it
//     replaces. Any other 409 is something refusing the pod.
//   - 403 from a ResourceQuota the namespace has no room in is acceptance
//     too (see quotaExceeded; askAPIServer makes it a rejection for a Lite
//     shard's running pod), and 403 from a ResourceQuota whose usage is
//     not computed yet was not a judgement (see quotaUsageUnknown). 403 for
//     a resource no quota of the namespace covers stays a rejection: the
//     probe is not short of room that the pod it replaces would give back,
//     so giving that pod up leaves a replacement that cannot be created
//     until a covering quota exists or the spec changes.
//   - 400 from an admission webhook that does not support dry runs means the
//     question cannot be asked.
//   - 401, 404, 408 and 429 were not judgements of the pod.
//
// A 5xx, a timeout and a transport error were not judgements either.
func classifyDryRun(err error, probe *corev1.Pod) replacementVerdict {
	if err == nil {
		return replacementAccepted
	}
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return replacementUnanswered
	}
	code := status.Status().Code
	message := status.Status().Message
	switch {
	case code == http.StatusConflict:
		if nameTakenInStorage(status.Status(), probe) {
			return replacementAccepted
		}
		return replacementRejected
	case code < 400 || code >= 500:
		return replacementUnanswered
	case code == http.StatusUnauthorized, code == http.StatusNotFound,
		code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
		return replacementUnanswered
	case code == http.StatusBadRequest && strings.Contains(message, "does not support dry run"):
		return replacementUnaskable
	case code == http.StatusForbidden && quotaUsageUnknown.MatchString(message):
		return replacementUnanswered
	case code == http.StatusForbidden && strings.Contains(message, quotaNotCovered):
		return replacementRejected
	case quotaExceeded(err):
		return replacementAccepted
	}
	return replacementRejected
}

// nameTakenInStorage reports whether status is the storage layer saying that
// a pod of probe's name exists. An admission webhook may deny a create with
// 409 as well, and that is a rejection of the pod, not a taken name.
func nameTakenInStorage(status metav1.Status, probe *corev1.Pod) bool {
	details := status.Details
	return status.Reason == metav1.StatusReasonAlreadyExists && details != nil &&
		details.Group == "" && details.Kind == "pods" && details.Name == probe.Name
}

// quotaExceeded reports whether err is the ResourceQuota admission plugin
// turning a pod down for lack of room: the pod does not fit in a quota, or
// the cluster requires a quota for a resource the pod uses and the namespace
// has none that covers it. The API gives a quota rejection no structured
// form: it is a 403 with reason Forbidden and no causes, like a LimitRange
// or Pod Security rejection, and only the message tells them apart.
//
// A pod over quota is not a pod the API server will not take. Quota runs
// last in admission, so the pod has passed validation and every other
// admission check, and what is left is the namespace's capacity. The probe
// is charged while the pod it replaces still counts, so a namespace sized
// for exactly its pods exceeds its quota with every probe, and holding the
// pod for that would stop every rollout there. Where the replacement really
// does not fit once the old pod is gone, its create fails and is tried again
// by the passes that follow, with the rate limiter's delay between them:
// how much a namespace may hold is its administrator's decision, and making
// room is theirs to do. While a desired pod of the shard does not
// exist, no other pod is drained for a rollout (see isPoolHealthy), so the
// shard is short of that one pod and no more; pods that serve nothing are
// still replaced: terminal ones, ones stranded on an earlier topology, ones
// whose sidecar never started. A quota that turns the pod down
// for what it is (a request or limit the quota requires and the pod does not
// state) has another message and is a rejection.
func quotaExceeded(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) || status.Status().Code != http.StatusForbidden {
		return false
	}
	return quotaNoRoom.MatchString(status.Status().Message)
}

var quotaNoRoom = regexp.MustCompile(
	`exceeded quota: .*, requested: .*, used: .*, limited: |` + quotaNotCovered,
)

// quotaNotCovered is the plugin's answer where the cluster requires a quota
// for a resource and the namespace has none that covers it.
const quotaNotCovered = `insufficient quota to consume: `

// createUnderQuota creates obj, a pod or claim of the shard, and records a
// quota's answer to it (see underQuota).
func (r *ShardReconciler) createUnderQuota(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	obj client.Object,
) error {
	return r.underQuota(ctx, shard, quotaKindOf(obj), obj.GetName(),
		func() error { return r.Create(ctx, obj) })
}

// underQuota runs create, which creates the object of kind called name that a
// ResourceQuota may count, unless a quota turned the object's last create
// down and its wait is not over (see quotaWaiters.heldAt): the quota's last
// answer is returned then, so the pass does what it does when the create is
// turned down. An answer only a quota changing can end (no room, no covering
// quota, usage not computed yet) is recorded for status (see quotaWaiters)
// and on the pass: the pass may go on to end with another error, or with
// none where the failure is carried to its end behind a requeue, so the
// record reads what the creates saw and not what the pass returned.
func (r *ShardReconciler) underQuota(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	kind quotaKind,
	name string,
	create func() error,
) error {
	key := client.ObjectKeyFromObject(shard)
	pass, _ := ctx.Value(replacementPassKey{}).(*replacementPass)
	if held := r.quotaWaiting.heldAt(
		key, quotaObject{kind, name}, shard.Generation, r.now(),
	); held != nil {
		pass.waitsOnQuota(kind, name)
		return held
	}
	err := create()
	if quotaWaitAnswer(err) {
		pass.waitsOnQuota(kind, name)
		r.quotaWaiting.turnedDown(key, kind, name, shard.Generation, r.now(), err)
	} else if err == nil {
		// Only a create that succeeded: AlreadyExists says an object of
		// that name is there, not that the quota has room for another.
		r.quotaWaiting.fitted(key, kind, name)
	}
	return err
}

// waitsOnQuota records on the pass that the create of the object of kind
// called name was turned down by a quota, or not sent because its last one
// was.
func (p *replacementPass) waitsOnQuota(kind quotaKind, name string) {
	if p == nil {
		return
	}
	p.quotaWait = true
	if p.quotaReached == nil {
		p.quotaReached = map[quotaObject]bool{}
	}
	p.quotaReached[quotaObject{kind, name}] = true
}

// retryCreatesRequested passes the update of a pod whose
// metadata.AnnotationRetryCreates was set or changed.
var retryCreatesRequested = predicate.Funcs{
	CreateFunc:  func(event.CreateEvent) bool { return false },
	DeleteFunc:  func(event.DeleteEvent) bool { return false },
	GenericFunc: func(event.GenericEvent) bool { return false },
	UpdateFunc: func(e event.UpdateEvent) bool {
		is := e.ObjectNew.GetAnnotations()[metadata.AnnotationRetryCreates]
		return is != "" && is != e.ObjectOld.GetAnnotations()[metadata.AnnotationRetryCreates]
	},
}

// retryCreatesNow maps a pod on which a retry was asked for (see
// retryCreatesRequested) to the Shard that owns it, and ends that Shard's
// waits between creates, so that the pass the request starts sends them.
func (r *ShardReconciler) retryCreatesNow(
	_ context.Context,
	obj client.Object,
) []reconcile.Request {
	owner := metav1.GetControllerOf(obj)
	if owner == nil || owner.Kind != "Shard" {
		return nil
	}
	shard := types.NamespacedName{Namespace: obj.GetNamespace(), Name: owner.Name}
	r.quotaWaiting.retryNow(shard)
	return []reconcile.Request{{NamespacedName: shard}}
}

// quotaWaitAnswer reports whether err is an answer of the ResourceQuota
// admission plugin that only a quota changing can end.
func quotaWaitAnswer(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) || status.Status().Code != http.StatusForbidden {
		return false
	}
	message := status.Status().Message
	return quotaNoRoom.MatchString(message) || quotaUsageUnknown.MatchString(message)
}

// quotaUsageUnknown matches the plugin's answer for a ResourceQuota whose
// usage the quota controller has not computed yet, which is the state of a
// quota just created or just given a new resource. The plugin admits nothing
// the quota covers until then, so the pod was not judged, and the answer
// changes without anyone acting.
var quotaUsageUnknown = regexp.MustCompile(`status unknown for quota: `)

// judgeReplacement asks the API server about desired, the pod that takes the
// place of old, and keeps ConditionPodTemplateAccepted in step with the
// answer.
//
// A rejection is recorded with one event, and written to status here because
// a refused pass makes no later status write. One refusal is recorded at a
// time, and a recorded one is not rewritten while it stands: a second pod
// refused in the same pass, or an admission message that differs between two
// identical requests, must not rewrite status, because every status write
// starts another pass of this controller. The recorded refusal is looked at
// again whenever another pod is asked about, so that it gives way to that
// pod's refusal once its own pod is gone or acceptable.
//
// An answer that was not a judgement changes no status: in particular it
// does not remove a recorded refusal. A question the API server did not
// answer is reported with an event, since a pod may be kept waiting for the
// answer and no condition says so: a condition would be rewritten with every
// slow request, and each status write starts another pass.
func (r *ShardReconciler) judgeReplacement(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	old, desired *corev1.Pod,
) (replacementVerdict, error) {
	verdict, answer := r.askAPIServer(ctx, shard, old, desired)
	switch verdict {
	case replacementUnanswered:
		// An answer that stands in for a dry run not sent again yet was
		// reported when it was given.
		if _, held := answer.(heldAnswer); held {
			return verdict, nil
		}
		log.FromContext(ctx).Info(
			"The API server did not judge the replacement pod",
			"pod", old.Name, "error", answer.Error())
		r.Recorder.Event(shard, "Warning", "PodReplacementUnanswered",
			podTemplateSubject(old.Name)+"the API server did not say whether it "+
				"accepts the pod that would replace it: "+answer.Error())
		return verdict, nil
	case replacementUnaskable:
		log.FromContext(ctx).Info(
			"The API server cannot be asked about the replacement pod",
			"pod", old.Name, "error", answer.Error())
		return verdict, nil
	}

	// A refusal of an earlier generation was about another spec.
	before := len(shard.Status.Conditions)
	clearStalePodTemplateRefusal(shard)
	changed := len(shard.Status.Conditions) != before
	if recorded := refusedPod(shard); recorded != "" && recorded != old.Name {
		changed = r.reexamineRecordedRefusal(ctx, shard, shard) || changed
	}
	recorded := refusedPod(shard)
	phase := shard.Status.Phase
	switch {
	case verdict == replacementAccepted && recorded == old.Name:
		meta.RemoveStatusCondition(
			&shard.Status.Conditions, multigresv1alpha1.ConditionPodTemplateAccepted,
		)
		changed = true
	case verdict == replacementRejected && recorded == "":
		why, reason := "the API server rejects the pod that would replace it",
			multigresv1alpha1.ReasonPodRejectedByAPIServer
		if errors.As(answer, &unusableCA{}) {
			why, reason = "the pod that would replace it could not start",
				multigresv1alpha1.ReasonPodCABundleUnusable
		}
		message := podTemplateSubject(old.Name) + why +
			", so it is left running on its current spec: " + answer.Error()
		r.Recorder.Event(shard, "Warning", "PodTemplateRejected", message)
		meta.SetStatusCondition(&shard.Status.Conditions, metav1.Condition{
			Type:               multigresv1alpha1.ConditionPodTemplateAccepted,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: shard.Generation,
		})
		// The phase goes out with the condition, or the next pass would
		// write status a second time to report it.
		applyRefusalPhase(shard)
		changed = true
	}
	if changed {
		if err := r.patchStatus(ctx, shard, phase); err != nil {
			return verdict, err
		}
	}
	return verdict, nil
}

// refusalRecheckInterval is how long a Shard with a refusal on record goes
// before it is reconciled again for nothing but the refusal. The pass asks
// the API server about the refused pod once more. A LimitRange reconciles
// the Shard at once (see enqueueRefusedShards); the interval is for what has
// no watch: a ResourceQuota, or an admission webhook or policy, that would
// accept the pod now. Each requeue is up to a fifth longer, so that
// Shards refused together do not ask together.
const refusalRecheckInterval = 5 * time.Minute

// afterRefusal returns result with the recheck requeue added when shard has
// a refusal on record for its current generation and the pass asked to run
// no sooner. A Shard without one is not requeued.
func afterRefusal(result ctrl.Result, shard *multigresv1alpha1.Shard) ctrl.Result {
	if refusedPod(shard) == "" {
		return result
	}
	// The sooner of the two, so that a requeue the pass asked for is never
	// moved later.
	recheck := wait.Jitter(refusalRecheckInterval, 0.2)
	if result.RequeueAfter == 0 || result.RequeueAfter > recheck {
		result.RequeueAfter = recheck
	}
	return result
}

// enqueueRefusedShards maps an event in a namespace to the Shards there that
// have a refusal on record. It serves the watches on objects that change what
// the API server says about a pod and that no Shard owns: LimitRanges. The
// Shards are read from the cache and sorted by name.
func (r *ShardReconciler) enqueueRefusedShards(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	shards := &multigresv1alpha1.ShardList{}
	if err := r.List(ctx, shards, client.InNamespace(obj.GetNamespace())); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list Shards for an admission change",
			"namespace", obj.GetNamespace())
		return nil
	}
	var requests []reconcile.Request
	for i := range shards.Items {
		if refusedPod(&shards.Items[i]) != "" {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&shards.Items[i]),
			})
		}
	}
	slices.SortFunc(requests, func(a, b reconcile.Request) int {
		return strings.Compare(a.Name, b.Name)
	})
	return requests
}

func podTemplateSubject(pod string) string {
	return fmt.Sprintf("pod %s: ", pod)
}

// refusedPod is the name of the pod whose replacement is recorded as refused
// for the shard's current generation, or "".
func refusedPod(shard *multigresv1alpha1.Shard) string {
	cond := meta.FindStatusCondition(
		shard.Status.Conditions, multigresv1alpha1.ConditionPodTemplateAccepted,
	)
	if cond == nil || cond.Status != metav1.ConditionFalse ||
		cond.ObservedGeneration != shard.Generation {
		return ""
	}
	name, _, ok := strings.Cut(strings.TrimPrefix(cond.Message, "pod "), ": ")
	if !ok || !strings.HasPrefix(cond.Message, "pod ") {
		return ""
	}
	return name
}

// reexamineRecordedRefusal removes the recorded refusal, in memory, when the
// pod it names is gone or its replacement is acceptable now. A refusal
// can have a cause outside the spec, such as a LimitRange, and its pod can
// stop needing a replacement without a spec edit; without this the shard
// would report that pod and that cause for as long as the spec is not
// edited, and say nothing about another pod that is refused. It reports
// whether it removed the refusal.
//
// The replacement is built from desiredFrom: the shard as the pod builders
// see it when they build the pod to create, config hashes included (see
// withConfigHashes), so that the pod asked about here is that pod.
func (r *ShardReconciler) reexamineRecordedRefusal(
	ctx context.Context,
	shard, desiredFrom *multigresv1alpha1.Shard,
) bool {
	name := refusedPod(shard)
	if name == "" {
		return false
	}
	pod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{Namespace: shard.Namespace, Name: name}, pod)
	stale := apierrors.IsNotFound(err)
	if err == nil && pod.Labels[metadata.LabelMultigresPool] != "" {
		if desired, known, buildErr := r.replacementFor(desiredFrom, pod); buildErr == nil {
			if !known {
				stale = true
			} else if verdict, _ := r.askAPIServer(ctx, shard, pod, desired); verdict == replacementAccepted {
				stale = true
			}
		}
	}
	if stale {
		meta.RemoveStatusCondition(
			&shard.Status.Conditions, multigresv1alpha1.ConditionPodTemplateAccepted,
		)
	}
	return stale
}

// replacementFor builds the pod that would be created in place of pod, which
// for a Lite shard is its one pod. known is as for poolReplacement.
func (r *ShardReconciler) replacementFor(
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
) (desired *corev1.Pod, known bool, err error) {
	if shard.Spec.Mode != multigresv1alpha1.ModeLite {
		return r.poolReplacement(shard, pod)
	}
	poolName, cellName, poolSpec, err := litePool(shard)
	if err != nil {
		return nil, false, err
	}
	desired, err = r.buildLitePod(shard, poolName, cellName, poolSpec)
	if err != nil || desired.Name != pod.Name {
		return nil, false, err
	}
	return desired, true, nil
}

// poolReplacement builds the pod that would be created in place of an
// existing pool pod. known is false for a pod with no place in the spec (its
// pool is gone): it gets no replacement.
func (r *ShardReconciler) poolReplacement(
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
) (desired *corev1.Pod, known bool, err error) {
	poolName := pod.Labels[metadata.LabelMultigresPool]
	cellName := pod.Labels[metadata.LabelMultigresCell]
	poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	if !ok {
		return nil, false, nil
	}
	index, ok := resolvePodIndex(pod.Name)
	if !ok {
		return nil, false, nil
	}
	desired, err = BuildPoolPod(
		shard, poolName, cellName, poolSpec, index, r.Scheme, r.podOptions()...,
	)
	return desired, true, err
}

// poolReplacementVerdict is judgeReplacement for the pod that would be
// created in place of an existing pool pod. A pod with no place in the spec
// gets no replacement, so there is nothing to refuse. A replacement that
// cannot even be built is rejected.
func (r *ShardReconciler) poolReplacementVerdict(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
) (replacementVerdict, error) {
	desired, known, err := r.poolReplacement(shard, pod)
	if err != nil {
		log.FromContext(ctx).Info("Not replacing pod: its replacement cannot be built",
			"pod", pod.Name, "error", err.Error())
		return replacementRejected, nil
	}
	if !known {
		return replacementAccepted, nil
	}
	return r.judgeReplacement(ctx, shard, pod, desired)
}

// poolReplacementAccepted reports whether a pool pod may be disrupted so
// that another is created in its place. A rejected replacement holds the pod.
// An answer that was not a judgement does not, as long as no refusal is on
// record: a pool has other pods, and an API server that is slow for a while
// would otherwise stop every rollout (see replacementProceeds).
func (r *ShardReconciler) poolReplacementAccepted(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
) (bool, error) {
	verdict, err := r.poolReplacementVerdict(ctx, shard, pod)
	if err != nil {
		return false, err
	}
	return replacementProceeds(verdict, shard, pod.Name, false), nil
}

// drainForReplacement starts the drain of a pool pod that is drained so that
// another pod can be created in its place: a spec change, a filesystem
// resize, a pod that cannot become Ready. It starts nothing, and reports
// false, when the API server would not accept that other pod. A pod drained
// to be removed for good (scale-down, an external delete, shard deletion)
// goes through initiateDrain directly.
func (r *ShardReconciler) drainForReplacement(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
) (bool, error) {
	if ok, err := r.poolReplacementAccepted(ctx, shard, pod); err != nil || !ok {
		return false, err
	}
	return true, r.initiateDrain(ctx, pod)
}

// drainUnreadyExtra starts the drain of a not-Ready pod beyond the pool's
// replica count. It is on its way out and gets no successor, so there is no
// replacement for the API server to judge, and a rejected pod template must
// not keep it.
func (r *ShardReconciler) drainUnreadyExtra(ctx context.Context, pod *corev1.Pod) error {
	return r.initiateDrain(ctx, pod)
}

// withConfigHashes returns the shard as the pod builders see it once
// reconcilePostgresConfig has stamped the hashes of the config rendered for
// this pass, which happens after the status pass. A pod built from the shard
// before that carries other annotations and another spec hash than the pod
// the pass goes on to create. ok is false when the config did not render:
// the pass creates no pod then.
func withConfigHashes(
	shard *multigresv1alpha1.Shard,
	rendered renderedConfig,
) (stamped *multigresv1alpha1.Shard, ok bool) {
	if rendered.err != nil {
		return nil, false
	}
	stamped = shard.DeepCopy()
	stampConfigHashes(stamped, rendered)
	return stamped, true
}

// clearStalePodTemplateRefusal drops a refusal recorded for an earlier
// generation of the spec. Nothing checks the template again until a pod is
// about to be replaced, and after an edit that makes the pods current no pod
// is, so the refusal would otherwise outlive the spec it was about.
func clearStalePodTemplateRefusal(shard *multigresv1alpha1.Shard) {
	cond := meta.FindStatusCondition(
		shard.Status.Conditions, multigresv1alpha1.ConditionPodTemplateAccepted,
	)
	if cond != nil && cond.ObservedGeneration != shard.Generation {
		meta.RemoveStatusCondition(
			&shard.Status.Conditions, multigresv1alpha1.ConditionPodTemplateAccepted,
		)
	}
}

// applyRefusalPhase reports the shard Degraded while the controller refuses
// to act on its spec. A refused shard's pods keep running on the last spec
// that was applied, so the phase computed from them would say Healthy while
// nothing the spec asks for is happening.
//
// A spec with no sidecar image is left out: every shard is in that state for
// a moment during an operator upgrade, until the cluster controller writes
// the image.
func applyRefusalPhase(shard *multigresv1alpha1.Shard) {
	for _, t := range []string{
		multigresv1alpha1.ConditionBackupRepositoryValid,
		multigresv1alpha1.ConditionPodTemplateAccepted,
	} {
		cond := meta.FindStatusCondition(shard.Status.Conditions, t)
		if cond == nil || cond.Status != metav1.ConditionFalse ||
			cond.Reason == multigresv1alpha1.ReasonBackupRepositorySidecarNotConfigured {
			continue
		}
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		shard.Status.Message = cond.Message
		return
	}
}
