package shard

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// The shard controller watches its Pods and PVCs, so a pass that waits on one
// of them (a pod created and not yet Ready, a pod or PVC being deleted, a
// drain in progress) is restarted by the event that ends the wait and does
// not poll. pendingBackstop is the requeue such a pass still asks for, so a
// missed event cannot leave the wait unattended until the next resync. It is
// longer than drain.DrainTimeout, which makes the backstop pass the one that
// times a stuck drain out, and short enough that an operator watching a
// rollout sees it resume within minutes. With upward jitter of
// pendingBackstopJitter it falls in [6m, 9m], so shards that started waiting
// together do not retry together.
const (
	pendingBackstop       = 6 * time.Minute
	pendingBackstopJitter = 0.5
)

// rolloutRequeue returns the requeue the pass's rollout state asks for, or
// zero for none. A refusal that only a pooler RPC can show the end of is
// polled on the readiness backoff, from readinessBackoffMinDelay up to
// readinessBackoffMaxDelay plus its jitter, measured from when that refusal
// was first reported for the shard. Anything else the pass waits on ends in a watch
// event and gets only the backstop.
func (r *ShardReconciler) rolloutRequeue(
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
) time.Duration {
	// A pass that started a disruption, or that waits for nothing, ended the
	// refusal. One that stopped before the checks (a pod not Ready for a
	// moment, a failed pool) says nothing about it.
	ended := rollout.started || !rollout.isPending()
	refused := r.recordDisruptionRefusal(
		shard, rollout.blocked, rollout.waitingForRecovery, ended)
	switch {
	case rollout.waitingForRecovery:
		return readinessBackoffDelay(refused)
	case rollout.isPending():
		return wait.Jitter(pendingBackstop, pendingBackstopJitter)
	}
	return 0
}

// disruptionRefusal is the refusal a shard's disruption walk is being polled
// for and when it was first reported.
type disruptionRefusal struct {
	reason string
	since  time.Time
}

// recordDisruptionRefusal tracks, per shard, how long the walk has been
// refused for the same reason, and returns that duration. A different reason
// starts over, so a rollout that moves from one refusal to the next is not
// polled at the previous one's interval. Elapsed time rather than a count of
// passes: pod events start passes too and must not advance the backoff. Nor
// may they reset it: the record is dropped only when ended says the refusal
// is over, not by every pass that did not reach the checks.
func (r *ShardReconciler) recordDisruptionRefusal(
	shard *multigresv1alpha1.Shard,
	reason string,
	refused, ended bool,
) time.Duration {
	key := fmt.Sprintf("%s/%s", shard.Namespace, shard.Name)
	now := r.now()

	r.disruptionRefusedMu.Lock()
	defer r.disruptionRefusedMu.Unlock()
	if !refused {
		if ended {
			delete(r.disruptionRefused, key)
		}
		return 0
	}
	if r.disruptionRefused == nil {
		r.disruptionRefused = make(map[string]disruptionRefusal)
	}
	first, ok := r.disruptionRefused[key]
	if !ok || first.reason != reason {
		first = disruptionRefusal{reason: reason, since: now}
		r.disruptionRefused[key] = first
	}
	return now.Sub(first.since)
}

// defaultUnregisteredPodGrace is ShardReconciler.UnregisteredPodGrace's
// default: long enough that a pooler which is merely slow to start has
// registered, so it cannot join the cohort between the preflight and the
// drain, and short against a spec that keeps it from ever starting.
const defaultUnregisteredPodGrace = 2 * time.Minute

// listDisruptionPods reads from the API server so a new reconcile cannot miss
// a drain annotation that a previous reconcile just wrote through the cache.
func (r *ShardReconciler) listDisruptionPods(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (*corev1.PodList, error) {
	pods := &corev1.PodList{}
	err := r.disruptionReader().List(ctx, pods, client.InNamespace(shard.Namespace),
		client.MatchingLabels(map[string]string{
			metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
			metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
			metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
			metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
			metadata.LabelAppComponent:        PoolComponentName,
		}))
	return pods, err
}

// disruptionReader is the uncached reader disruption preflights use, so a
// state written moments ago is not missed.
func (r *ShardReconciler) disruptionReader() client.Reader {
	if r.APIReader == nil {
		return r.Client
	}
	return r.APIReader
}

// mayBeUnregistered reports whether pod, read fresh, has been scheduled for
// longer than the grace and is not Ready, so a missing topology registration
// means its pooler cannot start rather than that it has not started yet. The
// grace runs from scheduling: a pod that sat unschedulable has not had the
// chance to start.
func (r *ShardReconciler) mayBeUnregistered(pod *corev1.Pod) bool {
	grace := r.UnregisteredPodGrace
	if grace == 0 {
		grace = defaultUnregisteredPodGrace
	}
	var scheduled time.Time
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionTrue {
			scheduled = condition.LastTransitionTime.Time
		}
	}
	return !isPodReady(pod) && !scheduled.IsZero() && r.now().Sub(scheduled) >= grace
}

// disruptionPreflight is one uncached read of the shard's pool Pods and one
// RPC observation of every pooler they run, shared by every candidate the
// disruption walk weighs so each decision rests on the same evidence.
type disruptionPreflight struct {
	pods        *corev1.PodList
	observation *posture.DisruptionObservation
}

// observeDisruption returns a nil preflight, with no error, while any
// disruption is already in flight or the data plane cannot be observed; both
// mean wait. In the second case it also returns why. pods is the walk's
// fresh listDisruptionPods result.
//
// Every pool Pod is observed, whatever its readiness. Building the peer set
// from Pod readiness would let the very condition a rollout repairs (Pods
// running but not Ready) hide the committed primary and cohort from the
// check, so nothing could ever be replaced. Which unobserved poolers matter
// is decided per target by posture.DisruptionObservation.Check.
func (r *ShardReconciler) observeDisruption(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pods *corev1.PodList,
	observed []string,
) (*disruptionPreflight, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	terminating, err := r.terminatingClaimsInUse(ctx, shard, pods.Items)
	if err != nil {
		return nil, "", err
	}
	if len(terminating) > 0 {
		return nil, "", nil
	}
	if disruptionInFlight(pods) {
		return nil, "", nil
	}
	if r.PoolerClients == nil {
		return nil, "no multipooler RPC client is configured", nil
	}
	rpc, err := r.PoolerClients.ClientFor(ctx, shard)
	// Refusal reasons end up in status, so they are fixed strings; the
	// error text, which varies between attempts, goes to the log.
	if err != nil || rpc == nil {
		log.FromContext(ctx).Info("No multipooler RPC client for the disruption preflight",
			"error", err)
		return nil, "no multipooler RPC client", nil
	}
	store, err := r.topoStore(ctx, shard)
	if err != nil || store == nil {
		log.FromContext(ctx).Info("Cannot open the shard's topology for the disruption preflight",
			"error", err)
		return nil, "cannot open the shard's topology", nil
	}
	defer func() { _ = store.Close() }()
	observation, err := posture.ObserveDisruption(ctx, store, rpc, shard, observed)
	if err != nil {
		if errors.Is(err, posture.ErrTopologyUnavailable) {
			log.FromContext(ctx).
				Info("Cannot read the shard's topology for the disruption preflight",
					"error", err)
			return nil, "cannot read the shard's topology", nil
		}
		return nil, err.Error(), nil
	}
	if unobserved := observation.Unobserved(); len(unobserved) > 0 {
		log.FromContext(ctx).V(1).Info("Poolers did not answer the disruption preflight",
			"unobserved", fmt.Sprint(unobserved))
	}
	return &disruptionPreflight{pods: pods, observation: observation}, "", nil
}

// disruptionCheck decides whether target, a pod of preflight's own read, may
// be removed on preflight's evidence. Scheduling, readiness and age are the
// target's as that read saw them.
func (r *ShardReconciler) disruptionCheck(
	shard *multigresv1alpha1.Shard,
	preflight *disruptionPreflight,
	target *corev1.Pod,
) error {
	disruptionTarget := posture.DisruptionTarget{
		Name:              target.Name,
		MayBeUnregistered: r.mayBeUnregistered(target),
		Unscheduled: target.Spec.NodeName == "" &&
			target.Status.Phase == corev1.PodPending,
	}
	for _, condition := range target.Status.Conditions {
		if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionTrue {
			disruptionTarget.Unscheduled = false
		}
	}
	return preflight.observation.Check(shard, disruptionTarget)
}

func (r *ShardReconciler) recordDisruptionBlocked(
	shard *multigresv1alpha1.Shard,
	reason string,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(shard, "Normal", "DisruptionBlocked",
		"Waiting for shard recovery before removing a pod: %s", reason)
}
