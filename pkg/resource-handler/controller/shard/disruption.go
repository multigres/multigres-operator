package shard

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const disruptionRecoveryRequeue = 5 * time.Second

// repeatedRefusalRequeue replaces disruptionRecoveryRequeue once the same
// not-Ready replacement refusal has been reported twice in a row.
const repeatedRefusalRequeue = 30 * time.Second

// defaultUnregisteredPodGrace is ShardReconciler.UnregisteredPodGrace's
// default: long enough that a pooler which is merely slow to start has
// registered, so it cannot join the cohort between the preflight and the
// drain, and short against a spec that keeps it from ever starting.
const defaultUnregisteredPodGrace = 2 * time.Minute

// disruptionBlockedLogEvery bounds how often one pod's blocked disruption is
// logged. The preflight reruns every disruptionRecoveryRequeue, so logging each
// pass would flood the log, yet it must repeat so a long stall stays visible.
const disruptionBlockedLogEvery = 5 * time.Minute

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

// canStartDisruption is a fresh preflight, independent of cached Shard status.
// Existing operations must still be allowed to complete when it returns false.
func (r *ShardReconciler) canStartDisruption(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	target *corev1.Pod,
	tracker *shardRolloutTracker,
) (allowed bool, err error) {
	if tracker.waitingForRecovery {
		return false, nil
	}
	defer func() {
		if !allowed {
			tracker.waitingForRecovery = true
		}
	}()
	preflight, unobservable, err := r.observeDisruption(ctx, shard, nil)
	if err != nil || preflight == nil {
		if unobservable != "" {
			r.recordDisruptionBlocked(shard, "", unobservable)
		}
		return false, err
	}
	if err := r.disruptionCheck(shard, preflight, target); err != nil {
		r.logDisruptionBlocked(ctx, shard, target, err)
	}
	return r.disruptionAllowed(shard, preflight, target), nil
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
// RPC observation of every pooler they run, shared by every candidate a
// reconcile weighs so each decision rests on the same evidence.
type disruptionPreflight struct {
	pods        *corev1.PodList
	observation *posture.DisruptionObservation
}

// observeDisruption returns a nil preflight, with no error, while any
// disruption is already in flight or the data plane cannot be observed; both
// mean wait. In the second case it also returns why. pods is a fresh
// listDisruptionPods result the caller already holds, or nil to list here.
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
) (*disruptionPreflight, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if pods == nil {
		var err error
		if pods, err = r.listDisruptionPods(ctx, shard); err != nil {
			return nil, "", fmt.Errorf("list ongoing shard disruptions: %w", err)
		}
	}
	// Removing a pod that mounts a PVC being deleted moves that PVC one pod
	// closer to finalizing with its data; see terminating_pvc.go.
	terminating, err := mountedTerminatingPVCs(ctx, r.disruptionReader(), shard)
	if err != nil {
		return nil, "", fmt.Errorf("check for mounted PVCs being deleted: %w", err)
	}
	if len(terminating) > 0 {
		return nil, "", nil
	}
	names := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			return nil, "", nil
		}
		names = append(names, pod.Name)
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
	observation, err := posture.ObserveDisruption(ctx, store, rpc, shard, names)
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

// disruptionCheck decides whether target may be removed on preflight's
// evidence. Scheduling, readiness and age come from the preflight's fresh
// read of the target.
func (r *ShardReconciler) disruptionCheck(
	shard *multigresv1alpha1.Shard,
	preflight *disruptionPreflight,
	target *corev1.Pod,
) error {
	disruptionTarget := posture.DisruptionTarget{Name: target.Name}
	for i := range preflight.pods.Items {
		pod := &preflight.pods.Items[i]
		if pod.Name != target.Name || pod.UID != target.UID {
			continue
		}
		disruptionTarget.MayBeUnregistered = r.mayBeUnregistered(pod)
		disruptionTarget.Unscheduled = pod.Spec.NodeName == "" &&
			pod.Status.Phase == corev1.PodPending
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodScheduled &&
				condition.Status == corev1.ConditionTrue {
				disruptionTarget.Unscheduled = false
			}
		}
	}
	return preflight.observation.Check(shard, disruptionTarget)
}

func (r *ShardReconciler) disruptionAllowed(
	shard *multigresv1alpha1.Shard,
	preflight *disruptionPreflight,
	target *corev1.Pod,
) bool {
	if err := r.disruptionCheck(shard, preflight, target); err != nil {
		r.recordDisruptionBlocked(shard, target.Name, err.Error())
		return false
	}
	return true
}

func (r *ShardReconciler) recordDisruptionBlocked(
	shard *multigresv1alpha1.Shard,
	target string,
	reason string,
) {
	if r.Recorder == nil {
		return
	}
	if target == "" {
		r.Recorder.Eventf(shard, "Normal", "DisruptionBlocked",
			"Waiting for shard recovery before any removal: %s", reason)
		return
	}
	r.Recorder.Eventf(shard, "Normal", "DisruptionBlocked",
		"Waiting for shard recovery before removing %s: %s", target, reason)
}

func (r *ShardReconciler) logDisruptionBlocked(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	target *corev1.Pod,
	reason error,
) {
	key := client.ObjectKeyFromObject(target).String()
	now := r.now()
	r.disruptionLogMu.Lock()
	if last, ok := r.disruptionLoggedAt[key]; ok && now.Sub(last) < disruptionBlockedLogEvery {
		r.disruptionLogMu.Unlock()
		return
	}
	if r.disruptionLoggedAt == nil {
		r.disruptionLoggedAt = make(map[string]time.Time)
	}
	r.disruptionLoggedAt[key] = now
	r.disruptionLogMu.Unlock()
	log.FromContext(ctx).Info("Disruption blocked, waiting for shard recovery",
		"shard", client.ObjectKeyFromObject(shard).String(),
		"pod", target.Name,
		"reason", reason.Error())
}

// selectShardScaleDownPod ranks removable pods across pool/cell boundaries.
// Active surges in other cells are retained by their maintenance workflow;
// released local surges are already present in localExtras.
func (r *ShardReconciler) selectShardScaleDownPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	localExtras []*corev1.Pod,
) (*corev1.Pod, error) {
	pods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("list shard scale-down candidates: %w", err)
	}
	candidates := map[string]*corev1.Pod{}
	for _, pod := range localExtras {
		candidates[pod.Name] = pod
	}
	groups := map[string]map[string]*corev1.Pod{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		key := pod.Labels[metadata.LabelMultigresPool] + "/" + pod.Labels[metadata.LabelMultigresCell]
		if groups[key] == nil {
			groups[key] = map[string]*corev1.Pod{}
		}
		groups[key][pod.Name] = pod
	}
	for poolName, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			group := groups[string(poolName)+"/"+string(cell)]
			replicas := poolReplicas(pool)
			for _, pod := range group {
				index, ok := resolvePodIndex(pod.Name)
				if ok && index >= int(replicas) && !isMaintenanceSurge(pod) {
					candidates[pod.Name] = pod
				}
			}
		}
	}
	names := make([]string, 0, len(candidates))
	for name := range candidates {
		names = append(names, name)
	}
	slices.Sort(names)
	ordered := make([]*corev1.Pod, 0, len(names))
	for _, name := range names {
		ordered = append(ordered, candidates[name])
	}
	return r.selectPodToDrain(ctx, ordered, shard), nil
}
