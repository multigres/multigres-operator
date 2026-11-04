package shard

import (
	"context"
	"maps"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	// sidecarDefaultStartupDeadline is the sidecar startup probe's period times
	// its failure threshold (5s x 60), used when a pod's probe cannot be read.
	sidecarDefaultStartupDeadline = 300 * time.Second

	// sidecarImagePullAllowance is added to the startup deadline because a
	// pod's age includes the time the kubelet spends pulling images, which the
	// startup probe does not start counting until after.
	sidecarImagePullAllowance = 120 * time.Second
)

// sidecarStuckPod returns the lowest-named pool pod that is stale and whose
// pgbackrest sidecar has never reported started, or nil.
//
// The rollout loop waits for shard health, and a pod whose sidecar never
// passes its startup probe is unready for good, so a fixed sidecar image
// would never roll it. The pod's Initialized condition is False, which
// means no regular container is running in it now, and postgres runs in a
// native sidecar ordered after pgbackrest, which kubelet starts only once
// pgbackrest has started. So nothing in the pod is serving, and replacing it
// costs no availability. That is why this path skips isShardHealthy, and why
// it stays narrow: a pod that is merely unready (lagging, probe blip, sidecar
// restarting after it had started once) may be serving and must keep the
// gated path.
//
// One case is accepted rather than excluded: after a node reboot that loses
// the runtime's container records, a pod that was the topology primary can
// briefly look like this. Draining it skips the primary-last ordering, but it
// keeps its PVC and nothing in it is running, so the failover multiorch must
// do is the one it already faces.
//
// Nothing here reads shard health, pooler status or the sidecar's own health
// endpoint. Each of those is fed by the stuck pod, so consulting them could
// hold the escape closed for exactly the pod it exists for. The only inputs
// are the pod's own status and one uncached pod list; failing that list
// leaves the gated path in charge for this pass and the next pass retries.
func (r *ShardReconciler) sidecarStuckPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pods map[string]*corev1.Pod,
) *corev1.Pod {
	now := r.now()
	var candidates []*corev1.Pod
	for _, name := range slices.Sorted(maps.Keys(pods)) {
		pod := pods[name]
		if r.sidecarStuck(shard, pod, now) {
			candidates = append(candidates, pod)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	// isAnyPodDraining and the rollout tracker only serialize within one
	// pool/cell and one pass. Read uncached, like canStartDisruption, so a
	// drain annotation written by the previous pass is not missed.
	shardPods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		log.FromContext(ctx).Info(
			"Not replacing pods with a stuck pgbackrest sidecar: cannot list the shard's pods",
			"error", err)
		return nil
	}
	fresh := make(map[types.UID]*corev1.Pod, len(shardPods.Items))
	for i := range shardPods.Items {
		pod := &shardPods.Items[i]
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			return nil
		}
		fresh[pod.UID] = pod
	}
	// The cached status that nominated a candidate can be behind the API
	// server: a sidecar that has just started, with postgres coming up, must
	// not be drained. Judge the uncached copy, matched by UID so a pod
	// recreated under the same name is never annotated by mistake.
	for _, c := range candidates {
		if live := fresh[c.UID]; live != nil && r.sidecarStuck(shard, live, now) {
			return live
		}
	}
	return nil
}

func (r *ShardReconciler) sidecarStuck(
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	now time.Time,
) bool {
	return r.sidecarNeverStarted(pod, now) && r.podIsStale(shard, pod)
}

// podIsStale reports whether pod's spec hash differs from the one the shard
// now wants, using the same comparison as the gated rollout.
func (r *ShardReconciler) podIsStale(shard *multigresv1alpha1.Shard, pod *corev1.Pod) bool {
	poolName := pod.Labels[metadata.LabelMultigresPool]
	cellName := pod.Labels[metadata.LabelMultigresCell]
	poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	if !ok {
		return false
	}
	index, ok := resolvePodIndex(pod.Name)
	if !ok {
		return false
	}
	return podNeedsUpdate(
		pod,
		shard,
		poolName,
		cellName,
		poolSpec,
		index,
		r.Scheme,
		r.podOptions()...)
}

// sidecarNeverStarted reports whether pod's pgbackrest init container has not
// started and the pod is older than the time the probe is allowed to take.
//
// A container's Started flag is false both before its first start and while it
// restarts after a crash, so it cannot tell the two apart. The pod's
// Initialized condition can: kubelet sets it True while every init container
// has started or while any regular container is running or has run, so with it
// True the pod has served and keeps the gated path. It is recomputed on each
// sync rather than latched, which is why it is read as "nothing regular is
// running now" and not "never started".
//
// The previous run's exit code is deliberately not consulted. Kubelet kills a
// live sidecar that fails its startup probe with SIGTERM, which the sidecar
// handles and exits 0 on, so a clean exit does not mean it ever started.
func (r *ShardReconciler) sidecarNeverStarted(pod *corev1.Pod, now time.Time) bool {
	if pod == nil || !pod.DeletionTimestamp.IsZero() ||
		pod.Annotations[metadata.AnnotationDrainState] != "" {
		return false
	}
	var spec *corev1.Container
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == PgBackRestSidecarContainerName {
			spec = &pod.Spec.InitContainers[i]
		}
	}
	if spec == nil {
		return false
	}
	var status *corev1.ContainerStatus
	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].Name == PgBackRestSidecarContainerName {
			status = &pod.Status.InitContainerStatuses[i]
		}
	}
	if status == nil || (status.Started != nil && *status.Started) {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodInitialized && cond.Status == corev1.ConditionTrue {
			return false
		}
	}
	deadline := r.SidecarStartupDeadline
	if deadline <= 0 {
		deadline = sidecarStartupDeadline(spec)
	}
	return now.Sub(pod.CreationTimestamp.Time) >= deadline
}

// sidecarStartupDeadline is how long a pod may take to start its sidecar:
// the container's own startup probe budget (read from the pod, so it follows
// the builder) plus the image pull allowance.
func sidecarStartupDeadline(c *corev1.Container) time.Duration {
	budget := sidecarDefaultStartupDeadline
	if p := c.StartupProbe; p != nil && p.PeriodSeconds > 0 && p.FailureThreshold > 0 {
		budget = time.Duration(p.PeriodSeconds) * time.Duration(p.FailureThreshold) * time.Second
	}
	return budget + sidecarImagePullAllowance
}
