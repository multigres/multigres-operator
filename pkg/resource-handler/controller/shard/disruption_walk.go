package shard

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// disruptionKind is why the walk would remove a pod.
type disruptionKind int

const (
	// disruptResize restarts a pod so its data PVC's filesystem can grow.
	disruptResize disruptionKind = iota
	// disruptExtra removes a pod beyond its pool/cell's replica count.
	disruptExtra
	// disruptDrifted replaces a replica on an old spec.
	disruptDrifted
	// disruptPrimary replaces the primary on an old spec, by switchover.
	disruptPrimary
)

// disruptionCandidate is one pod the walk may remove.
type disruptionCandidate struct {
	pod  *corev1.Pod
	kind disruptionKind
	// pvc is the data PVC a disruptResize restart is for.
	pvc string
}

// startDisruption starts at most one disruption of a pool pod in the shard:
// a scale-down, a rolling-update replacement, or a restart for a filesystem
// resize. Pool maintenance only records which pool/cells ask for one; every
// decision is made here, once per pass, for the whole shard.
//
// The rule is to observe once, judge every candidate against that one
// observation, act on the first candidate it allows, and stop, even if the
// write fails. A refusal is usually about the pod being removed, not about
// the shard, so a refused candidate does not stop the ones after it: the pod
// a refusal names may be the only one that can be removed, and the one whose
// replacement repairs the shard.
//
// Nothing starts while a pool pod is draining or terminating, or when the
// shard's spec is no longer the one pool maintenance read. A pass that has
// no pool/cell asking reads nothing. What the pass then waits for, and
// whether that is polled, is rolloutRequeue's.
func (r *ShardReconciler) startDisruption(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
) error {
	if rollout.HasStarted() || len(rollout.cells) == 0 {
		return nil
	}
	if shard.Generation != rollout.generation {
		// A status write earlier in the pass replaced the shard with the
		// server's copy, spec included. The requests were computed from the
		// spec before it, and a replica count from one spec must not pick
		// extras that another spec wants. The spec change is itself a watch
		// event.
		return nil
	}
	pods, err := r.listDisruptionPods(ctx, shard)
	if err != nil {
		return fmt.Errorf("list pool pods for the disruption walk: %w", err)
	}
	// The data plane is observed on every pod that carries the shard's pool
	// labels: a cohort member is one whoever controls its pod. Every gate,
	// count, candidate and target below is over the pods the shard manages.
	observed := listedPodNames(pods.Items)
	pods.Items = r.managedPoolPods(ctx, shard, pods.Items)
	if disruptionInFlight(pods) {
		// Every step of a drain and the pod's deletion are Pod watch events.
		return nil
	}

	cells := slices.SortedFunc(maps.Keys(rollout.cells), func(a, b poolCell) int {
		return cmp.Or(cmp.Compare(a.pool, b.pool), cmp.Compare(a.cell, b.cell))
	})
	// byCell holds the pods the walk may remove. The whole managed list is
	// still what the shard is judged on.
	byCell := map[poolCell][]*corev1.Pod{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if rollout.cells[podPoolCell(pod)] != nil {
			byCell[podPoolCell(pod)] = append(byCell[podPoolCell(pod)], pod)
		}
	}
	for _, cellPods := range byCell {
		slices.SortFunc(cellPods, func(a, b *corev1.Pod) int {
			return strings.Compare(a.Name, b.Name)
		})
	}

	healthy := shardPodsHealthy(shard, pods.Items)
	scaleDown := r.scaleDownCells(shard, rollout, cells, byCell, healthy)

	if slices.ContainsFunc(cells, func(c poolCell) bool { return rollout.cells[c].rolling }) {
		// A pod still configured for a global topology the shard has moved
		// off cannot register where multiorch and the readiness gate now
		// look, so it never turns Ready again. The health, capacity and
		// posture checks below all wait on Ready poolers, so they would hold
		// its replacement forever. Draining it costs nothing: it is already
		// invisible to the rest of the cluster. It does not wait for the
		// previous replacement to turn Ready, because the first replacement
		// alone cannot form a quorum and so cannot turn Ready either.
		var drifted []*corev1.Pod
		for _, cell := range cells {
			if rollout.cells[cell].rolling {
				drifted = append(drifted, byCell[cell]...)
			}
		}
		if pod := r.topoStrandedPod(ctx, shard, drifted); pod != nil {
			return r.drainTopoStranded(ctx, shard, rollout, pods, pod)
		}

		// A drifted Pod that is not Ready may be the one the new spec
		// repairs, so it cannot wait for the shard to be healthy: that wait
		// would last forever. Ready drifted Pods still wait below, and any
		// not-Ready Pod here also keeps the shard unhealthy, so the two
		// paths never both act.
		if handled, err := r.replaceUnreadyDriftedPod(
			ctx,
			shard,
			rollout,
			pods,
			observed,
		); err != nil ||
			handled {
			return err
		}
	}

	// Nothing else is removed while some other pool/cell in the shard has
	// not come back Ready, extras left over from a scale-down included, or
	// while a desired pod does not exist. The health check counts pods, so
	// an extra can stand in for a missing desired pod there; a pool whose
	// maintenance could not create its pod leaves exactly that.
	if !healthy || len(missingDesiredPods(shard, pods.Items)) > 0 {
		return nil
	}
	// Capacity is not recorded as a refusal: a maintenance surge supplies
	// it, and its pod turning Ready triggers the next pass. Candidates
	// without it are dropped before the data plane is observed, so a pass
	// that only waits for capacity sends no RPC.
	var candidates []disruptionCandidate
	for _, candidate := range r.readyCandidates(shard, rollout, cells, byCell, scaleDown, pods) {
		pod := candidate.pod
		if candidate.kind == disruptResize {
			pending, err := r.filesystemResizePending(ctx, pod.Namespace, candidate.pvc)
			if err != nil {
				return err
			}
			if !pending {
				continue
			}
		}
		if maintenanceCapacityAmong(shard, pods.Items,
			pod.Labels[metadata.LabelMultigresCell], pod.Name, isAvailablePooler) {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	preflight, err := r.observeForWalk(ctx, shard, rollout, pods, observed)
	if err != nil || preflight == nil {
		return err
	}
	var refused string
	for _, candidate := range candidates {
		pod := candidate.pod
		if err := r.disruptionCheck(shard, preflight, pod); err != nil {
			if refused == "" {
				refused = fmt.Sprintf("not removing pod %s: %s", pod.Name, err)
			}
			continue
		}
		return r.disrupt(ctx, shard, rollout, candidate)
	}
	if refused != "" {
		// Consensus changes do not generate Kubernetes watch events.
		rollout.waitingForRecovery = true
		rollout.block(refused)
	}
	return nil
}

// isPoolPod reports whether pod is a pool pod the shard manages: the shard
// is its controller, it carries its pool/cell's selector labels, and its name
// is the one those labels and its ordinal give. Pool maintenance creates,
// cleans up and recreates exactly these, by name. Any other pod that carries
// the shard's labels is nobody's to finish: nothing replaces it, nothing
// deletes it once drained, and its labels say nothing reliable about which
// PVC is its own.
func isPoolPod(shard *multigresv1alpha1.Shard, pod *corev1.Pod) bool {
	if !metav1.IsControlledBy(pod, shard) {
		return false
	}
	cell := podPoolCell(pod)
	index, ok := resolvePodIndex(pod.Name)
	if !ok || pod.Name != BuildPoolPodName(shard, cell.pool, cell.cell, index) {
		return false
	}
	selector := metadata.GetSelectorLabels(buildPoolLabelsWithCell(shard, cell.pool, cell.cell))
	return labels.SelectorFromSet(selector).Matches(labels.Set(pod.Labels))
}

// managedPoolPods returns the pods of a list that the shard manages; see
// isPoolPod. Pool maintenance, the maintenance surge, quarantine
// remediation, the drain state machine and the disruption walk all reduce
// what they list to this set before anything else, so a gate, a capacity
// count and a write target never disagree about which pods exist. A pod left
// out is not counted, not waited on and not written to.
func (r *ShardReconciler) managedPoolPods(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pods []corev1.Pod,
) []corev1.Pod {
	managed := make([]corev1.Pod, 0, len(pods))
	for i := range pods {
		if isPoolPod(shard, &pods[i]) {
			managed = append(managed, pods[i])
		}
	}
	return managed
}

// logIgnoredPods logs the pods of one pool/cell's list that the shard does
// not manage, each once for as long as it stays in the list. What was logged
// is kept per pool/cell and replaced on every pass, so it is bounded by the
// pods that exist now, and forgetStrikes drops it with the shard.
func (r *ShardReconciler) logIgnoredPods(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	listed []corev1.Pod,
	managed map[string]bool,
) {
	key := ignoredPodsKey{
		shard: types.NamespacedName{Namespace: shard.Namespace, Name: shard.Name},
		cell:  poolCell{poolName, cellName},
	}
	now := map[types.UID]struct{}{}
	r.ignoredLoggedMu.Lock()
	defer r.ignoredLoggedMu.Unlock()
	for i := range listed {
		if managed[listed[i].Name] {
			continue
		}
		now[listed[i].UID] = struct{}{}
		if _, logged := r.ignoredLogged[key][listed[i].UID]; !logged {
			log.FromContext(ctx).Info(
				"Ignoring a pod that carries the pool's labels but is not one the shard manages",
				"pod", listed[i].Name, "pool", poolName, "cell", cellName)
		}
	}
	if r.ignoredLogged == nil {
		r.ignoredLogged = map[ignoredPodsKey]map[types.UID]struct{}{}
	}
	r.ignoredLogged[key] = now
}

// ignoredPodsKey names one pool/cell of one shard.
type ignoredPodsKey struct {
	shard types.NamespacedName
	cell  poolCell
}

// filesystemResizePending reads the PVC from the API server and reports
// whether its filesystem still waits for a pod restart. Pool maintenance asked
// from its cache, and the resize may have completed since.
func (r *ShardReconciler) filesystemResizePending(
	ctx context.Context,
	namespace, name string,
) (bool, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.disruptionReader().Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pvc)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read PVC %s for filesystem resize: %w", name, err)
	}
	return pvcNeedsFilesystemResize(map[string]*corev1.PersistentVolumeClaim{name: pvc}, name), nil
}

// listedPodNames returns the names of pods.
func listedPodNames(pods []corev1.Pod) []string {
	names := make([]string, len(pods))
	for i := range pods {
		names[i] = pods[i].Name
	}
	return names
}

// disruptionInFlight reports whether any of the shard's pool Pods is draining
// or terminating.
func disruptionInFlight(pods *corev1.PodList) bool {
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations[metadata.AnnotationDrainState] != "" || !pod.DeletionTimestamp.IsZero() {
			return true
		}
	}
	return false
}

// observeForWalk takes the walk's one observation of the data plane. A nil
// preflight with no error means wait. When the data plane could not be
// observed the tracker says why and asks to be polled; otherwise the wait is
// for a pod or PVC to finish being deleted, which a watch event ends.
func (r *ShardReconciler) observeForWalk(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
	pods *corev1.PodList,
	observed []string,
) (*disruptionPreflight, error) {
	preflight, unobservable, err := r.observeDisruption(ctx, shard, pods, observed)
	if err != nil {
		return nil, err
	}
	if preflight == nil && unobservable != "" {
		rollout.waitingForRecovery = true
		rollout.block("cannot observe the data plane: " + unobservable)
	}
	return preflight, nil
}

// scaleDownCells returns the pool/cells whose extras the walk may remove:
// those that asked, while their own desired pods and the shard's are Ready.
// Removing pods from a pool that is already degraded could turn a degraded
// pool into an outage, so the others are deferred and say so.
func (r *ShardReconciler) scaleDownCells(
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
	cells []poolCell,
	byCell map[poolCell][]*corev1.Pod,
	healthy bool,
) map[poolCell]bool {
	allowed := map[poolCell]bool{}
	for _, cell := range cells {
		request := rollout.cells[cell]
		if !request.scaleDown {
			continue
		}
		extras := cellExtras(byCell[cell], request.effectiveReplicas)
		if len(extras) == 0 {
			continue
		}
		local := make(map[string]*corev1.Pod, len(byCell[cell]))
		for _, pod := range byCell[cell] {
			local[pod.Name] = pod
		}
		scope := "shard"
		if !isPoolHealthy(local, request.effectiveReplicas, shard) {
			scope = "current pool"
		} else if healthy {
			allowed[cell] = true
			continue
		}
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ScaleDownBlocked",
			"Deferring scale-down of %d extra pod(s): %s has non-ready pods",
			len(extras),
			scope,
		)
	}
	return allowed
}

// cellExtras returns the pods of one pool/cell beyond effectiveReplicas.
func cellExtras(pods []*corev1.Pod, effectiveReplicas int32) []*corev1.Pod {
	var extras []*corev1.Pod
	for _, pod := range pods {
		if index, ok := resolvePodIndex(pod.Name); !ok || index >= int(effectiveReplicas) {
			extras = append(extras, pod)
		}
	}
	return extras
}

// readyCandidates lists what the walk may remove from a healthy shard, in
// the order it tries them. The order between kinds holds across the whole
// shard and is the order pool maintenance works in: pods waiting on a
// filesystem resize, extras, drifted replicas, then the primary. Within a
// kind pool/cells are taken by pool name, then cell name, and pods by name,
// except extras, which are ranked across the shard (rankPodsToDrain). The
// primary is a candidate only once no other desired pod in the shard is
// drifted, so it is switched over once rather than onto a replica that still
// has to be replaced.
//
// A resize request names the pod maintenance saw by UID: a pod recreated
// under the same name mounted the grown volume when it started.
func (r *ShardReconciler) readyCandidates(
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
	cells []poolCell,
	byCell map[poolCell][]*corev1.Pod,
	scaleDown map[poolCell]bool,
	pods *corev1.PodList,
) []disruptionCandidate {
	var candidates []disruptionCandidate
	var extras, drifted []*corev1.Pod
	var primary *corev1.Pod
	for _, cell := range cells {
		request := rollout.cells[cell]
		byUID := make(map[types.UID]*corev1.Pod, len(byCell[cell]))
		for _, pod := range byCell[cell] {
			byUID[pod.UID] = pod
		}
		for _, resize := range request.resize {
			if pod := byUID[resize.uid]; pod != nil {
				candidates = append(candidates,
					disruptionCandidate{pod: pod, kind: disruptResize, pvc: resize.pvc})
			}
		}
		if scaleDown[cell] {
			extras = append(extras, cellExtras(byCell[cell], request.effectiveReplicas)...)
		}
		if !request.rolling {
			continue
		}
		for _, pod := range byCell[cell] {
			switch {
			case !r.podDrifted(shard, pod):
			case resolvePodRole(shard, pod.Name) == "PRIMARY":
				primary = pod
			default:
				drifted = append(drifted, pod)
			}
		}
	}
	slices.SortFunc(extras, func(a, b *corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	for _, pod := range rankPodsToDrain(extras, shard) {
		candidates = append(candidates, disruptionCandidate{pod: pod, kind: disruptExtra})
	}
	for _, pod := range drifted {
		candidates = append(candidates, disruptionCandidate{pod: pod, kind: disruptDrifted})
	}
	if primary != nil && !driftedReplicaExists(shard, r.classifyDesiredPods(shard, pods.Items)) {
		candidates = append(candidates, disruptionCandidate{pod: primary, kind: disruptPrimary})
	}
	return candidates
}

// podDrifted reports whether pod, in a pool/cell of the shard's spec, is on
// an old spec.
func (r *ShardReconciler) podDrifted(shard *multigresv1alpha1.Shard, pod *corev1.Pod) bool {
	cell := podPoolCell(pod)
	poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(cell.pool)]
	if !ok || !poolUsesCell(poolSpec, cell.cell) {
		return false
	}
	index, _ := resolvePodIndex(pod.Name)
	return podNeedsUpdate(pod, shard, cell.pool, cell.cell, poolSpec, index, r.Scheme)
}

// driftedReplicaExists reports whether a desired pool Pod of the shard other
// than the primary is on an old spec, in any pool/cell, whether or not its
// pool/cell asked the walk for anything this pass.
func driftedReplicaExists(shard *multigresv1alpha1.Shard, desired rolloutCandidates) bool {
	return len(desired.unready)+len(desired.ready) > 0
}

// startJudgedDrain requests the drain of a pod the walk allowed, with the
// write bound to the pod as it was observed. The pass is reserved before
// the write whatever its result: a write that returned an error may still
// have been applied. It reports whether the drain was written; a pod that
// changed since it was observed is left to the next pass.
func (r *ShardReconciler) startJudgedDrain(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
	pod *corev1.Pod,
	reason string,
) (bool, error) {
	if unchanged, err := r.specUnchanged(ctx, shard, rollout); err != nil || !unchanged {
		return false, err
	}
	rollout.SetStarted()
	err := r.initiateJudgedDrain(ctx, pod, reason)
	if apierrors.IsConflict(err) {
		log.FromContext(ctx).Info("Pod changed since it was observed; leaving it to the next pass",
			"pod", pod.Name)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// The spec can change between the read above and the write. The drain
	// is still only requested, so it is withdrawn here rather than left for
	// the drain state machine, which does not cancel every kind of drain.
	if unchanged, err := r.specUnchanged(ctx, shard, rollout); err != nil || unchanged {
		return err == nil, err
	}
	if err := clearDrainAnnotations(ctx, r.Client, pod); err != nil {
		return false, err
	}
	return false, nil
}

// specUnchanged reads the Shard from the API server and reports whether it
// is still on the generation pool maintenance computed its requests from.
// The pod's resourceVersion does not show a change to the Shard, so every
// write a request leads to is preceded by this read. A newer spec is a watch
// event; its pass decides.
func (r *ShardReconciler) specUnchanged(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
) (bool, error) {
	current := &multigresv1alpha1.Shard{}
	if err := r.disruptionReader().
		Get(ctx, client.ObjectKeyFromObject(shard), current); err != nil {
		return false, fmt.Errorf("read the shard before acting on its pool requests: %w", err)
	}
	if current.Generation != rollout.generation {
		log.FromContext(ctx).Info("Shard spec changed since pool maintenance; acting on nothing",
			"generation", current.Generation, "maintained", rollout.generation)
		return false, nil
	}
	return true, nil
}

// terminatingClaimsInUse returns the PVCs being deleted that one of pods
// still mounts, on a fresh read of the shard's PVCs. Removing a pod that
// mounts a PVC being deleted moves that PVC one pod closer to finalizing
// with its data; see terminating_pvc.go.
func (r *ShardReconciler) terminatingClaimsInUse(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pods []corev1.Pod,
) ([]string, error) {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.disruptionReader().List(
		ctx, pvcs, client.InNamespace(shard.Namespace), shardSelector(shard),
	); err != nil {
		return nil, fmt.Errorf("check for mounted PVCs being deleted: %w", err)
	}
	return terminatingClaimsInUse(pods, pvcs.Items, time.Now()), nil
}

// disrupt requests the drain of candidate's pod, the pass's one disruption.
func (r *ShardReconciler) disrupt(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
	candidate disruptionCandidate,
) error {
	logger := log.FromContext(ctx)
	pod := candidate.pod
	reason := ""
	if candidate.kind == disruptResize {
		reason = metadata.FilesystemResizeDrainReason(pod.UID)
	}
	written, err := r.startJudgedDrain(ctx, shard, rollout, pod, reason)
	if err == nil && !written {
		return nil
	}
	switch candidate.kind {
	case disruptResize:
		if err != nil {
			return fmt.Errorf("failed to drain pod %s for filesystem resize: %w", pod.Name, err)
		}
		logger.Info("Draining pod for PVC filesystem expansion", "pod", pod.Name,
			"pvc", candidate.pvc)
		r.Recorder.Eventf(shard, "Normal", "FilesystemResize",
			"Draining pod %s to expand filesystem on PVC %s", pod.Name, candidate.pvc)
	case disruptExtra:
		if err != nil {
			return fmt.Errorf("failed to initiate drain for extra pod %s: %w", pod.Name, err)
		}
		logger.Info("Requested drain for extra pod", "pod", pod.Name)
		r.Recorder.Eventf(shard, "Normal", "DrainStarted",
			"Initiated drain for extra pod %s", pod.Name)
	case disruptDrifted:
		if err != nil {
			return fmt.Errorf("failed to initiate drain for drifted pod %s: %w", pod.Name, err)
		}
		logger.Info("Initiated drain for drifted replica pod during rolling update",
			"pod", pod.Name)
		r.Recorder.Eventf(shard, "Normal", "PodUpdated",
			"Initiated drain for drifted replica pod %s", pod.Name)
	case disruptPrimary:
		if err != nil {
			return fmt.Errorf("failed to request drain for primary pod %s: %w", pod.Name, err)
		}
		logger.Info("Requested switchover for primary pod rolling update", "pod", pod.Name)
		r.Recorder.Eventf(shard, "Normal", "RollingUpdateStarted",
			"Initiating primary switchover for rolling update of pod %s", pod.Name)
	}
	return nil
}

// drainTopoStranded drains pod, stranded on a previous global topology. The
// health, capacity and posture checks are skipped; keeping a PVC that is
// being deleted in place is not. See terminating_pvc.go.
func (r *ShardReconciler) drainTopoStranded(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
	pods *corev1.PodList,
	pod *corev1.Pod,
) error {
	terminating, err := r.terminatingClaimsInUse(ctx, shard, pods.Items)
	if err != nil {
		return err
	}
	if len(terminating) > 0 {
		return nil
	}
	if written, err := r.startJudgedDrain(ctx, shard, rollout, pod, ""); err != nil {
		return fmt.Errorf("failed to initiate drain for topology-stranded pod %s: %w",
			pod.Name, err)
	} else if !written {
		return nil
	}
	log.FromContext(ctx).Info(
		"Initiated drain for pod stranded on a previous global topology", "pod", pod.Name)
	r.Recorder.Eventf(shard, "Normal", "PodUpdated",
		"Initiated drain for pod %s, stranded on a previous global topology", pod.Name)
	return nil
}

// replaceUnreadyDriftedPod starts the replacement of at most one drifted,
// not-Ready Pod anywhere in the shard. It reports handled when such Pods
// exist, whether or not one could be replaced, because the shard cannot be
// healthy while any of them does.
//
// Readiness is not consulted for safety. Every pool Pod is observed by RPC,
// and the target must pass the same posture check every other disruption
// does: a committed primary that is serving, every other committed member
// observed and settled on the committed rule, and a remainder that satisfies
// durability. So a not-Ready Pod that is still a serving member is protected
// exactly as a Ready one would be, and a Pod that is not serving costs the
// shard nothing to restart.
//
// Nothing is replaced while a desired Pod does not exist yet or a Pod already
// on the desired spec is not both Ready and serving. The desired spec has then
// not shown that it works, and replacing more Pods onto it would let a bad
// spec advance every time an old Pod briefly turned not Ready; it also paces
// the roll forward, so each replacement is up before the next starts.
//
// Candidates are tried in order until one passes: not-Ready extras (the
// scale-down already asked for, which scale-down itself defers while desired
// Pods are not Ready), not-Ready drifted replicas, and, when the primary is
// itself drifted and not Ready, the Ready drifted replicas and then the
// primary, so the primary is replaced once, last, rather than failed over
// onto a replica that still has to be replaced. A target restoring a backup
// or creating the first backup is left alone so that work is not thrown away.
func (r *ShardReconciler) replaceUnreadyDriftedPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rollout *shardRolloutTracker,
	pods *corev1.PodList,
	observed []string,
) (bool, error) {
	desired := r.classifyDesiredPods(shard, pods.Items)
	if len(desired.unready) == 0 && desired.primary == nil {
		return false, nil
	}
	extras := unreadyExtras(shard, pods.Items)
	isExtra := make(map[string]bool, len(extras))
	for _, pod := range extras {
		isExtra[pod.Name] = true
	}
	candidates := append(slices.Clone(extras), desired.unready...)
	if desired.primary != nil {
		// A replica that is tried and refused does not hold the primary: a
		// refusal such as a primary left on an older rule permits no removal
		// but the primary's own.
		candidates = append(append(candidates, desired.ready...), desired.primary)
	}
	// The pods above decide whether this path applies and what it waits for;
	// only the removable ones may be acted on.
	candidates = slices.DeleteFunc(candidates, func(pod *corev1.Pod) bool {
		return rollout.cells[podPoolCell(pod)] == nil
	})
	// The two waits below end on Pod writes the controller watches (the Pod
	// being created, the Pod turning Ready), so they are not polled.
	if missing := missingDesiredPods(shard, pods.Items); len(missing) > 0 {
		rollout.block(fmt.Sprintf(
			"desired pod %s does not exist yet; not replacing more pods until it is up",
			missing[0]))
		return true, nil
	}
	for _, pod := range desired.current {
		if !isPodReady(pod) {
			rollout.block(fmt.Sprintf(
				"pod %s on the desired spec is not Ready; not replacing more pods onto it",
				pod.Name))
			return true, nil
		}
	}
	if len(candidates) == 0 {
		// The pods to replace are in pool/cells that asked for nothing this
		// pass, because maintenance acted there; the next pass asks again.
		return true, nil
	}
	preflight, err := r.observeForWalk(ctx, shard, rollout, pods, observed)
	if err != nil || preflight == nil {
		return true, err
	}
	for _, pod := range desired.current {
		if !preflight.observation.Serving(pod.Name) {
			rollout.waitingForRecovery = true
			rollout.block(fmt.Sprintf(
				"pod %s on the desired spec is not serving; not replacing more pods onto it",
				pod.Name))
			return true, nil
		}
	}

	logger := log.FromContext(ctx)
	var firstBlock string
	var surgeFor *corev1.Pod
	for _, pod := range candidates {
		blocked, needsSurge := r.unreadyReplacementBlocked(shard, preflight, pod, isExtra[pod.Name])
		if needsSurge && surgeFor == nil {
			surgeFor = pod
		}
		if blocked != "" {
			logger.V(1).Info("Not replacing drifted pod", "pod", pod.Name, "reason", blocked)
			if firstBlock == "" {
				firstBlock = fmt.Sprintf("not replacing pod %s: %s", pod.Name, blocked)
			}
			continue
		}
		if written, err := r.startJudgedDrain(ctx, shard, rollout, pod, ""); err != nil {
			return true, fmt.Errorf("failed to initiate drain for pod %s: %w", pod.Name, err)
		} else if !written {
			return true, nil
		}
		logger.Info("Initiated drain while drifted pods are not Ready", "pod", pod.Name)
		r.Recorder.Eventf(
			shard,
			"Normal",
			"PodUpdated",
			"Initiated drain for pod %s while drifted pods are not Ready",
			pod.Name,
		)
		return true, nil
	}
	// Only when nothing could be replaced: a surge is temporary capacity the
	// shard pays for until the cell settles.
	if surgeFor != nil {
		// The surge's PVC and pod are built from the spec.
		if unchanged, err := r.specUnchanged(ctx, shard, rollout); err != nil || !unchanged {
			return true, err
		}
		if err := r.requestMaintenanceSurge(ctx, shard, surgeFor, rollout); err != nil {
			return true, err
		}
	}
	rollout.waitingForRecovery = true
	rollout.block(firstBlock)
	return true, nil
}
