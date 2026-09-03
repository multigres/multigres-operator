package shard

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/multigres/multigres/go/common/topoclient"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

const (
	// quarantineRemediationRequeue is how soon to requeue after taking a
	// quarantine-remediation action, so the recreate converges and the result is
	// observed promptly (the operator does not watch topology).
	quarantineRemediationRequeue = 5 * time.Second

	// quarantineRemediationMinPodAge is how long a pod must have existed before
	// it is eligible for quarantine remediation. A freshly (re)created pod
	// re-registers its topology record within seconds, so requiring a minimum age
	// avoids acting on a stale QUARANTINED record left by the prior process and
	// wiping a pod that is actually mid-bootstrap.
	//
	// There is deliberately no quarantine-specific attempt-cap or terminal
	// "unrecoverable" state: a reconstructed pod is just a new standby, so if it
	// repeatedly fails to come up (e.g. a corrupt backup) that is a general
	// standby bring-up / restore failure — surfaced by backup health and
	// standby-provisioning monitoring — independent of whether the pod was ever
	// quarantined. This age-based pacing only avoids tight re-wipe loops.
	quarantineRemediationMinPodAge = 3 * time.Minute
)

// reconcileQuarantineRemediation replaces poolers that have self-quarantined
// (LIFECYCLE_QUARANTINED: postgres is unrecoverably failing to start, e.g. a
// genuinely diverged standby) by deleting the backing pod AND hard-deleting its
// data PVC. The next reconcile's createMissingResources recreates both; the
// fresh, empty data volume makes pgctld re-bootstrap from backup — the only
// remediation that heals genuine on-disk divergence, since a same-PVC restart
// FATAL-loops identically.
//
// Gating is conservative:
//   - at most one pod per reconcile (destructive action),
//   - never the primary (leave primary loss to failover),
//   - only when the rest of the pool is otherwise healthy (don't pile on during
//     a broader outage), and
//   - only for pods old enough to rule out a stale topology record.
//
// Returns true when it took a (destructive) action so the caller requeues and
// skips other disruptive work this cycle.
func (r *ShardReconciler) reconcileQuarantineRemediation(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
) (bool, error) {
	logger := log.FromContext(ctx)

	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}

	podList := &corev1.PodList{}
	inNamespace := client.InNamespace(shard.Namespace)
	matchingLabels := client.MatchingLabels(lbls)
	if err := r.List(ctx, podList, inNamespace, matchingLabels); err != nil {
		return false, fmt.Errorf("failed to list pods for quarantine remediation: %w", err)
	}

	pods := make(map[string]*corev1.Pod, len(podList.Items))
	podNames := make([]string, 0, len(podList.Items))
	for i := range podList.Items {
		p := &podList.Items[i]
		pods[p.Name] = p
		podNames = append(podNames, p.Name)
	}

	quarantined, err := topo.GetQuarantinedPods(ctx, store, shard, podNames)
	if err != nil {
		// Topology hiccup: skip this cycle and retry on the next reconcile rather
		// than failing the whole data-plane phase.
		logger.Error(err, "Failed to list quarantined poolers; skipping remediation this cycle")
		return false, nil
	}
	if len(quarantined) == 0 {
		return false, nil
	}

	// Safety gate: never wipe a node's data PVC unless a healthy backup exists to
	// restore from — the hard-delete is irreversible, so without a good backup it
	// would destroy the last copy. Uses the backup-health condition computed on
	// the previous reconcile (this phase runs before backuphealth.Evaluate); an
	// absent/false condition conservatively blocks remediation.
	if !status.IsConditionTrue(shard.Status.Conditions, backuphealth.ConditionHealthy) {
		logger.Info(
			"Deferring quarantine remediation: no healthy backup to restore from",
			"quarantinedPods", len(quarantined),
		)
		r.Recorder.Eventf(
			shard, "Warning", "QuarantineRemediationBlocked",
			"Deferring replacement of %d quarantined pod(s): no healthy backup to restore from",
			len(quarantined),
		)
		return false, nil
	}

	quarantinedSet := make(map[string]bool, len(quarantined))
	for _, q := range quarantined {
		quarantinedSet[q.PodName] = true
	}

	// quarantined is sorted by pod name, so remediation is deterministic and
	// one-at-a-time.
	for _, q := range quarantined {
		podName := q.PodName
		pod, ok := pods[podName]
		if !ok || !pod.DeletionTimestamp.IsZero() {
			continue // already gone or being deleted
		}

		// Never wipe the primary; a quarantined node's postgres is down so it
		// should not be primary, but guard defensively.
		if shard.Status.PodRoles[podName] == "PRIMARY" {
			logger.Info(
				"Skipping quarantine remediation for pod currently marked PRIMARY",
				"pod", podName,
			)
			continue
		}

		// Guard against a stale QUARANTINED record on a freshly recreated pod.
		if age := time.Since(pod.CreationTimestamp.Time); age < quarantineRemediationMinPodAge {
			logger.V(1).Info(
				"Deferring quarantine remediation: pod too young to rule out a stale record",
				"pod", podName, "age", age.Round(time.Second),
			)
			continue
		}

		// Don't pile on during a broader outage: require the rest of the pool
		// (excluding other quarantined pods, which are themselves remediated) to
		// be healthy before removing this one.
		if !poolHealthyExcluding(pods, pod, quarantinedSet) {
			logger.Info(
				"Deferring quarantine remediation: pool not otherwise healthy",
				"pod", podName,
			)
			r.Recorder.Eventf(shard, "Warning", "QuarantineRemediationDeferred",
				"Deferring replacement of quarantined pod %s: pool has other non-ready pods",
				podName)
			continue
		}

		if err := r.wipeQuarantinedPod(ctx, shard, pod, q.Reason); err != nil {
			return false, err
		}
		return true, nil // one destructive action per reconcile
	}

	return false, nil
}

// wipeQuarantinedPod deletes a quarantined pod and hard-deletes its data PVC so
// the recreated pod bootstraps from backup on an empty volume. Unlike
// scale-down cleanup (which may orphan a PVC for later reuse), the data here is
// known-bad and must be discarded.
func (r *ShardReconciler) wipeQuarantinedPod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	reason string,
) error {
	logger := log.FromContext(ctx)

	poolName := pod.Labels[metadata.LabelMultigresPool]
	cellName := pod.Labels[metadata.LabelMultigresCell]
	idx, ok := resolvePodIndex(pod.Name)
	if !ok {
		logger.Info(
			"Skipping quarantine remediation for pod with unparseable index",
			"pod", pod.Name,
		)
		return nil
	}

	if reason == "" {
		reason = "unspecified"
	}
	logger.Info(
		"Quarantine remediation: replacing unrecoverable pooler (delete pod + wipe PVC)",
		"pod", pod.Name, "pool", poolName, "cell", cellName, "reason", reason,
	)
	r.Recorder.Eventf(shard, "Warning", "QuarantineRemediation",
		"Replacing quarantined pod %s (reason: %s): wiping data PVC to re-bootstrap from backup",
		pod.Name, reason)

	// Delete the pod first so the data PVC can be released.
	if err := r.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete quarantined pod %s: %w", pod.Name, err)
	}

	pvcName := BuildPoolDataPVCName(shard, poolName, cellName, idx)
	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKey{Namespace: pod.Namespace, Name: pvcName}
	if err := r.Get(ctx, key, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf(
			"failed to fetch data PVC %s for quarantined pod %s: %w",
			pvcName, pod.Name, err,
		)
	}
	if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf(
			"failed to delete data PVC %s for quarantined pod %s: %w",
			pvcName, pod.Name, err,
		)
	}
	logger.Info(
		"Quarantine remediation: deleted pod and data PVC",
		"pod", pod.Name, "pvc", pvcName,
	)
	return nil
}

// poolHealthyExcluding reports whether every other pod in the target's pool+cell
// is Ready, excluding the target itself, other quarantined pods (which are
// separately remediated), and pods already draining or terminating. It is the
// "don't remediate during a broader outage" gate.
func poolHealthyExcluding(
	pods map[string]*corev1.Pod,
	target *corev1.Pod,
	quarantinedSet map[string]bool,
) bool {
	pool := target.Labels[metadata.LabelMultigresPool]
	cell := target.Labels[metadata.LabelMultigresCell]
	for name, pod := range pods {
		if name == target.Name || quarantinedSet[name] {
			continue
		}
		if pod.Labels[metadata.LabelMultigresPool] != pool ||
			pod.Labels[metadata.LabelMultigresCell] != cell {
			continue
		}
		if !pod.DeletionTimestamp.IsZero() ||
			pod.Annotations[metadata.AnnotationDrainState] != "" {
			continue
		}
		if !isPodReady(pod) {
			return false
		}
	}
	return true
}
