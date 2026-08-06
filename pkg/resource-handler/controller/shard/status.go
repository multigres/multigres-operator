package shard

import (
	"context"
	"fmt"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

// updateStatus updates the Shard status based on observed state. rendered is the
// shard's effective-config render result (hash + any render/read error) computed
// once per reconcile by renderEffectiveConfig: the hash detects whether pods
// carry the current config, and a non-nil rendered.err (e.g. a missing
// PostgresConfigRef ConfigMap) is surfaced in the config status.
func (r *ShardReconciler) updateStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rendered renderedConfig,
) error {
	oldPhase := shard.Status.Phase
	cellsSet := make(map[multigresv1alpha1.CellName]bool)

	// Update pools status
	pools, err := r.updatePoolsStatus(ctx, shard, cellsSet, rendered.hash)
	if err != nil {
		return err
	}

	// Update Multiorch status
	orchDegraded, err := r.updateMultiorchStatus(ctx, shard, cellsSet)
	if err != nil {
		return err
	}

	// Update cells list from all observed cells
	shard.Status.Cells = cellSetToSlice(cellsSet)

	// Update aggregate status fields
	shard.Status.PoolsReady = (pools.totalPods > 0 && pools.totalPods == pools.readyPods)
	shard.Status.ReadyReplicas = pools.readyPods

	// Report config rollout state from the pool scan (content drift or a
	// desired-config pod crash-looping; see poolStatus.configInProgress). A
	// render/read error is reported without settling.
	r.setPostgresConfigStatus(shard, pools.configInProgress, rendered.err)

	// Update Phase — Degraded takes priority over Healthy so crash-looping
	// pods are always surfaced even when the old replica is still serving.
	switch {
	case pools.poolDegraded || orchDegraded:
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		if pools.poolDegraded {
			shard.Status.Message = "One or more pool pods are crash-looping"
		} else {
			shard.Status.Message = "One or more Multiorch pods are crash-looping"
		}
	case shard.Status.PoolsReady && shard.Status.OrchReady:
		shard.Status.Phase = multigresv1alpha1.PhaseHealthy
		shard.Status.Message = "Ready"
	default:
		shard.Status.Phase = multigresv1alpha1.PhaseProgressing
		shard.Status.Message = fmt.Sprintf(
			"PoolsReady: %v, OrchReady: %v",
			shard.Status.PoolsReady,
			shard.Status.OrchReady,
		)
	}

	// Update conditions
	r.setConditions(shard, pools.totalPods, pools.readyPods)

	shard.Status.ObservedGeneration = shard.Generation

	// Filter conditions for the SSA patch. Each condition type must belong to
	// exactly one SSA field manager
	var patchConditions []metav1.Condition
	for i := range shard.Status.Conditions {
		if shard.Status.Conditions[i].Type == conditionStorageClassValid {
			continue
		}
		patchConditions = append(patchConditions, shard.Status.Conditions[i])
	}

	patchObj := &multigresv1alpha1.Shard{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "Shard",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      shard.Name,
			Namespace: shard.Namespace,
		},
		Status: multigresv1alpha1.ShardStatus{
			Phase:              shard.Status.Phase,
			Message:            shard.Status.Message,
			ObservedGeneration: shard.Status.ObservedGeneration,
			PoolsReady:         shard.Status.PoolsReady,
			OrchReady:          shard.Status.OrchReady,
			ReadyReplicas:      shard.Status.ReadyReplicas,
			Cells:              shard.Status.Cells,
			Conditions:         patchConditions,
			LastBackupTime:     shard.Status.LastBackupTime,
			LastBackupType:     shard.Status.LastBackupType,
			PodRoles:           shard.Status.PodRoles,
			PostgresConfig:     shard.Status.PostgresConfig,
		},
	}

	// 2. Apply the Patch
	if oldPhase != shard.Status.Phase {
		r.Recorder.Eventf(
			shard,
			"Normal",
			"PhaseChange",
			"Transitioned from '%s' to '%s'",
			oldPhase,
			shard.Status.Phase,
		)
	}

	// Note: We rely on Server-Side Apply (SSA) to handle idempotency.
	// If the status hasn't changed, the API server will treat this Patch as a no-op,
	// so we don't need a manual DeepEqual check here.
	if err := r.Status().Patch(
		ctx,
		patchObj,
		client.Apply,
		client.FieldOwner("multigres-resource-handler"),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to patch status: %w", err)
	}

	return nil
}

// poolStatus is the aggregate of the pool-pod scan: pod counts, whether any pool
// pod is crash-looping (drives Phase=Degraded), and whether the rendered config
// has not yet settled on every pod (drives PostgresConfigStatus). Returning it as
// a struct keeps the pod-health and config-rollout signals from being smuggled
// through a positional multi-bool return.
type poolStatus struct {
	totalPods, readyPods int32
	poolDegraded         bool

	// configInProgress is true while the rendered config has not yet converged
	// onto every live pod (content drift) or a pod already carrying the desired
	// config is crash-looping. The latter is the narrow, config-attributable
	// slice of pod health we intentionally couple to: a GUC value Postgres
	// rejects at startup would otherwise be reported as applied. An unrelated
	// crash-loop on a pod that does not carry the desired config does not affect
	// config status (it still lacks the hash, so it reads as drift, not apply
	// failure — either way the rollout is correctly reported as unsettled).
	configInProgress bool
}

// updatePoolsStatus aggregates status from all pool pods and tracks cells in the
// cellsSet.
func (r *ShardReconciler) updatePoolsStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellsSet map[multigresv1alpha1.CellName]bool,
	desiredConfigHash string,
) (poolStatus, error) {
	clusterName := shard.Labels[metadata.LabelMultigresCluster]

	var ps poolStatus

	// Track whether every live pod carries the desired config hash, and whether
	// a pod that already carries it is crash-looping (config that Postgres
	// rejects at startup manifests exactly this way).
	liveCount := 0
	allConfigCurrent := true
	configApplyFailing := false

	for poolName, poolSpec := range shard.Spec.Pools {
		var poolDesired, poolReady int32

		for _, cell := range poolSpec.Cells {
			cellName := string(cell)
			cellsSet[cell] = true

			// List pods for this specific pool and cell
			labels := buildPoolLabelsWithCell(shard, string(poolName), cellName)
			selector := metadata.GetSelectorLabels(labels)
			podList := &corev1.PodList{}
			if err := r.List(
				ctx,
				podList,
				client.InNamespace(shard.Namespace),
				client.MatchingLabels(selector),
			); err != nil {
				return poolStatus{}, fmt.Errorf("failed to list pods for status: %w", err)
			}

			var cellReady int32
			for i := range podList.Items {
				pod := &podList.Items[i]

				// Exclude terminating pods from total/ready counts
				if !pod.DeletionTimestamp.IsZero() {
					continue
				}

				// Draining pods are transitioning connections away — not considered ready.
				// They still count toward the desired total (spec-driven), so readyPods < totalPods
				// naturally sets PoolsReady=false → Phase=Progressing while drain is in flight.
				if pod.Annotations[metadata.AnnotationDrainState] != "" {
					continue
				}

				// A live pod is config-current when it carries the desired hash.
				liveCount++
				podHasDesiredConfig := pod.Annotations[metadata.AnnotationPostgresConfigHash] == desiredConfigHash
				if !podHasDesiredConfig {
					allConfigCurrent = false
				}

				// Detect crash-looping pods so the phase can escalate to Degraded.
				// When the crash-looper already carries the desired config, treat
				// the rollout as not-yet-settled: a bad GUC value fails Postgres
				// startup here, and reporting such a config as applied would be a
				// silent false positive.
				if status.IsCrashLooping(pod) {
					ps.poolDegraded = true
					if podHasDesiredConfig {
						configApplyFailing = true
					}
				}

				// Check if pod is ready
				for _, cond := range pod.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						cellReady++
						break
					}
				}
			}

			// Emit a warning explicitly if the cell pool should have replicas but is empty
			replicas := DefaultPoolReplicas
			if poolSpec.ReplicasPerCell != nil {
				replicas = *poolSpec.ReplicasPerCell
			}
			if replicas > 0 && cellReady == 0 {
				r.Recorder.Eventf(
					shard,
					"Warning",
					"PoolEmpty",
					"Pool %s in cell %s has 0 ready replicas",
					poolName,
					cellName,
				)
			}

			poolDesired += replicas
			poolReady += cellReady
		}

		ps.totalPods += poolDesired
		ps.readyPods += poolReady

		monitoring.SetShardPoolReplicas(
			clusterName, shard.Name, string(poolName), "", shard.Namespace,
			poolDesired, poolReady,
		)
	}

	// Content-based drift: at least one live pod carries a config hash other than
	// the desired render. This is the "effective != desired" signal — it covers a
	// spec edit, a PostgresConfigRef ConfigMap edit, and an operator-baseline
	// change on upgrade alike, none of which the shard generation captures. When
	// reload lands, the observed side migrates from "pod recreated with the hash"
	// to "pgctld reports the loaded version"; the comparison stays the same.
	configDrift := liveCount > 0 && !allConfigCurrent
	ps.configInProgress = configDrift || configApplyFailing
	return ps, nil
}

// updateMultiorchStatus checks Multiorch Deployments and sets OrchReady status.
// Returns whether any Multiorch pod is crash-looping (degraded).
// Also tracks cells in the cellsSet.
func (r *ShardReconciler) updateMultiorchStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellsSet map[multigresv1alpha1.CellName]bool,
) (orchDegraded bool, err error) {
	multiorchCells, cellsErr := getMultiorchCells(shard)
	if cellsErr != nil {
		shard.Status.OrchReady = false
		return false, nil
	}

	orchReady := true
	for _, cell := range multiorchCells {
		cellName := string(cell)
		cellsSet[cell] = true

		// Check Multiorch Deployment status (deployments use long names)
		deployName := buildMultiorchNameWithCell(shard, cellName, name.DefaultConstraints)
		deploy := &appsv1.Deployment{}
		if getErr := r.Get(
			ctx,
			client.ObjectKey{Namespace: shard.Namespace, Name: deployName},
			deploy,
		); getErr != nil {
			if errors.IsNotFound(getErr) {
				orchReady = false
				break
			}
			return false, fmt.Errorf("failed to get Multiorch Deployment for status: %w", getErr)
		}

		// Check if deployment is ready
		if deploy.Spec.Replicas == nil ||
			deploy.Status.ObservedGeneration != deploy.Generation ||
			deploy.Status.ReadyReplicas != *deploy.Spec.Replicas {
			orchReady = false
		}

		// Check if any Multiorch pods are crash-looping
		orchSelector := metadata.GetSelectorLabels(
			buildMultiorchLabelsWithCell(shard, cellName),
		)
		podList := &corev1.PodList{}
		if listErr := r.List(ctx, podList,
			client.InNamespace(shard.Namespace),
			client.MatchingLabels(orchSelector),
		); listErr != nil {
			return false, fmt.Errorf("failed to list Multiorch pods for status: %w", listErr)
		}
		if status.AnyCrashLooping(podList.Items) {
			orchDegraded = true
		}
	}

	shard.Status.OrchReady = orchReady
	return orchDegraded, nil
}

// setPostgresConfigStatus records the rollout state of the rendered config on
// the shard status from the content-drift signal. inProgress is true while some
// pod still lacks the desired config; LastAppliedAt is (re)stamped only on the
// transition into "settled", so it stays stable while nothing is changing and
// advances for any change — including a PostgresConfigRef edit or a new operator
// baseline that never bumps the shard generation. A config error is reported
// without settling.
func (r *ShardReconciler) setPostgresConfigStatus(
	shard *multigresv1alpha1.Shard,
	inProgress bool,
	configErr error,
) {
	prev := shard.Status.PostgresConfig
	st := &multigresv1alpha1.PostgresConfigStatus{}
	if prev != nil {
		st.LastAppliedAt = prev.LastAppliedAt
	}

	switch {
	case configErr != nil:
		st.Error = configErr.Error()
	case inProgress:
		st.InProgress = true
	default:
		// Settled: stamp LastAppliedAt only when transitioning from an unsettled
		// state, so a steady-state config does not churn the timestamp.
		wasUnsettled := prev == nil || prev.InProgress || prev.Error != ""
		if wasUnsettled {
			now := metav1.Now()
			st.LastAppliedAt = &now
		}
	}

	shard.Status.PostgresConfig = st
}

// cellSetToSlice converts a cell set (map) to a slice.
func cellSetToSlice(cellsSet map[multigresv1alpha1.CellName]bool) []multigresv1alpha1.CellName {
	cells := make([]multigresv1alpha1.CellName, 0, len(cellsSet))
	for cell := range cellsSet {
		cells = append(cells, cell)
	}
	slices.Sort(cells)
	return cells
}

// setConditions creates status conditions based on observed state.
func (r *ShardReconciler) setConditions(
	shard *multigresv1alpha1.Shard,
	totalPods, readyPods int32,
) {
	// Available condition
	availableCondition := metav1.Condition{
		Type:               "Available",
		ObservedGeneration: shard.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             "NotAllPodsReady",
		Message:            fmt.Sprintf("%d/%d pods ready", readyPods, totalPods),
	}

	if readyPods == totalPods && totalPods > 0 {
		availableCondition.Status = metav1.ConditionTrue
		availableCondition.Reason = "AllPodsReady"
		availableCondition.Message = fmt.Sprintf("All %d pods are ready", readyPods)
	}

	meta.SetStatusCondition(&shard.Status.Conditions, availableCondition)
}
