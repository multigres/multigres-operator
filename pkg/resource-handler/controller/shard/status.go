package shard

import (
	"context"
	"fmt"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/ownership"
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
	if shard.Spec.Mode == multigresv1alpha1.ModeLite {
		return r.updateLiteStatus(ctx, shard, rendered)
	}

	oldPhase := shard.Status.Phase
	cellsSet := make(map[multigresv1alpha1.CellName]bool)

	// Update pools status
	pools, err := r.updatePoolsStatus(
		ctx,
		shard,
		cellsSet,
		rendered.restartHash,
		rendered.reloadHash,
	)
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

	terminatingPVCs, err := mountedTerminatingPVCs(ctx, r.Client, shard)
	if err != nil {
		return err
	}
	r.setPVCTerminatingCondition(shard, terminatingPVCs)

	// Update Phase — Degraded takes priority over Healthy so crash-looping
	// pods are always surfaced even when the old replica is still serving.
	postureCondition := meta.FindStatusCondition(
		shard.Status.Conditions,
		posture.ConditionConsistent,
	)
	switch {
	case status.IsConditionFalse(shard.Status.Conditions, posture.ConditionConsistent):
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		shard.Status.Message = "Postgres posture inconsistent with topology roles (possible split brain)"
	case len(terminatingPVCs) > 0:
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		shard.Status.Message = mountedPVCBeingDeletedMessage(terminatingPVCs)
	case pools.poolDegraded || orchDegraded:
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		if pools.poolDegraded {
			shard.Status.Message = "One or more pool pods are crash-looping"
		} else {
			shard.Status.Message = "One or more Multiorch pods are crash-looping"
		}
	case postureCondition != nil && postureCondition.Status == metav1.ConditionUnknown:
		shard.Status.Phase = multigresv1alpha1.PhaseProgressing
		shard.Status.Message = "Postgres posture check incomplete"
	case shard.Status.PoolsReady && shard.Status.OrchReady && !hasPrimary(shard.Status.PodRoles):
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		shard.Status.Message = "No primary pod for shard"
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

	monitoring.SetShardHasPrimary(
		shard.Labels[metadata.LabelMultigresCluster],
		shard.Name,
		shard.Namespace,
		hasPrimary(shard.Status.PodRoles),
	)

	// Update conditions
	r.setConditions(shard, pools.totalPods, pools.readyPods)
	claims, err := r.setStorageSpecIgnoredCondition(ctx, shard)
	if err != nil {
		return err
	}
	if pools.seen.wanted != nil {
		pools.seen.seeClaims(claims)
	}
	r.nameQuotaWaits(shard, pools.seen)
	r.setBackupRepositoryCondition(shard)
	clearStalePodTemplateRefusal(shard)
	// A pass that replaces no pod asks the API server nothing, so the
	// refusal on record is looked at here: its pod may be gone, or
	// acceptable now for a reason outside the spec.
	if refusedPod(shard) != "" {
		if desiredFrom, ok := withConfigHashes(shard, rendered); ok {
			r.reexamineRecordedRefusal(ctx, shard, desiredFrom)
		}
	}
	applyRefusalPhase(shard)

	shard.Status.ObservedGeneration = shard.Generation

	return r.patchStatus(ctx, shard, oldPhase)
}

// patchStatus writes the status computed in memory to the Shard with a
// server-side apply patch and records a phase transition from oldPhase.
func (r *ShardReconciler) patchStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	oldPhase multigresv1alpha1.Phase,
) error {
	if err := r.adoptBackupStatus(ctx, shard); err != nil {
		return err
	}
	// Filter conditions for the SSA patch. Each condition type must belong to
	// exactly one SSA field manager. The backup verdict and the last backup's
	// time and type belong to applyBackupStatus and are never carried here,
	// whatever manager the cached Shard shows holding them: this apply runs
	// from the cached Shard, which can predate the backup managers' latest
	// write, and carrying its copy would put an older verdict back. On a Shard
	// an earlier operator wrote, adoptBackupStatus has handed them to the
	// backup managers first, so leaving them out here removes nothing.
	var patchConditions []metav1.Condition
	for i := range shard.Status.Conditions {
		switch shard.Status.Conditions[i].Type {
		case conditionStorageClassValid, backuphealth.ConditionHealthy:
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
			PodRoles:           shard.Status.PodRoles,
			PodPostures:        shard.Status.PodPostures,
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

	// An unchanged status is not sent; see ownership.SkipNoopApplies.
	if err := ownership.ApplyStatus(ctx, r.Client, patchObj, ownership.ManagerShard); err != nil {
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
	// seen is what the lists read for this status say about the pods and
	// claims a quota wait may name.
	seen quotaObjectsSeen

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
// cellsSet. desiredConfigHash is the restart-hash (applied via pod recreation)
// and desiredReloadHash is the reload-hash (applied in place via SIGHUP); a pod
// is config-current only when it carries both, so status stays "in progress"
// through a reload as well as a rolling restart.
func (r *ShardReconciler) updatePoolsStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellsSet map[multigresv1alpha1.CellName]bool,
	desiredConfigHash string,
	desiredReloadHash string,
) (poolStatus, error) {
	clusterName := shard.Labels[metadata.LabelMultigresCluster]

	var ps poolStatus
	// Only a Shard with a quota wait has a name in status to check.
	waitsOnQuota := r.quotaWaiting.waiting(client.ObjectKeyFromObject(shard))

	// Track whether every live pod carries the desired config hashes, and whether
	// a pod that already carries them is crash-looping (config that Postgres
	// rejects at startup manifests exactly this way).
	liveCount := 0
	allConfigCurrent := true
	configApplyFailing := false

	var stuckSamples []monitoring.PoolPodsStuck
	var nodes []corev1.Node
	nodesListed := false
	nodesListFailed := false

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

			replicas := DefaultPoolReplicas
			if poolSpec.ReplicasPerCell != nil {
				replicas = *poolSpec.ReplicasPerCell
			}

			// cellReady counts the ready pods the spec asks for, the ones
			// below the replica count. A ready pod beyond it, left by a
			// lowered count or created as a maintenance surge, is on its
			// way out and must not stand in for a desired pod that is
			// missing: the shard would report every pod ready while one
			// waits to be created. liveReady counts every ready pod.
			var cellReady, liveReady int32
			var livePods []corev1.Pod
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

				// A live pod is config-current only when it carries BOTH desired
				// hashes: the restart-hash (applied by recreating the pod) and the
				// reload-hash (applied in place via SIGHUP). Requiring both keeps
				// status "in progress" through a reload, not just a rolling restart.
				liveCount++
				livePods = append(livePods, *pod)
				podHasDesiredConfig := pod.Annotations[metadata.AnnotationPostgresConfigHash] == desiredConfigHash &&
					pod.Annotations[metadata.AnnotationPostgresReloadHash] == desiredReloadHash
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
						liveReady++
						if index, ok := resolvePodIndex(pod.Name); ok && index < int(replicas) {
							cellReady++
						}
						break
					}
				}
			}

			// Emit a warning explicitly if the cell pool should have replicas but is empty
			if waitsOnQuota {
				ps.seen.see(shard, string(poolName), cellName, replicas, podList.Items)
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

			// Nodes are listed lazily so a fully Ready shard never touches them.
			if !nodesListed && len(livePods) > int(liveReady) {
				nodesListed = true
				nodeList := &corev1.NodeList{}
				if err := r.List(ctx, nodeList); err != nil {
					log.FromContext(ctx).Error(err, "Failed to list nodes for stuck pod detection")
					nodesListFailed = true
				} else {
					nodes = nodeList.Items
				}
			}
			stuckSamples = append(stuckSamples, monitoring.PoolPodsStuck{
				Pool:  string(poolName),
				Cell:  cellName,
				Count: stuckPoolPods(livePods, nodes),
			})

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

	if !nodesListFailed {
		monitoring.ReplaceShardPoolPodsStuck(clusterName, shard.Name, shard.Namespace, stuckSamples)
	}

	// Content-based drift: at least one live pod is missing a desired config hash.
	// This is the "effective != desired" signal — it covers a spec edit, a
	// PostgresConfigRef ConfigMap edit, and an operator-baseline change on upgrade
	// alike, none of which the shard generation captures. The observed side is the
	// pod's own hashes: the restart-hash lands when the pod is recreated, and the
	// reload-hash lands when the reload reconcile confirms a SIGHUP applied it, so
	// a reload-only change is reported in progress until it has actually reloaded.
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

// nameQuotaWaits adds the Shard's quota waits to the message of the
// Available condition that setConditions wrote. A create that a
// ResourceQuota turns down leaves a pod missing with nothing on the Shard to
// say why: its events are rate limited per object and are dropped once the
// Shard's earlier ones have used the budget. The wait is known from the
// passes before this one, and its text is the same on every pass until the
// wait changes, so naming it costs one status write when it starts and one
// when it ends. seen is what this status update listed, against which the
// object named is checked.
func (r *ShardReconciler) nameQuotaWaits(shard *multigresv1alpha1.Shard, seen quotaObjectsSeen) {
	waits := r.quotaWaiting.describe(client.ObjectKeyFromObject(shard), shard.Generation, seen)
	available := meta.FindStatusCondition(shard.Status.Conditions, "Available")
	if len(waits) == 0 || available == nil {
		return
	}
	available.Message += "; " + strings.Join(waits, "; ")
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

// adoptBackupStatus hands the backup fields an earlier operator wrote with the
// rest of the status, under ManagerShard, to ManagerShardBackup and
// ManagerShardLastBackup before patchStatus stops carrying them: the
// BackupHealthy condition and the last backup's time and type. Leaving them
// out first would remove them while this pass still acts on the verdict in
// memory, quarantine among them, and on a Shard whose backups cannot be
// listed, or which has no primary to list them from, nothing would write them
// again. It writes the values the pass holds, which are the stored ones unless
// the pass has already replaced the verdict, as it does with Unknown when no
// pooler client can be built, and then writes the same value itself.
//
// Each apply carries the cached Shard's resourceVersion as a precondition. A
// snapshot older than what is stored, which may predate a newer verdict
// already written under the backup manager, is refused with a conflict rather
// than put back, and the pass is retried from a newer one. The Shard in memory
// then takes the resourceVersion and managedFields the last apply returned: a
// pass writes status more than once from the one Shard it read, and a later
// write must see the fields already handed off, not hand them off again on a
// snapshot this apply made stale.
func (r *ShardReconciler) adoptBackupStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	rv := shard.ResourceVersion
	// A field is handed off while the main manager holds it and the backup
	// manager does not: an apply of the same value shares ownership, so once
	// the backup manager holds it the main apply leaving it out removes
	// nothing.
	unadopted := func(path, manager string) bool {
		return ownership.Owns(shard, ownership.ManagerShard, path) &&
			!ownership.Owns(shard, manager, path)
	}
	var verdict backupStatus
	if c := meta.FindStatusCondition(
		shard.Status.Conditions,
		backuphealth.ConditionHealthy,
	); c != nil && unadopted(
		ownership.ConditionPath(backuphealth.ConditionHealthy),
		ownership.ManagerShardBackup,
	) {
		verdict.Conditions = []metav1.Condition{*c}
	}
	var last backupStatus
	if shard.Status.LastBackupTime != nil &&
		unadopted(".status.lastBackupTime", ownership.ManagerShardLastBackup) {
		last.LastBackupTime = shard.Status.LastBackupTime
	}
	if shard.Status.LastBackupType != "" &&
		unadopted(".status.lastBackupType", ownership.ManagerShardLastBackup) {
		last.LastBackupType = shard.Status.LastBackupType
	}
	for _, a := range []struct {
		fields  backupStatus
		manager string
	}{{verdict, ownership.ManagerShardBackup}, {last, ownership.ManagerShardLastBackup}} {
		if a.fields.empty() {
			continue
		}
		applied, err := r.applyPartialStatus(ctx, shard, a.fields, a.manager, rv)
		if err != nil {
			return fmt.Errorf("adopt backup status under %s: %w", a.manager, err)
		}
		rv = applied.GetResourceVersion()
		shard.ResourceVersion = rv
		shard.ManagedFields = applied.GetManagedFields()
	}
	return nil
}

// applyBackupStatus writes the backup verdict computed in memory: the
// BackupHealthy condition under ManagerShardBackup, and, when observed is
// true, the last backup's time and type under ManagerShardLastBackup. A pass
// computes the verdict after its other status applies, which run from the
// cached Shard: were the verdict written with them, a pass starting before its
// cache held the previous pass's verdict would put the older verdict back, and
// the next would restore the newer one, flipping the condition every pass
// until the cache caught up. A pass that could not list backups has observed
// no last backup, so it leaves those fields as they are; applying without them
// under the manager that holds them would remove them.
func (r *ShardReconciler) applyBackupStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	observed bool,
) error {
	var verdict backupStatus
	if c := meta.FindStatusCondition(
		shard.Status.Conditions,
		backuphealth.ConditionHealthy,
	); c != nil {
		verdict.Conditions = []metav1.Condition{*c}
	}
	if _, err := r.applyPartialStatus(
		ctx, shard, verdict, ownership.ManagerShardBackup, "",
	); err != nil {
		return fmt.Errorf("failed to apply backup status: %w", err)
	}
	if !observed {
		return nil
	}
	last := backupStatus{
		LastBackupTime: shard.Status.LastBackupTime,
		LastBackupType: shard.Status.LastBackupType,
	}
	if _, err := r.applyPartialStatus(
		ctx, shard, last, ownership.ManagerShardLastBackup, "",
	); err != nil {
		return fmt.Errorf("failed to apply last backup status: %w", err)
	}
	return nil
}

// backupStatus is the part of a Shard's status the backup managers apply. A
// typed ShardStatus would also send orchReady and poolsReady, which encode
// even when false, and claim them for those managers.
type backupStatus struct {
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
	LastBackupTime *metav1.Time       `json:"lastBackupTime,omitempty"`
	LastBackupType string             `json:"lastBackupType,omitempty"`
}

func (b backupStatus) empty() bool {
	return len(b.Conditions) == 0 && b.LastBackupTime == nil && b.LastBackupType == ""
}

// applyPartialStatus applies exactly fields under manager, with resourceVersion
// as a precondition when it is set, and returns the Shard as the apply left
// it. An empty apply removes what manager holds; the client skips
// it when manager holds nothing, which only the client can tell, since it
// remembers writes its cache has not yet shown.
func (r *ShardReconciler) applyPartialStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	fields backupStatus,
	manager, resourceVersion string,
) (*unstructured.Unstructured, error) {
	st, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&fields)
	if err != nil {
		return nil, err
	}
	u := &unstructured.Unstructured{Object: map[string]any{"status": st}}
	u.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("Shard"))
	u.SetName(shard.Name)
	u.SetNamespace(shard.Namespace)
	u.SetResourceVersion(resourceVersion)
	if err := ownership.ApplyStatus(ctx, r.Client, u, manager); err != nil {
		return nil, err
	}
	return u, nil
}
