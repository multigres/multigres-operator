package shard

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/drain"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

const (
	// terminatingTimeout is the maximum time to wait for a pod stuck
	// in Terminating state before considering it gone. This prevents
	// shard deletion from blocking indefinitely on a dead kubelet.
	terminatingTimeout = 60 * time.Second

	// podTerminationRequeueDelay is how long to wait between checks for pods
	// to finish terminating during Shard deletion, before PVCs are cleaned up.
	podTerminationRequeueDelay = 2 * time.Second
)

// handleDeletion performs best-effort cleanup when a Shard is deleted.
// Without finalizers, Kubernetes GC handles cascade deletion via ownerRefs.
// This method does best-effort topo cleanup and PVC policy enforcement.
func (r *ShardReconciler) handleDeletion(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Determine matching labels for all resources belonging to this shard.
	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	selector := map[string]string{
		metadata.LabelMultigresCluster:    clusterName,
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}

	// Delete all Deployments owned by this shard.
	deployList := &appsv1.DeploymentList{}
	if err := r.List(
		ctx,
		deployList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list deployments for deletion: %w", err)
	}
	for i := range deployList.Items {
		deploy := &deployList.Items[i]
		if deploy.DeletionTimestamp.IsZero() {
			logger.Info(
				"Initiating deployment deletion during shard cleanup",
				"deployment",
				deploy.Name,
			)
			if err := r.Delete(ctx, deploy); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf(
					"failed to delete deployment %s: %w",
					deploy.Name,
					err,
				)
			}
		}
	}

	// Delete pods before touching PVCs. A PVC must not be handed to the
	// multigres-gc CronJob (or deleted in-line) while a pod still references
	// it: if the pod deletion is stuck (e.g. unhealthy node), GC could delete
	// the backing volume out from under a pod object Kubernetes still tracks.
	// We delete every pod, then requeue until they are all actually gone; the
	// finalizer keeps the Shard CR around until that happens.
	podList := &corev1.PodList{}
	if err := r.List(
		ctx,
		podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list pods for deletion: %w", err)
	}
	blocking := 0
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("failed to delete pod %s: %w", pod.Name, err)
			}
			blocking++
			continue
		}
		// Pod is terminating. Treat it as gone once it has been stuck past
		// terminatingTimeout (e.g. dead kubelet), so a single unhealthy node
		// cannot block Shard deletion indefinitely.
		if time.Since(pod.DeletionTimestamp.Time) < terminatingTimeout {
			blocking++
		} else {
			logger.Info("Pod stuck terminating, proceeding with PVC cleanup",
				"pod", pod.Name, "terminatingSince", pod.DeletionTimestamp.Time)
		}
	}
	if blocking > 0 {
		logger.Info(
			"Waiting for pods to terminate before cleaning up PVCs",
			"remaining", blocking,
		)
		return ctrl.Result{RequeueAfter: podTerminationRequeueDelay}, nil
	}

	// All pods gone. Clean up PVCs whose policy resolves to Delete. Small
	// shards (<= pvcOrphanReplicasThreshold replicas) defer the work to
	// multigres-gc by labelling the PVC with multigres.com/orphan-since=<now>,
	// larger shards are deleted in-line.
	if err := r.cleanupShardPVCs(ctx, shard); err != nil {
		return ctrl.Result{}, err
	}

	// Remove the finalizer last so Kubernetes can finish deletion now that the
	// PVC cleanup has run.
	if slices.Contains(shard.Finalizers, shardFinalizer) {
		patch := client.MergeFrom(shard.DeepCopy())
		shard.Finalizers = slices.DeleteFunc(shard.Finalizers, func(s string) bool {
			return s == shardFinalizer
		})
		if err := r.Patch(ctx, shard, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to remove shard finalizer: %w", err)
		}
	}

	logger.Info("Shard best-effort cleanup complete")
	return ctrl.Result{}, nil
}

// cleanupShardPVCs handles per-PVC cleanup when a Shard is being deleted.
// Only PVCs whose effective WhenDeleted policy is Delete are touched.
func (r *ShardReconciler) cleanupShardPVCs(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	logger := log.FromContext(ctx)

	selector := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(
		ctx,
		pvcList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return fmt.Errorf("failed to list PVCs for cleanup: %w", err)
	}

	// Group eligible PVCs by pool+cell so the keep-threshold decision is made
	// per group (matching replicasPerCell semantics).
	groups := map[string][]*corev1.PersistentVolumeClaim{}
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !shardPVCShouldBeCleaned(shard, pvc) {
			continue
		}
		key := pvc.Labels[metadata.LabelMultigresPool] + "/" + pvc.Labels[metadata.LabelMultigresCell]
		groups[key] = append(groups[key], pvc)
	}

	now := time.Now()
	for _, group := range groups {
		// Sort descending by name so the highest-ordinal PVCs are visited first
		// and become the deleted excess, the lowest are kept as orphans.
		slices.SortFunc(group, func(a, b *corev1.PersistentVolumeClaim) int {
			return strings.Compare(b.Name, a.Name)
		})
		live := len(group)
		for _, pvc := range group {
			_, hasIndex := resolvePodIndex(pvc.Name)
			if !hasIndex || orphanByRemainingCount(live) {
				if err := pvcutil.MarkOrphan(ctx, r.Client, pvc, shard.GetUID(), now); err != nil {
					return fmt.Errorf("failed to mark PVC %s orphan: %w", pvc.Name, err)
				}
				logger.Info("Marked PVC orphan on Shard deletion", "pvc", pvc.Name)
				live--
				continue
			}
			if err := r.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("failed to delete PVC %s: %w", pvc.Name, err)
			}
			logger.Info("Deleted PVC on Shard deletion", "pvc", pvc.Name)
			live--
		}
	}
	return nil
}

// shardPVCShouldBeCleaned returns true when the PVC's effective
// PVCDeletionPolicy (WhenDeleted) resolves to Delete.
func shardPVCShouldBeCleaned(
	shard *multigresv1alpha1.Shard,
	pvc *corev1.PersistentVolumeClaim,
) bool {
	poolName := pvc.Labels[metadata.LabelMultigresPool]
	if poolName == "" {
		return ShouldDeleteShardLevelPVCOnRemoval(shard)
	}
	poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	if !ok {
		return ShouldDeleteShardLevelPVCOnRemoval(shard)
	}
	return ShouldDeletePVCOnShardRemoval(shard, poolSpec)
}

// handlePendingDeletion handles graceful shard deletion. When a shard is marked
// with the PendingDeletion annotation, this method drains all pods via the drain
// state machine. Once all pods are drained (or gone), it sets the
// ReadyForDeletion condition so the TableGroup controller can safely delete
// the Shard CR.
func (r *ShardReconciler) handlePendingDeletion(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Handling PendingDeletion")

	// List all pods belonging to this shard.
	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}
	podList := &corev1.PodList{}
	if err := r.List(
		ctx,
		podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(lbls),
	); err != nil {
		logger.Error(err, "Failed to list pods for PendingDeletion")
		return ctrl.Result{}, fmt.Errorf("failed to list pods for pending deletion: %w", err)
	}

	// Open topo store if we have pods that need drain state machine execution.
	var store topoclient.Store
	if len(podList.Items) > 0 {
		var err error
		store, err = r.topoStore(shard)
		if err != nil {
			logger.Error(err, "Failed to get topo store for PendingDeletion")
			r.Recorder.Eventf(shard, "Warning", "TopologyError",
				"Cannot connect to topology during pending deletion: %v", err)
			return ctrl.Result{RequeueAfter: topoUnavailableRequeueDelay}, nil
		}
		defer func() { _ = store.Close() }()

		// Update PodRoles so the drain state machine has current role info.
		r.reconcilePodRoles(ctx, store, shard)
	}

	allDrained := true
	for i := range podList.Items {
		pod := &podList.Items[i]

		// Skip pods already being deleted, but apply a timeout for pods
		// stuck in Terminating (e.g. kubelet failure, node down).
		if !pod.DeletionTimestamp.IsZero() {
			if time.Since(pod.DeletionTimestamp.Time) < terminatingTimeout {
				allDrained = false
			} else {
				logger.Info("Pod stuck terminating, treating as gone",
					"pod", pod.Name,
					"terminatingSince", pod.DeletionTimestamp.Time)
				r.Recorder.Eventf(shard, "Warning", "StuckTerminating",
					"Pod %s has been terminating for >%s, treating as gone",
					pod.Name, terminatingTimeout)
			}
			continue
		}

		drainState := pod.Annotations[metadata.AnnotationDrainState]

		switch drainState {
		case metadata.DrainStateReadyForDeletion:
			// Pod is fully drained, delete it.
			logger.Info("Deleting drained pod during PendingDeletion", "pod", pod.Name)
			if err := r.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf(
					"failed to delete drained pod %s: %w", pod.Name, err)
			}
			allDrained = false

		case "":
			// Not draining yet — initiate drain.
			logger.Info("Initiating drain for PendingDeletion", "pod", pod.Name)
			if err := r.initiateDrain(ctx, pod); err != nil {
				return ctrl.Result{}, fmt.Errorf(
					"failed to initiate drain for pod %s: %w", pod.Name, err)
			}
			r.Recorder.Eventf(shard, "Normal", "DrainStarted",
				"Initiated drain for pod %s (pending deletion)", pod.Name)
			allDrained = false

		default:
			// Drain in progress — run the drain state machine.
			if store != nil {
				if _, derr := drain.ExecuteDrainStateMachine(
					ctx, r.Client, r.RPCClient, r.Recorder, store, shard, pod,
				); derr != nil {
					logger.Error(derr, "Failed to execute drain state machine", "pod", pod.Name)
				}
			}
			allDrained = false
		}
	}

	if !allDrained {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// All pods drained and deleted — set ReadyForDeletion condition.
	if !status.IsConditionTrue(
		shard.Status.Conditions,
		multigresv1alpha1.ConditionReadyForDeletion,
	) {
		statusBase := shard.DeepCopy()
		status.SetCondition(&shard.Status.Conditions, metav1.Condition{
			Type:               multigresv1alpha1.ConditionReadyForDeletion,
			Status:             metav1.ConditionTrue,
			Reason:             "DrainComplete",
			Message:            "All pods drained; shard is ready for deletion",
			ObservedGeneration: shard.Generation,
			LastTransitionTime: metav1.Now(),
		})
		if err := r.Status().Patch(ctx, shard, client.MergeFrom(statusBase)); err != nil {
			return ctrl.Result{}, fmt.Errorf("setting ReadyForDeletion condition: %w", err)
		}
		logger.Info("Set ReadyForDeletion condition")
		r.Recorder.Event(shard, "Normal", "ReadyForDeletion",
			"All pods drained; shard is ready for deletion")
	}

	return ctrl.Result{}, nil
}
