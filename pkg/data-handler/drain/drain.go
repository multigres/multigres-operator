// Package drain implements the drain state machine for graceful pod removal.
// It handles the lifecycle of a pod being drained: from the initial drain
// request through standby removal, topo unregistration, and final readiness
// for deletion.
package drain

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	// DrainTimeout is the maximum time to wait for a failover or drain operation
	// before giving up and reporting an error.
	DrainTimeout = 5 * time.Minute
)

// ExecuteDrainStateMachine handles the graceful scale down and etcd unregistration for a pod.
// Returns true if reconciliation should be requeued.
func ExecuteDrainStateMachine(
	ctx context.Context,
	k8sClient client.Client,
	rpcClient rpcclient.MultipoolerClient,
	recorder record.EventRecorder,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
) (bool, error) {
	logger := log.FromContext(ctx)

	state := pod.Annotations[metadata.AnnotationDrainState]
	if state == "" || state == metadata.DrainStateReadyForDeletion {
		return false, nil
	}

	clusterName := shard.Labels[metadata.LabelMultigresCluster]

	// Node Failure Safety: If the pod is stuck draining for > 5 minutes, force unregister.
	// We use AnnotationDrainRequestedAt if available, otherwise fall back to DeletionTimestamp.
	var drainStart time.Time
	if reqAtStr := pod.Annotations[metadata.AnnotationDrainRequestedAt]; reqAtStr != "" {
		if reqAt, err := time.Parse(time.RFC3339, reqAtStr); err == nil {
			drainStart = reqAt
		} else {
			logger.Error(
				err,
				"Malformed drain-requested-at annotation, using current time as fallback",
				"pod",
				pod.Name,
				"value",
				reqAtStr,
			)
			drainStart = time.Now()
		}
	}
	if drainStart.IsZero() && !pod.DeletionTimestamp.IsZero() {
		drainStart = pod.DeletionTimestamp.Time
	}

	if !drainStart.IsZero() && time.Since(drainStart) > DrainTimeout {
		logger.Info("Pod is stuck draining, forcing unregistration", "pod", pod.Name)
		cellName := pod.Labels[metadata.LabelMultigresCell]
		if err := topo.ForceUnregisterPod(ctx, store, shard, pod.Name, cellName); err != nil {
			return false, fmt.Errorf("forcing unregistration: %w", err)
		}
		return UpdateDrainState(ctx, k8sClient, pod, metadata.DrainStateReadyForDeletion)
	}

	cells := topo.CollectCells(shard)
	cellName := pod.Labels[metadata.LabelMultigresCell]

	opt := topo.ShardFilter(shard)

	// Find the pooler entry for this pod
	var myPooler *topoclient.MultipoolerInfo
	poolers, err := store.GetMultipoolersByCell(ctx, cellName, opt)
	if err != nil {
		if topo.IsTopoUnavailable(err) && !pod.DeletionTimestamp.IsZero() {
			logger.Info(
				"Topology is unavailable while pod is being deleted. Bypassing drain",
				"pod",
				pod.Name,
			)
			return UpdateDrainState(ctx, k8sClient, pod, metadata.DrainStateReadyForDeletion)
		}
		if !topo.IsTopoUnavailable(err) {
			return false, fmt.Errorf("listing poolers in cell %q: %w", cellName, err)
		}
		logger.Info("Topology unavailable for drain, will retry",
			"pod", pod.Name, "cell", cellName)
		return true, nil
	}

	for _, p := range poolers {
		if topo.PodMatchesPooler(pod.Name, p) {
			myPooler = p
			break
		}
	}

	isPrimary := myPooler != nil && topo.IsPrimaryPooler(myPooler.Multipooler)

	switch state {
	case metadata.DrainStateRequested:
		if isPrimary {
			logger.Info("Draining PRIMARY pod, multiorch will handle failover", "pod", pod.Name)
			recorder.Eventf(
				shard,
				"Warning",
				"PrimaryDrain",
				"Draining PRIMARY pod %s; multiorch will elect a new leader",
				pod.Name,
			)
		} else {
			logger.Info("Proceeding to drain replica pod", "pod", pod.Name)
			primary, err := topo.FindPrimaryPooler(ctx, store, shard, cells)
			if err != nil {
				logger.Error(
					err,
					"Failed to find primary for standby removal, will retry",
					"pod",
					pod.Name,
				)
				return true, nil
			}
			if primary != nil && myPooler != nil && rpcClient != nil {
				if IsPrimaryTerminatingOrMissing(ctx, k8sClient, shard, primary) {
					logger.Info(
						"Primary pod is dead or terminating, skipping standby removal",
						"pod",
						pod.Name,
					)
				} else if IsPrimaryDraining(ctx, k8sClient, shard, primary) {
					logger.Info(
						"Primary pod is being drained, delaying standby removal",
						"pod",
						pod.Name,
					)
					return true, nil
				} else if IsPrimaryNotReady(ctx, k8sClient, shard, primary) {
					logger.Info(
						"Primary pod is not ready, delaying standby removal",
						"pod",
						pod.Name,
					)
					return true, nil
				} else {
					req := &multipoolermanagerdatapb.UpdateConsensusRuleRequest{
						Operation:  multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE,
						StandbyIds: []*clustermetadatapb.ID{myPooler.Id},
					}
					_, rpcErr := rpcClient.UpdateConsensusRule(ctx, primary, req)
					if rpcErr != nil {
						logger.Error(
							rpcErr,
							"Failed to remove pod from synchronous standby list",
							"pod",
							pod.Name,
						)
						return true, nil
					}
				}
			}
		}

		return UpdateDrainState(ctx, k8sClient, pod, metadata.DrainStateDraining)

	case metadata.DrainStateDraining:
		if !isPrimary {
			primary, err := topo.FindPrimaryPooler(ctx, store, shard, cells)
			if err != nil {
				logger.Error(
					err,
					"Failed to find primary for drain verification, will retry",
					"pod",
					pod.Name,
				)
				return true, nil
			}
			if primary != nil && myPooler != nil && rpcClient != nil {
				if IsPrimaryTerminatingOrMissing(ctx, k8sClient, shard, primary) {
					logger.Info(
						"Primary pod is dead or terminating, skipping standby removal verification",
						"pod",
						pod.Name,
					)
				} else if IsPrimaryDraining(ctx, k8sClient, shard, primary) {
					logger.Info(
						"Primary pod is being drained, delaying standby removal verification",
						"pod",
						pod.Name,
					)
					return true, nil
				} else if IsPrimaryNotReady(ctx, k8sClient, shard, primary) {
					logger.Info(
						"Primary pod is not ready, delaying standby removal verification",
						"pod",
						pod.Name,
					)
					return true, nil
				} else {
					req := &multipoolermanagerdatapb.UpdateConsensusRuleRequest{
						Operation:  multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE,
						StandbyIds: []*clustermetadatapb.ID{myPooler.Id},
					}
					_, rpcErr := rpcClient.UpdateConsensusRule(ctx, primary, req)
					if rpcErr != nil {
						logger.Error(
							rpcErr,
							"Standby removal verification failed, will retry",
							"pod",
							pod.Name,
						)
						return true, nil
					}
				}
			}
		}
		return UpdateDrainState(ctx, k8sClient, pod, metadata.DrainStateAcknowledged)

	case metadata.DrainStateAcknowledged:
		if myPooler != nil {
			cellName := pod.Labels[metadata.LabelMultigresCell]
			if err := topo.ForceUnregisterPod(ctx, store, shard, pod.Name, cellName); err != nil {
				return false, fmt.Errorf("unregistering pooler: %w", err)
			}
		}

		monitoring.IncrementDrainOperations(clusterName, shard.Name, "success")
		recorder.Eventf(shard, "Normal", "DrainCompleted", "Pod %s completely drained", pod.Name)
		return UpdateDrainState(ctx, k8sClient, pod, metadata.DrainStateReadyForDeletion)
	}

	return false, nil
}

// UpdateDrainState patches a pod's drain state annotation.
func UpdateDrainState(
	ctx context.Context,
	k8sClient client.Client,
	pod *corev1.Pod,
	newState string,
) (bool, error) {
	patch := client.MergeFrom(pod.DeepCopy())
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[metadata.AnnotationDrainState] = newState
	if err := k8sClient.Patch(ctx, pod, patch); err != nil {
		return false, fmt.Errorf("updating pod drain state to %s: %w", newState, err)
	}
	return true, nil
}

// IsPrimaryTerminatingOrMissing checks if the primary pooler's corresponding Kubernetes pod
// is unavailable for receiving RPCs because it is either missing or terminating.
func IsPrimaryTerminatingOrMissing(
	ctx context.Context,
	k8sClient client.Client,
	shard *multigresv1alpha1.Shard,
	primary *clustermetadatapb.Multipooler,
) bool {
	if primary == nil || primary.Id == nil {
		return true
	}
	primaryPod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: shard.Namespace, Name: primary.Id.Name}
	if err := k8sClient.Get(ctx, key, primaryPod); err != nil {
		if errors.IsNotFound(err) {
			return true
		}
		log.FromContext(ctx).
			Error(err, "Transient error checking primary pod status", "pod", key.Name)
		return false
	}
	return !primaryPod.DeletionTimestamp.IsZero()
}

// IsPrimaryDraining checks if the primary pooler's corresponding Kubernetes pod
// has drain annotations, indicating it is mid-drain and should not receive RPCs.
func IsPrimaryDraining(
	ctx context.Context,
	k8sClient client.Client,
	shard *multigresv1alpha1.Shard,
	primary *clustermetadatapb.Multipooler,
) bool {
	if primary == nil || primary.Id == nil {
		return false
	}
	primaryPod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: shard.Namespace, Name: primary.Id.Name}
	if err := k8sClient.Get(ctx, key, primaryPod); err != nil {
		if errors.IsNotFound(err) {
			return false
		}
		log.FromContext(ctx).
			Error(err, "Transient error checking primary drain status, assuming draining",
				"pod", key.Name)
		return true
	}
	state := primaryPod.Annotations[metadata.AnnotationDrainState]
	return state != "" && state != metadata.DrainStateReadyForDeletion
}

// IsPrimaryNotReady checks if the primary pooler's corresponding Kubernetes pod
// has containers that are not passing readiness probes. This prevents sending
// RPCs to a pod whose multipooler cannot reach its local postgres.
func IsPrimaryNotReady(
	ctx context.Context,
	k8sClient client.Client,
	shard *multigresv1alpha1.Shard,
	primary *clustermetadatapb.Multipooler,
) bool {
	if primary == nil || primary.Id == nil {
		return true
	}
	primaryPod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: shard.Namespace, Name: primary.Id.Name}
	if err := k8sClient.Get(ctx, key, primaryPod); err != nil {
		if errors.IsNotFound(err) {
			return true
		}
		log.FromContext(ctx).
			Error(err, "Transient error checking primary pod readiness", "pod", key.Name)
		return true
	}
	for _, cond := range primaryPod.Status.Conditions {
		if cond.Type == corev1.ContainersReady {
			return cond.Status != corev1.ConditionTrue
		}
	}
	// Optimistic default: if ContainersReady hasn't been populated yet, the pod
	// is already registered as PRIMARY in the topology, so assume it's reachable
	// and let the RPC call itself surface any real unavailability.
	return false
}
