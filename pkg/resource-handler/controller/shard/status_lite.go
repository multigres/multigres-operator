package shard

import (
	"context"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

// litePrimaryTimeout bounds the Status RPC that reads the Lite pooler's role.
const litePrimaryTimeout = 5 * time.Second

// updateLiteStatus is the Lite branch of updateStatus. It reports the single
// pod as a pool of one, so PoolsReady, ReadyReplicas and Phase roll up through
// the TableGroup as they do for any other shard, and sets three conditions:
// Available from the pod's readiness, Primary from the pooler's own routing
// role, and UpToDate from the pod's spec-hash and confirmed reload-hash.
func (r *ShardReconciler) updateLiteStatus(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rendered renderedConfig,
) error {
	oldPhase := shard.Status.Phase

	poolName, cellName, poolSpec, err := litePool(shard)
	if err != nil {
		return err
	}

	podName := BuildPoolPodName(shard, poolName, cellName, 0)
	var pod *corev1.Pod
	existing := &corev1.Pod{}
	if err := r.Get(
		ctx,
		client.ObjectKey{Namespace: shard.Namespace, Name: podName},
		existing,
	); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get pod %s for status: %w", podName, err)
		}
	} else if existing.DeletionTimestamp.IsZero() &&
		existing.Status.Phase != corev1.PodFailed && existing.Status.Phase != corev1.PodSucceeded {
		pod = existing
	}

	var readyPods int32
	if isPodReady(pod) {
		readyPods = 1
	}
	shard.Status.Cells = []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cellName)}
	shard.Status.PoolsReady = readyPods == 1
	shard.Status.ReadyReplicas = readyPods
	shard.Status.OrchReady = false

	r.setConditions(shard, 1, readyPods)
	meta.SetStatusCondition(
		&shard.Status.Conditions,
		r.litePrimaryCondition(ctx, shard, cellName, pod),
	)
	meta.SetStatusCondition(
		&shard.Status.Conditions,
		r.liteUpToDateCondition(shard, poolName, cellName, poolSpec, rendered, pod),
	)

	primary := meta.IsStatusConditionTrue(
		shard.Status.Conditions,
		multigresv1alpha1.ConditionPrimary,
	)
	switch {
	case pod != nil && status.IsCrashLooping(pod):
		shard.Status.Phase = multigresv1alpha1.PhaseDegraded
		shard.Status.Message = "The Lite pod is crash-looping"
	case shard.Status.PoolsReady && primary:
		shard.Status.Phase = multigresv1alpha1.PhaseHealthy
		shard.Status.Message = "Ready"
	default:
		shard.Status.Phase = multigresv1alpha1.PhaseProgressing
		shard.Status.Message = fmt.Sprintf(
			"PoolsReady: %v, Primary: %v",
			shard.Status.PoolsReady,
			primary,
		)
	}

	shard.Status.ObservedGeneration = shard.Generation
	return r.patchStatus(ctx, shard, oldPhase)
}

// litePrimaryCondition asks the Lite pooler for its routing role over the
// Status RPC, with the client the data-plane phase uses for this shard. It is
// read from the pooler rather than from pod readiness, because readiness only
// says the gateway half is serving.
func (r *ShardReconciler) litePrimaryCondition(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	cellName string,
	pod *corev1.Pod,
) metav1.Condition {
	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionPrimary,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: shard.Generation,
	}

	if pod == nil || pod.Status.PodIP == "" {
		cond.Reason = "PodUnavailable"
		cond.Message = "The Lite pod is not running"
		return cond
	}
	if r.PoolerClients == nil {
		cond.Reason = reasonPoolerClientUnavailable
		cond.Message = "No multipooler RPC client is configured"
		return cond
	}
	rpc, err := r.PoolerClients.ClientFor(ctx, shard)
	if err != nil {
		cond.Reason = reasonPoolerClientUnavailable
		cond.Message = fmt.Sprintf("Failed to build multipooler RPC client: %v", err)
		return cond
	}

	resp, err := liteStatus(ctx, rpc, cellName, pod)
	if err != nil {
		cond.Reason = "PoolerUnreachable"
		cond.Message = fmt.Sprintf("multipooler status RPC failed: %v", err)
		return cond
	}
	role := resp.GetStatus().GetPoolerType()
	if role != clustermetadatapb.PoolerType_PRIMARY {
		cond.Reason = "NotPrimary"
		cond.Message = fmt.Sprintf("The pooler reports routing role %s", role)
		return cond
	}

	cond.Status = metav1.ConditionTrue
	cond.Reason = "PoolerPrimary"
	cond.Message = "The pooler reports routing role PRIMARY"
	return cond
}

// liteStatus calls the Status RPC on the Lite pod's gRPC port. The pooler's
// topology ID is built the way the pod registers it: its cell and service ID.
func liteStatus(
	ctx context.Context,
	rpc rpcclient.MultipoolerClient,
	cellName string,
	pod *corev1.Pod,
) (*multipoolermanagerdatapb.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, litePrimaryTimeout)
	defer cancel()
	return rpc.Status(ctx, &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cellName,
			Name:      BuildPoolServiceID(pod.Name),
		},
		Hostname: pod.Status.PodIP,
		PortMap:  map[string]int32{"grpc": DefaultMultipoolerGRPCPort},
	}, &multipoolermanagerdatapb.StatusRequest{})
}

// liteUpToDateCondition compares the Lite pod's spec-hash annotation with the
// hash of the pod BuildLitePod would build now. The shard's config hashes are
// stamped by reconcilePostgresConfig later in the reconcile, so the desired
// pod is built from a copy carrying the hashes rendered for this reconcile.
func (r *ShardReconciler) liteUpToDateCondition(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	rendered renderedConfig,
	pod *corev1.Pod,
) metav1.Condition {
	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionUpToDate,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: shard.Generation,
	}
	if rendered.err != nil {
		cond.Reason = "ConfigError"
		cond.Message = fmt.Sprintf("Failed to render postgresql.conf: %v", rendered.err)
		return cond
	}
	if pod == nil {
		cond.Reason = "PodMissing"
		cond.Message = "The Lite pod does not exist"
		return cond
	}

	desiredShard := shard.DeepCopy()
	if desiredShard.Annotations == nil {
		desiredShard.Annotations = make(map[string]string)
	}
	desiredShard.Annotations[metadata.AnnotationPostgresConfigHash] = rendered.restartHash
	desiredShard.Annotations[metadata.AnnotationPostgresReloadHash] = rendered.reloadHash
	desired, err := r.buildLitePod(desiredShard, poolName, cellName, poolSpec)
	if err != nil {
		cond.Reason = "BuildFailed"
		cond.Message = fmt.Sprintf("Failed to build the desired Lite pod: %v", err)
		return cond
	}

	if pod.Annotations[metadata.AnnotationSpecHash] != desired.Annotations[metadata.AnnotationSpecHash] {
		cond.Reason = "SpecChanged"
		cond.Message = "The Lite pod does not match the desired spec and will be replaced"
		return cond
	}
	if pod.Annotations[metadata.AnnotationPostgresReloadHash] != rendered.reloadHash {
		cond.Reason = "ReloadPending"
		cond.Message = "Waiting for the Lite pooler to confirm the PostgreSQL config reload"
		return cond
	}
	cond.Status = metav1.ConditionTrue
	cond.Reason = "SpecApplied"
	cond.Message = "The Lite pod matches the desired spec"
	return cond
}
