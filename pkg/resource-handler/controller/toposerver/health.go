package toposerver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const healthProbeTimeout = 5 * time.Second

// reconcileHealth runs independently of resource reconciliation: a failed apply
// or an interrupted maintenance operation must not stop health observations.
func (r *TopoServerReconciler) reconcileHealth(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	ts := &multigresv1alpha1.TopoServer{}
	if err := r.maintenanceReader().Get(ctx, req.NamespacedName, ts); err != nil {
		if apierrors.IsNotFound(err) {
			monitoring.DeleteTopologyHealth(req.Name, req.Namespace)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !ts.DeletionTimestamp.IsZero() {
		monitoring.DeleteTopologyHealth(req.Name, req.Namespace)
		return ctrl.Result{}, nil
	}
	condition, members := r.probeHealth(ctx, ts)
	r.observeMemberPods(ctx, ts, members)
	checked := metav1.Now()
	monitoring.SetTopologyHealth(
		ts.Labels[metadata.LabelMultigresCluster],
		ts.Name,
		ts.Namespace,
		condition,
		checked.Time,
		members,
	)
	meta.SetStatusCondition(&ts.Status.Conditions, condition)
	condition = *meta.FindStatusCondition(ts.Status.Conditions, "QuorumAvailable")
	patch := &multigresv1alpha1.TopoServer{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "TopoServer",
		},
		ObjectMeta: metav1.ObjectMeta{Name: ts.Name, Namespace: ts.Namespace},
		Status: multigresv1alpha1.TopoServerStatus{
			Conditions:      []metav1.Condition{condition},
			HealthCheckedAt: &checked,
		},
	}
	err := r.Status().
		Patch(ctx, patch, client.Apply, client.FieldOwner("multigres-topology-health"), client.ForceOwnership)
	return ctrl.Result{RequeueAfter: statusRecheckDelay}, err
}

func (r *TopoServerReconciler) probeHealth(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) (metav1.Condition, []monitoring.TopologyMemberMetrics) {
	condition := metav1.Condition{
		Type:               "QuorumAvailable",
		ObservedGeneration: ts.Generation,
		Status:             metav1.ConditionUnknown,
		Reason:             "ProbeFailed",
		Message:            "Could not check etcd quorum",
	}
	endpoints := maintenanceEndpoints(ts)
	members := make([]monitoring.TopologyMemberMetrics, len(endpoints))
	for i := range members {
		members[i].Name = fmt.Sprintf("%s-%d", ts.Name, i)
	}
	probeCtx, cancel := context.WithTimeout(ctx, healthProbeTimeout)
	defer cancel()
	c, err := r.maintenanceClient(probeCtx, ts)
	if err != nil {
		condition.Message = fmt.Sprintf("Could not create etcd health client: %v", err)
		return condition, members
	}
	defer c.Close()
	clusterIDs := make([]uint64, len(endpoints))
	statusErrors := make([][]string, len(endpoints))
	var wg sync.WaitGroup
	for i, endpoint := range endpoints {
		wg.Go(func() {
			s, err := c.Status(probeCtx, endpoint)
			if err != nil || s == nil || s.Header == nil || s.Header.ClusterId == 0 ||
				s.Header.MemberId == 0 {
				return
			}
			clusterIDs[i] = s.Header.ClusterId
			statusErrors[i] = s.Errors
			members[i].BackendBytes = ptr.To(s.DbSize)
			members[i].BackendInUseBytes = ptr.To(s.DbSizeInUse)
			members[i].Revision = ptr.To(s.Header.Revision)
			members[i].Up = c.Health(probeCtx, endpoint) == nil
		})
	}
	wg.Wait()
	var clusterID uint64
	for _, observedID := range clusterIDs {
		if observedID == 0 {
			continue
		}
		if clusterID != 0 && observedID != clusterID {
			condition.Status, condition.Reason = metav1.ConditionFalse, "ClusterMismatch"
			condition.Message = "Etcd endpoints report different cluster IDs; topology membership is inconsistent and failover protection is unavailable"
			return condition, members
		}
		clusterID = observedID
	}
	var failures []string
	for i, errors := range statusErrors {
		if len(errors) > 0 {
			failures = append(
				failures,
				fmt.Sprintf("%s: %s", members[i].Name, strings.Join(errors, ", ")),
			)
		}
	}
	if len(failures) > 0 {
		// NOSPACE can reject writes cluster-wide while linearizable reads still
		// succeed. A read through another member must not mask a reported error.
		condition.Status, condition.Reason = metav1.ConditionFalse, "EtcdStatusError"
		condition.Message = fmt.Sprintf(
			"Etcd members report status errors (%s); failover protection is unavailable",
			strings.Join(failures, "; "),
		)
		return condition, members
	}
	responding, readable := 0, 0
	for _, member := range members {
		if member.BackendBytes != nil {
			responding++
		}
		if member.Up {
			readable++
		}
	}
	switch {
	case readable > 0:
		// A successful linearizable read requires a quorum round trip. Counting
		// reachable endpoints alone would misclassify asymmetric network failures.
		condition.Status, condition.Reason = metav1.ConditionTrue, "QuorumAvailable"
		condition.Message = fmt.Sprintf(
			"Linearizable reads succeeded through %d/%d etcd members",
			readable,
			len(endpoints),
		)
	case responding > 0:
		condition.Status, condition.Reason = metav1.ConditionFalse, "QuorumUnavailable"
		condition.Message = "Etcd members respond to status requests, but no member completed a linearizable read; failover protection is unavailable"
	default:
		condition.Status, condition.Reason = metav1.ConditionFalse, "TopologyUnreachable"
		condition.Message = "No etcd member responded to the operator; quorum cannot be verified and failover protection is unavailable"
	}
	return condition, members
}

func (r *TopoServerReconciler) observeMemberPods(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
	members []monitoring.TopologyMemberMetrics,
) {
	sts := &appsv1.StatefulSet{}
	if err := r.maintenanceReader().Get(ctx, client.ObjectKeyFromObject(ts), sts); err != nil {
		return
	}
	for i := range members {
		pod := &corev1.Pod{}
		if err := r.maintenanceReader().
			Get(ctx, client.ObjectKey{Namespace: ts.Namespace, Name: members[i].Name}, pod); err != nil {
			continue
		}
		if !metav1.IsControlledBy(pod, sts) {
			continue
		}
		for _, container := range pod.Spec.Containers {
			if container.Name != "etcd" {
				continue
			}
			members[i].MemoryLimitBytes = ptr.To(container.Resources.Limits.Memory().Value())
			// Older managed pods use etcd's default quota until their template rolls.
			members[i].QuotaBytes = ptr.To(int64(2 * 1024 * 1024 * 1024))
			for _, env := range container.Env {
				if env.Name == "ETCD_QUOTA_BACKEND_BYTES" {
					quota, err := strconv.ParseInt(env.Value, 10, 64)
					if err == nil {
						members[i].QuotaBytes = &quota
					} else {
						members[i].QuotaBytes = nil
					}
				}
			}
		}
		for _, state := range pod.Status.ContainerStatuses {
			if state.Name != "etcd" {
				continue
			}
			members[i].Restarts = ptr.To(int64(state.RestartCount))
			members[i].OOMTimestamp = ptr.To(int64(0))
			for _, terminated := range []*corev1.ContainerStateTerminated{state.LastTerminationState.Terminated, state.State.Terminated} {
				if terminated != nil && terminated.Reason == "OOMKilled" &&
					!terminated.FinishedAt.IsZero() {
					members[i].OOMTimestamp = ptr.To(terminated.FinishedAt.Unix())
				}
			}
		}
	}
}
