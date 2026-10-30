package toposerver

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
)

const (
	maintenanceInterval            = time.Hour
	maintenanceTimeout             = 2 * time.Minute
	maintenanceHealthTimeout       = 15 * time.Second
	maintenanceStablePeriod        = time.Minute
	minimumReclaimBytes      int64 = 100 * 1024 * 1024
	maintenanceFieldOwner          = ownership.ManagerEtcdMaintenance
)

func maintenanceEnabled(ts *multigresv1alpha1.TopoServer) bool {
	return ts.Spec.Etcd != nil && ts.Spec.Etcd.Maintenance.DefragmentationIsEnabled()
}

func (r *TopoServerReconciler) maintenanceReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// MaintenanceEndpoints returns the member addresses the controller dials for
// ts. The test suite builds its etcd simulator from this so the simulator
// answers on exactly the strings the controller passes, and cannot drift.
func MaintenanceEndpoints(ts *multigresv1alpha1.TopoServer) []string {
	return maintenanceEndpoints(ts)
}

func maintenanceEndpoints(ts *multigresv1alpha1.TopoServer) []string {
	replicas := DefaultReplicas
	if ts.Spec.Etcd != nil && ts.Spec.Etcd.Replicas != nil {
		replicas = *ts.Spec.Etcd.Replicas
	}
	endpoints := make([]string, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		endpoints = append(
			endpoints,
			fmt.Sprintf(
				"%s://%s-%d.%s-headless.%s.svc.cluster.local:2379",
				clientScheme(ts.Spec.TLS.IsEnabled()),
				ts.Name,
				i,
				ts.Name,
				ts.Namespace,
			),
		)
	}
	return endpoints
}

// healthyMembers requires all expected voting members, a common leader, and a
// successful linearizable read through each member. Pod readiness alone cannot
// establish quorum health. Status also rejects members reporting etcd alarms.
func healthyMembers(
	ctx context.Context,
	c EtcdMaintenanceClient,
	endpoints []string,
) (map[string]*clientv3.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, maintenanceHealthTimeout)
	defer cancel()
	members, err := c.Members(ctx)
	if err != nil {
		return nil, err
	}
	if len(members.Members) != len(endpoints) {
		return nil, fmt.Errorf("etcd membership differs from expected replicas")
	}
	ids := make(map[uint64]bool, len(endpoints))
	for _, m := range members.Members {
		if m.IsLearner || m.ID == 0 {
			return nil, fmt.Errorf("etcd membership includes an unready voting member")
		}
		ids[m.ID] = true
	}
	statuses := make(map[string]*clientv3.StatusResponse, len(endpoints))
	var leader, clusterID uint64
	for _, ep := range endpoints {
		s, err := c.Status(ctx, ep)
		if err != nil {
			return nil, fmt.Errorf("etcd member %s status: %w", ep, err)
		}
		if s == nil || s.Header == nil || s.Header.ClusterId == 0 || !ids[s.Header.MemberId] ||
			s.Leader == 0 ||
			s.IsLearner ||
			len(s.Errors) != 0 {
			return nil, fmt.Errorf("etcd member %s is not healthy", ep)
		}
		if leader == 0 {
			leader, clusterID = s.Leader, s.Header.ClusterId
		}
		if s.Leader != leader || s.Header.ClusterId != clusterID {
			return nil, fmt.Errorf("etcd members disagree on leader or cluster identity")
		}
		delete(ids, s.Header.MemberId)
		if err := c.Health(ctx, ep); err != nil {
			return nil, fmt.Errorf("etcd member %s linearizable read: %w", ep, err)
		}
		statuses[ep] = s
	}
	foundLeader := false
	for _, s := range statuses {
		foundLeader = foundLeader || s.Header.MemberId == leader
	}
	if !foundLeader {
		return nil, fmt.Errorf("etcd leader is not an expected member")
	}
	return statuses, nil
}

// maintenanceWorkloadReady uses uncached observations, checks ownership, and
// excludes partially applied templates, terminating pods, and recent restarts.
func (r *TopoServerReconciler) maintenanceWorkloadReady(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.maintenanceReader().Get(ctx, client.ObjectKeyFromObject(ts), sts); err != nil {
		return false, err
	}
	owner := metav1.GetControllerOf(sts)
	if owner == nil || owner.UID != ts.UID || !sts.DeletionTimestamp.IsZero() ||
		sts.Spec.Replicas == nil {
		return false, nil
	}
	n := *sts.Spec.Replicas
	if n < 3 || int64(n) != int64(len(maintenanceEndpoints(ts))) ||
		sts.Status.ObservedGeneration != sts.Generation ||
		sts.Status.ReadyReplicas != n ||
		sts.Status.UpdatedReplicas != n ||
		sts.Status.CurrentRevision == "" ||
		sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		return false, nil
	}
	for i := int32(0); i < n; i++ {
		pod := &corev1.Pod{}
		if err := r.maintenanceReader().
			Get(ctx, client.ObjectKey{Namespace: ts.Namespace, Name: fmt.Sprintf("%s-%d", ts.Name, i)}, pod); err != nil {
			return false, err
		}
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.UID != sts.UID || !pod.DeletionTimestamp.IsZero() ||
			pod.Labels[appsv1.StatefulSetRevisionLabel] != sts.Status.UpdateRevision {
			return false, nil
		}
		ready := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue &&
				!condition.LastTransitionTime.IsZero() &&
				time.Since(condition.LastTransitionTime.Time) >= maintenanceStablePeriod {
				ready = true
			}
		}
		if !ready {
			return false, nil
		}
	}
	return true, nil
}

func (r *TopoServerReconciler) saveMaintenance(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
	state *multigresv1alpha1.EtcdMaintenanceStatus,
) error {
	before := ts.DeepCopy()
	ts.Status.EtcdMaintenance = state
	// The resourceVersion precondition is the lock: only one controller can
	// reserve this generation. A conflict never authorizes a maintenance RPC.
	return r.Status().
		Patch(ctx, ts, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}), client.FieldOwner(maintenanceFieldOwner))
}

func (r *TopoServerReconciler) resumeMaintenance(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) (bool, error) {
	if !maintenanceEnabled(ts) && ts.Status.EtcdMaintenance == nil {
		return false, nil
	}
	fresh := &multigresv1alpha1.TopoServer{}
	if err := r.maintenanceReader().Get(ctx, client.ObjectKeyFromObject(ts), fresh); err != nil {
		return false, err
	}
	*ts = *fresh
	state := ts.Status.EtcdMaintenance
	if state == nil || !state.InProgress {
		return false, nil
	}
	if time.Since(state.LastAttemptTime.Time) < maintenanceTimeout {
		return true, nil
	}
	// Disabling maintenance explicitly releases an abandoned reservation after
	// its RPC deadline, allowing an administrator to repair an unhealthy member.
	if maintenanceEnabled(ts) {
		c, err := r.maintenanceClient(ctx, ts)
		if err != nil {
			return true, err
		}
		defer c.Close()
		if _, err := healthyMembers(ctx, c, maintenanceEndpoints(ts)); err != nil {
			return true, err
		}
	}
	state = state.DeepCopy()
	state.InProgress = false
	return false, r.saveMaintenance(ctx, ts, state)
}

func (r *TopoServerReconciler) reconcileMaintenance(
	ctx context.Context,
	ts *multigresv1alpha1.TopoServer,
) error {
	if !maintenanceEnabled(ts) {
		return nil
	}
	if state := ts.Status.EtcdMaintenance; state != nil &&
		(state.InProgress || time.Since(state.LastAttemptTime.Time) < maintenanceInterval) {
		return nil
	}
	ready, err := r.maintenanceWorkloadReady(ctx, ts)
	if err != nil || !ready {
		return err
	}
	c, err := r.maintenanceClient(ctx, ts)
	if err != nil {
		return err
	}
	defer c.Close()
	endpoints := maintenanceEndpoints(ts)
	statuses, err := healthyMembers(ctx, c, endpoints)
	if err != nil {
		return err
	}
	var target string
	var reclaim int64
	for _, ep := range endpoints {
		s := statuses[ep]
		free := s.DbSize - s.DbSizeInUse
		if s.DbSizeInUse <= 0 || free < minimumReclaimBytes ||
			float64(free)/float64(s.DbSize) < 0.3 {
			continue
		}
		if free > reclaim {
			target, reclaim = ep, free
		}
	}
	if target == "" {
		return nil
	}

	// Re-read the CR immediately before the CAS. A changed spec is handled by
	// the next reconcile, never with the old endpoint or maintenance settings.
	fresh := &multigresv1alpha1.TopoServer{}
	if err := r.maintenanceReader().Get(ctx, client.ObjectKeyFromObject(ts), fresh); err != nil {
		return err
	}
	if fresh.UID != ts.UID || fresh.Generation != ts.Generation ||
		!fresh.DeletionTimestamp.IsZero() {
		return nil
	}
	if state := fresh.Status.EtcdMaintenance; state != nil &&
		(state.InProgress || time.Since(state.LastAttemptTime.Time) < maintenanceInterval) {
		return nil
	}
	state := &multigresv1alpha1.EtcdMaintenanceStatus{
		LastAttemptTime: metav1.Now(),
		Endpoint:        target,
		InProgress:      true,
	}
	if err := r.saveMaintenance(ctx, fresh, state); err != nil {
		return err
	}
	ts.Status.EtcdMaintenance = state.DeepCopy()

	operationCtx, cancel := context.WithTimeout(ctx, maintenanceTimeout)
	defer cancel()
	// Once reserved, leave InProgress set on any uncertainty. A later reconcile
	// must verify health before releasing it; it cannot jump to another member.
	ready, err = r.maintenanceWorkloadReady(operationCtx, fresh)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("etcd workload changed before defragmentation")
	}
	statuses, err = healthyMembers(operationCtx, c, endpoints)
	if err != nil {
		return err
	}
	if s := statuses[target]; s.Header.MemberId == s.Leader {
		for _, ep := range endpoints {
			if ep == target {
				continue
			}
			if err := c.MoveLeader(operationCtx, target, statuses[ep].Header.MemberId); err != nil {
				return err
			}
			break
		}
		statuses, err = healthyMembers(operationCtx, c, endpoints)
		if err != nil {
			return err
		}
		if statuses[target].Header.MemberId == statuses[target].Leader {
			return fmt.Errorf("etcd leadership transfer has not completed")
		}
	}
	if err := c.Defragment(operationCtx, target); err != nil {
		return fmt.Errorf("defragmenting %s: %w", target, err)
	}
	if _, err := healthyMembers(operationCtx, c, endpoints); err != nil {
		return err
	}
	state = state.DeepCopy()
	state.InProgress = false
	if err := r.saveMaintenance(ctx, fresh, state); err != nil {
		return err
	}
	ts.Status.EtcdMaintenance = state
	r.Recorder.Eventf(
		ts,
		"Normal",
		"EtcdDefragmented",
		"Defragmented member %s (estimated reclaimable bytes: %d)",
		target,
		reclaim,
	)
	return nil
}
