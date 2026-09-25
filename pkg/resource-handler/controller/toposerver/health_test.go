package toposerver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

type healthTestClient struct {
	*fakeEtcdMaintenance
	unavailable  map[string]bool
	blockedReads map[string]bool
}

func (c *healthTestClient) Status(
	ctx context.Context,
	endpoint string,
) (*clientv3.StatusResponse, error) {
	if c.unavailable[endpoint] {
		return nil, errors.New("connection refused")
	}
	return c.fakeEtcdMaintenance.Status(ctx, endpoint)
}

func (c *healthTestClient) Health(_ context.Context, endpoint string) error {
	if c.blockedReads[endpoint] {
		return errors.New("linearizable read timed out")
	}
	return nil
}

func TestProbeTopologyQuorum(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		unavailable, blocked         []int
		statusErrors                 map[int][]string
		clientError, clusterMismatch bool
		want                         metav1.ConditionStatus
		reason                       string
	}{
		{name: "different etcd clusters", clusterMismatch: true, want: metav1.ConditionFalse, reason: "ClusterMismatch"},
		{name: "healthy", want: metav1.ConditionTrue, reason: "QuorumAvailable"},
		{name: "NOSPACE despite successful reads", statusErrors: map[int][]string{0: {"NOSPACE"}}, want: metav1.ConditionFalse, reason: "EtcdStatusError"},
		{name: "healthy members cannot mask another member alarm", statusErrors: map[int][]string{2: {"NOSPACE"}}, want: metav1.ConditionFalse, reason: "EtcdStatusError"},
		{name: "multiple status errors", statusErrors: map[int][]string{0: {"NOSPACE", "CORRUPT"}, 1: {"unexpected health error"}}, want: metav1.ConditionFalse, reason: "EtcdStatusError"},
		{name: "alarm on member with failed reads", blocked: []int{0}, statusErrors: map[int][]string{0: {"NOSPACE"}}, want: metav1.ConditionFalse, reason: "EtcdStatusError"},
		{name: "one unreachable member", unavailable: []int{0}, want: metav1.ConditionTrue, reason: "QuorumAvailable"},
		{name: "only one endpoint reachable but quorum read succeeds", unavailable: []int{0, 1}, want: metav1.ConditionTrue, reason: "QuorumAvailable"},
		{name: "status works without quorum", blocked: []int{0, 1, 2}, want: metav1.ConditionFalse, reason: "QuorumUnavailable"},
		{name: "all members unreachable", unavailable: []int{0, 1, 2}, want: metav1.ConditionFalse, reason: "TopologyUnreachable"},
		{name: "credential error", clientError: true, want: metav1.ConditionUnknown, reason: "ProbeFailed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ts, f := maintenanceFixture(t)
			c := &healthTestClient{
				fakeEtcdMaintenance: f,
				unavailable:         map[string]bool{},
				blockedReads:        map[string]bool{},
			}
			endpoints := maintenanceEndpoints(ts)
			if tc.clusterMismatch {
				f.statuses[endpoints[1]].Header.ClusterId = 456
			}
			for _, i := range tc.unavailable {
				c.unavailable[endpoints[i]] = true
			}
			for _, i := range tc.blocked {
				c.blockedReads[endpoints[i]] = true
			}
			for i, errors := range tc.statusErrors {
				f.statuses[endpoints[i]].Errors = errors
			}
			r.newMaintenanceClient = func(ctx context.Context, _ *multigresv1alpha1.TopoServer) (etcdMaintenanceClient, error) {
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.LessOrEqual(t, time.Until(deadline), healthProbeTimeout)
				if tc.clientError {
					return nil, errors.New("invalid TLS certificate")
				}
				return c, nil
			}
			condition, members := r.probeHealth(t.Context(), ts)
			require.Equal(t, tc.want, condition.Status)
			require.Equal(t, tc.reason, condition.Reason)
			for _, i := range tc.unavailable {
				require.False(t, members[i].Up)
				require.Nil(t, members[i].BackendBytes)
			}
			for i, errors := range tc.statusErrors {
				require.Contains(t, condition.Message, members[i].Name)
				for _, err := range errors {
					require.Contains(t, condition.Message, err)
				}
				require.Equal(t, !c.blockedReads[endpoints[i]], members[i].Up)
				require.Equal(t, f.statuses[endpoints[i]].DbSize, *members[i].BackendBytes)
				require.Equal(
					t,
					f.statuses[endpoints[i]].DbSizeInUse,
					*members[i].BackendInUseBytes,
				)
				require.Equal(t, f.statuses[endpoints[i]].Header.Revision, *members[i].Revision)
			}
			require.Empty(t, f.defragged)
			require.Empty(t, f.moved)
		})
	}
}

func TestHealthObservationRecoversAfterEtcdAlarm(t *testing.T) {
	r, ts, f := maintenanceFixture(t)
	for _, alarmed := range []bool{false, true, false} {
		status := f.statuses[maintenanceEndpoints(ts)[0]]
		status.Errors = nil
		want, reason := metav1.ConditionTrue, "QuorumAvailable"
		if alarmed {
			status.Errors = []string{"NOSPACE"}
			want, reason = metav1.ConditionFalse, "EtcdStatusError"
		}
		_, err := r.reconcileHealth(
			t.Context(),
			ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ts)},
		)
		require.NoError(t, err)
		fresh := &multigresv1alpha1.TopoServer{}
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh))
		condition := meta.FindStatusCondition(fresh.Status.Conditions, "QuorumAvailable")
		require.NotNil(t, condition)
		require.Equal(t, want, condition.Status)
		require.Equal(t, reason, condition.Reason)
		if !alarmed {
			require.NotContains(t, condition.Message, "NOSPACE")
		}
	}
}

func TestHealthObservationDuringMaintenance(t *testing.T) {
	r, ts, f := maintenanceFixture(t)
	state := &multigresv1alpha1.EtcdMaintenanceStatus{
		LastAttemptTime: metav1.Now(),
		Endpoint:        maintenanceEndpoints(ts)[0],
		InProgress:      true,
	}
	require.NoError(t, r.saveMaintenance(t.Context(), ts, state))
	f.healthErr = errors.New("quorum unavailable")
	result, err := r.reconcileHealth(
		t.Context(),
		ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ts)},
	)
	require.NoError(t, err)
	require.Equal(t, statusRecheckDelay, result.RequeueAfter)
	fresh := &multigresv1alpha1.TopoServer{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(ts), fresh))
	require.Equal(
		t,
		metav1.ConditionFalse,
		meta.FindStatusCondition(fresh.Status.Conditions, "QuorumAvailable").Status,
	)
	require.True(t, fresh.Status.EtcdMaintenance.InProgress)
	require.NotNil(t, fresh.Status.HealthCheckedAt)
}

func TestObserveRunningEtcdPodLimitsAndOOM(t *testing.T) {
	r, ts, _ := maintenanceFixture(t)
	_, members := r.probeHealth(t.Context(), ts)
	pod := &corev1.Pod{}
	require.NoError(
		t,
		r.Get(t.Context(), client.ObjectKey{Namespace: ts.Namespace, Name: members[0].Name}, pod),
	)
	pod.Spec.Containers = []corev1.Container{
		{
			Name: "etcd",
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
			},
			Env: []corev1.EnvVar{{Name: "ETCD_QUOTA_BACKEND_BYTES", Value: "1073741824"}},
		},
	}
	require.NoError(t, r.Update(t.Context(), pod))
	finished := metav1.NewTime(time.Now().Add(-time.Minute))
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name:         "etcd",
			RestartCount: 3,
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Reason:     "OOMKilled",
					FinishedAt: finished,
				},
			},
		},
	}
	require.NoError(t, r.Status().Update(t.Context(), pod))
	r.observeMemberPods(t.Context(), ts, members)
	require.EqualValues(t, 512<<20, *members[0].MemoryLimitBytes)
	require.EqualValues(t, 1<<30, *members[0].QuotaBytes)
	require.EqualValues(t, 3, *members[0].Restarts)
	require.Equal(t, finished.Unix(), *members[0].OOMTimestamp)
}
