//go:build integration

package toposerver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

type mappedHealthClient struct {
	etcdMaintenanceClient
	endpoints map[string]string
}

func (c mappedHealthClient) Status(
	ctx context.Context,
	endpoint string,
) (*clientv3.StatusResponse, error) {
	return c.etcdMaintenanceClient.Status(ctx, c.endpoints[endpoint])
}

func (c mappedHealthClient) Health(ctx context.Context, endpoint string) error {
	return c.etcdMaintenanceClient.Health(ctx, c.endpoints[endpoint])
}
func (c mappedHealthClient) Close() {}

func TestLiveEtcdNOSPACEHealth(t *testing.T) {
	c, endpoints, _ := startTestEtcd(t)
	ts := certTestTopoServer(nil)
	mapping := map[string]string{}
	for i, endpoint := range maintenanceEndpoints(ts) {
		mapping[endpoint] = endpoints[i]
	}
	r := &TopoServerReconciler{
		newMaintenanceClient: func(context.Context, *multigresv1alpha1.TopoServer) (etcdMaintenanceClient, error) {
			return mappedHealthClient{c, mapping}, nil
		},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	status, err := c.Status(ctx, endpoints[0])
	require.NoError(t, err)
	_, err = pb.NewMaintenanceClient(c.first.ActiveConnection()).Alarm(ctx, &pb.AlarmRequest{
		Action:   pb.AlarmRequest_ACTIVATE,
		MemberID: status.Header.MemberId,
		Alarm:    pb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		condition, _ := r.probeHealth(ctx, ts)
		assert.Equal(t, metav1.ConditionFalse, condition.Status)
		assert.Equal(t, "EtcdStatusError", condition.Reason)
		assert.Contains(t, condition.Message, "NOSPACE")
	}, 10*time.Second, 100*time.Millisecond)
	for _, endpoint := range endpoints {
		require.NoError(t, c.Health(ctx, endpoint), "NOSPACE still permits linearizable reads")
		_, err = c.clients[endpoint].Put(ctx, "/health-test", "blocked")
		require.ErrorIs(
			t,
			err,
			rpctypes.ErrNoSpace,
			"a single member alarm blocks writes through every endpoint",
		)
	}
	_, err = c.first.AlarmDisarm(ctx, &clientv3.AlarmMember{
		MemberID: status.Header.MemberId, Alarm: pb.AlarmType_NOSPACE,
	})
	require.NoError(t, err)
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		condition, _ := r.probeHealth(ctx, ts)
		assert.Equal(t, metav1.ConditionTrue, condition.Status)
		assert.Equal(t, "QuorumAvailable", condition.Reason)
		assert.NotContains(t, condition.Message, "NOSPACE")
	}, 10*time.Second, 100*time.Millisecond)
	_, err = c.first.Put(ctx, "/health-test", "recovered")
	require.NoError(t, err)
}

func TestLiveEtcdQuorumHealth(t *testing.T) {
	c, endpoints, stop := startTestEtcd(t)
	ts := certTestTopoServer(nil)
	mapping := map[string]string{}
	for i, endpoint := range maintenanceEndpoints(ts) {
		mapping[endpoint] = endpoints[i]
	}
	r := &TopoServerReconciler{
		newMaintenanceClient: func(context.Context, *multigresv1alpha1.TopoServer) (etcdMaintenanceClient, error) {
			return mappedHealthClient{c, mapping}, nil
		},
	}
	condition, _ := r.probeHealth(t.Context(), ts)
	require.Equal(t, metav1.ConditionTrue, condition.Status)
	stop[2]()
	require.Eventually(t, func() bool {
		condition, _ := r.probeHealth(t.Context(), ts)
		return condition.Status == metav1.ConditionTrue
	}, 20*time.Second, 100*time.Millisecond, "two surviving members retain quorum")
	stop[1]()
	require.Eventually(t, func() bool {
		condition, _ := r.probeHealth(t.Context(), ts)
		return condition.Status == metav1.ConditionFalse && condition.Reason == "QuorumUnavailable"
	}, 20*time.Second, 100*time.Millisecond, "one live member can answer status but cannot serve linearizable reads")
	stop[0]()
	require.Eventually(t, func() bool {
		condition, _ := r.probeHealth(t.Context(), ts)
		return condition.Reason == "TopologyUnreachable"
	}, 20*time.Second, 100*time.Millisecond)
}
