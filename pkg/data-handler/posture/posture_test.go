package posture_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"

	"github.com/multigres/testkit/assert"
)

type mockTopoStore struct {
	topoclient.Store
	getMultipoolersByCellFunc func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error)
}

func (m *mockTopoStore) GetMultipoolersByCell(
	ctx context.Context,
	cellName string,
	opt *topoclient.GetMultipoolersByCellOptions,
) ([]*topoclient.MultipoolerInfo, error) {
	if m.getMultipoolersByCellFunc != nil {
		return m.getMultipoolersByCellFunc(ctx, cellName, opt)
	}
	return nil, nil
}

func testShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "0",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}
}

func poolerInfo(
	name string,
	role clustermetadata.RoutingRole,
	lifecycle clustermetadata.PoolerLifecycleStatus,
) *topoclient.MultipoolerInfo {
	mp := &clustermetadata.Multipooler{
		Id:           &clustermetadata.ID{Cell: "cell1", Name: name},
		Hostname:     name,
		RoutingState: &clustermetadata.RoutingState{Role: role},
	}
	if lifecycle != clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN {
		mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{Status: lifecycle}
	}
	return &topoclient.MultipoolerInfo{Multipooler: mp}
}

func withStatus(
	rpc *rpcclient.FakeClient,
	mp *topoclient.MultipoolerInfo,
	s multipoolermanagerdata.PostgresStatus,
) {
	rpc.SetStatusResponse(
		topoclient.ComponentIDString(mp.Id),
		&multipoolermanagerdata.StatusResponse{
			Status: &multipoolermanagerdata.Status{
				PostgresStatus: s,
				IsInitialized:  true,
				PostgresReady:  true,
			},
			AvailabilityStatus: &clustermetadata.AvailabilityStatus{
				CohortEligibilityStatus: &clustermetadata.CohortEligibilityStatus{
					Signal: clustermetadata.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
			ConsensusStatus: &clustermetadata.ConsensusStatus{
				Id: mp.Id,
				CurrentPosition: &clustermetadata.PoolerPosition{
					Position: &clustermetadata.RulePosition{
						Decision: &clustermetadata.ShardRule{
							CohortMembers: []*clustermetadata.ID{mp.Id},
						},
					},
				},
			},
		},
	)
}

func TestEvaluate(t *testing.T) {
	t.Parallel()

	t.Run("consistent postures", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := testShard()

		primary := poolerInfo(
			"primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		replica := poolerInfo(
			"replica-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primary, replica}, nil
			},
		}

		rpc := rpcclient.NewFakeClient()
		withStatus(rpc, primary, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY)
		withStatus(rpc, replica, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_STANDBY)

		result, err := posture.Evaluate(
			context.Background(), store, rpc, shard, []string{"primary-pod", "replica-pod"},
		)
		c.Require().NoError(err, "unexpected error")
		c.Require().NotNil(result, "expected result, got nil")
		c.False(result.MultiplePrimaries, "expected MultiplePrimaries=false")
		c.Empty(result.Mismatches, "expected no mismatches, got")
		c.Eq(1, result.PrimaryCount, "expected PrimaryCount=1, got")
		wantPrimary := result.Postures["primary-pod"] != "PRIMARY"
		wantReplica := result.Postures["replica-pod"] != "STANDBY"
		if wantPrimary || wantReplica {
			t.Errorf("unexpected postures: %v", result.Postures)
		}
		c.Eq("postures consistent with topology roles", result.Message, "unexpected message")
		for _, podName := range []string{"primary-pod", "replica-pod"} {
			if got := result.Readiness[podName]; !got.Ready || got.Reason != "DataPlaneReady" {
				t.Errorf("readiness[%s] = %#v, want data-plane ready", podName, got)
			}
		}
	})

	t.Run("postgres readiness is required", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		replica := poolerInfo(
			"replica-pod", clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{replica}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		withStatus(rpc, replica, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_STANDBY)
		response, _ := rpc.Status(
			t.Context(), replica.Multipooler, &multipoolermanagerdata.StatusRequest{},
		)
		response.Status.PostgresReady = false
		rpc.SetStatusResponse(topoclient.ComponentIDString(replica.Id), response)

		result, err := posture.Evaluate(t.Context(), store, rpc, shard, []string{"replica-pod"})
		assert.NewAborting(t).NoError(err, "unexpected error")
		if got := result.Readiness["replica-pod"]; got.Ready || got.Reason != "PostgresNotReady" {
			t.Errorf("readiness = %#v, want PostgresNotReady", got)
		}
	})

	t.Run("cohort membership is required", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		replica := poolerInfo(
			"replica-pod", clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{replica}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		withStatus(rpc, replica, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_STANDBY)
		response, _ := rpc.Status(
			t.Context(), replica.Multipooler, &multipoolermanagerdata.StatusRequest{},
		)
		response.ConsensusStatus.CurrentPosition.Position.Decision.CohortMembers = nil
		rpc.SetStatusResponse(topoclient.ComponentIDString(replica.Id), response)

		result, err := posture.Evaluate(t.Context(), store, rpc, shard, []string{"replica-pod"})
		assert.NewAborting(t).NoError(err, "unexpected error")
		if got := result.Readiness["replica-pod"]; got.Ready || got.Reason != "NotCohortMember" {
			t.Errorf("readiness = %#v, want NotCohortMember", got)
		}
	})

	t.Run("multiple primaries detected", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := testShard()

		primaryA := poolerInfo(
			"pod-a", clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		staleReplica := poolerInfo(
			"pod-b", clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primaryA, staleReplica}, nil
			},
		}

		rpc := rpcclient.NewFakeClient()
		withStatus(rpc, primaryA, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY)
		withStatus(rpc, staleReplica, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY)

		result, err := posture.Evaluate(
			context.Background(), store, rpc, shard, []string{"pod-a", "pod-b"},
		)
		c.Require().NoError(err, "unexpected error")
		c.Require().NotNil(result, "expected result, got nil")
		c.True(result.MultiplePrimaries, "expected MultiplePrimaries=true")
		c.Eq(2, result.PrimaryCount, "expected PrimaryCount=2, got")
		if len(result.Mismatches) != 1 || result.Mismatches[0] != "pod-b" {
			t.Errorf("expected mismatch [pod-b], got %v", result.Mismatches)
		}
	})

	t.Run("replica reporting primary posture is a mismatch", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := testShard()

		replica := poolerInfo(
			"replica-pod", clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{replica}, nil
			},
		}

		rpc := rpcclient.NewFakeClient()
		withStatus(rpc, replica, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY)

		result, err := posture.Evaluate(
			context.Background(), store, rpc, shard, []string{"replica-pod"},
		)
		c.Require().NoError(err, "unexpected error")
		c.Require().NotNil(result, "expected result, got nil")
		c.False(
			result.MultiplePrimaries,
			"expected MultiplePrimaries=false with only one observed primary",
		)
		if len(result.Mismatches) != 1 || result.Mismatches[0] != "replica-pod" {
			t.Errorf("expected mismatch [replica-pod], got %v", result.Mismatches)
		}
		wantMsg := "pod replica-pod reports postgres primary but topology role is REPLICA"
		c.Eq(wantMsg, result.Message, "unexpected message: got")
	})

	t.Run("promoting is not a mismatch", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := testShard()

		replica := poolerInfo(
			"replica-pod", clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{replica}, nil
			},
		}

		rpc := rpcclient.NewFakeClient()
		withStatus(rpc, replica, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PROMOTING)

		result, err := posture.Evaluate(
			context.Background(), store, rpc, shard, []string{"replica-pod"},
		)
		c.Require().NoError(err, "unexpected error")
		c.Require().NotNil(result, "expected result, got nil")
		c.Empty(result.Mismatches, "expected no mismatches during promotion transition, got")
		c.Eq("PROMOTING", result.Postures["replica-pod"], "expected posture PROMOTING, got")
	})

	t.Run("RPC error records UNKNOWN without false positive", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := testShard()

		replica := poolerInfo(
			"replica-pod", clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{replica}, nil
			},
		}

		rpc := rpcclient.NewFakeClient()
		rpc.Errors[topoclient.ComponentIDString(replica.Id)] = fmt.Errorf("fake rpc failure")

		result, err := posture.Evaluate(
			context.Background(), store, rpc, shard, []string{"replica-pod"},
		)
		c.Require().NoError(err, "unexpected error")
		c.Require().NotNil(result, "expected result, got nil")
		c.Eq(
			"UNKNOWN",
			result.Postures["replica-pod"],
			"expected UNKNOWN posture on RPC error, got",
		)
		c.Empty(result.Mismatches, "expected no mismatches, got")
		c.False(result.MultiplePrimaries, "expected MultiplePrimaries=false")
		c.True(result.Incomplete, "expected RPC failure to mark observation incomplete")
	})

	t.Run("unavailable topology cell returns incomplete observation", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := testShard()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, fmt.Errorf("Code: UNAVAILABLE")
			},
		}

		result, err := posture.Evaluate(
			context.Background(), store, rpcclient.NewFakeClient(), shard, nil,
		)
		c.Require().NoError(err, "unexpected error")
		c.Require().
			False(result == nil || !result.Incomplete, "expected incomplete result, got %#v", result)
		c.Eq("posture observation incomplete", result.Message, "unexpected message")
	})

	t.Run("shutdown pooler is skipped", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := testShard()

		dead := poolerInfo(
			"dead-pod", clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{dead}, nil
			},
		}

		rpc := rpcclient.NewFakeClient()
		withStatus(rpc, dead, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY)

		result, err := posture.Evaluate(
			context.Background(), store, rpc, shard, []string{"dead-pod"},
		)
		c.Require().NoError(err, "unexpected error")
		c.Nil(result, "expected nil result when only pooler is shut down, got")
	})

	t.Run("no poolers matched returns nil result", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := testShard()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, nil
			},
		}
		rpc := rpcclient.NewFakeClient()

		result, err := posture.Evaluate(context.Background(), store, rpc, shard, nil)
		c.Require().NoError(err, "unexpected error")
		c.Nil(result, "expected nil result, got")
	})

	t.Run("non-unavailable topo error is returned", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, fmt.Errorf("fake topo list error")
			},
		}
		rpc := rpcclient.NewFakeClient()

		_, err := posture.Evaluate(context.Background(), store, rpc, shard, nil)
		assert.NewCollecting(t).Error(err, "expected error, got nil")
	})
}

func TestApply(t *testing.T) {
	t.Parallel()

	t.Run("sets consistent condition", func(t *testing.T) {
		t.Parallel()
		ck := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{ObjectMeta: metav1.ObjectMeta{Generation: 3}}
		result := &posture.Result{
			Postures: map[string]string{"pod-a": "PRIMARY"},
			Message:  "postures consistent with topology roles",
		}

		posture.Apply(shard, result)

		ck.Require().
			Len(shard.Status.Conditions, 1, "expected 1 condition, got %d", len(shard.Status.Conditions))
		c := shard.Status.Conditions[0]
		ck.Eq(posture.ConditionConsistent, c.Type, "expected type")
		ck.Eq(metav1.ConditionTrue, c.Status, "expected True, got")
		ck.Eq("Consistent", c.Reason, "expected reason Consistent, got")
		ck.Eq(
			"PRIMARY",
			shard.Status.PodPostures["pod-a"],
			"expected PodPostures to be set, got %v",
			shard.Status.PodPostures,
		)
	})

	t.Run("sets MultiplePrimaries condition, takes priority over mismatches", func(t *testing.T) {
		t.Parallel()
		ck := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{}
		result := &posture.Result{
			Postures:          map[string]string{"pod-a": "PRIMARY", "pod-b": "PRIMARY"},
			MultiplePrimaries: true,
			Mismatches:        []string{"pod-b"},
			Message:           "observed 2 write-capable primaries: [pod-a pod-b]",
		}

		posture.Apply(shard, result)

		c := shard.Status.Conditions[0]
		ck.Eq(metav1.ConditionFalse, c.Status, "expected False, got")
		ck.Eq("MultiplePrimaries", c.Reason, "expected reason MultiplePrimaries, got")
	})

	t.Run("sets RoleMismatch condition", func(t *testing.T) {
		t.Parallel()
		ck := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{}
		result := &posture.Result{
			Postures:   map[string]string{"pod-a": "PRIMARY"},
			Mismatches: []string{"pod-a"},
			Message:    "pod pod-a reports postgres primary but topology role is REPLICA",
		}

		posture.Apply(shard, result)

		c := shard.Status.Conditions[0]
		ck.Eq(metav1.ConditionFalse, c.Status, "expected False, got")
		ck.Eq("RoleMismatch", c.Reason, "expected reason RoleMismatch, got")
	})

	t.Run("incomplete observation sets Unknown instead of consistent", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{}
		posture.Apply(shard, &posture.Result{
			Postures:   map[string]string{"pod-a": "UNKNOWN"},
			Incomplete: true,
			Message:    "posture observation incomplete",
		})

		condition := shard.Status.Conditions[0]
		c.Eq(metav1.ConditionUnknown, condition.Status, "expected Unknown, got")
		c.Eq("ObservationIncomplete", condition.Reason, "expected ObservationIncomplete, got")
	})

	t.Run("incomplete observation preserves confirmed failure", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{Status: multigresv1alpha1.ShardStatus{
			Conditions: []metav1.Condition{{
				Type:    posture.ConditionConsistent,
				Status:  metav1.ConditionFalse,
				Reason:  "MultiplePrimaries",
				Message: "confirmed split brain",
			}},
		}}

		posture.Apply(shard, &posture.Result{
			Postures:   map[string]string{"pod-a": "UNKNOWN"},
			Incomplete: true,
			Message:    "posture observation incomplete",
		})

		condition := shard.Status.Conditions[0]
		c.False(
			condition.Status != metav1.ConditionFalse || condition.Reason != "MultiplePrimaries",
			"expected existing failure to remain, got %#v",
			condition,
		)
		c.Eq(
			"UNKNOWN",
			shard.Status.PodPostures["pod-a"],
			"expected latest posture visibility, got %v",
			shard.Status.PodPostures,
		)
	})

	t.Run(
		"definite mismatch remains actionable when observation is incomplete",
		func(t *testing.T) {
			t.Parallel()
			shard := &multigresv1alpha1.Shard{}
			posture.Apply(shard, &posture.Result{
				Postures:   map[string]string{"pod-a": "PRIMARY", "pod-b": "UNKNOWN"},
				Mismatches: []string{"pod-a"},
				Incomplete: true,
				Message:    "pod pod-a reports postgres primary but topology role is REPLICA",
			})

			condition := shard.Status.Conditions[0]
			assert.NewCollecting(t).
				False(condition.Status != metav1.ConditionFalse || condition.Reason != "RoleMismatch", "expected definite mismatch failure, got %#v", condition)
		},
	)

	t.Run("nil result is no-op", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{}
		posture.Apply(shard, nil)
		c.Empty(shard.Status.Conditions, "expected no conditions for nil result")
		c.Nil(shard.Status.PodPostures, "expected PodPostures to remain nil for nil result")
	})
}
