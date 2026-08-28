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
			Status: &multipoolermanagerdata.Status{PostgresStatus: s},
		},
	)
}

func TestEvaluate(t *testing.T) {
	t.Parallel()

	t.Run("consistent postures", func(t *testing.T) {
		t.Parallel()
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
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected result, got nil")
		}
		if result.MultiplePrimaries {
			t.Error("expected MultiplePrimaries=false")
		}
		if len(result.Mismatches) != 0 {
			t.Errorf("expected no mismatches, got %v", result.Mismatches)
		}
		if result.PrimaryCount != 1 {
			t.Errorf("expected PrimaryCount=1, got %d", result.PrimaryCount)
		}
		wantPrimary := result.Postures["primary-pod"] != "PRIMARY"
		wantReplica := result.Postures["replica-pod"] != "STANDBY"
		if wantPrimary || wantReplica {
			t.Errorf("unexpected postures: %v", result.Postures)
		}
		if result.Message != "postures consistent with topology roles" {
			t.Errorf("unexpected message: %s", result.Message)
		}
	})

	t.Run("multiple primaries detected", func(t *testing.T) {
		t.Parallel()
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
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected result, got nil")
		}
		if !result.MultiplePrimaries {
			t.Error("expected MultiplePrimaries=true")
		}
		if result.PrimaryCount != 2 {
			t.Errorf("expected PrimaryCount=2, got %d", result.PrimaryCount)
		}
		if len(result.Mismatches) != 1 || result.Mismatches[0] != "pod-b" {
			t.Errorf("expected mismatch [pod-b], got %v", result.Mismatches)
		}
	})

	t.Run("replica reporting primary posture is a mismatch", func(t *testing.T) {
		t.Parallel()
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
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected result, got nil")
		}
		if result.MultiplePrimaries {
			t.Error("expected MultiplePrimaries=false with only one observed primary")
		}
		if len(result.Mismatches) != 1 || result.Mismatches[0] != "replica-pod" {
			t.Errorf("expected mismatch [replica-pod], got %v", result.Mismatches)
		}
		wantMsg := "pod replica-pod reports postgres primary but topology role is REPLICA"
		if result.Message != wantMsg {
			t.Errorf("unexpected message: got %q, want %q", result.Message, wantMsg)
		}
	})

	t.Run("promoting is not a mismatch", func(t *testing.T) {
		t.Parallel()
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
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected result, got nil")
		}
		if len(result.Mismatches) != 0 {
			t.Errorf(
				"expected no mismatches during promotion transition, got %v",
				result.Mismatches,
			)
		}
		if result.Postures["replica-pod"] != "PROMOTING" {
			t.Errorf("expected posture PROMOTING, got %s", result.Postures["replica-pod"])
		}
	})

	t.Run("RPC error records UNKNOWN without false positive", func(t *testing.T) {
		t.Parallel()
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
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil {
			t.Fatal("expected result, got nil")
		}
		if result.Postures["replica-pod"] != "UNKNOWN" {
			t.Errorf(
				"expected UNKNOWN posture on RPC error, got %s",
				result.Postures["replica-pod"],
			)
		}
		if len(result.Mismatches) != 0 {
			t.Errorf("expected no mismatches, got %v", result.Mismatches)
		}
		if result.MultiplePrimaries {
			t.Error("expected MultiplePrimaries=false")
		}
		if !result.Incomplete {
			t.Error("expected RPC failure to mark observation incomplete")
		}
	})

	t.Run("unavailable topology cell returns incomplete observation", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, fmt.Errorf("Code: UNAVAILABLE")
			},
		}

		result, err := posture.Evaluate(
			context.Background(), store, rpcclient.NewFakeClient(), shard, nil,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result == nil || !result.Incomplete {
			t.Fatalf("expected incomplete result, got %#v", result)
		}
		if result.Message != "posture observation incomplete" {
			t.Errorf("unexpected message: %q", result.Message)
		}
	})

	t.Run("shutdown pooler is skipped", func(t *testing.T) {
		t.Parallel()
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
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result when only pooler is shut down, got %v", result)
		}
	})

	t.Run("no poolers matched returns nil result", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, nil
			},
		}
		rpc := rpcclient.NewFakeClient()

		result, err := posture.Evaluate(context.Background(), store, rpc, shard, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil result, got %v", result)
		}
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
		if err == nil {
			t.Error("expected error, got nil")
		}
	})
}

func TestApply(t *testing.T) {
	t.Parallel()

	t.Run("sets consistent condition", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{ObjectMeta: metav1.ObjectMeta{Generation: 3}}
		result := &posture.Result{
			Postures: map[string]string{"pod-a": "PRIMARY"},
			Message:  "postures consistent with topology roles",
		}

		posture.Apply(shard, result)

		if len(shard.Status.Conditions) != 1 {
			t.Fatalf("expected 1 condition, got %d", len(shard.Status.Conditions))
		}
		c := shard.Status.Conditions[0]
		if c.Type != posture.ConditionConsistent {
			t.Errorf("expected type %s, got %s", posture.ConditionConsistent, c.Type)
		}
		if c.Status != metav1.ConditionTrue {
			t.Errorf("expected True, got %s", c.Status)
		}
		if c.Reason != "Consistent" {
			t.Errorf("expected reason Consistent, got %s", c.Reason)
		}
		if shard.Status.PodPostures["pod-a"] != "PRIMARY" {
			t.Errorf("expected PodPostures to be set, got %v", shard.Status.PodPostures)
		}
	})

	t.Run("sets MultiplePrimaries condition, takes priority over mismatches", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		result := &posture.Result{
			Postures:          map[string]string{"pod-a": "PRIMARY", "pod-b": "PRIMARY"},
			MultiplePrimaries: true,
			Mismatches:        []string{"pod-b"},
			Message:           "observed 2 write-capable primaries: [pod-a pod-b]",
		}

		posture.Apply(shard, result)

		c := shard.Status.Conditions[0]
		if c.Status != metav1.ConditionFalse {
			t.Errorf("expected False, got %s", c.Status)
		}
		if c.Reason != "MultiplePrimaries" {
			t.Errorf("expected reason MultiplePrimaries, got %s", c.Reason)
		}
	})

	t.Run("sets RoleMismatch condition", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		result := &posture.Result{
			Postures:   map[string]string{"pod-a": "PRIMARY"},
			Mismatches: []string{"pod-a"},
			Message:    "pod pod-a reports postgres primary but topology role is REPLICA",
		}

		posture.Apply(shard, result)

		c := shard.Status.Conditions[0]
		if c.Status != metav1.ConditionFalse {
			t.Errorf("expected False, got %s", c.Status)
		}
		if c.Reason != "RoleMismatch" {
			t.Errorf("expected reason RoleMismatch, got %s", c.Reason)
		}
	})

	t.Run("incomplete observation sets Unknown instead of consistent", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		posture.Apply(shard, &posture.Result{
			Postures:   map[string]string{"pod-a": "UNKNOWN"},
			Incomplete: true,
			Message:    "posture observation incomplete",
		})

		condition := shard.Status.Conditions[0]
		if condition.Status != metav1.ConditionUnknown {
			t.Errorf("expected Unknown, got %s", condition.Status)
		}
		if condition.Reason != "ObservationIncomplete" {
			t.Errorf("expected ObservationIncomplete, got %s", condition.Reason)
		}
	})

	t.Run("incomplete observation preserves confirmed failure", func(t *testing.T) {
		t.Parallel()
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
		if condition.Status != metav1.ConditionFalse || condition.Reason != "MultiplePrimaries" {
			t.Errorf("expected existing failure to remain, got %#v", condition)
		}
		if shard.Status.PodPostures["pod-a"] != "UNKNOWN" {
			t.Errorf("expected latest posture visibility, got %v", shard.Status.PodPostures)
		}
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
			if condition.Status != metav1.ConditionFalse || condition.Reason != "RoleMismatch" {
				t.Errorf("expected definite mismatch failure, got %#v", condition)
			}
		},
	)

	t.Run("nil result is no-op", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		posture.Apply(shard, nil)
		if len(shard.Status.Conditions) != 0 {
			t.Error("expected no conditions for nil result")
		}
		if shard.Status.PodPostures != nil {
			t.Error("expected PodPostures to remain nil for nil result")
		}
	})
}
