package posture_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
)

// withCommittedStatus is like withStatus but also sets a RuleNumber and
// LeaderId, which CheckCohortAbsence requires to consider a rule committed
// and its reporter authoritative (withStatus omits them since posture.Evaluate
// does not consult them). leaderID defaults to mp.Id when nil, letting callers
// build a stale/split-brain rule that names a different leader.
func withCommittedStatus(
	rpc *rpcclient.FakeClient,
	mp *topoclient.MultipoolerInfo,
	s multipoolermanagerdata.PostgresStatus,
	leaderID *clustermetadata.ID,
	cohortMembers []*clustermetadata.ID,
) {
	if leaderID == nil {
		leaderID = mp.Id
	}
	rpc.SetStatusResponse(
		topoclient.ComponentIDString(mp.Id),
		&multipoolermanagerdata.StatusResponse{
			Status: &multipoolermanagerdata.Status{PostgresStatus: s},
			ConsensusStatus: &clustermetadata.ConsensusStatus{
				Id: mp.Id,
				CurrentPosition: &clustermetadata.PoolerPosition{
					Position: &clustermetadata.RulePosition{
						Decision: &clustermetadata.ShardRule{
							RuleNumber:    &clustermetadata.RuleNumber{CoordinatorTerm: 1},
							LeaderId:      leaderID,
							CohortMembers: cohortMembers,
						},
					},
				},
			},
		},
	)
}

func TestCheckCohortAbsence(t *testing.T) {
	t.Parallel()

	t.Run("pooler absent from committed rule is safe to delete", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		primary := poolerInfo(
			"primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primary}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		withCommittedStatus(
			rpc, primary, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			nil, []*clustermetadata.ID{primary.Id},
		)

		gone := &clustermetadata.ID{Cell: "cell1", Name: "already-deleted-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, gone,
		); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("pooler still a committed cohort member blocks deletion", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		primary := poolerInfo(
			"primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primary}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		withCommittedStatus(
			rpc, primary, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			nil, []*clustermetadata.ID{primary.Id},
		)
		err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, primary.Id,
		)
		if !errors.Is(err, posture.ErrConfirmedCohortMember) {
			t.Fatalf("expected confirmed-member error, got %v", err)
		}
	})

	t.Run("pending proposal blocks absence confirmation", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		primary := poolerInfo(
			"primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primary}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		rpc.SetStatusResponse(
			topoclient.ComponentIDString(primary.Id),
			&multipoolermanagerdata.StatusResponse{
				Status: &multipoolermanagerdata.Status{
					PostgresStatus: multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
				},
				ConsensusStatus: &clustermetadata.ConsensusStatus{
					Id: primary.Id,
					CurrentPosition: &clustermetadata.PoolerPosition{
						Position: &clustermetadata.RulePosition{
							Decision: &clustermetadata.ShardRule{
								RuleNumber:    &clustermetadata.RuleNumber{CoordinatorTerm: 1},
								LeaderId:      primary.Id,
								CohortMembers: []*clustermetadata.ID{primary.Id},
							},
							Proposal: &clustermetadata.ShardRule{
								RuleNumber: &clustermetadata.RuleNumber{CoordinatorTerm: 2},
								LeaderId:   primary.Id,
							},
						},
					},
				},
			},
		)

		gone := &clustermetadata.ID{Cell: "cell1", Name: "already-deleted-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, gone,
		); err == nil {
			t.Fatal("expected pending proposal to block absence confirmation")
		}
	})

	t.Run("no reachable primary cannot establish a rule", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, nil
			},
		}
		rpc := rpcclient.NewFakeClient()

		id := &clustermetadata.ID{Cell: "cell1", Name: "some-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, id,
		); err == nil {
			t.Fatal("expected error when no committed rule can be established, got nil")
		}
	})

	t.Run("unreachable primary cannot establish a rule", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		primary := poolerInfo(
			"primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primary}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		rpc.Errors[topoclient.ComponentIDString(primary.Id)] = fmt.Errorf("fake rpc failure")

		id := &clustermetadata.ID{Cell: "cell1", Name: "some-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, id,
		); err == nil {
			t.Fatal("expected error when the primary is unreachable, got nil")
		}
	})

	t.Run("consensus identity mismatch is not trusted as leader", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		primary := poolerInfo(
			"primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primary}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		// Response claims a different consensus identity than the topology
		// entry it was fetched for (e.g. a stale/reused RPC target).
		imposter := &clustermetadata.ID{Cell: "cell1", Name: "someone-else"}
		rpc.SetStatusResponse(
			topoclient.ComponentIDString(primary.Id),
			&multipoolermanagerdata.StatusResponse{
				Status: &multipoolermanagerdata.Status{
					PostgresStatus: multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
				},
				ConsensusStatus: &clustermetadata.ConsensusStatus{
					Id: imposter,
					CurrentPosition: &clustermetadata.PoolerPosition{
						Position: &clustermetadata.RulePosition{
							Decision: &clustermetadata.ShardRule{
								RuleNumber:    &clustermetadata.RuleNumber{CoordinatorTerm: 1},
								LeaderId:      primary.Id,
								CohortMembers: []*clustermetadata.ID{primary.Id},
							},
						},
					},
				},
			},
		)

		id := &clustermetadata.ID{Cell: "cell1", Name: "some-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, id,
		); err == nil {
			t.Fatal("expected error: identity mismatch must not be trusted, got nil")
		}
	})

	t.Run("topology role disagreement blocks authority", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		// Reports PostgreSQL PRIMARY over RPC, but topology still has it as a
		// REPLICA — a stale/self-promoted pooler must not authorize deletion.
		staleReplica := poolerInfo(
			"stale-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{staleReplica}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		withCommittedStatus(
			rpc, staleReplica, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			nil, []*clustermetadata.ID{staleReplica.Id},
		)

		id := &clustermetadata.ID{Cell: "cell1", Name: "some-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, id,
		); err == nil {
			t.Fatal(
				"expected error: topology role disagreement must not authorize deletion, got nil",
			)
		}
	})

	t.Run("stale rule naming a different leader blocks authority", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		stale := poolerInfo(
			"stale-primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{stale}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		otherLeader := &clustermetadata.ID{Cell: "cell1", Name: "actual-leader-pod"}
		withCommittedStatus(
			rpc, stale, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			otherLeader, []*clustermetadata.ID{otherLeader},
		)

		id := &clustermetadata.ID{Cell: "cell1", Name: "some-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, id,
		); err == nil {
			t.Fatal("expected error: stale rule.LeaderId mismatch must block authority, got nil")
		}
	})

	t.Run("split-brain: two authoritative primaries disagree", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		primaryA := poolerInfo(
			"pod-a",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		primaryB := poolerInfo(
			"pod-b",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primaryA, primaryB}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		withCommittedStatus(
			rpc, primaryA, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			nil, []*clustermetadata.ID{primaryA.Id},
		)
		withCommittedStatus(
			rpc, primaryB, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			nil, []*clustermetadata.ID{primaryB.Id},
		)

		id := &clustermetadata.ID{Cell: "cell1", Name: "some-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, id,
		); err == nil {
			t.Fatal("expected error on split-brain (two disagreeing primaries), got nil")
		}
	})

	t.Run("membership matches real topology IDs on cell and name", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		primary := poolerInfo(
			"primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{primary}, nil
			},
		}
		// Real members carry Component and a service-ID name, a caller rebuilding the ID must still match them.
		member := &clustermetadata.ID{
			Component: clustermetadata.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "p-0badcafe",
		}
		rpc := rpcclient.NewFakeClient()
		withCommittedStatus(
			rpc, primary, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			nil, []*clustermetadata.ID{primary.Id, member},
		)

		withoutComponent := &clustermetadata.ID{Cell: "cell1", Name: "p-0badcafe"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, withoutComponent,
		); err == nil {
			t.Fatal("expected error: member must match regardless of Component, got nil")
		}
		otherCell := &clustermetadata.ID{Cell: "cell2", Name: "p-0badcafe"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, otherCell,
		); err != nil {
			t.Fatalf("expected nil for same name in another cell, got %v", err)
		}
	})

	t.Run("non-primary poolers are not queried", func(t *testing.T) {
		t.Parallel()
		shard := testShard()
		primary := poolerInfo(
			"primary-pod",
			clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		deadReplica := poolerInfo(
			"dead-replica",
			clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
			clustermetadata.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN,
		)
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				// Unreachable replica listed first must not consume the deadline.
				return []*topoclient.MultipoolerInfo{deadReplica, primary}, nil
			},
		}
		rpc := rpcclient.NewFakeClient()
		rpc.Errors[topoclient.ComponentIDString(deadReplica.Id)] = fmt.Errorf("unreachable")
		withCommittedStatus(
			rpc, primary, multipoolermanagerdata.PostgresStatus_POSTGRES_STATUS_PRIMARY,
			nil, []*clustermetadata.ID{primary.Id},
		)

		gone := &clustermetadata.ID{Cell: "cell1", Name: "already-deleted-pod"}
		if err := posture.CheckCohortAbsence(
			context.Background(), store, rpc, shard, gone,
		); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		for _, call := range rpc.GetCallLog() {
			if strings.Contains(call, string(topoclient.ComponentIDString(deadReplica.Id))) {
				t.Fatalf("replica must not be queried, saw call %q", call)
			}
		}
	})
}
