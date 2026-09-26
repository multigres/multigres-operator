package posture_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/proto"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"

	"github.com/multigres/testkit/assert"
)

func TestCheckDisruption(t *testing.T) {
	for _, tc := range []struct {
		name          string
		change        func([]*md.StatusResponse, *[]string)
		wantError     bool
		cells         []string
		targetPrimary bool
		stalePrimary  bool
		rpcError      int
		topoError     bool
		leadership    *cm.LeadershipStatus
		unregistered  bool
		unscheduled   bool
		stillInCohort bool
	}{
		{name: "healthy primary omits leadership status"},
		{name: "unregistered unscheduled extra can be removed", unregistered: true, unscheduled: true},
		{name: "unregistered scheduled extra remains blocked", unregistered: true, wantError: true},
		{name: "unregistered unscheduled pod still in cohort remains blocked", unregistered: true, unscheduled: true, stillInCohort: true, wantError: true},
		{name: "empty leadership status is not a resignation", leadership: &cm.LeadershipStatus{}},
		{name: "explicit active leadership is optional", leadership: &cm.LeadershipStatus{
			LeaderTerm: 2, Signal: cm.LeadershipSignal_LEADERSHIP_SIGNAL_ACTIVE,
		}},
		{name: "primary removal with surviving quorum", targetPrimary: true},
		{name: "stale primary ordering", targetPrimary: true, stalePrimary: true, wantError: true},
		{name: "two cells survive", cells: []string{"cell1", "cell2", "cell1"}},
		{name: "last member of second cell", cells: []string{"cell1", "cell1", "cell2"}, wantError: true},
		{name: "unreachable survivor", rpcError: 1, wantError: true},
		{name: "unreachable extra can be removed", rpcError: 2},
		{name: "extra with no consensus status can be removed", change: func(r []*md.StatusResponse, _ *[]string) {
			r[2].ConsensusStatus = nil
		}},
		{name: "lagging extra can be removed", change: func(r []*md.StatusResponse, _ *[]string) {
			r[2].ConsensusStatus.CurrentPosition.Position.Decision.RuleNumber.CoordinatorTerm--
		}},
		{name: "topology unavailable", topoError: true, wantError: true},
		{name: "deleted member still in committed cohort", wantError: true, change: func(_ []*md.StatusResponse, names *[]string) { *names = (*names)[:1] }},
		{name: "no primary", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[0].Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_STANDBY
		}},
		{name: "two primaries", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[1].Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_PRIMARY
		}},
		{name: "primary resigning in current term", wantError: true, leadership: &cm.LeadershipStatus{
			LeaderTerm: 2, Signal: cm.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
		}},
		{name: "resignation from previous term is stale", leadership: &cm.LeadershipStatus{
			LeaderTerm: 1, Signal: cm.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
		}},
		{name: "resignation without a term is not a current term signal", leadership: &cm.LeadershipStatus{
			Signal: cm.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
		}},
		{name: "ineligible primary without leadership signal", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[0].AvailabilityStatus.CohortEligibilityStatus.Signal = cm.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE
		}},
		{name: "ineligibility still blocks with stale resignation", wantError: true, leadership: &cm.LeadershipStatus{
			LeaderTerm: 1, Signal: cm.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
		}, change: func(r []*md.StatusResponse, _ *[]string) {
			r[0].AvailabilityStatus.CohortEligibilityStatus.Signal = cm.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE
		}},
		{name: "missing primary details still blocks", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[0].Status.PrimaryStatus = nil
		}},
		{name: "missing availability is not a healthy primary", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[0].AvailabilityStatus = nil
		}},
		{name: "primary identity disagrees with committed rule", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[0].ConsensusStatus.CurrentPosition.Position.Decision.LeaderId = r[1].ConsensusStatus.Id
		}},
		{name: "primary has pending proposal", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			p := r[0].ConsensusStatus.CurrentPosition.Position
			p.Proposal = proto.Clone(p.Decision).(*cm.ShardRule)
		}},
		{name: "primary not serving", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) { r[0].Status.PrimaryStatus.Ready = false }},
		{name: "follower disconnected", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) { r[0].Status.PrimaryStatus.ConnectedFollowers = nil }},
		{name: "cohort not converged", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[1].ConsensusStatus.CurrentPosition.Position.Decision.RuleNumber.CoordinatorTerm++
		}},
		{name: "pending proposal", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			p := r[1].ConsensusStatus.CurrentPosition.Position
			p.Proposal = proto.Clone(p.Decision).(*cm.ShardRule)
		}},
		{name: "pending proposal on target", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			p := r[2].ConsensusStatus.CurrentPosition.Position
			p.Proposal = proto.Clone(p.Decision).(*cm.ShardRule)
		}},
		{name: "target has newer committed rule", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[2].ConsensusStatus.CurrentPosition.Position.Decision.RuleNumber.CoordinatorTerm++
		}},
		{name: "rewind recovery incomplete", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[1].ConsensusStatus.RecruitBlockedUntil = &cm.LsnPosition{}
		}},
		{name: "missing status", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) { r[1].Status = nil }},
		{name: "survivor not yet a standby", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			r[1].Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_UNKNOWN
		}},
		{name: "two members cannot lose another", wantError: true, change: func(r []*md.StatusResponse, _ *[]string) {
			for _, response := range r {
				rule := response.ConsensusStatus.CurrentPosition.Position.Decision
				rule.CohortMembers = []*cm.ID{rule.CohortMembers[0], rule.CohortMembers[2]}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shard := testShard()
			poolers := []*topoclient.MultipoolerInfo{
				poolerInfo(
					"p0",
					cm.RoutingRole_ROUTING_ROLE_PRIMARY,
					cm.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
				),
				poolerInfo(
					"p1",
					cm.RoutingRole_ROUTING_ROLE_REPLICA,
					cm.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
				),
				poolerInfo(
					"p2",
					cm.RoutingRole_ROUTING_ROLE_REPLICA,
					cm.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
				),
			}
			ids := []*cm.ID{poolers[0].Id, poolers[1].Id, poolers[2].Id}
			rule := &cm.ShardRule{
				RuleNumber:       &cm.RuleNumber{CoordinatorTerm: 2},
				LeaderId:         ids[0],
				CohortMembers:    ids,
				DurabilityPolicy: topoclient.AtLeastN(2),
			}
			if tc.cells != nil {
				for i, cell := range tc.cells {
					ids[i].Cell = cell
				}
				rule.DurabilityPolicy = topoclient.MultiCellAtLeastN(2)
				pool := shard.Spec.Pools["default"]
				pool.Cells = []multigresv1alpha1.CellName{"cell1", "cell2"}
				shard.Spec.Pools["default"] = pool
			}
			responses := make([]*md.StatusResponse, 3)
			for i := range responses {
				responses[i] = &md.StatusResponse{
					Status: &md.Status{
						IsInitialized:  true,
						PostgresReady:  true,
						PostgresStatus: md.PostgresStatus_POSTGRES_STATUS_STANDBY,
					},
					AvailabilityStatus: &cm.AvailabilityStatus{
						CohortEligibilityStatus: &cm.CohortEligibilityStatus{
							Signal: cm.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
						},
					},
					ConsensusStatus: &cm.ConsensusStatus{
						Id: ids[i],
						CurrentPosition: &cm.PoolerPosition{
							Position: &cm.RulePosition{Decision: proto.Clone(rule).(*cm.ShardRule)},
						},
					},
				}
			}
			responses[0].Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_PRIMARY
			responses[0].Status.PrimaryStatus = &md.PrimaryStatus{
				Ready:              true,
				ConnectedFollowers: ids[1:],
			}
			// The pinned ConsensusManager.LeadershipStatus returns nil when no
			// resignation is recorded. Only signal-specific cases set this field.
			responses[0].AvailabilityStatus.LeadershipStatus = tc.leadership
			names := []string{"p0", "p1", "p2"}
			if tc.change != nil {
				tc.change(responses, &names)
			}
			if tc.unregistered {
				poolers = poolers[:2]
				if !tc.stillInCohort {
					for _, response := range responses {
						rule := response.ConsensusStatus.CurrentPosition.Position.Decision
						rule.CohortMembers = rule.CohortMembers[:2]
					}
				}
			}
			rpc := rpcclient.NewFakeClient()
			for i, p := range poolers {
				rpc.SetStatusResponse(topoclient.ComponentIDString(p.Id), responses[i])
			}
			if tc.rpcError > 0 {
				rpc.Errors[topoclient.ComponentIDString(ids[tc.rpcError])] = fmt.Errorf(
					"unreachable",
				)
			}
			store := &mockTopoStore{
				getMultipoolersByCellFunc: func(_ context.Context, cell string, _ *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
					if tc.topoError {
						return nil, fmt.Errorf("unreachable")
					}
					var result []*topoclient.MultipoolerInfo
					for _, p := range poolers {
						if p.Id.Cell == cell {
							result = append(result, p)
						}
					}
					return result, nil
				},
			}
			target := "p2"
			if tc.targetPrimary {
				target = "p0"
				if !tc.stalePrimary {
					shard.Status.PodRoles = map[string]string{"p0": "PRIMARY"}
				}
			}
			err := posture.CheckDisruption(
				t.Context(),
				store,
				rpc,
				shard,
				names,
				posture.DisruptionTarget{
					Name: target, Unscheduled: tc.unscheduled,
				},
			)
			assert.NewAborting(t).ErrorWhen(tc.wantError, err, "CheckDisruption()")
		})
	}
}
