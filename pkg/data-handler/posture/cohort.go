package posture

import (
	"context"
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/proto"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

// ErrConfirmedCohortMember means the authoritative committed rule still
// contains the target pooler.
var ErrConfirmedCohortMember = errors.New("pooler is a confirmed cohort member")

// CheckCohortAbsence confirms that poolerID is absent from the authoritative
// committed cohort. Missing or conflicting evidence blocks deletion.
func CheckCohortAbsence(
	ctx context.Context,
	store topoclient.Store,
	rpc rpcclient.MultipoolerClient,
	shard *multigresv1alpha1.Shard,
	poolerID *clustermetadatapb.ID,
) error {
	var leader *clustermetadatapb.ID
	var rule *clustermetadatapb.ShardRule
	for _, cell := range topo.CollectCells(shard) {
		poolers, err := store.GetMultipoolersByCell(ctx, cell, topo.ShardFilter(shard))
		if err != nil {
			return fmt.Errorf("observe cell %s: %w", cell, err)
		}
		for _, pooler := range poolers {
			if pooler.Id == nil || !topo.IsPrimaryPooler(pooler.Multipooler) {
				continue
			}
			rpcCtx, cancel := context.WithTimeout(ctx, statusRPCTimeout)
			resp, err := rpc.Status(
				rpcCtx,
				pooler.Multipooler,
				&multipoolermanagerdatapb.StatusRequest{},
			)
			cancel()
			if err != nil {
				continue // An unreachable pooler cannot attest to the rule.
			}
			if !proto.Equal(resp.GetConsensusStatus().GetId(), pooler.Id) {
				continue // Do not trust an identity mismatch.
			}
			if resp.GetStatus().GetPostgresStatus() !=
				multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY {
				continue
			}
			position := resp.GetConsensusStatus().GetCurrentPosition().GetPosition()
			if position.GetProposal() != nil {
				return fmt.Errorf("primary has an unsettled consensus rule")
			}
			candidateRule := position.GetDecision()
			if !proto.Equal(candidateRule.GetLeaderId(), pooler.Id) {
				continue
			}
			if leader != nil {
				return fmt.Errorf("primary observations disagree")
			}
			leader = pooler.Id
			rule = candidateRule
		}
	}
	if leader == nil || rule.GetRuleNumber() == nil {
		return fmt.Errorf("cannot establish a committed rule from reachable poolers")
	}
	// Match on Cell and Name (the pooler's service ID), Component is ignored so
	// a caller-built ID cannot silently miss a real member.
	for _, member := range rule.GetCohortMembers() {
		if member.GetCell() == poolerID.GetCell() && member.GetName() == poolerID.GetName() {
			return fmt.Errorf(
				"%w: pooler %s under rule %v",
				ErrConfirmedCohortMember,
				topoclient.ClusterIDString(poolerID),
				rule.GetRuleNumber(),
			)
		}
	}
	return nil
}
