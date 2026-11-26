package resolver

import (
	"fmt"
	"sort"

	"github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// MinFailureSafePoolers is the smallest shard cohort that multiorch will
// bootstrap under the supported durability policies. Since
// multigres/multigres#1349, multiorch only initializes a shard whose initial
// cohort still satisfies the policy after losing any one pooler, so the
// 2 poolers that merely satisfy AT_LEAST_2 are not enough.
const MinFailureSafePoolers = 3

// failureSafeReplicasPerCell returns the replicasPerCell a pool spread over
// cells needs for the shard to bootstrap on its own: 3 in one cell, 2 per
// cell across two cells, and 1 per cell from three cells up.
func failureSafeReplicasPerCell(cells int) int32 {
	if cells <= 0 {
		return DefaultPoolReplicasPerCell
	}
	n := DefaultPoolReplicasPerCell
	for int(n)*cells < MinFailureSafePoolers {
		n++
	}
	return n
}

// durabilityPolicyProto mirrors the policy names the topology registration
// writes for multiorch (pkg/data-handler/topo).
func durabilityPolicyProto(name string) (*clustermetadatapb.DurabilityPolicy, error) {
	p := &clustermetadatapb.DurabilityPolicy{PolicyName: name, RequiredCount: 2}
	switch name {
	case "AT_LEAST_2":
		p.QuorumType = clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N
	case "MULTI_CELL_AT_LEAST_2":
		p.QuorumType = clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N
	default:
		return nil, fmt.Errorf("unsupported durability policy %q", name)
	}
	return p, nil
}

// cohortIsFailureSafe reports whether the poolers the resolved pools would
// create form a cohort that multiorch's own rule accepts for bootstrap.
// Pool member names are synthetic; only cells and counts matter to the rule.
func cohortIsFailureSafe(
	policyName string,
	pools map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec,
) (bool, error) {
	pb, err := durabilityPolicyProto(policyName)
	if err != nil {
		return false, err
	}
	policy, err := consensus.NewPolicyFromProto(pb)
	if err != nil {
		return false, err
	}
	names := make([]string, 0, len(pools))
	for name := range pools {
		names = append(names, string(name))
	}
	sort.Strings(names)
	var cohort []*clustermetadatapb.ID
	for _, name := range names {
		pool := pools[multigresv1alpha1.PoolName(name)]
		replicas := DefaultPoolReplicasPerCell
		if pool.ReplicasPerCell != nil {
			replicas = *pool.ReplicasPerCell
		}
		for _, cell := range pool.Cells {
			for i := int32(0); i < replicas; i++ {
				cohort = append(cohort, &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      string(cell),
					Name:      fmt.Sprintf("%s-%d", name, i),
				})
			}
		}
	}
	return consensus.CohortSurvivesAnyMemberLoss(policy, cohort), nil
}
