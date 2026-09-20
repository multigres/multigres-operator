// Package posture compares observed postgres states with topology roles.
package posture

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

// ConditionConsistent is the condition type reporting whether observed
// postgres postures agree with topology-advertised roles.
const ConditionConsistent = "PostureConsistent"

const statusRPCTimeout = 5 * time.Second

// Result holds the computed posture-consistency information.
type Result struct {
	Postures          map[string]string
	Readiness         map[string]Readiness
	MultiplePrimaries bool
	Mismatches        []string
	PrimaryCount      int
	// Incomplete reports that at least one cell or pooler was not observed.
	Incomplete bool
	Message    string
}

// Readiness is the data-plane availability signal projected onto a Kubernetes
// Pod readiness gate. A pooler is ready only when PostgreSQL is accepting
// connections, the pooler is willing to participate, and its committed rule
// includes it in the shard cohort.
type Readiness struct {
	Ready   bool
	Reason  string
	Message string
}

// ReasonAwaitingRegistration is the readiness reason carried by a managed pod
// that has no corresponding pooler in the shard topology.
//
// Every managed pod is seeded with it and only overwritten once a topology
// entry matches, so its survival is the signal that topology is behind the
// pod set. Note this is NOT what Result.Incomplete reports: that covers an
// unreachable cell, a topology entry with no matching pod, or an UNKNOWN
// posture, all of which are the opposite direction.
const ReasonAwaitingRegistration = "AwaitingRegistration"

// Evaluate compares each managed pooler's observed postgres state with its
// topology role. It returns nil when topology contains no active poolers, as
// during bootstrap.
func Evaluate(
	ctx context.Context,
	store topoclient.Store,
	rpcClient rpcclient.MultipoolerClient,
	shard *multigresv1alpha1.Shard,
	managedPodNames []string,
) (*Result, error) {
	postures := make(map[string]string)
	readiness := make(map[string]Readiness, len(managedPodNames))
	for _, podName := range managedPodNames {
		readiness[podName] = Readiness{
			Reason:  ReasonAwaitingRegistration,
			Message: "pooler has not registered in the shard topology",
		}
	}
	isTopoPrimary := make(map[string]bool)
	incomplete := false

	for _, cell := range topo.CollectCells(shard) {
		poolers, err := store.GetMultipoolersByCell(ctx, cell, topo.ShardFilter(shard))
		if err != nil {
			if topo.IsTopoUnavailable(err) {
				incomplete = true
				continue
			}
			return nil, fmt.Errorf("listing poolers in cell %q for posture check: %w", cell, err)
		}

		for _, p := range poolers {
			if isLifecycleShutdown(p.Multipooler) {
				continue
			}
			podName := matchPod(p, managedPodNames)
			if podName == "" {
				incomplete = true
				continue
			}

			isTopoPrimary[podName] = topo.IsPrimaryPooler(p.Multipooler)
			postures[podName], readiness[podName] = observePooler(
				ctx,
				rpcClient,
				p.Multipooler,
			)
			if postures[podName] == "UNKNOWN" {
				incomplete = true
			}
		}
	}

	if len(postures) == 0 && !incomplete {
		return nil, nil
	}

	result := &Result{Postures: postures, Readiness: readiness, Incomplete: incomplete}

	var primaries []string
	for podName, observed := range postures {
		if observed != "PRIMARY" {
			continue
		}
		primaries = append(primaries, podName)
		if !isTopoPrimary[podName] {
			result.Mismatches = append(result.Mismatches, podName)
		}
	}
	slices.Sort(primaries)
	slices.Sort(result.Mismatches)

	result.PrimaryCount = len(primaries)
	result.MultiplePrimaries = result.PrimaryCount > 1

	switch {
	case result.MultiplePrimaries:
		result.Message = fmt.Sprintf(
			"observed %d write-capable primaries: %v", result.PrimaryCount, primaries,
		)
	case len(result.Mismatches) > 0:
		result.Message = mismatchMessage(result.Mismatches)
	case result.Incomplete:
		result.Message = "posture observation incomplete"
	default:
		result.Message = "postures consistent with topology roles"
	}

	return result, nil
}

func mismatchMessage(mismatches []string) string {
	if len(mismatches) == 1 {
		return fmt.Sprintf(
			"pod %s reports postgres primary but topology role is REPLICA", mismatches[0],
		)
	}
	return fmt.Sprintf(
		"pods %s report postgres primary but topology role is REPLICA",
		strings.Join(mismatches, ", "),
	)
}

func observePooler(
	ctx context.Context,
	rpcClient rpcclient.MultipoolerClient,
	mp *clustermetadatapb.Multipooler,
) (string, Readiness) {
	rpcCtx, cancel := context.WithTimeout(ctx, statusRPCTimeout)
	defer cancel()

	resp, err := rpcClient.Status(rpcCtx, mp, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return "UNKNOWN", Readiness{
			Reason:  "StatusUnavailable",
			Message: fmt.Sprintf("multipooler status RPC failed: %v", err),
		}
	}
	return poolerReadiness(resp, mp.GetId())
}

func poolerReadiness(
	resp *multipoolermanagerdatapb.StatusResponse,
	id *clustermetadatapb.ID,
) (string, Readiness) {
	status := resp.GetStatus()
	posture := postureString(status.GetPostgresStatus())
	if !status.GetIsInitialized() {
		return posture, Readiness{
			Reason:  "NotInitialized",
			Message: "pooler initialization has not completed",
		}
	}
	if !status.GetPostgresReady() {
		return posture, Readiness{
			Reason:  "PostgresNotReady",
			Message: "PostgreSQL is not accepting connections",
		}
	}
	eligibility := resp.GetAvailabilityStatus().GetCohortEligibilityStatus()
	if eligibility == nil ||
		eligibility.GetSignal() !=
			clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE {
		return posture, Readiness{
			Reason:  "CohortIneligible",
			Message: "pooler is not eligible to participate in the shard cohort",
		}
	}
	if !committedCohortContains(resp, id) {
		return posture, Readiness{
			Reason:  "NotCohortMember",
			Message: "pooler is not a member of its committed shard cohort",
		}
	}
	return posture, Readiness{
		Ready:   true,
		Reason:  "DataPlaneReady",
		Message: "PostgreSQL is ready and the pooler is an eligible shard cohort member",
	}
}

func committedCohortContains(
	resp *multipoolermanagerdatapb.StatusResponse,
	poolerID *clustermetadatapb.ID,
) bool {
	decision := resp.GetConsensusStatus().
		GetCurrentPosition().
		GetPosition().
		GetDecision()
	if decision == nil || poolerID == nil {
		return false
	}
	poolerKey := topoclient.ClusterIDString(poolerID)
	for _, member := range decision.GetCohortMembers() {
		if topoclient.ClusterIDString(member) == poolerKey {
			return true
		}
	}
	return false
}

func postureString(s multipoolermanagerdatapb.PostgresStatus) string {
	switch s {
	case multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY:
		return "PRIMARY"
	case multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY:
		return "STANDBY"
	case multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING:
		return "PROMOTING"
	case multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STARTING:
		return "STARTING"
	default:
		return "UNKNOWN"
	}
}

func matchPod(p *topoclient.MultipoolerInfo, podNames []string) string {
	for _, name := range podNames {
		if topo.PodMatchesPooler(name, p) {
			return name
		}
	}
	return ""
}

func isLifecycleShutdown(mp *clustermetadatapb.Multipooler) bool {
	return mp.GetLifecycleStatus().GetStatus() ==
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN
}

// Apply copies observed postures and their consistency condition to status.
func Apply(shard *multigresv1alpha1.Shard, result *Result) {
	if result == nil {
		return
	}

	shard.Status.PodPostures = result.Postures

	condition := metav1.Condition{
		Type:               ConditionConsistent,
		ObservedGeneration: shard.Generation,
		LastTransitionTime: metav1.Now(),
		Message:            result.Message,
	}

	switch {
	case result.MultiplePrimaries:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "MultiplePrimaries"
	case len(result.Mismatches) > 0:
		condition.Status = metav1.ConditionFalse
		condition.Reason = "RoleMismatch"
	case result.Incomplete:
		// A partial scan cannot clear a confirmed failure.
		if status.IsConditionFalse(shard.Status.Conditions, ConditionConsistent) {
			return
		}
		condition.Status = metav1.ConditionUnknown
		condition.Reason = "ObservationIncomplete"
	default:
		condition.Status = metav1.ConditionTrue
		condition.Reason = "Consistent"
	}

	status.SetCondition(&shard.Status.Conditions, condition)
}
