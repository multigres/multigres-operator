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
	Ready bool
	// Observed reports whether this is a real answer from Multigres: a
	// topology entry whose Status RPC succeeded, or a confirmed absence from
	// a cell that was actually listed. It is false for a gap in observation
	// instead: a cell whose listing failed, or a Status RPC that itself
	// failed. Ready is only ever true when Observed is true, so a caller that
	// only checks Ready already treats an unobserved pooler as not ready;
	// reconcilePoolerReadiness additionally reports Unknown rather than False
	// whenever Observed is false, so a lost observation is never confused
	// with Multigres itself reporting the pooler not ready.
	Observed bool
	Reason   string
	Message  string
}

// reasonAwaitingRegistration is the readiness reason carried by a managed pod
// confirmed absent from the shard topology: every cell was listed
// successfully and none of them had a matching pooler.
//
// It is recorded only by the confirmation loop below, once every cell has
// actually been listed. Note this is NOT what Result.Incomplete reports: that
// covers an unreachable cell, a topology entry with no matching pod, or an
// UNKNOWN posture, all of which are the opposite direction.
const reasonAwaitingRegistration = "AwaitingRegistration"

// UnobservedReadiness is the Readiness recorded for a managed pod before
// Evaluate has confirmed it one way or the other. reconcilePoolerReadiness
// uses the same value for a pod it has no Evaluate result for at all (e.g.
// a config-level topology dial error), so both gaps in observation read
// identically on the pod.
var UnobservedReadiness = Readiness{
	Reason:  "ObservationUnavailable",
	Message: "Multigres data-plane readiness has not been observed",
}

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
		readiness[podName] = UnobservedReadiness
	}
	isTopoPrimary := make(map[string]bool)
	incomplete := false
	// cellUnavailable, distinct from incomplete: incomplete also covers an
	// orphaned topology entry or an UNKNOWN posture reading, neither of which
	// says anything about whether an unmatched managed pod was actually
	// checked. cellUnavailable specifically means at least one cell's own
	// listing failed, so a pod that never matched any entry might simply
	// belong to that cell rather than being confirmed absent everywhere.
	// It is shard-global rather than per-cell, so one cell failing its
	// listing reports Unknown even for unmatched pods in other, healthy
	// cells; conservative, since the alternative is guessing which cell an
	// unmatched pod would have belonged to.
	cellUnavailable := false
	matched := make(map[string]bool, len(managedPodNames))

	for _, cell := range topo.CollectCells(shard) {
		poolers, err := store.GetMultipoolersByCell(ctx, cell, topo.ShardFilter(shard))
		if err != nil {
			if topo.IsTopoUnavailable(err) {
				incomplete = true
				cellUnavailable = true
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
			matched[podName] = true
			if postures[podName] == "UNKNOWN" {
				incomplete = true
			}
		}
	}

	// A pod that never matched any topology entry is a confirmed
	// "not registered" fact only once every cell was actually listed. If any
	// cell's own listing failed, an unmatched pod might simply belong to
	// that cell, and reporting it as a confirmed AwaitingRegistration
	// negative would assert something that was never actually checked.
	if !cellUnavailable {
		for _, podName := range managedPodNames {
			if matched[podName] {
				continue
			}
			readiness[podName] = Readiness{
				Observed: true,
				Reason:   reasonAwaitingRegistration,
				Message:  "pooler has not registered in the shard topology",
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
			Observed: false,
			Reason:   "StatusUnavailable",
			Message:  fmt.Sprintf("multipooler status RPC failed: %v", err),
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
			Observed: true,
			Reason:   "NotInitialized",
			Message:  "pooler initialization has not completed",
		}
	}
	if !status.GetPostgresReady() {
		return posture, Readiness{
			Observed: true,
			Reason:   "PostgresNotReady",
			Message:  "PostgreSQL is not accepting connections",
		}
	}
	eligibility := resp.GetAvailabilityStatus().GetCohortEligibilityStatus()
	if eligibility == nil ||
		eligibility.GetSignal() !=
			clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE {
		return posture, Readiness{
			Observed: true,
			Reason:   "CohortIneligible",
			Message:  "pooler is not eligible to participate in the shard cohort",
		}
	}
	if !committedCohortContains(resp, id) {
		return posture, Readiness{
			Observed: true,
			Reason:   "NotCohortMember",
			Message:  "pooler is not a member of its committed shard cohort",
		}
	}
	return posture, Readiness{
		Observed: true,
		Ready:    true,
		Reason:   "DataPlaneReady",
		Message:  "PostgreSQL is ready and the pooler is an eligible shard cohort member",
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
