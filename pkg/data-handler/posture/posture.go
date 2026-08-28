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
	MultiplePrimaries bool
	Mismatches        []string
	PrimaryCount      int
	// Incomplete reports that at least one cell or pooler was not observed.
	Incomplete bool
	Message    string
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
			postures[podName] = observePosture(ctx, rpcClient, p.Multipooler)
			if postures[podName] == "UNKNOWN" {
				incomplete = true
			}
		}
	}

	if len(postures) == 0 && !incomplete {
		return nil, nil
	}

	result := &Result{Postures: postures, Incomplete: incomplete}

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

func observePosture(
	ctx context.Context,
	rpcClient rpcclient.MultipoolerClient,
	mp *clustermetadatapb.Multipooler,
) string {
	rpcCtx, cancel := context.WithTimeout(ctx, statusRPCTimeout)
	defer cancel()

	resp, err := rpcClient.Status(rpcCtx, mp, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return "UNKNOWN"
	}
	return postureString(resp.GetStatus().GetPostgresStatus())
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
