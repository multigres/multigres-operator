package topo

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// deadPoolerReason is recorded on the pooler's lifecycle entry when the
// operator marks it shut down because no backing pod exists.
const deadPoolerReason = "operator: no backing pod for pooler"

// PoolerStatusResult holds the result of querying pooler roles from the topology.
type PoolerStatusResult struct {
	// Roles maps hostname to its operator-facing role
	// (PRIMARY, REPLICA, DRAINED).
	Roles map[string]string
	// QuerySuccess indicates whether all topology queries succeeded.
	QuerySuccess bool
}

// GetPoolerStatus queries the topology for all poolers belonging to a shard
// and returns a map of pod name to role string. managedPodNames is the set of
// Kubernetes pod names currently managed by the shard controller; topology
// entries are matched to pods via PodMatchesPooler (FQDN-aware).
func GetPoolerStatus(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
	managedPodNames []string,
) *PoolerStatusResult {
	result := &PoolerStatusResult{
		Roles:        make(map[string]string),
		QuerySuccess: true,
	}

	cells := CollectCells(shard)
	for _, cell := range cells {
		opt := ShardFilter(shard)
		poolers, err := store.GetMultipoolersByCell(ctx, cell, opt)
		if err == nil {
			for _, p := range poolers {
				if isLifecycleShutdown(p.Multipooler) {
					continue
				}
				roleName := "REPLICA"
				if isLifecycleQuarantined(p.Multipooler) {
					roleName = "DRAINED"
				} else if IsPrimaryPooler(p.Multipooler) {
					roleName = "PRIMARY"
				}
				// Match the topology entry to an actual managed pod.
				podName := matchPoolerToPod(p, managedPodNames)
				if podName == "" {
					continue // Orphaned entry; pruning handles cleanup.
				}
				result.Roles[podName] = roleName
			}
		} else {
			result.QuerySuccess = false
		}
	}
	return result
}

// matchPoolerToPod finds the managed pod name that matches a topology pooler
// entry, using the FQDN-aware PodMatchesPooler comparison.
func matchPoolerToPod(p *topoclient.MultipoolerInfo, podNames []string) string {
	for _, name := range podNames {
		if PodMatchesPooler(name, p) {
			return name
		}
	}
	return ""
}

// FindPrimaryPooler discovers the PRIMARY multipooler from the given cells
// in the topology. Returns (nil, nil) only if at least one cell was
// successfully queried but no primary was found. Returns an error if all
// cells are unavailable.
func FindPrimaryPooler(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
	cells []string,
) (*clustermetadatapb.Multipooler, error) {
	var anySuccess bool
	for _, cell := range cells {
		opt := ShardFilter(shard)
		poolers, err := store.GetMultipoolersByCell(ctx, cell, opt)
		if err != nil {
			if IsTopoUnavailable(err) {
				continue
			}
			return nil, fmt.Errorf("listing poolers in cell %q: %w", cell, err)
		}
		anySuccess = true
		for _, p := range poolers {
			if IsPrimaryPooler(p.Multipooler) &&
				!isLifecycleShutdown(p.Multipooler) &&
				!isLifecycleQuarantined(p.Multipooler) {
				return p.Multipooler, nil
			}
		}
	}
	if !anySuccess && len(cells) > 0 {
		return nil, fmt.Errorf("all %d cell(s) unavailable, cannot determine primary", len(cells))
	}
	return nil, nil
}

// PoolerRoutingRole returns the pooler's advertised routing role. RoutingState
// is the upstream source of truth for primary/replica decisions; the legacy
// Multipooler.Type projection is deprecated and will be removed upstream.
func PoolerRoutingRole(mp *clustermetadatapb.Multipooler) clustermetadatapb.RoutingRole {
	if mp == nil {
		return clustermetadatapb.RoutingRole_ROUTING_ROLE_UNKNOWN
	}
	return mp.GetRoutingState().GetRole()
}

// IsPrimaryPooler reports whether the pooler advertises the writable primary
// routing role.
func IsPrimaryPooler(mp *clustermetadatapb.Multipooler) bool {
	return PoolerRoutingRole(mp) == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
}

// CollectCells returns the sorted, deduplicated set of cell names from the shard's pools.
func CollectCells(shard *multigresv1alpha1.Shard) []string {
	seen := make(map[string]bool)
	for _, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			seen[string(cell)] = true
		}
	}
	cells := make([]string, 0, len(seen))
	for cell := range seen {
		cells = append(cells, cell)
	}
	slices.Sort(cells)
	return cells
}

// ShardFilter builds a GetMultipoolersByCellOptions filter for a shard.
func ShardFilter(shard *multigresv1alpha1.Shard) *topoclient.GetMultipoolersByCellOptions {
	return &topoclient.GetMultipoolersByCellOptions{
		DatabaseShard: &topoclient.DatabaseShard{
			Database:   string(shard.Spec.DatabaseName),
			TableGroup: string(shard.Spec.TableGroupName),
			Shard:      string(shard.Spec.ShardName),
		},
	}
}

// PodMatchesPooler checks if the topology pooler record corresponds to the given Kubernetes pod name.
func PodMatchesPooler(podName string, p *topoclient.MultipoolerInfo) bool {
	if p.Id != nil && p.Id.Name == podName {
		return true
	}
	h := p.GetHostname()
	return h == podName || strings.HasPrefix(h, podName+".")
}

// ForceUnregisterPod unregisters a specific pod's pooler from the topology.
func ForceUnregisterPod(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
	podName string,
	cellName string,
) error {
	if cellName == "" {
		return nil
	}

	opt := ShardFilter(shard)
	poolers, err := store.GetMultipoolersByCell(ctx, cellName, opt)
	if err != nil {
		return err
	}

	for _, p := range poolers {
		if PodMatchesPooler(podName, p) {
			return store.UnregisterMultipooler(ctx, p.Id)
		}
	}
	log.FromContext(ctx).
		Info("No matching pooler found in topology for pod, skipping unregistration",
			"pod", podName, "cell", cellName)
	return nil
}

// MarkDeadPoolers scans topology entries for poolers that have no corresponding
// running pod and marks them LIFECYCLE_SHUTDOWN, the same lifecycle signal a
// pooler writes for itself on graceful shutdown. The orchestrator's pooler
// watcher keys solely on this lifecycle transition to tear down the per-pooler
// health stream and clear the pooler from the cohort, so it stops treating the
// pooler as a lingering, unreachable member.
// activePodNames should contain the names of all pods currently managed by the
// shard. Returns the number of entries newly marked shut down.
func MarkDeadPoolers(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
	activePodNames map[string]bool,
) (int, error) {
	logger := log.FromContext(ctx)
	marked := 0

	cells := CollectCells(shard)
	for _, cell := range cells {
		opt := ShardFilter(shard)
		poolers, err := store.GetMultipoolersByCell(ctx, cell, opt)
		if err != nil {
			if IsTopoUnavailable(err) {
				continue
			}
			return marked, fmt.Errorf(
				"listing poolers in cell %q for dead-pooler detection: %w",
				cell,
				err,
			)
		}

		for _, p := range poolers {
			if poolerMatchesAnyActivePod(p, activePodNames) {
				continue
			}
			if isLifecycleShutdown(p.Multipooler) {
				continue // Already marked; nothing to do.
			}

			hostname := p.GetHostname()
			if hostname == "" && p.Id != nil {
				hostname = p.Id.Name
			}

			if _, err := store.UpdateMultipoolerFields(ctx, p.Id,
				func(mp *clustermetadatapb.Multipooler) error {
					// TODO: Consider a dedicated lifecycle status (e.g.
					// DEPROVISIONED) to distinguish an operator-detected dead
					// pooler from a graceful shutdown. Doesn't affect orch
					// behavior, but aids logging and anyone inspecting topo
					// state directly.
					mp.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
						Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
						Reason:  deadPoolerReason,
						Updated: timestamppb.New(time.Now()),
					}
					return nil
				}); err != nil {
				logger.Error(err, "Failed to mark dead pooler shut down",
					"pooler", hostname, "cell", cell)
				continue
			}
			logger.Info("Marked dead pooler LIFECYCLE_SHUTDOWN in topology",
				"pooler", hostname, "cell", cell)
			marked++
		}
	}

	return marked, nil
}

// isLifecycleShutdown reports whether the pooler is already recorded as shut
// down in topology.
func isLifecycleShutdown(mp *clustermetadatapb.Multipooler) bool {
	return mp != nil &&
		mp.LifecycleStatus != nil &&
		mp.LifecycleStatus.Status == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN
}

// isLifecycleQuarantined reports whether the pooler is retained for
// investigation and must not count toward healthy shard capacity.
func isLifecycleQuarantined(mp *clustermetadatapb.Multipooler) bool {
	return mp != nil &&
		mp.LifecycleStatus != nil &&
		mp.LifecycleStatus.Status == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED
}

// poolerMatchesAnyActivePod returns true when the pooler record corresponds to
// any pod in activePodNames. Uses PodMatchesPooler for FQDN-aware comparison.
func poolerMatchesAnyActivePod(p *topoclient.MultipoolerInfo, activePodNames map[string]bool) bool {
	for podName := range activePodNames {
		if PodMatchesPooler(podName, p) {
			return true
		}
	}
	return false
}
