package multigrescluster

import (
	"fmt"
	"slices"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
)

// Bound tablegroups. A tablegroup whose shard the catalog binds to an external
// PostgreSQL (it lists a backing connection for the shard) is provisioned as
// one Shard, 0-inf, with one pool of one pod in one cell: the Shard controller
// runs a pooler there and nothing else. The TableGroup is otherwise shaped like
// any dynamic one, from the database's default tablegroup.

// boundPlacement says where the one pooler of a bound tablegroup runs.
type boundPlacement struct {
	pool multigresv1alpha1.PoolName
	cell multigresv1alpha1.CellName
}

// boundConnection is the connection that serves tg's one shard, or "" for a
// managed tablegroup. It is only meaningful once validateBoundLayout passed.
func boundConnection(tg multiadmin.TableGroup) string {
	return tg.ShardBackingConnections[multigresv1alpha1.BoundShardName]
}

// storedBindings is the binding a provisioned TableGroup was built with.
func storedBindings(tg *multigresv1alpha1.TableGroup) map[string]string {
	conn := multigresv1alpha1.BackingConnectionOf(tg)
	if conn == "" {
		return nil
	}
	return map[string]string{multigresv1alpha1.BoundShardName: conn}
}

// validateBoundLayout checks that only the one shard of an unsharded
// tablegroup is bound. The catalog enforces it; this keeps a bad entry from
// being provisioned.
func validateBoundLayout(tg multiadmin.TableGroup) string {
	if len(tg.ShardBackingConnections) == 0 {
		return ""
	}
	if len(tg.Shards) != 1 || tg.Shards[0] != multigresv1alpha1.BoundShardName ||
		len(tg.ShardBackingConnections) != 1 || boundConnection(tg) == "" {
		return fmt.Sprintf(
			"binds a shard to a connection, so it must have exactly one shard, %s, and bind only it",
			multigresv1alpha1.BoundShardName,
		)
	}
	return ""
}

// resolveBoundPlacement returns the placement of want, nil for a managed
// tablegroup, or why a bound one cannot be provisioned. current is the
// TableGroup already provisioned for it, if any.
//
// The pooler runs in the first cell of the default tablegroup's shard: the
// first entry of the cells list of its first pool, taking the pools in name
// order since they are a map, and skipping a pool with no cells. That is the
// spec's own order, or the sorted order the resolver gives a list it filled in
// from the cluster's cells. A pool takes its resources, affinity and tolerations
// from that same pool.
//
// Once a TableGroup exists it keeps its own placement: a pool's cells are
// append-only, so the API server would refuse a different cell on every apply,
// and the pooler must never be in two places at once.
func resolveBoundPlacement(
	tmpl dynamicTemplate,
	current *multigresv1alpha1.TableGroup,
	want multiadmin.TableGroup,
) (*boundPlacement, string) {
	if boundConnection(want) == "" {
		return nil, ""
	}
	place := &boundPlacement{}
	if current != nil {
		place.pool, place.cell = pinnedPoolAndCell(current)
	}
	if place.cell == "" {
		place.pool, place.cell = firstPoolAndCell(tmpl.shard.Pools)
	}
	if place.cell == "" {
		return nil, "no cell to run the pooler in: the default tablegroup's pools list none"
	}
	return place, ""
}

// pinnedPoolAndCell returns the pool and cell of the one pooler of an existing
// bound TableGroup, or empty values when it does not have exactly one.
func pinnedPoolAndCell(
	tg *multigresv1alpha1.TableGroup,
) (multigresv1alpha1.PoolName, multigresv1alpha1.CellName) {
	if len(tg.Spec.Shards) != 1 || len(tg.Spec.Shards[0].Pools) != 1 {
		return "", ""
	}
	return firstPoolAndCell(tg.Spec.Shards[0].Pools)
}

// firstPoolAndCell returns the first pool by name that lists a cell, and the
// first cell it lists.
func firstPoolAndCell(
	pools map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec,
) (multigresv1alpha1.PoolName, multigresv1alpha1.CellName) {
	names := make([]multigresv1alpha1.PoolName, 0, len(pools))
	for name := range pools {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if cells := pools[name].Cells; len(cells) > 0 {
			return name, cells[0]
		}
	}
	return "", ""
}

// buildBoundShard turns a copy of the default tablegroup's shard into the
// shard of a bound tablegroup: one pool of one pod, in one cell. What only
// matters to a PostgreSQL the operator runs is dropped, so nothing in the spec
// suggests a volume, a backup or an orchestrator: the Shard controller starts
// the pooler alone.
func buildBoundShard(s *multigresv1alpha1.ShardResolvedSpec, place *boundPlacement) {
	pool := s.Pools[place.pool]
	s.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
		place.pool: {
			Type:        pool.Type,
			Cells:       []multigresv1alpha1.CellName{place.cell},
			Multipooler: pool.Multipooler,
			Affinity:    pool.Affinity,
			Tolerations: pool.Tolerations,
		},
	}
	s.Backup = nil
	s.InitdbArgs = ""
	s.PostgresConfig = nil
	s.PostgresConfigRef = nil
	s.PVCDeletionPolicy = nil
}
