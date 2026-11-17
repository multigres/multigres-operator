package topo

import (
	"context"
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
)

// ShardRef names one shard of one tablegroup in topology.
type ShardRef struct {
	Database, TableGroup, Shard string
}

// UnregisterTableGroupPoolers deletes, in every cell, the pooler records of
// ref whose tablegroup id equals tableGroupID. Records of the same name with
// another id belong to another incarnation of the tablegroup and are left
// alone, as are records with id 0. Returns how many records it deleted.
//
// Unlike MarkDeadPoolers this deletes the records rather than marking them
// shut down: a dropped tablegroup's DROP TABLEGROUP completes only once no
// record carrying its id remains. Any unreadable cell is an error, so the
// caller retries rather than mistaking it for an empty one.
func UnregisterTableGroupPoolers(
	ctx context.Context,
	store topoclient.Store,
	ref ShardRef,
	tableGroupID int64,
) (int, error) {
	if tableGroupID <= 0 {
		return 0, fmt.Errorf("refusing to unregister poolers of %s/%s/%s for tablegroup id %d",
			ref.Database, ref.TableGroup, ref.Shard, tableGroupID)
	}
	cells, err := store.GetCellNames(ctx)
	if err != nil {
		return 0, fmt.Errorf("listing cells: %w", err)
	}
	filter := &topoclient.GetMultipoolersByCellOptions{
		DatabaseShard: &topoclient.DatabaseShard{
			Database:   ref.Database,
			TableGroup: ref.TableGroup,
			Shard:      ref.Shard,
		},
	}
	deleted := 0
	for _, cell := range cells {
		poolers, err := store.GetMultipoolersByCell(ctx, cell, filter)
		if err != nil {
			return deleted, fmt.Errorf("listing poolers in cell %q: %w", cell, err)
		}
		for _, p := range poolers {
			if p.GetTablegroupId() != tableGroupID {
				continue
			}
			err := store.UnregisterMultipooler(ctx, p.Id)
			if err != nil && !errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
				return deleted, fmt.Errorf("unregistering pooler %s: %w",
					topoclient.ComponentIDString(p.Id), err)
			}
			deleted++
		}
	}
	return deleted, nil
}
