package topo

import (
	"slices"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// poolerWithTableGroupID builds a pooler record as a pooler started with
// --tablegroup-id registers it.
func poolerWithTableGroupID(cell, name, tg, shard string, id int64) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		Hostname: name,
		ShardRef: &clustermetadatapb.ShardRef{
			Database:   "postgres",
			TableGroup: tg,
			Shard:      shard,
		},
		TablegroupId: id,
	}
}

func TestUnregisterTableGroupPoolers(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "cell1", "cell2")
	defer func() { _ = store.Close() }()

	records := []*clustermetadatapb.Multipooler{
		poolerWithTableGroupID("cell1", "this-a", "commerce", "0-80", 7),
		poolerWithTableGroupID("cell2", "this-b", "commerce", "0-80", 7),
		poolerWithTableGroupID("cell1", "older-incarnation", "commerce", "0-80", 3),
		poolerWithTableGroupID("cell1", "no-id", "commerce", "0-80", 0),
		poolerWithTableGroupID("cell1", "other-shard", "commerce", "80-inf", 7),
		poolerWithTableGroupID("cell1", "other-tablegroup", "orders", "0-80", 7),
	}
	for _, mp := range records {
		if err := store.RegisterMultipooler(ctx, mp, false); err != nil {
			t.Fatal(err)
		}
	}
	ref := ShardRef{Database: "postgres", TableGroup: "commerce", Shard: "0-80"}

	n, err := UnregisterTableGroupPoolers(ctx, store, ref, 7)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("deleted %d records, want 2", n)
	}
	var left []string
	for _, cell := range []string{"cell1", "cell2"} {
		poolers, err := store.GetMultipoolersByCell(ctx, cell, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range poolers {
			left = append(left, p.Id.Name)
		}
	}
	slices.Sort(left)
	want := []string{"no-id", "older-incarnation", "other-shard", "other-tablegroup"}
	if !slices.Equal(left, want) {
		t.Errorf("records left = %v, want %v", left, want)
	}

	if _, err := UnregisterTableGroupPoolers(ctx, store, ref, 0); err == nil {
		t.Error("id 0 must be refused: it would match default-tablegroup poolers")
	}
}
