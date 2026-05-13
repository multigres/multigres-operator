package topo_test

import (
	"context"
	"errors"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

func TestFindPrimaryPooler(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when no primary exists", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "db",
				TableGroupName: "tg",
				ShardName:      "0",
			},
		}

		primary, err := topo.FindPrimaryPooler(
			context.Background(),
			store,
			shard,
			[]string{"cell1"},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if primary != nil {
			t.Error("expected nil primary when none registered")
		}
	})

	t.Run("returns error for non-unavailable topo errors", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell2")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
			},
		}

		_, err := topo.FindPrimaryPooler(
			context.Background(), store, shard, []string{"nonexistent-cell"},
		)
		if err == nil {
			t.Error("expected error for non-unavailable topo error")
		}
	})

	t.Run("returns primary from second cell", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1", "cell2")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod"},
			Hostname: "replica-pod", Type: clustermetadata.PoolerType_REPLICA,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell2", Name: "primary-pod"},
			Hostname: "primary-pod", Type: clustermetadata.PoolerType_PRIMARY,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
			},
		}

		primary, err := topo.FindPrimaryPooler(ctx, store, shard, []string{"cell1", "cell2"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if primary == nil {
			t.Fatal("expected primary to be found")
		}
		if primary.Id.Name != "primary-pod" {
			t.Errorf("expected primary-pod, got %s", primary.Id.Name)
		}
	})
}

// multiCellStore is a mock store that returns different results per cell.
type multiCellStore struct {
	topoclient.Store
	cells      map[string][]*topoclient.MultiPoolerInfo
	errorCells map[string]error
}

// mockPoolerTopoStore allows mocking more functions for pooler pruning tests.
type mockPoolerTopoStore struct {
	topoclient.Store
	getMultiPoolersByCellFunc func(ctx context.Context, cell string, opts *topoclient.GetMultiPoolersByCellOptions) ([]*topoclient.MultiPoolerInfo, error)
	unregisterMultiPoolerFunc func(ctx context.Context, id *clustermetadata.ID) error
}

func (s *mockPoolerTopoStore) GetMultiPoolersByCell(
	ctx context.Context, cell string, opts *topoclient.GetMultiPoolersByCellOptions,
) ([]*topoclient.MultiPoolerInfo, error) {
	if s.getMultiPoolersByCellFunc != nil {
		return s.getMultiPoolersByCellFunc(ctx, cell, opts)
	}
	return nil, nil
}

func (s *mockPoolerTopoStore) UnregisterMultiPooler(
	ctx context.Context,
	id *clustermetadata.ID,
) error {
	if s.unregisterMultiPoolerFunc != nil {
		return s.unregisterMultiPoolerFunc(ctx, id)
	}
	return nil
}

func (s *multiCellStore) GetMultiPoolersByCell(
	ctx context.Context, cell string, opts *topoclient.GetMultiPoolersByCellOptions,
) ([]*topoclient.MultiPoolerInfo, error) {
	if err, ok := s.errorCells[cell]; ok {
		return nil, err
	}
	return s.cells[cell], nil
}

func (s *multiCellStore) Close() error { return nil }

func TestFindPrimaryPooler_TopoUnavailableSkip(t *testing.T) {
	t.Parallel()

	t.Run("skips unavailable cell and finds primary in next cell", func(t *testing.T) {
		t.Parallel()
		store := &multiCellStore{
			cells: map[string][]*topoclient.MultiPoolerInfo{
				"cell2": {{
					MultiPooler: &clustermetadata.MultiPooler{
						Id:       &clustermetadata.ID{Cell: "cell2", Name: "primary-pod"},
						Hostname: "primary-pod", Type: clustermetadata.PoolerType_PRIMARY,
					},
				}},
			},
			errorCells: map[string]error{
				"cell1": errors.New("Code: UNAVAILABLE"),
			},
		}

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
			},
		}

		primary, err := topo.FindPrimaryPooler(
			context.Background(), store, shard, []string{"cell1", "cell2"},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if primary == nil {
			t.Fatal("expected primary from cell2 after skipping unavailable cell1")
		}
		if primary.Id.Name != "primary-pod" {
			t.Errorf("expected primary-pod, got %s", primary.Id.Name)
		}
	})

	t.Run("returns error when all cells are unavailable", func(t *testing.T) {
		t.Parallel()
		store := &multiCellStore{
			errorCells: map[string]error{
				"cell1": errors.New("Code: UNAVAILABLE"),
				"cell2": errors.New("Code: UNAVAILABLE"),
			},
		}

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
			},
		}

		primary, err := topo.FindPrimaryPooler(
			context.Background(), store, shard, []string{"cell1", "cell2"},
		)
		if err == nil {
			t.Fatal("expected error when all cells are unavailable")
		}
		if primary != nil {
			t.Error("expected nil primary when all cells are unavailable")
		}
	})
}

func TestPrunePoolers(t *testing.T) {
	t.Parallel()

	t.Run("prunes stale poolers", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "active-pod"},
			Hostname: "active-pod", Type: clustermetadata.PoolerType_REPLICA,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "stale-pod"},
			Hostname: "stale-pod", Type: clustermetadata.PoolerType_REPLICA,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "dead-pod"},
			Hostname: "dead-pod", Type: clustermetadata.PoolerType_DRAINED,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		activePods := map[string]bool{"active-pod": true}
		pruned, err := topo.PrunePoolers(ctx, store, shard, activePods)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pruned != 2 {
			t.Errorf("expected 2 pruned, got %d", pruned)
		}

		// Verify only active-pod remains.
		remaining, _ := store.GetMultiPoolersByCell(ctx, "cell1", nil)
		if len(remaining) != 1 {
			t.Fatalf("expected 1 remaining pooler, got %d", len(remaining))
		}
		if remaining[0].Id.Name != "active-pod" {
			t.Errorf("expected active-pod to remain, got %s", remaining[0].Id.Name)
		}
	})

	t.Run("noop when all poolers are active", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "pod-1"},
			Hostname: "pod-1", Type: clustermetadata.PoolerType_REPLICA,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		activePods := map[string]bool{"pod-1": true}
		pruned, err := topo.PrunePoolers(ctx, store, shard, activePods)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pruned != 0 {
			t.Errorf("expected 0 pruned, got %d", pruned)
		}
	})

	t.Run("skips unavailable cells gracefully", func(t *testing.T) {
		t.Parallel()
		store := &multiCellStore{
			errorCells: map[string]error{
				"cell1": errors.New("Code: UNAVAILABLE"),
			},
		}

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		pruned, err := topo.PrunePoolers(
			context.Background(), store, shard, map[string]bool{},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pruned != 0 {
			t.Errorf("expected 0 pruned for unavailable cell, got %d", pruned)
		}
	})

	t.Run("does not prune active poolers with FQDN hostnames", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "active-pod"},
			Hostname: "active-pod.headless-svc.ns.svc.cluster.local",
			Type:     clustermetadata.PoolerType_PRIMARY,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "stale-pod"},
			Hostname: "stale-pod.headless-svc.ns.svc.cluster.local",
			Type:     clustermetadata.PoolerType_REPLICA,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		// active-pod is in the active set; stale-pod is NOT.
		activePods := map[string]bool{"active-pod": true}
		pruned, err := topo.PrunePoolers(ctx, store, shard, activePods)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pruned != 1 {
			t.Errorf("expected 1 pruned (stale-pod), got %d", pruned)
		}

		remaining, _ := store.GetMultiPoolersByCell(ctx, "cell1", nil)
		if len(remaining) != 1 {
			t.Fatalf("expected 1 remaining pooler, got %d", len(remaining))
		}
		if remaining[0].Id.Name != "active-pod" {
			t.Errorf("expected active-pod to remain, got %s", remaining[0].Id.Name)
		}
	})

	t.Run("returns error on topology listing failure", func(t *testing.T) {
		t.Parallel()
		store := &errorGetPoolersStore{}

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		_, err := topo.PrunePoolers(context.Background(), store, shard, map[string]bool{})
		if err == nil {
			t.Error("expected error when GetMultiPoolersByCell fails")
		}
	})

	t.Run("continues and logs error on UnregisterMultiPooler failure", func(t *testing.T) {
		t.Parallel()
		store := &mockPoolerTopoStore{
			getMultiPoolersByCellFunc: func(ctx context.Context, cell string, opts *topoclient.GetMultiPoolersByCellOptions) ([]*topoclient.MultiPoolerInfo, error) {
				p := &topoclient.MultiPoolerInfo{
					MultiPooler: &clustermetadata.MultiPooler{
						Id:       &clustermetadata.ID{Cell: "cell1", Name: "stale-pod"},
						Hostname: "stale-pod",
						Type:     clustermetadata.PoolerType_REPLICA,
						ShardKey: &clustermetadata.ShardKey{
							Database:   "db",
							TableGroup: "tg",
							Shard:      "0",
						},
					},
				}
				return []*topoclient.MultiPoolerInfo{p}, nil
			},
			unregisterMultiPoolerFunc: func(ctx context.Context, id *clustermetadata.ID) error {
				return errors.New("fake unregister error")
			},
		}

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		pruned, err := topo.PrunePoolers(context.Background(), store, shard, map[string]bool{})
		if err != nil {
			t.Fatalf("expected nil error (caught and logged), got %v", err)
		}
		if pruned != 0 {
			t.Errorf("expected 0 pruned due to error, got %d", pruned)
		}
	})

	t.Run("uses Id.Name when hostname is empty during pruning", func(t *testing.T) {
		t.Parallel()
		store := &mockPoolerTopoStore{
			getMultiPoolersByCellFunc: func(ctx context.Context, cell string, opts *topoclient.GetMultiPoolersByCellOptions) ([]*topoclient.MultiPoolerInfo, error) {
				p := &topoclient.MultiPoolerInfo{
					MultiPooler: &clustermetadata.MultiPooler{
						Id:       &clustermetadata.ID{Cell: "cell1", Name: "stale-pod-no-hostname"},
						Hostname: "",
						Type:     clustermetadata.PoolerType_REPLICA,
						ShardKey: &clustermetadata.ShardKey{
							Database:   "db",
							TableGroup: "tg",
							Shard:      "0",
						},
					},
				}
				return []*topoclient.MultiPoolerInfo{p}, nil
			},
			unregisterMultiPoolerFunc: func(ctx context.Context, id *clustermetadata.ID) error {
				return nil
			},
		}

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		pruned, err := topo.PrunePoolers(context.Background(), store, shard, map[string]bool{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pruned != 1 {
			t.Errorf("expected 1 pruned, got %d", pruned)
		}
	})
}

func TestForceUnregisterPod(t *testing.T) {
	t.Parallel()

	t.Run("returns nil for empty cell label", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		err := topo.ForceUnregisterPod(context.Background(), store, shard, "pod", "")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("skips unregistration when no matching pooler found", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "other-pod"},
			Hostname: "other-pod", Type: clustermetadata.PoolerType_REPLICA,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
			},
		}

		err := topo.ForceUnregisterPod(ctx, store, shard, "nonexistent-pod", "cell1")
		if err != nil {
			t.Errorf("expected nil error for missing pooler, got %v", err)
		}

		poolers, _ := store.GetMultiPoolersByCell(ctx, "cell1", nil)
		if len(poolers) != 1 {
			t.Errorf("expected 1 pooler remaining, got %d", len(poolers))
		}
	})

	t.Run("removes pooler that matches pod", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "my-pod"},
			Hostname: "my-pod", Type: clustermetadata.PoolerType_REPLICA,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
			},
		}

		err := topo.ForceUnregisterPod(ctx, store, shard, "my-pod", "cell1")
		if err != nil {
			t.Errorf("expected nil error for successful unregistration, got %v", err)
		}

		poolers, _ := store.GetMultiPoolersByCell(ctx, "cell1", nil)
		if len(poolers) != 0 {
			t.Errorf("expected 0 poolers remaining, got %d", len(poolers))
		}
	})
}

// errorGetPoolersStore returns an error for GetMultiPoolersByCell.
type errorGetPoolersStore struct {
	topoclient.Store
}

func (s *errorGetPoolersStore) GetMultiPoolersByCell(
	ctx context.Context, cell string, opts *topoclient.GetMultiPoolersByCellOptions,
) ([]*topoclient.MultiPoolerInfo, error) {
	return nil, errors.New("topo error")
}

func (s *errorGetPoolersStore) Close() error { return nil }

func TestForceUnregisterPod_GetPoolersError(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
		},
	}

	store := &errorGetPoolersStore{}
	err := topo.ForceUnregisterPod(context.Background(), store, shard, "test-pod", "cell1")
	if err == nil {
		t.Error("expected error when GetMultiPoolersByCell fails")
	}
}

func TestCollectCells(t *testing.T) {
	t.Parallel()

	shard := &multigresv1alpha1.Shard{
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {Cells: []multigresv1alpha1.CellName{"zone-a", "zone-b"}},
				"replica": {Cells: []multigresv1alpha1.CellName{"zone-b", "zone-c"}},
			},
		},
	}

	cells := topo.CollectCells(shard)
	if len(cells) != 3 {
		t.Errorf("expected 3 unique cells, got %d: %v", len(cells), cells)
	}

	cellSet := make(map[string]bool)
	for _, c := range cells {
		cellSet[c] = true
	}
	for _, want := range []string{"zone-a", "zone-b", "zone-c"} {
		if !cellSet[want] {
			t.Errorf("expected cell %q in result", want)
		}
	}
}

func TestGetPoolerStatus(t *testing.T) {
	t.Parallel()

	t.Run("returns roles for all pooler types", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "primary"},
			Hostname: "primary", Type: clustermetadata.PoolerType_PRIMARY,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica"},
			Hostname: "replica", Type: clustermetadata.PoolerType_REPLICA,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "drained"},
			Hostname: "drained", Type: clustermetadata.PoolerType_DRAINED,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		result := topo.GetPoolerStatus(ctx, store, shard, []string{"primary", "replica", "drained"})
		if !result.QuerySuccess {
			t.Error("expected QuerySuccess=true")
		}
		if result.Roles["primary"] != "PRIMARY" {
			t.Errorf("expected PRIMARY, got %s", result.Roles["primary"])
		}
		if result.Roles["replica"] != "REPLICA" {
			t.Errorf("expected REPLICA, got %s", result.Roles["replica"])
		}
		if result.Roles["drained"] != "DRAINED" {
			t.Errorf("expected DRAINED, got %s", result.Roles["drained"])
		}
	})

	t.Run("skips orphaned poolers gracefully", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "orphaned-pod"},
			Hostname: "orphaned-pod", Type: clustermetadata.PoolerType_PRIMARY,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		result := topo.GetPoolerStatus(ctx, store, shard, []string{"other-pod"})
		if !result.QuerySuccess {
			t.Error("expected QuerySuccess=true")
		}
		if len(result.Roles) != 0 {
			t.Errorf("expected no roles mapped for orphaned pod, got %v", result.Roles)
		}
	})

	t.Run("sets QuerySuccess false on error", func(t *testing.T) {
		t.Parallel()
		store := &errorGetPoolersStore{}

		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		result := topo.GetPoolerStatus(context.Background(), store, shard, nil)
		if result.QuerySuccess {
			t.Error("expected QuerySuccess=false when store errors")
		}
	})

	t.Run("uses Id.Name when hostname is empty", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := context.Background()
		_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "my-pod-0"},
			Hostname: "", Type: clustermetadata.PoolerType_PRIMARY,
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		result := topo.GetPoolerStatus(ctx, store, shard, []string{"my-pod-0"})
		if !result.QuerySuccess {
			t.Error("expected QuerySuccess=true")
		}
		if result.Roles["my-pod-0"] != "PRIMARY" {
			t.Errorf("expected key 'my-pod-0' with PRIMARY, got roles: %v", result.Roles)
		}
	})
}
