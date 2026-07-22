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

func routingState(role clustermetadata.RoutingRole) *clustermetadata.RoutingState {
	return &clustermetadata.RoutingState{Role: role}
}

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
			t.Context(), store, shard, []string{"nonexistent-cell"},
		)
		if err == nil {
			t.Error("expected error for non-unavailable topo error")
		}
	})

	t.Run("returns primary from second cell", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1", "cell2")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "replica-pod"},
			Hostname:     "replica-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell2", Name: "primary-pod"},
			Hostname:     "primary-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
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

	t.Run("skips a shut-down primary", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		// A dead primary whose pod is gone, marked LIFECYCLE_SHUTDOWN, must not
		// be returned as the live primary.
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "dead-primary"},
			Hostname:     "dead-primary",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			LifecycleStatus: &clustermetadata.PoolerLifecycle{
				Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
			},
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
			},
		}

		primary, err := topo.FindPrimaryPooler(ctx, store, shard, []string{"cell1"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if primary != nil {
			t.Errorf("expected nil primary (dead primary skipped), got %s", primary.Id.Name)
		}
	})

	t.Run("skips a quarantined primary", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "quarantined-primary"},
			Hostname:     "quarantined-primary",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			LifecycleStatus: &clustermetadata.PoolerLifecycle{
				Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED,
			},
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)

		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
			},
		}

		primary, err := topo.FindPrimaryPooler(ctx, store, shard, []string{"cell1"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if primary != nil {
			t.Errorf("expected nil primary (quarantined primary skipped), got %s", primary.Id.Name)
		}
	})
}

// multiCellStore is a mock store that returns different results per cell.
type multiCellStore struct {
	topoclient.Store
	cells      map[string][]*topoclient.MultipoolerInfo
	errorCells map[string]error
}

// mockPoolerTopoStore allows mocking more functions for dead-pooler tests.
type mockPoolerTopoStore struct {
	topoclient.Store
	getMultipoolersByCellFunc   func(ctx context.Context, cell string, opts *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error)
	updateMultipoolerFieldsFunc func(ctx context.Context, id *clustermetadata.ID, update func(*clustermetadata.Multipooler) error) (*clustermetadata.Multipooler, error)
}

func (s *mockPoolerTopoStore) GetMultipoolersByCell(
	ctx context.Context, cell string, opts *topoclient.GetMultipoolersByCellOptions,
) ([]*topoclient.MultipoolerInfo, error) {
	if s.getMultipoolersByCellFunc != nil {
		return s.getMultipoolersByCellFunc(ctx, cell, opts)
	}
	return nil, nil
}

func (s *mockPoolerTopoStore) UpdateMultipoolerFields(
	ctx context.Context,
	id *clustermetadata.ID,
	update func(*clustermetadata.Multipooler) error,
) (*clustermetadata.Multipooler, error) {
	if s.updateMultipoolerFieldsFunc != nil {
		return s.updateMultipoolerFieldsFunc(ctx, id, update)
	}
	return nil, nil
}

func (s *multiCellStore) GetMultipoolersByCell(
	ctx context.Context, cell string, opts *topoclient.GetMultipoolersByCellOptions,
) ([]*topoclient.MultipoolerInfo, error) {
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
			cells: map[string][]*topoclient.MultipoolerInfo{
				"cell2": {{
					Multipooler: &clustermetadata.Multipooler{
						Id: &clustermetadata.ID{
							Cell: "cell2",
							Name: "primary-pod",
						},
						Hostname: "primary-pod",
						RoutingState: routingState(
							clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY,
						),
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
			t.Context(), store, shard, []string{"cell1", "cell2"},
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
			t.Context(), store, shard, []string{"cell1", "cell2"},
		)
		if err == nil {
			t.Fatal("expected error when all cells are unavailable")
		}
		if primary != nil {
			t.Error("expected nil primary when all cells are unavailable")
		}
	})
}

func TestMarkDeadPoolers(t *testing.T) {
	t.Parallel()

	isShutdown := func(mp *clustermetadata.Multipooler) bool {
		return mp.LifecycleStatus != nil &&
			mp.LifecycleStatus.Status == clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN
	}

	t.Run("marks dead poolers shut down without deleting them", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "active-pod"},
			Hostname:     "active-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "stale-pod"},
			Hostname:     "stale-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "dead-pod"},
			Hostname:     "dead-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
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
		marked, err := topo.MarkDeadPoolers(ctx, store, shard, activePods)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if marked != 2 {
			t.Errorf("expected 2 marked, got %d", marked)
		}

		// Entries are left in place (tombstones), not deleted.
		remaining, _ := store.GetMultipoolersByCell(ctx, "cell1", nil)
		if len(remaining) != 3 {
			t.Fatalf("expected 3 remaining poolers (none deleted), got %d", len(remaining))
		}

		byName := make(map[string]*clustermetadata.Multipooler, len(remaining))
		for _, p := range remaining {
			byName[p.Id.Name] = p.Multipooler
		}

		// Dead poolers are marked LIFECYCLE_SHUTDOWN. Routing state is left as-is;
		// lifecycle is the source of truth and role readers skip shut-down entries.
		for _, name := range []string{"stale-pod", "dead-pod"} {
			mp := byName[name]
			if !isShutdown(mp) {
				t.Errorf("expected %s to be LIFECYCLE_SHUTDOWN, got %v", name, mp.LifecycleStatus)
			}
		}
		mp := byName["stale-pod"]
		if topo.PoolerRoutingRole(mp) != clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA {
			t.Errorf(
				"expected stale-pod routing role left untouched (REPLICA), got %v",
				topo.PoolerRoutingRole(mp),
			)
		}

		// Active pooler left untouched.
		if active := byName["active-pod"]; isShutdown(active) {
			t.Error("expected active-pod to be left untouched, but it was marked shut down")
		}
	})

	t.Run("is idempotent for already-shutdown poolers", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "already-dead"},
			Hostname:     "already-dead",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			LifecycleStatus: &clustermetadata.PoolerLifecycle{
				Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
			},
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

		marked, err := topo.MarkDeadPoolers(ctx, store, shard, map[string]bool{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if marked != 0 {
			t.Errorf("expected 0 marked for already-shutdown pooler, got %d", marked)
		}
	})

	t.Run("noop when all poolers are active", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "pod-1"},
			Hostname:     "pod-1",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
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
		marked, err := topo.MarkDeadPoolers(ctx, store, shard, activePods)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if marked != 0 {
			t.Errorf("expected 0 marked, got %d", marked)
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

		marked, err := topo.MarkDeadPoolers(
			t.Context(), store, shard, map[string]bool{},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if marked != 0 {
			t.Errorf("expected 0 marked for unavailable cell, got %d", marked)
		}
	})

	t.Run("does not mark active poolers with FQDN hostnames", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "active-pod"},
			Hostname:     "active-pod.headless-svc.ns.svc.cluster.local",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "stale-pod"},
			Hostname:     "stale-pod.headless-svc.ns.svc.cluster.local",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
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
		marked, err := topo.MarkDeadPoolers(ctx, store, shard, activePods)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if marked != 1 {
			t.Errorf("expected 1 marked (stale-pod), got %d", marked)
		}

		remaining, _ := store.GetMultipoolersByCell(ctx, "cell1", nil)
		if len(remaining) != 2 {
			t.Fatalf("expected 2 remaining poolers (none deleted), got %d", len(remaining))
		}
		for _, p := range remaining {
			if p.Id.Name == "active-pod" && isShutdown(p.Multipooler) {
				t.Error("expected active-pod to be left untouched")
			}
			if p.Id.Name == "stale-pod" && !isShutdown(p.Multipooler) {
				t.Error("expected stale-pod to be marked LIFECYCLE_SHUTDOWN")
			}
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

		_, err := topo.MarkDeadPoolers(t.Context(), store, shard, map[string]bool{})
		if err == nil {
			t.Error("expected error when GetMultipoolersByCell fails")
		}
	})

	t.Run("continues and logs error on UpdateMultipoolerFields failure", func(t *testing.T) {
		t.Parallel()
		store := &mockPoolerTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cell string, opts *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				p := &topoclient.MultipoolerInfo{
					Multipooler: &clustermetadata.Multipooler{
						Id:       &clustermetadata.ID{Cell: "cell1", Name: "stale-pod"},
						Hostname: "stale-pod",
						RoutingState: routingState(
							clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
						),
						ShardKey: &clustermetadata.ShardKey{
							Database:   "db",
							TableGroup: "tg",
							Shard:      "0",
						},
					},
				}
				return []*topoclient.MultipoolerInfo{p}, nil
			},
			updateMultipoolerFieldsFunc: func(ctx context.Context, id *clustermetadata.ID, update func(*clustermetadata.Multipooler) error) (*clustermetadata.Multipooler, error) {
				return nil, errors.New("fake update error")
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

		marked, err := topo.MarkDeadPoolers(t.Context(), store, shard, map[string]bool{})
		if err != nil {
			t.Fatalf("expected nil error (caught and logged), got %v", err)
		}
		if marked != 0 {
			t.Errorf("expected 0 marked due to error, got %d", marked)
		}
	})

	t.Run("uses Id.Name when hostname is empty", func(t *testing.T) {
		t.Parallel()
		store := &mockPoolerTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cell string, opts *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				p := &topoclient.MultipoolerInfo{
					Multipooler: &clustermetadata.Multipooler{
						Id: &clustermetadata.ID{
							Cell: "cell1",
							Name: "stale-pod-no-hostname",
						},
						Hostname: "",
						RoutingState: routingState(
							clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
						),
						ShardKey: &clustermetadata.ShardKey{
							Database:   "db",
							TableGroup: "tg",
							Shard:      "0",
						},
					},
				}
				return []*topoclient.MultipoolerInfo{p}, nil
			},
			updateMultipoolerFieldsFunc: func(ctx context.Context, id *clustermetadata.ID, update func(*clustermetadata.Multipooler) error) (*clustermetadata.Multipooler, error) {
				return nil, nil
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

		marked, err := topo.MarkDeadPoolers(t.Context(), store, shard, map[string]bool{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if marked != 1 {
			t.Errorf("expected 1 marked, got %d", marked)
		}
	})
}

func TestForceUnregisterPod(t *testing.T) {
	t.Parallel()

	t.Run("returns nil for empty cell label", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		err := topo.ForceUnregisterPod(t.Context(), store, shard, "pod", "")
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("skips unregistration when no matching pooler found", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "other-pod"},
			Hostname:     "other-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
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

		poolers, _ := store.GetMultipoolersByCell(ctx, "cell1", nil)
		if len(poolers) != 1 {
			t.Errorf("expected 1 pooler remaining, got %d", len(poolers))
		}
	})

	t.Run("removes pooler that matches pod", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "my-pod"},
			Hostname:     "my-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
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

		poolers, _ := store.GetMultipoolersByCell(ctx, "cell1", nil)
		if len(poolers) != 0 {
			t.Errorf("expected 0 poolers remaining, got %d", len(poolers))
		}
	})
}

// errorGetPoolersStore returns an error for GetMultipoolersByCell.
type errorGetPoolersStore struct {
	topoclient.Store
}

func (s *errorGetPoolersStore) GetMultipoolersByCell(
	ctx context.Context, cell string, opts *topoclient.GetMultipoolersByCellOptions,
) ([]*topoclient.MultipoolerInfo, error) {
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
	err := topo.ForceUnregisterPod(t.Context(), store, shard, "test-pod", "cell1")
	if err == nil {
		t.Error("expected error when GetMultipoolersByCell fails")
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

	t.Run("returns advertised routing roles", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "primary"},
			Hostname:     "primary",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "replica"},
			Hostname:     "replica",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:       &clustermetadata.ID{Cell: "cell1", Name: "unknown"},
			Hostname: "unknown",
			ShardKey: &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		}, false)
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "quarantined"},
			Hostname:     "quarantined",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			LifecycleStatus: &clustermetadata.PoolerLifecycle{
				Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED,
			},
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

		result := topo.GetPoolerStatus(
			ctx,
			store,
			shard,
			[]string{"primary", "replica", "unknown", "quarantined"},
		)
		if !result.QuerySuccess {
			t.Error("expected QuerySuccess=true")
		}
		if result.Roles["primary"] != "PRIMARY" {
			t.Errorf("expected PRIMARY, got %s", result.Roles["primary"])
		}
		if result.Roles["replica"] != "REPLICA" {
			t.Errorf("expected REPLICA, got %s", result.Roles["replica"])
		}
		if result.Roles["unknown"] != "REPLICA" {
			t.Errorf("expected REPLICA fallback, got %s", result.Roles["unknown"])
		}
		if result.Roles["quarantined"] != "DRAINED" {
			t.Errorf("expected DRAINED, got %s", result.Roles["quarantined"])
		}
	})

	t.Run("skips shut-down poolers even if a pod name matches", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "shutdown-pod"},
			Hostname:     "shutdown-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			LifecycleStatus: &clustermetadata.PoolerLifecycle{
				Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
			},
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

		result := topo.GetPoolerStatus(ctx, store, shard, []string{"shutdown-pod"})
		if !result.QuerySuccess {
			t.Error("expected QuerySuccess=true")
		}
		if len(result.Roles) != 0 {
			t.Errorf("expected no roles for shut-down pooler, got %v", result.Roles)
		}
	})

	t.Run("skips orphaned poolers gracefully", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "orphaned-pod"},
			Hostname:     "orphaned-pod",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
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

		result := topo.GetPoolerStatus(t.Context(), store, shard, nil)
		if result.QuerySuccess {
			t.Error("expected QuerySuccess=false when store errors")
		}
	})

	t.Run("uses Id.Name when hostname is empty", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		ctx := t.Context()
		_ = store.RegisterMultipooler(ctx, &clustermetadata.Multipooler{
			Id:           &clustermetadata.ID{Cell: "cell1", Name: "my-pod-0"},
			Hostname:     "",
			RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			ShardKey:     &clustermetadata.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
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
