package topo_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

func newTestCell(name string) *multigresv1alpha1.Cell {
	return &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: multigresv1alpha1.CellSpec{
			Name: multigresv1alpha1.CellName(name),
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:  "localhost:2379",
				RootPath: "/test",
			},
			TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{
						"http://local-etcd-1:2379",
						"http://local-etcd-2:2379",
					},
					RootPath: "/multigres/cells/" + name,
				},
			},
		},
	}
}

type mockTopoStore struct {
	topoclient.Store
	createCellFunc       func(ctx context.Context, cellName string, cell *clustermetadata.Cell) error
	updateCellFieldsFunc func(ctx context.Context, cellName string, updater func(*clustermetadata.Cell) error) error
	deleteCellFunc       func(ctx context.Context, cellName string, force bool) error
}

func (m *mockTopoStore) CreateCell(
	ctx context.Context,
	cellName string,
	cell *clustermetadata.Cell,
) error {
	if m.createCellFunc != nil {
		return m.createCellFunc(ctx, cellName, cell)
	}
	return nil
}

func (m *mockTopoStore) UpdateCellFields(
	ctx context.Context,
	cellName string,
	updater func(*clustermetadata.Cell) error,
) error {
	if m.updateCellFieldsFunc != nil {
		return m.updateCellFieldsFunc(ctx, cellName, updater)
	}
	return nil
}

func (m *mockTopoStore) DeleteCell(ctx context.Context, cellName string, force bool) error {
	if m.deleteCellFunc != nil {
		return m.deleteCellFunc(ctx, cellName, force)
	}
	return nil
}

func TestRegisterCell(t *testing.T) {
	t.Parallel()

	t.Run("creates new cell in topology", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		// Register a different cell name to ensure it's not already in topo
		cell := newTestCell("cell2")

		if err := topo.RegisterCell(context.Background(), store, recorder, cell); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got, err := store.GetCell(context.Background(), "cell2")
		if err != nil {
			t.Fatalf("cell not found in topo after registration: %v", err)
		}
		if got.Name != "cell2" {
			t.Errorf("expected cell name cell2, got %s", got.Name)
		}
		if !reflect.DeepEqual(
			got.ServerAddresses,
			[]string{"http://local-etcd-1:2379", "http://local-etcd-2:2379"},
		) {
			t.Errorf("expected local topo addresses, got %v", got.ServerAddresses)
		}
		if got.Root != "/multigres/cells/cell2" {
			t.Errorf("expected local topo root, got %s", got.Root)
		}
	})

	t.Run("returns error on failure", func(t *testing.T) {
		t.Parallel()
		store := &mockTopoStore{
			createCellFunc: func(ctx context.Context, cellName string, cell *clustermetadata.Cell) error {
				return fmt.Errorf("fake connection error")
			},
		}

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")

		err := topo.RegisterCell(context.Background(), store, recorder, cell)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("idempotent when cell already exists", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")

		if err := topo.RegisterCell(context.Background(), store, recorder, cell); err != nil {
			t.Fatalf("first registration failed: %v", err)
		}
		if err := topo.RegisterCell(context.Background(), store, recorder, cell); err != nil {
			t.Fatalf("second registration should succeed (idempotent), got: %v", err)
		}
	})

	t.Run("updates stale cell topology on re-registration", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")
		if err := store.UpdateCellFields(
			context.Background(),
			"cell1",
			func(existing *clustermetadata.Cell) error {
				existing.ServerAddresses = []string{"http://stale-local-etcd:2379"}
				existing.Root = "/stale/root"
				return nil
			},
		); err != nil {
			t.Fatalf("seeding stale cell: %v", err)
		}

		if err := topo.RegisterCell(context.Background(), store, recorder, cell); err != nil {
			t.Fatalf("re-registration should update stale cell, got: %v", err)
		}

		got, err := store.GetCell(context.Background(), "cell1")
		if err != nil {
			t.Fatalf("cell not found: %v", err)
		}
		if !reflect.DeepEqual(
			got.ServerAddresses,
			[]string{"http://local-etcd-1:2379", "http://local-etcd-2:2379"},
		) {
			t.Errorf("expected local topo addresses, got %v", got.ServerAddresses)
		}
		if got.Root != "/multigres/cells/cell1" {
			t.Errorf("expected local topo root, got %s", got.Root)
		}
	})

	t.Run("falls back to global topology when no local topology is configured", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell2")
		cell.Spec.TopoServer = nil

		if err := topo.RegisterCell(context.Background(), store, recorder, cell); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got, err := store.GetCell(context.Background(), "cell2")
		if err != nil {
			t.Fatalf("cell not found: %v", err)
		}
		if !reflect.DeepEqual(got.ServerAddresses, []string{"localhost:2379"}) {
			t.Errorf("expected global topo address fallback, got %v", got.ServerAddresses)
		}
		if got.Root != "/test" {
			t.Errorf("expected global topo root fallback, got %s", got.Root)
		}
	})
}

func TestUnregisterCell(t *testing.T) {
	t.Parallel()

	t.Run("removes existing cell", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")
		ctx := context.Background()

		if err := topo.RegisterCell(ctx, store, recorder, cell); err != nil {
			t.Fatalf("registration failed: %v", err)
		}
		if err := topo.UnregisterCell(ctx, store, recorder, cell); err != nil {
			t.Fatalf("unregistration failed: %v", err)
		}

		_, err := store.GetCell(ctx, "cell1")
		if err == nil {
			t.Error("expected cell to be gone from topo after unregistration")
		}
	})

	t.Run("idempotent when cell does not exist", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("nonexistent")

		if err := topo.UnregisterCell(context.Background(), store, recorder, cell); err != nil {
			t.Fatalf("unregistering nonexistent cell should succeed (idempotent), got: %v", err)
		}
	})

	t.Run("returns error on failure other than TopoUnavailable", func(t *testing.T) {
		t.Parallel()
		store := &mockTopoStore{
			deleteCellFunc: func(ctx context.Context, cellName string, force bool) error {
				return fmt.Errorf("some other error")
			},
		}

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")

		err := topo.UnregisterCell(context.Background(), store, recorder, cell)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error on TopoUnavailable", func(t *testing.T) {
		t.Parallel()
		store := &mockTopoStore{
			deleteCellFunc: func(ctx context.Context, cellName string, force bool) error {
				return fmt.Errorf("fake UNAVAILABLE error")
			},
		}

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")

		err := topo.UnregisterCell(context.Background(), store, recorder, cell)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
