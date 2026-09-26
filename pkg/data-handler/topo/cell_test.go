package topo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func newTestCell(name string) *multigresv1alpha1.Cell {
	return &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "cluster"},
		},
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
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		// Register a different cell name to ensure it's not already in topo
		cell := newTestCell("cell2")

		c.Require().
			NoError(topo.RegisterCell(t.Context(), store, recorder, cell, false), "unexpected error")

		got, err := store.GetCell(context.Background(), "cell2")
		c.Require().NoError(err, "cell not found in topo after registration")
		c.Eq("cell2", got.Name, "expected cell name cell2, got")
		c.EqDiff(
			[]string{"http://local-etcd-1:2379", "http://local-etcd-2:2379"},
			got.ServerAddresses,
			"expected local topo addresses, got",
		)
		c.Eq("/multigres/cells/cell2", got.Root, "expected local topo root, got")
	})

	t.Run("copies metadata verbatim into the topo record", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell2")
		cell.Spec.Metadata = `{"zoneId":"use1-az1","custom":"value"}`

		c.Require().
			NoError(topo.RegisterCell(t.Context(), store, recorder, cell, false), "unexpected error")

		got, err := store.GetCell(context.Background(), "cell2")
		c.Require().NoError(err, "cell not found")
		c.Eq(
			`{"zoneId":"use1-az1","custom":"value"}`,
			got.Metadata,
			"expected metadata copied verbatim, got",
		)
	})

	t.Run("updates metadata on re-registration", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")
		cell.Spec.Metadata = `{"zoneId":"use1-az1"}`

		c.Require().
			NoError(topo.RegisterCell(t.Context(), store, recorder, cell, false), "first registration failed")

		cell.Spec.Metadata = `{"zoneId":"use1-az2"}`
		c.Require().
			NoError(topo.RegisterCell(t.Context(), store, recorder, cell, false), "re-registration failed")

		got, err := store.GetCell(context.Background(), "cell1")
		c.Require().NoError(err, "cell not found")
		c.Eq(`{"zoneId":"use1-az2"}`, got.Metadata, "expected updated metadata, got")
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

		err := topo.RegisterCell(t.Context(), store, recorder, cell, false)
		assert.NewAborting(t).Error(err, "expected error, got nil")
	})

	t.Run("idempotent when cell already exists", func(t *testing.T) {
		t.Parallel()
		c := assert.NewAborting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")

		c.NoError(
			topo.RegisterCell(t.Context(), store, recorder, cell, false),
			"first registration failed",
		)
		c.NoError(
			topo.RegisterCell(t.Context(), store, recorder, cell, false),
			"second registration should succeed (idempotent), got",
		)
	})

	t.Run("updates stale cell topology on re-registration", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")
		c.Require().NoError(store.UpdateCellFields(
			context.Background(),
			"cell1",
			func(existing *clustermetadata.Cell) error {
				existing.ServerAddresses = []string{"http://stale-local-etcd:2379"}
				existing.Root = "/stale/root"
				return nil
			},
		), "seeding stale cell")

		c.Require().
			NoError(topo.RegisterCell(t.Context(), store, recorder, cell, false), "re-registration should update stale cell, got")

		got, err := store.GetCell(context.Background(), "cell1")
		c.Require().NoError(err, "cell not found")
		c.EqDiff(
			[]string{"http://local-etcd-1:2379", "http://local-etcd-2:2379"},
			got.ServerAddresses,
			"expected local topo addresses, got",
		)
		c.Eq("/multigres/cells/cell1", got.Root, "expected local topo root, got")
	})

	t.Run("falls back to global topology when no local topology is configured", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell2")
		cell.Spec.TopoServer = nil

		c.Require().
			NoError(topo.RegisterCell(t.Context(), store, recorder, cell, false), "unexpected error")

		got, err := store.GetCell(context.Background(), "cell2")
		c.Require().NoError(err, "cell not found")
		c.EqDiff(
			[]string{"localhost:2379"},
			got.ServerAddresses,
			"expected global topo address fallback, got",
		)
		c.Eq("/test", got.Root, "expected global topology root fallback, got")
	})

	t.Run("uses project identity for a defaulted local topology root", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		cell := newTestCell("cell2")
		cell.Annotations = map[string]string{metadata.AnnotationProjectRef: "proj_123"}
		cell.Spec.TopoServer.External.RootPath = ""

		c.Require().NoError(topo.RegisterCell(
			context.Background(), store, record.NewFakeRecorder(10), cell, false,
		), "unexpected error")

		got, err := store.GetCell(context.Background(), "cell2")
		c.Require().NoError(err, "cell not found")
		c.Eq("/multigres/proj_123/cell2", got.Root, "expected project-scoped cell root, got")
	})
}

func TestUnregisterCell(t *testing.T) {
	t.Parallel()

	t.Run("removes existing cell", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		cell := newTestCell("cell1")
		ctx := context.Background()

		c.Require().
			NoError(topo.RegisterCell(ctx, store, recorder, cell, false), "registration failed")
		c.Require().
			NoError(topo.UnregisterCell(ctx, store, recorder, cell), "unregistration failed")

		_, err := store.GetCell(ctx, "cell1")
		c.Error(err, "expected cell to be gone from topo after unregistration")
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

		assert.NewAborting(t).
			NoError(topo.UnregisterCell(context.Background(), store, recorder, cell), "unregistering nonexistent cell should succeed (idempotent), got")
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
		assert.NewAborting(t).Error(err, "expected error, got nil")
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
		assert.NewAborting(t).Error(err, "expected error, got nil")
	})
}
