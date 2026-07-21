package topo_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

func newMemoryStore(t *testing.T, cells ...string) topoclient.Store {
	t.Helper()
	_, factory := memorytopo.NewServerAndFactory(context.Background(), cells...)
	store := topoclient.NewWithFactory(
		factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
	)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// mockTopologyStore allows mocking topo functions to trigger error branches.
type mockTopologyStore struct {
	topoclient.Store
	createDatabaseFunc       func(ctx context.Context, dbName string, db *clustermetadatapb.Database) error
	updateDatabaseFieldsFunc func(ctx context.Context, dbName string, updater func(*clustermetadatapb.Database) error) error
	createCellFunc           func(ctx context.Context, cellName string, cell *clustermetadatapb.Cell) error
	updateCellFieldsFunc     func(ctx context.Context, cellName string, updater func(*clustermetadatapb.Cell) error) error
	getDatabaseNamesFunc     func(ctx context.Context) ([]string, error)
	deleteDatabaseFunc       func(ctx context.Context, dbName string, force bool) error
	getCellNamesFunc         func(ctx context.Context) ([]string, error)
	deleteCellFunc           func(ctx context.Context, cellName string, force bool) error
}

func (s *mockTopologyStore) CreateDatabase(
	ctx context.Context,
	dbName string,
	db *clustermetadatapb.Database,
) error {
	if s.createDatabaseFunc != nil {
		return s.createDatabaseFunc(ctx, dbName, db)
	}
	return s.Store.CreateDatabase(ctx, dbName, db)
}

func (s *mockTopologyStore) UpdateDatabaseFields(
	ctx context.Context,
	dbName string,
	updater func(*clustermetadatapb.Database) error,
) error {
	if s.updateDatabaseFieldsFunc != nil {
		return s.updateDatabaseFieldsFunc(ctx, dbName, updater)
	}
	return s.Store.UpdateDatabaseFields(ctx, dbName, updater)
}

func (s *mockTopologyStore) CreateCell(
	ctx context.Context,
	cellName string,
	cell *clustermetadatapb.Cell,
) error {
	if s.createCellFunc != nil {
		return s.createCellFunc(ctx, cellName, cell)
	}
	return s.Store.CreateCell(ctx, cellName, cell)
}

func (s *mockTopologyStore) UpdateCellFields(
	ctx context.Context,
	cellName string,
	updater func(*clustermetadatapb.Cell) error,
) error {
	if s.updateCellFieldsFunc != nil {
		return s.updateCellFieldsFunc(ctx, cellName, updater)
	}
	return s.Store.UpdateCellFields(ctx, cellName, updater)
}

func (s *mockTopologyStore) GetDatabaseNames(ctx context.Context) ([]string, error) {
	if s.getDatabaseNamesFunc != nil {
		return s.getDatabaseNamesFunc(ctx)
	}
	return s.Store.GetDatabaseNames(ctx)
}

func (s *mockTopologyStore) DeleteDatabase(ctx context.Context, dbName string, force bool) error {
	if s.deleteDatabaseFunc != nil {
		return s.deleteDatabaseFunc(ctx, dbName, force)
	}
	return s.Store.DeleteDatabase(ctx, dbName, force)
}

func (s *mockTopologyStore) GetCellNames(ctx context.Context) ([]string, error) {
	if s.getCellNamesFunc != nil {
		return s.getCellNamesFunc(ctx)
	}
	return s.Store.GetCellNames(ctx)
}

func (s *mockTopologyStore) DeleteCell(ctx context.Context, cellName string, force bool) error {
	if s.deleteCellFunc != nil {
		return s.deleteCellFunc(ctx, cellName, force)
	}
	return s.Store.DeleteCell(ctx, cellName, force)
}

func TestRegisterDatabaseFromSpec(t *testing.T) {
	t.Parallel()

	owner := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
	}

	t.Run("creates database with filesystem backup", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)

		dbConfig := multigresv1alpha1.DatabaseConfig{
			Name: "mydb",
		}

		err := topo.RegisterDatabaseFromSpec(
			context.Background(), store, recorder, owner,
			dbConfig, []string{"cell1"}, nil, "",
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		db, err := store.GetDatabase(context.Background(), "mydb")
		if err != nil {
			t.Fatalf("database not found: %v", err)
		}
		if db.BootstrapDurabilityPolicy.GetPolicyName() != "AT_LEAST_2" {
			t.Errorf(
				"expected default durability AT_LEAST_2, got %s",
				db.BootstrapDurabilityPolicy.GetPolicyName(),
			)
		}
		fs := db.BackupLocation.GetFilesystem()
		if fs == nil || fs.Path != "/backups" {
			t.Errorf("expected filesystem backup at /backups, got %v", db.BackupLocation)
		}
	})

	t.Run("creates database with S3 backup", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)

		backup := &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
		}

		err := topo.RegisterDatabaseFromSpec(
			context.Background(), store, recorder, owner,
			multigresv1alpha1.DatabaseConfig{Name: "s3db"},
			[]string{"cell1"}, backup, "",
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		db, err := store.GetDatabase(context.Background(), "s3db")
		if err != nil {
			t.Fatalf("database not found: %v", err)
		}
		s3 := db.BackupLocation.GetS3()
		if s3 == nil || s3.Bucket != "my-bucket" {
			t.Errorf("expected S3 backup with bucket my-bucket, got %v", db.BackupLocation)
		}
	})

	t.Run("creates database with custom filesystem path", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)

		backup := &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
			Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
				Path: "/custom/path",
			},
		}

		err := topo.RegisterDatabaseFromSpec(
			context.Background(), store, recorder, owner,
			multigresv1alpha1.DatabaseConfig{Name: "fsdb"},
			[]string{"cell1"}, backup, "",
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		db, err := store.GetDatabase(context.Background(), "fsdb")
		if err != nil {
			t.Fatalf("database not found: %v", err)
		}
		fs := db.BackupLocation.GetFilesystem()
		if fs == nil || fs.Path != "/custom/path" {
			t.Errorf("expected filesystem backup at /custom/path, got %v", db.BackupLocation)
		}
	})

	t.Run("updates existing database on re-registration", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1", "cell2")
		recorder := record.NewFakeRecorder(10)
		ctx := context.Background()

		dbConfig := multigresv1alpha1.DatabaseConfig{Name: "upddb"}
		if err := topo.RegisterDatabaseFromSpec(
			ctx, store, recorder, owner, dbConfig,
			[]string{"cell1"}, nil, "",
		); err != nil {
			t.Fatalf("first registration: %v", err)
		}

		// Re-register with different cells.
		if err := topo.RegisterDatabaseFromSpec(
			ctx, store, recorder, owner, dbConfig,
			[]string{"cell1", "cell2"}, nil, "MULTI_CELL_AT_LEAST_2",
		); err != nil {
			t.Fatalf("re-registration: %v", err)
		}

		db, err := store.GetDatabase(ctx, "upddb")
		if err != nil {
			t.Fatalf("database not found: %v", err)
		}
		if len(db.Cells) != 2 {
			t.Errorf("expected 2 cells, got %d", len(db.Cells))
		}
		if db.BootstrapDurabilityPolicy.GetPolicyName() != "MULTI_CELL_AT_LEAST_2" {
			t.Errorf(
				"expected MULTI_CELL_AT_LEAST_2, got %s",
				db.BootstrapDurabilityPolicy.GetPolicyName(),
			)
		}
	})

	t.Run("creates database with encryption enabled", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)

		backup := &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
			Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
				Path: "/custom/path",
			},
			Encryption: &multigresv1alpha1.BackupEncryptionConfig{
				SecretName: "my-cipher-secret",
			},
		}

		err := topo.RegisterDatabaseFromSpec(
			context.Background(), store, recorder, owner,
			multigresv1alpha1.DatabaseConfig{Name: "encdb"},
			[]string{"cell1"}, backup, "",
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		db, err := store.GetDatabase(context.Background(), "encdb")
		if err != nil {
			t.Fatalf("database not found: %v", err)
		}
		if !db.BackupLocation.GetRequireInitialRepoEncryption() {
			t.Error("expected RequireInitialRepoEncryption=true")
		}
	})

	t.Run("update path propagates encryption flag", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)
		ctx := context.Background()

		dbConfig := multigresv1alpha1.DatabaseConfig{Name: "encupddb"}

		if err := topo.RegisterDatabaseFromSpec(
			ctx, store, recorder, owner, dbConfig,
			[]string{"cell1"}, nil, "",
		); err != nil {
			t.Fatalf("first registration: %v", err)
		}

		backup := &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
			Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
				Path: "/custom/path",
			},
			Encryption: &multigresv1alpha1.BackupEncryptionConfig{
				SecretName: "my-cipher-secret",
			},
		}

		if err := topo.RegisterDatabaseFromSpec(
			ctx, store, recorder, owner, dbConfig,
			[]string{"cell1"}, backup, "",
		); err != nil {
			t.Fatalf("re-registration: %v", err)
		}

		db, err := store.GetDatabase(ctx, "encupddb")
		if err != nil {
			t.Fatalf("database not found: %v", err)
		}
		if !db.BackupLocation.GetRequireInitialRepoEncryption() {
			t.Error("expected RequireInitialRepoEncryption=true after update")
		}
	})

	t.Run("uses database-level durability policy", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)

		dbConfig := multigresv1alpha1.DatabaseConfig{
			Name:             "durdb",
			DurabilityPolicy: "MULTI_CELL_AT_LEAST_2",
		}

		err := topo.RegisterDatabaseFromSpec(
			context.Background(), store, recorder, owner,
			dbConfig, []string{"cell1"}, nil, "AT_LEAST_2",
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		db, err := store.GetDatabase(context.Background(), "durdb")
		if err != nil {
			t.Fatalf("database not found: %v", err)
		}
		if db.BootstrapDurabilityPolicy.GetPolicyName() != "MULTI_CELL_AT_LEAST_2" {
			t.Errorf(
				"expected database-level policy MULTI_CELL_AT_LEAST_2, got %s",
				db.BootstrapDurabilityPolicy.GetPolicyName(),
			)
		}
	})

	t.Run("returns error on CreateDatabase failure", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			createDatabaseFunc: func(ctx context.Context, dbName string, db *clustermetadatapb.Database) error {
				return errors.New("fake create error")
			},
		}

		err := topo.RegisterDatabaseFromSpec(
			context.Background(), store, record.NewFakeRecorder(10), owner,
			multigresv1alpha1.DatabaseConfig{Name: "errdb"}, []string{"cell1"}, nil, "",
		)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error on UpdateDatabaseFields failure", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			createDatabaseFunc: func(ctx context.Context, dbName string, db *clustermetadatapb.Database) error {
				return topoclient.NewError(topoclient.NodeExists, "node exists")
			},
			updateDatabaseFieldsFunc: func(ctx context.Context, dbName string, updater func(*clustermetadatapb.Database) error) error {
				return errors.New("fake update error")
			},
		}

		err := topo.RegisterDatabaseFromSpec(
			context.Background(), store, record.NewFakeRecorder(10), owner,
			multigresv1alpha1.DatabaseConfig{Name: "errdb"}, []string{"cell1"}, nil, "",
		)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestRegisterCellFromSpec(t *testing.T) {
	t.Parallel()

	topoRef := multigresv1alpha1.GlobalTopoServerRef{
		Address:  "global:2379",
		RootPath: "/multigres/global",
	}
	owner := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
	}

	t.Run("creates new cell", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)

		localTopo := &multigresv1alpha1.LocalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{"http://cell2-local:2379"},
				RootPath:  "/multigres/cells/cell2",
			},
		}
		cellCfg := multigresv1alpha1.CellConfig{Name: "cell2"} // cell2 does not exist yet!
		err := topo.RegisterCellFromSpec(
			context.Background(), store, recorder, owner, cellCfg, localTopo, topoRef,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		cell, err := store.GetCell(context.Background(), "cell2")
		if err != nil {
			t.Fatalf("cell not found: %v", err)
		}
		if cell.Name != "cell2" {
			t.Errorf("expected cell2, got %s", cell.Name)
		}
		if !reflect.DeepEqual(cell.ServerAddresses, []string{"http://cell2-local:2379"}) {
			t.Errorf("expected local topo addresses, got %v", cell.ServerAddresses)
		}
		if cell.Root != "/multigres/cells/cell2" {
			t.Errorf("expected local topo root, got %s", cell.Root)
		}
	})

	t.Run("idempotent on re-registration", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)
		ctx := context.Background()

		cellCfg := multigresv1alpha1.CellConfig{Name: "cell1"}
		localTopo := &multigresv1alpha1.LocalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{"http://cell1-local:2379"},
				RootPath:  "/multigres/cells/cell1",
			},
		}
		if err := topo.RegisterCellFromSpec(
			ctx,
			store,
			recorder,
			owner,
			cellCfg,
			localTopo,
			topoRef,
		); err != nil {
			t.Fatalf("first: %v", err)
		}
		if err := topo.RegisterCellFromSpec(
			ctx,
			store,
			recorder,
			owner,
			cellCfg,
			localTopo,
			topoRef,
		); err != nil {
			t.Fatalf("second: %v", err)
		}
	})

	t.Run("updates stale cell topology on re-registration", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)
		ctx := context.Background()

		if err := store.UpdateCellFields(
			ctx,
			"cell1",
			func(existing *clustermetadatapb.Cell) error {
				existing.ServerAddresses = []string{"http://stale-cell1-local:2379"}
				existing.Root = "/stale/cell1"
				return nil
			},
		); err != nil {
			t.Fatalf("seeding stale cell: %v", err)
		}

		localTopo := &multigresv1alpha1.LocalTopoServerSpec{
			External: &multigresv1alpha1.ExternalTopoServerSpec{
				Endpoints: []multigresv1alpha1.EndpointUrl{
					"http://cell1-local-a:2379",
					"http://cell1-local-b:2379",
				},
				RootPath: "/multigres/cells/cell1",
			},
		}
		if err := topo.RegisterCellFromSpec(
			ctx,
			store,
			recorder,
			owner,
			multigresv1alpha1.CellConfig{Name: "cell1"},
			localTopo,
			topoRef,
		); err != nil {
			t.Fatalf("re-registration should update stale cell, got: %v", err)
		}

		cell, err := store.GetCell(ctx, "cell1")
		if err != nil {
			t.Fatalf("cell not found: %v", err)
		}
		if !reflect.DeepEqual(
			cell.ServerAddresses,
			[]string{"http://cell1-local-a:2379", "http://cell1-local-b:2379"},
		) {
			t.Errorf("expected local topo addresses, got %v", cell.ServerAddresses)
		}
		if cell.Root != "/multigres/cells/cell1" {
			t.Errorf("expected local topo root, got %s", cell.Root)
		}
	})

	t.Run("falls back to global topology when no local topology is configured", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)

		err := topo.RegisterCellFromSpec(
			context.Background(),
			store,
			recorder,
			owner,
			multigresv1alpha1.CellConfig{Name: "cell2"},
			nil,
			topoRef,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		cell, err := store.GetCell(context.Background(), "cell2")
		if err != nil {
			t.Fatalf("cell not found: %v", err)
		}
		if !reflect.DeepEqual(cell.ServerAddresses, []string{"global:2379"}) {
			t.Errorf("expected global topo address fallback, got %v", cell.ServerAddresses)
		}
		if cell.Root != "/multigres/global" {
			t.Errorf("expected global topo root fallback, got %s", cell.Root)
		}
	})

	t.Run("uses managed local topology address for etcd local topology", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)

		localTopo := &multigresv1alpha1.LocalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{
				RootPath: "/multigres/cells/cell2",
			},
		}
		managedAddress := topo.ManagedLocalTopoServerAddress("cluster-cell2", "default")
		err := topo.RegisterCellFromSpec(
			context.Background(),
			store,
			recorder,
			owner,
			multigresv1alpha1.CellConfig{Name: "cell2"},
			localTopo,
			topoRef,
			managedAddress,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		cell, err := store.GetCell(context.Background(), "cell2")
		if err != nil {
			t.Fatalf("cell not found: %v", err)
		}
		if !reflect.DeepEqual(cell.ServerAddresses, []string{managedAddress}) {
			t.Errorf("expected managed local topo address, got %v", cell.ServerAddresses)
		}
		if cell.Root != "/multigres/cells/cell2" {
			t.Errorf("expected managed local topo root, got %s", cell.Root)
		}
	})

	t.Run("returns error on CreateCell failure", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			createCellFunc: func(ctx context.Context, cellName string, cell *clustermetadatapb.Cell) error {
				return errors.New("fake create error")
			},
		}

		err := topo.RegisterCellFromSpec(
			context.Background(), store, record.NewFakeRecorder(10), owner,
			multigresv1alpha1.CellConfig{Name: "cell1"}, nil, topoRef,
		)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestPruneDatabases(t *testing.T) {
	t.Parallel()

	owner := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
	}

	t.Run("removes stale database", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)
		ctx := context.Background()

		// Register two databases.
		for _, name := range []string{"db1", "db2"} {
			err := topo.RegisterDatabaseFromSpec(
				ctx, store, recorder, owner,
				multigresv1alpha1.DatabaseConfig{Name: multigresv1alpha1.DatabaseName(name)},
				[]string{"cell1"}, nil, "",
			)
			if err != nil {
				t.Fatalf("registering %s: %v", name, err)
			}
		}

		// Prune, keeping only db1.
		if err := topo.PruneDatabases(ctx, store, recorder, owner, []string{"db1"}); err != nil {
			t.Fatalf("prune: %v", err)
		}

		if _, err := store.GetDatabase(ctx, "db1"); err != nil {
			t.Error("db1 should still exist")
		}
		if _, err := store.GetDatabase(ctx, "db2"); err == nil {
			t.Error("db2 should have been pruned")
		}
	})

	t.Run("no-op when all databases active", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)
		ctx := context.Background()

		err := topo.RegisterDatabaseFromSpec(
			ctx, store, recorder, owner,
			multigresv1alpha1.DatabaseConfig{Name: "db1"},
			[]string{"cell1"}, nil, "",
		)
		if err != nil {
			t.Fatalf("registering: %v", err)
		}

		if err := topo.PruneDatabases(ctx, store, recorder, owner, []string{"db1"}); err != nil {
			t.Fatalf("prune: %v", err)
		}

		if _, err := store.GetDatabase(ctx, "db1"); err != nil {
			t.Error("db1 should still exist")
		}
	})

	t.Run("returns error on getting database names", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			getDatabaseNamesFunc: func(ctx context.Context) ([]string, error) {
				return nil, errors.New("fake list error")
			},
		}

		err := topo.PruneDatabases(
			context.Background(),
			store,
			record.NewFakeRecorder(10),
			owner,
			nil,
		)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("skips if database delete returns NoNode", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			getDatabaseNamesFunc: func(ctx context.Context) ([]string, error) {
				return []string{"staledb"}, nil
			},
			deleteDatabaseFunc: func(ctx context.Context, dbName string, force bool) error {
				return topoclient.NewError(topoclient.NoNode, "no node")
			},
		}

		err := topo.PruneDatabases(
			context.Background(),
			store,
			record.NewFakeRecorder(10),
			owner,
			nil,
		)
		if err != nil {
			t.Fatalf("expected nil as NoNode is skipped, got %v", err)
		}
	})

	t.Run("returns error on deleting database", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			getDatabaseNamesFunc: func(ctx context.Context) ([]string, error) {
				return []string{"staledb"}, nil
			},
			deleteDatabaseFunc: func(ctx context.Context, dbName string, force bool) error {
				return errors.New("fake delete error")
			},
		}

		err := topo.PruneDatabases(
			context.Background(),
			store,
			record.NewFakeRecorder(10),
			owner,
			nil,
		)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestPruneCells(t *testing.T) {
	t.Parallel()

	topoRef := multigresv1alpha1.GlobalTopoServerRef{
		Address:  "global:2379",
		RootPath: "/multigres/global",
	}
	owner := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"},
	}

	t.Run("removes stale cell", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1", "cell2")
		recorder := record.NewFakeRecorder(10)
		ctx := context.Background()

		for _, c := range []string{"cell1", "cell2"} {
			if err := topo.RegisterCellFromSpec(
				ctx, store, recorder, owner,
				multigresv1alpha1.CellConfig{Name: multigresv1alpha1.CellName(c)}, nil, topoRef,
			); err != nil {
				t.Fatalf("registering %s: %v", c, err)
			}
		}

		// Prune, keeping only cell1.
		if err := topo.PruneCells(ctx, store, recorder, owner, []string{"cell1"}); err != nil {
			t.Fatalf("prune: %v", err)
		}

		if _, err := store.GetCell(ctx, "cell1"); err != nil {
			t.Error("cell1 should still exist")
		}
		if _, err := store.GetCell(ctx, "cell2"); err == nil {
			t.Error("cell2 should have been pruned")
		}
	})

	t.Run("no-op when all cells active", func(t *testing.T) {
		t.Parallel()
		store := newMemoryStore(t, "cell1")
		recorder := record.NewFakeRecorder(10)
		ctx := context.Background()

		if err := topo.RegisterCellFromSpec(
			ctx, store, recorder, owner,
			multigresv1alpha1.CellConfig{Name: "cell1"}, nil, topoRef,
		); err != nil {
			t.Fatalf("registering: %v", err)
		}

		if err := topo.PruneCells(ctx, store, recorder, owner, []string{"cell1"}); err != nil {
			t.Fatalf("prune: %v", err)
		}

		if _, err := store.GetCell(ctx, "cell1"); err != nil {
			t.Error("cell1 should still exist")
		}
	})

	t.Run("returns error on getting cell names", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			getCellNamesFunc: func(ctx context.Context) ([]string, error) {
				return nil, errors.New("fake list error")
			},
		}

		err := topo.PruneCells(context.Background(), store, record.NewFakeRecorder(10), owner, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("skips if cell delete returns NoNode", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			getCellNamesFunc: func(ctx context.Context) ([]string, error) {
				return []string{"stalecell"}, nil
			},
			deleteCellFunc: func(ctx context.Context, cellName string, force bool) error {
				return topoclient.NewError(topoclient.NoNode, "no node")
			},
		}

		err := topo.PruneCells(context.Background(), store, record.NewFakeRecorder(10), owner, nil)
		if err != nil {
			t.Fatalf("expected nil as NoNode is skipped, got %v", err)
		}
	})

	t.Run("returns error on deleting cell", func(t *testing.T) {
		t.Parallel()
		store := &mockTopologyStore{
			getCellNamesFunc: func(ctx context.Context) ([]string, error) {
				return []string{"stalecell"}, nil
			},
			deleteCellFunc: func(ctx context.Context, cellName string, force bool) error {
				return errors.New("fake delete error")
			},
		}

		err := topo.PruneCells(context.Background(), store, record.NewFakeRecorder(10), owner, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
