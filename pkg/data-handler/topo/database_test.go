package topo_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
)

func newTestShard(name string) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "test-db",
			TableGroupName: "test-tg",
			ShardName:      "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:  "localhost:2379",
				RootPath: "/test",
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}
}

type mockDatabaseTopoStore struct {
	topoclient.Store
	createDatabaseFunc       func(ctx context.Context, dbName string, db *clustermetadatapb.Database) error
	updateDatabaseFieldsFunc func(ctx context.Context, dbName string, updater func(*clustermetadatapb.Database) error) error
	deleteDatabaseFunc       func(ctx context.Context, dbName string, force bool) error
}

func (m *mockDatabaseTopoStore) CreateDatabase(
	ctx context.Context,
	dbName string,
	db *clustermetadatapb.Database,
) error {
	if m.createDatabaseFunc != nil {
		return m.createDatabaseFunc(ctx, dbName, db)
	}
	return nil
}

func (m *mockDatabaseTopoStore) UpdateDatabaseFields(
	ctx context.Context,
	dbName string,
	updater func(*clustermetadatapb.Database) error,
) error {
	if m.updateDatabaseFieldsFunc != nil {
		return m.updateDatabaseFieldsFunc(ctx, dbName, updater)
	}
	return nil
}

func (m *mockDatabaseTopoStore) DeleteDatabase(
	ctx context.Context,
	dbName string,
	force bool,
) error {
	if m.deleteDatabaseFunc != nil {
		return m.deleteDatabaseFunc(ctx, dbName, force)
	}
	return nil
}

func TestRegisterDatabase(t *testing.T) {
	t.Parallel()

	t.Run("creates new database in topology", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")

		if err := topo.RegisterDatabase(context.Background(), store, recorder, shard); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		db, err := store.GetDatabase(context.Background(), "test-db")
		if err != nil {
			t.Fatalf("database not found in topo after registration: %v", err)
		}
		if db.Name != "test-db" {
			t.Errorf("expected database name test-db, got %s", db.Name)
		}
	})

	t.Run("updates existing database on re-registration", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1", "cell2")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")
		ctx := context.Background()

		if err := topo.RegisterDatabase(ctx, store, recorder, shard); err != nil {
			t.Fatalf("first registration failed: %v", err)
		}

		// Modify shard to add a second cell, re-register should update.
		shard.Spec.Pools["pool2"] = multigresv1alpha1.PoolSpec{
			Cells: []multigresv1alpha1.CellName{"cell2"},
		}
		if err := topo.RegisterDatabase(ctx, store, recorder, shard); err != nil {
			t.Fatalf("second registration (update) failed: %v", err)
		}

		db, err := store.GetDatabase(ctx, "test-db")
		if err != nil {
			t.Fatalf("database not found after update: %v", err)
		}
		if len(db.Cells) != 2 {
			t.Errorf("expected 2 cells after update, got %d: %v", len(db.Cells), db.Cells)
		}
	})

	t.Run("syncs durability policy on re-registration", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")
		ctx := context.Background()

		// First registration with default AT_LEAST_2.
		if err := topo.RegisterDatabase(ctx, store, recorder, shard); err != nil {
			t.Fatalf("first registration failed: %v", err)
		}
		db, err := store.GetDatabase(ctx, "test-db")
		if err != nil {
			t.Fatalf("database not found: %v", err)
		}
		if db.BootstrapDurabilityPolicy.GetPolicyName() != "AT_LEAST_2" {
			t.Errorf(
				"expected AT_LEAST_2 after first registration, got %s",
				db.BootstrapDurabilityPolicy.GetPolicyName(),
			)
		}

		// Change to MULTI_CELL_AT_LEAST_2 and re-register.
		shard.Spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"
		if err := topo.RegisterDatabase(ctx, store, recorder, shard); err != nil {
			t.Fatalf("second registration failed: %v", err)
		}
		db, err = store.GetDatabase(ctx, "test-db")
		if err != nil {
			t.Fatalf("database not found after update: %v", err)
		}
		if db.BootstrapDurabilityPolicy.GetPolicyName() != "MULTI_CELL_AT_LEAST_2" {
			t.Errorf(
				"expected MULTI_CELL_AT_LEAST_2 after update, got %s",
				db.BootstrapDurabilityPolicy.GetPolicyName(),
			)
		}
	})

	t.Run("returns error on creation failure", func(t *testing.T) {
		t.Parallel()
		store := &mockDatabaseTopoStore{
			createDatabaseFunc: func(ctx context.Context, dbName string, db *clustermetadatapb.Database) error {
				return fmt.Errorf("fake creation error")
			},
		}

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")

		err := topo.RegisterDatabase(context.Background(), store, recorder, shard)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error on UpdateDatabaseFields failure", func(t *testing.T) {
		t.Parallel()
		store := &mockDatabaseTopoStore{
			createDatabaseFunc: func(ctx context.Context, dbName string, db *clustermetadatapb.Database) error {
				return topoclient.NewError(topoclient.NodeExists, "node exists")
			},
			updateDatabaseFieldsFunc: func(ctx context.Context, dbName string, updater func(*clustermetadatapb.Database) error) error {
				return fmt.Errorf("fake update error")
			},
		}

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")

		err := topo.RegisterDatabase(context.Background(), store, recorder, shard)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestUnregisterDatabase(t *testing.T) {
	t.Parallel()

	t.Run("removes existing database", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")
		ctx := context.Background()

		if err := topo.RegisterDatabase(ctx, store, recorder, shard); err != nil {
			t.Fatalf("registration failed: %v", err)
		}
		if err := topo.UnregisterDatabase(ctx, store, recorder, shard); err != nil {
			t.Fatalf("unregistration failed: %v", err)
		}

		_, err := store.GetDatabase(ctx, "test-db")
		if err == nil {
			t.Error("expected database to be gone after unregistration")
		}
	})

	t.Run("idempotent when database does not exist", func(t *testing.T) {
		t.Parallel()
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")

		if err := topo.UnregisterDatabase(
			context.Background(),
			store,
			recorder,
			shard,
		); err != nil {
			t.Fatalf("unregistering nonexistent database should succeed, got: %v", err)
		}
	})

	t.Run("returns error on failure other than TopoUnavailable", func(t *testing.T) {
		t.Parallel()
		store := &mockDatabaseTopoStore{
			deleteDatabaseFunc: func(ctx context.Context, dbName string, force bool) error {
				return fmt.Errorf("some other error")
			},
		}

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")

		err := topo.UnregisterDatabase(context.Background(), store, recorder, shard)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("returns error on TopoUnavailable", func(t *testing.T) {
		t.Parallel()
		store := &mockDatabaseTopoStore{
			deleteDatabaseFunc: func(ctx context.Context, dbName string, force bool) error {
				return fmt.Errorf("fake UNAVAILABLE error")
			},
		}

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")

		err := topo.UnregisterDatabase(context.Background(), store, recorder, shard)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestGetDurabilityPolicy(t *testing.T) {
	t.Parallel()

	t.Run("defaults to AT_LEAST_2 when empty", func(t *testing.T) {
		t.Parallel()
		shard := newTestShard("test-shard")
		got, err := topo.GetDurabilityPolicy(shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.GetPolicyName() != "AT_LEAST_2" {
			t.Errorf("expected AT_LEAST_2, got %s", got.GetPolicyName())
		}
		if got.GetQuorumType() != clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N {
			t.Errorf("expected QUORUM_TYPE_AT_LEAST_N, got %s", got.GetQuorumType())
		}
		if got.GetRequiredCount() != 2 {
			t.Errorf("expected RequiredCount 2, got %d", got.GetRequiredCount())
		}
	})

	t.Run("returns explicit AT_LEAST_2", func(t *testing.T) {
		t.Parallel()
		shard := newTestShard("test-shard")
		shard.Spec.DurabilityPolicy = "AT_LEAST_2"
		got, err := topo.GetDurabilityPolicy(shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.GetPolicyName() != "AT_LEAST_2" {
			t.Errorf("expected AT_LEAST_2, got %s", got.GetPolicyName())
		}
		if got.GetQuorumType() != clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N {
			t.Errorf("expected QUORUM_TYPE_AT_LEAST_N, got %s", got.GetQuorumType())
		}
		if got.GetRequiredCount() != 2 {
			t.Errorf("expected RequiredCount 2, got %d", got.GetRequiredCount())
		}
	})

	t.Run("returns MULTI_CELL_AT_LEAST_2 when set", func(t *testing.T) {
		t.Parallel()
		shard := newTestShard("test-shard")
		shard.Spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"
		got, err := topo.GetDurabilityPolicy(shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.GetPolicyName() != "MULTI_CELL_AT_LEAST_2" {
			t.Errorf("expected MULTI_CELL_AT_LEAST_2, got %s", got.GetPolicyName())
		}
		if got.GetQuorumType() != clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N {
			t.Errorf("expected QUORUM_TYPE_MULTI_CELL_AT_LEAST_N, got %s", got.GetQuorumType())
		}
		if got.GetRequiredCount() != 2 {
			t.Errorf("expected RequiredCount 2, got %d", got.GetRequiredCount())
		}
	})

	t.Run("unknown policy returns error", func(t *testing.T) {
		t.Parallel()
		shard := newTestShard("test-shard")
		shard.Spec.DurabilityPolicy = "CUSTOM"
		got, err := topo.GetDurabilityPolicy(shard)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if got != nil {
			t.Errorf("expected nil policy on error, got %+v", got)
		}
		msg := err.Error()
		for _, want := range []string{"CUSTOM", "AT_LEAST_2", "MULTI_CELL_AT_LEAST_2"} {
			if !strings.Contains(msg, want) {
				t.Errorf("expected error message to contain %q, got %q", want, msg)
			}
		}
	})
}

func TestGetBackupLocation(t *testing.T) {
	t.Parallel()

	t.Run("S3 backup", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeS3,
					S3: &multigresv1alpha1.S3BackupConfig{
						Bucket:            "my-bucket",
						Region:            "us-west-2",
						Endpoint:          "https://s3.example.com",
						KeyPrefix:         "prefix/",
						UseEnvCredentials: true,
					},
				},
			},
		}
		loc := topo.GetBackupLocation(shard)
		s3 := loc.GetS3()
		if s3 == nil {
			t.Fatal("expected S3 backup location")
		}
		if s3.Bucket != "my-bucket" {
			t.Errorf("expected bucket my-bucket, got %s", s3.Bucket)
		}
		if s3.Region != "us-west-2" {
			t.Errorf("expected region us-west-2, got %s", s3.Region)
		}
		if s3.Endpoint != "https://s3.example.com" {
			t.Errorf("expected endpoint, got %s", s3.Endpoint)
		}
		if s3.KeyPrefix != "prefix/" {
			t.Errorf("expected key prefix, got %s", s3.KeyPrefix)
		}
		if !s3.UseEnvCredentials {
			t.Error("expected UseEnvCredentials=true")
		}
	})

	t.Run("filesystem with custom path", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
						Path: "/custom/backups",
					},
				},
			},
		}
		loc := topo.GetBackupLocation(shard)
		fs := loc.GetFilesystem()
		if fs == nil {
			t.Fatal("expected filesystem backup location")
		}
		if fs.Path != "/custom/backups" {
			t.Errorf("expected path /custom/backups, got %s", fs.Path)
		}
	})

	t.Run("default filesystem", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		loc := topo.GetBackupLocation(shard)
		fs := loc.GetFilesystem()
		if fs == nil {
			t.Fatal("expected filesystem backup location")
		}
		if fs.Path != "/backups" {
			t.Errorf("expected default path /backups, got %s", fs.Path)
		}
	})

	t.Run("encryption enabled", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
						Path: "/custom/backups",
					},
					Encryption: &multigresv1alpha1.BackupEncryptionConfig{
						SecretName: "my-cipher-secret",
					},
				},
			},
		}
		loc := topo.GetBackupLocation(shard)
		if !loc.GetRequireInitialRepoEncryption() {
			t.Error("expected RequireInitialRepoEncryption=true")
		}
	})

	t.Run("encryption not set", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		loc := topo.GetBackupLocation(shard)
		if loc.GetRequireInitialRepoEncryption() {
			t.Error("expected RequireInitialRepoEncryption=false")
		}
	})
}
