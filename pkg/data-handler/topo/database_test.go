package topo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"

	"github.com/multigres/testkit/assert"
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
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")

		c.Require().
			NoError(topo.RegisterDatabase(context.Background(), store, recorder, shard), "unexpected error")

		db, err := store.GetDatabase(context.Background(), "test-db")
		c.Require().NoError(err, "database not found in topo after registration")
		c.Eq("test-db", db.Name, "expected database name test-db, got")
	})

	t.Run("updates existing database on re-registration", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1", "cell2")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")
		ctx := context.Background()

		c.Require().
			NoError(topo.RegisterDatabase(ctx, store, recorder, shard), "first registration failed")

		// Modify shard to add a second cell, re-register should update.
		shard.Spec.Pools["pool2"] = multigresv1alpha1.PoolSpec{
			Cells: []multigresv1alpha1.CellName{"cell2"},
		}
		c.Require().
			NoError(topo.RegisterDatabase(ctx, store, recorder, shard), "second registration (update) failed")

		db, err := store.GetDatabase(ctx, "test-db")
		c.Require().NoError(err, "database not found after update")
		c.Len(db.Cells, 2, "expected 2 cells after update, got %d", len(db.Cells))
	})

	t.Run("syncs durability policy on re-registration", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")
		ctx := context.Background()

		// First registration with default AT_LEAST_2.
		c.Require().
			NoError(topo.RegisterDatabase(ctx, store, recorder, shard), "first registration failed")
		db, err := store.GetDatabase(ctx, "test-db")
		c.Require().NoError(err, "database not found")
		c.Eq(
			"AT_LEAST_2",
			db.BootstrapDurabilityPolicy.GetPolicyName(),
			"expected AT_LEAST_2 after first registration, got",
		)

		// Change to MULTI_CELL_AT_LEAST_2 and re-register.
		shard.Spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"
		c.Require().
			NoError(topo.RegisterDatabase(ctx, store, recorder, shard), "second registration failed")
		db, err = store.GetDatabase(ctx, "test-db")
		c.Require().NoError(err, "database not found after update")
		c.Eq(
			"MULTI_CELL_AT_LEAST_2",
			db.BootstrapDurabilityPolicy.GetPolicyName(),
			"expected MULTI_CELL_AT_LEAST_2 after update, got",
		)
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
		assert.NewAborting(t).Error(err, "expected error, got nil")
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
		assert.NewAborting(t).Error(err, "expected error, got nil")
	})
}

func TestUnregisterDatabase(t *testing.T) {
	t.Parallel()

	t.Run("removes existing database", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		)
		defer func() { _ = store.Close() }()

		recorder := record.NewFakeRecorder(10)
		shard := newTestShard("test-shard")
		ctx := context.Background()

		c.Require().
			NoError(topo.RegisterDatabase(ctx, store, recorder, shard), "registration failed")
		c.Require().
			NoError(topo.UnregisterDatabase(ctx, store, recorder, shard), "unregistration failed")

		_, err := store.GetDatabase(ctx, "test-db")
		c.Error(err, "expected database to be gone after unregistration")
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

		assert.NewAborting(t).NoError(topo.UnregisterDatabase(
			context.Background(),
			store,
			recorder,
			shard,
		), "unregistering nonexistent database should succeed, got")
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
		assert.NewAborting(t).Error(err, "expected error, got nil")
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
		assert.NewAborting(t).Error(err, "expected error, got nil")
	})
}

func TestGetDurabilityPolicy(t *testing.T) {
	t.Parallel()

	t.Run("defaults to AT_LEAST_2 when empty", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := newTestShard("test-shard")
		got, err := topo.GetDurabilityPolicy(shard)
		c.Require().NoError(err, "unexpected error")
		c.Eq("AT_LEAST_2", got.GetPolicyName(), "expected AT_LEAST_2, got")
		c.Eq(
			clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			got.GetQuorumType(),
			"expected QUORUM_TYPE_AT_LEAST_N, got",
		)
		c.Eq(2, got.GetRequiredCount(), "expected RequiredCount 2, got")
	})

	t.Run("returns explicit AT_LEAST_2", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := newTestShard("test-shard")
		shard.Spec.DurabilityPolicy = "AT_LEAST_2"
		got, err := topo.GetDurabilityPolicy(shard)
		c.Require().NoError(err, "unexpected error")
		c.Eq("AT_LEAST_2", got.GetPolicyName(), "expected AT_LEAST_2, got")
		c.Eq(
			clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			got.GetQuorumType(),
			"expected QUORUM_TYPE_AT_LEAST_N, got",
		)
		c.Eq(2, got.GetRequiredCount(), "expected RequiredCount 2, got")
	})

	t.Run("returns MULTI_CELL_AT_LEAST_2 when set", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := newTestShard("test-shard")
		shard.Spec.DurabilityPolicy = "MULTI_CELL_AT_LEAST_2"
		got, err := topo.GetDurabilityPolicy(shard)
		c.Require().NoError(err, "unexpected error")
		c.Eq("MULTI_CELL_AT_LEAST_2", got.GetPolicyName(), "expected MULTI_CELL_AT_LEAST_2, got")
		c.Eq(
			clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
			got.GetQuorumType(),
			"expected QUORUM_TYPE_MULTI_CELL_AT_LEAST_N, got",
		)
		c.Eq(2, got.GetRequiredCount(), "expected RequiredCount 2, got")
	})

	t.Run("unknown policy returns error", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := newTestShard("test-shard")
		shard.Spec.DurabilityPolicy = "CUSTOM"
		got, err := topo.GetDurabilityPolicy(shard)
		c.Require().Error(err, "expected error, got nil")
		c.Nil(got, "expected nil policy on error, got")
		msg := err.Error()
		for _, want := range []string{"CUSTOM", "AT_LEAST_2", "MULTI_CELL_AT_LEAST_2"} {
			c.StrContains(msg, want, "expected error message to contain")
		}
	})
}

func TestGetBackupLocation(t *testing.T) {
	t.Parallel()

	t.Run("S3 backup", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
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
		c.Require().NotNil(s3, "expected S3 backup location")
		c.Eq("my-bucket", s3.Bucket, "expected bucket my-bucket, got")
		c.Eq("us-west-2", s3.Region, "expected region us-west-2, got")
		c.Eq("https://s3.example.com", s3.Endpoint, "expected endpoint, got")
		c.Eq("prefix/", s3.KeyPrefix, "expected key prefix, got")
		c.True(s3.UseEnvCredentials, "expected UseEnvCredentials=true")
	})

	t.Run("filesystem with custom path", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
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
		c.Require().NotNil(fs, "expected filesystem backup location")
		c.Eq("/custom/backups", fs.Path, "expected path /custom/backups, got")
	})

	t.Run("default filesystem", func(t *testing.T) {
		t.Parallel()
		c := assert.NewCollecting(t)
		shard := &multigresv1alpha1.Shard{}
		loc := topo.GetBackupLocation(shard)
		fs := loc.GetFilesystem()
		c.Require().NotNil(fs, "expected filesystem backup location")
		c.Eq("/backups", fs.Path, "expected default path /backups, got")
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
		assert.NewCollecting(t).
			True(loc.GetRequireInitialRepoEncryption(), "expected RequireInitialRepoEncryption=true")
	})

	t.Run("encryption not set", func(t *testing.T) {
		t.Parallel()
		shard := &multigresv1alpha1.Shard{}
		loc := topo.GetBackupLocation(shard)
		assert.NewCollecting(t).
			False(loc.GetRequireInitialRepoEncryption(), "expected RequireInitialRepoEncryption=false")
	})
}
