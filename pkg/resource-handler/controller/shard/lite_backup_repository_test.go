package shard

import (
	"context"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const liteTestBucket = "shared-bucket"

func liteShardWithBackup(name string, b *multigresv1alpha1.BackupConfig) *multigresv1alpha1.Shard {
	shard, pool := newTestLiteShard(), newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	shard.Name = name
	shard.Spec.Backup = b
	return shard
}

func s3Backup(keyPrefix string) *multigresv1alpha1.BackupConfig {
	return &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket:    liteTestBucket,
			Region:    "us-east-1",
			KeyPrefix: keyPrefix,
		},
	}
}

// liteTopo is one cluster's own topology: each Lite cluster has its own root.
func liteTopo(t *testing.T) topoclient.Store {
	t.Helper()
	_, factory := memorytopo.NewServerAndFactory(context.Background(), "z1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	t.Cleanup(func() { _ = store.Close() })
	return store
}

type liteRegistration struct {
	r        *ShardReconciler
	store    topoclient.Store
	recorder *record.FakeRecorder
}

func newLiteRegistration(t *testing.T, objects ...client.Object) *liteRegistration {
	t.Helper()
	store := liteTopo(t)
	recorder := record.NewFakeRecorder(10)
	scheme := reloadTestScheme(t)
	return &liteRegistration{
		r: &ShardReconciler{
			Client: fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				WithStatusSubresource(&multigresv1alpha1.Shard{}).
				Build(),
			Scheme:   scheme,
			Recorder: recorder,
			CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
				return nopCloseStore{store}, nil
			},
		},
		store:    store,
		recorder: recorder,
	}
}

// nopCloseStore keeps the shared test store open across registrations, which
// close the store they are handed.
type nopCloseStore struct{ topoclient.Store }

func (nopCloseStore) Close() error { return nil }

func (l *liteRegistration) register(t *testing.T, shard *multigresv1alpha1.Shard) error {
	t.Helper()
	poolName, cellName, _, err := litePool(shard)
	require.NoError(t, err)
	stored := &multigresv1alpha1.Shard{}
	if l.r.Get(t.Context(), client.ObjectKeyFromObject(shard), stored) != nil {
		require.NoError(t, l.r.Create(t.Context(), shard.DeepCopy()))
	}
	registered, err := l.r.registerLiteDatabase(t.Context(), shard, poolName, cellName)
	if err == nil {
		require.True(t, registered)
	}
	return err
}

// condition returns the stored BackupRepositoryValid condition, or nil.
func (l *liteRegistration) condition(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
) *metav1.Condition {
	t.Helper()
	stored := &multigresv1alpha1.Shard{}
	require.NoError(t, l.r.Get(t.Context(), client.ObjectKeyFromObject(shard), stored))
	return meta.FindStatusCondition(stored.Status.Conditions, conditionBackupRepositoryValid)
}

func (l *liteRegistration) record(t *testing.T) *cm.Database {
	t.Helper()
	db, err := l.store.GetDatabase(t.Context(), "postgres")
	require.NoError(t, err)
	return db
}

// repository is where the Lite pooler puts its pgBackRest repository, read
// from the database record it bootstraps from: the S3 bucket and key prefix,
// or the filesystem path. Upstream appends the fixed stanza name to either.
func repository(t *testing.T, db *cm.Database) string {
	t.Helper()
	if s3 := db.GetBackupLocation().GetS3(); s3 != nil {
		return "s3://" + s3.GetBucket() + "/" + s3.GetKeyPrefix()
	}
	return "file://" + db.GetBackupLocation().GetFilesystem().GetPath()
}

func TestLiteBackupRepositoryCheck(t *testing.T) {
	t.Run("new shard without a key prefix is refused before registration", func(t *testing.T) {
		l := newLiteRegistration(t)
		shard := liteShardWithBackup("new", s3Backup(""))
		err := l.register(t, shard)
		require.ErrorIs(t, err, errLiteDefaultS3Repository)
		cond := l.condition(t, shard)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionFalse, cond.Status)
		require.Equal(t, reasonDefaultS3Repository, cond.Reason)

		_, getErr := l.store.GetDatabase(t.Context(), "postgres")
		require.ErrorIs(t, getErr, &topoclient.TopoError{Code: topoclient.NoNode},
			"no database record may be written")
		require.Contains(t, <-l.recorder.Events, "keyPrefix")
	})

	t.Run("a prefix made only of slashes is refused", func(t *testing.T) {
		l := newLiteRegistration(t)
		err := l.register(t, liteShardWithBackup("new", s3Backup("/")))
		require.ErrorIs(t, err, errLiteDefaultS3Repository)
	})

	t.Run("explicit prefix is registered unchanged", func(t *testing.T) {
		l := newLiteRegistration(t)
		require.NoError(t, l.register(t, liteShardWithBackup("new", s3Backup("tenant-a"))))
		require.Equal(t, "tenant-a", l.record(t).GetBackupLocation().GetS3().GetKeyPrefix())
		require.Equal(t, "s3://"+liteTestBucket+"/tenant-a", repository(t, l.record(t)))
	})

	t.Run("filesystem backups are not affected", func(t *testing.T) {
		l := newLiteRegistration(t)
		fs := &multigresv1alpha1.BackupConfig{Type: multigresv1alpha1.BackupTypeFilesystem}
		require.NoError(t, l.register(t, liteShardWithBackup("new", fs)))
		require.Equal(t, "file:///backups", repository(t, l.record(t)))
	})

	// An existing cluster registered before the check keeps /multigres on
	// every later reconcile, so its backups and WAL do not move.
	t.Run("existing record keeps the default repository", func(t *testing.T) {
		l := newLiteRegistration(t)
		shard := liteShardWithBackup("existing", s3Backup(""))
		require.NoError(t, l.store.CreateDatabase(t.Context(), "postgres", &cm.Database{
			Name:  "postgres",
			Cells: []string{"z1"},
			BackupLocation: &cm.BackupLocation{Location: &cm.BackupLocation_S3{
				S3: &cm.S3Backup{Bucket: liteTestBucket, Region: "us-east-1"},
			}},
		}))
		before := repository(t, l.record(t))

		for range 2 {
			require.NoError(t, l.register(t, shard))
			require.Equal(t, before, repository(t, l.record(t)))
		}
		cond := l.condition(t, shard)
		require.NotNil(t, cond)
		require.Equal(t, metav1.ConditionTrue, cond.Status)
		require.Equal(t, reasonExistingDefaultRepository, cond.Reason)
		require.Equal(t, "s3://"+liteTestBucket+"/", before)
	})

	// An existing shard may keep its default repository but not move into
	// another one: a different bucket's, or one it did not use before.
	t.Run("existing record cannot move into a default repository", func(t *testing.T) {
		for name, registered := range map[string]*cm.BackupLocation{
			"other bucket": {Location: &cm.BackupLocation_S3{
				S3: &cm.S3Backup{Bucket: "other-bucket", Region: "us-east-1"},
			}},
			"other endpoint": {Location: &cm.BackupLocation_S3{
				S3: &cm.S3Backup{Bucket: liteTestBucket, Region: "us-east-1", Endpoint: "https://minio:9000"},
			}},
			"had a prefix": {Location: &cm.BackupLocation_S3{
				S3: &cm.S3Backup{Bucket: liteTestBucket, Region: "us-east-1", KeyPrefix: "tenant-a"},
			}},
			"was filesystem": {Location: &cm.BackupLocation_Filesystem{
				Filesystem: &cm.FilesystemBackup{Path: "/backups"},
			}},
		} {
			t.Run(name, func(t *testing.T) {
				l := newLiteRegistration(t)
				require.NoError(t, l.store.CreateDatabase(t.Context(), "postgres", &cm.Database{
					Name: "postgres", Cells: []string{"z1"}, BackupLocation: registered,
				}))
				before := repository(t, l.record(t))

				shard := liteShardWithBackup("existing", s3Backup(""))
				err := l.register(t, shard)
				require.ErrorIs(t, err, errLiteMoveToDefaultS3Repository)
				require.Equal(t, reasonMoveToDefaultS3Repository, l.condition(t, shard).Reason)
				require.Equal(t, before, repository(t, l.record(t)), "the record must not change")
			})
		}
	})

	// The data PVC proves the shard ran before, even when its topology record
	// is gone, so the check does not strand a provisioned cluster.
	t.Run("existing data PVC keeps the default repository", func(t *testing.T) {
		shard := liteShardWithBackup("existing", s3Backup(""))
		poolName, cellName, pool, err := litePool(shard)
		require.NoError(t, err)
		pvc, err := BuildPoolDataPVC(shard, poolName, cellName, pool, 0, false, reloadTestScheme(t))
		require.NoError(t, err)

		l := newLiteRegistration(t, pvc)
		require.NoError(t, l.register(t, shard))
		require.Equal(t, "s3://"+liteTestBucket+"/", repository(t, l.record(t)))
	})

	// Two Lite clusters on one bucket, each with its own topology root: with
	// key prefixes their repositories differ, and without them the second one
	// is refused instead of sharing the first one's repository.
	t.Run("two clusters sharing a bucket", func(t *testing.T) {
		a, b := newLiteRegistration(t), newLiteRegistration(t)
		require.NoError(t, a.register(t, liteShardWithBackup("a", s3Backup("cluster-a"))))
		require.NoError(t, b.register(t, liteShardWithBackup("b", s3Backup("cluster-b"))))
		require.NotEqual(t, repository(t, a.record(t)), repository(t, b.record(t)))

		c, d := newLiteRegistration(t), newLiteRegistration(t)
		require.ErrorIs(
			t,
			c.register(t, liteShardWithBackup("c", s3Backup(""))),
			errLiteDefaultS3Repository,
		)
		require.ErrorIs(
			t,
			d.register(t, liteShardWithBackup("d", s3Backup(""))),
			errLiteDefaultS3Repository,
		)
	})
}

// A refused shard gets no data PVC and no pod: reconcileLite stops at the
// registration step, before anything that would hold data exists.
func TestReconcileLiteRefusesDefaultS3Repository(t *testing.T) {
	ctx := t.Context()
	shard, pool := newTestLiteShard(), newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	shard.Spec.Backup = s3Backup("")
	rendered := reloadTestRendered()
	shard.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: rendered.restartHash,
		metadata.AnnotationPostgresReloadHash: rendered.reloadHash,
	}
	scheme := reloadTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()
	store := liteTopo(t)
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
		CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return nopCloseStore{store}, nil
		},
	}

	_, err := r.reconcileLite(ctx, shard, rendered, storageClassCheck{})
	require.ErrorIs(t, err, errLiteDefaultS3Repository)

	pvcs := &corev1.PersistentVolumeClaimList{}
	require.NoError(t, c.List(ctx, pvcs))
	require.Empty(t, pvcs.Items)
	pods := &corev1.PodList{}
	require.NoError(t, c.List(ctx, pods))
	require.Empty(t, pods.Items)

	stored := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), stored))
	cond := meta.FindStatusCondition(stored.Status.Conditions, conditionBackupRepositoryValid)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)

	// Setting a prefix lets the shard provision and clears the refusal.
	stored.Spec.Backup = s3Backup("tenant-a")
	_, err = r.reconcileLite(ctx, stored, rendered, storageClassCheck{})
	require.NoError(t, err)
	require.NoError(t, c.List(ctx, pods))
	require.Len(t, pods.Items, 1)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), stored))
	cond = meta.FindStatusCondition(stored.Status.Conditions, conditionBackupRepositoryValid)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Equal(t, reasonKeyPrefixSet, cond.Reason)
}

// liteTopoOutage fails every topology read and write as unavailable.
type liteTopoOutage struct{ topoclient.Store }

func (liteTopoOutage) GetDatabase(context.Context, string) (*cm.Database, error) {
	return nil, topoclient.NewError(topoclient.Timeout, "/databases/postgres")
}

func (liteTopoOutage) CreateDatabase(context.Context, string, *cm.Database) error {
	return topoclient.NewError(topoclient.Timeout, "/databases/postgres")
}

func (liteTopoOutage) Close() error { return nil }

// A topology outage during the check is an outage, not a refusal: the shard
// requeues and outage recovery can bring back an existing pod.
func TestLiteBackupRepositoryCheckDuringTopologyOutage(t *testing.T) {
	scheme := reloadTestScheme(t)
	r := &ShardReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
		CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return liteTopoOutage{}, nil
		},
	}
	registered, err := r.registerLiteDatabase(
		t.Context(),
		liteShardWithBackup("existing", s3Backup("")),
		"main",
		"z1",
	)
	require.NoError(t, err)
	require.False(t, registered)
}
