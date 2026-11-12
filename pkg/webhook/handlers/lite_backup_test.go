package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func liteBackupCluster(b *multigresv1alpha1.BackupConfig) *multigresv1alpha1.MultigresCluster {
	c := identityCluster()
	c.Spec.Backup = b
	return c
}

func liteS3(keyPrefix string) *multigresv1alpha1.BackupConfig {
	return &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket: "shared", Region: "us-east-1", KeyPrefix: keyPrefix,
		},
	}
}

func liteBackupValidator() *MultigresClusterValidator {
	original := identityTemplate("original", "main")
	pool := original.Spec.Pools["main"]
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	original.Spec.Pools["main"] = pool
	return NewMultigresClusterValidator(identityClient(original))
}

func TestLiteBackupRepositoryAdmission(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			cluster func() *multigresv1alpha1.MultigresCluster
			want    string
		}{
			{"s3 without key prefix", func() *multigresv1alpha1.MultigresCluster {
				return liteBackupCluster(liteS3(""))
			}, "requires spec.backup.s3.keyPrefix"},
			{"s3 with a slash-only prefix", func() *multigresv1alpha1.MultigresCluster {
				return liteBackupCluster(liteS3("//"))
			}, "requires spec.backup.s3.keyPrefix"},
			{"s3 with key prefix", func() *multigresv1alpha1.MultigresCluster {
				return liteBackupCluster(liteS3("tenant-a"))
			}, ""},
			// The prefix is checked on the resolved config, so one set at a
			// lower level satisfies a bucket set at the cluster level.
			{"prefix set on the table group", func() *multigresv1alpha1.MultigresCluster {
				c := liteBackupCluster(liteS3(""))
				c.Spec.Databases[0].TableGroups[0].Backup = &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeS3,
					S3:   &multigresv1alpha1.S3BackupConfig{KeyPrefix: "tenant-a"},
				}
				return c
			}, ""},
			{"prefix missing on the shard override", func() *multigresv1alpha1.MultigresCluster {
				c := liteBackupCluster(nil)
				c.Spec.Databases[0].TableGroups[0].Shards[0].Backup = liteS3("")
				return c
			}, "requires spec.backup.s3.keyPrefix"},
			{"filesystem", func() *multigresv1alpha1.MultigresCluster {
				return liteBackupCluster(&multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
				})
			}, ""},
			{"no backup config", func() *multigresv1alpha1.MultigresCluster {
				return liteBackupCluster(nil)
			}, ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := liteBackupValidator().ValidateCreate(t.Context(), tc.cluster())
				if tc.want == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tc.want)
				}
			})
		}
	})

	t.Run("update", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			old, new *multigresv1alpha1.BackupConfig
			want     string
		}{
			// Clusters created before the check keep their repository.
			{"existing default repository is kept", liteS3(""), liteS3(""), ""},
			{"existing default repository can get a prefix", liteS3(""), liteS3("tenant-a"), ""},
			{"prefix cannot be removed", liteS3("tenant-a"), liteS3(""), "would move"},
			{
				"filesystem cannot move to the default repository",
				&multigresv1alpha1.BackupConfig{Type: multigresv1alpha1.BackupTypeFilesystem},
				liteS3(""), "would move",
			},
			{"prefix kept", liteS3("tenant-a"), liteS3("tenant-a"), ""},
			{"default repository cannot move to another bucket", liteS3(""), func() *multigresv1alpha1.BackupConfig {
				b := liteS3("")
				b.S3.Bucket = "other"
				return b
			}(), "would move"},
			{"default repository cannot move to another endpoint", liteS3(""), func() *multigresv1alpha1.BackupConfig {
				b := liteS3("")
				b.S3.Endpoint = "https://minio:9000"
				return b
			}(), "would move"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := liteBackupValidator().ValidateUpdate(
					t.Context(), liteBackupCluster(tc.old), liteBackupCluster(tc.new),
				)
				if tc.want == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tc.want)
				}
			})
		}
	})

	// Backup settings never come from templates, so an existing cluster whose
	// template is gone is still judged on its own backup config.
	t.Run("existing default repository with a missing template", func(t *testing.T) {
		old := liteBackupCluster(liteS3(""))
		old.Spec.Databases[0].TableGroups[0].Shards[0].ShardTemplate = "deleted"
		updated := old.DeepCopy()
		require.NoError(t, validateLiteBackupRepository(old, updated))
	})

	t.Run("distributed clusters are not checked", func(t *testing.T) {
		c := liteBackupCluster(liteS3(""))
		c.Spec.Mode = ""
		require.NoError(t, validateLiteBackupRepository(nil, c))
	})
}
