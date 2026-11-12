package handlers

import (
	"errors"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// validateLiteBackupRepository rejects a Lite cluster whose S3 backups have no
// key prefix: the repository path then depends only on the database layout,
// so any other cluster on the bucket without a prefix shares it. oldCluster is
// nil on create. An update may keep an existing cluster's default repository
// on the same bucket and endpoint, so clusters created before this check keep
// their backups where they are, but may not move a cluster into a default
// repository, including another bucket's.
func validateLiteBackupRepository(
	oldCluster, newCluster *multigresv1alpha1.MultigresCluster,
) error {
	if newCluster.Spec.Mode != multigresv1alpha1.ModeLite {
		return nil
	}
	backup, ok := liteBackup(newCluster)
	if !ok || !multigresv1alpha1.UsesDefaultS3Repository(backup) {
		// Lite shape errors are reported by validateLiteMode.
		return nil
	}
	if oldCluster == nil {
		return errors.New(multigresv1alpha1.LiteDefaultS3RepositoryMessage)
	}
	oldBackup, ok := liteBackup(oldCluster)
	if ok && multigresv1alpha1.UsesDefaultS3Repository(oldBackup) &&
		oldBackup.S3.Bucket == backup.S3.Bucket && oldBackup.S3.Endpoint == backup.S3.Endpoint {
		return nil
	}
	return errors.New(multigresv1alpha1.LiteMoveToDefaultS3RepositoryMessage)
}

// liteBackup returns the backup config of the Lite shard, merged from the
// cluster, database, table group and shard the way the resolver merges it.
// Templates carry no backup settings, so nothing else contributes and the
// result does not depend on any other object existing. ok is false when the
// cluster is not shaped like a Lite cluster.
func liteBackup(
	cluster *multigresv1alpha1.MultigresCluster,
) (*multigresv1alpha1.BackupConfig, bool) {
	if len(cluster.Spec.Databases) != 1 || len(cluster.Spec.Databases[0].TableGroups) != 1 ||
		len(cluster.Spec.Databases[0].TableGroups[0].Shards) != 1 {
		return nil, false
	}
	db := cluster.Spec.Databases[0]
	tg := db.TableGroups[0]
	inherited := multigresv1alpha1.MergeBackupConfig(
		tg.Backup,
		multigresv1alpha1.MergeBackupConfig(db.Backup, cluster.Spec.Backup),
	)
	return multigresv1alpha1.MergeBackupConfig(tg.Shards[0].Backup, inherited), true
}
