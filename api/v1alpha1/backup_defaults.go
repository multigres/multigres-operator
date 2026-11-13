package v1alpha1

import "strings"

// AnnotationDefaultBackupKeyPrefix records the S3 prefix allocated before a
// cluster is first provisioned. Its absence on an already reconciled cluster
// preserves the pre-existing repository paths, including unprefixed ones.
const AnnotationDefaultBackupKeyPrefix = "multigres.com/default-backup-key-prefix"

// DefaultBackupKeyPrefixes fills missing S3 prefixes using the cluster's durable
// default. Explicit prefixes take precedence, including inherited ones. Backup
// settings are merged top-down just as they are when building child resources.
// Filesystem backups and clusters without an allocated default are unchanged.
func DefaultBackupKeyPrefixes(cluster *MultigresCluster) {
	prefix := cluster.Annotations[AnnotationDefaultBackupKeyPrefix]
	if prefix == "" {
		return
	}
	clusterBackup := defaultBackupKeyPrefix(cluster.Spec.Backup, nil, prefix)
	for i := range cluster.Spec.Databases {
		db := &cluster.Spec.Databases[i]
		dbBackup := defaultBackupKeyPrefix(db.Backup, clusterBackup, prefix)
		for j := range db.TableGroups {
			tg := &db.TableGroups[j]
			tgBackup := defaultBackupKeyPrefix(tg.Backup, dbBackup, prefix)
			for k := range tg.Shards {
				defaultBackupKeyPrefix(tg.Shards[k].Backup, tgBackup, prefix)
			}
		}
	}
}

func defaultBackupKeyPrefix(config, parent *BackupConfig, prefix string) *BackupConfig {
	merged := MergeBackupConfig(config, parent)
	if config != nil && merged != nil && merged.Type == BackupTypeS3 &&
		merged.S3 != nil && strings.Trim(merged.S3.KeyPrefix, "/") == "" {
		if config.S3 == nil {
			config.S3 = &S3BackupConfig{}
		}
		config.S3.KeyPrefix = prefix
		merged.S3.KeyPrefix = prefix
	}
	return merged
}
