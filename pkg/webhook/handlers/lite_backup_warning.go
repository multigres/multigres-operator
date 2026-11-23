package handlers

import (
	"fmt"
	"slices"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// s3Location is where a backup config puts backups in S3. The zero value is
// a config that does not resolve to S3.
type s3Location struct {
	s3                               bool
	bucket, prefix, endpoint, region string
}

func s3LocationOf(config *multigresv1alpha1.BackupConfig) s3Location {
	if !config.ResolvesToS3() || config.S3 == nil {
		return s3Location{}
	}
	return s3Location{
		s3:       true,
		bucket:   config.S3.Bucket,
		prefix:   config.S3.KeyPrefix,
		endpoint: config.S3.Endpoint,
		region:   config.S3.Region,
	}
}

// describe names the location next to other, the one it is contrasted
// with: bucket and prefix always, endpoint and region where the two S3
// locations differ in them, so that two locations that differ are never
// described alike.
func (l s3Location) describe(other s3Location) string {
	text := fmt.Sprintf("bucket %q, prefix %q", l.bucket, l.prefix)
	if other.s3 && l.endpoint != other.endpoint {
		text += fmt.Sprintf(", endpoint %q", l.endpoint)
	}
	if other.s3 && l.region != other.region {
		text += fmt.Sprintf(", region %q", l.region)
	}
	return text
}

// liteShardBackupWarnings names each shard of a Lite cluster that an update
// leaves backing up somewhere other than where the update moved the levels
// above it.
//
// A Lite shard backs up to the location its own block resolves to, and
// defaulting keeps an unchanged S3 shard block when S3 is removed above it or
// when the block states a location of its own, because removing it would
// move the shard's backups. The update is therefore admitted, and takes
// effect for every shard but that one. Nothing in the stored object says so,
// which is what the warning is for. It is given on the update that changes
// the location above the shard and not on later ones, so a shard that is
// meant to keep its own location is not reported with every write.
func liteShardBackupWarnings(
	old, cluster *multigresv1alpha1.MultigresCluster,
) admission.Warnings {
	if cluster.Spec.Mode != multigresv1alpha1.ModeLite {
		return nil
	}
	type shardKey struct {
		db multigresv1alpha1.DatabaseName
		tg multigresv1alpha1.TableGroupName
		sh multigresv1alpha1.ShardName
	}
	type storedShard struct {
		block *multigresv1alpha1.BackupConfig
		above s3Location
	}
	stored := map[shardKey]storedShard{}
	for _, db := range old.Spec.Databases {
		dbBackup := multigresv1alpha1.MergeBackupConfig(db.Backup, old.Spec.Backup)
		for _, tg := range db.TableGroups {
			above := s3LocationOf(multigresv1alpha1.MergeBackupConfig(tg.Backup, dbBackup))
			for _, shard := range tg.Shards {
				stored[shardKey{db.Name, tg.Name, shard.Name}] = storedShard{shard.Backup, above}
			}
		}
	}

	var warnings admission.Warnings
	for _, db := range cluster.Spec.Databases {
		dbBackup := multigresv1alpha1.MergeBackupConfig(db.Backup, cluster.Spec.Backup)
		for _, tg := range db.TableGroups {
			inherited := multigresv1alpha1.MergeBackupConfig(tg.Backup, dbBackup)
			above := s3LocationOf(inherited)
			for _, shard := range tg.Shards {
				was, known := stored[shardKey{db.Name, tg.Name, shard.Name}]
				if shard.Backup == nil || !known || above == was.above ||
					!apiequality.Semantic.DeepEqual(shard.Backup, was.block) {
					continue
				}
				own := s3LocationOf(multigresv1alpha1.MergeBackupConfig(shard.Backup, inherited))
				if !own.s3 || own == above {
					continue
				}
				now := "the filesystem"
				if above.s3 {
					now = "S3 " + above.describe(own)
				}
				warnings = append(warnings, fmt.Sprintf(
					"shard %s/%s/%s still backs up to its own S3 location (%s), "+
						"while the levels above it now resolve to %s; "+
						"delete the shard's backup block to make it follow them",
					db.Name, tg.Name, shard.Name, own.describe(above), now,
				))
			}
		}
	}
	slices.Sort(warnings)
	return warnings
}
