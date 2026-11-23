package v1alpha1

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AnnotationDefaultBackupKeyPrefix records the S3 prefix allocated before a
// cluster is first provisioned. Its absence on an already reconciled cluster
// preserves the pre-existing repository paths, including unprefixed ones.
const AnnotationDefaultBackupKeyPrefix = "multigres.com/default-backup-key-prefix"

// CopyDefaultBackupKeyPrefix carries the cluster allocation down to its storage.
func CopyDefaultBackupKeyPrefix(source metav1.Object, target *metav1.ObjectMeta) {
	if prefix := source.GetAnnotations()[AnnotationDefaultBackupKeyPrefix]; prefix != "" {
		metav1.SetMetaDataAnnotation(target, AnnotationDefaultBackupKeyPrefix, prefix)
	}
}

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
		merged.S3 != nil && merged.S3.KeyPrefix != "/" && strings.Trim(merged.S3.KeyPrefix, "/") == "" {
		if config.S3 == nil {
			config.S3 = &S3BackupConfig{}
		}
		config.S3.KeyPrefix = prefix
		merged.S3.KeyPrefix = prefix
	}
	return merged
}

// RepositoryKeyPrefix is the key prefix pgBackRest is configured with. It is
// KeyPrefix as written, except that "/", which selects the legacy bucket root,
// becomes empty: pgBackRest adds the leading slash to the repository path.
//
// The database record in topology and the pgbackrestd sidecar's flags must
// both carry this value. Multipooler reads the location from topology and
// refuses a sidecar started with a different one, and a prefix that differed
// between the two would name two repositories for one shard.
func (c *S3BackupConfig) RepositoryKeyPrefix() string {
	if c.KeyPrefix == "/" {
		return ""
	}
	return c.KeyPrefix
}

// ConfiguresS3 reports whether this backup block asks for an S3 repository,
// by its type or by carrying an S3 block.
func (c *BackupConfig) ConfiguresS3() bool {
	return c != nil && (c.Type == BackupTypeS3 || c.S3 != nil)
}

// ResolvesToS3 reports whether this merged backup config stores backups in S3.
// Every other config, a nil one included, stores them on the cluster's own
// filesystem volume.
func (c *BackupConfig) ResolvesToS3() bool {
	return c != nil && c.Type == BackupTypeS3
}

// SidecarBackupLocation selects the location a shard's pgBackRest sidecar
// serves, which is the one recorded for the shard's database in topology and
// read from there by multipooler.
//
// A distributed cluster has one record per database, written from the
// database's merged backup config, so a tablegroup or shard backup block
// cannot move the repository. A Lite cluster's shard writes the record itself
// from its own resolved backup config, which includes tablegroup and shard
// blocks.
func SidecarBackupLocation(mode string, database, shard *BackupConfig) *BackupConfig {
	if mode == ModeLite {
		return shard
	}
	return database
}

// BackupOverride is a setting of a backup block that the pgBackRest sidecar's
// location does not carry.
// +kubebuilder:object:generate=false
type BackupOverride struct {
	// Field is the setting's path within the backup block.
	Field string
	// Value is what the block sets, and Applied what the location carries.
	// An empty string means the setting is absent.
	Value, Applied string
}

// UnappliedBackupOverride returns the first setting of block that location,
// the location a pgBackRest sidecar serves, would not apply: the backup type,
// encryption, the pgBackRest TLS secret, or the S3 bucket, endpoint or region.
// It returns nil when block sets none of them or sets each to the value
// location carries, so a block that repeats the location is accepted.
//
// A sidecar is configured from its location alone. A block that asks for a
// cipher, a certificate or a bucket the location does not have would be
// ignored without an error: backups would be written unencrypted, with
// another certificate, or to another bucket than the block says.
//
// The type is compared because a block of another type replaces everything
// above it when the levels are merged. Such a block looks as if it turned
// encryption off for its shards while the location still has it, or the
// other way round, and none of the comparisons below would see it.
func UnappliedBackupOverride(block, location *BackupConfig) *BackupOverride {
	if block == nil {
		return nil
	}
	if location == nil {
		location = &BackupConfig{}
	}
	differs := func(field, value, applied string) *BackupOverride {
		if value == applied {
			return nil
		}
		return &BackupOverride{Field: field, Value: value, Applied: applied}
	}
	// No type on either side means the filesystem default.
	typeOf := func(c *BackupConfig) string {
		if c.Type == "" {
			return string(BackupTypeFilesystem)
		}
		return string(c.Type)
	}
	if o := differs("type", typeOf(block), typeOf(location)); o != nil {
		return o
	}
	if block.Encryption != nil {
		applied := ""
		if location.Encryption != nil {
			applied = location.Encryption.SecretName
		}
		if o := differs("encryption.secretName", block.Encryption.SecretName, applied); o != nil {
			return o
		}
	}
	// A pgbackrestTLS block without a secret name asks for the generated
	// certificate, which differs from a location that names a secret.
	if block.PgBackRestTLS != nil {
		applied := ""
		if location.PgBackRestTLS != nil {
			applied = location.PgBackRestTLS.SecretName
		}
		if o := differs(
			"pgbackrestTLS.secretName", block.PgBackRestTLS.SecretName, applied,
		); o != nil {
			return o
		}
	}
	if block.S3 == nil || location.S3 == nil {
		return nil
	}
	// An empty S3 field inherits the value above it.
	if block.S3.Bucket != "" {
		if o := differs("s3.bucket", block.S3.Bucket, location.S3.Bucket); o != nil {
			return o
		}
	}
	if block.S3.Endpoint != "" &&
		normalizeEndpoint(block.S3.Endpoint) != normalizeEndpoint(location.S3.Endpoint) {
		return &BackupOverride{
			Field: "s3.endpoint", Value: block.S3.Endpoint, Applied: location.S3.Endpoint,
		}
	}
	if block.S3.Region != "" {
		if o := differs("s3.region", block.S3.Region, location.S3.Region); o != nil {
			return o
		}
	}
	return nil
}

// normalizeEndpoint reduces two spellings of one S3 endpoint to one string,
// and never two endpoints to one: a scheme and a host name are
// case-insensitive, and an endpoint that ends at its host is the same with
// and without the one slash of an empty path. User information, a path
// longer than "/", a query and a fragment are kept exactly as written.
func normalizeEndpoint(endpoint string) string {
	scheme, rest := "", endpoint
	if i := strings.Index(endpoint, "://"); i >= 0 {
		scheme, rest = strings.ToLower(endpoint[:i+len("://")]), endpoint[i+len("://"):]
	}
	authority, tail := rest, ""
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authority, tail = rest[:i], rest[i:]
	}
	if tail == "/" {
		tail = ""
	}
	userinfo, host := "", authority
	if i := strings.LastIndex(authority, "@"); i >= 0 {
		userinfo, host = authority[:i+1], authority[i+1:]
	}
	return scheme + userinfo + strings.ToLower(host) + tail
}

// AllocationWithoutKeyPrefix reports whether an S3 location has no key prefix
// although its cluster was allocated one. Defaulting fills every missing
// prefix of such a cluster, and "/" is how the bucket root is asked for, so
// this location was not built from the cluster's configuration and would put
// the repository at the bucket root, where other clusters' repositories can
// be.
func AllocationWithoutKeyPrefix(owner metav1.Object, location *BackupConfig) bool {
	if owner.GetAnnotations()[AnnotationDefaultBackupKeyPrefix] == "" || !location.ResolvesToS3() {
		return false
	}
	prefix := ""
	if location.S3 != nil {
		prefix = location.S3.KeyPrefix
	}
	return prefix != "/" && strings.Trim(prefix, "/") == ""
}
