package v1alpha1

import (
	"net/url"
	"regexp"
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

// S3CAFilePath is where the pgbackrest sidecar's container has the CA bundle
// of an S3 location that names one. The sidecar is started with this path
// and the database's topology record carries it: multipooler compares the
// two, so both come from here.
const S3CAFilePath = "/etc/pgbackrest/s3-ca/ca.crt"

// DefaultS3CAKey is the key read from the Secret or ConfigMap of an
// S3CAReference that names none.
const DefaultS3CAKey = "ca.crt"

// KeyOrDefault is the key the reference reads.
func (r *S3CAKeyReference) KeyOrDefault() string {
	if r.Key == "" {
		return DefaultS3CAKey
	}
	return r.Key
}

// SkipsTLSVerification reports whether the S3 endpoint's TLS certificate is
// not verified: verifyTLS is set, to false.
func (c *S3BackupConfig) SkipsTLSVerification() bool {
	return c != nil && c.VerifyTLS != nil && !*c.VerifyTLS
}

// inheritTLSTrust applies to c, which holds what a level inherits, how the
// level's own block child says the endpoint's certificate is verified. A CA
// and verifyTLS: false exclude each other, so each replaces the other: a CA
// below turns an inherited verifyTLS: false back on, and verifyTLS: false
// below drops an inherited CA. verifyTLS: true says "verify" and no more: it
// replaces an inherited false and keeps an inherited CA, which is what the
// endpoint is verified with. A block that sets neither inherits both.
func (c *S3BackupConfig) inheritTLSTrust(child *S3BackupConfig) {
	switch {
	case child.CA != nil:
		c.CA, c.VerifyTLS = child.CA.DeepCopy(), child.VerifyTLS
	case child.SkipsTLSVerification():
		c.CA, c.VerifyTLS = nil, child.VerifyTLS
	case child.VerifyTLS != nil:
		c.VerifyTLS = child.VerifyTLS
	}
}

// tlsTrust is how the endpoint's certificate is verified, as one comparable
// string: "skip", the CA's source, name and key, or empty for the system
// trust store. verifyTLS: true and an absent verifyTLS are the same.
func (c *S3BackupConfig) tlsTrust() string {
	switch {
	case c.SkipsTLSVerification():
		return "verifyTLS: false"
	case c.CA != nil && c.CA.Secret != nil:
		return "secret " + c.CA.Secret.Name + "/" + c.CA.Secret.KeyOrDefault()
	case c.CA != nil && c.CA.ConfigMap != nil:
		return "configMap " + c.CA.ConfigMap.Name + "/" + c.CA.ConfigMap.KeyOrDefault()
	}
	return ""
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
// encryption, the pgBackRest TLS secret, or the S3 bucket, endpoint, region,
// key prefix, CA or TLS verification.
// It returns nil when block sets none of them or sets each to the value
// location carries, so a block that repeats the location is accepted.
//
// A sidecar is configured from its location alone. A block that asks for a
// cipher, a certificate or a bucket the location does not have would be
// ignored without an error: backups would be written unencrypted, with
// another certificate, or to another bucket or prefix than the block says,
// and a restore that follows the block would look where no backup is.
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
			Field: "s3.endpoint",
			Value: RedactEndpoint(block.S3.Endpoint), Applied: RedactEndpoint(location.S3.Endpoint),
		}
	}
	if block.S3.Region != "" {
		if o := differs("s3.region", block.S3.Region, location.S3.Region); o != nil {
			return o
		}
	}
	// Compared as pgBackRest is given them (see RepositoryKeyPrefix), and
	// reported as written.
	if block.S3.KeyPrefix != "" &&
		block.S3.RepositoryKeyPrefix() != location.S3.RepositoryKeyPrefix() {
		return &BackupOverride{
			Field: "s3.keyPrefix", Value: block.S3.KeyPrefix, Applied: location.S3.KeyPrefix,
		}
	}
	// A block that sets neither inherits how the endpoint is verified. One
	// that turns verification off, or names another CA, where the location
	// does otherwise would be read as "this shard's backups are verified
	// this way" while its sidecar does what the location says.
	if block.S3.CA != nil || block.S3.VerifyTLS != nil {
		// What the block would give a sidecar if it were applied below the
		// location: verifyTLS: true alone keeps the location's CA.
		asked := &S3BackupConfig{CA: location.S3.CA, VerifyTLS: location.S3.VerifyTLS}
		asked.inheritTLSTrust(block.S3)
		if o := differs("s3.ca/verifyTLS", asked.tlsTrust(), location.S3.tlsTrust()); o != nil {
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

// RedactEndpoint returns an S3 endpoint as it may be shown in a condition, an
// event or a log: without the user information of its authority, without
// its query and without its fragment, any of which can carry a credential.
// What was removed is marked, so that two endpoints that differ only there
// do not read as the same endpoint without saying so. The authority is what
// follows "//", with or without a scheme before it, or the start of an
// endpoint that has no "//".
func RedactEndpoint(endpoint string) string {
	scheme, rest := "", endpoint
	if i := strings.Index(endpoint, "//"); i >= 0 && !strings.ContainsAny(endpoint[:i], "/?#@") {
		scheme, rest = endpoint[:i+len("//")], endpoint[i+len("//"):]
	}
	authority, tail := rest, ""
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authority, tail = rest[:i], rest[i:]
	}
	if i := strings.LastIndex(authority, "@"); i >= 0 {
		authority = "<redacted>@" + authority[i+1:]
	}
	if i := strings.Index(tail, "#"); i >= 0 {
		tail = tail[:i] + "#<redacted>"
	}
	if i := strings.Index(tail, "?"); i >= 0 {
		fragment := ""
		if j := strings.Index(tail, "#"); j > i {
			fragment = tail[j:]
		}
		tail = tail[:i] + "?<redacted>" + fragment
	}
	return scheme + authority + tail
}

// bareEndpoint matches an S3 endpoint in pgBackRest's own form: a host name
// or address, optionally followed by a port.
var bareEndpoint = regexp.MustCompile(`^(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+)(:[0-9]{1,5})?$`)

// S3EndpointProblem says why endpoint is not an S3 endpoint the operator
// configures a pgBackRest sidecar with, or returns "". The sidecar hands the
// endpoint to pgBackRest as repo-s3-endpoint, which connects with TLS to a
// bare host[:port] and to an https URL, and without TLS to an http URL: the
// signed requests, and with them a session token, would then be readable on
// the network. So an endpoint is a bare host[:port] or an https URL with a
// host, and an empty one is AWS S3.
//
// The text does not quote the endpoint, which may carry a credential in its
// user information, query or fragment, or in a part that did not parse.
func S3EndpointProblem(endpoint string) string {
	const fix = "; use host[:port] or https://host[:port], and verifyTLS: false or a ca " +
		"for an endpoint with a private or self-signed certificate"
	switch {
	case endpoint == "", bareEndpoint.MatchString(endpoint):
		return ""
	case !strings.Contains(endpoint, "://"):
		return "s3 endpoint is neither a host[:port] nor an https URL" + fix
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "s3 endpoint is not a URL" + fix
	}
	if u.Scheme != "https" {
		return "s3 endpoint has a scheme other than https: pgBackRest would send signed " +
			"requests and credentials over an unencrypted connection" + fix
	}
	if u.Host == "" {
		return "s3 endpoint is an https URL without a host" + fix
	}
	return ""
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
