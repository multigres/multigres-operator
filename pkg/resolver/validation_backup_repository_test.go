package resolver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// repositoryCluster is a cluster with one database, tablegroup and shard, an
// S3 location with an endpoint, a cipher and a TLS secret at database level,
// and the given blocks below it.
func repositoryCluster(
	mode string,
	tablegroup, shard *multigresv1alpha1.BackupConfig,
) *multigresv1alpha1.MultigresCluster {
	return &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Mode: mode,
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name:   "postgres",
				Backup: databaseLocation(),
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name:   "default",
					Backup: tablegroup,
					Shards: []multigresv1alpha1.ShardConfig{{Name: "0-inf", Backup: shard}},
				}},
			}},
		},
	}
}

func databaseLocation() *multigresv1alpha1.BackupConfig {
	return &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket: "db-bucket", Region: "us-east-1", Endpoint: "https://s3.example",
			KeyPrefix: "prod", CredentialsSecret: "keys",
		},
		Encryption:    &multigresv1alpha1.BackupEncryptionConfig{SecretName: "db-cipher"},
		PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{SecretName: "db-tls"},
	}
}

// A distributed cluster's sidecars are configured from the database-level
// location. A tablegroup or shard block that sets a cipher, a certificate or
// a bucket that location lacks cannot be applied, and ignoring it would leave
// a repository that is unencrypted or in another place than the block says,
// so the cluster is refused. A block that sets nothing of these, or repeats
// the database's values, is accepted, and so is any block in a Lite cluster,
// whose shard-level config is the location.
func TestBackupRepositoryProblem_OverrideBelowTheDatabase(t *testing.T) {
	t.Parallel()
	s3Block := func(mut func(*multigresv1alpha1.S3BackupConfig)) *multigresv1alpha1.BackupConfig {
		cfg := &multigresv1alpha1.S3BackupConfig{}
		mut(cfg)
		return &multigresv1alpha1.BackupConfig{Type: multigresv1alpha1.BackupTypeS3, S3: cfg}
	}
	for name, tc := range map[string]struct {
		block     *multigresv1alpha1.BackupConfig
		wantField string
	}{
		"repeats the database block": {block: databaseLocation()},
		"sets only the type": {
			block: &multigresv1alpha1.BackupConfig{Type: multigresv1alpha1.BackupTypeS3},
		},
		"sets only credentials": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.CredentialsSecret = "other-keys"
			}),
		},
		"another cipher": {
			block: &multigresv1alpha1.BackupConfig{
				Type:       multigresv1alpha1.BackupTypeS3,
				Encryption: &multigresv1alpha1.BackupEncryptionConfig{SecretName: "own-cipher"},
			},
			wantField: `encryption.secretName to "own-cipher"`,
		},
		"another TLS secret": {
			block: &multigresv1alpha1.BackupConfig{
				Type:          multigresv1alpha1.BackupTypeS3,
				PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{SecretName: "own-tls"},
			},
			wantField: `pgbackrestTLS.secretName to "own-tls"`,
		},
		"the generated certificate instead of the database's secret": {
			block: &multigresv1alpha1.BackupConfig{
				Type:          multigresv1alpha1.BackupTypeS3,
				PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{},
			},
			wantField: `pgbackrestTLS.secretName to ""`,
		},
		"another bucket": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.Bucket = "own-bucket"
				c.Region = "us-east-1"
			}),
			wantField: `s3.bucket to "own-bucket"`,
		},
		"another endpoint": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.Endpoint = "https://other.example"
			}),
			wantField: `s3.endpoint to "https://other.example"`,
		},
		"another region": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.Region = "eu-west-1"
			}),
			wantField: `s3.region to "eu-west-1"`,
		},
		"another key prefix": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.KeyPrefix = "own-prefix"
			}),
			wantField: `s3.keyPrefix to "own-prefix"`,
		},
		// The bucket root, where the database's repository is under "prod".
		"the bucket root instead of the database's prefix": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.KeyPrefix = "/"
			}),
			wantField: `s3.keyPrefix to "/"`,
		},
		"the database's key prefix": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.KeyPrefix = "prod"
			}),
		},
		// One endpoint, written with another case and a trailing slash.
		"the same endpoint spelled differently": {
			block: s3Block(func(c *multigresv1alpha1.S3BackupConfig) {
				c.Endpoint = "HTTPS://S3.Example/"
			}),
		},
	} {
		for _, level := range []string{"tablegroup", "shard"} {
			t.Run(name+"/"+level, func(t *testing.T) {
				t.Parallel()
				build := func(mode string) *multigresv1alpha1.MultigresCluster {
					if level == "tablegroup" {
						return repositoryCluster(mode, tc.block.DeepCopy(), nil)
					}
					return repositoryCluster(mode, nil, tc.block.DeepCopy())
				}

				reason, err := BackupRepositoryProblem(build(""))
				liteReason, liteErr := BackupRepositoryProblem(build(multigresv1alpha1.ModeLite))
				require.NoError(t, liteErr, "a Lite shard's own config is its location")
				assert.Equal(t, multigresv1alpha1.ReasonBackupRepositoryResolved, liteReason)

				if tc.wantField == "" {
					require.NoError(t, err)
					assert.Equal(t, multigresv1alpha1.ReasonBackupRepositoryResolved, reason)
					require.NoError(t, ValidateSidecarConfig(build("")))
					return
				}
				require.ErrorIs(t, err, ErrBackupRepository)
				assert.Equal(t, multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied, reason)
				assert.ErrorContains(t, err, tc.wantField)
				assert.ErrorContains(t, err, level+` "`)
				require.ErrorIs(t, ValidateSidecarConfig(build("")), ErrBackupRepository,
					"admission must refuse it too")
			})
		}
	}
}

// A stored distributed cluster can carry a shard block whose key prefix
// differs from the database's: that was admitted before the prefix was
// compared. An update that leaves the block as it is must be admitted with a
// warning naming both prefixes, or the cluster could not be edited or
// deleted. An update that introduces such a block, or changes its prefix, is
// rejected.
func TestValidateClusterLogicUpdate_StoredKeyPrefixBelowTheDatabase(t *testing.T) {
	t.Parallel()
	withPrefix := func(prefix string) *multigresv1alpha1.MultigresCluster {
		block := databaseLocation()
		block.S3.KeyPrefix = prefix
		return repositoryCluster("", nil, block)
	}
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	r := NewResolver(fake.NewClientBuilder().WithScheme(scheme).Build(), "default")

	stored := withPrefix("own-prefix")
	_, err := BackupRepositoryProblem(stored)
	require.ErrorContains(t, err, `sets s3.keyPrefix to "own-prefix"`)
	require.ErrorContains(t, err, `has "prod"`)

	// Only the sidecar rules are under test: a later rule of the update
	// path may refuse this small fixture for its own reasons.
	warnings, err := r.ValidateClusterLogicUpdate(t.Context(), stored, stored.DeepCopy())
	if err != nil {
		require.NotErrorIs(t, err, ErrBackupRepository, "an unchanged stored prefix")
	} else {
		require.NotEmpty(t, warnings)
		assert.Contains(t, warnings[0], `sets s3.keyPrefix to "own-prefix"`)
	}

	_, err = r.ValidateClusterLogicUpdate(t.Context(), withPrefix("prod"), stored)
	require.ErrorIs(t, err, ErrBackupRepository, "a prefix the update introduces")
	_, err = r.ValidateClusterLogicUpdate(t.Context(), stored, withPrefix("other"))
	require.ErrorIs(t, err, ErrBackupRepository, "a prefix the update changes")
}

// A block of another type replaces everything the shard would inherit, the
// cipher and the TLS secret included, while the sidecar keeps the database
// location with both. The shard's resolved config then says "filesystem,
// unencrypted" about a repository that is on S3 with a key, so the block is
// refused whatever else it sets.
func TestBackupRepositoryProblem_BlockOfAnotherTypeBelowTheDatabase(t *testing.T) {
	t.Parallel()
	filesystem := &multigresv1alpha1.BackupConfig{
		Type:       multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{},
	}
	for _, level := range []string{"tablegroup", "shard"} {
		cluster := repositoryCluster("", filesystem.DeepCopy(), nil)
		if level == "shard" {
			cluster = repositoryCluster("", nil, filesystem.DeepCopy())
		}
		reason, err := BackupRepositoryProblem(cluster)
		require.ErrorIs(t, err, ErrBackupRepository, level)
		assert.Equal(t, multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied, reason)
		assert.ErrorContains(t, err, `sets type to "filesystem"`)
		assert.ErrorContains(t, err, level+` "`)

		// The same block without a cipher or TLS secret above it is refused
		// too: it asks for a repository on the volume and gets one on S3.
		plain := cluster.DeepCopy()
		plain.Spec.Databases[0].Backup.Encryption = nil
		plain.Spec.Databases[0].Backup.PgBackRestTLS = nil
		_, err = BackupRepositoryProblem(plain)
		require.ErrorIs(t, err, ErrBackupRepository, level)
	}

	// A filesystem cluster whose lower blocks are filesystem too is the same
	// type throughout.
	cluster := repositoryCluster("", filesystem.DeepCopy(), filesystem.DeepCopy())
	cluster.Spec.Databases[0].Backup = filesystem.DeepCopy()
	_, err := BackupRepositoryProblem(cluster)
	require.NoError(t, err)
}

// Admission and the cluster controller decide from SidecarConfigProblem, so
// it has to name every rule: the repository rules, the path rule and the
// credential rule, each with the reason its condition reports.
func TestSidecarConfigProblem_Reasons(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		mutate     func(*multigresv1alpha1.MultigresCluster)
		wantReason string
		wantErr    error
	}{
		"accepted": {
			mutate:     func(*multigresv1alpha1.MultigresCluster) {},
			wantReason: multigresv1alpha1.ReasonBackupRepositoryResolved,
		},
		"override": {
			mutate: func(c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].TableGroups[0].Backup = &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeS3,
					S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "own", Region: "us-east-1"},
				}
			},
			wantReason: multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied,
			wantErr:    ErrBackupRepository,
		},
		"no credential source": {
			mutate: func(c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].Backup.S3.CredentialsSecret = ""
			},
			wantReason: multigresv1alpha1.ReasonBackupRepositoryUnsupportedCredentials,
			wantErr:    ErrSidecarCredentials,
		},
		"http endpoint": {
			mutate: func(c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].Backup.S3.Endpoint = "http://key:secret@minio:9000"
			},
			wantReason: multigresv1alpha1.ReasonBackupRepositoryUnsupportedEndpoint,
			wantErr:    ErrSidecarEndpoint,
		},
		"scheme-relative endpoint": {
			mutate: func(c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].Backup.S3.Endpoint = "//key:secret@minio:9000"
			},
			wantReason: multigresv1alpha1.ReasonBackupRepositoryUnsupportedEndpoint,
			wantErr:    ErrSidecarEndpoint,
		},
		"endpoint without a scheme": {
			mutate: func(c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].Backup.S3.Endpoint = "minio.storage.svc:9000"
			},
			wantReason: multigresv1alpha1.ReasonBackupRepositoryResolved,
		},
		"https endpoint": {
			mutate: func(c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].Backup.S3.Endpoint = "HTTPS://minio.storage.svc:9000"
			},
			wantReason: multigresv1alpha1.ReasonBackupRepositoryResolved,
		},
		"filesystem path": {
			mutate: func(c *multigresv1alpha1.MultigresCluster) {
				c.Spec.Databases[0].Backup = &multigresv1alpha1.BackupConfig{
					Type:       multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/data/bk"},
				}
			},
			wantReason: multigresv1alpha1.ReasonBackupRepositoryUnsupportedPath,
		},
	} {
		cluster := repositoryCluster("", nil, nil)
		tc.mutate(cluster)
		reason, err := SidecarConfigProblem(cluster)
		assert.Equal(t, tc.wantReason, reason, name)
		assert.Equal(t, err, ValidateSidecarConfig(cluster), name)
		switch {
		case tc.wantReason == multigresv1alpha1.ReasonBackupRepositoryResolved:
			require.NoError(t, err, name)
		case tc.wantErr != nil:
			require.ErrorIs(t, err, tc.wantErr, name)
		default:
			require.ErrorContains(t, err, "/data/bk", name)
		}
	}
}

// A cipher or TLS secret below a database that has none is the case that
// would silently give an unencrypted repository.
func TestBackupRepositoryProblem_OverrideWhereTheDatabaseHasNone(t *testing.T) {
	t.Parallel()
	for name, block := range map[string]*multigresv1alpha1.BackupConfig{
		"encryption.secretName": {
			Type:       multigresv1alpha1.BackupTypeS3,
			Encryption: &multigresv1alpha1.BackupEncryptionConfig{SecretName: "cipher"},
		},
		"pgbackrestTLS.secretName": {
			Type:          multigresv1alpha1.BackupTypeS3,
			PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{SecretName: "tls"},
		},
	} {
		cluster := repositoryCluster("", nil, block)
		cluster.Spec.Databases[0].Backup.Encryption = nil
		cluster.Spec.Databases[0].Backup.PgBackRestTLS = nil
		reason, err := BackupRepositoryProblem(cluster)
		require.ErrorIs(t, err, ErrBackupRepository, name)
		assert.Equal(t, multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied, reason)
		assert.ErrorContains(t, err, name)
	}
}

// An S3 section under a filesystem type is S3 configuration that the type
// leaves unused, so the location is the filesystem while S3 was asked for.
func TestBackupRepositoryProblem_S3SectionUnderFilesystemType(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Mode: mode,
				Backup: &multigresv1alpha1.BackupConfig{
					Type:       multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{},
					S3: &multigresv1alpha1.S3BackupConfig{
						Bucket: "b", Region: "r", CredentialsSecret: "keys",
					},
				},
			},
		}
		reason, err := BackupRepositoryProblem(cluster)
		require.ErrorIs(t, err, ErrBackupRepository, "mode %q", mode)
		assert.Equal(t, multigresv1alpha1.ReasonBackupRepositoryFilesystemFallback, reason)

		cluster.Spec.Backup.S3 = nil
		_, err = BackupRepositoryProblem(cluster)
		require.NoError(t, err, "a filesystem block without S3 settings is a filesystem cluster")
	}
}

// Defaulting fills every missing prefix of a cluster that was allocated one,
// so this cluster is built without it: the rule is what refuses a spec that
// reached the controller some other way.
func TestBackupRepositoryProblem_AllocatedClusterWithoutKeyPrefix(t *testing.T) {
	t.Parallel()
	cluster := func(prefix string) *multigresv1alpha1.MultigresCluster {
		c := repositoryCluster("", nil, nil)
		c.Annotations = map[string]string{
			multigresv1alpha1.AnnotationDefaultBackupKeyPrefix: "clusters/uid-a",
		}
		c.Spec.Databases[0].Backup.S3.KeyPrefix = prefix
		return c
	}
	for _, prefix := range []string{"", "//"} {
		reason, err := BackupRepositoryProblem(cluster(prefix))
		require.ErrorIs(t, err, ErrBackupRepository, "prefix %q", prefix)
		assert.Equal(t, multigresv1alpha1.ReasonBackupRepositoryMissingKeyPrefix, reason)
		assert.ErrorContains(t, err, "clusters/uid-a")
	}
	for _, prefix := range []string{"/", "clusters/uid-a"} {
		_, err := BackupRepositoryProblem(cluster(prefix))
		require.NoError(t, err, "prefix %q", prefix)
	}
	unallocated := cluster("")
	unallocated.Annotations = nil
	_, err := BackupRepositoryProblem(unallocated)
	require.NoError(t, err, "a cluster without an allocation keeps the bucket root")
}

// Pod validation rejects more than a limit below its request, and the CRD
// stores all of it. Admission names each shape early; a resource it cannot
// know (an extended resource, a hugepage size) is let through for the API
// server to judge when a pod is replaced.
func TestValidateContainerResources(t *testing.T) {
	t.Parallel()
	q := resource.MustParse
	for name, tc := range map[string]struct {
		res     corev1.ResourceRequirements
		wantErr string
	}{
		"empty": {},
		"cpu, memory and ephemeral storage": {
			res: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: q("100m"), corev1.ResourceMemory: q("1Gi"),
					corev1.ResourceEphemeralStorage: q("1Gi"),
				},
				Limits: corev1.ResourceList{corev1.ResourceMemory: q("2Gi")},
			},
		},
		"hugepages and an extended resource": {
			res: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{"hugepages-2Mi": q("4Mi"), "example.com/gpu": q("1")},
			},
		},
		"zero": {
			res: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: q("0")}},
		},
		"limit below request": {
			res: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: q("2Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: q("1Gi")},
			},
			wantErr: "resource memory limit (1Gi) must be >= request (2Gi)",
		},
		"misspelled name in limits": {
			res:     corev1.ResourceRequirements{Limits: corev1.ResourceList{"memroy": q("1Gi")}},
			wantErr: `"memroy" is not a resource a container can request`,
		},
		"misspelled name in requests": {
			res:     corev1.ResourceRequirements{Requests: corev1.ResourceList{"cpus": q("1")}},
			wantErr: `"cpus" is not a resource a container can request`,
		},
		"negative request": {
			res: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: q("-1Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: q("1Gi")},
			},
			wantErr: "resource memory request (-1Mi) must not be negative",
		},
		"negative limit": {
			res:     corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: q("-1")}},
			wantErr: "resource cpu limit (-1) must not be negative",
		},
		"claim": {
			res:     corev1.ResourceRequirements{Claims: []corev1.ResourceClaim{{Name: "gpu"}}},
			wantErr: `resource claims are not supported (claim "gpu")`,
		},
	} {
		err := ValidateContainerResources(tc.res, "pgbackrest")
		if tc.wantErr == "" {
			require.NoError(t, err, name)
			continue
		}
		require.ErrorContains(t, err, "pgbackrest: ", name)
		require.ErrorContains(t, err, tc.wantErr, name)
	}
}

// The API server checks a pod's resources but not a custom resource's, so a
// sidecar block a pod could not have is rejected here, however it reaches the
// pool: written inline, merged from an override, or inherited from a
// ShardTemplate.
func TestValidateClusterLogic_RejectsInvalidPgBackRestResources(t *testing.T) {
	t.Parallel()
	invalid := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}
	valid := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
	}
	// The cluster path must use the container rules, not only the
	// limit-and-request comparison the other components get.
	misspelled := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{"memroy": resource.MustParse("1Gi")},
	}
	template := func(name string, res corev1.ResourceRequirements) *multigresv1alpha1.ShardTemplate {
		return &multigresv1alpha1.ShardTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: multigresv1alpha1.ShardTemplateSpec{
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"default": {
						Type:                "readWrite",
						Cells:               []multigresv1alpha1.CellName{"zone-1"},
						PgBackRestResources: res,
					},
				},
			},
		}
	}
	r := NewResolver(
		fake.NewClientBuilder().WithScheme(setupScheme()).WithObjects(
			template("plain", corev1.ResourceRequirements{}),
			template("valid-sidecar", valid),
			template("invalid-sidecar", invalid),
			&storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standard",
					Annotations: map[string]string{
						"storageclass.kubernetes.io/is-default-class": "true",
					},
				},
				Provisioner: "k8s.io/fake",
			},
		).Build(),
		"default",
	)
	pools := func(
		res corev1.ResourceRequirements,
	) map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec {
		return map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
			"default": {
				Type:                "readWrite",
				Cells:               []multigresv1alpha1.CellName{"zone-1"},
				PgBackRestResources: res,
			},
		}
	}
	for name, tc := range map[string]struct {
		shard   multigresv1alpha1.ShardConfig
		invalid bool
		wantErr string
	}{
		"inline, invalid": {
			shard: multigresv1alpha1.ShardConfig{
				Spec: &multigresv1alpha1.ShardInlineSpec{Pools: pools(invalid)},
			},
			invalid: true,
		},
		"inline, valid": {
			shard: multigresv1alpha1.ShardConfig{
				Spec: &multigresv1alpha1.ShardInlineSpec{Pools: pools(valid)},
			},
		},
		"override, invalid": {
			shard: multigresv1alpha1.ShardConfig{
				ShardTemplate: "plain",
				Overrides:     &multigresv1alpha1.ShardOverrides{Pools: pools(invalid)},
			},
			invalid: true,
		},
		"override, valid over an invalid template": {
			shard: multigresv1alpha1.ShardConfig{
				ShardTemplate: "invalid-sidecar",
				Overrides:     &multigresv1alpha1.ShardOverrides{Pools: pools(valid)},
			},
		},
		"template, invalid": {
			shard:   multigresv1alpha1.ShardConfig{ShardTemplate: "invalid-sidecar"},
			invalid: true,
		},
		"template, valid": {
			shard: multigresv1alpha1.ShardConfig{ShardTemplate: "valid-sidecar"},
		},
		"inline, misspelled resource name": {
			shard: multigresv1alpha1.ShardConfig{
				Spec: &multigresv1alpha1.ShardInlineSpec{Pools: pools(misspelled)},
			},
			invalid: true,
			wantErr: `shard '0-inf' pool 'default' pgbackrest: "memroy" is not a resource`,
		},
		// A block of claims only reaches validation: resolution must not
		// take it for an empty block and drop it.
		"inline, claims only": {
			shard: multigresv1alpha1.ShardConfig{
				Spec: &multigresv1alpha1.ShardInlineSpec{Pools: pools(corev1.ResourceRequirements{
					Claims: []corev1.ResourceClaim{{Name: "gpu"}},
				})},
			},
			invalid: true,
			wantErr: "claim",
		},
	} {
		for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
			t.Run(name+"/mode="+mode, func(t *testing.T) {
				t.Parallel()
				shard := *tc.shard.DeepCopy()
				shard.Name = "0-inf"
				cluster := &multigresv1alpha1.MultigresCluster{
					ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
					Spec: multigresv1alpha1.MultigresClusterSpec{
						Mode:  mode,
						Cells: []multigresv1alpha1.CellConfig{{Name: "zone-1"}},
						Databases: []multigresv1alpha1.DatabaseConfig{{
							Name: "postgres", Default: true,
							TableGroups: []multigresv1alpha1.TableGroupConfig{{
								Name: "default", Default: true,
								Shards: []multigresv1alpha1.ShardConfig{shard},
							}},
						}},
					},
				}
				_, err := r.ValidateClusterLogic(t.Context(), cluster)
				if !tc.invalid {
					require.NoError(t, err)
					return
				}
				want := tc.wantErr
				if want == "" {
					want = "shard '0-inf' pool 'default' pgbackrest: " +
						"resource memory limit (1Gi) must be >= request (2Gi)"
				}
				require.ErrorContains(t, err, want)
			})
		}
	}
}

// A block with several violations is refused with one message, the same on
// every call: map order must not pick which violation is reported.
func TestValidateResourceRequirements_MessageIsStable(t *testing.T) {
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}
	seen := map[string]bool{}
	for range 50 {
		seen[ValidateResourceRequirements(resources, "c").Error()] = true
		seen[ValidateContainerResources(resources, "c").Error()] = true
	}
	require.Len(t, seen, 1, seen)
	for message := range seen {
		require.Contains(t, message, "resource cpu limit")
	}
}

// A cluster with verifyTLS: false is admitted, with a warning that names the
// locations on every write, and one without is not warned about.
func TestValidateClusterLogic_WarnsAboutUnverifiedS3(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	r := NewResolver(fake.NewClientBuilder().WithScheme(scheme).Build(), "default")
	off := false

	verified := repositoryCluster("", nil, nil)
	require.Empty(t, UnverifiedS3Locations(verified))
	warnings, err := r.ValidateClusterLogic(t.Context(), verified)
	if err == nil {
		for _, w := range warnings {
			assert.NotContains(t, w, "verifyTLS")
		}
	}

	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		unverified := repositoryCluster(mode, nil, nil)
		unverified.Spec.Databases[0].Backup.S3.VerifyTLS = &off
		require.Len(t, UnverifiedS3Locations(unverified), 1, mode)
		warnings, err = r.ValidateClusterLogicUpdate(t.Context(), unverified, unverified.DeepCopy())
		require.NotErrorIs(t, err, ErrBackupRepository, mode)
		if err == nil {
			require.NotEmpty(t, warnings, mode)
			assert.Contains(t, warnings[0], "verifyTLS is false for database", mode)
		}
	}
}
