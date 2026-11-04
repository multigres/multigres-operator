package resolver

import (
	"testing"

	"github.com/stretchr/testify/require"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// A Secret the sidecar is given for the repository is not also given to the
// postgres container, by any of the three fields that mount a Secret there
// and for any of the three backup Secrets at any level.
func TestValidateBackupSecretIsolation(t *testing.T) {
	cluster := func() *multigresv1alpha1.MultigresCluster {
		c := &multigresv1alpha1.MultigresCluster{}
		c.Spec.PostgresPasswordSecretRef.Name = "password"
		c.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "b", Region: "r", CredentialsSecret: "s3-creds",
			},
			Encryption:    &multigresv1alpha1.BackupEncryptionConfig{SecretName: "cipher"},
			PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{SecretName: "backrest-tls"},
		}
		c.Spec.Databases = []multigresv1alpha1.DatabaseConfig{{
			Name: "db",
			TableGroups: []multigresv1alpha1.TableGroupConfig{{
				Name: "tg",
				Shards: []multigresv1alpha1.ShardConfig{{
					Name: "0-inf",
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3,
						S3:   &multigresv1alpha1.S3BackupConfig{CredentialsSecret: "shard-creds"},
					},
				}},
			}},
		}}
		c.Spec.PostgresInitSecretsRef = &multigresv1alpha1.PostgresInitSecretsRef{Name: "init"}
		c.Spec.PostgresMountsSecretRef = []multigresv1alpha1.PostgresSecretMount{
			{Name: "certs", MountPath: "/certs"}, {Name: "extra", MountPath: "/extra"},
		}
		return c
	}
	require.NoError(t, ValidateBackupSecretIsolation(cluster()))
	require.NoError(t, ValidateBackupSecretIsolation(&multigresv1alpha1.MultigresCluster{}))

	for name, tc := range map[string]struct {
		mutate func(*multigresv1alpha1.MultigresClusterSpec)
		want   []string
	}{
		"mount of the S3 credentials": {
			func(s *multigresv1alpha1.MultigresClusterSpec) { s.PostgresMountsSecretRef[1].Name = "s3-creds" },
			[]string{"spec.postgresMountsSecretRef[1]", `"s3-creds"`, "spec.backup s3.credentialsSecret"},
		},
		"mount of the cipher key": {
			func(s *multigresv1alpha1.MultigresClusterSpec) { s.PostgresMountsSecretRef[0].Name = "cipher" },
			[]string{"spec.postgresMountsSecretRef[0]", "spec.backup encryption.secretName"},
		},
		"init secrets are the TLS Secret": {
			func(s *multigresv1alpha1.MultigresClusterSpec) { s.PostgresInitSecretsRef.Name = "backrest-tls" },
			[]string{"spec.postgresInitSecretsRef", "spec.backup pgbackrestTLS.secretName"},
		},
		"password Secret is a shard's credentials": {
			func(s *multigresv1alpha1.MultigresClusterSpec) { s.PostgresPasswordSecretRef.Name = "shard-creds" },
			[]string{"spec.postgresPasswordSecretRef", `shard "0-inf" backup s3.credentialsSecret`},
		},
	} {
		c := cluster()
		tc.mutate(&c.Spec)
		err := ValidateBackupSecretIsolation(c)
		require.ErrorIs(t, err, ErrBackupSecretShared, name)
		for _, want := range tc.want {
			require.ErrorContains(t, err, want, name)
		}
	}
}

// On an update the rule rejects a shared Secret that the update introduces,
// and only warns about one the stored cluster already has, so that a stored
// cluster stays writable.
func TestValidateClusterLogicUpdate_BackupSecretIsolationIsRatcheted(t *testing.T) {
	shared := func(mount string) *multigresv1alpha1.MultigresCluster {
		c := &multigresv1alpha1.MultigresCluster{}
		c.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "b", Region: "r", CredentialsSecret: "s3-creds",
			},
		}
		c.Spec.PostgresMountsSecretRef = []multigresv1alpha1.PostgresSecretMount{
			{Name: mount, MountPath: "/m"},
		}
		return c
	}
	require.Error(t, ValidateBackupSecretIsolation(shared("s3-creds")))
	require.NoError(t, ValidateBackupSecretIsolation(shared("other")))
}

// A stored cluster with a sidecar violation stays writable, but an update
// may not add a violation behind the one it has, and may repair one of two.
// The first violation alone cannot tell: it is the same message when a
// second is added, and another message when the first of two is repaired.
func TestValidateClusterLogicUpdate_SidecarViolationsAreJudgedAsASet(t *testing.T) {
	noCredentials := func() *multigresv1alpha1.BackupConfig {
		return &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "b", Region: "r"},
		}
	}
	withCredentials := func() *multigresv1alpha1.BackupConfig {
		cfg := noCredentials()
		cfg.S3.CredentialsSecret = "creds"
		return cfg
	}
	cluster := func(db1, db2 *multigresv1alpha1.BackupConfig) *multigresv1alpha1.MultigresCluster {
		c := &multigresv1alpha1.MultigresCluster{}
		c.Spec.Databases = []multigresv1alpha1.DatabaseConfig{
			{Name: "db1", Backup: db1}, {Name: "db2", Backup: db2},
		}
		return c
	}
	one := cluster(noCredentials(), withCredentials())
	two := cluster(noCredentials(), noCredentials())
	require.Len(t, sidecarConfigErrors(one), 1)
	require.Len(t, sidecarConfigErrors(two), 2)
	require.Equal(t, ValidateSidecarConfig(one).Error(), ValidateSidecarConfig(two).Error(),
		"the fixture needs two clusters whose first violation is the same")

	r := &Resolver{}
	ctx := t.Context()
	_, err := r.ValidateClusterLogicUpdate(ctx, one, two)
	require.ErrorContains(t, err, `database "db2"`, "a violation added behind a stored one")

	second := cluster(withCredentials(), noCredentials())
	warnings, err := r.ValidateClusterLogicUpdate(ctx, two, second)
	if err != nil {
		require.NotErrorIs(t, err, ErrSidecarCredentials, "repairing one of two stored violations")
	} else {
		require.NotEmpty(t, warnings)
	}
	warnings, err = r.ValidateClusterLogicUpdate(ctx, two, two)
	if err != nil {
		require.NotErrorIs(t, err, ErrSidecarCredentials, "an unchanged stored violation")
	} else {
		require.NotEmpty(t, warnings)
	}
}

// The refusal of an S3 endpoint does not quote it, so two refused endpoints
// read the same. An update is still judged by what it changes: a stored
// endpoint that is refused may stay, with a warning, and replacing it with
// another refused endpoint is a new violation. No message carries what was
// written into either endpoint.
func TestValidateClusterLogicUpdate_EndpointViolationIsJudgedByTheEndpoint(t *testing.T) {
	const marker = "hunter2-MARKER"
	cluster := func(endpoint string) *multigresv1alpha1.MultigresCluster {
		c := &multigresv1alpha1.MultigresCluster{}
		c.Spec.Databases = []multigresv1alpha1.DatabaseConfig{{
			Name: "db1",
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3: &multigresv1alpha1.S3BackupConfig{
					Bucket: "b", Region: "r", CredentialsSecret: "creds", Endpoint: endpoint,
				},
			},
		}}
		return c
	}
	//nolint:gosec // Made-up endpoints: what must not be quoted has to be in the input.
	stored := cluster("http://key:" + marker + "@minio:9000")
	other := cluster("http://key:" + marker + "@other:9000")
	require.Equal(t, ValidateSidecarConfig(stored).Error(), ValidateSidecarConfig(other).Error(),
		"the fixture needs two endpoints refused with the same text")

	r := &Resolver{}
	ctx := t.Context()
	_, err := r.ValidateClusterLogicUpdate(ctx, nil, stored)
	require.ErrorIs(t, err, ErrSidecarEndpoint, "a create")
	require.NotContains(t, err.Error(), marker)

	warnings, err := r.ValidateClusterLogicUpdate(ctx, stored, stored)
	require.NoError(t, err, "an unchanged stored violation")
	require.NotEmpty(t, warnings)
	for _, warning := range warnings {
		require.NotContains(t, warning, marker)
	}

	_, err = r.ValidateClusterLogicUpdate(ctx, stored, other)
	require.ErrorIs(
		t,
		err,
		ErrSidecarEndpoint,
		"another refused endpoint in place of the stored one",
	)
	require.NotContains(t, err.Error(), marker)

	for _, fixed := range []string{"minio:9000", "https://minio:9000", ""} {
		_, err = r.ValidateClusterLogicUpdate(ctx, stored, cluster(fixed))
		require.NoError(t, err, fixed)
	}
}
