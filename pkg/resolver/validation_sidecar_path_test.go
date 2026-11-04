package resolver

import (
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestValidateSidecarFilesystemPath(t *testing.T) {
	t.Parallel()
	fs := func(path string) *multigresv1alpha1.BackupConfig {
		return &multigresv1alpha1.BackupConfig{
			Type:       multigresv1alpha1.BackupTypeFilesystem,
			Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: path},
		}
	}
	cluster := func(
		top, db *multigresv1alpha1.BackupConfig,
	) *multigresv1alpha1.MultigresCluster {
		return &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Backup: top,
				Databases: []multigresv1alpha1.DatabaseConfig{
					{Name: "postgres", Backup: db},
				},
			},
		}
	}

	tests := map[string]struct {
		cluster *multigresv1alpha1.MultigresCluster
		wantErr string
	}{
		"path /backups": {cluster(fs("/backups"), nil), ""},
		"path unset":    {cluster(fs(""), nil), ""},
		"nil backup":    {cluster(nil, nil), ""},
		"S3 backup": {
			cluster(
				&multigresv1alpha1.BackupConfig{Type: multigresv1alpha1.BackupTypeS3},
				nil,
			),
			"",
		},
		"rejects another cluster path": {
			cluster(fs("/data/bk"), nil),
			`path "/data/bk" is not supported by the pgBackRest sidecar`,
		},
		"rejects database override": {
			cluster(fs("/backups"), fs("/var/lib/pooler")),
			`database "postgres" backup`,
		},
		"rejects trailing slash": {
			cluster(fs("/backups/"), nil),
			"must be \"/backups\" or unset",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := validateSidecarFilesystemPath(tc.cluster)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

// The check must be reachable from the admission path, not only callable.
func TestValidateClusterLogic_RejectsSidecarFilesystemPath(t *testing.T) {
	t.Parallel()
	r := NewResolver(fake.NewClientBuilder().WithScheme(setupScheme()).Build(), "default")
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Backup: &multigresv1alpha1.BackupConfig{
				Type:       multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/elsewhere"},
			},
		},
	}
	_, err := r.ValidateClusterLogic(t.Context(), cluster)
	if err == nil || !strings.Contains(err.Error(), "/elsewhere") {
		t.Fatalf("want path rejection, got %v", err)
	}
}

func TestValidateSidecarCredentials(t *testing.T) {
	t.Parallel()
	s3 := func(mut func(*multigresv1alpha1.S3BackupConfig)) *multigresv1alpha1.BackupConfig {
		cfg := &multigresv1alpha1.S3BackupConfig{Bucket: "b", Region: "r"}
		mut(cfg)
		return &multigresv1alpha1.BackupConfig{Type: multigresv1alpha1.BackupTypeS3, S3: cfg}
	}
	env := s3(func(c *multigresv1alpha1.S3BackupConfig) { c.UseEnvCredentials = true })
	none := s3(func(*multigresv1alpha1.S3BackupConfig) {})
	irsa := s3(func(c *multigresv1alpha1.S3BackupConfig) { c.ServiceAccountName = "sa" })
	static := s3(func(c *multigresv1alpha1.S3BackupConfig) { c.CredentialsSecret = "keys" })
	cluster := func(
		top, db *multigresv1alpha1.BackupConfig,
	) *multigresv1alpha1.MultigresCluster {
		return &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Backup: top,
				Databases: []multigresv1alpha1.DatabaseConfig{
					{Name: "postgres", Backup: db},
				},
			},
		}
	}

	tests := map[string]struct {
		cluster *multigresv1alpha1.MultigresCluster
		wantErr string
	}{
		"with IRSA": {cluster(irsa, nil), ""},
		"with documented static form": {
			cluster(s3(func(c *multigresv1alpha1.S3BackupConfig) {
				c.UseEnvCredentials = true
				c.CredentialsSecret = "keys"
			}), nil),
			"",
		},
		"with static keys": {cluster(static, nil), ""},
		"with no backup":   {cluster(nil, nil), ""},
		"with filesystem backup": {
			cluster(
				&multigresv1alpha1.BackupConfig{Type: multigresv1alpha1.BackupTypeFilesystem},
				nil,
			),
			"",
		},
		"rejects cluster env": {cluster(env, nil), "spec.backup"},
		"rejects no credential source": {
			cluster(none, nil),
			"no credential source the sidecar supports",
		},
		"rejects database with no source": {
			cluster(nil, none),
			`database "postgres" backup`,
		},
		"accepts the shard service account as IRSA": {
			func() *multigresv1alpha1.MultigresCluster {
				c := cluster(none, nil)
				c.Spec.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{Name: "sa"}
				return c
			}(),
			"",
		},
		"message names the alternatives": {
			cluster(env, nil),
			"EKS Pod Identity are not supported",
		},
		"message names IRSA and static": {
			cluster(env, nil),
			"IRSA (serviceAccountName or shardServiceAccount) and static keys",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := ValidateSidecarCredentials(tc.cluster)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
			if !errors.Is(err, ErrSidecarCredentials) {
				t.Fatalf("error %v does not wrap ErrSidecarCredentials", err)
			}
		})
	}

	r := NewResolver(fake.NewClientBuilder().WithScheme(setupScheme()).Build(), "default")
	if _, err := r.ValidateClusterLogic(t.Context(), cluster(env, nil)); err == nil ||
		!errors.Is(err, ErrSidecarCredentials) {
		t.Fatalf("ValidateClusterLogic must reject it, got %v", err)
	}
	lite := cluster(env, nil)
	lite.Spec.Mode = multigresv1alpha1.ModeLite
	if _, err := r.ValidateClusterLogic(t.Context(), lite); err == nil ||
		!errors.Is(err, ErrSidecarCredentials) {
		t.Fatalf("a Lite cluster runs the sidecar too and must be rejected, got %v", err)
	}
}

// An update of an object that already violates a sidecar rule must not be
// rejected, or the operator's own writes and finalizer removal would fail. A
// new or changed violation still is.
func TestValidateClusterLogicUpdate_ToleratesExistingSidecarViolation(t *testing.T) {
	t.Parallel()
	r := NewResolver(fake.NewClientBuilder().WithScheme(setupScheme()).Build(), "default")
	badPath := func(path string) *multigresv1alpha1.MultigresCluster {
		return &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type:       multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: path},
				},
			},
		}
	}
	noSource := badPath("")
	noSource.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "b", Region: "r"},
	}

	for name, cluster := range map[string]*multigresv1alpha1.MultigresCluster{
		"path":              badPath("/data/bk"),
		"no credential src": noSource,
	} {
		old := cluster.DeepCopy()
		updated := cluster.DeepCopy()
		updated.Finalizers = []string{"x"}
		warnings, err := r.ValidateClusterLogicUpdate(t.Context(), old, updated)
		if err != nil || len(warnings) == 0 {
			t.Errorf(
				"%s: unchanged violation: warnings=%v err=%v, want a warning only",
				name,
				warnings,
				err,
			)
		}
		if _, err := r.ValidateClusterLogicUpdate(t.Context(), nil, updated); err == nil {
			t.Errorf("%s: create must still be rejected", name)
		}
	}

	// A different violation than the stored one is introduced, not tolerated.
	if _, err := r.ValidateClusterLogicUpdate(
		t.Context(), badPath("/data/bk"), badPath("/other"),
	); err == nil {
		t.Error("changing the offending path must be rejected")
	}
	// A previously valid object turning invalid is rejected.
	if _, err := r.ValidateClusterLogicUpdate(
		t.Context(), badPath("/backups"), badPath("/data/bk"),
	); err == nil {
		t.Error("introducing the violation must be rejected")
	}
}
