package shard

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func sidecarTestShard() *multigresv1alpha1.Shard {
	s := newTestShard()
	loc := &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket:            "bkt",
			Region:            "us-east-1",
			Endpoint:          "https://s3.example",
			KeyPrefix:         "pfx",
			CredentialsSecret: "s3-creds",
		},
	}
	loc.Encryption = &multigresv1alpha1.BackupEncryptionConfig{SecretName: "cipher"}
	s.Spec.Backup = loc.DeepCopy()
	s.Spec.SidecarBackupLocation = loc
	s.Spec.ShardServiceAccountName = "shard-sa"
	s.Spec.Images.PgBackRestd = "registry/multigres@sha256:abc"
	return s
}

func buildSidecarPod(t *testing.T, s *multigresv1alpha1.Shard) *corev1.Pod {
	t.Helper()
	pod, err := BuildPoolPod(s, "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.NoError(t, err)
	return pod
}

func sidecarTestContainer(t *testing.T, pod *corev1.Pod, name string) corev1.Container {
	t.Helper()
	for _, c := range pod.Spec.InitContainers {
		if c.Name == name {
			return c
		}
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("container %q not found", name)
	return corev1.Container{}
}

func hasMountPath(c corev1.Container, p string) bool {
	for _, m := range c.VolumeMounts {
		if m.MountPath == p {
			return true
		}
	}
	return false
}

func hasMountName(c corev1.Container, n string) bool {
	for _, m := range c.VolumeMounts {
		if m.Name == n {
			return true
		}
	}
	return false
}

func TestBuildPoolPod_SidecarContract(t *testing.T) {
	pod := buildSidecarPod(t, sidecarTestShard())

	postgres := sidecarTestContainer(t, pod, "postgres")
	multipooler := sidecarTestContainer(t, pod, "multipooler")
	sidecar := sidecarTestContainer(t, pod, "pgbackrest")

	for _, e := range postgres.Env {
		assert.False(t, strings.HasPrefix(e.Name, "AWS_"), "postgres env %s", e.Name)
	}
	for _, e := range multipooler.Env {
		assert.False(t, strings.HasPrefix(e.Name, "AWS_"), "multipooler env %s", e.Name)
	}
	for _, c := range []corev1.Container{postgres, multipooler} {
		for _, name := range []string{
			PgBackRestCertVolumeName, PgBackRestCipherKeyVolumeName,
			PgBackRestS3VolumeName, BackupVolumeName, PgBackRestStateVolumeName,
		} {
			assert.False(t, hasMountName(c, name), "%s mounts %s", c.Name, name)
		}
	}
	assert.False(t, hasMountPath(postgres, PgBackRestControlMountPath))
	assert.True(t, hasMountPath(postgres, PgBackRestWALMountPath))

	assert.True(t, hasMountPath(multipooler, PgBackRestControlMountPath))
	assert.False(t, hasMountPath(multipooler, PgBackRestWALMountPath))

	for _, c := range pod.Spec.InitContainers {
		if c.Name != "pgbackrest" {
			assert.False(t, hasMountPath(c, PgBackRestStateMountPath), c.Name)
		}
	}
	for _, c := range pod.Spec.Containers {
		assert.False(t, hasMountPath(c, PgBackRestStateMountPath), c.Name)
	}
	assert.True(t, hasMountPath(sidecar, PgBackRestStateMountPath))
	assert.True(t, hasMountPath(sidecar, PgBackRestCertMountPath))
	assert.True(t, hasMountPath(sidecar, PgBackRestCipherKeyMountPath))
	assert.True(t, hasMountPath(sidecar, PgBackRestS3MountPath))
	assert.True(t, hasMountPath(sidecar, BackupMountPath))
	assert.True(t, hasMountPath(sidecar, PostgresPasswordMountPath))
	assert.True(t, hasMountPath(sidecar, DataMountPath))

	if pod.Spec.ShareProcessNamespace != nil {
		assert.False(t, *pod.Spec.ShareProcessNamespace)
	}
	require.NotNil(t, sidecar.SecurityContext)
	require.NotNil(t, postgres.SecurityContext)
	assert.Equal(t, *postgres.SecurityContext.RunAsUser, *sidecar.SecurityContext.RunAsUser)
	assert.Equal(t, *postgres.SecurityContext.RunAsGroup, *sidecar.SecurityContext.RunAsGroup)
	assert.False(t, *sidecar.SecurityContext.AllowPrivilegeEscalation)
	assert.Equal(t, []corev1.Capability{"ALL"}, sidecar.SecurityContext.Capabilities.Drop)

	assert.Equal(t,
		"multipooler,postgres,postgres-exporter",
		pod.Annotations[SkipContainersAnnotation])
	require.NotNil(t, pod.Spec.AutomountServiceAccountToken)
	assert.False(t, *pod.Spec.AutomountServiceAccountToken)
	assert.Equal(t, "shard-sa", pod.Spec.ServiceAccountName)

	assert.Equal(t, "registry/multigres@sha256:abc", sidecar.Image)
	assert.Equal(t,
		[]string{"tini", "--", "/multigres/bin/pgbackrestd", "serve"},
		sidecar.Command)
	require.NotNil(t, sidecar.RestartPolicy)
	assert.Equal(t, corev1.ContainerRestartPolicyAlways, *sidecar.RestartPolicy)
	assert.Nil(t, sidecar.ReadinessProbe)
	require.NotNil(t, sidecar.StartupProbe)
	assert.Equal(t,
		[]string{"/multigres/bin/pgbackrestd", "health"},
		sidecar.StartupProbe.Exec.Command)
	assert.Equal(t, int32(5), sidecar.StartupProbe.PeriodSeconds)
	assert.Equal(t, int32(60), sidecar.StartupProbe.FailureThreshold)
	assert.Equal(
		t,
		[]corev1.ContainerPort{
			{Name: "pgbackrest", ContainerPort: 8432, Protocol: corev1.ProtocolTCP},
		},
		sidecar.Ports,
	)

	state := findVolume(pod.Spec.Volumes, PgBackRestStateVolumeName)
	require.NotNil(t, state)
	assert.Equal(t, "544Mi", state.EmptyDir.SizeLimit.String())
	assert.NotNil(t, findVolume(pod.Spec.Volumes, PgBackRestWALVolumeName))
	assert.NotNil(t, findVolume(pod.Spec.Volumes, PgBackRestControlVolumeName))
	creds := findVolume(pod.Spec.Volumes, PgBackRestS3VolumeName)
	require.NotNil(t, creds)
	assert.Equal(t, "s3-creds", creds.Secret.SecretName)
}

func TestBuildPoolPod_SidecarArgs(t *testing.T) {
	pod := buildSidecarPod(t, sidecarTestShard())
	sidecar := sidecarTestContainer(t, pod, "pgbackrest")
	assert.Equal(t, []string{
		"--data-volume=/var/lib/pooler",
		"--pgdata=/var/lib/pooler/pg_data",
		"--pg-socket-dir=/var/lib/pooler/pg_sockets",
		"--pg-port=5432",
		"--pg-user=postgres",
		"--pg-password-file=/etc/postgres-password/password",
		"--location-type=s3",
		"--sandbox=required",
		"--s3-bucket=bkt",
		"--s3-region=us-east-1",
		"--s3-endpoint=https://s3.example",
		"--s3-key-prefix=pfx",
		"--s3-cred-source=static",
		"--s3-static-key-dir=/secrets/pgbackrest-s3",
		"--tls-cert-dir=/certs/pgbackrest",
		"--cipher-key-file=/secrets/pgbackrest-cipher/keys.json",
		"--restore-pidfile=/var/lib/pooler/restore_command.pid",
	}, sidecar.Args)

	postgres := sidecarTestContainer(t, pod, "postgres")
	for _, a := range postgres.Args {
		assert.False(t, strings.HasPrefix(a, "--pgbackrest"), a)
	}

	multipooler := sidecarTestContainer(t, pod, "multipooler")
	assert.Contains(
		t,
		multipooler.Args,
		"--pgbackrest-control-socket=/run/pgbackrest-control/control.sock",
	)
	assert.Contains(t, multipooler.Args, "--pgbackrest-wal-socket=/run/pgbackrest-wal/wal.sock")
	for _, a := range multipooler.Args {
		assert.False(t, strings.HasPrefix(a, "--pgbackrest-cert-file"), a)
		assert.False(t, strings.HasPrefix(a, "--pgbackrest-key-file"), a)
		assert.False(t, strings.HasPrefix(a, "--pgbackrest-ca-file"), a)
		assert.False(t, strings.HasPrefix(a, "--pgbackrest-cipher-key-file"), a)
	}
}

func TestBuildPoolPod_SidecarWebIdentityWithoutCredentialsSecret(t *testing.T) {
	s := sidecarTestShard()
	s.Spec.SidecarBackupLocation.S3.CredentialsSecret = ""
	s.Spec.Backup.S3 = s.Spec.SidecarBackupLocation.S3
	pod := buildSidecarPod(t, s)
	sidecar := sidecarTestContainer(t, pod, "pgbackrest")
	assert.Contains(t, sidecar.Args, "--s3-cred-source=web-identity")
	assert.NotContains(t, strings.Join(sidecar.Args, " "), "--s3-static-key-dir")
	assert.False(t, hasMountPath(sidecar, PgBackRestS3MountPath))
	assert.Nil(t, findVolume(pod.Spec.Volumes, PgBackRestS3VolumeName))
}

// A nil location renders as filesystem /backups, exactly as topology does.
func TestBuildPoolPod_SidecarNilLocationIsFilesystemBackups(t *testing.T) {
	s := sidecarTestShard()
	s.Spec.SidecarBackupLocation = nil
	s.Spec.Backup = nil
	pod := buildSidecarPod(t, s)
	sidecar := sidecarTestContainer(t, pod, "pgbackrest")
	assert.Contains(t, sidecar.Args, "--location-type=filesystem")
	assert.Contains(t, sidecar.Args, "--fs-repo-path=/backups")
	assert.True(t, hasMountPath(sidecar, BackupMountPath))
	assert.NotContains(t, strings.Join(sidecar.Args, " "), "--s3-")
}

func TestBuildPoolPod_SidecarOrderedBeforePostgres(t *testing.T) {
	pod := buildSidecarPod(t, sidecarTestShard())
	var names []string
	for _, c := range pod.Spec.InitContainers {
		names = append(names, c.Name)
	}
	assert.Equal(t, []string{"pgbackrest", "postgres"}, names)
}

func liteSidecarTestShard() *multigresv1alpha1.Shard {
	s := sidecarTestShard()
	s.Spec.Mode = multigresv1alpha1.ModeLite
	return s
}

func buildLiteTestPod(t *testing.T, s *multigresv1alpha1.Shard) *corev1.Pod {
	t.Helper()
	pod, err := BuildLitePod(s, "main", "z1", newTestPoolSpec(), testScheme())
	require.NoError(t, err)
	return pod
}

// A Lite pod is wired to the sidecar exactly as a pool pod is: minigres embeds
// multipooler, so it takes the same two socket flags and none of the flags and
// mounts the sidecar replaces.
func TestBuildLitePod_PgBackRestSidecar(t *testing.T) {
	pod := buildLiteTestPod(t, liteSidecarTestShard())

	names := []string{}
	for _, c := range pod.Spec.InitContainers {
		names = append(names, c.Name)
	}
	assert.Equal(t, []string{"pgbackrest", "postgres"}, names)
	assert.Equal(t, "minigres,postgres,postgres-exporter",
		pod.Annotations[SkipContainersAnnotation])
	require.NotNil(t, pod.Spec.AutomountServiceAccountToken)
	assert.False(t, *pod.Spec.AutomountServiceAccountToken)
	assert.Equal(t, "shard-sa", pod.Spec.ServiceAccountName)
	assert.NotNil(t, findVolume(pod.Spec.Volumes, PgBackRestStateVolumeName))

	minigres := sidecarTestContainer(t, pod, "minigres")
	assert.Contains(t, minigres.Args, "--pgbackrest-control-socket="+PgBackRestControlSocketPath)
	assert.Contains(t, minigres.Args, "--pgbackrest-wal-socket="+PgBackRestWALSocketPath)
	for _, arg := range minigres.Args {
		assert.NotContains(t, arg, "--pgbackrest-cert-file")
		assert.NotContains(t, arg, "--pgbackrest-key-file")
		assert.NotContains(t, arg, "--pgbackrest-ca-file")
		assert.NotContains(t, arg, "--pgbackrest-cipher-key-file")
	}
	assert.True(t, hasMountPath(minigres, PgBackRestControlMountPath))
	assert.False(t, hasMountPath(minigres, PgBackRestWALMountPath), "minigres has no WAL socket")
	assert.False(t, hasMountName(minigres, BackupVolumeName))
	assert.False(t, hasMountName(minigres, PgBackRestCertVolumeName))
	assert.False(t, hasMountName(minigres, PgBackRestCipherKeyVolumeName))
	for _, e := range minigres.Env {
		assert.NotContains(t, e.Name, "AWS_")
	}

	postgres := sidecarTestContainer(t, pod, "postgres")
	assert.True(t, hasMountPath(postgres, PgBackRestWALMountPath))
	assert.False(t, hasMountPath(postgres, PgBackRestControlMountPath))

	assert.Empty(t, sidecarSecretLeaks(t, pod), "only the sidecar may reach backup secrets")
}

// The Lite pod's sidecar and pod security context match a pool pod's,
// so the one set of builders covers both.
func TestBuildLitePod_SharesSidecarWithPoolPod(t *testing.T) {
	lite := buildLiteTestPod(t, liteSidecarTestShard())
	pool := buildSidecarPod(t, sidecarTestShard())

	assert.Equal(t,
		sidecarTestContainer(t, pool, "pgbackrest"),
		sidecarTestContainer(t, lite, "pgbackrest"))
	assert.Equal(t, pool.Spec.SecurityContext, lite.Spec.SecurityContext)
}

func TestBuildLitePod_RejectsUnrenderableLocation(t *testing.T) {
	s := liteSidecarTestShard()
	s.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Path: "/data/x",
		},
	}
	_, err := BuildLitePod(s, "main", "z1", newTestPoolSpec(), testScheme())
	require.Error(t, err)

	s = liteSidecarTestShard()
	s.Spec.Images.PgBackRestd = ""
	_, err = BuildLitePod(s, "main", "z1", newTestPoolSpec(), testScheme())
	require.Error(t, err, "a spec stored before the sidecar is refused until it is rewritten")
}

// The sidecar derives its identity from the postgres container config, so a
// pool overriding postgres.runAsUser cannot split them.
func TestBuildPoolPod_SidecarIdentityFollowsPostgresOverride(t *testing.T) {
	pool := newTestPoolSpec()
	pool.Postgres.RunAsUser = ptr.To(int64(2000))
	pool.Postgres.RunAsGroup = ptr.To(int64(2001))
	pool.Multipooler.RunAsUser = ptr.To(int64(2000))
	pod, err := BuildPoolPod(sidecarTestShard(), "main", "z1", pool, 0, testScheme())
	require.NoError(t, err)
	sc := sidecarTestContainer(t, pod, "pgbackrest").SecurityContext
	pg := sidecarTestContainer(t, pod, "postgres").SecurityContext
	assert.Equal(t, int64(2000), *sc.RunAsUser)
	assert.Equal(t, *pg.RunAsUser, *sc.RunAsUser)
	assert.Equal(t, *pg.RunAsGroup, *sc.RunAsGroup)
}

func TestPgbackrestStateSizeLimit(t *testing.T) {
	const mib = int64(1 << 20)
	// limit = max(256MiB, 2*seg) + 2*seg + 256MiB
	tests := []struct {
		name  string
		args  string
		wantM int64
	}{
		{"default", "", 544},
		{"64MiB", "--wal-segsize=64", 640},
		{"512MiB", "--data-checksums --wal-segsize=512", 2304},
		{"space separated", "--wal-segsize 64", 640},
		{"garbage falls back", "--wal-segsize=abc", 544},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestShard()
			s.Spec.InitdbArgs = multigresv1alpha1.InitdbArgs(tt.args)
			got := pgbackrestStateSizeLimit(s)
			assert.Equal(t, tt.wantM*mib, got.Value())
		})
	}
}

func TestPgbackrestStateSizeLimit_AppliedToVolume(t *testing.T) {
	s := sidecarTestShard()
	s.Spec.InitdbArgs = "--wal-segsize=512"
	pod := buildSidecarPod(t, s)
	state := findVolume(pod.Spec.Volumes, PgBackRestStateVolumeName)
	require.NotNil(t, state)
	assert.Equal(t, int64(2304<<20), state.EmptyDir.SizeLimit.Value())
}

func TestComputeSpecHash_SidecarFields(t *testing.T) {
	t.Run("a pool pod hashes the new fields", func(t *testing.T) {
		base := buildSidecarPod(t, sidecarTestShard())
		baseHash := ComputeSpecHash(base)

		automount := base.DeepCopy()
		automount.Spec.AutomountServiceAccountToken = ptr.To(true)
		assert.NotEqual(t, baseHash, ComputeSpecHash(automount))

		skip := base.DeepCopy()
		skip.Annotations[SkipContainersAnnotation] = "postgres"
		assert.NotEqual(t, baseHash, ComputeSpecHash(skip))

		restart := base.DeepCopy()
		restart.Spec.InitContainers[0].RestartPolicy = nil
		assert.NotEqual(t, baseHash, ComputeSpecHash(restart))
	})

	t.Run("a Lite pod hashes them too", func(t *testing.T) {
		base := buildLiteTestPod(t, liteSidecarTestShard())
		baseHash := ComputeSpecHash(base)

		automount := base.DeepCopy()
		automount.Spec.AutomountServiceAccountToken = ptr.To(true)
		assert.NotEqual(t, baseHash, ComputeSpecHash(automount))

		skip := base.DeepCopy()
		skip.Annotations[SkipContainersAnnotation] = "postgres"
		assert.NotEqual(t, baseHash, ComputeSpecHash(skip))
	})
}

func TestBuildPoolPod_SidecarMultipoolerWALSocketFlagWithoutMount(t *testing.T) {
	pod := buildSidecarPod(t, sidecarTestShard())
	mp := sidecarTestContainer(t, pod, "multipooler")
	assert.Contains(t, mp.Args, "--pgbackrest-wal-socket=/run/pgbackrest-wal/wal.sock")
	assert.False(t, hasMountName(mp, PgBackRestWALVolumeName))
	assert.False(t, hasMountPath(mp, PgBackRestWALMountPath))
}

func TestBuildPoolPod_SidecarCipherFollowsLocation(t *testing.T) {
	t.Run("location without encryption", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.SidecarBackupLocation.Encryption = nil
		pod := buildSidecarPod(t, s)
		sc := sidecarTestContainer(t, pod, "pgbackrest")
		assert.NotContains(t, strings.Join(sc.Args, " "), "--cipher-key-file")
		assert.False(t, hasMountPath(sc, PgBackRestCipherKeyMountPath))
		assert.Nil(t, findVolume(pod.Spec.Volumes, PgBackRestCipherKeyVolumeName))
	})
	t.Run("location with encryption", func(t *testing.T) {
		pod := buildSidecarPod(t, sidecarTestShard())
		sc := sidecarTestContainer(t, pod, "pgbackrest")
		assert.Contains(t, sc.Args, "--cipher-key-file=/secrets/pgbackrest-cipher/keys.json")
		assert.True(t, hasMountPath(sc, PgBackRestCipherKeyMountPath))
		v := findVolume(pod.Spec.Volumes, PgBackRestCipherKeyVolumeName)
		require.NotNil(t, v)
		assert.Equal(t, "cipher", v.Secret.SecretName)
	})
	t.Run("nil location has no cipher", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.SidecarBackupLocation = nil
		pod := buildSidecarPod(t, s)
		sc := sidecarTestContainer(t, pod, "pgbackrest")
		assert.NotContains(t, strings.Join(sc.Args, " "), "--cipher-key-file")
		assert.Nil(t, findVolume(pod.Spec.Volumes, PgBackRestCipherKeyVolumeName))
	})
}

// Spec.Backup is the shard-level config, which can differ from the database
// location the sidecar must match.
func TestBuildPoolPod_SidecarFollowsLocationNotShardBackup(t *testing.T) {
	s := sidecarTestShard()
	s.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type:          multigresv1alpha1.BackupTypeFilesystem,
		Encryption:    &multigresv1alpha1.BackupEncryptionConfig{SecretName: "shard-cipher"},
		PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{SecretName: "shard-tls"},
	}
	s.Spec.SidecarBackupLocation.Encryption = &multigresv1alpha1.BackupEncryptionConfig{
		SecretName: "loc-cipher",
	}
	s.Spec.SidecarBackupLocation.PgBackRestTLS = &multigresv1alpha1.PgBackRestTLSConfig{
		SecretName: "loc-tls",
	}
	pod := buildSidecarPod(t, s)

	cipher := findVolume(pod.Spec.Volumes, PgBackRestCipherKeyVolumeName)
	require.NotNil(t, cipher)
	assert.Equal(t, "loc-cipher", cipher.Secret.SecretName)

	certs := findVolume(pod.Spec.Volumes, PgBackRestCertVolumeName)
	require.NotNil(t, certs)
	require.Len(t, certs.Projected.Sources, 1)
	assert.Equal(t, "loc-tls", certs.Projected.Sources[0].Secret.Name)

	t.Run("shard backup without a location adds nothing", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.SidecarBackupLocation = nil
		pod := buildSidecarPod(t, s)
		sc := sidecarTestContainer(t, pod, "pgbackrest")
		assert.False(t, hasMountPath(sc, PgBackRestCipherKeyMountPath))
		assert.False(t, hasMountPath(sc, PgBackRestCertMountPath))
	})
}

func sidecarReconciler(t *testing.T, objs ...client.Object) (*ShardReconciler, client.Client) {
	t.Helper()
	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &ShardReconciler{
		Client:    c,
		Scheme:    scheme,
		Recorder:  record.NewFakeRecorder(10),
		APIReader: c,
	}, c
}

func secretExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"},
		&corev1.Secret{})
	if err == nil {
		return true
	}
	require.True(t, apierrors.IsNotFound(err), "get %s: %v", name, err)
	return false
}

// The pod mounts the secrets the builders name; the reconciler must have
// generated every one of them.
func TestReconcileSidecar_AutoTLSFollowsLocation(t *testing.T) {
	s := sidecarTestShard()
	s.Spec.Backup = nil
	s.Spec.SidecarBackupLocation.PgBackRestTLS = nil
	s.Labels[metadata.LabelMultigresCluster] = "test-cluster"
	r, c := sidecarReconciler(t, s)

	require.NoError(t, r.reconcilePgBackRestCerts(context.Background(), s))
	assert.True(t, secretExists(t, c, s.Name+"-pgbackrest-ca"))
	assert.True(t, secretExists(t, c, s.Name+"-pgbackrest-tls"))

	pod := buildSidecarPod(t, s)
	certs := findVolume(pod.Spec.Volumes, PgBackRestCertVolumeName)
	require.NotNil(t, certs)
	for _, src := range certs.Projected.Sources {
		assert.True(t, secretExists(t, c, src.Secret.Name), src.Secret.Name)
	}
}

func TestReconcileSidecar_UserTLSValidatedFromLocation(t *testing.T) {
	s := sidecarTestShard()
	s.Spec.Backup = &multigresv1alpha1.BackupConfig{Type: multigresv1alpha1.BackupTypeFilesystem}
	s.Spec.SidecarBackupLocation.PgBackRestTLS = &multigresv1alpha1.PgBackRestTLSConfig{
		SecretName: "loc-tls",
	}

	r, _ := sidecarReconciler(t, s)
	err := r.reconcilePgBackRestCerts(context.Background(), s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"loc-tls" not found`)

	good := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "loc-tls", Namespace: "default"},
		Data: map[string][]byte{
			"ca.crt": []byte("a"), "tls.crt": []byte("b"), "tls.key": []byte("c"),
		},
	}
	r, c := sidecarReconciler(t, s, good)
	require.NoError(t, r.reconcilePgBackRestCerts(context.Background(), s))
	assert.False(t, secretExists(t, c, s.Name+"-pgbackrest-ca"), "no auto certs for a user secret")
}

func TestReconcileSidecar_CipherValidatedFromLocation(t *testing.T) {
	t.Run("location cipher is validated even when Spec.Backup differs", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.Backup = nil
		s.Spec.SidecarBackupLocation.Encryption = &multigresv1alpha1.BackupEncryptionConfig{
			SecretName: "loc-cipher",
		}

		r, _ := sidecarReconciler(t, s)
		err := r.reconcileBackupCipherSecret(context.Background(), s)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"loc-cipher" not found`)

		missingKey := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "loc-cipher", Namespace: "default"},
		}
		r, _ = sidecarReconciler(t, s, missingKey)
		err = r.reconcileBackupCipherSecret(context.Background(), s)
		require.Error(t, err)
		assert.Contains(t, err.Error(), PgBackRestCipherKeyDataKey)

		ok := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "loc-cipher", Namespace: "default"},
			Data:       map[string][]byte{PgBackRestCipherKeyDataKey: []byte("{}")},
		}
		r, _ = sidecarReconciler(t, s, ok)
		require.NoError(t, r.reconcileBackupCipherSecret(context.Background(), s))
	})

	t.Run("shard-level cipher is ignored when the location has none", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.SidecarBackupLocation.Encryption = nil
		s.Spec.Backup.Encryption = &multigresv1alpha1.BackupEncryptionConfig{
			SecretName: "shard-cipher",
		}
		r, _ := sidecarReconciler(t, s)
		require.NoError(t, r.reconcileBackupCipherSecret(context.Background(), s))
		pod := buildSidecarPod(t, s)
		assert.Nil(t, findVolume(pod.Spec.Volumes, PgBackRestCipherKeyVolumeName))
	})
}

func TestReconcileSidecar_NilLocationHasNoCertsAndNoVolume(t *testing.T) {
	s := sidecarTestShard()
	s.Spec.SidecarBackupLocation = nil
	s.Labels[metadata.LabelMultigresCluster] = "test-cluster"
	r, c := sidecarReconciler(t, s)

	require.NoError(t, r.reconcilePgBackRestCerts(context.Background(), s))
	require.NoError(t, r.reconcileBackupCipherSecret(context.Background(), s))
	var secrets corev1.SecretList
	require.NoError(t, c.List(context.Background(), &secrets))
	assert.Empty(t, secrets.Items)

	pod := buildSidecarPod(t, s)
	assert.Nil(t, findVolume(pod.Spec.Volumes, PgBackRestCertVolumeName))
	assert.Nil(t, findVolume(pod.Spec.Volumes, PgBackRestCipherKeyVolumeName))
}

// Walks every container, so one added later that mounts a socket, the state
// dir or a backup secret fails here instead of passing unchecked.
func TestBuildPoolPod_SidecarMountsOnlyOnIntendedContainers(t *testing.T) {
	pod := buildSidecarPod(t, sidecarTestShard())
	allowed := map[string][]string{
		PgBackRestWALVolumeName:       {"postgres", "pgbackrest"},
		PgBackRestControlVolumeName:   {"multipooler", "pgbackrest"},
		PgBackRestStateVolumeName:     {"pgbackrest"},
		PgBackRestCertVolumeName:      {"pgbackrest"},
		PgBackRestCipherKeyVolumeName: {"pgbackrest"},
		PgBackRestS3VolumeName:        {"pgbackrest"},
		BackupVolumeName:              {"pgbackrest"},
	}
	all := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
	var wantSkip []string
	for _, c := range all {
		if c.Name != "pgbackrest" {
			wantSkip = append(wantSkip, c.Name)
		}
		for vol, owners := range allowed {
			if hasMountName(c, vol) {
				assert.Contains(t, owners, c.Name, "%s must not mount %s", c.Name, vol)
			}
		}
		for _, p := range []string{
			PgBackRestWALMountPath, PgBackRestControlMountPath, PgBackRestStateMountPath,
			PgBackRestCertMountPath, PgBackRestCipherKeyMountPath, PgBackRestS3MountPath,
			BackupMountPath,
		} {
			if c.Name != "pgbackrest" && hasMountPath(c, p) {
				assert.Contains(t,
					[]string{"postgres", "multipooler"}, c.Name,
					"%s must not mount path %s", c.Name, p)
				assert.Contains(t, []string{PgBackRestWALMountPath, PgBackRestControlMountPath}, p)
			}
		}
		if c.Name != "pgbackrest" {
			for _, e := range c.Env {
				assert.False(t, strings.HasPrefix(e.Name, "AWS_"), "%s env %s", c.Name, e.Name)
			}
		}
	}
	slices.Sort(wantSkip)
	assert.Equal(t, strings.Join(wantSkip, ","), pod.Annotations[SkipContainersAnnotation])
}

func TestBuildPoolPod_SidecarServiceAccount(t *testing.T) {
	t.Run("shard service account wins", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.SidecarBackupLocation.S3.ServiceAccountName = "legacy-irsa"
		assert.Equal(t, "shard-sa", buildSidecarPod(t, s).Spec.ServiceAccountName)
	})
	t.Run("falls back to the legacy IRSA account", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.ShardServiceAccountName = ""
		s.Spec.SidecarBackupLocation.S3.CredentialsSecret = ""
		s.Spec.SidecarBackupLocation.S3.ServiceAccountName = "legacy-irsa"
		pod := buildSidecarPod(t, s)
		assert.Equal(t, "legacy-irsa", pod.Spec.ServiceAccountName)
		assert.False(t, *pod.Spec.AutomountServiceAccountToken)
	})
	t.Run("neither set leaves the default account", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.ShardServiceAccountName = ""
		assert.Empty(t, buildSidecarPod(t, s).Spec.ServiceAccountName)
	})
}

func fsLocation(size, class string) *multigresv1alpha1.BackupConfig {
	return &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Storage: multigresv1alpha1.StorageSpec{Size: size, Class: class},
		},
	}
}

func TestBackupVolumeFollowsSidecarLocation(t *testing.T) {
	s3Backup := func() *multigresv1alpha1.BackupConfig {
		return &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "b", Region: "r"},
		}
	}

	t.Run("filesystem location, S3 Spec.Backup: PVC volume and PVC built", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.Backup = s3Backup()
		s.Spec.SidecarBackupLocation = fsLocation("20Gi", "fast")
		pod := buildSidecarPod(t, s)
		v := findVolume(pod.Spec.Volumes, BackupVolumeName)
		require.NotNil(t, v)
		require.NotNil(t, v.PersistentVolumeClaim, "repo must survive pod deletion")
		assert.Equal(t, BuildSharedBackupPVCName(s), v.PersistentVolumeClaim.ClaimName)
		assert.Equal(t, "fast", backupFilesystemStorageClassName(s))

		pvc, err := BuildSharedBackupPVC(s, false, testScheme())
		require.NoError(t, err)
		require.NotNil(t, pvc)
		assert.Equal(t, "20Gi", pvc.Spec.Resources.Requests.Storage().String())
		assert.Equal(t, "fast", *pvc.Spec.StorageClassName)

		r, c := sidecarReconciler(t, s)
		require.NoError(t, r.reconcileSharedBackupPVC(context.Background(), s))
		require.NoError(t, c.Get(context.Background(),
			types.NamespacedName{Name: BuildSharedBackupPVCName(s), Namespace: "default"},
			&corev1.PersistentVolumeClaim{}))
	})

	t.Run("S3 location, filesystem Spec.Backup: emptyDir and no PVC", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.Backup = fsLocation("20Gi", "fast")
		pod := buildSidecarPod(t, s)
		v := findVolume(pod.Spec.Volumes, BackupVolumeName)
		require.NotNil(t, v)
		assert.NotNil(t, v.EmptyDir)
		assert.Empty(t, backupFilesystemStorageClassName(s))

		r, c := sidecarReconciler(t, s)
		require.NoError(t, r.reconcileSharedBackupPVC(context.Background(), s))
		var pvcs corev1.PersistentVolumeClaimList
		require.NoError(t, c.List(context.Background(), &pvcs))
		assert.Empty(t, pvcs.Items)
	})

	t.Run("nil location is a filesystem repo on a PVC", func(t *testing.T) {
		s := sidecarTestShard()
		s.Spec.SidecarBackupLocation = nil
		s.Spec.Backup = s3Backup()
		pod := buildSidecarPod(t, s)
		v := findVolume(pod.Spec.Volumes, BackupVolumeName)
		require.NotNil(t, v)
		assert.NotNil(t, v.PersistentVolumeClaim)
	})

	t.Run("a Lite pod follows the location too", func(t *testing.T) {
		s := liteSidecarTestShard()
		s.Spec.SidecarBackupLocation = fsLocation("20Gi", "fast")
		pod := buildLiteTestPod(t, s)
		v := findVolume(pod.Spec.Volumes, BackupVolumeName)
		require.NotNil(t, v)
		assert.NotNil(t, v.PersistentVolumeClaim)
	})
}

func pvcWith(
	class *string,
	modes []corev1.PersistentVolumeAccessMode,
	size string,
) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: class,
			AccessModes:      modes,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(size),
			}},
		},
	}
}

func ownedBy(pvc *corev1.PersistentVolumeClaim, fields string) *corev1.PersistentVolumeClaim {
	pvc.ManagedFields = []metav1.ManagedFieldsEntry{{
		APIVersion: "v1",
		Manager:    pvcFieldOwner,
		Operation:  metav1.ManagedFieldsOperationApply,
		FieldsType: "FieldsV1",
		FieldsV1:   metav1.NewFieldsV1(`{"f:spec":{` + fields + `}}`),
	}}
	return pvc
}

func bound(pvc *corev1.PersistentVolumeClaim, capacity string) *corev1.PersistentVolumeClaim {
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Spec.VolumeName = "pv-1"
	if capacity != "" {
		pvc.Status.Capacity = corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse(capacity),
		}
	}
	return pvc
}

var (
	rwo = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	rwx = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
)

func TestAlignPVC(t *testing.T) {
	storage := func(p *corev1.PersistentVolumeClaim) string {
		return p.Spec.Resources.Requests.Storage().String()
	}

	t.Run("a differing class on an unowned claim is omitted and reported", func(t *testing.T) {
		d := pvcWith(ptr.To("new"), rwo, "10Gi")
		notes := alignPVC(d, bound(pvcWith(ptr.To("old"), rwo, "10Gi"), "10Gi"))
		assert.Nil(t, d.Spec.StorageClassName)
		require.Len(t, notes, 1)
		assert.Contains(t, notes[0], "storageClassName")
	})
	t.Run("a differing class on an owned claim re-sends the live class", func(t *testing.T) {
		d := pvcWith(ptr.To("new"), rwo, "10Gi")
		e := ownedBy(pvcWith(ptr.To("old"), rwo, "10Gi"), `"f:storageClassName":{}`)
		notes := alignPVC(d, e)
		assert.Equal(t, "old", *d.Spec.StorageClassName)
		require.Len(t, notes, 1)
	})
	t.Run("a class on a claim with none is sent, the claim must bind", func(t *testing.T) {
		d := pvcWith(ptr.To("fast"), rwo, "10Gi")
		notes := alignPVC(d, pvcWith(nil, rwo, "10Gi"))
		assert.Equal(t, "fast", *d.Spec.StorageClassName)
		assert.Empty(t, notes)
	})
	t.Run("an unspecified class never takes ownership of the defaulted one", func(t *testing.T) {
		d := pvcWith(nil, rwo, "10Gi")
		notes := alignPVC(d, pvcWith(ptr.To("std"), rwo, "10Gi"))
		assert.Nil(t, d.Spec.StorageClassName)
		assert.Empty(t, notes)
	})
	t.Run("an owned class survives the location dropping it", func(t *testing.T) {
		d := pvcWith(nil, rwo, "10Gi")
		e := ownedBy(pvcWith(ptr.To("std"), rwo, "10Gi"), `"f:storageClassName":{}`)
		alignPVC(d, e)
		assert.Equal(t, "std", *d.Spec.StorageClassName)
	})
	t.Run("changed access modes are never sent", func(t *testing.T) {
		d := pvcWith(nil, rwx, "10Gi")
		notes := alignPVC(d, pvcWith(nil, rwo, "10Gi"))
		assert.Empty(t, d.Spec.AccessModes)
		require.Len(t, notes, 1)
		d = pvcWith(nil, rwx, "10Gi")
		alignPVC(d, ownedBy(pvcWith(nil, rwo, "10Gi"), `"f:accessModes":{}`))
		assert.Equal(t, rwo, d.Spec.AccessModes)
	})
	t.Run("volumeName is never sent", func(t *testing.T) {
		d := pvcWith(nil, rwo, "10Gi")
		d.Spec.VolumeName = "x"
		alignPVC(d, bound(pvcWith(nil, rwo, "10Gi"), "10Gi"))
		assert.Empty(t, d.Spec.VolumeName)
	})
	t.Run("a bound claim grows", func(t *testing.T) {
		d := pvcWith(nil, rwo, "30Gi")
		notes := alignPVC(d, bound(pvcWith(nil, rwo, "10Gi"), "10Gi"))
		assert.Equal(t, "30Gi", storage(d))
		assert.Empty(t, notes)
	})
	t.Run(
		"a shrink at or below capacity keeps the live request and is reported",
		func(t *testing.T) {
			d := pvcWith(nil, rwo, "5Gi")
			notes := alignPVC(d, bound(pvcWith(nil, rwo, "10Gi"), "10Gi"))
			assert.Equal(t, "10Gi", storage(d))
			require.Len(t, notes, 1)
			assert.Contains(t, notes[0], "storage request")
		},
	)
	t.Run("lowering above capacity after a failed expansion is allowed", func(t *testing.T) {
		d := pvcWith(nil, rwo, "20Gi")
		notes := alignPVC(d, bound(pvcWith(nil, rwo, "30Gi"), "10Gi"))
		assert.Equal(t, "20Gi", storage(d))
		assert.Empty(t, notes)
	})
	t.Run("a pending claim keeps its request", func(t *testing.T) {
		d := pvcWith(nil, rwo, "30Gi")
		notes := alignPVC(d, pvcWith(nil, rwo, "10Gi"))
		assert.Equal(t, "10Gi", storage(d))
		require.Len(t, notes, 1)
	})
}

// Every rewrite is reported as a Warning event naming the field, so a field the
// API server refuses to change is never dropped silently.
func TestReconcileSharedBackupPVC_SidecarReportsIgnoredFields(t *testing.T) {
	s := sidecarTestShard()
	s.Spec.SidecarBackupLocation = fsLocation("5Gi", "new-class")
	s.Spec.Backup = s.Spec.SidecarBackupLocation

	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	existing := bound(pvcWith(ptr.To("old-class"), rwo, "10Gi"), "10Gi")
	existing.Name = BuildSharedBackupPVCName(s)
	existing.Namespace = "default"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s, existing).Build()
	rec := record.NewFakeRecorder(20)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: rec, APIReader: c}

	require.NoError(t, r.reconcileSharedBackupPVC(context.Background(), s))
	var events []string
	for len(rec.Events) > 0 {
		events = append(events, <-rec.Events)
	}
	joined := strings.Join(events, "\n")
	assert.Contains(t, joined, "Warning BackupPVCSpecIgnored")
	assert.Contains(t, joined, "storageClassName")
	assert.Contains(t, joined, "storage request")
}

func TestStorageClassGuard_SidecarIgnoresLocationClassWhenClaimExists(t *testing.T) {
	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	s := sidecarTestShard()
	s.Spec.SidecarBackupLocation = fsLocation("10Gi", "missing-class")
	s.Spec.Backup = s.Spec.SidecarBackupLocation

	check := func(t *testing.T, objs ...client.Object) storageClassCheck {
		t.Helper()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, APIReader: c}
		got, err := r.validateStorageClassDependencies(context.Background(), s)
		require.NoError(t, err)
		return got
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: BuildSharedBackupPVCName(s), Namespace: "default"},
	}

	assert.NoError(t, check(t, pvc).backupDependency, "claim exists: nothing needs the class")
	assert.Error(t, check(t).backupDependency, "no claim yet: the class is needed")
}

func TestValidateSidecarLocation(t *testing.T) {
	mk := func(path string) *multigresv1alpha1.Shard {
		s := sidecarTestShard()
		s.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
			Type:       multigresv1alpha1.BackupTypeFilesystem,
			Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: path},
		}
		return s
	}
	require.NoError(t, validateSidecarLocation(mk("")))
	require.NoError(t, validateSidecarLocation(mk("/backups")))
	err := validateSidecarLocation(mk("/data/x"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"/data/x"`)

	// Never render a different path than topology uses: the pod build fails.
	_, err = BuildPoolPod(mk("/data/x"), "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.Error(t, err)

	// The shard reconcile reports it as a ConfigError event.
	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	bad := mk("/data/x")
	password := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: "default"},
		Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bad, password).
		WithStatusSubresource(bad).Build()
	rec := record.NewFakeRecorder(20)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: rec, APIReader: c}
	_, err = r.Reconcile(context.Background(), ctrlRequest(bad))
	require.Error(t, err)
	var found bool
	for len(rec.Events) > 0 {
		if e := <-rec.Events; strings.Contains(e, "ConfigError") && strings.Contains(e, "/data/x") {
			found = true
		}
	}
	assert.True(t, found, "expected a ConfigError event naming the path")
}

func TestValidateSidecarLocation_RejectsEnvCredentials(t *testing.T) {
	mk := func() *multigresv1alpha1.Shard {
		s := sidecarTestShard()
		s.Spec.ShardServiceAccountName = ""
		s.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "b", Region: "r", UseEnvCredentials: true,
			},
		}
		return s
	}
	err := validateSidecarLocation(mk())
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"IRSA (serviceAccountName or shardServiceAccount) and static keys",
	)
	assert.Contains(t, err.Error(), "Pod Identity are not supported")

	// The default chain (no source at all) is rejected the same way; IRSA by
	// either route and static keys are accepted.
	none := mk()
	none.Spec.SidecarBackupLocation.S3.UseEnvCredentials = false
	require.Error(t, validateSidecarLocation(none))
	irsa := none.DeepCopy()
	irsa.Spec.SidecarBackupLocation.S3.ServiceAccountName = "irsa"
	require.NoError(t, validateSidecarLocation(irsa))
	shardSA := none.DeepCopy()
	shardSA.Spec.ShardServiceAccountName = "shard-sa"
	require.NoError(t, validateSidecarLocation(shardSA))
	staticOnly := none.DeepCopy()
	staticOnly.Spec.SidecarBackupLocation.S3.CredentialsSecret = "keys"
	require.NoError(t, validateSidecarLocation(staticOnly))

	// The documented static-key form (credentialsSecret with useEnvCredentials)
	// renders as static keys and stays valid.
	static := mk()
	static.Spec.SidecarBackupLocation.S3.CredentialsSecret = "keys"
	require.NoError(t, validateSidecarLocation(static))

	// No sidecar that cannot get credentials is rendered.
	_, err = BuildPoolPod(mk(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.Error(t, err)

	// The shard reconcile reports it as a ConfigError event.
	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	bad := mk()
	password := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: "default"},
		Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bad, password).
		WithStatusSubresource(bad).Build()
	rec := record.NewFakeRecorder(20)
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: rec, APIReader: c}
	_, err = r.Reconcile(context.Background(), ctrlRequest(bad))
	require.Error(t, err)
	var found bool
	for len(rec.Events) > 0 {
		if e := <-rec.Events; strings.Contains(e, "ConfigError") &&
			strings.Contains(e, "useEnvCredentials") {
			found = true
		}
	}
	assert.True(t, found, "expected a ConfigError event naming useEnvCredentials")
}

// secretNamesOf lists the Secrets a volume reads.
func secretNamesOf(v corev1.Volume) []string {
	var names []string
	if v.Secret != nil {
		names = append(names, v.Secret.SecretName)
	}
	if v.Projected != nil {
		for _, src := range v.Projected.Sources {
			if src.Secret != nil {
				names = append(names, src.Secret.Name)
			}
		}
	}
	return names
}

// sidecarSecretLeaks reports every route by which a container other than the
// sidecar reaches a backup, cipher, cert or S3 Secret: a volume over the same
// Secret under any name, envFrom, or a secretKeyRef.
func sidecarSecretLeaks(t *testing.T, pod *corev1.Pod) []string {
	t.Helper()
	secrets := map[string]bool{}
	for _, vn := range []string{
		PgBackRestCertVolumeName, PgBackRestCipherKeyVolumeName, PgBackRestS3VolumeName,
	} {
		v := findVolume(pod.Spec.Volumes, vn)
		require.NotNil(t, v, vn)
		for _, n := range secretNamesOf(*v) {
			secrets[n] = true
		}
	}
	require.GreaterOrEqual(t, len(secrets), 3, "fixture should expose cert, cipher and S3 Secrets")

	volumeSecrets := map[string][]string{}
	for _, v := range pod.Spec.Volumes {
		volumeSecrets[v.Name] = secretNamesOf(v)
	}
	all := append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...)
	var leaks []string
	for _, c := range all {
		if c.Name == "pgbackrest" {
			continue
		}
		for _, m := range c.VolumeMounts {
			for _, n := range volumeSecrets[m.Name] {
				if secrets[n] {
					leaks = append(leaks, c.Name+" mounts volume "+m.Name+" over Secret "+n)
				}
			}
		}
		for _, ef := range c.EnvFrom {
			if ef.SecretRef != nil && secrets[ef.SecretRef.Name] {
				leaks = append(leaks, c.Name+" envFrom Secret "+ef.SecretRef.Name)
			}
		}
		for _, e := range c.Env {
			if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil &&
				secrets[e.ValueFrom.SecretKeyRef.Name] {
				leaks = append(
					leaks,
					c.Name+" env "+e.Name+" from Secret "+e.ValueFrom.SecretKeyRef.Name,
				)
			}
		}
	}
	return leaks
}

func TestBuildPoolPod_SidecarSecretsReachableOnlyBySidecar(t *testing.T) {
	pod := buildSidecarPod(t, sidecarTestShard())
	assert.Empty(t, sidecarSecretLeaks(t, pod))

	t.Run("the walk catches each route", func(t *testing.T) {
		envFrom := pod.DeepCopy()
		envFrom.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "s3-creds"},
			},
		}}
		assert.NotEmpty(t, sidecarSecretLeaks(t, envFrom))

		keyRef := pod.DeepCopy()
		keyRef.Spec.Containers[0].Env = append(keyRef.Spec.Containers[0].Env, corev1.EnvVar{
			Name: "X",
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cipher"}, Key: "k",
			}},
		})
		assert.NotEmpty(t, sidecarSecretLeaks(t, keyRef))

		volume := pod.DeepCopy()
		volume.Spec.Volumes = append(volume.Spec.Volumes, corev1.Volume{
			Name: "innocuous",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: "s3-creds"},
			},
		})
		volume.Spec.Containers[0].VolumeMounts = append(
			volume.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "innocuous", MountPath: "/x"},
		)
		assert.NotEmpty(t, sidecarSecretLeaks(t, volume))
	})
}

func ctrlRequest(s *multigresv1alpha1.Shard) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: s.Name, Namespace: s.Namespace}}
}

// The fake client drops managedFields, so ownership is tested on the helper
// directly; the envtest flip tests exercise it against a real API server.
func TestOperatorOwnedSpecFields(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{ManagedFields: []metav1.ManagedFieldsEntry{
			{
				Manager:   pvcFieldOwner,
				Operation: metav1.ManagedFieldsOperationApply,
				FieldsV1: metav1.NewFieldsV1(
					`{"f:spec":{"f:storageClassName":{},"f:accessModes":{}}}`,
				),
			},
			{
				Manager: "kube-controller-manager", Operation: metav1.ManagedFieldsOperationUpdate,
				FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:volumeName":{}}}`),
			},
			{
				Manager: pvcFieldOwner, Operation: metav1.ManagedFieldsOperationUpdate,
				FieldsV1: metav1.NewFieldsV1(`{"f:spec":{"f:selector":{}}}`),
			},
		}},
	}
	owned := operatorOwnedSpecFields(pvc)
	assert.True(t, owned["f:storageClassName"])
	assert.True(t, owned["f:accessModes"])
	assert.False(t, owned["f:volumeName"], "another manager's field")
	assert.False(t, owned["f:selector"], "only apply entries count")
}

func TestBuildPoolPod_SidecarHardening(t *testing.T) {
	pod := buildSidecarPod(t, sidecarTestShard())
	sc := sidecarTestContainer(t, pod, "pgbackrest").SecurityContext
	require.NotNil(t, sc.SeccompProfile)
	assert.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, sc.SeccompProfile.Type)
	require.NotNil(t, sc.ReadOnlyRootFilesystem)
	assert.True(t, *sc.ReadOnlyRootFilesystem)
	assert.Equal(t, int64(999), *sc.RunAsUser)
	assert.False(t, *sc.AllowPrivilegeEscalation)

	for _, name := range []string{"postgres", "multipooler"} {
		other := sidecarTestContainer(t, pod, name).SecurityContext
		assert.Nil(t, other.SeccompProfile, name)
		assert.Nil(t, other.ReadOnlyRootFilesystem, name)
	}
}

func TestBuildPoolPod_SidecarFSGroup(t *testing.T) {
	build := func(t *testing.T, s *multigresv1alpha1.Shard, mutate func(*multigresv1alpha1.PoolSpec)) *corev1.Pod {
		t.Helper()
		pool := newTestPoolSpec()
		pool.FSGroup = nil
		mutate(&pool)
		pod, err := BuildPoolPod(s, "main", "z1", pool, 0, testScheme())
		require.NoError(t, err)
		return pod
	}

	t.Run("defaults to the postgres gid", func(t *testing.T) {
		pod := build(t, sidecarTestShard(), func(*multigresv1alpha1.PoolSpec) {})
		psc := pod.Spec.SecurityContext
		require.NotNil(t, psc)
		assert.Equal(t, DefaultPostgresGID, *psc.FSGroup)
		assert.Equal(t, corev1.FSGroupChangeOnRootMismatch, *psc.FSGroupChangePolicy)
	})

	t.Run("follows a postgres group override", func(t *testing.T) {
		pod := build(t, sidecarTestShard(), func(p *multigresv1alpha1.PoolSpec) {
			p.Postgres.RunAsGroup = ptr.To(int64(2001))
		})
		assert.Equal(t, int64(2001), *pod.Spec.SecurityContext.FSGroup)
	})

	t.Run("an explicit fsGroup is respected", func(t *testing.T) {
		pod := build(t, sidecarTestShard(), func(p *multigresv1alpha1.PoolSpec) {
			p.FSGroup = ptr.To(int64(4242))
		})
		assert.Equal(t, int64(4242), *pod.Spec.SecurityContext.FSGroup)
	})

	t.Run("a Lite pod defaults it too", func(t *testing.T) {
		pod := buildLiteTestPod(t, liteSidecarTestShard())
		assert.Equal(t, int64(999), *pod.Spec.SecurityContext.FSGroup)
	})
}

// The new fields live in the pod and container securityContexts, which the
// spec hash already covers, so a change rolls the pod.
func TestComputeSpecHash_SidecarHardeningRollsPod(t *testing.T) {
	base := buildSidecarPod(t, sidecarTestShard())
	baseHash := ComputeSpecHash(base)

	seccomp := base.DeepCopy()
	seccomp.Spec.InitContainers[0].SecurityContext.SeccompProfile = nil
	assert.NotEqual(t, baseHash, ComputeSpecHash(seccomp))

	ro := base.DeepCopy()
	ro.Spec.InitContainers[0].SecurityContext.ReadOnlyRootFilesystem = nil
	assert.NotEqual(t, baseHash, ComputeSpecHash(ro))

	fsg := base.DeepCopy()
	fsg.Spec.SecurityContext = nil
	assert.NotEqual(t, baseHash, ComputeSpecHash(fsg))
}

func TestBuildPoolPod_SidecarResources(t *testing.T) {
	build := func(t *testing.T, s *multigresv1alpha1.Shard, mutate func(*multigresv1alpha1.PoolSpec)) *corev1.Pod {
		t.Helper()
		pool := newTestPoolSpec()
		mutate(&pool)
		pod, err := BuildPoolPod(s, "main", "z1", pool, 0, testScheme())
		require.NoError(t, err)
		return pod
	}

	t.Run("defaults request cpu and memory and cap only memory", func(t *testing.T) {
		pod := build(t, sidecarTestShard(), func(*multigresv1alpha1.PoolSpec) {})
		res := sidecarTestContainer(t, pod, "pgbackrest").Resources
		assert.True(t, res.Requests.Cpu().Equal(resource.MustParse("100m")))
		assert.True(t, res.Requests.Memory().Equal(resource.MustParse("256Mi")))
		assert.True(t, res.Limits.Memory().Equal(resource.MustParse("1Gi")))
		_, hasCPULimit := res.Limits[corev1.ResourceCPU]
		assert.False(t, hasCPULimit, "pgBackRest compression bursts, so no cpu limit")
	})

	t.Run("an override replaces the defaults", func(t *testing.T) {
		pod := build(t, sidecarTestShard(), func(p *multigresv1alpha1.PoolSpec) {
			p.PgBackRestResources = corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
			}
		})
		res := sidecarTestContainer(t, pod, "pgbackrest").Resources
		assert.True(t, res.Requests.Cpu().Equal(resource.MustParse("2")))
		assert.True(t, res.Requests.Memory().IsZero())
		assert.True(t, res.Limits.Memory().Equal(resource.MustParse("4Gi")))
	})

	t.Run("the override does not touch the other containers", func(t *testing.T) {
		override := corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
		}
		pod := build(t, sidecarTestShard(), func(p *multigresv1alpha1.PoolSpec) {
			p.PgBackRestResources = override
		})
		base := build(t, sidecarTestShard(), func(*multigresv1alpha1.PoolSpec) {})
		for _, name := range []string{"postgres", "multipooler"} {
			assert.Equal(t,
				sidecarTestContainer(t, base, name).Resources,
				sidecarTestContainer(t, pod, name).Resources, name)
		}
	})

	t.Run("a Lite pod takes the override", func(t *testing.T) {
		lite := liteSidecarTestShard()
		pool := newTestPoolSpec()
		pool.PgBackRestResources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
		}
		pod, err := BuildLitePod(lite, "main", "z1", pool, testScheme())
		require.NoError(t, err)
		assert.Equal(t, pool.PgBackRestResources,
			sidecarTestContainer(t, pod, "pgbackrest").Resources)
	})

	t.Run("a changed override rolls the pod", func(t *testing.T) {
		defaults := build(t, sidecarTestShard(), func(*multigresv1alpha1.PoolSpec) {})
		changed := build(t, sidecarTestShard(), func(p *multigresv1alpha1.PoolSpec) {
			p.PgBackRestResources = corev1.ResourceRequirements{
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
			}
		})
		assert.NotEqual(t, ComputeSpecHash(defaults), ComputeSpecHash(changed))
	})
}

func TestBuildPoolPod_SidecarProbes(t *testing.T) {
	sidecar := sidecarTestContainer(t, buildSidecarPod(t, sidecarTestShard()), "pgbackrest")
	health := []string{"/multigres/bin/pgbackrestd", "health"}

	require.NotNil(t, sidecar.StartupProbe)
	assert.Equal(t, health, sidecar.StartupProbe.Exec.Command)
	assert.Equal(t, int32(5), sidecar.StartupProbe.TimeoutSeconds)
	assert.Equal(t, int32(5), sidecar.StartupProbe.PeriodSeconds)
	assert.Equal(t, int32(60), sidecar.StartupProbe.FailureThreshold)

	require.NotNil(t, sidecar.LivenessProbe)
	assert.Equal(t, health, sidecar.LivenessProbe.Exec.Command)
	assert.Equal(t, int32(30), sidecar.LivenessProbe.PeriodSeconds)
	assert.Equal(t, int32(5), sidecar.LivenessProbe.TimeoutSeconds)
	assert.Equal(t, int32(6), sidecar.LivenessProbe.FailureThreshold)
	assert.Zero(t, sidecar.LivenessProbe.InitialDelaySeconds,
		"kubelet runs no liveness probe before the startup probe passes")

	pod := buildSidecarPod(t, sidecarTestShard())

	// A change to the probe rolls the pod.
	changed := pod.DeepCopy()
	for i := range changed.Spec.InitContainers {
		if changed.Spec.InitContainers[i].Name == "pgbackrest" {
			changed.Spec.InitContainers[i].LivenessProbe.FailureThreshold = 3
		}
	}
	assert.NotEqual(t, ComputeSpecHash(pod), ComputeSpecHash(changed))
}

// Upgrading in place gives a pool without an explicit fsGroup one, on a data
// volume that has never had a group applied. Kubelet then recursively chowns
// the whole volume once before the pod starts (OnRootMismatch skips it on every
// later start), so a large PGDATA delays that pod's start. Safe for postgres:
// pgctld normalises PGDATA to 0700 and rewrites the passfile 0600. The claim is
// the one a pod that ran pgBackRest in-process already mounts, so no data moves.
func TestBuildPoolPod_SidecarFirstFSGroupOnPopulatedVolume(t *testing.T) {
	pool := newTestPoolSpec()
	pool.FSGroup = nil
	s := sidecarTestShard()
	pod, err := BuildPoolPod(s, "main", "z1", pool, 0, testScheme())
	require.NoError(t, err)

	require.NotNil(t, pod.Spec.SecurityContext)
	require.NotNil(t, pod.Spec.SecurityContext.FSGroup)
	assert.Equal(t, corev1.FSGroupChangeOnRootMismatch,
		*pod.Spec.SecurityContext.FSGroupChangePolicy,
		"the walk must happen once, not on every start")

	v := findVolume(pod.Spec.Volumes, DataVolumeName)
	require.NotNil(t, v)
	require.NotNil(t, v.PersistentVolumeClaim)
	assert.Equal(t, BuildPoolDataPVCName(s, "main", "z1", 0), v.PersistentVolumeClaim.ClaimName,
		"the same populated claim is reused across the upgrade")
}
