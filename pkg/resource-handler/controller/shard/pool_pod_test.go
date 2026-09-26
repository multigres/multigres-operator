package shard

import (
	"crypto/x509"
	"fmt"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func newTestShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: "multigres-admin-password",
				Key:  PostgresPasswordSecretKey,
			},
		},
	}
}

func newTestPoolSpec() multigresv1alpha1.PoolSpec {
	return multigresv1alpha1.PoolSpec{
		Type: "replica",
		Storage: multigresv1alpha1.StorageSpec{
			Size: "10Gi",
		},
	}
}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	return scheme
}

func TestBuildPoolPod_BasicStructure(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	pool := newTestPoolSpec()

	pod, err := BuildPoolPod(shard, "main", "z1", pool, 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Eq("default", pod.Namespace, "namespace")

	// Verify owner reference
	c.Require().
		Len(pod.OwnerReferences, 1, "expected 1 owner reference, got %d", len(pod.OwnerReferences))
	c.Eq("test-shard", pod.OwnerReferences[0].Name, "owner name")
	c.Eq("Shard", pod.OwnerReferences[0].Kind, "owner kind")

	// Verify labels
	expectedLabels := map[string]string{
		"app.kubernetes.io/instance":   "test-cluster",
		"app.kubernetes.io/component":  PoolComponentName,
		"app.kubernetes.io/managed-by": "multigres-operator",
		"multigres.com/cluster":        "test-cluster",
		"multigres.com/cell":           "z1",
		"multigres.com/pool":           "main",
		"multigres.com/shard":          "0-inf",
		"multigres.com/database":       "postgres",
		"multigres.com/tablegroup":     "default",
	}
	for k, want := range expectedLabels {
		got := pod.Labels[k]
		c.Eq(want, got, "label %q = %q, want", k, got)
	}

	got := pod.Annotations[metadata.AnnotationProjectRef]
	c.Eq("test-cluster", got, "annotation %q = %q, want", metadata.AnnotationProjectRef, got)
	if len(pod.Spec.ReadinessGates) != 1 ||
		pod.Spec.ReadinessGates[0].ConditionType != PoolerDataReadyCondition {
		t.Errorf(
			"readiness gates = %#v, want %q",
			pod.Spec.ReadinessGates,
			PoolerDataReadyCondition,
		)
	}
}

func TestBuildPoolPod_ProjectRefAnnotation(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		want        string
	}{
		"falls back to cluster name": {
			want: "test-cluster",
		},
		"uses explicit project ref": {
			annotations: map[string]string{
				metadata.AnnotationProjectRef: "proj_123",
			},
			want: "proj_123",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewAborting(t)
			shard := newTestShard()
			shard.Annotations = tc.annotations

			pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
			c.NoError(err, "unexpected error")

			got := pod.Annotations[metadata.AnnotationProjectRef]
			c.Eq(tc.want, got, "annotation %q = %q, want", metadata.AnnotationProjectRef, got)
		})
	}
}

func TestBuildPoolPod_PrometheusScrapeAnnotations(t *testing.T) {
	c := assert.NewAborting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.NoError(err, "unexpected error")

	wantAnnotations := map[string]string{
		metadata.AnnotationPrometheusScrape: "true",
		metadata.AnnotationPrometheusPort:   "9187",
		metadata.AnnotationPrometheusPath:   "/metrics",
	}
	for key, want := range wantAnnotations {
		got := pod.Annotations[key]
		c.Eq(want, got, "annotation %q = %q, want", key, got)
	}
}

func TestBuildPoolPod_Containers(t *testing.T) {
	c := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Require().
		Len(pod.Spec.InitContainers, 1, "expected 1 init container (pgctld sidecar), got %d", len(pod.Spec.InitContainers))
	c.Eq("postgres", pod.Spec.InitContainers[0].Name, "init container name")

	c.Require().
		Len(pod.Spec.Containers, 2, "expected 2 containers (multipooler + postgres-exporter), got %d", len(pod.Spec.Containers))
	c.Eq("multipooler", pod.Spec.Containers[0].Name, "container name")
	c.Eq("postgres-exporter", pod.Spec.Containers[1].Name, "container name")
}

func TestBuildPoolPod_Volumes(t *testing.T) {
	c := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	volumeNames := make(map[string]bool)
	for _, v := range pod.Spec.Volumes {
		volumeNames[v.Name] = true
	}

	required := []string{
		DataVolumeName,
		"backup-data",
		"socket-dir",
		PgHbaVolumeName,
		PostgresPasswordVolumeName,
	}
	for _, name := range required {
		c.False(!volumeNames[name], "missing required volume %q", name)
	}

	// Verify data volume references PVC
	for _, v := range pod.Spec.Volumes {
		if v.Name == DataVolumeName {
			c.Require().NotNil(v.PersistentVolumeClaim, "data volume should reference a PVC")
			pvcName := v.PersistentVolumeClaim.ClaimName
			c.NotEq("", pvcName, "data volume PVC claim name is empty")
		}
	}
}

func TestBuildPoolPod_UsesShardWideBackupPVC(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type:       multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{},
	}

	pool := newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"zone-a", "zone-b"}
	podA, err := BuildPoolPod(shard, "main", "zone-a", pool, 0, testScheme())
	c.Require().NoError(err, "build zone-a pooler pod")
	podB, err := BuildPoolPod(shard, "main", "zone-b", pool, 0, testScheme())
	c.Require().NoError(err, "build zone-b pooler pod")

	backupClaim := func(pod *corev1.Pod) string {
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == BackupVolumeName && volume.PersistentVolumeClaim != nil {
				return volume.PersistentVolumeClaim.ClaimName
			}
		}
		return ""
	}

	want := BuildSharedBackupPVCName(shard)
	c.Eq(want, backupClaim(podA), "zone-a backup claim")
	c.Eq(want, backupClaim(podB), "zone-b backup claim")
}

func TestBuildPoolPod_PostgresPasswordFile(t *testing.T) {
	ck := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	ck.Require().NoError(err, "unexpected error")

	passwordVolume := findVolume(pod.Spec.Volumes, PostgresPasswordVolumeName)
	ck.Require().
		NotNil(passwordVolume, "missing postgres password volume %q", PostgresPasswordVolumeName)
	ck.Require().NotNil(passwordVolume.Secret, "postgres password volume should use Secret source")
	ck.Eq(
		"multigres-admin-password",
		passwordVolume.Secret.SecretName,
		"postgres password SecretName",
	)
	if passwordVolume.Secret.DefaultMode == nil || *passwordVolume.Secret.DefaultMode != 0o444 {
		t.Errorf("postgres password defaultMode = %v, want 0444", passwordVolume.Secret.DefaultMode)
	}

	postgres := pod.Spec.InitContainers[0]
	multipooler := pod.Spec.Containers[0]
	for name, c := range map[string]corev1.Container{
		"postgres":    postgres,
		"multipooler": multipooler,
	} {
		t.Run(name, func(t *testing.T) {
			assertEnvVarValue(t, c.Env, "POSTGRES_PASSWORD_FILE", PostgresPasswordFilePath)
			assertNotContainsEnvVar(t, c.Env, "POSTGRES_PASSWORD")
			assertReadOnlyVolumeMount(
				t,
				c.VolumeMounts,
				PostgresPasswordVolumeName,
				PostgresPasswordMountPath,
			)
		})
	}

	exporter := pod.Spec.Containers[1]
	assertEnvVarValue(t, exporter.Env, "DATA_SOURCE_PASS_FILE", PostgresPasswordFilePath)
	assertNotContainsEnvVar(t, exporter.Env, "DATA_SOURCE_PASS")
	assertReadOnlyVolumeMount(
		t,
		exporter.VolumeMounts,
		PostgresPasswordVolumeName,
		PostgresPasswordMountPath,
	)
	assertNotContainsEnvVar(t, exporter.Env, "POSTGRES_PASSWORD")
	assertNotContainsEnvVar(t, exporter.Env, "POSTGRES_PASSWORD_FILE")
}

func TestBuildPoolPod_PostgresPasswordSecretRef(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	shard.Spec.PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef{
		Name: "multigres-admin-password",
		Key:  "current",
	}

	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	passwordVolume := findVolume(pod.Spec.Volumes, PostgresPasswordVolumeName)
	c.Require().
		NotNil(passwordVolume, "missing postgres password volume %q", PostgresPasswordVolumeName)
	c.Require().NotNil(passwordVolume.Secret, "postgres password volume should use Secret source")
	c.Eq(
		"multigres-admin-password",
		passwordVolume.Secret.SecretName,
		"postgres password SecretName",
	)
	if len(passwordVolume.Secret.Items) != 1 ||
		passwordVolume.Secret.Items[0].Key != "current" ||
		passwordVolume.Secret.Items[0].Path != PostgresPasswordSecretKey {
		t.Errorf(
			"postgres password Secret items = %+v, want key current projected to password",
			passwordVolume.Secret.Items,
		)
	}

	exporter := pod.Spec.Containers[1]
	assertEnvVarValue(t, exporter.Env, "DATA_SOURCE_PASS_FILE", PostgresPasswordFilePath)
	assertNotContainsEnvVar(t, exporter.Env, "DATA_SOURCE_PASS")
	assertReadOnlyVolumeMount(
		t,
		exporter.VolumeMounts,
		PostgresPasswordVolumeName,
		PostgresPasswordMountPath,
	)
}

func TestBuildPoolPod_PostgresInitSecretsRef(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	shard.Spec.PostgresInitSecretsRef = &multigresv1alpha1.PostgresInitSecretsRef{
		Name: "multigres-init-secrets",
		Key:  "custom-key.json",
	}

	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	initSecretsVolume := findVolume(pod.Spec.Volumes, PostgresInitSecretsVolumeName)
	c.Require().
		NotNil(initSecretsVolume, "missing postgres init-secrets volume %q", PostgresInitSecretsVolumeName)
	c.Require().
		NotNil(initSecretsVolume.Secret, "postgres init-secrets volume should use Secret source")
	c.Eq(
		"multigres-init-secrets",
		initSecretsVolume.Secret.SecretName,
		"postgres init-secrets SecretName",
	)
	if len(initSecretsVolume.Secret.Items) != 1 ||
		initSecretsVolume.Secret.Items[0].Key != "custom-key.json" ||
		initSecretsVolume.Secret.Items[0].Path != PostgresInitSecretsFileName {
		t.Errorf(
			"postgres init-secrets Secret items = %+v, want key custom-key.json projected to %q",
			initSecretsVolume.Secret.Items,
			PostgresInitSecretsFileName,
		)
	}

	postgres := pod.Spec.InitContainers[0]
	assertEnvVarValue(t, postgres.Env, "POSTGRES_INIT_SECRETS_FILE", PostgresInitSecretsFilePath)
	assertReadOnlyVolumeMount(
		t,
		postgres.VolumeMounts,
		PostgresInitSecretsVolumeName,
		PostgresInitSecretsMountPath,
	)

	multipooler := pod.Spec.Containers[0]
	assertNotContainsEnvVar(t, multipooler.Env, "POSTGRES_INIT_SECRETS_FILE")
	assertNotContainsVolumeMount(t, multipooler.VolumeMounts, PostgresInitSecretsVolumeName)

	exporter := pod.Spec.Containers[1]
	assertNotContainsEnvVar(t, exporter.Env, "POSTGRES_INIT_SECRETS_FILE")
	assertNotContainsVolumeMount(t, exporter.VolumeMounts, PostgresInitSecretsVolumeName)
}

func TestBuildPoolPod_PostgresInitSecretsRef_Absent(t *testing.T) {
	c := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Nil(
		findVolume(pod.Spec.Volumes, PostgresInitSecretsVolumeName),
		"expected no postgres init-secrets volume, got",
	)
}

func TestComputeSpecHash_ChangesOnPostgresInitSecretsRef(t *testing.T) {
	c := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")
	wantHash := ComputeSpecHash(pod)

	shardWithRef := newTestShard()
	shardWithRef.Spec.PostgresInitSecretsRef = &multigresv1alpha1.PostgresInitSecretsRef{
		Name: "multigres-init-secrets",
	}
	podWithRef, err := BuildPoolPod(shardWithRef, "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.NotEq(
		wantHash,
		ComputeSpecHash(podWithRef),
		"spec hash should differ when postgres init-secrets ref is set vs unset",
	)
}

func TestBuildPoolPod_SecurityContext(t *testing.T) {
	c := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Nil(pod.Spec.SecurityContext, "pod security context")

	if pod.Spec.TerminationGracePeriodSeconds == nil ||
		*pod.Spec.TerminationGracePeriodSeconds != 30 {
		t.Errorf(
			"terminationGracePeriodSeconds = %v, want 30",
			pod.Spec.TerminationGracePeriodSeconds,
		)
	}
}

func TestBuildPoolPod_FSGroupDoesNotOverrideContainerRuntimeIdentity(t *testing.T) {
	t.Run("custom images get the default identity", func(t *testing.T) {
		shard := newTestShard()
		shard.Spec.Images.Postgres = "example/pgctld:custom"
		shard.Spec.Images.Multipooler = "example/multipooler:custom"
		pool := newTestPoolSpec()
		pool.FSGroup = ptr.To(int64(2000))

		pod, err := BuildPoolPod(shard, "main", "z1", pool, 0, testScheme())
		assert.NewAborting(t).NoError(err, "unexpected error")

		// Overriding the image must not drop the numeric identity. pgctld
		// declares USER postgres by name, so leaving RunAsUser unset pairs
		// RunAsNonRoot with a username the kubelet cannot resolve, and every
		// pod fails CreateContainerConfigError. An image that genuinely runs
		// as something else sets RunAsUser explicitly — see the case below.
		assertPoolPodFSGroup(t, pod, 2000)
		assertContainerIdentity(
			t,
			pod.Spec.InitContainers[0],
			ptr.To(DefaultPostgresUID),
			ptr.To(DefaultPostgresGID),
		)
		assertContainerIdentity(
			t,
			pod.Spec.Containers[0],
			ptr.To(DefaultMultipoolerUID),
			ptr.To(DefaultMultipoolerGID),
		)
		assertContainerIdentity(
			t,
			pod.Spec.Containers[1],
			ptr.To(DefaultPostgresExporterUID),
			ptr.To(DefaultPostgresExporterGID),
		)
	})

	t.Run("default pgctld identity is independent", func(t *testing.T) {
		pool := newTestPoolSpec()
		pool.FSGroup = ptr.To(int64(2000))

		pod, err := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())
		assert.NewAborting(t).NoError(err, "unexpected error")

		assertPoolPodFSGroup(t, pod, 2000)
		assertContainerIdentity(
			t,
			pod.Spec.InitContainers[0],
			ptr.To(DefaultPostgresUID),
			ptr.To(DefaultPostgresGID),
		)
		assertContainerIdentity(
			t,
			pod.Spec.Containers[0],
			ptr.To(DefaultMultipoolerUID),
			ptr.To(DefaultMultipoolerGID),
		)
		assertContainerIdentity(
			t,
			pod.Spec.Containers[1],
			ptr.To(DefaultPostgresExporterUID),
			ptr.To(DefaultPostgresExporterGID),
		)
	})

	t.Run("explicit identity is independent", func(t *testing.T) {
		shard := newTestShard()
		shard.Spec.Images.Postgres = "example/pgctld:named-user"
		shard.Spec.Images.Multipooler = "example/multipooler:named-user"
		pool := newTestPoolSpec()
		pool.FSGroup = ptr.To(int64(2000))
		pool.Postgres.RunAsUser = ptr.To(int64(1000))
		pool.Postgres.RunAsGroup = ptr.To(int64(1001))
		pool.Multipooler.RunAsUser = ptr.To(int64(1000))
		pool.Multipooler.RunAsGroup = ptr.To(int64(3001))

		pod, err := BuildPoolPod(shard, "main", "z1", pool, 0, testScheme())
		assert.NewAborting(t).NoError(err, "unexpected error")

		assertPoolPodFSGroup(t, pod, 2000)
		assertContainerIdentity(
			t,
			pod.Spec.InitContainers[0],
			ptr.To(int64(1000)),
			ptr.To(int64(1001)),
		)
		assertContainerIdentity(
			t,
			pod.Spec.Containers[0],
			ptr.To(int64(1000)),
			ptr.To(int64(3001)),
		)
		assertContainerIdentity(
			t,
			pod.Spec.Containers[1],
			ptr.To(DefaultPostgresExporterUID),
			ptr.To(DefaultPostgresExporterGID),
		)
	})

	t.Run("multipooler inherits explicit postgres UID when unset", func(t *testing.T) {
		shard := newTestShard()
		shard.Spec.Images.Postgres = "example/pgctld:named-user"
		pool := newTestPoolSpec()
		pool.FSGroup = ptr.To(int64(2000))
		pool.Postgres.RunAsUser = ptr.To(int64(1000))
		pool.Postgres.RunAsGroup = ptr.To(int64(1001))

		pod, err := BuildPoolPod(shard, "main", "z1", pool, 0, testScheme())
		assert.NewAborting(t).NoError(err, "unexpected error")

		assertPoolPodFSGroup(t, pod, 2000)
		assertContainerIdentity(
			t,
			pod.Spec.InitContainers[0],
			ptr.To(int64(1000)),
			ptr.To(int64(1001)),
		)
		assertContainerIdentity(
			t,
			pod.Spec.Containers[0],
			ptr.To(int64(1000)),
			ptr.To(DefaultMultipoolerGID),
		)
	})

	t.Run("rejects explicit multipooler UID without postgres UID", func(t *testing.T) {
		pool := newTestPoolSpec()
		pool.FSGroup = ptr.To(int64(2000))
		pool.Multipooler.RunAsUser = ptr.To(int64(1000))

		_, err := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())
		assert.NewAborting(t).ErrorContains(err, "requires matching postgres runAsUser")
	})

	t.Run("rejects mismatched explicit shared data UIDs", func(t *testing.T) {
		pool := newTestPoolSpec()
		pool.FSGroup = ptr.To(int64(2000))
		pool.Postgres.RunAsUser = ptr.To(int64(999))
		pool.Multipooler.RunAsUser = ptr.To(int64(1000))

		_, err := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())
		assert.NewAborting(t).ErrorContains(err, "must match because both access PGDATA")
	})
}

func TestBuildContainerSecurityContext(t *testing.T) {
	t.Run("image identity", func(t *testing.T) {
		c := assert.NewCollecting(t)
		sc := buildContainerSecurityContext(nil, nil)
		c.True(*sc.RunAsNonRoot)
		c.Nil(sc.RunAsUser)
		c.Nil(sc.RunAsGroup)
	})

	t.Run("explicit identity", func(t *testing.T) {
		c := assert.NewCollecting(t)
		sc := buildContainerSecurityContext(ptr.To(int64(1000)), ptr.To(int64(1001)))
		c.True(*sc.RunAsNonRoot)
		c.EqDeep(int64(1000), *sc.RunAsUser)
		c.EqDeep(int64(1001), *sc.RunAsGroup)
	})

	t.Run("non-root user with root group", func(t *testing.T) {
		c := assert.NewCollecting(t)
		sc := buildContainerSecurityContext(ptr.To(int64(1000)), ptr.To(int64(0)))
		c.True(*sc.RunAsNonRoot)
		c.EqDeep(int64(1000), *sc.RunAsUser)
		c.EqDeep(int64(0), *sc.RunAsGroup)
	})
}

func assertPoolPodFSGroup(t *testing.T, pod *corev1.Pod, want int64) {
	t.Helper()
	c := assert.NewCollecting(t)
	c.Require().NotNil(pod.Spec.SecurityContext, "pod security context is nil")
	c.Require().NotNil(pod.Spec.SecurityContext.FSGroup, "pod fsGroup is nil")
	c.EqDeep(want, *pod.Spec.SecurityContext.FSGroup)
	c.Nil(pod.Spec.SecurityContext.RunAsUser)
	c.Nil(pod.Spec.SecurityContext.RunAsGroup)
}

func assertContainerIdentity(
	t *testing.T,
	container corev1.Container,
	wantUser *int64,
	wantGroup *int64,
) {
	t.Helper()
	c := assert.NewCollecting(t)
	c.Require().
		NotNil(container.SecurityContext, "container %q security context is nil", container.Name)
	c.True(*container.SecurityContext.RunAsNonRoot)
	c.EqDeep(wantUser, container.SecurityContext.RunAsUser, container.Name)
	c.EqDeep(wantGroup, container.SecurityContext.RunAsGroup, container.Name)
}

func TestBuildPoolPod_SpecHash(t *testing.T) {
	c := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	hash, ok := pod.Annotations[metadata.AnnotationSpecHash]
	c.Require().True(ok, "spec-hash annotation missing")
	c.NotEq("", hash, "spec-hash annotation is empty")
	c.Len(hash, 8, "spec-hash length = %d, want 8 (FNV-1a 32-bit hex)", len(hash))
}

func TestComputeSpecHash_ChangesOnPostgresPasswordFileSpec(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	assert.NewAborting(t).NoError(err, "unexpected error")
	wantHash := ComputeSpecHash(pod)

	t.Run("volume", func(t *testing.T) {
		oldPod := pod.DeepCopy()
		oldPod.Spec.Volumes = removeVolume(oldPod.Spec.Volumes, PostgresPasswordVolumeName)
		assert.NewCollecting(t).
			NotEq(wantHash, ComputeSpecHash(oldPod), "spec hash should differ when postgres password volume is removed")
	})

	t.Run("pgctld env", func(t *testing.T) {
		oldPod := pod.DeepCopy()
		useLegacyPasswordEnv(&oldPod.Spec.InitContainers[0])
		assert.NewCollecting(t).
			NotEq(wantHash, ComputeSpecHash(oldPod), "spec hash should differ when pgctld password env changes")
	})

	t.Run("multipooler env", func(t *testing.T) {
		oldPod := pod.DeepCopy()
		useLegacyPasswordEnv(&oldPod.Spec.Containers[0])
		assert.NewCollecting(t).
			NotEq(wantHash, ComputeSpecHash(oldPod), "spec hash should differ when multipooler password env changes")
	})

	t.Run("volume mount", func(t *testing.T) {
		oldPod := pod.DeepCopy()
		oldPod.Spec.Containers[0].VolumeMounts = removeVolumeMount(
			oldPod.Spec.Containers[0].VolumeMounts,
			PostgresPasswordVolumeName,
		)
		assert.NewCollecting(t).
			NotEq(wantHash, ComputeSpecHash(oldPod), "spec hash should differ when postgres password volume mount is removed")
	})

	t.Run("volume mount read-only", func(t *testing.T) {
		changedPod := pod.DeepCopy()
		for i := range changedPod.Spec.Containers[0].VolumeMounts {
			if changedPod.Spec.Containers[0].VolumeMounts[i].Name == PostgresPasswordVolumeName {
				changedPod.Spec.Containers[0].VolumeMounts[i].ReadOnly = false
			}
		}
		assert.NewCollecting(t).
			NotEq(wantHash, ComputeSpecHash(changedPod), "spec hash should differ when postgres password volume mount readOnly changes")
	})
}

func TestBuildPoolPod_NoFinalizers(t *testing.T) {
	c := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Empty(pod.Finalizers, "finalizers")
}

func TestBuildPoolPod_Affinity(t *testing.T) {
	c := assert.NewAborting(t)
	pool := newTestPoolSpec()
	pool.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "disk-type",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"ssd"},
						},
					},
				}},
			},
		},
	}

	pod, err := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())
	c.NoError(err, "unexpected error")

	c.False(
		pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil,
		"affinity not set on pod",
	)
}

func TestBuildPoolPod_Tolerations(t *testing.T) {
	c := assert.NewCollecting(t)
	pool := newTestPoolSpec()
	pool.Tolerations = []corev1.Toleration{
		{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "database",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	pod, err := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Require().
		Len(pod.Spec.Tolerations, 1, "expected 1 toleration, got %d", len(pod.Spec.Tolerations))
	c.Eq("dedicated", pod.Spec.Tolerations[0].Key, "toleration key")
	c.Eq("database", pod.Spec.Tolerations[0].Value, "toleration value")
}

func TestComputeSpecHash_ChangesOnTolerations(t *testing.T) {
	pool1 := newTestPoolSpec()
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", pool1, 0, testScheme())

	pool2 := newTestPoolSpec()
	pool2.Tolerations = []corev1.Toleration{
		{
			Key:      "dedicated",
			Operator: corev1.TolerationOpEqual,
			Value:    "database",
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}
	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", pool2, 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	assert.NewCollecting(t).NotEq(hash2, hash1, "spec hash should differ when tolerations change")
}

func TestComputeSpecHash_ChangesOnFSGroup(t *testing.T) {
	pool1 := newTestPoolSpec()
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", pool1, 0, testScheme())

	pool2 := newTestPoolSpec()
	pool2.FSGroup = ptr.To(int64(1234))
	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", pool2, 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	assert.NewCollecting(t).NotEq(hash2, hash1, "spec hash should differ when fsGroup changes")
}

func TestComputeSpecHash_ChangesOnRuntimeIdentity(t *testing.T) {
	c := assert.NewCollecting(t)
	pool1 := newTestPoolSpec()
	pool1.Postgres.RunAsUser = ptr.To(int64(1000))
	pool1.Multipooler.RunAsUser = ptr.To(int64(1000))
	pod1, err := BuildPoolPod(newTestShard(), "main", "z1", pool1, 0, testScheme())
	c.Require().NoError(err)

	pool2 := newTestPoolSpec()
	pool2.Postgres.RunAsUser = ptr.To(int64(2000))
	pool2.Multipooler.RunAsUser = ptr.To(int64(2000))
	pod2, err := BuildPoolPod(newTestShard(), "main", "z1", pool2, 0, testScheme())
	c.Require().NoError(err)

	c.NotEqDeep(
		pod1.Annotations[metadata.AnnotationSpecHash],
		pod2.Annotations[metadata.AnnotationSpecHash],
	)
}

func TestBuildPoolPod_NodeSelector(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	shard.Spec.CellTopologyLabels = map[multigresv1alpha1.CellName]map[string]string{
		"z1": {"topology.kubernetes.io/zone": "us-east-1a"},
	}

	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Require().NotNil(pod.Spec.NodeSelector, "node selector is nil")
	c.Eq("us-east-1a", pod.Spec.NodeSelector["topology.kubernetes.io/zone"], "node selector zone")
}

func TestBuildPoolPod_Hostname(t *testing.T) {
	tests := map[string]struct {
		clusterName string
		namespace   string
		poolName    string
		cellName    string
		index       int
	}{
		"first replica":         {"test-cluster", "default", "main", "z1", 0},
		"another pool and cell": {"test-cluster", "tenant-a", "extra", "zone-b", 1},
		"truncated names and surge index": {
			strings.Repeat("cluster", 8), "tenant-b", "read-replicas", "us-east-1a", 10,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			shard := newTestShard()
			shard.Labels["multigres.com/cluster"] = tt.clusterName
			shard.Namespace = tt.namespace
			pool := newTestPoolSpec()
			pool.Cells = []multigresv1alpha1.CellName{multigresv1alpha1.CellName(tt.cellName)}
			shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				multigresv1alpha1.PoolName(tt.poolName): pool,
			}
			pod, err := BuildPoolPod(shard, tt.poolName, tt.cellName, pool, tt.index, testScheme())
			c.Require().NoError(err)
			svc, err := BuildPoolHeadlessService(
				shard,
				tt.poolName,
				tt.cellName,
				pool,
				testScheme(),
			)
			c.Require().NoError(err)

			c.EqDeep(pod.Name, pod.Spec.Hostname)
			c.EqDeep(svc.Name, pod.Spec.Subdomain)
			c.True(svc.Spec.PublishNotReadyAddresses)
			for key, value := range svc.Spec.Selector {
				c.EqDeep(value, pod.Labels[key], "headless service must select the pod")
			}
			hostname := fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod.Name, svc.Name, tt.namespace)
			var hostnameArgs []string
			for _, container := range pod.Spec.Containers {
				for _, arg := range container.Args {
					if strings.HasPrefix(arg, "--hostname=") {
						c.EqDeep("multipooler", container.Name)
						hostnameArgs = append(hostnameArgs, arg)
					}
				}
			}
			c.EqDeep([]string{"--hostname=" + hostname}, hostnameArgs)
			cert := &x509.Certificate{DNSNames: pgBackRestPoolDNSNames(shard)}
			c.NoError(
				cert.VerifyHostname(hostname),
				"advertised address must match backup TLS SANs",
			)

			// Existing pods without the explicit address must enter the normal drift rollout.
			legacyPod := pod.DeepCopy()
			for i := range legacyPod.Spec.Containers {
				legacyPod.Spec.Containers[i].Args = slices.DeleteFunc(
					legacyPod.Spec.Containers[i].Args,
					func(arg string) bool { return strings.HasPrefix(arg, "--hostname=") },
				)
			}
			c.EqDeep(ComputeSpecHash(pod), pod.Annotations[metadata.AnnotationSpecHash])
			c.NotEqDeep(ComputeSpecHash(legacyPod), pod.Annotations[metadata.AnnotationSpecHash])
		})
	}
}

func TestBuildPoolPod_ServiceAccountName(t *testing.T) {
	t.Run("set when S3 serviceAccountName configured", func(t *testing.T) {
		c := assert.NewCollecting(t)
		shard := newTestShard()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket:             "my-bucket",
				Region:             "us-east-1",
				ServiceAccountName: "multigres-backup",
			},
		}
		pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
		c.Require().NoError(err, "unexpected error")
		c.Eq("multigres-backup", pod.Spec.ServiceAccountName, "ServiceAccountName")
	})

	t.Run("empty when no backup config", func(t *testing.T) {
		c := assert.NewCollecting(t)
		pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
		c.Require().NoError(err, "unexpected error")
		c.Eq("", pod.Spec.ServiceAccountName, "ServiceAccountName")
	})

	t.Run("empty when S3 has no serviceAccountName", func(t *testing.T) {
		c := assert.NewCollecting(t)
		shard := newTestShard()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "my-bucket",
				Region: "us-east-1",
			},
		}
		pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
		c.Require().NoError(err, "unexpected error")
		c.Eq("", pod.Spec.ServiceAccountName, "ServiceAccountName")
	})

	t.Run("empty when backup is filesystem type", func(t *testing.T) {
		c := assert.NewCollecting(t)
		shard := newTestShard()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
		}
		pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
		c.Require().NoError(err, "unexpected error")
		c.Eq("", pod.Spec.ServiceAccountName, "ServiceAccountName")
	})
}

func TestComputeSpecHash_ChangesOnServiceAccountName(t *testing.T) {
	shard := newTestShard()
	pod1, _ := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())

	shardWithSA := newTestShard()
	shardWithSA.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket:             "my-bucket",
			Region:             "us-east-1",
			ServiceAccountName: "multigres-backup",
		},
	}
	pod2, _ := BuildPoolPod(shardWithSA, "main", "z1", newTestPoolSpec(), 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	assert.NewCollecting(t).
		NotEq(hash2, hash1, "spec hash should differ when ServiceAccountName is added")
}

func TestComputeSpecHash_Deterministic(t *testing.T) {
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	assert.NewCollecting(t).Eq(hash2, hash1, "spec hash not deterministic")
}

func TestPodNeedsUpdateWhenReadinessProtectionsChange(t *testing.T) {
	c := assert.NewAborting(t)
	shard := newTestShard()
	pool := newTestPoolSpec()
	desired, err := BuildPoolPod(shard, "main", "z1", pool, 0, testScheme())
	c.NoError(err)

	legacy := desired.DeepCopy()
	legacy.Spec.ReadinessGates = nil
	for i := range legacy.Spec.InitContainers {
		legacy.Spec.InitContainers[i].ReadinessProbe = nil
	}
	for i := range legacy.Spec.Containers {
		legacy.Spec.Containers[i].ReadinessProbe = nil
	}
	legacy.Annotations[metadata.AnnotationSpecHash] = ComputeSpecHash(legacy)

	c.True(
		podNeedsUpdate(legacy, shard, "main", "z1", pool, 0, testScheme()),
		"pod without the current readiness gates and probes must be replaced",
	)
}

func TestComputeSpecHashIncludesReadinessGatesAndProbes(t *testing.T) {
	desired, err := BuildPoolPod(
		newTestShard(),
		"main",
		"z1",
		newTestPoolSpec(),
		0,
		testScheme(),
	)
	assert.NewAborting(t).NoError(err)
	wantHash := ComputeSpecHash(desired)

	tests := map[string]func(*corev1.Pod){
		"readiness gate": func(pod *corev1.Pod) {
			pod.Spec.ReadinessGates = nil
		},
		"startup probe": func(pod *corev1.Pod) {
			pod.Spec.Containers[0].StartupProbe = nil
		},
		"liveness probe": func(pod *corev1.Pod) {
			pod.Spec.Containers[0].LivenessProbe = nil
		},
		"readiness probe": func(pod *corev1.Pod) {
			pod.Spec.Containers[0].ReadinessProbe = nil
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := desired.DeepCopy()
			mutate(changed)
			assert.NewAborting(t).
				NotEq(wantHash, ComputeSpecHash(changed), "spec hash did not change when %s changed", name)
		})
	}
}

func TestComputeSpecHash_ChangesOnDrift(t *testing.T) {
	pool := newTestPoolSpec()
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", pool, 0, testScheme())

	// Build a second pod with different affinity (changes operator-managed fields)
	pool2 := newTestPoolSpec()
	pool2.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{
						{
							Key:      "disk-type",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"ssd"},
						},
					},
				}},
			},
		},
	}
	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", pool2, 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	assert.NewCollecting(t).NotEq(hash2, hash1, "spec hash should differ when affinity changes")
}

func TestComputeSpecHash_ChangesOnValueFromDrift(t *testing.T) {
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod1.Spec.Containers[0].Env = append(pod1.Spec.Containers[0].Env, corev1.EnvVar{
		Name: "TEST_ENV",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "secret1"},
				Key:                  "key1",
			},
		},
	})

	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod2.Spec.Containers[0].Env = append(pod2.Spec.Containers[0].Env, corev1.EnvVar{
		Name: "TEST_ENV",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "secret2"},
				Key:                  "key1",
			},
		},
	})

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	assert.NewCollecting(t).
		NotEq(hash2, hash1, "spec hash should differ when ValueFrom secret name changes")
}

func TestComputeSpecHash_ChangesOnEnvFromDrift(t *testing.T) {
	pod1, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod1.Spec.Containers[0].EnvFrom = append(pod1.Spec.Containers[0].EnvFrom, corev1.EnvFromSource{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "config1"},
		},
	})

	pod2, _ := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	pod2.Spec.Containers[0].EnvFrom = append(pod2.Spec.Containers[0].EnvFrom, corev1.EnvFromSource{
		ConfigMapRef: &corev1.ConfigMapEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "config2"},
		},
	})

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)

	assert.NewCollecting(t).
		NotEq(hash2, hash1, "spec hash should differ when EnvFrom config map name changes")
}

func TestBuildPoolPodName_Truncation(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	shard.Labels["multigres.com/cluster"] = "very-long-cluster-name-for-testing"
	shard.Spec.DatabaseName = "long-database-name"
	shard.Spec.TableGroupName = "long-tablegroup"
	shard.Spec.ShardName = "long-shard"

	name := BuildPoolPodName(shard, "main-pool", "us-east-1a", 99)

	c.LessOrEqual(63, len(name), "pod name %q exceeds 63 chars (len=%d)", name, len(name))
	c.True(strings.HasSuffix(name, "-99"), "pod name %q should end with -99", name)
}

func TestBuildPoolPodName_ShortName(t *testing.T) {
	c := assert.NewCollecting(t)
	name := BuildPoolPodName(newTestShard(), "main", "z1", 0)

	c.LessOrEqual(63, len(name), "pod name %q exceeds 63 chars (len=%d)", name, len(name))
	c.True(strings.HasSuffix(name, "-0"), "pod name %q should end with -0", name)
	// Pod name should contain meaningful parts
	c.StrContains(name, "pool", "pod name")
}

func TestComputeSpecHash_ChangesOnPostgresConfigHash(t *testing.T) {
	shard1 := newTestShard()
	pod1, _ := BuildPoolPod(shard1, "main", "z1", newTestPoolSpec(), 0, testScheme())

	shard2 := newTestShard()
	shard2.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: "abc123",
	}
	pod2, _ := BuildPoolPod(shard2, "main", "z1", newTestPoolSpec(), 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)
	assert.NewCollecting(t).
		NotEq(hash2, hash1, "spec hash should differ when postgres config hash annotation is added")
}

func TestComputeSpecHash_ChangesOnDifferentPostgresConfigHash(t *testing.T) {
	shard1 := newTestShard()
	shard1.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: "hash-v1",
	}
	pod1, _ := BuildPoolPod(shard1, "main", "z1", newTestPoolSpec(), 0, testScheme())

	shard2 := newTestShard()
	shard2.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: "hash-v2",
	}
	pod2, _ := BuildPoolPod(shard2, "main", "z1", newTestPoolSpec(), 0, testScheme())

	hash1 := ComputeSpecHash(pod1)
	hash2 := ComputeSpecHash(pod2)
	assert.NewCollecting(t).
		NotEq(hash2, hash1, "spec hash should differ when postgres config hash value changes")
}

func TestBuildPoolPod_PropagatesPostgresConfigHash(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	shard.Annotations = map[string]string{
		metadata.AnnotationPostgresConfigHash: "deadbeef",
	}

	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	got := pod.Annotations[metadata.AnnotationPostgresConfigHash]
	c.Eq("deadbeef", got, "postgres config hash annotation")
}

func TestBuildPoolPod_OmitsPostgresConfigHashWhenAbsent(t *testing.T) {
	c := assert.NewCollecting(t)
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	c.Require().NoError(err, "unexpected error")

	_, ok := pod.Annotations[metadata.AnnotationPostgresConfigHash]
	c.False(ok, "postgres config hash annotation should not be present when shard has none")
}

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func removeVolume(volumes []corev1.Volume, name string) []corev1.Volume {
	filtered := make([]corev1.Volume, 0, len(volumes))
	for _, v := range volumes {
		if v.Name != name {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

func removeVolumeMount(mounts []corev1.VolumeMount, name string) []corev1.VolumeMount {
	filtered := make([]corev1.VolumeMount, 0, len(mounts))
	for _, m := range mounts {
		if m.Name != name {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

func useLegacyPasswordEnv(container *corev1.Container) {
	for i, e := range container.Env {
		if e.Name == "POSTGRES_PASSWORD_FILE" {
			container.Env[i] = oldPostgresPasswordEnvVar()
			return
		}
	}
}

func oldPostgresPasswordEnvVar() corev1.EnvVar {
	return corev1.EnvVar{
		Name: "POSTGRES_PASSWORD",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "multigres-admin-password",
				},
				Key: PostgresPasswordSecretKey,
			},
		},
	}
}
