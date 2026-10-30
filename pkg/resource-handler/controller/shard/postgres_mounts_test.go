package shard

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func findPostgresInitContainer(t *testing.T, pod *corev1.Pod) corev1.Container {
	t.Helper()
	for _, c := range pod.Spec.InitContainers {
		if c.Name == "postgres" {
			return c
		}
	}
	t.Fatal("postgres container not found")
	return corev1.Container{}
}

func findMountByPath(mounts []corev1.VolumeMount, path string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].MountPath == path {
			return &mounts[i]
		}
	}
	return nil
}

func TestPostgresMountsSecretRef_NotSetAddsNothing(t *testing.T) {
	pod, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.NoError(t, err)

	for _, v := range pod.Spec.Volumes {
		assert.NotContains(t, v.Name, PostgresMountsVolumeNamePrefix)
	}
	for _, c := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		for _, m := range c.VolumeMounts {
			assert.NotContains(t, m.Name, PostgresMountsVolumeNamePrefix)
		}
	}
}

func TestPostgresMountsSecretRef_KeyMountsSingleFile(t *testing.T) {
	shard := newTestShard()
	shard.Spec.PostgresMountsSecretRef = []multigresv1alpha1.PostgresSecretMount{{
		Name:      "my-root-key",
		MountPath: "/etc/postgresql-custom/pgsodium_root.key",
		Key:       "root.key",
	}}
	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.NoError(t, err)

	vol := findVolume(pod.Spec.Volumes, "postgres-mounts-0")
	require.NotNil(t, vol)
	require.NotNil(t, vol.Secret)
	assert.Equal(t, "my-root-key", vol.Secret.SecretName)
	assert.Equal(t, []corev1.KeyToPath{{Key: "root.key", Path: "root.key"}}, vol.Secret.Items)
	require.NotNil(t, vol.Secret.DefaultMode)
	assert.Equal(t, int32(0o444), *vol.Secret.DefaultMode)

	m := findMountByPath(
		findPostgresInitContainer(t, pod).VolumeMounts,
		"/etc/postgresql-custom/pgsodium_root.key",
	)
	require.NotNil(t, m, "postgres container must have the mount")
	assert.Equal(t, "postgres-mounts-0", m.Name)
	assert.Equal(t, "root.key", m.SubPath, "a key must mount as a single file")
	assert.True(t, m.ReadOnly)
}

func TestPostgresMountsSecretRef_NoKeyMountsDirectory(t *testing.T) {
	shard := newTestShard()
	shard.Spec.PostgresMountsSecretRef = []multigresv1alpha1.PostgresSecretMount{{
		Name:      "my-certs",
		MountPath: "/etc/my-certs",
	}}
	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.NoError(t, err)

	vol := findVolume(pod.Spec.Volumes, "postgres-mounts-0")
	require.NotNil(t, vol)
	assert.Empty(t, vol.Secret.Items, "no key means the whole Secret")

	m := findMountByPath(findPostgresInitContainer(t, pod).VolumeMounts, "/etc/my-certs")
	require.NotNil(t, m)
	assert.Empty(t, m.SubPath)
	assert.True(t, m.ReadOnly)
}

func TestPostgresMountsSecretRef_OnlyPostgresContainerMounts(t *testing.T) {
	shard := newTestShard()
	shard.Spec.PostgresMountsSecretRef = []multigresv1alpha1.PostgresSecretMount{{
		Name: "s", MountPath: "/etc/s",
	}}
	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.NoError(t, err)

	for _, c := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		got := findMountByPath(c.VolumeMounts, "/etc/s")
		if c.Name == "postgres" {
			assert.NotNil(t, got)
		} else {
			assert.Nil(t, got, "container %s must not mount the Secret", c.Name)
		}
	}
}

func TestPostgresMountsSecretRef_MultipleEntries(t *testing.T) {
	shard := newTestShard()
	shard.Spec.PostgresMountsSecretRef = []multigresv1alpha1.PostgresSecretMount{
		{Name: "a", MountPath: "/etc/a", Key: "k"},
		{Name: "b", MountPath: "/etc/b"},
	}
	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.NoError(t, err)

	require.NotNil(t, findVolume(pod.Spec.Volumes, "postgres-mounts-0"))
	require.NotNil(t, findVolume(pod.Spec.Volumes, "postgres-mounts-1"))
	mounts := findPostgresInitContainer(t, pod).VolumeMounts
	require.NotNil(t, findMountByPath(mounts, "/etc/a"))
	require.NotNil(t, findMountByPath(mounts, "/etc/b"))
}

func TestPostgresMountsSecretRef_LitePodMounts(t *testing.T) {
	shard := newTestLiteShard()
	shard.Spec.PostgresMountsSecretRef = []multigresv1alpha1.PostgresSecretMount{{
		Name: "s", MountPath: "/etc/s", Key: "k",
	}}
	pod, err := BuildLitePod(shard, "main", "z1", newTestPoolSpec(), testScheme())
	require.NoError(t, err)

	require.NotNil(t, findVolume(pod.Spec.Volumes, "postgres-mounts-0"))
	require.NotNil(t, findMountByPath(findPostgresInitContainer(t, pod).VolumeMounts, "/etc/s"))
}

func TestComputeSpecHash_PostgresMountsSecretRef(t *testing.T) {
	build := func(mounts ...multigresv1alpha1.PostgresSecretMount) string {
		shard := newTestShard()
		shard.Spec.PostgresMountsSecretRef = mounts
		pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), 0, testScheme())
		require.NoError(t, err)
		return ComputeSpecHash(pod)
	}
	m := func(name string) multigresv1alpha1.PostgresSecretMount {
		return multigresv1alpha1.PostgresSecretMount{Name: name, MountPath: "/etc/s", Key: "k"}
	}

	none := build()
	assert.NotEqual(t, none, build(m("a")), "adding a mount must roll pods")
	assert.NotEqual(t, build(m("a")), build(m("b")), "a new Secret name must roll pods")
	assert.Equal(t, build(m("a")), build(m("a")))
}
