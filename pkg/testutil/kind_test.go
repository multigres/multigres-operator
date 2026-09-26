//go:build e2e

package testutil

import (
	"testing"

	"github.com/multigres/testkit/assert"
)

func TestDefaultKindConfig_ClusterNameFromEnv(t *testing.T) {
	t.Setenv("KIND_CLUSTER", "my-custom-cluster")

	cfg := defaultKindConfig()
	assert.NewCollecting(t).Eq("my-custom-cluster", cfg.clusterName, "clusterName")
}

func TestDefaultKindConfig_ClusterNameFallback(t *testing.T) {
	t.Setenv("KIND_CLUSTER", "")

	cfg := defaultKindConfig()
	assert.NewCollecting(t).Eq(defaultKindCluster, cfg.clusterName, "clusterName")
}

func TestDefaultKindConfig_KubectlFromEnv(t *testing.T) {
	t.Setenv("KUBECTL", "/usr/local/bin/kubectl")

	cfg := defaultKindConfig()
	assert.NewCollecting(t).Eq("/usr/local/bin/kubectl", cfg.kubectl, "kubectl")
}

func TestDefaultKindConfig_KubectlFallback(t *testing.T) {
	t.Setenv("KUBECTL", "")

	cfg := defaultKindConfig()
	assert.NewCollecting(t).Eq(defaultKubectl, cfg.kubectl, "kubectl")
}

func TestWithKindCluster(t *testing.T) {
	cfg := defaultKindConfig()
	WithKindCluster("override-cluster")(cfg)

	assert.NewCollecting(t).Eq("override-cluster", cfg.clusterName, "clusterName")
}

func TestWithKindKubectl(t *testing.T) {
	cfg := defaultKindConfig()
	WithKindKubectl("/opt/bin/kubectl")(cfg)

	assert.NewCollecting(t).Eq("/opt/bin/kubectl", cfg.kubectl, "kubectl")
}

func TestWithKindCRDPaths(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := defaultKindConfig()
	WithKindCRDPaths("/path/to/crds", "/another/path")(cfg)

	c.Require().Len(cfg.crdPaths, 2, "len(crdPaths) = %d, want 2", len(cfg.crdPaths))
	c.Eq("/path/to/crds", cfg.crdPaths[0], "crdPaths[0]")
}

func TestWithKindCreateCluster(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := defaultKindConfig()
	c.False(cfg.createCluster, "createCluster should be false by default")
	WithKindCreateCluster()(cfg)
	c.True(cfg.createCluster, "createCluster should be true after WithKindCreateCluster")
}

func TestWithKindImages(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := defaultKindConfig()
	WithKindImages("img1:latest", "img2:v1")(cfg)

	c.Require().Len(cfg.images, 2, "len(images) = %d, want 2", len(cfg.images))
	c.Eq("img1:latest", cfg.images[0], "images[0]")
	c.Eq("img2:v1", cfg.images[1], "images[1]")
}

func TestKindClusterName(t *testing.T) {
	name := KindClusterName(WithKindCluster("test-cluster"))
	assert.NewCollecting(t).Eq("test-cluster", name, "KindClusterName")
}

func TestKindClusterName_Default(t *testing.T) {
	t.Setenv("KIND_CLUSTER", "")
	name := KindClusterName()
	assert.NewCollecting(t).Eq(defaultKindCluster, name, "KindClusterName")
}

func TestRandomSuffix(t *testing.T) {
	ck := assert.NewCollecting(t)
	s1 := randomSuffix()
	s2 := randomSuffix()

	ck.Len(s1, 8, "len(randomSuffix()) = %d, want 8", len(s1))
	ck.NotEq(s2, s1, "randomSuffix() returned same value twice")
	// Check all chars are valid
	for _, c := range s1 {
		ck.False(
			!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')),
			"randomSuffix() contains invalid char %q",
			c,
		)
	}
}
