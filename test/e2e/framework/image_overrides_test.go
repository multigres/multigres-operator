//go:build e2e

package framework

import (
	"slices"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestApplyImageOverrides(t *testing.T) {
	t.Setenv(postgresImageEnv, "example.test/postgres:custom")
	t.Setenv(multigatewayImageEnv, "example.test/multigres:gateway")

	cluster := &multigresv1alpha1.MultigresCluster{}
	applyImageOverrides(cluster)

	if got := string(cluster.Spec.Images.Postgres); got != "example.test/postgres:custom" {
		t.Fatalf("postgres image = %q", got)
	}
	if got := string(cluster.Spec.Images.Multigateway); got != "example.test/multigres:gateway" {
		t.Fatalf("multigateway image = %q", got)
	}
	if cluster.Spec.Images.Multiadmin != "" {
		t.Fatalf("unset multiadmin override unexpectedly changed to %q", cluster.Spec.Images.Multiadmin)
	}
}

func TestRuntimeImagesUsesOverridesAndDeduplicates(t *testing.T) {
	const multigresNightly = "ghcr.io/multigres/multigres:nightly-sha-abcdef0"
	t.Setenv(multiadminImageEnv, multigresNightly)
	t.Setenv(multiorchImageEnv, multigresNightly)
	t.Setenv(multipoolerImageEnv, multigresNightly)
	t.Setenv(multigatewayImageEnv, multigresNightly)

	images := runtimeImages()
	if got := count(images, multigresNightly); got != 1 {
		t.Fatalf("nightly multigres image occurs %d times in %v", got, images)
	}
	if slices.Contains(images, "ghcr.io/multigres/multigres:main") {
		t.Fatalf("default multigres image retained despite complete override: %v", images)
	}
}

func TestKindLoadableImagesSkipsDigestReferences(t *testing.T) {
	images := []string{
		"ghcr.io/multigres/multigres-operator:test",
		"ghcr.io/multigres/multigres@sha256:6193a83dc4db60c61965a0f1bfac071987569eb8775d9040c0ef5d0a867213b0",
		"gcr.io/etcd-development/etcd:v3.6.7",
	}
	want := []string{
		"ghcr.io/multigres/multigres-operator:test",
		"gcr.io/etcd-development/etcd:v3.6.7",
	}

	if got := kindLoadableImages(images); !slices.Equal(got, want) {
		t.Fatalf("kindLoadableImages() = %v, want %v", got, want)
	}
}

func count(values []string, target string) int {
	total := 0
	for _, value := range values {
		if value == target {
			total++
		}
	}
	return total
}
