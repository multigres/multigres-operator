//go:build e2e

package framework

import (
	"slices"
	"strings"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestApplyImageOverrides(t *testing.T) {
	t.Setenv(postgresImageEnv, "example.test/postgres:custom")
	t.Setenv(multigatewayImageEnv, "example.test/multigres:gateway")
	c := assert.NewAborting(t)

	cluster := &multigresv1alpha1.MultigresCluster{}
	applyImageOverrides(cluster)

	c.Eq("example.test/postgres:custom", string(cluster.Spec.Images.Postgres), "postgres image =")
	c.Eq(
		"example.test/multigres:gateway",
		string(cluster.Spec.Images.Multigateway),
		"multigateway image =",
	)
	c.Eq("", cluster.Spec.Images.Multiadmin, "unset multiadmin override unexpectedly changed to")
}

func TestRuntimeImagesUsesOverridesAndDeduplicates(t *testing.T) {
	c := assert.NewAborting(t)
	const multigresNightly = "ghcr.io/multigres/multigres:nightly-sha-abcdef0"
	t.Setenv(multiadminImageEnv, multigresNightly)
	t.Setenv(multiorchImageEnv, multigresNightly)
	t.Setenv(multipoolerImageEnv, multigresNightly)
	t.Setenv(multigatewayImageEnv, multigresNightly)

	images := runtimeImages()
	got := count(images, multigresNightly)
	c.Eq(1, got, "nightly multigres image occurs %d times in %v", got, images)
	c.NotContains(
		images,
		multigresv1alpha1.DefaultMultiadminImage,
		"default multigres image retained despite complete override",
	)
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

func TestRuntimeImagesUsesCommittedDefaults(t *testing.T) {
	for _, key := range []string{
		postgresImageEnv, multiadminImageEnv, multiadminWebImageEnv,
		multiorchImageEnv, multipoolerImageEnv, multigatewayImageEnv,
	} {
		t.Setenv(key, "")
	}
	for _, tc := range []struct {
		name     string
		postgres string
	}{
		{name: "all committed defaults"},
		{name: "partial override", postgres: "example.test/pgctld@sha256:" + strings.Repeat("a", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(postgresImageEnv, tc.postgres)
			wantPostgres := multigresv1alpha1.DefaultPostgresImage
			if tc.postgres != "" {
				wantPostgres = tc.postgres
			}
			want := []string{
				wantPostgres,
				multigresv1alpha1.DefaultMultiadminImage,
				multigresv1alpha1.DefaultMultiadminWebImage,
				multigresv1alpha1.DefaultMultiorchImage,
				multigresv1alpha1.DefaultMultipoolerImage,
				multigresv1alpha1.DefaultMultigatewayImage,
				multigresv1alpha1.DefaultEtcdImage,
				multigresv1alpha1.DefaultPostgresExporterImage,
			}
			slices.Sort(want)
			want = slices.Compact(want)
			got := runtimeImages()
			slices.Sort(got)
			assert.NewAborting(t).EqDiff(want, got, "runtimeImages()")
		})
	}
}
