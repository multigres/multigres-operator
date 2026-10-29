//go:build e2e

package framework

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Nightly qualification must exercise its candidate, even though PR runs use
// immutable image pins already present in the Minigres fixture.
func TestMinigresFixtureAcceptsNightlyImages(t *testing.T) {
	for _, key := range []string{
		postgresImageEnv, multipoolerImageEnv, multiadminImageEnv,
		multiadminWebImageEnv, multiorchImageEnv, multigatewayImageEnv,
	} {
		t.Setenv(key, "")
	}
	const fixture = "test/e2e/dedicated/minigres/testdata/cluster.yaml"
	pinned := MustLoadCluster(fixture, "test")
	require.Regexp(
		t,
		`^ghcr.io/multigres/multigres@sha256:[0-9a-f]{64}$`,
		pinned.Spec.Images.Multipooler,
	)
	require.Regexp(t, `^ghcr.io/multigres/pgctld@sha256:[0-9a-f]{64}$`, pinned.Spec.Images.Postgres)

	candidate := "ghcr.io/multigres/multigres@sha256:" + strings.Repeat("a", 64)
	postgres := "ghcr.io/multigres/pgctld@sha256:" + strings.Repeat("b", 64)
	t.Setenv(multipoolerImageEnv, candidate)
	partial := MustLoadCluster(fixture, "test")
	require.Equal(t, candidate, string(partial.Spec.Images.Multipooler))
	require.Equal(t, pinned.Spec.Images.Postgres, partial.Spec.Images.Postgres)

	t.Setenv(postgresImageEnv, postgres)
	full := MustLoadCluster(fixture, "test")
	require.Equal(t, candidate, string(full.Spec.Images.Multipooler))
	require.Equal(t, postgres, string(full.Spec.Images.Postgres))
}
