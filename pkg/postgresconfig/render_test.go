package postgresconfig

import (
	"strings"
	"testing"

	"github.com/multigres/testkit/assert"
)

func TestRender(t *testing.T) {
	t.Run("renders the baseline from Config", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := Render(Defaults(), "", nil)
		c.Require().NoError(err, "Render() error =")
		// A few representative baseline lines must be present with default values.
		for _, want := range []string{
			"max_connections = 60",
			"shared_buffers = 64MB",
			"effective_cache_size = 192MB",
			"wal_level = logical",
			"cluster_name = 'default'",
		} {
			c.StrContains(got, want, "rendered baseline missing")
		}
	})

	t.Run("Config values flow into the template", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cfg := Defaults()
		cfg.SharedBuffers = "2GB"
		cfg.MaxConnections = 200
		got, err := Render(cfg, "", nil)
		c.Require().NoError(err, "Render() error =")
		c.StrContains(got, "shared_buffers = 2GB", "shared_buffers override missing, got:\n")
		c.StrContains(got, "max_connections = 200", "max_connections override missing, got:\n")
	})

	t.Run("ref content emitted verbatim before the baseline", func(t *testing.T) {
		c := assert.NewCollecting(t)
		ref := "shared_buffers = '8GB'\n# a comment"
		got, err := Render(Defaults(), ref, nil)
		c.Require().NoError(err, "Render() error =")
		c.StrContains(got, ref, "ref content not emitted verbatim, got:\n")
		// Ref must come BEFORE the baseline so the operator's resource-derived
		// baseline wins last-write-wins: here the baseline's shared_buffers = 64MB
		// must override the ref's 8GB. The deprecated ref may not override the
		// operator's sizing math — only inline spec.postgresConfig can.
		c.LessOrEqual(
			strings.Index(got, "shared_buffers = 64MB"),
			strings.Index(got, ref),
			"ref content should precede the baseline, got:\n%s",
			got,
		)
	})

	t.Run("inline map appended last as sorted quoted lines", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := Render(Defaults(), "ref = 'x'", map[string]string{
			"work_mem":        "16MB",
			"max_connections": "200",
		})
		c.Require().NoError(err, "Render() error =")
		wantMax := "max_connections = '200'"
		wantWork := "work_mem = '16MB'"
		c.Require().
			False(!strings.Contains(got, wantMax) || !strings.Contains(got, wantWork), "inline lines missing, got:\n%s", got)
		// Sorted keys, and the whole map block comes after the ref block.
		c.LessOrEqual(
			strings.Index(got, wantWork),
			strings.Index(got, wantMax),
			"inline keys not sorted, got:\n%s",
			got,
		)
		c.GreaterOrEqual(
			strings.Index(got, "ref = 'x'"),
			strings.Index(got, wantMax),
			"inline map should follow the ref, got:\n%s",
			got,
		)
	})

	t.Run("single quotes in inline values are escaped", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := Render(Defaults(), "", map[string]string{"log_line_prefix": "it's %m"})
		c.Require().NoError(err, "Render() error =")
		c.StrContains(got, "log_line_prefix = 'it''s %m'", "single quote not escaped, got:\n")
	})

	t.Run("empty ref and nil map render only the baseline", func(t *testing.T) {
		c := assert.NewCollecting(t)
		got, err := Render(Defaults(), "\n\n", nil)
		c.Require().NoError(err, "Render() error =")
		c.False(strings.Contains(got, "# postgresConfigRef") ||
			strings.Contains(
				got,
				"# spec.postgresConfig",
			), "unexpected override section for empty inputs, got:\n%s", got)
	})

	t.Run("deterministic across calls", func(t *testing.T) {
		c := assert.NewCollecting(t)
		in := map[string]string{"a": "1", "b": "2", "c": "3"}
		first, err := Render(Defaults(), "x = 'y'", in)
		c.Require().NoError(err, "Render() error =")
		second, err := Render(Defaults(), "x = 'y'", in)
		c.Require().NoError(err, "Render() error =")
		c.Eq(second, first, "Render is not deterministic")
	})
}

func TestDefaults(t *testing.T) {
	c := assert.NewCollecting(t)
	d := Defaults()
	c.Eq(60, d.MaxConnections, "MaxConnections")
	c.Eq("64MB", d.SharedBuffers, "SharedBuffers")
	c.Eq("default", d.ClusterName, "ClusterName")
}
