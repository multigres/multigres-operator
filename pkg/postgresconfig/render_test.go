package postgresconfig

import (
	"strings"
	"testing"
)

func TestRender(t *testing.T) {
	t.Run("renders the baseline from Config", func(t *testing.T) {
		got, err := Render(Defaults(), "", nil)
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}
		// A few representative baseline lines must be present with default values.
		for _, want := range []string{
			"max_connections = 60",
			"shared_buffers = 64MB",
			"effective_cache_size = 192MB",
			"wal_level = logical",
			"cluster_name = 'default'",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("rendered baseline missing %q, got:\n%s", want, got)
			}
		}
	})

	t.Run("Config values flow into the template", func(t *testing.T) {
		cfg := Defaults()
		cfg.SharedBuffers = "2GB"
		cfg.MaxConnections = 200
		got, err := Render(cfg, "", nil)
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}
		if !strings.Contains(got, "shared_buffers = 2GB") {
			t.Errorf("shared_buffers override missing, got:\n%s", got)
		}
		if !strings.Contains(got, "max_connections = 200") {
			t.Errorf("max_connections override missing, got:\n%s", got)
		}
	})

	t.Run("ref content appended verbatim after the baseline", func(t *testing.T) {
		ref := "shared_buffers = '8GB'\n# a comment"
		got, err := Render(Defaults(), ref, nil)
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}
		if !strings.Contains(got, ref) {
			t.Errorf("ref content not emitted verbatim, got:\n%s", got)
		}
		// Ref must come after the baseline so it wins last-write-wins.
		if strings.Index(got, ref) < strings.Index(got, "shared_buffers = 64MB") {
			t.Errorf("ref content should follow the baseline, got:\n%s", got)
		}
	})

	t.Run("inline map appended last as sorted quoted lines", func(t *testing.T) {
		got, err := Render(Defaults(), "ref = 'x'", map[string]string{
			"work_mem":        "16MB",
			"max_connections": "200",
		})
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}
		wantMax := "max_connections = '200'"
		wantWork := "work_mem = '16MB'"
		if !strings.Contains(got, wantMax) || !strings.Contains(got, wantWork) {
			t.Fatalf("inline lines missing, got:\n%s", got)
		}
		// Sorted keys, and the whole map block comes after the ref block.
		if strings.Index(got, wantMax) > strings.Index(got, wantWork) {
			t.Errorf("inline keys not sorted, got:\n%s", got)
		}
		if strings.Index(got, wantMax) < strings.Index(got, "ref = 'x'") {
			t.Errorf("inline map should follow the ref, got:\n%s", got)
		}
	})

	t.Run("single quotes in inline values are escaped", func(t *testing.T) {
		got, err := Render(Defaults(), "", map[string]string{"log_line_prefix": "it's %m"})
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}
		if !strings.Contains(got, "log_line_prefix = 'it''s %m'") {
			t.Errorf("single quote not escaped, got:\n%s", got)
		}
	})

	t.Run("empty ref and nil map render only the baseline", func(t *testing.T) {
		got, err := Render(Defaults(), "\n\n", nil)
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}
		if strings.Contains(got, "# postgresConfigRef") ||
			strings.Contains(got, "# spec.postgresConfig") {
			t.Errorf("unexpected override section for empty inputs, got:\n%s", got)
		}
	})

	t.Run("deterministic across calls", func(t *testing.T) {
		in := map[string]string{"a": "1", "b": "2", "c": "3"}
		first, err := Render(Defaults(), "x = 'y'", in)
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}
		second, err := Render(Defaults(), "x = 'y'", in)
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}
		if first != second {
			t.Error("Render is not deterministic")
		}
	})
}

func TestDefaults(t *testing.T) {
	d := Defaults()
	if d.MaxConnections != 60 {
		t.Errorf("MaxConnections = %d, want 60", d.MaxConnections)
	}
	if d.SharedBuffers != "64MB" {
		t.Errorf("SharedBuffers = %q, want 64MB", d.SharedBuffers)
	}
	if d.ClusterName != "default" {
		t.Errorf("ClusterName = %q, want default", d.ClusterName)
	}
}
