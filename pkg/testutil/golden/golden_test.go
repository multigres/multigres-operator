package golden

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

// compareGolden and writeGolden hold all of AssertYAML's decision logic. They
// are tested directly, rather than through AssertYAML's *testing.T, because a
// mismatch or a missing file is supposed to fail the test it's called from:
// routing that through a real t.Run would mark this package's own tests
// failed by design. AssertYAML's two non-failing paths (match, update) are
// still covered end to end below.

func TestCompareGolden(t *testing.T) {
	dir := t.TempDir()

	t.Run("match", func(t *testing.T) {
		path := filepath.Join(dir, "match.golden.yaml")
		if err := os.WriteFile(path, []byte("a: b\n"), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		diff, err := compareGolden(path, []byte("a: b\n"))
		if err != nil {
			t.Fatalf("compareGolden: %v", err)
		}
		if diff != "" {
			t.Errorf("diff = %q, want empty", diff)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		path := filepath.Join(dir, "mismatch.golden.yaml")
		if err := os.WriteFile(path, []byte("a: b\n"), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		diff, err := compareGolden(path, []byte("a: c\n"))
		if err != nil {
			t.Fatalf("compareGolden: %v", err)
		}
		if diff == "" {
			t.Error("diff = \"\", want a non-empty diff for differing content")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(dir, "missing.golden.yaml")
		_, err := compareGolden(path, []byte("a: b\n"))
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("err = %v, want os.ErrNotExist", err)
		}
	})
}

func TestWriteGolden(t *testing.T) {
	dir := t.TempDir()

	t.Run("new file", func(t *testing.T) {
		path := filepath.Join(dir, "new.golden.yaml")
		changeLog, err := writeGolden(path, []byte("a: b\n"))
		if err != nil {
			t.Fatalf("writeGolden: %v", err)
		}
		if changeLog != "" {
			t.Errorf("changeLog = %q, want empty for a brand new file", changeLog)
		}
		//nolint:gosec // path is a t.TempDir() fixture path, not user input
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read back %s: %v", path, err)
		}
		if string(got) != "a: b\n" {
			t.Errorf("file content = %q, want %q", got, "a: b\n")
		}
	})

	t.Run("unchanged content", func(t *testing.T) {
		path := filepath.Join(dir, "unchanged.golden.yaml")
		if err := os.WriteFile(path, []byte("a: b\n"), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		changeLog, err := writeGolden(path, []byte("a: b\n"))
		if err != nil {
			t.Fatalf("writeGolden: %v", err)
		}
		if changeLog != "" {
			t.Errorf("changeLog = %q, want empty when content is unchanged", changeLog)
		}
	})

	t.Run("changed content", func(t *testing.T) {
		path := filepath.Join(dir, "changed.golden.yaml")
		if err := os.WriteFile(path, []byte("a: b\n"), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		changeLog, err := writeGolden(path, []byte("a: c\n"))
		if err != nil {
			t.Fatalf("writeGolden: %v", err)
		}
		if changeLog == "" {
			t.Error("changeLog = \"\", want a non-empty summary when content changes")
		}
		//nolint:gosec // path is a t.TempDir() fixture path, not user input
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read back %s: %v", path, err)
		}
		if string(got) != "a: c\n" {
			t.Errorf("file content = %q, want %q", got, "a: c\n")
		}
	})
}

func TestAssertYAML_Match(t *testing.T) {
	obj := map[string]string{"a": "b"}
	data, err := yaml.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	path := filepath.Join(t.TempDir(), "match.golden.yaml")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	AssertYAML(t, obj, path)
}

func TestAssertYAML_Update(t *testing.T) {
	path := filepath.Join(t.TempDir(), "update.golden.yaml")
	obj := map[string]string{"a": "b"}

	orig := *update
	*update = true
	t.Cleanup(func() { *update = orig })

	AssertYAML(t, obj, path)

	//nolint:gosec // path is a t.TempDir() fixture path, not user input
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("-update did not create %s: %v", path, err)
	}
	want, err := yaml.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("golden file after -update = %q, want %q", got, want)
	}
}
