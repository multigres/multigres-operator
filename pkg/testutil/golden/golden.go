// Package golden compares a value's YAML marshalling against a checked-in
// file, for tests that would otherwise assert field-by-field against a
// literal struct.
//
// It is a leaf package on purpose: it imports only flag, os, testing, go-cmp
// and sigs.k8s.io/yaml, so any package under test can depend on it without
// risking an import cycle back to itself.
package golden

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/yaml"
)

var update = flag.Bool("update", false, "rewrite golden files")

// AssertYAML marshals got to YAML and compares it against the golden file at
// path, which is relative to the caller's package (e.g.
// "testdata/x.golden.yaml").
//
// Run the test with -update to write the file instead of comparing. The
// write logs what it changed relative to the previous content, if any, so a
// bulk -update run stays self-reporting rather than silently rewriting a
// file nobody meant to touch.
func AssertYAML(t *testing.T, got any, path string) {
	t.Helper()

	gotYAML, err := yaml.Marshal(got)
	if err != nil {
		t.Fatalf("marshal %s to YAML: %v", path, err)
	}

	if *update {
		changeLog, err := writeGolden(path, gotYAML)
		if err != nil {
			t.Fatalf("write golden file %s: %v", path, err)
		}
		if changeLog != "" {
			t.Log(changeLog)
		}
		return
	}

	diff, err := compareGolden(path, gotYAML)
	switch {
	case errors.Is(err, os.ErrNotExist):
		t.Fatalf(
			"golden file %s does not exist; create it with: go test -run %s -update",
			path, t.Name(),
		)
	case err != nil:
		t.Fatalf("read golden file: %v", err)
	case diff != "":
		t.Errorf("%s does not match golden file (-want +got):\n%s", path, diff)
	}
}

// compareGolden returns the diff (go-cmp's "-want +got" form) between the
// golden file at path and gotYAML, or an error if the file cannot be read.
// An empty, non-error diff means the two match.
func compareGolden(path string, gotYAML []byte) (string, error) {
	//nolint:gosec // path is a literal testdata path, not user input
	wantYAML, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return cmp.Diff(string(wantYAML), string(gotYAML)), nil
}

// writeGolden writes gotYAML to path and returns a human-readable summary of
// what changed relative to any previous content at path, or "" if there was
// no previous file or no difference. Separating this from AssertYAML is what
// lets a bulk -update run stay self-reporting instead of silently rewriting a
// file nobody meant to touch.
func writeGolden(path string, gotYAML []byte) (changeLog string, err error) {
	//nolint:gosec // path is a literal testdata path, not user input
	if want, err := os.ReadFile(path); err == nil {
		if diff := cmp.Diff(string(want), string(gotYAML)); diff != "" {
			changeLog = fmt.Sprintf("updating %s (-old +new):\n%s", path, diff)
		}
	}
	// Create the directory rather than requiring it. Exactly one testdata
	// directory exists in this repo today, in the package this helper moved
	// out of, so the first -update in any package the helper is adopted by
	// would otherwise fail on the remedy the missing-file error just told the
	// caller to run.
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("create golden directory: %w", err)
	}
	if err := os.WriteFile(path, gotYAML, 0o600); err != nil {
		return "", err
	}
	return changeLog, nil
}
