//go:build e2e

package framework

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/multigres/testkit/assert"
)

func TestLoadDigestImages(t *testing.T) {
	image := "example.test/runtime@sha256:" + strings.Repeat("a", 64)
	for _, tt := range []struct {
		name      string
		cached    string
		pullFails string
		empty     bool
		wantError bool
		wantCalls []string
	}{
		{
			name: "pull exact digest on every node",
			wantCalls: []string{
				"exec node-a crictl inspecti " + image,
				"exec node-a crictl pull --pull-timeout=10m " + image,
				"exec node-b crictl inspecti " + image,
				"exec node-b crictl pull --pull-timeout=10m " + image,
			},
		},
		{
			name:   "reuse cached digest without registry access",
			cached: "true",
			wantCalls: []string{
				"exec node-a crictl inspecti " + image,
				"exec node-b crictl inspecti " + image,
			},
		},
		{
			name:      "stop setup on pull failure",
			pullFails: "true",
			wantError: true,
			wantCalls: []string{
				"exec node-a crictl inspecti " + image,
				"exec node-a crictl pull --pull-timeout=10m " + image,
			},
		},
		{name: "no images", empty: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			dir := t.TempDir()
			logPath := filepath.Join(dir, "commands")
			c.Require().NoError(os.WriteFile(logPath, nil, 0o600))
			// #nosec G306 -- Owner-only executable test fixture in t.TempDir.
			c.Require().NoError(os.WriteFile(filepath.Join(dir, "kind"), []byte(
				"#!/bin/sh\nprintf 'node-a\\nnode-b\\n'\n"), 0o700))
			// #nosec G306 -- Owner-only executable test fixture in t.TempDir.
			c.Require().NoError(os.WriteFile(filepath.Join(dir, "docker"), []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$COMMAND_LOG"
case "$4" in
  inspecti) [ "$CACHE_HIT" = true ] ;;
  pull)
    if [ "$PULL_FAILS" = true ]; then
      echo 'registry unavailable' >&2
      exit 1
    fi
    ;;
  *) exit 2 ;;
esac
`), 0o700))
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("COMMAND_LOG", logPath)
			t.Setenv("CACHE_HIT", tt.cached)
			t.Setenv("PULL_FAILS", tt.pullFails)
			images := []string{image}
			if tt.empty {
				images = nil
			}
			err := LoadImages(context.Background(), "test-cluster", images)
			if tt.wantError {
				c.Require().ErrorContains(err, image)
				c.ErrorContains(err, "node-a")
				c.ErrorContains(err, "registry unavailable")
			} else {
				c.Require().NoError(err)
			}
			// #nosec G304 -- Read only the command log created in t.TempDir above.
			calls, err := os.ReadFile(logPath)
			c.Require().NoError(err)
			c.EqDeep(strings.Join(tt.wantCalls, "\n"), strings.TrimSpace(string(calls)))
		})
	}
}
