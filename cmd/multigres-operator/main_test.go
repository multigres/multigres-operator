package main

import (
	"testing"

	"github.com/multigres/testkit/assert"
)

func TestGoMemLimitFromEnv(t *testing.T) {
	tests := map[string]struct {
		raw    string
		want   int64
		wantOK bool
	}{
		"derives 90% of a 512Mi limit": {
			raw:    "536870912",
			want:   483183820,
			wantOK: true,
		},
		"unset means unlimited": {
			raw:    "",
			wantOK: false,
		},
		"non-numeric means unlimited": {
			raw:    "512Mi",
			wantOK: false,
		},
		"zero means unlimited": {
			raw:    "0",
			wantOK: false,
		},
		"negative means unlimited": {
			raw:    "-1",
			wantOK: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewAborting(t)
			got, ok := goMemLimitFromEnv(tc.raw)
			c.Eq(tc.wantOK, ok, "goMemLimitFromEnv(%q) ok = %v, want", tc.raw, ok)
			c.Eq(tc.want, got, "goMemLimitFromEnv(%q) = %d, want", tc.raw, got)
		})
	}
}

// A node's allocatable memory is what the downward API projects when the
// container declares no memory limit, so the conversion has to stay in range
// for values far larger than any limit we would set deliberately.
func TestGoMemLimitFromEnvDoesNotOverflowAtNodeScale(t *testing.T) {
	c := assert.NewAborting(t)
	const nodeAllocatable = 512 * 1024 * 1024 * 1024 // 512GiB

	got, ok := goMemLimitFromEnv("549755813888")
	c.True(ok, "goMemLimitFromEnv() rejected a node-sized limit")
	c.False(
		got <= 0 || got >= nodeAllocatable,
		"goMemLimitFromEnv() = %d, want a positive value below %d",
		got,
		nodeAllocatable,
	)
}
