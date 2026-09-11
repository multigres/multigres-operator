package main

import "testing"

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
			got, ok := goMemLimitFromEnv(tc.raw)
			if ok != tc.wantOK {
				t.Fatalf("goMemLimitFromEnv(%q) ok = %v, want %v", tc.raw, ok, tc.wantOK)
			}
			if got != tc.want {
				t.Fatalf("goMemLimitFromEnv(%q) = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

// A node's allocatable memory is what the downward API projects when the
// container declares no memory limit, so the conversion has to stay in range
// for values far larger than any limit we would set deliberately.
func TestGoMemLimitFromEnvDoesNotOverflowAtNodeScale(t *testing.T) {
	const nodeAllocatable = 512 * 1024 * 1024 * 1024 // 512GiB

	got, ok := goMemLimitFromEnv("549755813888")
	if !ok {
		t.Fatal("goMemLimitFromEnv() rejected a node-sized limit")
	}
	if got <= 0 || got >= nodeAllocatable {
		t.Fatalf("goMemLimitFromEnv() = %d, want a positive value below %d", got, nodeAllocatable)
	}
}
