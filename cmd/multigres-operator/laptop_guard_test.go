package main

import (
	"strings"
	"testing"
)

// TestLaptopModeStartupError pins the hard-error guards that make a laptop-mode
// run safe. main() turns any non-nil error into os.Exit(1) immediately after
// flag.Parse, before ctrl.NewManager, so these return values are the entire
// safeguard.
func TestLaptopModeStartupError(t *testing.T) {
	tests := map[string]struct {
		laptopNS      string
		leaderElect   bool
		webhooks      bool
		wantErrSubstr string
	}{
		"no laptop namespace is allowed here (preflight guards that case)": {
			leaderElect: true,
			webhooks:    true,
		},
		"laptop mode with leader election is refused": {
			laptopNS:      "dev",
			leaderElect:   true,
			wantErrSubstr: "--leader-elect=true",
		},
		"laptop mode with webhooks is refused, in-cluster or not": {
			laptopNS:      "dev",
			webhooks:      true,
			wantErrSubstr: "--webhook-enable=false",
		},
		"laptop mode without webhooks is allowed": {
			laptopNS: "dev",
			webhooks: false,
		},
		"leader election is checked before webhooks": {
			laptopNS:      "dev",
			leaderElect:   true,
			webhooks:      true,
			wantErrSubstr: "--leader-elect=true",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := laptopModeStartupError(
				tc.laptopNS,
				tc.leaderElect,
				tc.webhooks,
			)
			if tc.wantErrSubstr == "" {
				if err != nil {
					t.Fatalf("laptopModeStartupError() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf(
					"laptopModeStartupError() = nil, want an error mentioning %q",
					tc.wantErrSubstr,
				)
			}
			if got := err.Error(); !strings.Contains(got, tc.wantErrSubstr) {
				t.Fatalf(
					"laptopModeStartupError() = %q, want it to mention %q",
					got,
					tc.wantErrSubstr,
				)
			}
		})
	}
}
