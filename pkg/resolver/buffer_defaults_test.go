package resolver

import (
	"testing"

	"github.com/multigres/multigres/go/services/multigateway/buffer"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// TestBufferDefaultsMatchBinary pins the hardcoded admission constants to the
// multigres module's authoritative buffer defaults, so a dependency bump that
// changes a binary default fails here instead of silently desynchronizing
// webhook verdicts from binary startup validation.
func TestBufferDefaultsMatchBinary(t *testing.T) {
	cfg := buffer.NewConfig(viperutil.NewRegistry())
	if got := cfg.Window.Default(); got != defaultBufferWindow {
		t.Errorf("defaultBufferWindow = %s, binary default = %s", defaultBufferWindow, got)
	}
	if got := cfg.MaxFailoverDuration.Default(); got != defaultBufferMaxFailoverDuration {
		t.Errorf(
			"defaultBufferMaxFailoverDuration = %s, binary default = %s",
			defaultBufferMaxFailoverDuration, got,
		)
	}
}
