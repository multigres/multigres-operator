package resolver

import (
	"testing"
	"time"

	"github.com/multigres/multigres/go/services/multigateway/buffer"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// TestBufferDefaultsMatchBinary pins the hardcoded admission constants — and
// the binary defaults stated in the GatewayBufferConfig doc comments and the
// samples README — to the multigres module's authoritative values, so a
// dependency bump that changes a binary default fails here instead of
// silently desynchronizing webhook verdicts (or documentation) from binary
// startup behavior.
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
	// Documented (not validated) defaults: update api/v1alpha1/cell_types.go
	// and config/samples/README.md if any of these fail.
	if got := cfg.MinTimeBetweenFailovers.Default(); got != time.Minute {
		t.Errorf("documented minTimeBetweenFailovers default 1m, binary default = %s", got)
	}
	if got := cfg.Size.Default(); got != 1000 {
		t.Errorf("documented size default 1000, binary default = %d", got)
	}
	if got := cfg.DrainConcurrency.Default(); got != 1 {
		t.Errorf("documented drainConcurrency default 1, binary default = %d", got)
	}
}
