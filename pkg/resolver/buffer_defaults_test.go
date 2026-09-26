package resolver

import (
	"testing"
	"time"

	"github.com/multigres/multigres/go/services/multigateway/buffer"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/multigres/testkit/assert"
)

// TestBufferDefaultsMatchBinary pins the hardcoded admission constants — and
// the binary defaults stated in the GatewayBufferConfig doc comments and the
// samples README — to the multigres module's authoritative values, so a
// dependency bump that changes a binary default fails here instead of
// silently desynchronizing webhook verdicts (or documentation) from binary
// startup behavior.
func TestBufferDefaultsMatchBinary(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := buffer.NewConfig(viperutil.NewRegistry())
	c.Eq(defaultBufferWindow, cfg.Window.Default(), "defaultBufferWindow")
	c.Eq(
		defaultBufferMaxFailoverDuration,
		cfg.MaxFailoverDuration.Default(),
		"defaultBufferMaxFailoverDuration",
	)
	// Documented (not validated) defaults: update api/v1alpha1/cell_types.go
	// and config/samples/README.md if any of these fail.
	c.Eq(
		time.Minute,
		cfg.MinTimeBetweenFailovers.Default(),
		"documented minTimeBetweenFailovers default 1m, binary default =",
	)
	c.Eq(1000, cfg.Size.Default(), "documented size default 1000, binary default =")
	c.Eq(
		1,
		cfg.DrainConcurrency.Default(),
		"documented drainConcurrency default 1, binary default =",
	)
}
