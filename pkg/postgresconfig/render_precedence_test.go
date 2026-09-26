package postgresconfig

import (
	"testing"

	"github.com/multigres/testkit/assert"
)

// TestBaselineWinsOverRefForResourceDerivedKeys reproduces a precedence problem
// in Render(): the operator's own resource-derived baseline must not be
// overridable by the deprecated PostgresConfigRef.
//
// Render() appends layers in the order baseline -> ref -> inline, and PostgreSQL
// applies later assignments last-write-wins, so today ref overrides the baseline.
// That is fine when ref is a handful of direct GUC assignments, but ref is opaque
// raw text: if it contains an `include` directive, PostgreSQL transitively pulls
// in an external file the operator never sees, silently overriding the operator's
// carefully sized values (shared_buffers, effective_cache_size, ...) with
// whatever is baked into that file. Only inline spec.postgresConfig should be
// able to override the baseline; ref (deprecated) should not.
//
// This is a unit-level repro against Render()+StampAndSplit() alone — no include,
// no image, no k8s needed, because the defect lives entirely in the string
// ordering inside Render().
//
// Today this FAILS: split.ReloadSettings["effective_cache_size"] == "999MB" (ref
// wins). After swapping the order to ref -> baseline -> inline it must be "192MB"
// (baseline wins), since no inline override was given.
func TestBaselineWinsOverRefForResourceDerivedKeys(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := Defaults() // effective_cache_size baseline default is "192MB"

	// A ref value clearly different from the baseline so a precedence bug is
	// unmistakable. A direct assignment stands in for whatever an opaque ref
	// (or a file it includes) would set — Render() does not care which.
	const refContent = "effective_cache_size = '999MB'"

	rendered, err := Render(cfg, refContent, nil)
	c.Require().NoError(err, "Render")

	_, split := StampAndSplit(rendered)
	got := split.ReloadSettings["effective_cache_size"]
	c.Eq("192MB", got, "effective_cache_size")
}
