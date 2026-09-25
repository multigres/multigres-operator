package suite

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"

	"go.uber.org/goleak"
)

// TestMain boots the suite once, runs the package, tears it down, and only then
// checks for leaked goroutines.
//
// Deliberately not goleak.VerifyTestMain: that calls m.Run() and checks
// immediately afterwards, with no hook in between. Since envtest and the
// manager live for the whole package rather than for one test, the check would
// run while both were still up and report the entire manager as leaked.
//
// The ignore list is empty on purpose. A clean start/stop leaks nothing
// measurable (verified across 16 cycles before this suite existed), so every
// future entry should be justified against a real stack trace rather than
// pre-loaded against suspects. A pre-loaded ignore is a permanent hole.
func TestMain(m *testing.M) {
	s, teardown, err := Boot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "suite boot failed: %v\n", err)
		os.Exit(1)
	}
	Suite = s

	code := m.Run()

	if err := teardown(); err != nil {
		fmt.Fprintf(os.Stderr, "suite teardown failed: %v\n", err)
		if code == 0 {
			code = 1
		}
	}

	// Only leak-check a passing run: a failed test may have left its own
	// goroutines behind, and reporting those on top of a real failure buries
	// the real failure.
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintf(os.Stderr, "goroutine leak after suite teardown: %v\n", err)
			dumpGoroutineLeakProfile()
			code = 1
		}
	}
	os.Exit(code)
}

// dumpGoroutineLeakProfile writes the runtime's own leak profile when the
// toolchain has one. It returns nil before Go 1.27, so this is a no-op today
// and becomes a diagnostic on the next toolchain bump, with no build tag.
//
// Count() does not run the leak detector but WriteTo() does, so the profile has
// to be driven through WriteTo and the count read afterwards, never before.
func dumpGoroutineLeakProfile() {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		return
	}
	_ = p.WriteTo(os.Stderr, 1)
}
