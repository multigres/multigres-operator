package ctrltest

import (
	"testing"
	"time"
)

// Eventually polls until cond returns nil, failing with the last error.
//
// It lives in a non-test file because consumers of this package need it: the
// convergence a scenario waits on is the consumer's, not this package's.
func Eventually(t *testing.T, timeout time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if last = cond(); last == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s: %v", timeout, what, last)
}
