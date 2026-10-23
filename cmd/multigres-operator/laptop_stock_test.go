//go:build !kubedial

package main

import (
	"flag"
	"testing"
)

// The whole safeguard against a release binary being put into laptop mode is
// that --namespace does not exist in a stock build: requesting it is an
// unknown-flag error. Pin that here.
func TestNamespaceFlagAbsentWithoutKubedial(t *testing.T) {
	if f := flag.Lookup("namespace"); f != nil {
		t.Fatalf("--namespace flag exists in a stock build: %v", f)
	}
	if got := laptopNamespace(); got != "" {
		t.Fatalf("laptopNamespace() = %q in a stock build, want empty", got)
	}
}
