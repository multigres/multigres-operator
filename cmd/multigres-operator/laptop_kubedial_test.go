//go:build kubedial

package main

import (
	"flag"
	"testing"
)

// A kubedial build is the only one allowed to expose laptop mode, so the flag
// must be live here and read straight through.
func TestNamespaceFlagPresentUnderKubedial(t *testing.T) {
	if flag.Lookup("namespace") == nil {
		t.Fatal("--namespace flag is not registered in a -tags kubedial build")
	}
	namespaceFlag = "dev"
	t.Cleanup(func() { namespaceFlag = "" })
	if got := laptopNamespace(); got != "dev" {
		t.Fatalf("laptopNamespace() = %q, want %q", got, "dev")
	}
}
