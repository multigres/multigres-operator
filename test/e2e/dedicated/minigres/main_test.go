//go:build e2e

package minigres_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/multigres/multigres-operator/test/e2e/framework"
)

var cluster *framework.Cluster

func TestMain(m *testing.M) {
	var err error
	cluster, err = framework.NewDedicatedCluster("e2e-minigres")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e setup: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	if !framework.ShouldKeepCluster(code != 0) {
		cluster.Destroy()
	}
	os.Exit(code)
}
