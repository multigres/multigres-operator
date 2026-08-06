//go:build e2e

package postgresconfig_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/multigres/multigres-operator/test/e2e/framework"
)

var cluster *framework.Cluster

func TestMain(m *testing.M) {
	var err error
	cluster, err = framework.EnsureSharedCluster()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e setup: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
