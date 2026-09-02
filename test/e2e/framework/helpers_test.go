//go:build e2e

package framework

import "testing"

func TestMinimalFixtureUsesFailureSafeBootstrapCohort(t *testing.T) {
	cluster := MustLoadCluster("config/samples/minimal.yaml", "test")
	pool := cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools["default"]
	if pool.ReplicasPerCell == nil {
		t.Fatal("synthetic default pool replicasPerCell is nil")
	}
	if got, want := *pool.ReplicasPerCell, int32(3); got != want {
		t.Fatalf("synthetic default pool replicasPerCell = %d, want %d", got, want)
	}
}
