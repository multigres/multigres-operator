package suite

import (
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestTwoTableGroupsBothReachQuiescence pins the fix for poolerSim electing
// one primary per namespace instead of per shard (test/suite/fakes.go). Two
// TableGroups in one cluster share a namespace, so before the fix only one
// pod across both TableGroups was ever registered as PRIMARY; every other
// pod's Shard found no primary in podRoles and requeued every 10s forever
// (reconcile_data_plane.go's "No primary in podRoles" loop), and the
// namespace never went quiescent.
//
// Written red: with the old namespace-only grouping this hangs until
// RequireQuiescent's own timeout, with the op count still climbing rather
// than leveling off. Fixed, both TableGroups' Shards each get their own
// primary and the namespace settles like any single-TableGroup cluster.
func TestTwoTableGroupsBothReachQuiescence(t *testing.T) {
	c := newCase(t)

	shardSpec := func() multigresv1alpha1.ShardConfig {
		return multigresv1alpha1.ShardConfig{Name: "0-inf"}
	}

	cluster := c.newCluster("two-tablegroups", func(spec *MultigresClusterSpec) {
		spec.Databases = []multigresv1alpha1.DatabaseConfig{{
			Name:    "postgres",
			Default: true,
			TableGroups: []multigresv1alpha1.TableGroupConfig{
				{
					Name:    "default",
					Default: true,
					Shards:  []multigresv1alpha1.ShardConfig{shardSpec()},
				},
				{
					Name:   "second",
					Shards: []multigresv1alpha1.ShardConfig{shardSpec()},
				},
			},
		}}
	})

	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(10*time.Second, 90*time.Second)

	// Both TableGroups' Shards converged to Healthy, which needs a primary
	// elected for each; ReadyShards would stay short of TotalShards
	// otherwise.
	updated := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), updated), "get cluster")
	c.True(updated.Status.Phase == multigresv1alpha1.PhaseHealthy,
		"cluster should still be Healthy after quiescence, got phase %q", updated.Status.Phase)
}
