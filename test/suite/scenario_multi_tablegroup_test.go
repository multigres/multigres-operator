package suite

import (
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// removableTG is the TableGroup these scenarios remove and re-add. "default"
// stays throughout, since a database needs its default TableGroup.
const removableTG = "second"

// tableGroupConfig is one TableGroup entry of the fixture's single database.
func tableGroupConfig(name string) TableGroupConfig {
	return TableGroupConfig{
		Name:   multigresv1alpha1.TableGroupName(name),
		Shards: []ShardConfig{{Name: "0-inf"}},
	}
}

// newMultiTableGroupCluster creates a cluster whose one database has the
// default TableGroup plus each of extra, waits for it to be Healthy and for
// the namespace to settle, so what a test does next is an edit to an idle
// cluster rather than a race with its own creation.
//
// Spec.Databases is written in full because MinimalCluster leaves it empty and
// lets multigrescluster default it in memory (the suite runs no admission
// webhook to persist that default), so there is no stored entry to append to.
func (c *C) newMultiTableGroupCluster(name string, extra ...string) *MultigresCluster {
	c.Helper()
	cluster := c.newCluster(name, func(spec *MultigresClusterSpec) {
		def := tableGroupConfig("default")
		def.Default = true
		tgs := []TableGroupConfig{def}
		for _, n := range extra {
			tgs = append(tgs, tableGroupConfig(n))
		}
		spec.Databases = []DatabaseConfig{{Name: "postgres", Default: true, TableGroups: tgs}}
	})
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(10*time.Second, 90*time.Second)
	return cluster
}

// TestTwoTableGroupsBothReachQuiescence pins poolerSim electing one primary
// per shard (test/suite/fakes.go). Two TableGroups in one cluster share a
// namespace and a topology, so electing per topology alone registers one
// PRIMARY across both; the other TableGroup's Shard finds no primary in
// podRoles, requeues every 10s forever, and the namespace never settles.
// newMultiTableGroupCluster requires quiescence, so it fails on that loop.
func TestTwoTableGroupsBothReachQuiescence(t *testing.T) {
	c := newCase(t)
	cluster := c.newMultiTableGroupCluster("two-tablegroups", removableTG)

	updated := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), updated), "get cluster")
	c.True(updated.Status.Phase == multigresv1alpha1.PhaseHealthy,
		"cluster should still be Healthy after quiescence, got phase %q", updated.Status.Phase)
}
