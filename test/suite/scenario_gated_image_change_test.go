package suite

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// TestGatedNamespaceHoldsAnImageChange pins the staging step of a coordinated
// image migration (docs/backup-restore.md): with the reconcile gate on a
// namespace, the API server stores an edit to the cluster's images and no
// controller acts on it, so the image set can be written, checked and taken
// back before any pod is replaced. Lifting the gate is what starts the roll.
func TestGatedNamespaceHoldsAnImageChange(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("gated-images")
	c.RequireQuiescent(5*time.Second, 60*time.Second)
	original := cluster.Spec.Images.Postgres

	podUIDs := func() map[string]types.UID {
		c.Helper()
		uids := map[string]types.UID{}
		for _, pod := range c.sortedPoolPods() {
			uids[pod.Name] = pod.UID
		}
		return uids
	}
	tableGroupVersions := func() map[string]string {
		c.Helper()
		list := &TableGroupList{}
		c.NoError(c.List(list), "list tablegroups")
		versions := map[string]string{}
		for _, tg := range list.Items {
			versions[tg.Name] = tg.ResourceVersion
		}
		return versions
	}
	before := podUIDs()
	tableGroups := tableGroupVersions()
	c.NotEmpty(tableGroups, "tablegroups")

	c.setNamespaceGate(true)
	c.RequireQuiescent(2*time.Second, 30*time.Second)
	opsBefore := len(Suite.Ops.OpsInNamespace(c.NS))

	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.RequireQuiescent(5*time.Second, 30*time.Second)
	c.Empty(Suite.Ops.OpsInNamespace(c.NS)[opsBefore:],
		"a controller acted on a gated namespace")
	c.EqDiff(tableGroups, tableGroupVersions(), "TableGroups while gated")
	c.EqDiff(before, podUIDs(), "pool pods while gated")

	// Taking the edit back while gated leaves nothing to do once the gate is
	// lifted: no pod was ever built for the image in between.
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = original
	})
	c.setNamespaceGate(false)
	c.RequireQuiescent(5*time.Second, 60*time.Second)
	c.EqDiff(before, podUIDs(), "pool pods after an edit that was taken back while gated")
	c.WaitForClusterHealthy(cluster)

	// The same edit left in place rolls once the gate is lifted.
	c.setNamespaceGate(true)
	c.RequireQuiescent(2*time.Second, 30*time.Second)
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.RequireQuiescent(5*time.Second, 30*time.Second)
	c.EqDiff(before, podUIDs(), "pool pods while gated")
	c.setNamespaceGate(false)
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
}
