package suite

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// setTableGroups rewrites the database's TableGroup list to exactly the
// default TableGroup plus names, in one update.
func (c *C) setTableGroups(cluster *MultigresCluster, names ...string) {
	c.Helper()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		db := &got.Spec.Databases[0]
		db.TableGroups = slices.DeleteFunc(db.TableGroups, func(tg TableGroupConfig) bool {
			return !tg.Default
		})
		for _, n := range names {
			db.TableGroups = append(db.TableGroups, tableGroupConfig(n))
		}
	})
}

// tgLabels selects one logical TableGroup of cluster, and its children.
func tgLabels(cluster *MultigresCluster, tg string) client.MatchingLabels {
	return client.MatchingLabels{
		"multigres.com/cluster":    cluster.Name,
		"multigres.com/tablegroup": tg,
	}
}

// waitForTableGroupGone waits until no TableGroup or Shard of logical
// TableGroup tg remains.
func (c *C) waitForTableGroupGone(cluster *MultigresCluster, tg string, within time.Duration) {
	c.Helper()
	c.Eventually(within, "TableGroup "+tg+" and its Shard to be deleted", func() error {
		tgs := &TableGroupList{}
		if err := c.List(tgs, tgLabels(cluster, tg)); err != nil {
			return err
		}
		shards := &ShardList{}
		if err := c.List(shards, tgLabels(cluster, tg)); err != nil {
			return err
		}
		if n := len(tgs.Items) + len(shards.Items); n != 0 {
			return fmt.Errorf(
				"%d TableGroups and %d Shards remain",
				len(tgs.Items),
				len(shards.Items),
			)
		}
		return nil
	})
}

// TestTableGroupRemovalAloneStartsHandshake removes a TableGroup from a
// settled cluster and nothing else. multigrescluster's only write to the
// removed TableGroup is the PendingDeletion annotation, which changes neither
// its generation nor anything else the TableGroup controller's For predicate
// watched, and on a settled cluster no Shard event comes along to enqueue it
// instead. The annotation alone must start the TableGroup's reconcile, and so
// the whole drain, through to deletion.
func TestTableGroupRemovalAloneStartsHandshake(t *testing.T) {
	c := newCase(t)
	cluster := c.newMultiTableGroupCluster("tg-annotation", removableTG)

	c.setTableGroups(cluster)
	c.waitForTableGroupGone(cluster, removableTG, 60*time.Second)
	c.WaitForClusterHealthy(cluster)
}
