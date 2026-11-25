package suite

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// multiPoolCluster creates one shard with a one-pod pool per name, its sim
// cohort committed under AtLeastN(2), and waits for it to be healthy.
func (c *C) multiPoolCluster(name string, pools ...PoolName) *MultigresCluster {
	c.Helper()
	release := poolers.Model(c.NS, poolerModel{durability: topoclient.AtLeastN(2)})
	c.Cleanup(release)
	spec := map[PoolName]PoolSpec{}
	for _, pool := range pools {
		spec[pool] = poolSpecWithReplicas(1)
	}
	cluster := c.newCluster(name, func(s *MultigresClusterSpec) {
		s.Databases = []DatabaseConfig{{
			Name:    "postgres",
			Default: true,
			TableGroups: []TableGroupConfig{{
				Name:    "default",
				Default: true,
				Shards: []ShardConfig{{
					Name: "0-inf",
					Spec: &ShardInlineSpec{Pools: spec},
				}},
			}},
		}}
	})
	c.WaitForClusterHealthy(cluster)
	c.Len(c.sortedPoolPods(), len(pools), "pool pods")
	return cluster
}

// TestHeldRolloutAcrossPoolsKeepsItsCondition holds a rollout with a pod
// drifted in each of two pools: each pool has one pod and the cohort's
// durability policy needs both, so every drain is refused and the shard
// requeues to retry. The RollingUpdate condition must read the same on every
// pass. One that followed Go map iteration order would be rewritten on most
// passes, and each write requeues the Shard through its own watch.
//
// The Shard is still written while the rollout is held, for reasons outside
// this condition: writes that change only managedFields, and the
// BackupHealthy condition's observedGeneration, which alternates between the
// Shard's generations when a pass starts from a cached Shard older than the
// previous pass's last write. So this requires every write to leave the rest
// of the status as it was, rather than requiring no writes.
func TestHeldRolloutAcrossPoolsKeepsItsCondition(t *testing.T) {
	c := newCase(t)
	cluster := c.multiPoolCluster("held-roll", "beta", "alpha")
	before := c.sortedPoolPods()

	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitShardDrifted()

	shards := &ShardList{}
	c.NoError(c.List(shards), "list shards")
	c.Len(shards.Items, 1, "shards")
	key := client.ObjectKeyFromObject(&shards.Items[0])
	want := metav1.Condition{
		Type:    "RollingUpdate",
		Status:  metav1.ConditionTrue,
		Reason:  "PodsDrifted",
		Message: "1 pod needs update in pool alpha; 1 pod needs update in pool beta",
	}
	c.Eventually(30*time.Second, "RollingUpdate to name both pools", func() error {
		shard := &Shard{}
		if err := c.Get(key, shard); err != nil {
			return err
		}
		if got := rollingUpdate(shard); got != want {
			return fmt.Errorf("RollingUpdate %+v", got)
		}
		return nil
	})

	held := &Shard{}
	c.NoError(c.Get(key, held), "get shard")
	status := statusForComparison(c, held)
	const passes = 40
	c.WatchOverPasses(&ShardList{}, key.Name, "shard", passes,
		2*time.Minute, func(obj client.Object) {
			shard := obj.(*Shard)
			if got := rollingUpdate(shard); got != want {
				c.Errorf("resourceVersion %s: RollingUpdate %+v, want %+v",
					shard.ResourceVersion, got, want)
			}
			if got := statusForComparison(c, shard); got != status {
				c.Errorf("resourceVersion %s: status changed in a held rollout:\n%s\nwas:\n%s",
					shard.ResourceVersion, got, status)
				status = got
			}
		})
	c.requireNoneRemoved(before, "held rollout")
}

// rollingUpdate is shard's RollingUpdate condition without the fields that
// record when and for which generation it was written.
func rollingUpdate(shard *Shard) metav1.Condition {
	cond := meta.FindStatusCondition(shard.Status.Conditions, "RollingUpdate")
	if cond == nil {
		return metav1.Condition{}
	}
	return metav1.Condition{
		Type: cond.Type, Status: cond.Status, Reason: cond.Reason, Message: cond.Message,
	}
}

// statusForComparison is shard's status as JSON with the BackupHealthy
// condition's observedGeneration cleared.
func statusForComparison(c *C, shard *Shard) string {
	c.Helper()
	status := shard.Status.DeepCopy()
	for i := range status.Conditions {
		if status.Conditions[i].Type == "BackupHealthy" {
			status.Conditions[i].ObservedGeneration = 0
		}
	}
	out, err := json.Marshal(status)
	c.NoError(err, "marshal shard status")
	return string(out)
}
