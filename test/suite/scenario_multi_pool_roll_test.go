package suite

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// multiPoolCluster creates one shard with a one-pod pool per name, its sim
// cohort committed under AtLeastN(2), and waits for it to be healthy.
func (c *C) multiPoolCluster(name string, pools ...PoolName) *MultigresCluster {
	c.Helper()
	return c.multiPoolClusterOf(name, 1, pools...)
}

// multiPoolClusterOf is multiPoolCluster with replicas pods in each pool.
func (c *C) multiPoolClusterOf(name string, replicas int32, pools ...PoolName) *MultigresCluster {
	c.Helper()
	release := poolers.Model(c.NS, poolerModel{durability: topoclient.AtLeastN(2)})
	c.Cleanup(release)
	spec := map[PoolName]PoolSpec{}
	for _, pool := range pools {
		spec[pool] = poolSpecWithReplicas(replicas)
	}
	return c.clusterWithPools(name, spec, len(pools)*int(replicas))
}

func (c *C) clusterWithPools(
	name string, spec map[PoolName]PoolSpec, size int, cells ...CellConfig,
) *MultigresCluster {
	c.Helper()
	cluster := c.newCluster(name, func(s *MultigresClusterSpec) {
		s.Cells = append(s.Cells, cells...)
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
	c.Len(c.sortedPoolPods(), size, "pool pods")
	return cluster
}

// TestDistributedPrimaryLast rolls one pool per cell, with the first pool
// initially primary. Its replacement must not fail back when it rejoins.
func TestDistributedPrimaryLast(t *testing.T) {
	c := newCase(t)
	cluster, pods, election := c.distributedRoll()
	start := len(Suite.Ops.OpsInNamespace(c.NS))
	c.updateCluster(
		cluster,
		func(got *MultigresCluster) { got.Spec.Images.Postgres = fixedPostgresImage },
	)
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	var replacements []string
	for _, op := range Suite.Ops.OpsInNamespace(c.NS)[start:] {
		if op.Controller == "shard" && op.Kind == "Pod" && op.Verb == "create" && op.Err == nil {
			replacements = append(replacements, op.Key.Name)
		}
	}
	c.Check().
		EqDeep([]string{pods[1].Name, pods[2].Name, pods[0].Name}, replacements, "replacement order")
	c.EqDeep([]string{pods[0].Name, pods[1].Name}, election.leaders(), "primary history")
}

func (c *C) distributedRoll() (*MultigresCluster, []corev1.Pod, *stickyElection) {
	c.Helper()
	model := poolerModel{durability: topoclient.AtLeastN(2)}
	c.Cleanup(poolers.Model(c.NS, model))
	pools := map[PoolName]PoolSpec{}
	for i, cell := range []CellName{defaultSimCell, secondSimCell, thirdSimCell} {
		pool := poolSpecWithReplicas(1)
		pool.Cells = []CellName{cell}
		pools[PoolName(fmt.Sprintf("pool-%d", i+1))] = pool
	}
	cluster := c.clusterWithPools("distributed", pools, 3,
		CellConfig{Name: secondSimCell, ZoneID: "us-central1-b"},
		CellConfig{Name: thirdSimCell, ZoneID: "us-central1-c"})
	pods := c.sortedPoolPods()
	for i, pod := range pods {
		c.Eq(fmt.Sprintf("pool-%d", i+1), pod.Labels[metadata.LabelMultigresPool], "pool order")
	}
	// Enable stable elections after bootstrap, when the lowest-named pool is
	// primary, irrespective of the order Kubernetes created the initial pods.
	election := &stickyElection{}
	model.election = election
	c.Cleanup(poolers.Model(c.NS, model))
	c.Eventually(30*time.Second, "pool-1 elected", func() error {
		leaders := election.leaders()
		if len(leaders) != 1 || leaders[0] != pods[0].Name {
			return fmt.Errorf("leaders: %v", leaders)
		}
		return nil
	})
	return cluster, pods, election
}

// A resize is pending on the current-spec primary while only pool-2 needs
// a new pod template. Disconnect pool-3 until both inputs are visible so
// neither request can start before the other exists.
func TestDistributedPrimaryResize(t *testing.T) {
	c := newCase(t)
	cluster, pods, election := c.distributedRoll()
	primary, replica := &pods[0], &pods[1]
	c.disconnectFollower(primary, pods[2].Name, "")
	claim, err := ShardPVCOf(c.Context(), c.Client(), c.NS, primary.Name)
	c.NoError(err, "find primary data PVC")
	pvc := &corev1.PersistentVolumeClaim{}
	c.NoError(c.Get(client.ObjectKey{Namespace: c.NS, Name: claim}, pvc), "read data PVC")
	// Stand in for the CSI driver's status on the real operator-built PVC.
	base := pvc.DeepCopy()
	pvc.Status.Conditions = append(
		pvc.Status.Conditions,
		corev1.PersistentVolumeClaimCondition{
			Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
			Status: corev1.ConditionTrue,
		},
	)
	c.NoError(
		c.Client().Status().Patch(c.Context(), pvc, client.MergeFrom(base)),
		"report pending filesystem resize",
	)
	// At acknowledgement the old incarnation has already passed Requested,
	// the only cancellable drain state. Complete the synthetic CSI resize
	// before the controller can delete it and create its successor.
	cleared := make(chan error, 1)
	c.BeforeShardPatch(t, func(ctx context.Context, obj client.Object, _ client.Patch) {
		pod, ok := obj.(*corev1.Pod)
		if !ok || pod.UID != primary.UID ||
			pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateAcknowledged {
			return
		}
		live := &corev1.PersistentVolumeClaim{}
		err := c.Client().Get(ctx, client.ObjectKeyFromObject(pvc), live)
		if err == nil {
			before := live.DeepCopy()
			live.Status.Conditions = slices.DeleteFunc(
				live.Status.Conditions,
				func(cond corev1.PersistentVolumeClaimCondition) bool {
					return cond.Type == corev1.PersistentVolumeClaimFileSystemResizePending
				},
			)
			err = c.Client().Status().Patch(ctx, live, client.MergeFrom(before))
		}
		select {
		case cleared <- err:
		default:
		}
	})
	c.waitRolloutBlocked(primary.Name)
	start := len(Suite.Ops.OpsInNamespace(c.NS))
	c.updateCluster(cluster, func(got *MultigresCluster) {
		pools := got.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools
		pool := pools["pool-2"]
		pool.FSGroup = ptr.To(int64(1234))
		pools["pool-2"] = pool
	})
	key := c.shardKey()
	c.Eventually(30*time.Second, "new pool-2 template on shard", func() error {
		sh := &Shard{}
		if err := c.Get(key, sh); err != nil {
			return err
		}
		if ptr.Deref(sh.Spec.Pools["pool-2"].FSGroup, 0) != 1234 {
			return fmt.Errorf("old template")
		}
		return nil
	})
	poolers.SetFault(c.NS, nil)
	creates := func() []string {
		var names []string
		for _, op := range Suite.Ops.OpsInNamespace(c.NS)[start:] {
			if op.Controller == "shard" && op.Kind == "Pod" && op.Verb == "create" {
				names = append(names, op.Key.Name)
			}
		}
		return names
	}
	c.Eventually(unreadyRollWithin, "both replacement creates", func() error {
		names := creates()
		if len(names) < 2 {
			return fmt.Errorf("created %v", names)
		}
		c.EqDeep([]string{replica.Name, primary.Name}, names, "replacement order")
		return nil
	})
	// A Pod create is synchronous in the write log; the election sim may
	// not have seen that incarnation yet. Wait for its first handoff, then
	// compare the complete history rather than polling for a desired winner.
	c.Eventually(30*time.Second, "primary handoff", func() error {
		if len(election.leaders()) < 2 {
			return fmt.Errorf("sim has not elected a successor")
		}
		return nil
	})
	select {
	case err := <-cleared:
		c.NoError(err, "complete CSI resize")
	default:
		t.Fatal("old primary never acknowledged its drain")
	}
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(2*time.Second, 30*time.Second)
	c.EqDeep(
		[]string{replica.Name, primary.Name},
		creates(),
		"no further replacements after remount",
	)
	c.EqDeep([]string{primary.Name, replica.Name}, election.leaders(), "primary history")
}

// TestHeldRolloutAcrossPoolsKeepsItsCondition holds a rollout with a pod
// drifted in each of two pools: each pool has one pod and the cohort's
// durability policy needs both, so every drain is refused and the shard
// requeues to retry. The RollingUpdate condition must say why the rollout is
// blocked and read the same on every pass. One that changed from pass to
// pass, as a refusal reported for whichever pod Go map iteration reached
// first would, is a status write on most passes, and each write requeues the
// Shard through its own watch.
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
	// The primary is alpha's pod and goes last, so the refusal reported is
	// the one for beta's pod.
	c.waitRolloutBlocked("not removing pod " + before[1].Name)

	shards := &ShardList{}
	c.NoError(c.List(shards), "list shards")
	c.Len(shards.Items, 1, "shards")
	key := client.ObjectKeyFromObject(&shards.Items[0])
	held := &Shard{}
	c.NoError(c.Get(key, held), "get shard")
	want := rollingUpdate(held)
	c.Eq("RolloutBlocked", want.Reason, "RollingUpdate reason")
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
	for _, pass := range Suite.Reconciles.InNamespace(c.NS) {
		if pass.Controller == "shard" && pass.RequestedAfter > 0 &&
			pass.RequestedAfter < 5*time.Second {
			c.Errorf("shard pass %d requested a short retry: %s", pass.Seq, pass.RequestedAfter)
		}
	}
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
