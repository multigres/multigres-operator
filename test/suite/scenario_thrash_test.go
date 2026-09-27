package suite

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// TestThrash adds and removes things faster than the operator can converge,
// then asserts it lands in the right end state and stops. Both subtests
// assert only two things: what the namespace looks like once it settles, and
// that it does settle at all.
//
// Neither subtest uses a Script. A Script's steps are a closed-world
// assertion of one interleaving of events, and thrash does not produce one
// interleaving: which reconciler wins a given race, how many redundant SSA
// applies land, and how many requeues fire along the way are all legitimately
// unpredictable once changes are issued faster than the operator can react to
// them. A Script step here would be asserting an ordering nobody can
// guarantee, which is exactly the kind of flake this suite exists to stop
// writing. So the claim is only about the end state and about quiescence,
// made with ordinary reads and RequireQuiescent, not about the path taken to
// get there. Do not "improve" this into a Script.
func TestThrash(t *testing.T) {
	t.Run("pool replica thrash", testPoolReplicaThrash)
	t.Run("child CR thrash", testChildCRThrash)
}

// thrashPoolName matches resolver.DefaultPoolName, the name the operator
// gives the pool it injects when a cluster specifies none. Importing
// pkg/resolver for one string constant did not seem worth the dependency, so
// this is a literal deliberately kept next to its cross-check.
const thrashPoolName = PoolName("default")

// testPoolReplicaThrash scales one pool 1 -> 2 -> 1 -> 2 with no wait between
// changes, then requires the namespace to go quiet and every pool PVC to be
// bound to a live pod.
//
// It does not assert on this thrashed shard's status.podRoles itself:
// requirePoolScaleUpLandsRole asserts that claim deterministically on a
// namespace of its own, and asserting it here too would only be a second,
// weaker sample of the same thing.
func testPoolReplicaThrash(t *testing.T) {
	c := newCase(t)
	cluster := c.poolThrashCluster("pool-thrash", 1)
	c.WaitForClusterHealthy(cluster)

	// Fired back to back, not waited on between calls: each Patch is an
	// unconditional merge patch computed against the object's state as of the
	// previous call in this loop, so it lands regardless of what the operator
	// has or hasn't done with the prior one yet. That is the thrash.
	for _, n := range []int32{2, 1, 2} {
		c.scalePoolTo(cluster, n)
	}

	// Fixed: scaling a pool from N to N+1 lands the new pooler's role in
	// shard.Status.PodRoles even when the pooler registers in topology after
	// the shard has already reconciled to Healthy. The shard controller's
	// readiness requeue is what closes the gap, since nothing else notices a
	// registration: it is a write to the topology store, with no Kubernetes
	// event behind it.
	//
	// The pin this replaces was statistical because the defect was: whether it
	// bit depended on whether the pooler registered before or after the
	// reconcile that declared the shard converged, so it reproduced about a
	// third of the time. Fixed, it is deterministic, so a positive assertion
	// replaces what used to be a KnownDefect pin.
	requirePoolScaleUpLandsRole(t)

	c.RequireQuiescent(10*time.Second, 90*time.Second)

	// The end state, asserted on this shard rather than inferred from the
	// pin's namespace. Pod readiness is a Kubernetes-level fact that does
	// not travel through shard.Status.PodRoles, so this is immune to the defect
	// pinned above: without it a lost or reverted final scale-up settles at one
	// pod, one bound PVC and a quiet namespace, and every other assertion here
	// is satisfied by that.
	live := c.liveReadyPoolPodNames()
	c.Check().Len(live, 2, "ready pods")

	c.requireNoOrphanedPoolPVCs(live)
}

// requirePoolScaleUpLandsRole asserts that scaling a pool from one to two
// lands the new pooler's role in status.podRoles.
//
// On its own namespace and its own cluster, with no thrash, because the
// defect this replaces never needed one: a bare single scale-up reproduced
// it, and the thrash above only found it first.
//
// Nothing wakes the shard once it is Healthy: a registration is a write to
// the topology store, with no Kubernetes event behind it. The shard
// controller now requeues on a backoff while any managed pod has not reached
// posture readiness, so the role lands well inside a second here, because the
// suite compresses requeues, so the window below is generous.
func requirePoolScaleUpLandsRole(t *testing.T) {
	t.Helper()

	c := newCase(t)
	cluster := c.poolThrashCluster("scaleup", 1)
	c.WaitForClusterHealthy(cluster)
	key := c.shardKey()

	// Hold registration before scaling, so the second pooler cannot register
	// until this test says so. Without the hold the defect's precondition, a
	// shard that converged having seen fewer poolers than pods, arrives only
	// when registration loses a race against the last reconcile. Measured
	// 2026-09-19 by disabling the fix and running this six times: three runs
	// caught the regression and three did not. Constructing the state instead
	// of waiting for it takes that from roughly half to always.
	release := poolers.HoldRegistrations(c.NS)
	defer release()

	c.scalePoolTo(cluster, 2)

	// The precondition itself, waited on rather than assumed: two pool pods
	// exist and the shard has settled on a PodRoles that knows about one. A
	// release before this point would prove nothing, because the reconcile
	// that notices the new pooler might be one the scale-up was going to
	// trigger anyway.
	c.Eventually(
		60*time.Second,
		"the shard to converge having seen fewer poolers than pods",
		func() error {
			pods := &corev1.PodList{}
			if err := c.List(pods, client.MatchingLabels{
				metadata.LabelMultigresPool: string(thrashPoolName),
			}); err != nil {
				return err
			}
			if len(pods.Items) != 2 {
				return fmt.Errorf("want 2 pool pods, got %d", len(pods.Items))
			}
			shard := &Shard{}
			if err := c.Get(key, shard); err != nil {
				return err
			}
			if len(shard.Status.PodRoles) != 1 {
				return fmt.Errorf("want 1 pod role while held, got %d", len(shard.Status.PodRoles))
			}
			return nil
		},
	)
	// Deliberately not RequireQuiescent. A held namespace never goes quiet
	// once the fix is in, because the requeue this pins is firing on its
	// backoff the whole time, so quiescence holds before the fix and cannot
	// after it. A stability window is true either way: it confirms the shard
	// has settled on one role rather than being mid-pass, which is all the
	// release needs.
	stable := time.Now().Add(3 * time.Second)
	for time.Now().Before(stable) {
		shard := &Shard{}
		c.NoError(c.Get(key, shard), "read the shard while registration is held")
		c.Eq(1, len(shard.Status.PodRoles),
			"a held pooler reached status.podRoles, so the hold is not holding")
		time.Sleep(250 * time.Millisecond)
	}

	// Now the pooler appears, with no Kubernetes event to announce it: a
	// registration is a write to etcd. Only a requeue the operator asked for
	// itself can notice, which is the thing this asserts.
	release()

	c.Eventually(
		60*time.Second,
		"the scaled-up pooler's role to reach status.podRoles",
		func() error {
			members, err := MembersOf(c.Context(), c.Client(), key)
			// A read failure is the check's own setup failing, not the
			// convergence it is waiting for, so it fails the test immediately
			// rather than retrying it silently until the timeout.
			c.NoError(err, "read shard members")
			if len(members.Replicas) != 1 || len(members.Quarantined) != 0 {
				return fmt.Errorf("got %+v", members)
			}
			return nil
		},
	)
}

func (c *C) poolThrashCluster(
	name string,
	replicasPerCell int32,
) *MultigresCluster {
	c.Helper()
	return c.newCluster(name, func(s *MultigresClusterSpec) {
		s.Databases = []DatabaseConfig{{
			Name:    "postgres",
			Default: true,
			TableGroups: []TableGroupConfig{{
				Name:    "default",
				Default: true,
				Shards: []ShardConfig{{
					Name: "0-inf",
					Spec: &ShardInlineSpec{
						Pools: map[PoolName]PoolSpec{
							thrashPoolName: poolSpecWithReplicas(replicasPerCell),
						},
					},
				}},
			}},
		}}
	})
}

func poolSpecWithReplicas(n int32) PoolSpec {
	return PoolSpec{
		Type:            "readWrite",
		Cells:           []CellName{defaultSimCell},
		ReplicasPerCell: ptr.To(n),
	}
}

// scalePoolTo patches cluster's pool to n replicas per cell via a merge patch
// against cluster's own in-memory state, not a fresh read of the server. That
// makes each call in a back-to-back thrash loop independent of whatever the
// operator has done with the previous one: the patch always states the full
// desired pool spec, so it lands regardless of the server's current state.
func (c *C) scalePoolTo(cluster *MultigresCluster, n int32) {
	c.Helper()
	base := cluster.DeepCopy()
	pools := cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools
	pools[thrashPoolName] = poolSpecWithReplicas(n)
	c.NoError(
		c.Patch(cluster, client.MergeFrom(base)),
		"scale pool %s to %d replicas per cell",
		thrashPoolName,
		n,
	)
}

// liveReadyPoolPodNames lists Ready pods belonging to the thrashed pool.
//
// This deliberately does not go through MembersOf/shard.Status.PodRoles: that
// path is the one testPoolReplicaThrash's pool-scale-up assertion covers
// directly, and a pod being live is a Kubernetes-level fact independent of
// whether the operator's own role bookkeeping has caught up to it.
func (c *C) liveReadyPoolPodNames() []string {
	c.Helper()
	pods := &corev1.PodList{}
	c.NoError(
		c.List(pods, client.MatchingLabels{metadata.LabelMultigresPool: string(thrashPoolName)}),
		"list pool pods",
	)
	var names []string
	for i := range pods.Items {
		if podReady(&pods.Items[i]) {
			names = append(names, pods.Items[i].Name)
		}
	}
	return names
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// requireNoOrphanedPoolPVCs asserts every PVC belonging to the thrashed pool
// is bound to one of livePods. It resolves the binding with ShardPVCOf, reading it
// off each live pod's own volumes, rather than reconstructing a PVC name from
// the pool/cell/ordinal and asserting the two strings match: a pod bound to
// the wrong PVC would pass a name-arithmetic check and fail this one.
func (c *C) requireNoOrphanedPoolPVCs(livePods []string) {
	c.Helper()

	bound := map[string]bool{}
	for _, pod := range livePods {
		pvcName, err := ShardPVCOf(c.Context(), c.Client(), c.NS, pod)
		if err != nil {
			c.Fatalf("resolve PVC bound to live pod %s: %v", pod, err)
		}
		bound[pvcName] = true
	}

	pvcs := &corev1.PersistentVolumeClaimList{}
	c.NoError(
		c.List(pvcs, client.MatchingLabels{metadata.LabelMultigresPool: string(thrashPoolName)}),
		"list pool PVCs",
	)
	for _, pvc := range pvcs.Items {
		if !bound[pvc.Name] {
			c.Errorf(
				"PVC %s belongs to pool %s but is not bound to any live pod; live pods: %v",
				pvc.Name, thrashPoolName, livePods,
			)
		}
	}
}

// testChildCRThrash deletes a Shard out from under its TableGroup and lets
// the parent recreate it, three times in a row, then requires the shard to
// reconverge and the namespace to go quiet.
//
// This is the likeliest spot in the wave to find a live defect: the
// ReadyForDeletion protocol between the shard and tablegroup controllers is
// already the subject of two filed defects (a vacuous ReadyForDeletion, and
// whole-cluster teardown skipping the drain), and it found a third here, pinned
// below.
func testChildCRThrash(t *testing.T) {
	c := newCase(t)
	ns := c.NS
	cluster := c.MinimalCluster("cr-thrash")
	c.WaitForClusterHealthy(cluster)

	key := c.shardKey()
	shard := &Shard{}

	for i := 0; i < 3; i++ {
		c.NoError(c.Get(key, shard), "cycle %d: get shard %s", i, key.Name)
		oldUID := shard.UID
		c.NoError(c.Delete(shard), "cycle %d: delete shard %s", i, key.Name)

		// The only wait in this loop: for the parent to have recreated a
		// replacement (a new UID at the same name), which is a mechanical
		// precondition for the next delete to hit a live object rather than a
		// no-op against one already gone. It is not a wait for the replacement
		// to converge, and the loop does not wait for that before deleting
		// again: that is the thrash.
		what := fmt.Sprintf("the tablegroup to recreate Shard %s after delete #%d", key.Name, i+1)
		c.Eventually(30*time.Second, what, func() error {
			got := &Shard{}
			if err := c.Get(key, got); err != nil {
				return err
			}
			if got.UID == oldUID {
				return fmt.Errorf("shard %s not yet recreated", key.Name)
			}
			return nil
		})
	}

	c.NoError(c.Get(key, shard), "get final shard incarnation")

	c.WaitForClusterHealthy(cluster)

	c.Eventually(60*time.Second, "the shard to report one primary and one replica",
		func() error {
			members, err := MembersOf(c.Context(), c.Client(), key)
			if err != nil {
				return err
			}
			if len(members.Replicas) != 1 || len(members.Quarantined) != 0 {
				return fmt.Errorf(
					"want 1 primary + 1 replica + 0 quarantined, got %+v", members,
				)
			}
			return nil
		},
	)

	c.RequireQuiescent(10*time.Second, 90*time.Second)

	// Tearing a Shard down marks the shared backup PVC orphan rather than
	// deleting it, because resolvePodIndex finds no ordinal in its name-hash
	// suffix and the !hasIndex arm of cleanupShardPVCs short-circuits at any
	// replica count. The replacement shard then reclaims that same PVC, and
	// reconcileSharedBackupPVC must clear the label itself: server-side apply
	// does not, since the payload never mentions it and SSA only removes
	// fields it already owns. This is asserted after the shard has
	// reconverged and quiesced, not at any earlier point, because a stale
	// label mid-reconcile would be indistinguishable from one about to be
	// cleared.
	backupPVCKey := client.ObjectKey{
		Namespace: ns,
		Name:      shardcontroller.BuildSharedBackupPVCName(shard),
	}
	pvc := &corev1.PersistentVolumeClaim{}
	c.NoError(
		c.Get(backupPVCKey, pvc),
		"get shared backup PVC %s",
		backupPVCKey.Name,
	)
	since, stale := pvc.Labels[metadata.LabelOrphan]
	c.False(
		stale,
		"shared backup PVC %s was reclaimed by shard uid %s and must not still carry "+
			"%s=%s from the earlier teardown",
		backupPVCKey.Name, shard.UID, metadata.LabelOrphan, since,
	)
}
