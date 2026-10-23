package suite

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/testkit/ctrltest"
)

// TestGlobalTopoMove moves a live, healthy cluster from its managed global
// topology onto an external one and back, and requires every pool pod to end
// up on the new topology with the cluster Healthy each time.
//
// A pool pod still started against the old topology registers only there, so
// the topology the shard now reads never observes it and its readiness gate
// stays False. Every gate on the ordinary rolling update waits for Ready
// poolers, so before the shard controller learned to replace such stranded pods
// regardless, this deadlocked: the shard could not become healthy until its
// pods rolled, and would not roll them until it was healthy.
//
// The managed TopoServer must outlive the move: while pooler registrations
// are held, no replacement pod can be observed on the external topology, the
// cluster cannot become Healthy, and the TopoServer and its etcd volumes have
// to survive that. Released, the move finishes and both are retired; the
// fixture's WhenDeleted=Delete takes the volumes with it. The fake's in-memory
// copy of the managed topology is forgotten only once those volumes are
// deleting, which is what makes the return leg a real move to an empty
// topology rather than a return to one still holding every pooler the shard
// ever registered there.
func TestGlobalTopoMove(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("topo-move")
	c.WaitForClusterHealthy(cluster)

	managed := c.shardTopo()
	c.Logf("managed global topology: %s %s", managed.address, managed.root)

	external := c.externalTopoFor()
	release := poolers.HoldRegistrations(c.NS)
	defer release()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.GlobalTopoServer = externalTopoSpec(external)
	})
	c.Eventually(30*time.Second, "a pool pod to be started on the external topology", func() error {
		pods, err := c.poolPods()
		if err != nil {
			return err
		}
		for i := range pods {
			if got, _ := podTopo(&pods[i]); got == external {
				return nil
			}
		}
		return errors.New("no pool pod on the external topology yet")
	})
	c.requireManagedTopoKept(5 * time.Second)
	release()

	c.waitForPoolPodsOn(external)
	c.WaitForClusterHealthy(cluster)
	c.Eventually(30*time.Second, "managed TopoServer retired with its volumes", func() error {
		list := &TopoServerList{}
		if err := c.List(list); err != nil {
			return err
		}
		if n := len(list.Items); n != 0 {
			return fmt.Errorf("%d TopoServers remain", n)
		}
		pvcs, err := c.globalTopoPVCs()
		if err != nil {
			return err
		}
		for _, pvc := range pvcs {
			if pvc.DeletionTimestamp.IsZero() {
				return fmt.Errorf("PVC %s is not being deleted", pvc.Name)
			}
		}
		return nil
	})
	c.NoError(forgetTopology(managed), "forget the managed topology")

	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.GlobalTopoServer = nil
	})
	c.Eventually(30*time.Second, "shard spec back on the managed topology", func() error {
		if got := c.shardTopo(); got != managed {
			return fmt.Errorf("shard is on %s %s", got.address, got.root)
		}
		return nil
	})
	c.waitForPoolPodsOn(managed)
	c.WaitForClusterHealthy(cluster)
}

// TestGlobalTopoMove_UnhealthyOldTopoServer moves a cluster whose managed
// global TopoServer is Degraded, its etcd crashlooping, and requires the move
// to finish and the old TopoServer to be retired anyway.
//
// The old TopoServer is what is being retired, so its health must not be a
// condition of retiring it: once every workload runs on the external
// topology, the managed one is irrelevant to the cluster, however broken. If
// it still counted toward the cluster's phase, which retirement waits on, it
// would hold back its own cleanup forever.
func TestGlobalTopoMove_UnhealthyOldTopoServer(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("topo-sick")
	c.WaitForClusterHealthy(cluster)

	etcdPod := cluster.Name + "-global-topo-0"
	c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
		if pod.Name != etcdPod {
			return nil
		}
		names := make([]string, 0, len(pod.Spec.Containers))
		for _, ctr := range pod.Spec.Containers {
			names = append(names, ctr.Name)
		}
		return &ctrltest.PodFault{CrashLoop: names}
	})
	c.Eventually(30*time.Second, "the managed TopoServer to be Degraded", func() error {
		list := &TopoServerList{}
		if err := c.List(list); err != nil {
			return err
		}
		if len(list.Items) != 1 || list.Items[0].Status.Phase != multigresv1alpha1.PhaseDegraded {
			return fmt.Errorf("TopoServers: %d, want one Degraded", len(list.Items))
		}
		return nil
	})

	external := c.externalTopoFor()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.GlobalTopoServer = externalTopoSpec(external)
	})
	c.waitForPoolPodsOn(external)
	c.waitForManagedTopoRetired()
}

// TestGlobalTopoMove_DeadOldEtcd moves a cluster whose managed etcd has no
// serving member at all, every pod crashlooping, and requires it to be retired
// anyway: the external server's own membership shows the managed etcd is not
// behind it, which needs nothing from the dead etcd. Waiting until the old etcd
// answers would keep a cluster that moved off a broken etcd stuck on it.
func TestGlobalTopoMove_DeadOldEtcd(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("topo-dead")
	c.WaitForClusterHealthy(cluster)

	etcdPrefix := cluster.Name + "-global-topo-"
	c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
		if !strings.HasPrefix(pod.Name, etcdPrefix) {
			return nil
		}
		names := make([]string, 0, len(pod.Spec.Containers))
		for _, ctr := range pod.Spec.Containers {
			names = append(names, ctr.Name)
		}
		return &ctrltest.PodFault{CrashLoop: names}
	})

	external := c.externalTopoFor()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.GlobalTopoServer = externalTopoSpec(external)
	})
	c.waitForPoolPodsOn(external)
	c.waitForManagedTopoRetired()
}

// TestGlobalTopoMove_LeaderOutsideCohort moves a cluster whose new leaders
// each start outside their cohort and are admitted later, and requires the
// move to finish and the managed TopoServer to be retired.
//
// The stranded drains of a move do not wait for each replacement to be
// admitted, so multiorch can fail over to one that is still outside the
// cohort. That leader is not data-plane ready until multiorch admits it, and
// retirement waits for a Healthy cluster, so the move converges only through
// that admission.
func TestGlobalTopoMove_LeaderOutsideCohort(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("topo-leader")
	c.WaitForClusterHealthy(cluster)

	release, seen := poolers.AdmitLeadersLate(c.NS)
	defer release()
	external := c.externalTopoFor()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.GlobalTopoServer = externalTopoSpec(external)
	})
	c.waitForPoolPodsOn(external)
	c.waitForManagedTopoRetired()
	c.WaitForClusterHealthy(cluster)
	if !seen() {
		t.Fatal(
			"no leader was ever committed outside its cohort, so the move never passed through that state",
		)
	}
}

// TestGlobalTopoMove_RetainSetWithTheMove changes the global topology to
// external and sets its pvcDeletionPolicy.whenDeleted to Retain in the same
// update, over a cluster-wide Delete, and requires the etcd PVCs to survive
// the managed TopoServer's retirement.
//
// The managed TopoServer was created under the cluster's Delete and nothing
// updates it once the spec is external, so a decision read from it would
// delete them. The policy has to come from the spec as it stands.
func TestGlobalTopoMove_RetainSetWithTheMove(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("topo-retain")
	c.WaitForClusterHealthy(cluster)

	external := c.externalTopoFor()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		spec := externalTopoSpec(external)
		spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
			WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
		}
		got.Spec.GlobalTopoServer = spec
	})
	c.waitForPoolPodsOn(external)
	c.waitForManagedTopoRetired()

	// Retirement deletes the volumes before the TopoServer, so a wrong
	// decision has already landed by now; the wait only rules out a lagging
	// cache on the reading side.
	deadline := time.Now().Add(3 * time.Second)
	for {
		pvcs, err := c.globalTopoPVCs()
		c.NoError(err, "list global topo PVCs")
		if len(pvcs) != 3 {
			c.Fatalf("want the 3 retained global topo PVCs, found %d", len(pvcs))
		}
		for _, pvc := range pvcs {
			if !pvc.DeletionTimestamp.IsZero() {
				c.Fatalf("PVC %s deleted although the global topology's policy is Retain", pvc.Name)
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// externalTopoFor is the external global topology a test in c's namespace
// moves its cluster onto.
func (c *C) externalTopoFor() topoKey {
	return topoKey{
		address: fmt.Sprintf("https://shared-topo.%s.svc:2379", c.NS),
		root:    fmt.Sprintf("/multigres/%s/global", c.NS),
	}
}

func externalTopoSpec(k topoKey) *multigresv1alpha1.GlobalTopoServerSpec {
	return &multigresv1alpha1.GlobalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{
				multigresv1alpha1.EndpointUrl(k.address),
			},
			Implementation: "etcd",
			RootPath:       k.root,
		},
	}
}

// waitForManagedTopoRetired blocks until the managed global TopoServer is gone.
func (c *C) waitForManagedTopoRetired() {
	c.Helper()
	c.Eventually(60*time.Second, "managed TopoServer retired", func() error {
		list := &TopoServerList{}
		if err := c.List(list); err != nil {
			return err
		}
		if n := len(list.Items); n != 0 {
			return fmt.Errorf("%d TopoServers remain (phase %q)", n, list.Items[0].Status.Phase)
		}
		return nil
	})
}

// requireManagedTopoKept fails unless the managed global TopoServer and every
// one of its etcd PVCs stay present and not deleting for all of d.
func (c *C) requireManagedTopoKept(d time.Duration) {
	c.Helper()
	deadline := time.Now().Add(d)
	for {
		list := &TopoServerList{}
		c.NoError(c.List(list), "list TopoServers")
		if len(list.Items) != 1 || !list.Items[0].DeletionTimestamp.IsZero() {
			c.Fatalf(
				"managed TopoServer retired before the move finished: %d present",
				len(list.Items),
			)
		}
		pvcs, err := c.globalTopoPVCs()
		c.NoError(err, "list global topo PVCs")
		if len(pvcs) == 0 {
			c.Fatalf("no global topo PVCs found; the fixture should have some")
		}
		for _, pvc := range pvcs {
			if !pvc.DeletionTimestamp.IsZero() {
				c.Fatalf("PVC %s deleted before the move finished", pvc.Name)
			}
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// globalTopoPVCs lists the etcd data PVCs of the namespace's managed global
// TopoServer, by the name its StatefulSet's claim template gives them.
func (c *C) globalTopoPVCs() ([]corev1.PersistentVolumeClaim, error) {
	list := &corev1.PersistentVolumeClaimList{}
	if err := c.List(list); err != nil {
		return nil, err
	}
	var out []corev1.PersistentVolumeClaim
	for _, pvc := range list.Items {
		if strings.HasPrefix(pvc.Name, "data-") && strings.Contains(pvc.Name, "-global-topo-") {
			out = append(out, pvc)
		}
	}
	return out, nil
}

// shardTopo reads the global topology the namespace's only Shard is on.
func (c *C) shardTopo() topoKey {
	c.Helper()
	shards := &ShardList{}
	c.NoError(c.List(shards), "list Shards")
	if len(shards.Items) != 1 {
		c.Fatalf("want exactly one Shard in %s, found %d", c.NS, len(shards.Items))
	}
	ref := shards.Items[0].Spec.GlobalTopoServer
	return topoKey{address: ref.Address, root: ref.RootPath}
}

// waitForPoolPodsOn blocks until every pool pod has been started against k and
// is Ready. Ready includes the pooler readiness gate, which the shard
// controller sets only once the topology it reads has observed that pooler.
//
// 90 seconds rather than WaitForClusterHealthy's 30, because the pods roll one
// at a time and each goes through a drain, a delete and a recreate first.
func (c *C) waitForPoolPodsOn(k topoKey) {
	c.Helper()
	c.Eventually(90*time.Second, "every pool pod on "+k.address+" and ready", func() error {
		pods, err := c.poolPods()
		if err != nil {
			return err
		}
		if len(pods) == 0 {
			return errors.New("no pool pods")
		}
		for i := range pods {
			pod := &pods[i]
			if !pod.DeletionTimestamp.IsZero() {
				return fmt.Errorf("pod %s is terminating", pod.Name)
			}
			if got, _ := podTopo(pod); got != k {
				return fmt.Errorf("pod %s is on %s %s", pod.Name, got.address, got.root)
			}
			if !podReady(pod) {
				return fmt.Errorf("pod %s not Ready on %s: %+v",
					pod.Name, k.address, podCondition(pod, corev1.PodReady))
			}
		}
		return nil
	})
}

func podCondition(pod *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == t {
			return &pod.Status.Conditions[i]
		}
	}
	return nil
}
