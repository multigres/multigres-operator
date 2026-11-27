package suite

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// disconnectFollower makes the sim's primary stop reporting the named pod as
// a connected follower while that pod runs image, and waits until the
// primary's Status says so. The pod stays Ready and serving, so removing any
// other pod is refused (the disconnected one would be among the survivors)
// while removing the disconnected pod itself is allowed.
func (c *C) disconnectFollower(primary *corev1.Pod, name, image string) {
	c.Helper()
	poolers.SetFault(c.NS, func(pod *corev1.Pod) *poolerFault {
		if pod.Name == name && (image == "" || containerImage(pod, "postgres") == image) {
			return &poolerFault{disconnected: true}
		}
		return nil
	})
	id := &cm.ID{Cell: primary.Labels[metadata.LabelMultigresCell], Name: primary.Name}
	c.Eventually(30*time.Second, "the primary to report "+name+" disconnected", func() error {
		status, err := rpc.Status(c.Context(), &cm.Multipooler{Id: id}, &md.StatusRequest{})
		if err != nil {
			return err
		}
		for _, follower := range status.GetStatus().GetPrimaryStatus().GetConnectedFollowers() {
			if follower.GetName() == name {
				return fmt.Errorf("%s is still reported connected", name)
			}
		}
		return nil
	})
}

// watchRemovals samples the named pool pods until the returned function is
// called, which returns them in the order their removal was first seen:
// draining, terminating, gone, or replaced. It fails the test if two of them
// are draining or terminating at once.
func (c *C) watchRemovals(watched []corev1.Pod) func() []string {
	c.Helper()
	uids := map[string]string{}
	for _, pod := range watched {
		uids[pod.Name] = string(pod.UID)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var order []string
	sample := func() bool {
		pods, err := c.poolPods()
		if err != nil {
			return true
		}
		var removing []string
		live := map[string]bool{}
		for i := range pods {
			pod := &pods[i]
			if uids[pod.Name] != string(pod.UID) {
				continue
			}
			live[pod.Name] = true
			if pod.Annotations[metadata.AnnotationDrainState] != "" ||
				!pod.DeletionTimestamp.IsZero() {
				removing = append(removing, pod.Name)
			}
		}
		for _, pod := range watched {
			removed := !live[pod.Name] || slices.Contains(removing, pod.Name)
			if removed && !slices.Contains(order, pod.Name) {
				order = append(order, pod.Name)
			}
		}
		if len(removing) > 1 {
			c.Errorf("more than one pool pod being removed at once: %v", removing)
			return false
		}
		return true
	}
	wg.Go(func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			if !sample() {
				return
			}
		}
	})
	halt := sync.OnceFunc(func() {
		close(stop)
		wg.Wait()
	})
	// A scenario that fails before it stops the sampler must not leave it
	// listing pods for the rest of the suite.
	c.Cleanup(halt)
	return func() []string {
		halt()
		// One more look, for a removal that finished after the last tick.
		sample()
		return order
	}
}

// TestRolloutReachesPodPastRefusal rolls a four-pod pool whose primary, pod
// 0, reports pod 2 as not connected. Pod 1 comes first in the rollout's
// order and is refused, because pod 2 would be left among the survivors. Pod
// 2 is the one pod that may be removed, and its replacement reconnects. A
// rollout that stops at the first refusal never reaches it and never ends.
func TestRolloutReachesPodPastRefusal(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollClusterOf("past-refusal", 4)
	pods := c.sortedPoolPods()
	c.disconnectFollower(&pods[0], pods[2].Name, containerImage(&pods[2], "postgres"))

	stop := c.watchOneDisruption()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitAllOn(fixedPostgresImage, 2*unreadyRollWithin)
	order := stop()
	c.Len(order, 4, "pods replaced")
	c.Eq(pods[2].Name, order[0], "the disconnected pod is replaced first")
	c.Eq(pods[0].Name, order[3], "the primary is replaced last")

	// Every drain step and deletion in that rollout is a Pod watch event, so
	// no pass polls for one. The shortest timer any pass may ask for is the
	// five-second floor of the backoffs.
	for _, pass := range c.Suite.Reconciles.InNamespace(c.NS) {
		if pass.Controller == "shard" && pass.RequestedAfter > 0 &&
			pass.RequestedAfter < 5*time.Second {
			c.Errorf("shard pass %d asked to be requeued after %s", pass.Seq, pass.RequestedAfter)
		}
	}
}

// TestScaleDownReachesExtraPastRefusal scales a five-pod pool to three while
// the primary reports pod 3 as not connected. Scale-down ranks the highest
// ordinal first, so pod 4 is tried first and refused, because pod 3 would be
// left among the survivors. Pod 3 may be removed, and once it is gone so may
// pod 4.
func TestScaleDownReachesExtraPastRefusal(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollClusterOf("extra-refusal", 5)
	pods := c.sortedPoolPods()
	c.disconnectFollower(&pods[0], pods[3].Name, "")

	stop := c.watchRemovals(pods)
	c.scalePoolTo(cluster, 3)
	c.Eventually(unreadyRollWithin, "the pool to have three pods", func() error {
		left, err := c.poolPods()
		if err != nil {
			return err
		}
		if len(left) != 3 {
			return fmt.Errorf("%d pool pods", len(left))
		}
		return nil
	})
	c.EqDeep([]string{pods[3].Name, pods[4].Name}, stop(), "extras removed, in order")
}

// TestScaleDownInOnePoolBeforeReplacementInAnother changes the image of a
// shard with two pools of two pods and scales pool beta to one in the same
// update. Beta's extra pod goes first although alpha, which sorts first, has
// a drifted replica: the order between kinds of disruption holds across the
// shard, not within a pool.
func TestScaleDownInOnePoolBeforeReplacementInAnother(t *testing.T) {
	c := newCase(t)
	cluster := c.multiPoolClusterOf("scale-across-pools", 2, "alpha", "beta")
	pods := c.sortedPoolPods()
	c.Eq("alpha", pods[1].Labels[metadata.LabelMultigresPool], "pod 1's pool")
	c.Eq("beta", pods[3].Labels[metadata.LabelMultigresPool], "pod 3's pool")

	stop := c.watchRemovals(pods)
	unreadyPoolSize.Store(c.NS, 3)
	c.Cleanup(func() { unreadyPoolSize.Delete(c.NS) })
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
		shard := &got.Spec.Databases[0].TableGroups[0].Shards[0]
		pool := shard.Spec.Pools["beta"]
		pool.ReplicasPerCell = ptr.To(int32(1))
		shard.Spec.Pools["beta"] = pool
	})
	c.waitAllOn(fixedPostgresImage, 2*unreadyRollWithin)
	order := stop()
	c.Len(order, 4, "pods removed")
	c.Eq(pods[3].Name, order[0], "beta's extra pod is removed before any pod is replaced")
	c.Eq(pods[0].Name, order[3], "the primary is replaced last")
}
