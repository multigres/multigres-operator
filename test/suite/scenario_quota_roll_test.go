package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// quotaHoldWindow is how long the rollout is watched while the quota admits
// no pod. A pass that fails to create the missing pod is retried with a
// growing delay, and every write to the Shard or its pods starts another, so
// the window covers a few dozen passes.
const quotaHoldWindow = 25 * time.Second

// quotaRollPods is the pool size of TestRolloutStopsAtAPodTheQuotaDoesNotAdmit.
// With five pods under AtLeastN(2), the cohort stays durable without two of
// them, so the durability check alone would let a second pod go while the
// first is missing: what stops the rollout is that a pod is missing.
const quotaRollPods = 5

// TestRolloutStopsAtAPodTheQuotaDoesNotAdmit rolls a healthy five-pod pool
// in a namespace whose ResourceQuota admits no further pod.
//
// A pod over quota is not a pod the API server rejects, so the shard
// controller does not hold the rollout for it: the first pod is drained and
// deleted, and its replacement is then turned down by the quota on every
// pass. The rollout has to stop there. The four pods that are left, the
// primary among them, are never drained or deleted and stay Ready, no pod
// template refusal is recorded, and the missing pod is created, and the
// rollout finished, as soon as the namespace has room.
//
// Testkit gap: the harness runs no ResourceQuota controller, so the quota's
// usage is written by hand, full at first and empty to make room. With a
// usage sim this test could size the quota to the pods that exist and let a
// deleted pod free its own share.
func TestRolloutStopsAtAPodTheQuotaDoesNotAdmit(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollClusterOf("quota-roll", quotaRollPods)
	before := c.sortedPoolPods()
	primary := before[0]

	hard := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("100")}
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "full", Namespace: c.NS},
		Spec:       corev1.ResourceQuotaSpec{Hard: hard},
	}
	c.NoError(c.Create(quota), "create the quota")
	setUsed := func(used string) {
		c.Helper()
		c.NoError(c.Get(client.ObjectKeyFromObject(quota), quota), "get the quota")
		quota.Status = corev1.ResourceQuotaStatus{
			Hard: hard,
			Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse(used)},
		}
		c.NoError(c.Client().Status().Update(c.Context(), quota), "write the quota's usage")
	}
	setUsed("100")
	c.Eventually(30*time.Second, "the quota to turn pods down", func() error {
		probe := before[1].DeepCopy()
		probe.ObjectMeta = metav1.ObjectMeta{Namespace: c.NS, Name: "quota-probe"}
		err := c.Create(probe, client.DryRunAll)
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "exceeded quota: full") {
			return fmt.Errorf("a pod create is answered with %v", err)
		}
		return nil
	})

	stop := c.watchOneDisruption()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitShardDrifted()

	var kept []corev1.Pod
	c.Eventually(
		unreadyRollWithin,
		"one pool pod to be given up for its replacement",
		func() error {
			pods := c.sortedPoolPods()
			if len(pods) != quotaRollPods-1 {
				return fmt.Errorf("%d pool pods", len(pods))
			}
			kept = pods
			return nil
		},
	)
	c.Len(kept, quotaRollPods-1, "pool pods left")
	c.Eq(primary.Name, kept[0].Name, "the primary is not the first pod replaced")

	deadline := time.Now().Add(quotaHoldWindow)
	for time.Now().Before(deadline) {
		pods := c.sortedPoolPods()
		c.Len(pods, len(kept), "pool pods while the quota admits none")
		for i := range pods {
			pod := &pods[i]
			if pod.UID != kept[i].UID || !pod.DeletionTimestamp.IsZero() ||
				pod.Annotations[metadata.AnnotationDrainState] != "" {
				c.Fatalf(
					"pod %s was taken while pod %d of the pool is missing (uid %s, was %s, drain %q)",
					pod.Name,
					quotaRollPods-len(pods),
					pod.UID,
					kept[i].UID,
					pod.Annotations[metadata.AnnotationDrainState],
				)
			}
			if !podReady(pod) {
				c.Fatalf("pod %s stopped being Ready while the quota admits no pod", pod.Name)
			}
		}
		var shards ShardList
		c.NoError(c.List(&shards), "list shards")
		for i := range shards.Items {
			cond := meta.FindStatusCondition(
				shards.Items[i].Status.Conditions, api.ConditionPodTemplateAccepted)
			if cond != nil {
				c.Fatalf("a quota answer was recorded as a refusal of the pod template: %+v", cond)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}

	// The namespace gets room: the missing pod is created and the rest of
	// the pool follows, one pod at a time.
	setUsed("0")
	c.waitAllOn(fixedPostgresImage, 2*unreadyRollWithin)
	order := stop()
	c.Check().Eq(quotaRollPods, len(order), "pods replaced: %v", order)
	if len(order) > 0 {
		c.Check().Eq(primary.Name, order[len(order)-1], "the primary replaced last: %v", order)
	}
}

// quotaWatchWithin bounds the wait for a create that was over quota to be
// tried again once the quota has room. The test releases the quota only
// when the Shard's own next retry is further off than this, so only the
// ResourceQuota watch can meet it.
const quotaWatchWithin = 15 * time.Second

// lastShardPasses returns when the two most recent passes of the
// namespace's Shard ended.
func (c *C) lastShardPasses() (previous, last time.Time) {
	for _, pass := range Suite.Reconciles.InNamespace(c.NS) {
		if pass.Controller == "shard" && !pass.Gated {
			previous, last = last, pass.End
		}
	}
	return previous, last
}

// TestScaleDownMakesRoomForAPodTheQuotaDoesNotAdmit is the way out of a pool
// that is short of a pod because its namespace is at quota: the owner lowers
// replicasPerCell.
//
// A four-pod pool loses pod 1, and the full quota turns its replacement down
// on every pass. The cluster's Postgres image is then changed, so every pod
// that is left is out of date and Ready. Lowering the pool to three makes pod
// 3 a pod beyond the replica count, and it holds exactly the room pod 1
// needs. It has to be drained and deleted although a desired pod of its pool
// is missing and that pod's create keeps failing, or the pool would wait for
// pod 1 before removing what stands in pod 1's way. Pods 0 and 2 are never
// drained or deleted while pod 1 is missing, and pod 1 is created once pod
// 3's share of the quota is given back.
//
// Testkit gap: no ResourceQuota controller runs, so the usage is written by
// hand, also when the removed pod's share is released.
func TestScaleDownMakesRoomForAPodTheQuotaDoesNotAdmit(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollClusterOf("quota-scale", 4)
	before := c.sortedPoolPods()
	missing, extra := before[1], before[3]
	desired := []corev1.Pod{before[0], before[2]}

	hard := corev1.ResourceList{corev1.ResourcePods: resource.MustParse("100")}
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "full", Namespace: c.NS},
		Spec:       corev1.ResourceQuotaSpec{Hard: hard},
	}
	c.NoError(c.Create(quota), "create the quota")
	setUsed := func(used string) {
		c.Helper()
		c.NoError(c.Get(client.ObjectKeyFromObject(quota), quota), "get the quota")
		quota.Status = corev1.ResourceQuotaStatus{
			Hard: hard,
			Used: corev1.ResourceList{corev1.ResourcePods: resource.MustParse(used)},
		}
		c.NoError(c.Client().Status().Update(c.Context(), quota), "write the quota's usage")
	}
	setUsed("100")
	c.Eventually(30*time.Second, "the quota to turn pods down", func() error {
		probe := before[1].DeepCopy()
		probe.ObjectMeta = metav1.ObjectMeta{Namespace: c.NS, Name: "quota-probe"}
		err := c.Create(probe, client.DryRunAll)
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "exceeded quota: full") {
			return fmt.Errorf("a pod create is answered with %v", err)
		}
		return nil
	})

	// requireDesiredKept fails the test if pod 0 or pod 2 was drained,
	// deleted or replaced.
	requireDesiredKept := func(when string) {
		c.Helper()
		for i := range desired {
			pod := &corev1.Pod{}
			c.NoError(
				c.Get(client.ObjectKeyFromObject(&desired[i]), pod),
				"get pod %s",
				desired[i].Name,
			)
			if pod.UID != desired[i].UID || !pod.DeletionTimestamp.IsZero() ||
				pod.Annotations[metadata.AnnotationDrainState] != "" {
				c.Fatalf(
					"%s: desired pod %s was taken while pod %s is missing (uid %s, was %s, drain %q)",
					when,
					pod.Name,
					missing.Name,
					pod.UID,
					desired[i].UID,
					pod.Annotations[metadata.AnnotationDrainState],
				)
			}
		}
	}
	podGone := func(name string) error {
		err := c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, &corev1.Pod{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		requireDesiredKept("waiting for pod " + name + " to go")
		return fmt.Errorf("pod %s still exists", name)
	}

	c.NoError(c.Delete(missing.DeepCopy()), "delete pod %s", missing.Name)
	c.Eventually(unreadyRollWithin, "pod "+missing.Name+" to be gone", func() error {
		return podGone(missing.Name)
	})
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	// The failed create ends each pass before the pool reports its rollout,
	// so the Shard's spec is what shows that the pods are out of date.
	c.Eventually(30*time.Second, "the Shard to carry the new image", func() error {
		var shards ShardList
		if err := c.List(&shards); err != nil {
			return err
		}
		if len(shards.Items) != 1 || shards.Items[0].Spec.Images.Postgres != fixedPostgresImage {
			return fmt.Errorf("%d shards, or the image has not arrived", len(shards.Items))
		}
		return nil
	})
	// The stall: the pod stays missing and nothing else is taken.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		requireDesiredKept("while the quota admits no pod")
		c.Len(c.sortedPoolPods(), 3, "pool pods while the quota admits none")
		time.Sleep(250 * time.Millisecond)
	}

	// The owner makes room by asking for one pod less.
	scaledAt := time.Now()
	unreadyPoolSize.Store(c.NS, 3)
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), cluster), "get the cluster")
	c.scalePoolTo(cluster, 3)
	c.Eventually(unreadyRollWithin, "the pod beyond the replica count to be removed", func() error {
		return podGone(extra.Name)
	})
	requireDesiredKept("after the scale-down")
	// Every pass since then that wrote to the pod being scaled down also
	// tried the missing pod's create, and either failed with that create's
	// error, so that it is retried, or asked for the short requeue its drain
	// or its disruption preflight needs. A pass that did neither would leave
	// the create to the next event. Events about a Shard are rate limited
	// per object, so the recorded passes are read and not the FailedApply
	// events.
	scaledDown := 0
	for _, pass := range Suite.Reconciles.InNamespace(c.NS) {
		if pass.Controller != "shard" || pass.Gated || pass.Start.Before(scaledAt) {
			continue
		}
		wroteExtra := false
		for _, op := range Suite.Reconciles.Ops(pass) {
			wroteExtra = wroteExtra || (op.Kind == "Pod" && op.Key.Name == extra.Name)
		}
		if !wroteExtra {
			continue
		}
		scaledDown++
		failed := pass.Err != nil &&
			strings.Contains(pass.Err.Error(), "failed to create pod "+missing.Name) &&
			strings.Contains(pass.Err.Error(), "exceeded quota: full")
		requeued := pass.Err == nil && pass.RequestedAfter > 0 &&
			pass.RequestedAfter <= 30*time.Second
		if !failed && !requeued {
			c.Fatalf("a pass that scaled the pool down ended with error %v and requeue %s, "+
				"want the failed create of %s or a short requeue",
				pass.Err, pass.RequestedAfter, missing.Name)
		}
	}
	c.True(scaledDown > 0, "no recorded pass wrote to pod %s", extra.Name)
	// The create is to be sent because the quota changed, so the release
	// waits until the Shard's own retries are further apart than the time
	// the watch is given: a Shard a quota turned down retries after as long
	// as it has been waiting, so once two passes in a row are that far
	// apart, the next retry is at least as far from the later one. The
	// later pass must be a few seconds old at most, and at least one, so
	// that no pass started by an event follows it.
	c.Eventually(5*time.Minute, "the shard's retries to be far apart", func() error {
		previous, last := c.lastShardPasses()
		gap, since := last.Sub(previous), time.Since(last)
		if gap < quotaWatchWithin+10*time.Second || since < time.Second || since > 5*time.Second {
			return fmt.Errorf("the last two passes are %s apart, the later one %s ago", gap, since)
		}
		return nil
	})
	setUsed("99")
	// The shard controller watches the quota, so the create is retried when
	// usage falls and not when the delay of the failed passes runs out.
	c.Eventually(quotaWatchWithin, "pod "+missing.Name+" to be created", func() error {
		return c.Get(client.ObjectKey{Namespace: c.NS, Name: missing.Name}, &corev1.Pod{})
	})
	c.Eventually(unreadyRollWithin, "pod "+missing.Name+" to be Ready", func() error {
		pod := &corev1.Pod{}
		if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: missing.Name}, pod); err != nil {
			requireDesiredKept("waiting for pod " + missing.Name)
			return err
		}
		if pod.UID == missing.UID || !podReady(pod) {
			return fmt.Errorf("pod %s uid %s Ready=%v", pod.Name, pod.UID, podReady(pod))
		}
		if got := containerImage(pod, "postgres"); got != string(fixedPostgresImage) {
			return fmt.Errorf("pod %s runs %s", pod.Name, got)
		}
		return nil
	})
}
