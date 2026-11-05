package suite

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/testkit/ctrltest"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const fixedPostgresImage multigresv1alpha1.ImageRef = "example.com/postgres:fixed"

// unreadyRollPods is the pool size most tests here use. Two is too few for
// any replacement: a two-member cohort has no majority without the member
// being removed, so posture.CheckDisruption refuses every removal.
const unreadyRollPods = 3

// unreadyPoolSize is each namespace's expected pool size, read by the
// helpers below.
var unreadyPoolSize sync.Map

func (c *C) poolSize() int {
	n, ok := unreadyPoolSize.Load(c.NS)
	if !ok {
		return unreadyRollPods
	}
	return n.(int)
}

// unreadyRollWithin bounds a whole rollout of unreadyRollPods pods. Each
// replacement walks the drain annotations across several reconciles and then
// waits for the replacement to register and serve, with the 5s recovery
// requeue between preflights.
const unreadyRollWithin = 120 * time.Second

// unreadyRollCluster creates a three-pod pool whose sim cohort is committed
// under AtLeastN(2), the smallest shape in which a pod can be replaced, and
// waits for it to be healthy.
func (c *C) unreadyRollCluster(name string) *MultigresCluster {
	c.Helper()
	return c.unreadyRollClusterOf(name, unreadyRollPods)
}

func (c *C) unreadyRollClusterOf(name string, pods int) *MultigresCluster {
	c.Helper()
	unreadyPoolSize.Store(c.NS, pods)
	release := poolers.Model(c.NS, poolerModel{durability: topoclient.AtLeastN(2)})
	c.Cleanup(release)
	//nolint:gosec // a handful of pods
	cluster := c.poolThrashCluster(name, int32(pods))
	c.WaitForClusterHealthy(cluster)
	c.Len(c.sortedPoolPods(), pods, "pool pods")
	return cluster
}

// sortedPoolPods returns the namespace's pool pods in name order, so index 0
// is the sim's primary.
func (c *C) sortedPoolPods() []corev1.Pod {
	c.Helper()
	pods, err := c.poolPods()
	c.NoError(err, "list pool pods")
	slices.SortFunc(pods, func(a, b corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	return pods
}

// waitShardDrifted waits for the shard to want its pods replaced, so an
// assertion that nothing was replaced cannot pass because the spec change
// never arrived.
func (c *C) waitShardDrifted() {
	c.Helper()
	c.Eventually(30*time.Second, "the shard to report its pods drifted", func() error {
		shards := &ShardList{}
		if err := c.List(shards); err != nil {
			return err
		}
		if len(shards.Items) == 0 {
			return fmt.Errorf("no shards")
		}
		for _, sh := range shards.Items {
			cond := meta.FindStatusCondition(sh.Status.Conditions, "RollingUpdate")
			if cond == nil || (cond.Reason != "PodsDrifted" && cond.Reason != "RolloutBlocked") {
				return fmt.Errorf("shard %s RollingUpdate=%+v", sh.Name, cond)
			}
		}
		return nil
	})
}

// waitAllOn waits until every pool pod runs image and is Ready.
func (c *C) waitAllOn(image multigresv1alpha1.ImageRef, within time.Duration) {
	c.Helper()
	c.Eventually(within, "every pool pod Ready on "+string(image), func() error {
		pods, err := c.poolPods()
		if err != nil {
			return err
		}
		if len(pods) != c.poolSize() {
			return fmt.Errorf("%d pool pods", len(pods))
		}
		for i := range pods {
			pod := &pods[i]
			if got := containerImage(pod, "postgres"); got != string(image) || !podReady(pod) ||
				!pod.DeletionTimestamp.IsZero() {
				return fmt.Errorf("pod %s on %s Ready=%v", pod.Name, got, podReady(pod))
			}
		}
		return nil
	})
}

// watchOneDisruption fails the test if more than one pool pod is ever being
// removed at once: draining, terminating, or deleted and not yet recreated.
// Sampled, so it can miss a short overlap but never reports one that did not
// happen. The returned function stops it and returns the pods in the order
// their removal was first seen.
func (c *C) watchOneDisruption() func() []string {
	c.Helper()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var order []string
	// A pod's removal is recorded the first time it is seen draining,
	// terminating, missing, or under a new UID, whichever sampling catches;
	// a drain can finish between two samples.
	initial := map[string]string{}
	pods, err := c.poolPods()
	c.NoError(err, "list pool pods")
	for _, pod := range pods {
		initial[pod.Name] = string(pod.UID)
	}
	recorded := map[string]bool{}
	record := func(name string) {
		if _, tracked := initial[name]; tracked && !recorded[name] {
			recorded[name] = true
			order = append(order, name)
		}
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
			pods, err := c.poolPods()
			if err != nil {
				continue
			}
			var removing []string
			present := map[string]bool{}
			for i := range pods {
				pod := &pods[i]
				present[pod.Name] = true
				if pod.Annotations[metadata.AnnotationDrainState] != "" ||
					!pod.DeletionTimestamp.IsZero() {
					removing = append(removing, pod.Name)
					record(pod.Name)
				} else if uid, ok := initial[pod.Name]; ok && uid != string(pod.UID) {
					record(pod.Name)
				}
			}
			for _, name := range slices.Sorted(maps.Keys(initial)) {
				if !present[name] {
					record(name)
				}
			}
			missing := c.poolSize() - len(pods)
			if len(removing)+missing > 1 {
				c.Errorf("more than one pool pod disrupted at once: removing %v, %d missing",
					removing, missing)
				return
			}
		}
	})
	var once sync.Once
	return func() []string {
		once.Do(func() {
			close(stop)
			wg.Wait()
		})
		return order
	}
}

// requireNoneRemoved waits for long enough that the operator would have
// started a replacement, then fails if any pool pod was drained, deleted or
// recreated.
func (c *C) requireNoneRemoved(before []corev1.Pod, why string) {
	c.Helper()
	uids := map[string]string{}
	for _, pod := range before {
		uids[pod.Name] = string(pod.UID)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, pod := range c.sortedPoolPods() {
			if uids[pod.Name] != string(pod.UID) ||
				pod.Annotations[metadata.AnnotationDrainState] != "" ||
				!pod.DeletionTimestamp.IsZero() {
				c.Fatalf("%s: pod %s was disrupted (uid %s, drain %q)", why, pod.Name, pod.UID,
					pod.Annotations[metadata.AnnotationDrainState])
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitRolloutBlocked waits for the shard to report, in its RollingUpdate
// condition, that the rollout is waiting on a safety check and why, so a
// refusal is visible rather than silent.
func (c *C) waitRolloutBlocked(contains string) {
	c.Helper()
	c.Eventually(30*time.Second, "RollingUpdate=RolloutBlocked mentioning "+contains, func() error {
		shards := &ShardList{}
		if err := c.List(shards); err != nil {
			return err
		}
		if len(shards.Items) == 0 {
			return fmt.Errorf("no shards")
		}
		for _, sh := range shards.Items {
			cond := meta.FindStatusCondition(sh.Status.Conditions, "RollingUpdate")
			if cond == nil || cond.Reason != "RolloutBlocked" ||
				!strings.Contains(cond.Message, contains) {
				return fmt.Errorf("shard %s RollingUpdate=%+v", sh.Name, cond)
			}
		}
		return nil
	})
}

// waitPoolersNotReady waits until the operator has observed each named pod's
// pooler as not ready, through the pooler readiness gate it writes from its
// own Status RPCs. A sim fault takes effect on the sim's next tick, so a test
// that changes the spec before this can race the operator against it.
func (c *C) waitPoolersNotReady(names ...string) {
	c.Helper()
	c.Eventually(30*time.Second, fmt.Sprintf("poolers %v observed not ready", names), func() error {
		for _, name := range names {
			pod := &corev1.Pod{}
			if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, pod); err != nil {
				return err
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == shardcontroller.PoolerDataReadyCondition &&
					cond.Status == corev1.ConditionTrue {
					return fmt.Errorf("pod %s pooler still observed ready", name)
				}
			}
		}
		return nil
	})
}

// faultOnImage applies fault to pods running image, so a replacement on any
// other image is healthy.
func faultOnImage(image string, fault *ctrltest.PodFault) ctrltest.PodFaultFunc {
	return func(pod *corev1.Pod) *ctrltest.PodFault {
		if containerImage(pod, "postgres") == image {
			return fault
		}
		return nil
	}
}

func poolerFaultOnImage(image string, fault *poolerFault) func(*corev1.Pod) *poolerFault {
	return func(pod *corev1.Pod) *poolerFault {
		if containerImage(pod, "postgres") == image {
			return fault
		}
		return nil
	}
}

// TestRollForwardFromUnreadyPod: a replica crashlooping on the Postgres image
// the shard is moving off is replaced by the fix, and the rest of the pool
// then rolls one pod at a time.
//
// The crashlooping replica is built directly rather than by rolling onto a
// bad image, so only that one pod is broken. Its pooler answers RPCs with
// PostgreSQL down, as a real multipooler beside a crashlooping Postgres
// does. Once it is replaced the others are Ready and drifted, so they take
// the ordinary health-gated path.
func TestRollForwardFromUnreadyPod(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-roll")
	pods := c.sortedPoolPods()
	broken := pods[2].Name
	badImage := containerImage(&pods[2], "postgres")

	c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
		if pod.Name == broken && containerImage(pod, "postgres") == badImage {
			return &ctrltest.PodFault{CrashLoop: []string{"postgres"}}
		}
		return nil
	})
	poolers.SetFault(c.NS, func(pod *corev1.Pod) *poolerFault {
		if pod.Name == broken && containerImage(pod, "postgres") == badImage {
			return &poolerFault{notServing: true}
		}
		return nil
	})
	c.Eventually(30*time.Second, "pod "+broken+" not Ready", func() error {
		pod := &corev1.Pod{}
		if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: broken}, pod); err != nil {
			return err
		}
		if podReady(pod) {
			return fmt.Errorf("pod %s is still Ready", broken)
		}
		return nil
	})
	c.waitPoolersNotReady(broken)

	stop := c.watchOneDisruption()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitShardDrifted()
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	stop()
}

// TestRollForwardAllPodsUnready: every pod, the primary included, is not Ready
// on the old spec while its pooler still serves, as when a bad probe or
// sidecar fails readiness on a healthy data plane. The fix rolls every pod,
// one at a time, replicas before the primary.
func TestRollForwardAllPodsUnready(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-all")
	badImage := containerImage(&c.sortedPoolPods()[0], "postgres")

	c.FaultPods(faultOnImage(badImage, &ctrltest.PodFault{NotReady: []string{"postgres"}}))
	c.Eventually(30*time.Second, "every pool pod not Ready", func() error {
		for _, pod := range c.sortedPoolPods() {
			if podReady(&pod) {
				return fmt.Errorf("pod %s is still Ready", pod.Name)
			}
		}
		return nil
	})

	primary := c.sortedPoolPods()[0].Name
	stop := c.watchOneDisruption()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitShardDrifted()
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	order := stop()
	c.Check().Eq(unreadyRollPods, len(order), "pods replaced: %v", order)
	if len(order) > 0 {
		c.Check().Eq(primary, order[len(order)-1], "the primary replaced last: %v", order)
	}
}

// TestRollForwardBlockedWithoutServingPrimary: when no pod's pooler serves,
// there is no committed primary to protect durability with, so nothing is
// replaced and the wait says why. Once the data plane serves again, the same
// wait ends and the rollout completes without any further edit.
func TestRollForwardBlockedWithoutServingPrimary(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-noprimary")
	badImage := containerImage(&c.sortedPoolPods()[0], "postgres")

	c.FaultPods(faultOnImage(badImage, &ctrltest.PodFault{CrashLoop: []string{"postgres"}}))
	poolers.SetFault(c.NS, poolerFaultOnImage(badImage, &poolerFault{notServing: true}))
	c.Eventually(30*time.Second, "every pool pod not Ready", func() error {
		for _, pod := range c.sortedPoolPods() {
			if podReady(&pod) {
				return fmt.Errorf("pod %s is still Ready", pod.Name)
			}
		}
		return nil
	})
	var all []string
	for _, pod := range c.sortedPoolPods() {
		all = append(all, pod.Name)
	}
	c.waitPoolersNotReady(all...)

	before := c.sortedPoolPods()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitShardDrifted()
	c.waitRolloutBlocked("primary is not an eligible committed cohort member")
	c.requireNoneRemoved(before, "no serving primary")

	// The pods stay not Ready; only the poolers recover.
	stop := c.watchOneDisruption()
	poolers.SetFault(c.NS, nil)
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	stop()
}

// TestRollForwardPrimaryUnready: only the primary is not Ready, its pooler
// still serving, and a spec change drifts every pod. The Ready replicas would
// wait for shard health, which cannot come while the primary is not Ready, so
// they are replaced through the preflight instead, and the primary last, so
// it is not failed over onto a replica still to be replaced.
func TestRollForwardPrimaryUnready(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-primary")
	pods := c.sortedPoolPods()
	primary := pods[0].Name
	badImage := containerImage(&pods[0], "postgres")

	c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
		if pod.Name == primary && containerImage(pod, "postgres") == badImage {
			return &ctrltest.PodFault{NotReady: []string{"postgres"}}
		}
		return nil
	})
	c.Eventually(30*time.Second, "primary "+primary+" not Ready", func() error {
		pod := &corev1.Pod{}
		if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: primary}, pod); err != nil {
			return err
		}
		if podReady(pod) {
			return fmt.Errorf("pod %s is still Ready", primary)
		}
		return nil
	})

	stop := c.watchOneDisruption()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitShardDrifted()
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	order := stop()
	c.Check().Eq(unreadyRollPods, len(order), "pods replaced: %v", order)
	if len(order) > 0 {
		c.Check().Eq(primary, order[len(order)-1], "the primary replaced last: %v", order)
	}
}

// TestRollForwardOneSurvivor: both replicas are broken on the old image and
// their poolers do not answer, leaving the primary as the only serving
// member. Replacing either broken replica would leave a remainder that cannot
// satisfy AtLeastN(2), so nothing is replaced, the healthy primary least of
// all, and the wait names the member it needs. When one replica's pooler
// answers again the wait ends and the pool rolls forward.
func TestRollForwardOneSurvivor(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-survivor")
	pods := c.sortedPoolPods()
	badImage := containerImage(&pods[0], "postgres")
	brokenReplicas := map[string]bool{pods[1].Name: true, pods[2].Name: true}

	brokenFault := func(pod *corev1.Pod) *ctrltest.PodFault {
		if brokenReplicas[pod.Name] && containerImage(pod, "postgres") == badImage {
			return &ctrltest.PodFault{CrashLoop: []string{"postgres"}}
		}
		return nil
	}
	c.FaultPods(brokenFault)
	poolers.SetFault(c.NS, func(pod *corev1.Pod) *poolerFault {
		if brokenReplicas[pod.Name] && containerImage(pod, "postgres") == badImage {
			return &poolerFault{unreachable: true}
		}
		return nil
	})
	c.Eventually(30*time.Second, "both replicas not Ready", func() error {
		for _, pod := range c.sortedPoolPods() {
			if brokenReplicas[pod.Name] && podReady(&pod) {
				return fmt.Errorf("pod %s is still Ready", pod.Name)
			}
		}
		return nil
	})
	c.waitPoolersNotReady(pods[1].Name, pods[2].Name)

	before := c.sortedPoolPods()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitShardDrifted()
	c.waitRolloutBlocked("did not answer")
	c.requireNoneRemoved(before, "one serving member")

	// pods[2]'s pooler answers again, though its pod stays not Ready on the
	// old image. pods[1] can now go, then pods[2], then the primary.
	stop := c.watchOneDisruption()
	poolers.SetFault(c.NS, func(pod *corev1.Pod) *poolerFault {
		if pod.Name == pods[1].Name && containerImage(pod, "postgres") == badImage {
			return &poolerFault{unreachable: true}
		}
		return nil
	})
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	stop()
}

// TestRollForwardSkipsInFlightRestore: a not-Ready drifted replica whose
// pooler reports a restore in flight is not restarted, so the restore is not
// thrown away; once the restore finishes the replica is replaced.
func TestRollForwardSkipsInFlightRestore(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-restore")
	pods := c.sortedPoolPods()
	restoring := pods[2].Name
	badImage := containerImage(&pods[2], "postgres")

	c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
		if pod.Name == restoring && containerImage(pod, "postgres") == badImage {
			return &ctrltest.PodFault{NotReady: []string{"postgres"}}
		}
		return nil
	})
	poolers.SetFault(c.NS, func(pod *corev1.Pod) *poolerFault {
		if pod.Name == restoring && containerImage(pod, "postgres") == badImage {
			return &poolerFault{
				notServing: true,
				action:     md.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP,
			}
		}
		return nil
	})
	c.Eventually(30*time.Second, "pod "+restoring+" not Ready", func() error {
		pod := &corev1.Pod{}
		if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: restoring}, pod); err != nil {
			return err
		}
		if podReady(pod) {
			return fmt.Errorf("pod %s is still Ready", restoring)
		}
		return nil
	})
	c.waitPoolersNotReady(restoring)

	before := c.sortedPoolPods()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitShardDrifted()
	c.waitRolloutBlocked("in-flight postgres action POSTGRES_ACTION_RESTORING_FROM_BACKUP")
	c.requireNoneRemoved(before, "restore in flight")

	stop := c.watchOneDisruption()
	poolers.SetFault(c.NS, func(pod *corev1.Pod) *poolerFault {
		if pod.Name == restoring && containerImage(pod, "postgres") == badImage {
			return &poolerFault{notServing: true}
		}
		return nil
	})
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	stop()
}

// TestRollForwardRevertAndReturn walks a bad image out and back:
//
//  1. Roll onto a bad image. The first replica replaced crashloops on it, and
//     the rollout stops there: the Ready pods still on the good image wait
//     for shard health, and the broken pod is not drifted.
//  2. Revert to the good image while that is in flight. The broken replica is
//     now drifted and not Ready, so it is replaced, and the pool settles back
//     on the good image with every pod Ready.
//  3. The return trip: roll onto the bad image again, then fix forward to a
//     third image. The same single pod breaks, and the fix rolls every pod.
func TestRollForwardRevertAndReturn(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-revert")
	goodImage := containerImage(&c.sortedPoolPods()[0], "postgres")
	const badImage = "example.com/postgres:bad"

	c.FaultPods(faultOnImage(badImage, &ctrltest.PodFault{CrashLoop: []string{"postgres"}}))
	poolers.SetFault(c.NS, poolerFaultOnImage(badImage, &poolerFault{notServing: true}))

	rollOntoBad := func(step string) {
		c.Helper()
		c.updateCluster(cluster, func(got *MultigresCluster) {
			got.Spec.Images.Postgres = badImage
		})
		c.waitShardDrifted()
		c.Eventually(unreadyRollWithin, step+": one pod broken on the bad image", func() error {
			var bad []string
			for _, pod := range c.sortedPoolPods() {
				if containerImage(&pod, "postgres") == badImage && !podReady(&pod) {
					bad = append(bad, pod.Name)
				}
			}
			if len(bad) != 1 {
				return fmt.Errorf("pods broken on the bad image: %v", bad)
			}
			return nil
		})
		// The bad image must not spread past the pod it broke.
		before := c.sortedPoolPods()
		c.requireNoneRemoved(before, step+": rollout halted on a broken pod")
	}

	stop := c.watchOneDisruption()
	rollOntoBad("first roll")
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = multigresv1alpha1.ImageRef(goodImage)
	})
	c.waitAllOn(multigresv1alpha1.ImageRef(goodImage), unreadyRollWithin)

	rollOntoBad("return trip")
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	stop()
}

// TestRollForwardDoesNotSpreadBadSpec: with a pod broken on a bad desired
// spec, an old-spec pod that turns not Ready is not replaced onto that spec.
//
// Four pods, so the posture check alone would let the flapping pod go: a
// durable remainder of two is left either way. What holds it back is the pod
// on the desired spec not being both Ready and serving, for each way a spec
// can be bad: a pooler that never starts (so the broken pod is not even a
// cohort member), and a readiness probe that fails while the pooler serves.
func TestRollForwardDoesNotSpreadBadSpec(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pod    *ctrltest.PodFault
		pooler *poolerFault
	}{
		{
			name:   "pooler never starts",
			pod:    &ctrltest.PodFault{CrashLoop: []string{"postgres"}},
			pooler: &poolerFault{neverRegisters: true},
		},
		{
			name: "readiness fails while the pooler serves",
			pod:  &ctrltest.PodFault{NotReady: []string{"postgres"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCase(t)
			cluster := c.unreadyRollClusterOf("unready-spread", 4)
			const badImage = "example.com/postgres:bad"
			flapping := c.sortedPoolPods()[3].Name

			c.FaultPods(faultOnImage(badImage, tc.pod))
			poolers.SetFault(c.NS, poolerFaultOnImage(badImage, tc.pooler))
			c.updateCluster(cluster, func(got *MultigresCluster) {
				got.Spec.Images.Postgres = badImage
			})
			c.waitShardDrifted()
			c.Eventually(unreadyRollWithin, "one pod broken on the bad image", func() error {
				for _, pod := range c.sortedPoolPods() {
					if containerImage(&pod, "postgres") == badImage && !podReady(&pod) {
						return nil
					}
				}
				return fmt.Errorf("no pod broken on the bad image yet")
			})

			c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
				if containerImage(pod, "postgres") == badImage {
					return tc.pod
				}
				if pod.Name == flapping {
					return &ctrltest.PodFault{NotReady: []string{"postgres"}}
				}
				return nil
			})
			poolers.SetFault(c.NS, func(pod *corev1.Pod) *poolerFault {
				if containerImage(pod, "postgres") == badImage {
					return tc.pooler
				}
				if pod.Name == flapping {
					return &poolerFault{notServing: true}
				}
				return nil
			})
			c.Eventually(30*time.Second, "pod "+flapping+" not Ready", func() error {
				pod := &corev1.Pod{}
				if err := c.Get(
					client.ObjectKey{Namespace: c.NS, Name: flapping},
					pod,
				); err != nil {
					return err
				}
				if podReady(pod) {
					return fmt.Errorf("pod %s is still Ready", flapping)
				}
				return nil
			})
			c.waitRolloutBlocked("on the desired spec is not")
			c.requireNoneRemoved(c.sortedPoolPods(), "bad spec with a flapping old pod")
		})
	}
}

// TestRollForwardScaleUpWithBadImage: one update both scales the pool up and
// moves it to an image whose pooler never starts, so the new pod never
// registers in topology. Fixing the image replaces that unregistered pod,
// then rolls the rest.
func TestRollForwardScaleUpWithBadImage(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-scaleup")
	const badImage = "example.com/postgres:bad"

	c.FaultPods(faultOnImage(badImage, &ctrltest.PodFault{CrashLoop: []string{"postgres"}}))
	poolers.SetFault(c.NS, poolerFaultOnImage(badImage, &poolerFault{neverRegisters: true}))
	unreadyPoolSize.Store(c.NS, unreadyRollPods+1)
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = badImage
		pools := got.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools
		pool := pools[thrashPoolName]
		pool.ReplicasPerCell = ptr.To(int32(unreadyRollPods + 1))
		pools[thrashPoolName] = pool
	})
	c.waitShardDrifted()
	c.Eventually(unreadyRollWithin, "the new pod broken on the bad image", func() error {
		pods := c.sortedPoolPods()
		if len(pods) != unreadyRollPods+1 {
			return fmt.Errorf("%d pool pods", len(pods))
		}
		last := pods[len(pods)-1]
		if containerImage(&last, "postgres") != badImage || podReady(&last) {
			return fmt.Errorf("pod %s on %s Ready=%v", last.Name,
				containerImage(&last, "postgres"), podReady(&last))
		}
		return nil
	})

	stop := c.watchOneDisruption()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		got.Spec.Images.Postgres = fixedPostgresImage
	})
	c.waitAllOn(fixedPostgresImage, unreadyRollWithin)
	stop()
}

// TestRollForwardScaleUpRevertBoth: the return trip of a combined update.
// Scaling up together with an image whose pooler never starts leaves a new pod
// that never registers; reverting both fields in one update makes it an
// extra, which scale-down then removes although it never registered.
func TestRollForwardScaleUpRevertBoth(t *testing.T) {
	c := newCase(t)
	cluster := c.unreadyRollCluster("unready-revertboth")
	goodImage := containerImage(&c.sortedPoolPods()[0], "postgres")
	const badImage = "example.com/postgres:bad"

	c.FaultPods(faultOnImage(badImage, &ctrltest.PodFault{CrashLoop: []string{"postgres"}}))
	poolers.SetFault(c.NS, poolerFaultOnImage(badImage, &poolerFault{neverRegisters: true}))
	setPool := func(image string, replicas int32) {
		c.updateCluster(cluster, func(got *MultigresCluster) {
			got.Spec.Images.Postgres = multigresv1alpha1.ImageRef(image)
			pools := got.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools
			pool := pools[thrashPoolName]
			pool.ReplicasPerCell = ptr.To(replicas)
			pools[thrashPoolName] = pool
		})
	}
	setPool(badImage, unreadyRollPods+1)
	c.Eventually(unreadyRollWithin, "the new pod broken on the bad image", func() error {
		pods := c.sortedPoolPods()
		if len(pods) != unreadyRollPods+1 {
			return fmt.Errorf("%d pool pods", len(pods))
		}
		last := pods[len(pods)-1]
		if containerImage(&last, "postgres") != badImage || podReady(&last) {
			return fmt.Errorf("pod %s on %s Ready=%v", last.Name,
				containerImage(&last, "postgres"), podReady(&last))
		}
		return nil
	})

	setPool(goodImage, unreadyRollPods)
	c.waitAllOn(multigresv1alpha1.ImageRef(goodImage), unreadyRollWithin)
}

func (c *C) poolPods() ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	err := c.List(pods, client.MatchingLabels{
		metadata.LabelAppComponent: shardcontroller.PoolComponentName,
	})
	return pods.Items, err
}

// containerImage finds name among a pod's containers and init containers,
// since Postgres runs as a native sidecar.
func containerImage(pod *corev1.Pod, name string) string {
	for _, ctr := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		if ctr.Name == name {
			return ctr.Image
		}
	}
	return ""
}
