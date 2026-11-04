package suite

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/testkit/ctrltest"
)

const (
	sidecarName = shardcontroller.PgBackRestSidecarContainerName

	// fixedSidecarImage is the image an operator sets to replace a sidecar that
	// cannot start; changing it drifts every pool pod.
	fixedSidecarImage = "registry.example/multigres:sidecar-fixed"
)

func fixSidecarImage(got *MultigresCluster) {
	got.Spec.Images.PgBackRestd = fixedSidecarImage
}

// sidecarCluster creates a cluster whose pods are faulted from
// the moment they are created: ordinal 1 never starts its sidecar and
// ordinal 0 is merely unready, so the shard never reports healthy.
//
// Faulting from creation is what makes the stuck pod's status the real
// kubelet shape for a sidecar that never started: pod not Initialized, the
// regular containers Waiting in PodInitializing, pgbackrest Started=false.
// Faulting a pod that was already running would instead model a sidecar that
// started once and crashed, which kubelet reports differently (see
// TestSidecarStartedOnceIsNotReplaced).
func (c *C) sidecarCluster(
	name string,
	fault func(ordinal string) *ctrltest.PodFault,
) *MultigresCluster {
	c.Helper()
	c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
		if containerImage(pod, sidecarName) == "" {
			return nil
		}
		return fault(pod.Name[strings.LastIndex(pod.Name, "-")+1:])
	})
	return c.newCluster(name)
}

func (c *C) awaitPoolPods(n int, why string) []corev1.Pod {
	c.Helper()
	var pods []corev1.Pod
	c.Eventually(30*time.Second, why, func() error {
		var err error
		pods, err = c.poolPods()
		if err != nil {
			return err
		}
		if len(pods) != n {
			return fmt.Errorf("%d pool pods, want %d", len(pods), n)
		}
		for i := range pods {
			if len(pods[i].Status.InitContainerStatuses) == 0 {
				return fmt.Errorf("pod %s has no status yet", pods[i].Name)
			}
		}
		return nil
	})
	return pods
}

func podByOrdinal(pods []corev1.Pod, ordinal string) corev1.Pod {
	for _, p := range pods {
		if strings.HasSuffix(p.Name, "-"+ordinal) {
			return p
		}
	}
	panic("no pod with ordinal " + ordinal)
}

// TestSidecarStuckPodIsReplacedOnImageFix pins the narrow escape for a pod
// whose pgbackrest sidecar never started. Fixing the sidecar image drifts
// every pool pod, and the gated roll waits for shard health, which a pod that
// can never turn Ready would hold shut forever. The escape replaces exactly
// that pod and leaves a merely unready one to the gated roll.
func TestSidecarStuckPodIsReplacedOnImageFix(t *testing.T) {
	c := newCase(t)
	cluster := c.sidecarCluster("sidecar-stuck", func(ordinal string) *ctrltest.PodFault {
		switch ordinal {
		case "1":
			return &ctrltest.PodFault{CrashLoop: []string{sidecarName}}
		case "0":
			return &ctrltest.PodFault{NotReady: []string{sidecarName}}
		}
		return nil
	})
	pods := c.awaitPoolPods(2, "both pool pods to report status")
	stuck, unready := podByOrdinal(pods, "1"), podByOrdinal(pods, "0")

	c.updateCluster(cluster, fixSidecarImage)

	c.Eventually(60*time.Second, "the stuck pod "+stuck.Name+" to be replaced", func() error {
		pod := &corev1.Pod{}
		if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: stuck.Name}, pod); err != nil {
			return err
		}
		if pod.UID == stuck.UID {
			return fmt.Errorf("pod %s is still the original %s", stuck.Name, stuck.UID)
		}
		if got := containerImage(pod, sidecarName); got != fixedSidecarImage {
			return fmt.Errorf("replacement %s runs sidecar image %q, want %q",
				stuck.Name, got, fixedSidecarImage)
		}
		return nil
	})
	c.requirePodKept(unready, 10*time.Second,
		"unready but not stuck, so it must not be replaced outside the gated roll")
}

// TestSidecarStartedOnceIsNotReplaced pins what the escape must refuse: a pod
// whose sidecar started and is now failing is serving, so it stays on the
// gated roll.
//
// Testkit gap: DataPlaneSim does not model Initialized or init ordering for a
// failing sidecar that has already run. Its CrashLoop fault blocks the regular
// containers and flips Initialized to False, which is what kubelet reports
// only for a sidecar that never started. Kubelet keeps Initialized True while
// a regular container is running, so the shape written here is the real one:
// pgbackrest Started=false and restarting, Initialized True, the regular
// containers Running. The sim holds the pod at its own NotReady shape
// (sidecar Started=true, Initialized True) between writes. Both refuse, so the
// race with the sim cannot flip the outcome; only the real shape tests the
// Started=false half. Delete the writer once the sim models a started-once
// sidecar crash.
func TestSidecarStartedOnceIsNotReplaced(t *testing.T) {
	c := newCase(t)
	cluster := c.newCluster("sidecar-started-once")
	c.WaitForClusterHealthy(cluster)
	pods := c.awaitPoolPods(2, "both pool pods")
	for i := range pods {
		c.NotEmpty(containerImage(&pods[i], sidecarName), "pod %s runs the sidecar", pods[i].Name)
	}
	victim := pods[1]

	c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
		if pod.Name == victim.Name && containerImage(pod, sidecarName) != "" {
			return &ctrltest.PodFault{NotReady: []string{sidecarName}}
		}
		return nil
	})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.Cleanup(func() { stop(); <-done })
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			pod := &corev1.Pod{}
			if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: victim.Name}, pod); err == nil {
				base := pod.DeepCopy()
				for i := range pod.Status.InitContainerStatuses {
					cs := &pod.Status.InitContainerStatuses[i]
					if cs.Name == sidecarName {
						cs.Started = ptr.To(false)
						cs.RestartCount++
					}
				}
				_ = c.Client().Status().Patch(ctx, pod, client.MergeFrom(base))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	c.updateCluster(cluster, fixSidecarImage)
	c.requirePodKept(victim, 3*sidecarStuckDeadline+10*time.Second,
		"its sidecar started once, so the escape must not replace it")
}

// requirePodKept fails if pod's UID changes within d.
func (c *C) requirePodKept(orig corev1.Pod, d time.Duration, why string) {
	c.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		pod := &corev1.Pod{}
		c.NoError(c.Get(client.ObjectKey{Namespace: c.NS, Name: orig.Name}, pod),
			"get pod %s", orig.Name)
		if pod.UID != orig.UID {
			c.Fatalf("pod %s was replaced (%s -> %s): %s", orig.Name, orig.UID, pod.UID, why)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
