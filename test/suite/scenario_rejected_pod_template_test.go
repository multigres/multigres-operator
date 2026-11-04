package suite

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/testkit/ctrltest"
)

// TestPoolPodSurvivesARejectedPodTemplate runs a pool with one pod whose
// pgbackrest sidecar never started, which is a pod the shard controller
// replaces as soon as its spec is out of date, without waiting for the shard
// to be healthy. The cluster is then edited so that the pod is out of date
// and its successor is one the API server rejects: a fixed sidecar image
// together with a pgbackrestResources block that the CRDs store (there is no
// admission webhook here) and pod validation does not accept.
//
// Draining the pod would leave its slot empty until someone edits the spec
// again. The controller has to ask the API server first: the pod keeps its
// UID and is never marked for a drain, the Shard reports
// PodTemplateAccepted=False with the API server's complaint, and Shard and
// cluster report Degraded. Taking the block out, with the image fix still in
// place, lets the same pod be replaced and clears the refusal.
//
// The pod stuck on its sidecar is the pool replacement this suite can reach:
// its data plane sim never lets a healthy pool give up a replica.
func TestPoolPodSurvivesARejectedPodTemplate(t *testing.T) {
	c := newCase(t)
	c.FaultPods(func(pod *corev1.Pod) *ctrltest.PodFault {
		if containerImage(pod, sidecarName) == "" {
			return nil
		}
		switch pod.Name[strings.LastIndex(pod.Name, "-")+1:] {
		case "1":
			return &ctrltest.PodFault{CrashLoop: []string{sidecarName}}
		case "0":
			return &ctrltest.PodFault{NotReady: []string{sidecarName}}
		}
		return nil
	})
	cluster := c.lifecycleCluster("rejected")
	key := client.ObjectKeyFromObject(cluster)
	pods := c.awaitPoolPods(2, "both pool pods to report status")
	stuck := podByOrdinal(pods, "1")

	refusal := func() (*metav1.Condition, api.Phase, api.Phase, error) {
		var shards ShardList
		if err := c.List(&shards); err != nil {
			return nil, "", "", err
		}
		if len(shards.Items) != 1 {
			return nil, "", "", fmt.Errorf("%d shards", len(shards.Items))
		}
		shard := &shards.Items[0]
		var got MultigresCluster
		if err := c.Get(key, &got); err != nil {
			return nil, "", "", err
		}
		return meta.FindStatusCondition(
			shard.Status.Conditions, api.ConditionPodTemplateAccepted,
		), shard.Status.Phase, got.Status.Phase, nil
	}

	// refusalChanges watches the Shard's status until stop is called and
	// counts how often the refusal changed: the PodTemplateAccepted
	// condition, the phase or the message, which are the fields a refusal
	// writes. It samples faster than a controller that reconciles in a loop
	// can write. Changes to anything else are returned by name for the log.
	//
	// Only the refusal's own fields are judged because the rest of the
	// status moves for reasons that have nothing to do with a refusal. One
	// of them is frequent enough to matter: a pass applies status twice,
	// first with the BackupHealthy condition as its cached Shard has it and
	// later with that condition restamped to the current generation, so a
	// pass that starts before its cache has the previous pass's second
	// write flips the condition's observedGeneration back and forth. That
	// can go on for dozens of writes after any spec edit, with or without a
	// refusal.
	//
	// Testkit gap: ctrltest.Stream reports every change of a watched object
	// with its field path, but names a condition by its position in the
	// list rather than by its type, so it cannot say which condition moved.
	// With conditions keyed by type this sampler is a Watch and a filter.
	type statusTally struct {
		refusal int
		other   map[string]int
	}
	refusalChanges := func() (stop func() statusTally) {
		type view struct {
			refusal string
			other   map[string]string
		}
		read := func(status *api.ShardStatus) (view, error) {
			v := view{other: map[string]string{}}
			raw, err := json.Marshal(status)
			if err != nil {
				return v, err
			}
			fields := map[string]json.RawMessage{}
			if err := json.Unmarshal(raw, &fields); err != nil {
				return v, err
			}
			for name, value := range fields {
				if name != "conditions" && name != "phase" && name != "message" {
					v.other[name] = string(value)
				}
			}
			for _, cond := range status.Conditions {
				rendered, err := json.Marshal(cond)
				if err != nil {
					return v, err
				}
				if cond.Type == api.ConditionPodTemplateAccepted {
					v.refusal = string(rendered)
				} else {
					v.other["condition "+cond.Type] = string(rendered)
				}
			}
			v.refusal += " phase=" + string(status.Phase) + " message=" + status.Message
			return v, nil
		}
		done, result := make(chan struct{}), make(chan statusTally)
		go func() {
			tally := statusTally{other: map[string]int{}}
			var last *view
			for {
				var shards ShardList
				if err := c.List(&shards); err == nil && len(shards.Items) == 1 {
					if now, err := read(&shards.Items[0].Status); err == nil {
						if last != nil {
							if now.refusal != last.refusal {
								tally.refusal++
							}
							for name, value := range now.other {
								if last.other[name] != value {
									tally.other[name]++
								}
							}
							for name := range last.other {
								if _, still := now.other[name]; !still {
									tally.other[name]++
								}
							}
						}
						last = &now
					}
				}
				select {
				case <-done:
					result <- tally
					return
				case <-time.After(25 * time.Millisecond):
				}
			}
		}()
		// The sampler must not outlive the test when an assertion ends it
		// before stop is called.
		var once sync.Once
		var tally statusTally
		stop = func() statusTally {
			once.Do(func() { close(done); tally = <-result })
			return tally
		}
		t.Cleanup(func() { stop() })
		return stop
	}

	for i, tc := range []struct {
		name      string
		resources corev1.ResourceRequirements
		complaint string
	}{
		{
			name: "limit below request",
			resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
			},
			complaint: "must be less than or equal to memory limit",
		},
		{
			name: "misspelled resource name",
			resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{"memroy": resource.MustParse("1Gi")},
			},
			complaint: "must be a standard resource for containers",
		},
		{
			name: "negative quantity",
			resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-1Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
			},
			complaint: "must be greater than or equal to 0",
		},
		{
			name: "resource claim",
			resources: corev1.ResourceRequirements{
				Claims: []corev1.ResourceClaim{{Name: "gpu"}},
			},
			complaint: "pod.spec.resourceClaims",
		},
	} {
		// The first block goes in with the image fix that makes the pod
		// replaceable, in one update, so the pod is never out of date with
		// an acceptable successor until the end of the test.
		c.updateCluster(cluster, func(got *MultigresCluster) {
			if i == 0 {
				fixSidecarImage(got)
			}
			pools := got.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools
			pool := pools[resolver.DefaultPoolName]
			pool.PgBackRestResources = tc.resources
			pools[resolver.DefaultPoolName] = pool
		})
		c.Eventually(60*time.Second, tc.name+": the replacement to be refused", func() error {
			cond, shardPhase, clusterPhase, err := refusal()
			if err != nil {
				return err
			}
			if cond == nil || cond.Status != metav1.ConditionFalse ||
				!strings.Contains(cond.Message, tc.complaint) {
				return fmt.Errorf("PodTemplateAccepted is %+v", cond)
			}
			if shardPhase != api.PhaseDegraded || clusterPhase != api.PhaseDegraded {
				return fmt.Errorf("shard is %q and cluster is %q", shardPhase, clusterPhase)
			}
			return nil
		})
		// The refusal must hold, and be quiet: the shard controller watches
		// its own Shard, so a refusal that rewrote status on each pass
		// would run the controller in a loop, at tens of writes a second.
		// The first shape is watched for longer than the age at which a
		// stuck pod becomes replaceable. By the later ones the pod is past
		// that age, so any pass that would replace it does so at once.
		watch := 2 * time.Second
		if i == 0 {
			watch += sidecarStuckDeadline
		}
		stop := refusalChanges()
		c.requirePodKept(stuck, watch, tc.name+": the API server rejects its replacement")
		changes := stop()
		if changes.refusal != 0 {
			t.Errorf("%s: the refusal changed %d times while it stood (other changes: %v)",
				tc.name, changes.refusal, changes.other)
		} else if len(changes.other) > 0 {
			t.Logf("%s: the refusal stood unchanged; other status changes: %v",
				tc.name, changes.other)
		}
		pod := &corev1.Pod{}
		c.NoError(c.Get(client.ObjectKey{Namespace: c.NS, Name: stuck.Name}, pod))
		c.Eq("", pod.Annotations[metadata.AnnotationDrainState],
			tc.name+": a pod whose replacement is rejected must not be drained")
	}

	c.updateLifecyclePool(cluster.Name, func(pool *PoolSpec) {
		pool.PgBackRestResources = corev1.ResourceRequirements{}
	})
	c.Eventually(60*time.Second, "the stuck pod "+stuck.Name+" to be replaced", func() error {
		pod := &corev1.Pod{}
		if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: stuck.Name}, pod); err != nil {
			return err
		}
		if pod.UID == stuck.UID {
			return fmt.Errorf("pod %s is still the original %s", stuck.Name, stuck.UID)
		}
		if got := containerImage(pod, sidecarName); got != fixedSidecarImage {
			return fmt.Errorf("replacement runs sidecar image %q", got)
		}
		cond, _, _, err := refusal()
		if err != nil {
			return err
		}
		if cond != nil {
			return fmt.Errorf("PodTemplateAccepted is still %+v", cond)
		}
		return nil
	})
}
