package suite

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// poolPods reads every pool Pod in this case's namespace.
func (c *C) poolPods() []corev1.Pod {
	c.Helper()
	pods := &corev1.PodList{}
	c.NoError(c.List(
		pods,
		client.MatchingLabels{metadata.LabelAppComponent: shardcontroller.PoolComponentName},
	), "list pool pods")
	return pods.Items
}

// poolerDataReadyStatus reads a pool pod's PoolerDataReady condition, or the
// zero value if it has not been set yet.
func poolerDataReadyStatus(pod corev1.Pod) (corev1.ConditionStatus, string) {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == shardcontroller.PoolerDataReadyCondition {
			return cond.Status, cond.Reason
		}
	}
	return "", ""
}

// pokeShard makes a no-op-to-the-spec write to key's Shard, to trigger one
// reconcile of it.
//
// A shard that has already settled Healthy self-requeues nowhere: nothing in
// this package sets a manager SyncPeriod, matching production
// (cmd/multigres-operator/main.go does not either), so a fully idle shard
// sits until something touches an object it watches. Flipping the topology
// fake's call-failure mode is exactly such a silent change: it happens
// entirely inside this test process, with no Kubernetes write behind it, so
// without this poke the shard controller would never notice and this test
// would time out having exercised nothing.
//
// One poke is enough for both halves of this test. Once the poked reconcile
// hits the failing topology calls, reconcilePosture's own unsettled-posture
// backoff (postureDebounceRequeueDelay, then readinessBackoffDelay) keeps the
// shard re-checking on its own, same as it already does for any other
// not-yet-converged shard; this suite's interceptor clamps every RequeueAfter
// to RequeueClamp, so the same self-sustaining loop is what later notices
// topo.SetCallsUnavailable's release.
func (c *C) pokeShard(key client.ObjectKey) {
	c.Helper()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard to poke")
	if shard.Annotations == nil {
		shard.Annotations = map[string]string{}
	}
	shard.Annotations["multigres.com/staleready-test-poke"] = time.Now().Format(time.RFC3339Nano)
	c.NoError(c.Update(shard), "poke shard to trigger a reconcile")
}

// TestShardReadinessFailsClosedOnTopologyOutage pins the fix for the other
// half of the readiness-staleness gap: a topology outage must not let a pool
// pod's readiness stay published forever on whatever it last observed.
//
// A settled healthy shard's topology store keeps dialing successfully (real
// topology clients are lazy about this exactly the way
// topo.SetCallsUnavailable is: the store handle stays valid, and it is the
// calls against it that fail), so this uses the call-failure mode rather
// than SetUnavailable, matching what a live etcd outage actually looks like
// to the shard controller. Every pool pod must have PoolerDataReady pinned to
// Unknown in bounded time, rather than staying True for the life of the
// outage or going to a fabricated False. The
// budget below is generous, not a claim about any specific requeue delay:
// this suite runs every test's reconciles through one worker per controller
// shared across the whole package, so under full-suite load a namespace's
// turn can be queued behind a dozen others' convergence, the same contention
// TestShardLifecycle documents. Once topology returns, the next observation
// must flip it back. This must fail on the pre-fix base.
func TestShardReadinessFailsClosedOnTopologyOutage(t *testing.T) {
	c := newCase(t)
	ns := c.NS

	cluster := c.MinimalCluster("staleready")
	c.WaitForClusterHealthy(cluster)

	pods := c.poolPods()
	if len(pods) == 0 {
		t.Fatal("expected at least one pool pod once the cluster is healthy")
	}
	for _, pod := range pods {
		status, reason := poolerDataReadyStatus(pod)
		if status != corev1.ConditionTrue {
			t.Fatalf(
				"pod %s PoolerDataReady = %s/%s before the outage, want True",
				pod.Name, status, reason,
			)
		}
	}

	restore := topo.SetCallsUnavailable(ns)
	c.pokeShard(c.shardKey())
	c.Eventually(
		60*time.Second,
		"every pool pod to fail closed while topology is unavailable",
		func() error {
			for _, pod := range c.poolPods() {
				status, reason := poolerDataReadyStatus(pod)
				if status != corev1.ConditionUnknown || reason != "ObservationUnavailable" {
					return fmt.Errorf(
						"pod %s PoolerDataReady = %s/%s, want Unknown/ObservationUnavailable",
						pod.Name, status, reason,
					)
				}
			}
			return nil
		},
	)

	restore()
	c.Eventually(
		60*time.Second,
		"every pool pod to regain PoolerDataReady once topology returns",
		func() error {
			for _, pod := range c.poolPods() {
				status, reason := poolerDataReadyStatus(pod)
				if status != corev1.ConditionTrue {
					return fmt.Errorf(
						"pod %s PoolerDataReady = %s/%s, want True",
						pod.Name, status, reason,
					)
				}
			}
			return nil
		},
	)

	c.RequireQuiescent(5*time.Second, 60*time.Second)
}
