package suite

import (
	"fmt"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/ctrltest"
)

// certBootstrapBudget is how long a test will wait for the shard controller to
// finish generating pgBackRest's CA and server certificates.
//
// It is the one wait in this package deliberately larger than 30s, and it is
// sized against the step it waits on rather than against convergence.
// reconcilePgBackRestCerts generates two RSA keys, which is the most expensive
// thing this suite does and by far the most variable under -race: measured at
// 14s in one run of the suite and 75s in another, on the same machine. A
// number sized for the 14s case turns the 75s case into a red suite that says
// nothing about the operator.
const certBootstrapBudget = 2 * time.Minute

// waitForShardPastPKI blocks until the shard controller has written a pool Pod
// in ns, which is the evidence this suite has that the controller is past
// certificate generation.
//
// It exists to keep a slow precondition out of an assertion's budget. The
// shard reconciles fourteen steps in a fixed order: the pgBackRest certificate
// step is fifth, reconcilePool is twelfth, and reconcileDataPlane, the only
// step that returns the pooler-registration requeue, is last. A test that
// starts its requeue budget before the keys exist is timing RSA keygen, and it
// goes red when the crypto was slow rather than when the operator was wrong.
// Split in two, each wait is sized against what it actually waits for.
//
// A pool Pod is the signal rather than the certificate Secrets themselves for
// two reasons. It sits between the two steps that matter, so it proves the
// keygen is behind us without depending on it being the immediately preceding
// step. And a Shard creating Pods for its pools is a more stable fact than the
// name the operator builds its Secrets from, which a test has no business
// knowing. It is read from the recorder rather than from the apiserver so that
// what is observed is the shard controller having written, not an object that
// something else could have created.
func (c *C) waitForShardPastPKI() {
	c.Helper()
	c.Eventually(
		certBootstrapBudget,
		"the shard controller to get past certificate generation",
		func() error {
			for _, op := range Suite.Ops.OpsInNamespace(c.NS) {
				if op.Controller == "shard" && ctrltest.KindSuffix(op.Kind) == "Pod" {
					return nil
				}
			}
			return fmt.Errorf("the shard controller has written no pool Pod yet")
		},
	)
}

// holdPoolerRegistration keeps every pooler in this case's namespace out of
// the topology store for the rest of the test.
//
// The shard controller only asks for its one-minute requeue when it finds no
// poolers registered at all. Left to the data-plane fake, which registers on
// a 500ms tick as soon as a pool pod exists, whether the shard ever sees that
// empty topology is a race: when the first pooler registers before the
// shard's first data-plane pass, the shard goes straight to "some poolers",
// skips the requeue these tests wait for, and on today's operator returns no
// requeue at all (MGO-POOL-SCALEUP-ROLE-STALE). Measured at 1 run in 20 under
// full-suite load. Holding registration makes the empty topology the only
// state the shard can see.
func (c *C) holdPoolerRegistration() {
	c.Helper()
	c.Cleanup(poolers.HoldRegistrations(c.NS))
}

// awaitShard waits for the cluster controller to create this namespace's one
// Shard and returns it.
//
// Exactly one, not at least one. Both polls this replaces went on to read
// Items[0], and the weaker form picked element zero out of a set whose size
// it never checked. Every fixture in this package declares a single
// ShardConfig and suite_test.go asserts that, so the stronger claim is true
// today and costs nothing to state.
//
// It is what happens when that stops being true that decides it. Sharding is
// this project's whole point, so a multi-shard fixture is a matter of time,
// and at that moment "at least one" stays green while silently asserting
// about whichever Shard the API server happened to return first. "Exactly
// one" fails saying it got two, which points at the fixture that changed. A
// test that wants a particular shard out of several should name it rather
// than index into a list.
func (c *C) awaitShard() Shard {
	c.Helper()
	var shard Shard
	c.Eventually(30*time.Second, "the cluster's one Shard to exist", func() error {
		shards := &ShardList{}
		if err := c.List(shards); err != nil {
			return err
		}
		if len(shards.Items) != 1 {
			return fmt.Errorf("want exactly one Shard, got %d", len(shards.Items))
		}
		shard = shards.Items[0]
		return nil
	})
	return shard
}

// shardKey is awaitShard's key, for the callers that only need to address it.
func (c *C) shardKey() client.ObjectKey {
	c.Helper()
	shard := c.awaitShard()
	return client.ObjectKeyFromObject(&shard)
}

// TestShardAsksForAMinuteAwaitingPoolerRegistration is the assertion that
// replaces waiting a minute for the same information, and the reason requeue
// compression does not hide the defect it compresses.
//
// While no multipooler has registered in the topology store the shard
// controller asks to be woken in a minute. Nothing in Kubernetes watches that
// store, so in production nothing can wake it sooner, and before compression
// every convergence test in this package paid that minute whenever the last pod
// event happened to land before the data plane fake registered its poolers.
// Which test paid was a coin flip and the suite's wall time swung by a minute
// between runs for no visible reason.
//
// Compressed, the poll comes back in 50ms and the minute survives here as a
// fact about the operator. Fixing it is not this suite's job, and this
// assertion is what should fail when somebody does fix it.
//
// This test asserts about the operator and nothing else. That the suite clamps
// what it saw here is a fact about the harness and belongs to
// TestSuiteCompressesRequeues, so that a clamp change reports itself as a
// clamp change rather than as the shard controller's polling having moved.
func TestShardAsksForAMinuteAwaitingPoolerRegistration(t *testing.T) {
	c := newCase(t)
	c.holdPoolerRegistration()
	c.MinimalCluster("requeue")

	key := c.shardKey()
	c.waitForShardPastPKI()

	got := Suite.Reconciles.WaitForRequeue(t, "shard", key, time.Minute, 30*time.Second)

	// Exactly a minute, not merely at least a minute. One minute is the shard
	// controller's only requeue of that length (poolerRegistrationRetryDelay
	// in reconcile_data_plane.go), so the duration identifies the code path
	// that the reconcile boundary itself cannot: ctrl.Result carries no
	// reason, and the AwaitingPoolerRegistration reason lives on the shard's
	// PostureConsistent condition, which by the time a test can read it has
	// usually already moved on.
	c.Check().Eq(time.Minute, got.RequestedAfter, "the shard's requeue duration")
}

// TestSuiteCompressesRequeues is the canary on the harness half of the
// bargain, and it is live rather than a unit test for a reason the unit tests
// cannot cover: TestInterceptorCompressesRequeue proves that an interceptor
// clamps, not that this suite wired one around the real controllers. If that
// wiring is ever broken, compression dies silently, every timeout in the
// package regains its old "maybe it is just waiting" ambiguity, and nothing
// fails except runtimes nobody reads.
//
// It deliberately does not name a duration the operator chose. Any requeue
// longer than the clamp will do, so that fixing the shard controller's minute
// changes what this test observes but not whether it passes.
func TestSuiteCompressesRequeues(t *testing.T) {
	c := newCase(t)
	c.holdPoolerRegistration()
	c.MinimalCluster("clamp")

	key := c.shardKey()
	c.waitForShardPastPKI()

	got := Suite.Reconciles.WaitForRequeue(t, "shard", key, 2*ctrltest.RequeueClamp, 30*time.Second)

	c.Check().Eq(ctrltest.RequeueClamp, got.Result.RequeueAfter,
		"controller-runtime's requeue-after duration")
	c.Check().
		True(got.Compressed(), "Compressed() = false on a pass that asked for %s", got.RequestedAfter)
}
