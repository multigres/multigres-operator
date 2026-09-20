package suite

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"github.com/multigres/testkit/ctrltest"
)

// shardProbeAnnotation is a key neither controller's applied payload ever
// mentions: tablegroup's BuildShard only sets an annotation map at all when the
// TableGroup carries a project-ref annotation, which MinimalCluster's fixture
// never does. Writing it from the test is therefore a mutation neither
// manager's SSA apply will contend with or revert, and it is the only way this
// test can put a fresh, known-real change on the Shard once the cluster has
// converged, since a converged tablegroup no longer gets re-triggered by its own
// stale watch.
const shardProbeAnnotation = "scenario-race-test.multigres.com/probe"

// TestTwoControllersWriteOneShard names the two-writer relationship between the
// shard and tablegroup controllers on one object, which every other test in
// this package runs past without seeing: each of them boots the suite with
// every reconciler live, but none asserts anything about a Shard being written
// by both.
//
// The relationship was measured, not assumed: before a fix, the shard
// controller wrote its own status about ten times a second forever, and while
// that ran, the tablegroup controller issued 3,641 patches against the Shard in
// three minutes, every one a no-op. Fixing the hot loop did not remove the
// two-writer relationship, only the churn it used to cause, so it is still
// worth a name.
func TestTwoControllersWriteOneShard(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("race")

	c.WaitForClusterHealthy(cluster)
	key := c.shardKey()

	t.Run("shard and tablegroup both write the Shard", func(t *testing.T) {
		c := c.Sub(t)
		writers := map[string]bool{}
		for _, op := range Suite.Ops.OpsInNamespace(c.NS) {
			if ctrltest.KindSuffix(op.Kind) == "Shard" && op.Key.Name == key.Name {
				writers[op.Controller] = true
			}
		}
		for _, want := range []string{"shard", "tablegroup"} {
			c.Check().True(writers[want],
				"the %s controller has no recorded write to Shard %s; writers seen: %v",
				want, key.Name, writers)
		}
	})

	// Runs before the no-op check below, which mutates the Shard: a manager
	// entry left over from that mutation would be a false-positive third writer
	// and would blur what this is actually about, which is the two production
	// managers.
	t.Run("field ownership on the Shard is disjoint", func(t *testing.T) {
		c := c.Sub(t)
		// Several Shard status writes carry no field owner, so the API server
		// attributes them to the manager that happens to be the process name,
		// and that manager ends up co-owning fields the shard controller's own
		// applier claims. Retire this pin with an explicit owner on every
		// status write, and replace it with c.Empty on the conflicts.
		conflicts := c.fieldOwnershipConflicts(key)
		c.KnownDefect("MGO-SHARD-STATUS-WRITES-NO-FIELD-OWNER", func() error {
			if len(conflicts) == 0 {
				return nil
			}
			return fmt.Errorf(
				"Shard %s has fields claimed by more than one field manager, "+
					"the same shape of defect as the status hot loop:\n  %s",
				key.Name, strings.Join(conflicts, "\n  "))
		})
	})

	t.Run("tablegroup's patches to the Shard never change it", func(t *testing.T) {
		c.Sub(t).requireTableGroupPatchesAreNoOps(key)
	})
}

// fieldOwnershipConflicts returns every field path on the Shard that more
// than one field manager claims, sorted. An empty result is the invariant, and
// it is the check that generalises: it would have caught the status hot-loop
// directly; the loop was two field managers each asserting a value for the
// same field, ObservedGeneration or a phase, and the API server obliging both.
// A dump of writes only shows that as churn; managedFields shows it as the
// overlap it is.
//
// Field ownership is expressed by the API server as a Set per manager, encoded
// in metadata.managedFields[].fieldsV1: this decodes each manager's Set with
// the same library the server itself uses and collects every field path that
// is a member of more than one.
func (c *C) fieldOwnershipConflicts(key client.ObjectKey) []string {
	c.Helper()

	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard %s", key)

	claimants := map[string][]string{}
	for _, mf := range shard.ManagedFields {
		if mf.FieldsV1 == nil {
			continue
		}
		set := &fieldpath.Set{}
		c.NoError(set.FromJSON(mf.FieldsV1.GetRawReader()),
			"decode managedFields for manager %s", mf.Manager)
		set.Iterate(func(p fieldpath.Path) {
			path := p.String()
			claimants[path] = append(claimants[path], mf.Manager)
		})
	}

	// A Shard with no decodable managedFields claim at all would make the
	// disjointness check below pass having compared nothing, which is the one
	// way this assertion can go green without the invariant holding. Nothing in
	// this function's own reasoning rules that out, so it is asserted here
	// rather than inherited from the writer check above.
	c.True(len(claimants) > 0,
		"Shard %s yielded no decodable managedFields claims, so field ownership "+
			"was not checked at all; %d managedFields entries were present",
		key.Name, len(shard.ManagedFields))

	var conflicts []string
	for path, managers := range claimants {
		distinct := map[string]bool{}
		for _, m := range managers {
			distinct[m] = true
		}
		if len(distinct) <= 1 {
			continue
		}
		names := make([]string, 0, len(distinct))
		for m := range distinct {
			names = append(names, m)
		}
		sort.Strings(names)
		conflicts = append(conflicts, fmt.Sprintf("%s claimed by %v", path, names))
	}

	sort.Strings(conflicts)
	return conflicts
}

// requireTableGroupPatchesAreNoOps is assertion 2 from the brief, corrected to
// the harness as it exists now rather than as it was written: convergence is
// about 0.3s and requeues are compressed, so a fixed wall-clock window no
// longer separates from scheduling noise. It also has nothing to observe by the
// time a test could get around to sleeping: once the cluster is Healthy,
// tablegroup's own SSA patches to the Shard stop generating new watch events
// (a true no-op patch does not bump resourceVersion, so Owns(&Shard{}) never
// re-fires), so tablegroup simply stops reconciling and there is no ten-second
// window in which anything would happen anyway.
//
// So instead of waiting, this drives the relationship directly: it makes a
// small, known write of its own to the Shard (an annotation neither manager's
// apply payload mentions, see shardProbeAnnotation) to produce a fresh watch
// event, then waits for a COMPLETE tablegroup reconcile pass that both began
// after that write and wrote this Shard (see awaitTableGroupPassAfter), read
// from Suite.Reconciles rather than the raw op cursor. That matters: a
// reconcile record is only appended once the whole pass has returned, so
// waiting on it cannot observe half a pass the way a plain op-log cursor can,
// where a later step of the very pass that just satisfied the wait
// (tablegroup's own status-patch, a step after the Shard patch) can still land
// and be mistaken for the next pass's write.
//
// The no-op check itself compares content, not resourceVersion. The shard
// controller reconciles this same object roughly every clamp interval even at
// steady state (see ctrltest.RequeueClamp), so by the time tablegroup's pass
// has been observed, some other write to the Shard has almost always also
// landed in the same rough window; attributing a resourceVersion move to
// "whichever controller wrote most recently" is exactly as unsound as the
// wall-clock method this replaces; it was tried and produced a false pass
// under mutation (see the report). tablegroup's SSA apply payload is a
// complete statement of what it owns: spec, labels and ownerReferences,
// never status (see BuildShard), so comparing that payload's own fields
// before and after the pass answers the question directly and is immune to
// any concurrent, status-only write from the shard controller, no matter how
// often it fires.
//
// Repeated several times rather than once, since a single pass proves nothing
// about whether "never" holds.
func (c *C) requireTableGroupPatchesAreNoOps(key client.ObjectKey) {
	c.Helper()

	const passesToObserve = 5
	for i := range passesToObserve {
		before := &Shard{}
		c.NoError(c.Get(key, before), "get shard %s", key)

		probedFrom := c.probeShard(key, i)
		pass := awaitTableGroupPassAfter(c, c.NS, key, probedFrom)

		after := &Shard{}
		c.NoError(c.Get(key, after), "get shard %s", key)

		c.True(equality.Semantic.DeepEqual(before.Spec, after.Spec),
			"tablegroup's pass %s changed Shard %s's spec, the field surface its "+
				"SSA apply owns:\n  before: %+v\n  after:  %+v",
			pass, key.Name, before.Spec, after.Spec)
		c.True(
			equality.Semantic.DeepEqual(before.OwnerReferences, after.OwnerReferences),
			"tablegroup's pass %s changed Shard %s's ownerReferences:\n  before: %+v\n  after:  %+v",
			pass,
			key.Name,
			before.OwnerReferences,
			after.OwnerReferences,
		)
		c.True(equality.Semantic.DeepEqual(before.Labels, after.Labels),
			"tablegroup's pass %s changed Shard %s's labels:\n  before: %+v\n  after:  %+v",
			pass, key.Name, before.Labels, after.Labels)
	}
}

// awaitTableGroupPassAfter blocks until the tablegroup controller has completed
// a reconcile pass that started after notBefore and wrote the Shard at key, and
// returns that pass.
//
// The bar is Reconcile.Start measured against an instant captured BEFORE the
// probe write was issued, and both halves of that were arrived at by measuring
// a wrong version of it.
//
// Start, rather than a position in the log, because a record is appended when
// its pass RETURNS. A pass already in flight when the probe lands is therefore
// filed after the probe while having begun before it, so an index cursor
// admits it however freshly it was seeded. Five controllers converge this
// cluster before the first probe and leave dozens of finished passes behind,
// and a scan from index zero matched those exclusively: every iteration was
// satisfied by a pass that had started roughly half a second before the probe
// it was supposed to be reacting to.
//
// Before the write rather than after it, because a write becomes visible to
// watchers when the API server commits it, which is strictly before the
// client's own call returns. The gap is small but it is on the wrong side: the
// reacting tablegroup pass starts within a few tenths of a millisecond of the
// probe Patch returning, and measurably often starts just before it. A mark
// taken after the write returned then rejects the very pass it is waiting for,
// and since the probe is the only thing that writes this Shard once the cluster
// has converged, no later pass arrives to replace it. The wait cannot then
// succeed at any timeout, which is what made it fail about one run in three. A
// mark taken before the write has no such edge, because no pass can react to a
// write that has not been issued yet.
//
// The cost of moving the mark earlier is one API round trip of slack, in which a
// pass that did not see the probe would be accepted. That is bounded by a round
// trip instead of by the whole log, and at steady state nothing but this test
// writes the Shard, so the only candidate is a second pass caused by the
// previous iteration's probe. That pass has already been observed to completion
// before this iteration's mark is taken.
//
// Rescanning the namespace's whole log on each poll, rather than carrying a
// cursor across polls or across iterations, is deliberate for a related reason:
// the log is ordered by completion, not by start, so an index cursor and a
// start-time bar disagree about which entries are still candidates, and the
// cursor is the one that can step over the pass being waited for. The scan is
// bounded by one namespace's history and runs at most once per poll, so the
// cost of being right here is nothing worth optimising.
//
// No bookkeeping is needed to stop one iteration matching an earlier
// iteration's pass: that pass had already returned, and so had already started,
// before this iteration's mark was taken.
func awaitTableGroupPassAfter(
	c *C,
	ns string,
	key client.ObjectKey,
	notBefore time.Time,
) ctrltest.Reconcile {
	c.Helper()

	var pass ctrltest.Reconcile
	what := fmt.Sprintf("a tablegroup pass on Shard %s beginning after the probe", key.Name)
	c.Eventually(10*time.Second, what, func() error {
		for _, r := range Suite.Reconciles.InNamespace(ns) {
			if r.Controller != "tablegroup" || !r.Start.After(notBefore) {
				continue
			}
			for _, op := range Suite.Reconciles.Ops(r) {
				if ctrltest.KindSuffix(op.Kind) == "Shard" && op.Key.Name == key.Name {
					pass = r
					return nil
				}
			}
		}
		return fmt.Errorf(
			"no tablegroup pass touching Shard %s has begun since the probe write",
			key.Name,
		)
	})
	return pass
}

// probeShard sets a test-owned annotation on the Shard to a fresh value and
// returns the instant just before that write was issued, which is the latest
// mark a pass reacting to it is guaranteed to start after.
//
// Returning the instant the write completed is the obvious choice and is the
// wrong one: the API server commits the write and dispatches the watch event
// before the client's Patch call returns, so the reacting pass is often already
// running by then. See awaitTableGroupPassAfter.
//
// Neither field manager's apply payload mentions the annotation (tablegroup's
// BuildShard only sets an annotation map at all when the TableGroup carries a
// project-ref annotation, which MinimalCluster's fixture never does), so it is
// a change neither manager's SSA apply will contend with or revert. It is a
// merge patch rather than a full Update, so it carries no resourceVersion
// precondition and cannot spuriously conflict with either controller's own
// concurrent write to the same object. Writing it is the only way this test
// can put a fresh, real change on the Shard once the cluster has converged,
// since a converged tablegroup no longer gets re-triggered by its own stale
// watch (see requireTableGroupPatchesAreNoOps).
func (c *C) probeShard(key client.ObjectKey, seq int) time.Time {
	c.Helper()
	shard := &Shard{}
	c.NoError(c.Get(key, shard), "get shard %s", key)
	base := shard.DeepCopy()
	if shard.Annotations == nil {
		shard.Annotations = map[string]string{}
	}
	shard.Annotations[shardProbeAnnotation] = fmt.Sprintf("%d", seq)

	issued := time.Now()
	c.NoError(
		c.Patch(shard, client.MergeFrom(base)),
		"annotate shard %s",
		key,
	)
	return issued
}
