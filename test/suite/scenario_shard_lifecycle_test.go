package suite

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
	"github.com/multigres/testkit/ctrltest"
)

const lifecycleShardName multigresv1alpha1.ShardName = "0-inf"

// blockedObservationWindow is how long steps 5 and 6 watch for a reaction
// that never comes.
//
// The shard controller requeues every disruptionRecoveryRequeue (5s,
// pkg/resource-handler/controller/shard/disruption.go:17) for as long as a
// disruption is refused, so a window several times that long is the
// difference between "the operator tried repeatedly and refused every time"
// and "the operator had not got round to trying yet". A Quiet() step on its
// own asserts silence only over the 250ms settle window, which for this
// question would be almost nothing.
const blockedObservationWindow = 20 * time.Second

// lifecycleShardRef is a Shard carrying only the fields BuildPoolPodName,
// BuildPoolDataPVCName and BuildSharedBackupPVCName read: the cluster label
// and the three spec names. Those four values are known before the real
// Shard exists, since lifecycleCluster (below) chooses them, which lets the
// test predict a pod or PVC's name ahead of the create that produces it,
// using the operator's own name builders rather than a hand-rolled format
// string. It is never sent to the API server.
func lifecycleShardRef(clusterName string) *Shard {
	return &Shard{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{metadata.LabelMultigresCluster: clusterName},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   resolver.DefaultSystemDatabaseName,
			TableGroupName: resolver.DefaultSystemTableGroupName,
			ShardName:      lifecycleShardName,
		},
	}
}

// lifecycleStorageClassName is the StorageClass lifecycleCluster's pool
// references, distinct per namespace so concurrently-running instances of
// this test never collide on the same cluster-scoped object.
func lifecycleStorageClassName(ns string) string {
	return ns + "-lifecycle-expandable"
}

// lifecycleCluster creates the same MultigresCluster MinimalCluster does
// (one cell, one database, one table group, one shard), plus a StorageClass
// with AllowVolumeExpansion set and the pool's Storage.Class pointed at it.
//
// Two facts force this rather than a plain call to MinimalCluster. First,
// PopulateClusterDefaults's own injection of Databases is commented
// "in-memory" for a reason: with no mutating webhook running in this suite,
// nothing ever writes it back to the MultigresCluster object itself, every
// reconcile recomputes it from scratch, and cluster.Spec.Databases stays
// permanently empty on the server unless a caller sets it explicitly. That
// alone would still allow starting from MinimalCluster's bare cluster and
// seeding Databases later, in updateLifecyclePool's first call. But second,
// storageClassName is immutable once a PersistentVolumeClaim exists, and step
// 3 needs the pool's existing data PVCs to already reference a StorageClass
// that allows expansion, or the API server's resize admission check refuses
// the request outright ("only dynamically provisioned pvc can be resized").
// So the StorageClass has to be in place, and referenced, from this create.
//
// The shape mirrors what resolver.PopulateClusterDefaults would have
// injected for a MinimalCluster (one pool, one cell, the two-replica floor
// pkg/resolver/shard.go computes for a single-cell pool): every attribute
// this script mutates is still reached by changing a cluster that already
// exists, this just makes explicit at creation the one attribute (storage
// class) that cannot be introduced by a later mutation.
func (c *C) lifecycleCluster(clusterName string) *MultigresCluster {
	c.Helper()

	scName := lifecycleStorageClassName(c.NS)
	allowExpansion := true
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: scName},
		Provisioner:          "multigres-test/no-op",
		AllowVolumeExpansion: &allowExpansion,
	}
	c.NoError(c.Create(sc), "create StorageClass %s", scName)
	// Cluster-scoped, so the per-test namespace delete does not reach it and
	// every run would otherwise leave one behind in the shared envtest API
	// server for the life of the package. context.Background rather than
	// c.Context, which is already cancelled by the time cleanups run.
	c.Cleanup(func() {
		_ = c.Client().Delete(context.Background(), sc)
	})

	return c.newCluster(clusterName, func(s *MultigresClusterSpec) {
		s.Databases = []DatabaseConfig{{
			Name:    resolver.DefaultSystemDatabaseName,
			Default: true,
			TableGroups: []TableGroupConfig{{
				Name:    resolver.DefaultSystemTableGroupName,
				Default: true,
				Shards: []ShardConfig{{
					Name: lifecycleShardName,
					Spec: &ShardInlineSpec{
						Pools: map[PoolName]PoolSpec{
							resolver.DefaultPoolName: {
								Type:            "readWrite",
								Cells:           []CellName{defaultSimCell},
								ReplicasPerCell: ptr.To(int32(2)),
								Storage:         multigresv1alpha1.StorageSpec{Class: scName},
							},
						},
					},
				}},
			}},
		}}
	})
}

// updateLifecyclePool re-reads the cluster and applies mutate to the
// "default" pool's spec, retrying on a conflict from a concurrent status
// write. The cluster's own status subresource is patched by the
// multigrescluster controller on a completely separate write path, but any
// spec Update still carries the resourceVersion it read, so a status patch
// landing between our Get and our Update aborts it.
func (c *C) updateLifecyclePool(
	clusterName string,
	mutate func(*PoolSpec),
) {
	c.Helper()
	key := client.ObjectKey{Namespace: c.NS, Name: clusterName}
	for {
		cluster := &MultigresCluster{}
		c.NoError(c.Get(key, cluster), "get cluster %s", clusterName)
		pools := cluster.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools
		pool := pools[resolver.DefaultPoolName]
		mutate(&pool)
		pools[resolver.DefaultPoolName] = pool

		err := c.Update(cluster)
		if err == nil {
			return
		}
		if !apierrors.IsConflict(err) {
			c.Fatalf("update cluster %s: %v", clusterName, err)
		}
	}
}

// waitFor is eventually without the t.Fatalf, returning the last error instead.
//
// A KnownDefect body has to be able to contain a wait: KnownDefect reads a
// returned error as the pinned defect still being live, and eventually ends the
// goroutine through t.Fatalf rather than returning, so a pin whose subject is
// "this never happens" cannot be written with eventually at all.
func waitFor(timeout time.Duration, cond func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := cond()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// drainStateSeen reports nil once any pod in ns carries the drain state
// machine's annotation, and an error while none does.
//
// This is the first write a drain makes: initiateDrain patches the annotation
// to Requested (pkg/data-handler/drain/drain_helpers.go) before anything else
// moves, and ExecuteDrainStateMachine advances it one state per reconcile from
// there. So the annotation's mere presence is the earliest observable evidence
// that a drain started, which is exactly what step 2's pin is about, and unlike
// a count of pod events it does not depend on how many reconciles the operator
// took to get anywhere.
func (c *C) drainStateSeen() error {
	c.Helper()
	pods := &corev1.PodList{}
	if err := c.List(pods); err != nil {
		return err
	}
	for i := range pods.Items {
		if _, ok := pods.Items[i].Annotations[metadata.AnnotationDrainState]; ok {
			return nil
		}
	}
	return fmt.Errorf("no pod in %s carries %s", c.NS, metadata.AnnotationDrainState)
}

// poolPodFingerprints maps every pod in ns to its UID and resourceVersion.
//
// Comparing two of these across a window is how this file asserts that the
// operator left the pods alone, and it replaces what a Quiet() step used to say
// about pods before pods came out of the script's watch entirely (see the
// script's own comment in TestShardLifecycle). It is not the weaker claim:
// resourceVersion is monotonic per object and moves on every write the API
// server accepts, so any patch at all, by the operator or by the data-plane
// fake, shows up as a changed fingerprint. A pod created or deleted in the
// window changes the key set, and a delete-and-recreate at the same name
// changes the UID. What it does not do is care when any of that happened, which
// is the whole reason to read pods rather than watch them: the claim is about
// the operator's writes and not about the fake's pacing.
func (c *C) poolPodFingerprints() map[string]string {
	c.Helper()
	pods := &corev1.PodList{}
	c.NoError(c.List(pods), "list pods in %s", c.NS)
	out := make(map[string]string, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		out[p.Name] = string(p.UID) + "@" + p.ResourceVersion
	}
	return out
}

// podFingerprintDiff describes how two poolPodFingerprints snapshots differ, or
// returns "" when they are identical. Sorted, so a failure message is the same
// text on every run rather than whatever order the map iterated in.
func podFingerprintDiff(before, after map[string]string) string {
	var notes []string
	for name, was := range before {
		now, ok := after[name]
		switch {
		case !ok:
			notes = append(notes, fmt.Sprintf("%s was deleted", name))
		case now != was:
			notes = append(notes, fmt.Sprintf("%s was written (%s to %s)", name, was, now))
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			notes = append(notes, fmt.Sprintf("%s was created", name))
		}
	}
	slices.Sort(notes)
	return strings.Join(notes, "; ")
}

// firstCreateIndex returns the position in ops of controller's first accepted
// create of kind/name, and whether it made one.
//
// This is how step 4's ordering claim survives pods leaving the script's watch.
// The op log's order is arrival at the recorder's mutex, which recorder.go is
// explicit is program order within one controller and not a causal order across
// controllers, so two indexes are only comparable when both name the same
// controller. Both creates here are the shard controller's, in one pass of
// createMissingResources, which is what makes the comparison sound and is why
// the controller is a parameter rather than left implicit.
func firstCreateIndex(ops []ctrltest.Op, controller, kind, name string) (int, bool) {
	for i, op := range ops {
		if op.Controller == controller && op.Verb == "create" &&
			ctrltest.KindSuffix(op.Kind) == kind && op.Key.Name == name {
			return i, true
		}
	}
	return 0, false
}

// podRoleViolation reports whether err is one of the errors MembersOf returns
// about what status.podRoles actually says, as opposed to a transient "not
// reconciled yet" state (shard not found, status.podRoles still empty) that
// the standing invariant below must not treat as a violation.
//
// The unrecognized-role error has to count, not just the two primary-count
// ones. MembersOf returns it from its classification loop, before it counts
// primaries at all, so a snapshot holding one pod in a role this suite does
// not know (a future DRAINED, say) and two pods reporting PRIMARY comes back
// as the unrecognized-role error alone. Reading that as "not a violation"
// would wave the two primaries through, which is the one thing the invariant
// exists to catch.
//
// Matching on message text is the only option MembersOf offers, since it
// exports no sentinel errors. That coupling is invisible from identity.go, so
// rewording a message there disables this check silently; closing it needs
// either sentinels in identity.go or a unit test pinning these strings, and
// both are outside this file.
func podRoleViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no pod has role PRIMARY") ||
		strings.Contains(msg, "more than one pod has role PRIMARY") ||
		strings.Contains(msg, "has unrecognized role ")
}

// rollingUpdateDrift reports whether the Shard says wantPods of its pool pods
// have drifted from their desired spec, through the RollingUpdate condition
// handleRollingUpdates writes (reconcile_pool_pods.go:735-751).
//
// This is the only object state the operator changes in response to a spec
// change it then refuses to act on: the drain it would start next is blocked
// before it writes anything to a pod, and the DisruptionBlocked Event that
// refusal records goes through client-go's per-object spam filter (burst 25,
// one refill per 300s), which a chatty Shard inside a short envtest run has
// already spent. Steps 5 and 6 need this because a step asserting that
// nothing happened asserts nothing at all unless the stimulus provably
// arrived first.
//
// The expected message is built from the same format string the operator
// uses, so this is coupled to that wording. The coupling is deliberate and
// fails in the safe direction: a reworded message makes the step fail loudly
// rather than quietly stop asserting.
func rollingUpdateDrift(key client.ObjectKey, wantPods int) error {
	shard := &Shard{}
	if err := Suite.Client.Get(context.Background(), key, shard); err != nil {
		return err
	}
	cond := meta.FindStatusCondition(shard.Status.Conditions, "RollingUpdate")
	if cond == nil {
		return fmt.Errorf("shard %s has no RollingUpdate condition yet", key)
	}
	want := fmt.Sprintf("%d pods need update in pool %s", wantPods, resolver.DefaultPoolName)
	if cond.Status != metav1.ConditionTrue || cond.Reason != "PodsDrifted" ||
		cond.Message != want {
		return fmt.Errorf(
			"shard %s reports RollingUpdate=%s reason=%s %q, want True PodsDrifted %q",
			key, cond.Status, cond.Reason, cond.Message, want,
		)
	}
	return nil
}

// lifecycleResources builds a concrete, distinguishable resource request/limit
// pair so successive calls with different label values are guaranteed to
// differ from both the resolver's own defaults and from each other.
func lifecycleResources(cpuReq, memReq, cpuLim, memLim string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpuReq),
			corev1.ResourceMemory: resource.MustParse(memReq),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpuLim),
			corev1.ResourceMemory: resource.MustParse(memLim),
		},
	}
}

// TestShardLifecycle starts from the smallest cluster this suite can
// converge (one pool, two pods, under the two-replica floor a single-cell
// pool always resolves to) and mutates it forward: resource change, storage
// change, scale up, a second resource change with three members present,
// and scale back down. Every attribute is reached by changing a cluster
// that already exists, never by constructing the end state, so this
// exercises the operator's transition paths rather than its defaulting
// paths.
func TestShardLifecycle(t *testing.T) {
	c := newCase(t)
	ns := c.NS
	const clusterName = "lifecycle"

	shardKey := client.ObjectKey{
		Namespace: ns,
		Name: name.JoinWithConstraints(
			name.DefaultConstraints,
			clusterName,
			string(resolver.DefaultSystemDatabaseName),
			string(resolver.DefaultSystemTableGroupName),
			string(lifecycleShardName),
		),
	}
	shardRef := lifecycleShardRef(clusterName)
	const cellName = defaultSimCell

	poolName := string(resolver.DefaultPoolName)
	pod0 := shardcontroller.BuildPoolPodName(shardRef, poolName, cellName, 0)
	pvc0 := shardcontroller.BuildPoolDataPVCName(shardRef, poolName, cellName, 0)
	pod1 := shardcontroller.BuildPoolPodName(shardRef, poolName, cellName, 1)
	pvc1 := shardcontroller.BuildPoolDataPVCName(shardRef, poolName, cellName, 1)
	backupPVC := shardcontroller.BuildSharedBackupPVCName(shardRef)

	// Step 1: converge, out of the script's sight, and then open the script
	// over what converging produced.
	//
	// lifecycleCluster mirrors what MinimalCluster plus the resolver's own
	// defaults would produce (one cell, one pool at the two-replica floor
	// pkg/resolver/shard.go computes for a single-cell pool under the default
	// AT_LEAST_2 durability policy), so "the smallest possible cluster"
	// already has a primary and a replica the moment it is healthy: pods 0
	// and 1, their data PVCs, and the shard's one shared backup PVC.
	//
	// The watch therefore opens after its own fixture, against this suite's
	// standing rule that a stream is established before the objects it
	// watches are created. That rule exists so a reaction to the create
	// cannot land before anyone is listening, and the events it protects here
	// are ones no assertion reads: the real claims of this script are steps 2
	// through 6, each about one mutation of a cluster that already exists,
	// and none of them looks at how the cluster got there.
	//
	// What the closed step it replaces did read was the convergence sequence
	// itself, and that count is not the operator's to keep. A step's
	// allow-list is an exact multiset with no "N events of this kind" form,
	// so a declared count is sound only where the code fixes it. Pod
	// readiness here is paced by the data-plane sim (pkg/ctrltest/datasim.go)
	// racing reconcilePoolerReadiness's own readiness-gate patch on the same
	// Pod, and measured over runs of this test a pod settles in three status
	// modifications most of the time and four sometimes, which failed step 1
	// on an unpermitted event about one run in five. A fourth speculative
	// Changed leaf would only move that boundary, since nothing bounds the
	// sequence at three or four either. Do not restore the closed step: it
	// pinned the harness, not the operator.
	//
	// RequireQuiescent is what stands in its place, and over the convergence
	// it is strictly the stronger claim: no projected state change on any of
	// watchedKinds() and no attempted write for five seconds, rather than a
	// count of Pod and PVC events. It is also what makes the baseline below
	// a fixed set rather than a race, because it establishes that nothing is
	// still moving when the watch opens.
	c.lifecycleCluster(clusterName)
	c.Eventually(60*time.Second, "shard to report Healthy", func() error {
		shard := &Shard{}
		if err := c.Get(shardKey, shard); err != nil {
			return err
		}
		if shard.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("shard phase is %q", shard.Status.Phase)
		}
		return nil
	})
	c.RequireQuiescent(5*time.Second, 60*time.Second)

	// PersistentVolumeClaim and not Pod, which is the general form of what
	// step 1 above ran into rather than a second workaround for it.
	//
	// A closed step counts events, so it is sound only over writes whose number
	// follows from the operator's code. Every PVC event in this namespace is
	// one of those: the operator creates each PVC once, patches it once per
	// resize, and the data-plane fake's bind is a single terminal transition
	// from Pending to Bound rather than a progression, so it too is one write
	// by construction. Pod status is the opposite. The fake walks a pod's
	// readiness conditions in however many passes it takes while
	// reconcilePoolerReadiness writes its readiness gate on the same object,
	// and the number of modifications that takes is a property of that race:
	// three most of the time, four often enough to fail one run in five,
	// measured here twice, at step 1 and again at step 4.
	//
	// Note what is not available as a middle road: keeping Pod in the watch
	// while declining to enumerate status modifications. There is no "N events
	// of this kind" form, so a watched kind's events must each be permitted by
	// name or they fail the step in flight. Watching pods therefore forces the
	// enumeration, which is why pods come out of the watch altogether and every
	// pod-level claim below is a direct read instead. Those reads are not the
	// weaker choice; see poolPodFingerprints for why a state comparison is at
	// least as strong here as an event count, and firstCreateIndex for how the
	// one genuine ordering claim is made from the operator's own write log.
	s := c.NewScript(&corev1.PersistentVolumeClaimList{})

	// Standing invariant: exactly one PRIMARY whenever status.podRoles is
	// non-empty, no pod in a role this suite does not know, and no pod
	// quarantined. MembersOf distinguishes those errors from every other read
	// failure (shard not found, podRoles still empty), and podRoleViolation is
	// what keeps one of those from being reported as a violation. The script
	// opens after convergence, so podRoles is populated by the time this is
	// registered, but steps 2 through 6 mutate the pool and the shard rewrites
	// podRoles as those land, so a read taken mid-rewrite is still ordinary.
	//
	// Quarantined is checked explicitly because it is its own bucket in
	// Members, disjoint from Replicas: a script that only ever asserted
	// primary-count and replica-count could watch a pod sit quarantined for
	// its entire length without ever naming it. This script never triggers
	// quarantine, so any appearance here is itself something to catch.
	//
	// Written as a closure and registered, rather than only registered,
	// because an invariant is sampled once per event and this script now
	// watches one kind: the steps that matter most to it, 2 and 5 and 6, are
	// steps where the operator is refused and no event arrives at all, so
	// registration alone would leave those windows unsampled. requirePodRoles
	// is called directly at the end of each of them.
	checkPodRoles := func() error {
		members, err := MembersOf(context.Background(), c.Client(), shardKey)
		if err != nil {
			if podRoleViolation(err) {
				return err
			}
			return nil
		}
		if len(members.Quarantined) > 0 {
			return fmt.Errorf(
				"pod(s) unexpectedly quarantined: %s", strings.Join(members.Quarantined, ", "),
			)
		}
		return nil
	}
	requirePodRoles := func(where string) {
		c.Helper()
		c.Check().NoError(checkPodRoles(), "pod roles after %s", where)
	}
	s.Invariant(
		"shard has exactly one primary and no quarantined pod",
		func(_ ctrltest.Event) error { return checkPodRoles() },
	)

	// The watch carries no starting resourceVersion, so the API server replays
	// the namespace's PVCs as "added" and this step permits exactly that
	// baseline. Naming the three from the operator's own name builders, rather
	// than from a List taken a moment earlier, is what keeps it an assertion:
	// it says the converged pool's storage is those three claims and nothing
	// else, so a fourth PVC or a misnamed one fails here instead of being
	// permitted by whatever happened to exist.
	s.Step("the converged pool's PVCs replay as the baseline", nil,
		ctrltest.Added("PersistentVolumeClaim", pvc0),
		ctrltest.Added("PersistentVolumeClaim", pvc1),
		ctrltest.Added("PersistentVolumeClaim", backupPVC),
	)

	s.Step("cluster settles before any change", nil, ctrltest.Quiet())

	// The pod half of that same claim, made as a read because pods are not
	// watched: the converged pool is pods 0 and 1 and nothing else. This is
	// what the baseline's two Added("Pod", ...) leaves used to say.
	converged := c.poolPodFingerprints()
	for _, want := range []string{pod0, pod1} {
		c.HasKey(converged, want, "converged pool has no pod %s", want)
	}
	c.Eq(2, len(converged), "want exactly %s and %s", pod0, pod1)

	// The precondition that makes step 2 meaningful: a two-member pool, one of
	// them primary, which is the cohort size the defect below is about.
	//
	// Read under Eventually rather than once. The convergence check above is
	// paced by the data plane sim at 250ms, but status.podRoles is written
	// through the pooler sim at 500ms, so both pods can be ready while the
	// second pod's role has not propagated yet. A single read lands in that
	// window often enough to matter: observed failing one full run in six,
	// reporting one primary and zero replicas.
	var initialMembers Members
	c.Eventually(
		30*time.Second,
		"the pool to report one primary and one replica",
		func() error {
			members, err := MembersOf(c.Context(), c.Client(), shardKey)
			if err != nil {
				return err
			}
			if len(members.Replicas) != 1 || members.Primary == "" {
				return fmt.Errorf("got %+v", members)
			}
			initialMembers = members
			return nil
		},
	)

	// Step 2: change CPU and memory on the pool. This is written as the
	// positive assertion the brief describes, and it does not pass: the
	// mutation lands in the Shard spec and podNeedsUpdate correctly flags both
	// pool pods as drifted (the Shard reports RollingUpdate=True
	// reason=PodsDrifted, "2 pods need update in pool default"), but
	// handleRollingUpdates never drains either of them. canStartDisruption
	// (disruption.go) calls posture.CheckDisruption, which calls
	// consensus.CheckSufficientRecruitment against the two-pooler rule
	// test/suite/fakes.go's poolerSim registers; that function's own majority
	// rule is len(cohort)/2+1, which for a two-member cohort is 2, so
	// excluding either pod to disrupt it always leaves 1 short. This is not
	// gated by DurabilityPolicy at all: fakes.go's rule sets AtLeastN(1), not
	// the cluster's AT_LEAST_2 default, so the floor here comes from the
	// majority check that runs before any policy-specific one, and it blocks
	// a two-member pool categorically, not just under this suite's default.
	//
	// This step is pinned, where steps 5 and 6 below are not, because this one
	// is a claim about the operator. A single-cell pool takes ReplicasPerCell 2
	// from the resolver's own default (pkg/resolver/shard.go:102-113), and the
	// operator's purpose-built escape hatch for a member that cannot be
	// disrupted, reconcileCellMaintenanceSurge, is gated on
	// MULTI_CELL_AT_LEAST_2 with exactly two cells
	// (maintenance_surge.go:349-351), so the shape the operator defaults to
	// gets no surge and no other route. The pin may never expire, which is
	// worth saying plainly: it expires if the majority rule changes for a
	// 2-cohort, if the single-cell default moves off 2, or if the surge gate
	// widens to the condition that actually triggers it, and not otherwise.
	//
	// The pin is that no drain ever starts, and it is written as exactly that:
	// the drain state annotation never appears on any pod in the namespace.
	// What it replaced was a permitted set enumerating the whole nine-event
	// cycle a drain would produce on each of the two pods, which pinned the
	// same defect less precisely and could not survive the readiness leaves
	// three of those nine were (see step 1). Naming the annotation is the
	// better pin on its own terms: initiateDrain patches it before the drain
	// machine does anything else, so a drain that started and then stalled
	// halfway confirms the pin today and would be indistinguishable from
	// "never started" under a count of events that never arrived.
	//
	// The positive half comes first and is not part of the pin. A step that
	// asserts nothing happened asserts nothing at all unless the stimulus
	// provably arrived, so the drift condition is waited on with eventually,
	// which fails the test outright rather than confirming the pin, exactly as
	// KnownDefect's contract requires of a setup step.
	//
	// The step's own Quiet() carries the closed-world half over PVCs: a rolling
	// update rewrites pods and leaves their claims alone, so the PVC silence
	// here is the assertion that the operator did not take some other action
	// instead of the one it refused.
	s.Step("change CPU and memory on the pool", func() error {
		c.updateLifecyclePool(clusterName, func(p *PoolSpec) {
			p.Postgres.Resources = lifecycleResources("100m", "128Mi", "200m", "256Mi")
		})
		c.Eventually(
			30*time.Second,
			"the shard to report both pool pods drifted",
			func() error {
				return rollingUpdateDrift(shardKey, len(initialMembers.Replicas)+1)
			},
		)
		podsBefore := c.poolPodFingerprints()
		c.KnownDefect("shard-lifecycle-two-member-pool-never-rolls",
			func() error {
				return waitFor(blockedObservationWindow, func() error {
					return c.drainStateSeen()
				})
			},
		)
		// Stronger than the pin and independent of it: not only did no drain
		// annotation appear, no pod was written to at all while we watched.
		c.Check().Eq("", podFingerprintDiff(podsBefore, c.poolPodFingerprints()),
			"the refused rolling update still touched pods")
		requirePodRoles("the refused rolling update")
		return nil
	}, ctrltest.Quiet())

	// Step 3: change storage. expandPVCIfNeeded patches the PVC's storage
	// request directly; it is not drift the pod's spec hash notices (the pod
	// references the PVC by name, not by size), so no pod event follows.
	// This mutation is independent of step 2's never-applied one and is
	// unaffected by it.
	s.Step("change storage", func() error {
		c.updateLifecyclePool(clusterName, func(p *PoolSpec) {
			p.Storage.Size = "2Gi"
		})
		return nil
	}, ctrltest.Changed("PersistentVolumeClaim", pvc0), ctrltest.Changed("PersistentVolumeClaim", pvc1))

	// Step 4: add a replica. The closed step covers the new PVC, created once
	// by the operator and bound once by the data-plane fake. The new pod is
	// waited on as a read, and the one genuine code-level ordering here, that
	// the PVC exists before the pod that binds it, is asserted below from the
	// operator's own write log rather than from the arrival order of two
	// watches.
	//
	// This step is where the second instance of step 1's problem was measured:
	// it declared three Changed("Pod", pod2) leaves for the readiness settle
	// and failed on a fourth, one run in eight, with the same
	// status.conditions paths. Those leaves are gone rather than widened.
	pod2 := shardcontroller.BuildPoolPodName(shardRef, poolName, cellName, 2)
	pvc2 := shardcontroller.BuildPoolDataPVCName(shardRef, poolName, cellName, 2)

	s.Step("add a replica", func() error {
		c.updateLifecyclePool(clusterName, func(p *PoolSpec) {
			p.ReplicasPerCell = ptr.To(int32(3))
		})
		c.Eventually(30*time.Second, "new pod ready", func() error {
			pod := &corev1.Pod{}
			key := client.ObjectKey{Namespace: ns, Name: pod2}
			if err := c.Get(key, pod); err != nil {
				return err
			}
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return nil
				}
			}
			return fmt.Errorf("pod %s not Ready yet", pod2)
		})
		return nil
	},
		ctrltest.Added("PersistentVolumeClaim", pvc2),
		ctrltest.Changed("PersistentVolumeClaim", pvc2),
	)

	// The ordering claim, from the write log: createMissingResources creates
	// the data PVC and then the pod that mounts it, in that order, within one
	// pass (reconcile_pool_pods.go:178-198 and the Create that follows it).
	// Both are the shard controller's writes, which is what makes their
	// relative position in the log program order rather than arrival noise;
	// see firstCreateIndex.
	ops := Suite.Ops.OpsInNamespace(ns)
	pvcAt, pvcCreated := firstCreateIndex(ops, "shard", "PersistentVolumeClaim", pvc2)
	podAt, podCreated := firstCreateIndex(ops, "shard", "Pod", pod2)
	c.Check().True(pvcCreated, "the shard controller never created PVC %s", pvc2)
	c.Check().True(podCreated, "the shard controller never created pod %s", pod2)
	// Only meaningful once both exist. firstCreateIndex reports a miss as
	// index 0, so an absent pod would otherwise read as one created before
	// its PVC, and the run would carry a confident ordering complaint about
	// an object that was never created. The switch this replaced got that
	// right by being mutually exclusive; three independent checks have to say
	// it explicitly.
	if pvcCreated && podCreated {
		c.Check().True(pvcAt <= podAt,
			"pod %s was created before the PVC %s it binds (ops %d and %d)",
			pod2, pvc2, podAt, pvcAt)
	}

	requirePodRoles("the scale-up")

	// The new pod must bind the new PVC, not either existing one: resolved
	// through ShardPVCOf against the live pod rather than assumed from the names
	// above.
	c.Eventually(30*time.Second, "new pod bound to a PVC", func() error {
		_, err := ShardPVCOf(c.Context(), c.Client(), ns, pod2)
		return err
	})
	boundTo, err := ShardPVCOf(c.Context(), c.Client(), ns, pod2)
	c.NoError(err, "ShardPVCOf(%s)", pod2)
	// One comparison, not two: pvc0, pvc1 and pvc2 are distinct names, so
	// "it is pvc2" already says "it is not either existing PVC", and a second
	// check against those two could never fire.
	c.Eq(pvc2, boundTo, "pod %s is bound to the wrong PVC, want the new PVC rather than %s or %s",
		pod2, pvc0, pvc1)

	// Steps 5 and 6 do not assert the guarantee this script was written to
	// assert, and say so rather than implying otherwise.
	//
	// That guarantee, documented at
	// pkg/resource-handler/controller/shard/reconcile_pool_pods.go:711, is
	// that handleRollingUpdates drains drifted pods one at a time, replicas
	// before the primary, with the primary's switchover requested before it is
	// touched, and that handleScaleDown removes an extra replica by draining
	// it. Nothing in this harness can observe any of it, because no drain ever
	// starts at any cohort size. canStartDisruption calls
	// posture.CheckDisruption, whose revocation check
	// (consensus.CheckSufficientRecruitment) requires that the pod being
	// excluded cannot satisfy the durability policy on its own, and
	// test/suite/fakes.go's poolerSim registers DurabilityPolicy:
	// topoclient.AtLeastN(1) regardless of the shard's configured policy, so
	// any single excluded pod satisfies it alone and the check refuses every
	// exclusion. handleScaleDown calls the identical gate
	// (reconcile_pool_pods.go:629), which is why step 6 is in the same
	// position as step 5.
	//
	// Neither step is a KnownDefect, and that is the point. A pin records a
	// live operator defect and expires on the day the operator is fixed; this
	// is a harness fidelity gap, so a pin here could never expire, which makes
	// it a suppression wearing a pin's clothes. Nor is threading the shard's
	// real policy through fakes.go known to be enough to reach these steps: at
	// AtLeastN(2) the failure moves earlier instead, the shard loses its
	// PRIMARY from status.podRoles altogether, the standing invariant above
	// fires during step 4, and the operator hot-loops "No primary in podRoles,
	// requeueing to re-read topology". Whether that is further harness
	// infidelity (the fake models no election and no promotion, and registers
	// each pooler's routing role once, at first sight) or an operator defect at
	// AT_LEAST_2 with three members is unresolved, and settling it is the
	// prerequisite for writing these two steps for real.
	//
	// What is left is the pair of facts this harness can establish, asserted
	// positively: the operator sees the change, and then does nothing about it
	// for as long as we watch. Both halves carry weight. Without the first the
	// silence would also be satisfied by a mutation that never reached the
	// shard controller at all, which is the vacuous assertion this suite
	// exists to refuse; without the second there is no tripwire. The day
	// either half changes, for a harness reason or an operator one, these
	// steps fail and force the question open again.
	//
	// One note for whoever picks that up, because it is not obvious from the
	// guarantee's wording: the switchover half has no Pod or PVC footprint to
	// assert even in principle. handleRollingUpdates "requests a switchover"
	// by calling the same initiateDrain annotation patch it uses for a replica
	// (drain_helpers.go:58) and recording a RollingUpdateStarted Event on the
	// Shard. So the only Kubernetes-visible difference between draining the
	// primary and draining a replica is an Event, on an object this script
	// does not watch, delivered over the spam-filtered path rollingUpdateDrift
	// describes. Asserting it needs a different observation, not a better
	// permitted set.

	// Step 5: change CPU and memory again, now with three members. Resolved
	// through MembersOf rather than assumed from index, both as the
	// precondition that makes the step meaningful (silence about a rolling
	// update is only interesting over a pool that really does hold three
	// members, one of them primary) and because the drifted-pod count
	// asserted below is derived from it.
	members, err := MembersOf(c.Context(), c.Client(), shardKey)
	c.NoError(err, "MembersOf before step 5")
	c.Eq(2, len(members.Replicas), "want two replicas before step 5, got %+v", members)
	c.NotEq("", members.Primary, "want a primary before step 5, got %+v", members)
	poolPods := len(members.Replicas) + 1

	s.Step("change CPU and memory again, now with three members", func() error {
		podsBefore := c.poolPodFingerprints()
		c.updateLifecyclePool(clusterName, func(p *PoolSpec) {
			p.Postgres.Resources = lifecycleResources("150m", "192Mi", "300m", "384Mi")
		})
		// The count is what makes this non-vacuous. Step 2's change was never
		// applied either, so two pods have been drifted since then and a bare
		// "RollingUpdate is True" would have been satisfied before this step
		// ran; only the third pod, created at step 4 from the then-current
		// spec, drifts because of this mutation.
		c.Eventually(
			30*time.Second,
			"the shard to report every pool pod drifted",
			func() error {
				return rollingUpdateDrift(shardKey, poolPods)
			},
		)
		// Slept inside the step's own action rather than after it, so the
		// closed world covers the whole window: any event the operator
		// produces while we wait is buffered by the stream and fails the
		// Quiet() below as an unpermitted change.
		time.Sleep(blockedObservationWindow)
		// The pod half of the same silence, which the Quiet() cannot carry now
		// that pods are read rather than watched. Snapshotted before the
		// mutation rather than after the drift wait, so the window compared
		// here is the whole step and not just its tail, which is the span the
		// Quiet() covers.
		c.Check().Eq("", podFingerprintDiff(podsBefore, c.poolPodFingerprints()),
			"the refused rolling update still touched pods")
		requirePodRoles("the refused three-member rolling update")
		return nil
	}, ctrltest.Quiet())

	// Step 6: scale the pool back down, which should drain and remove one
	// replica. Same shape as step 5: prove the request reached the Shard the
	// controller reconciles, then watch it be refused.
	//
	// When this becomes assertable, do not resolve the pod being removed as
	// the highest-named replica. selectShardScaleDownPod (disruption.go:144)
	// selects on the pod index being at or above the desired replica count,
	// and the two only agree here because this fake always elects the
	// lowest-indexed pod primary: with pod 2 primary, the highest-named
	// replica is pod 1 and the operator would remove pod 2. Derive it from the
	// index, confirm through MembersOf that it is not the primary, and take
	// its PVC from ShardPVCOf. The permitted set then wants the PVC's change before
	// the pod's deletion, not after: cleanupDrainedPod patches the data PVC's
	// orphan mark (reconcile_pool_pods.go:560) before the Delete at :567, and
	// it patches rather than deletes because orphanByRemainingCount holds
	// while the pool has three data PVCs and the threshold is 3
	// (shard_controller.go:49).
	membersBeforeScaleDown, err := MembersOf(c.Context(), c.Client(), shardKey)
	c.NoError(err, "MembersOf before step 6")
	c.Eq(2, len(membersBeforeScaleDown.Replicas),
		"want two replicas before step 6, got %+v", membersBeforeScaleDown)
	c.NotEq("", membersBeforeScaleDown.Primary,
		"want a primary before step 6, got %+v", membersBeforeScaleDown)

	s.Step("scale the pool back down by one replica", func() error {
		podsBefore := c.poolPodFingerprints()
		c.updateLifecyclePool(clusterName, func(p *PoolSpec) {
			p.ReplicasPerCell = ptr.To(int32(2))
		})
		// Scale-down writes no condition of its own, so what is proved here is
		// narrower than step 5's: the desired count reached the Shard spec,
		// which is the input handleScaleDown reads, while three pods are still
		// live for it to act on.
		c.Eventually(
			30*time.Second,
			"the scale-down to reach the Shard spec",
			func() error {
				shard := &Shard{}
				if err := c.Get(shardKey, shard); err != nil {
					return err
				}
				pool, ok := shard.Spec.Pools[resolver.DefaultPoolName]
				if !ok {
					return fmt.Errorf("shard %s has no %q pool", shardKey, resolver.DefaultPoolName)
				}
				if got := ptr.Deref(pool.ReplicasPerCell, -1); got != 2 {
					return fmt.Errorf("pool %q wants %d replicas per cell, not 2",
						resolver.DefaultPoolName, got)
				}
				return nil
			},
		)
		time.Sleep(blockedObservationWindow)
		// The claim step 6 exists to make, in the only form this harness can
		// state it while the gate refuses every exclusion: no pod was removed,
		// and no pod was written to either. When the gate opens, the primary's
		// pod and PVC must still be here and the replica's must not, which is
		// what the note above is about; the fingerprint comparison is the half
		// of that which is assertable today, and it is resolved from the live
		// pods rather than from the names, so it does not assume which pod the
		// operator would have picked.
		c.Check().Eq("", podFingerprintDiff(podsBefore, c.poolPodFingerprints()),
			"the refused scale-down still touched pods")
		requirePodRoles("the refused scale-down")
		return nil
	}, ctrltest.Quiet())

	s.Finish(time.Second)
}
