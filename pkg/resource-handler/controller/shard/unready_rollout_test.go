package shard

import (
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// unreadyTwoCellFixture is a MULTI_CELL_AT_LEAST_2 shard with two desired
// pods in each of two cells, every one drifted (the fixture pods carry no spec
// hash) and not Ready, with a1 the primary and every pooler serving.
func unreadyTwoCellFixture(t *testing.T) (
	*ShardReconciler,
	*multigresv1alpha1.Shard,
	map[string]map[string]*corev1.Pod,
	func(cell string, index int) *md.StatusResponse,
	func(),
) {
	t.Helper()
	r, shard, groups, rpc, responses := fourToTwoFixture(t)
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(2))
	shard.Spec.Pools["main"] = pool
	for _, group := range groups {
		for _, pod := range group {
			stored := &corev1.Pod{}
			if err := r.Get(t.Context(), client.ObjectKeyFromObject(pod), stored); err != nil {
				t.Fatal(err)
			}
			setReady(stored, false)
			if err := r.Status().Update(t.Context(), stored); err != nil {
				t.Fatal(err)
			}
			group[pod.Name] = stored
		}
	}
	byName := map[string]*md.StatusResponse{}
	for _, resp := range responses {
		byName[resp.GetConsensusStatus().GetId().GetName()] = resp
	}
	status := func(cell string, index int) *md.StatusResponse {
		return byName[BuildPoolPodName(shard, "main", cell, index)]
	}
	publish := func() {
		for _, resp := range responses {
			rpc.SetStatusResponse(topoclient.ComponentIDString(resp.ConsensusStatus.Id), resp)
		}
	}
	return r, shard, groups, status, publish
}

// setCohort commits every pooler to a rule whose members are only the named
// pods, so a pooler can be left outside the cohort.
func setCohort(shard *multigresv1alpha1.Shard, status func(string, int) *md.StatusResponse,
	members ...[2]any,
) {
	var ids []*cm.ID
	for _, m := range members {
		cell, index := m[0].(string), m[1].(int)
		ids = append(ids, &cm.ID{Cell: cell, Name: BuildPoolPodName(shard, "main", cell, index)})
	}
	for _, cell := range []string{"a", "b"} {
		for i := range 2 {
			status(cell, i).ConsensusStatus.CurrentPosition.Position.Decision.CohortMembers = ids
		}
	}
}

func drainRequested(t *testing.T, r *ShardReconciler, pod *corev1.Pod) bool {
	t.Helper()
	got := &corev1.Pod{}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatal(err)
	}
	return got.Annotations[metadata.AnnotationDrainState] != ""
}

func TestReplaceUnreadyDriftedPod(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arrange adjusts the observed data plane; the target is a0.
		arrange   func(*multigresv1alpha1.Shard, func(string, int) *md.StatusResponse)
		wantDrain bool
	}{
		{
			// Capacity is counted from RPC observations. Counted from Pod
			// readiness, no pod here is available and nothing could move.
			name:      "serving target with serving peers is replaced although no pod is Ready",
			wantDrain: true,
		},
		{
			name: "serving target is kept when too few serving poolers would remain",
			arrange: func(shard *multigresv1alpha1.Shard, status func(string, int) *md.StatusResponse) {
				setCohort(shard, status, [2]any{"a", 0}, [2]any{"a", 1}, [2]any{"b", 0})
				status("b", 1).Status.PostgresReady = false
			},
		},
		{
			// The same shard, but the target is not serving: restarting it
			// takes no capacity away, so the floor the shard is already below
			// must not hold back its repair.
			name: "target that is not serving is replaced below the capacity floor",
			arrange: func(shard *multigresv1alpha1.Shard, status func(string, int) *md.StatusResponse) {
				setCohort(shard, status, [2]any{"a", 0}, [2]any{"a", 1}, [2]any{"b", 0})
				status("b", 1).Status.PostgresReady = false
				status("a", 0).Status.PostgresReady = false
			},
			wantDrain: true,
		},
		{
			name: "target with a restore in flight is kept",
			arrange: func(_ *multigresv1alpha1.Shard, status func(string, int) *md.StatusResponse) {
				status("a", 0).Status.PostgresReady = false
				status("a", 0).Status.PostgresAction = md.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP
			},
		},
		{
			name: "target is kept when a committed peer is not serving",
			arrange: func(_ *multigresv1alpha1.Shard, status func(string, int) *md.StatusResponse) {
				status("b", 0).Status.PostgresReady = false
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, shard, groups, status, publish := unreadyTwoCellFixture(t)
			// Only a0 is a candidate, so the outcome is a0's: with the primary
			// Ready, the Ready drifted pods are left to the Ready path.
			for _, pod := range append(slices.Collect(maps.Values(groups["b"])),
				groups["a"][BuildPoolPodName(shard, "main", "a", 1)]) {
				setReady(pod, true)
				if err := r.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			if tc.arrange != nil {
				tc.arrange(shard, status)
			}
			publish()
			target := groups["a"][BuildPoolPodName(shard, "main", "a", 0)]
			primary := groups["a"][BuildPoolPodName(shard, "main", "a", 1)]
			rollout := &shardRolloutTracker{}
			handled, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout)
			if err != nil {
				t.Fatal(err)
			}
			if !handled {
				t.Fatal("not-Ready drifted pods were not handled")
			}
			if got := drainRequested(t, r, target); got != tc.wantDrain {
				t.Fatalf("target drain requested = %v, want %v (blocked: %q)",
					got, tc.wantDrain, rollout.blocked)
			}
			// a0 is a not-Ready drifted replica in the same pool/cell, so the
			// primary is never the one tried while a0 remains.
			if drainRequested(t, r, primary) {
				t.Fatal("primary drained while a not-Ready drifted replica remained")
			}
			if !tc.wantDrain && (rollout.blocked == "" || !rollout.waitingForRecovery) {
				t.Fatalf("refusal not surfaced: blocked %q, waiting %v",
					rollout.blocked, rollout.waitingForRecovery)
			}
		})
	}
}

func TestClassifyDesiredPods(t *testing.T) {
	r, shard, groups, _, _ := unreadyTwoCellFixture(t)
	a0 := groups["a"][BuildPoolPodName(shard, "main", "a", 0)]
	a1 := groups["a"][BuildPoolPodName(shard, "main", "a", 1)]
	b0 := groups["b"][BuildPoolPodName(shard, "main", "b", 0)]
	b1 := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	pods := func() []corev1.Pod { return []corev1.Pod{*b1, *a1, *b0, *a0} }
	names := func(ps []*corev1.Pod) string { return strings.Join(podNames(ps), ",") }
	unready := func() []string { return podNames(r.classifyDesiredPods(shard, pods()).unready) }

	c := r.classifyDesiredPods(shard, pods())
	want := strings.Join([]string{a0.Name, b0.Name, b1.Name}, ",")
	if names(c.unready) != want || c.primary == nil || c.primary.Name != a1.Name {
		t.Fatalf(
			"unready %s primary %v; want %s and %s",
			names(c.unready),
			c.primary,
			want,
			a1.Name,
		)
	}

	shard.Status.PodRoles[a0.Name] = "QUARANTINED"
	if slices.Contains(unready(), a0.Name) {
		t.Fatal("quarantined pod selected")
	}
	shard.Status.PodRoles[a0.Name] = "REPLICA"

	setReady(a0, true)
	if c := r.classifyDesiredPods(shard, pods()); slices.Contains(podNames(c.unready), a0.Name) ||
		!slices.Contains(podNames(c.ready), a0.Name) {
		t.Fatalf("Ready drifted pod: unready %s ready %s", names(c.unready), names(c.ready))
	}
	setReady(a1, true)
	if c := r.classifyDesiredPods(shard, pods()); c.primary != nil {
		t.Fatal("Ready primary returned as a not-Ready drifted primary")
	}
	setReady(a0, false)
	setReady(a1, false)

	a0.Annotations = map[string]string{metadata.AnnotationDrainState: metadata.DrainStateRequested}
	if slices.Contains(unready(), a0.Name) {
		t.Fatal("draining pod selected")
	}
	a0.Annotations = nil

	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	if c := r.classifyDesiredPods(shard, pods()); names(c.unready) != a0.Name+","+b0.Name ||
		c.primary != nil {
		t.Fatalf(
			"pods beyond the desired count selected: %s, primary %v",
			names(c.unready),
			c.primary,
		)
	}
}

// With the primary drifted and not Ready, the Ready drifted replicas go
// before it, so the primary is replaced once, last.
func TestReplaceUnreadyDriftedPodPrimaryLast(t *testing.T) {
	r, shard, groups, _, publish := unreadyTwoCellFixture(t)
	for _, cell := range []string{"a", "b"} {
		for _, pod := range groups[cell] {
			if pod.Name == BuildPoolPodName(shard, "main", "a", 1) {
				continue
			}
			setReady(pod, true)
			if err := r.Status().Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
		}
	}
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	if !drainRequested(t, r, groups["a"][BuildPoolPodName(shard, "main", "a", 0)]) {
		t.Fatalf(
			"Ready drifted replica not replaced before the primary (blocked: %q)",
			rollout.blocked,
		)
	}
	if drainRequested(t, r, groups["a"][BuildPoolPodName(shard, "main", "a", 1)]) {
		t.Fatal("primary drained before the drifted replicas")
	}
}

// A candidate refused in the cell reconciled first must not keep a
// replaceable candidate in another cell from being tried in the same pass.
func TestReplaceUnreadyDriftedPodTriesEveryCell(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	// Still serving, so a0 blocks only its own replacement, not b0's.
	status("a", 0).Status.PostgresAction = md.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	if drainRequested(t, r, groups["a"][BuildPoolPodName(shard, "main", "a", 0)]) {
		t.Fatal("restoring pod drained")
	}
	if !drainRequested(t, r, groups["b"][BuildPoolPodName(shard, "main", "b", 0)]) {
		t.Fatalf("replaceable pod in the other cell not drained (blocked: %q)", rollout.blocked)
	}
	// The decision is made once per pass and reported to every pool/cell.
	handled, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout)
	if err != nil || !handled {
		t.Fatalf("second pool/cell of the pass: handled %v, err %v", handled, err)
	}
}

func podNames(pods []*corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return names
}

// A not-Ready drifted pod whose pooler is its cell's last serving member is
// held back by maintenance capacity, and asks for the surge that supplies it.
// One pod per cell has no remainder to lose one from, so only the surge can
// let the replacement through.
func TestUnreadyServingPodRequestsMaintenanceSurge(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	for _, cell := range []string{"a", "b"} {
		extra := groups[cell][BuildPoolPodName(shard, "main", cell, 1)]
		if err := r.Delete(t.Context(), extra); err != nil {
			t.Fatal(err)
		}
		status(cell, 1).Status.PostgresReady = false
	}
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	if drainRequested(t, r, groups["a"][BuildPoolPodName(shard, "main", "a", 0)]) {
		t.Fatal("cell a's last serving pooler drained")
	}
	surge := &corev1.Pod{}
	key := client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      BuildPoolPodName(shard, "main", "a", 1),
	}
	if err := r.Get(t.Context(), key, surge); err != nil || !isMaintenanceSurge(surge) {
		t.Fatalf(
			"no maintenance surge requested in cell a (err %v, blocked %q)",
			err,
			rollout.blocked,
		)
	}
	if !rollout.HasSurgeStarted("a") {
		t.Fatal("surge not recorded for the pass")
	}
}

// A pod already on the desired spec that is not serving means the desired
// spec is unproven, so no further pod is moved onto it.
func TestReplaceUnreadyDriftedPodWaitsForDesiredSpecPod(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	b1 := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	desired, err := BuildPoolPod(shard, "main", "b", shard.Spec.Pools["main"], 1, r.Scheme)
	if err != nil {
		t.Fatal(err)
	}
	b1.Annotations = map[string]string{
		metadata.AnnotationSpecHash: desired.Annotations[metadata.AnnotationSpecHash],
	}
	if err := r.Update(t.Context(), b1); err != nil {
		t.Fatal(err)
	}
	// Ready, so the serving half of the gate is the one under test.
	setReady(b1, true)
	if err := r.Status().Update(t.Context(), b1); err != nil {
		t.Fatal(err)
	}
	// Outside the cohort, so only the desired-spec gate, not the posture
	// check, can hold the others back.
	setCohort(shard, status, [2]any{"a", 0}, [2]any{"a", 1}, [2]any{"b", 0})
	status("b", 1).Status.PostgresReady = false
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		for _, pod := range group {
			if drainRequested(t, r, pod) {
				t.Fatalf(
					"pod %s drained while %s on the desired spec is not serving",
					pod.Name,
					b1.Name,
				)
			}
		}
	}
	if !strings.Contains(rollout.blocked, b1.Name) {
		t.Fatalf("refusal does not name the desired-spec pod: %q", rollout.blocked)
	}
}

// A maintenance surge broken on an earlier spec is replaced like a desired
// pod, or it would hold its cell's capacity, and the base pod it covers,
// forever.
func TestUnreadyDriftedPodsIncludesActiveSurge(t *testing.T) {
	r, shard, groups, _, _ := unreadyTwoCellFixture(t)
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	b1 := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	pods := []corev1.Pod{*b1}
	if c := r.classifyDesiredPods(shard, pods); len(c.unready) != 0 {
		t.Fatalf("extra pod selected: %v", podNames(c.unready))
	}
	pods[0].Annotations = map[string]string{
		metadata.AnnotationMaintenanceSurge: maintenanceAnnotationTrue,
	}
	if c := r.classifyDesiredPods(shard, pods); len(c.unready) != 1 {
		t.Fatalf("broken maintenance surge not selected: %v", podNames(c.unready))
	}
	// After a scale-down the surge sits further past the desired count.
	pods[0].Name = BuildPoolPodName(shard, "main", "b", 2)
	if c := r.classifyDesiredPods(shard, pods); len(c.unready) != 1 {
		t.Fatalf("broken maintenance surge past a scale-down not selected: %v", podNames(c.unready))
	}
}

func TestKeepTransitionTime(t *testing.T) {
	before := metav1.Condition{
		Type: "RollingUpdate", Status: metav1.ConditionTrue, Reason: "RolloutBlocked",
		LastTransitionTime: metav1.NewTime(time.Unix(1000, 0)),
	}
	conditions := []metav1.Condition{before}
	// Another pool/cell reported up to date, then the pass ended blocked.
	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type: "RollingUpdate", Status: metav1.ConditionFalse, Reason: "PodsUpToDate",
	})
	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type: "RollingUpdate", Status: metav1.ConditionTrue, Reason: "RolloutBlocked",
	})
	keepTransitionTime(conditions, &before)
	if got := conditions[0].LastTransitionTime; !got.Equal(&before.LastTransitionTime) {
		t.Fatalf("LastTransitionTime moved to %v with the status unchanged", got)
	}

	meta.SetStatusCondition(&conditions, metav1.Condition{
		Type: "RollingUpdate", Status: metav1.ConditionFalse, Reason: "PodsUpToDate",
	})
	keepTransitionTime(conditions, &before)
	if got := conditions[0].LastTransitionTime; got.Equal(&before.LastTransitionTime) {
		t.Fatal("LastTransitionTime kept across a status change")
	}
}

// A desired pod that does not exist yet, such as a replacement another cell
// has not recreated, is a replacement that has not served: nothing else moves.
func TestReplaceUnreadyDriftedPodWaitsForMissingPod(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	b1 := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	if err := r.Delete(t.Context(), b1); err != nil {
		t.Fatal(err)
	}
	// Removed from the cohort, so only the missing-pod gate can hold a0.
	setCohort(shard, status, [2]any{"a", 0}, [2]any{"a", 1}, [2]any{"b", 0})
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	if drainRequested(t, r, groups["a"][BuildPoolPodName(shard, "main", "a", 0)]) {
		t.Fatal("pod drained while a desired pod is missing")
	}
	if !strings.Contains(rollout.blocked, b1.Name) {
		t.Fatalf("refusal does not name the missing pod: %q", rollout.blocked)
	}
}

// A pod whose pooler never registered may be replaced once it has been not
// Ready for longer than the grace, and not before: a pooler merely slow to
// start could otherwise join the cohort between the check and the drain.
func TestReplaceUnreadyDriftedPodUnregisteredTarget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		age       time.Duration
		wantDrain bool
	}{
		{name: "unregistered pod scheduled recently waits", age: time.Second},
		{name: "unregistered pod scheduled past the grace is replaced", age: time.Hour, wantDrain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, shard, groups, status, publish := unreadyTwoCellFixture(t)
			a0 := groups["a"][BuildPoolPodName(shard, "main", "a", 0)]
			for _, pod := range groups["b"] {
				setReady(pod, true)
				if err := r.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			store, err := r.CreateTopoStore(shard)
			if err != nil {
				t.Fatal(err)
			}
			topoStore := store.(*disruptionTopo)
			topoStore.poolers = slices.DeleteFunc(topoStore.poolers,
				func(p *topoclient.MultipoolerInfo) bool { return p.Id.GetName() == a0.Name })
			setCohort(shard, status, [2]any{"a", 1}, [2]any{"b", 0}, [2]any{"b", 1})
			publish()
			scheduled := time.Now().Add(-tc.age)
			r.Clock = func() time.Time { return scheduled.Add(tc.age) }
			a0.Status.Conditions = append(a0.Status.Conditions, corev1.PodCondition{
				Type: corev1.PodScheduled, Status: corev1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(scheduled),
			})
			if err := r.Status().Update(t.Context(), a0); err != nil {
				t.Fatal(err)
			}
			rollout := &shardRolloutTracker{}
			if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
				t.Fatal(err)
			}
			if got := drainRequested(t, r, a0); got != tc.wantDrain {
				t.Fatalf(
					"drain requested = %v, want %v (blocked: %q)",
					got,
					tc.wantDrain,
					rollout.blocked,
				)
			}
		})
	}
}

// A target that is not serving still leaves its cell of a two-cell shard
// without a serving pooler, and the one-pooler cell's remainder cannot satisfy
// multi-cell durability until a surge joins, so the surge is asked for.
func TestUnreadyNotServingPodRequestsMaintenanceSurge(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	for _, cell := range []string{"a", "b"} {
		if err := r.Delete(
			t.Context(),
			groups[cell][BuildPoolPodName(shard, "main", cell, 1)],
		); err != nil {
			t.Fatal(err)
		}
		status(cell, 1).Status.PostgresReady = false
	}
	status("a", 0).Status.PostgresReady = false
	// b0 leads the remaining {a0, b0} cohort, so without a0 the remainder
	// cannot satisfy multi-cell durability: the refusal a surge resolves.
	moveLeader(t, r, shard, status, "b", 0)
	setCohort(shard, status, [2]any{"a", 0}, [2]any{"b", 0})
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	surge := &corev1.Pod{}
	key := client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      BuildPoolPodName(shard, "main", "a", 1),
	}
	if err := r.Get(t.Context(), key, surge); err != nil || !isMaintenanceSurge(surge) {
		t.Fatalf(
			"no maintenance surge requested in cell a (err %v, blocked %q)",
			err,
			rollout.blocked,
		)
	}
}

// Scale-down waits for every desired pod to be Ready, so while desired pods
// are being repaired a not-Ready extra that blocks the posture check (a
// committed member that is not serving) is removed here, first.
func TestReplaceUnreadyDriftedPodRemovesBlockingExtra(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	// a1 stays primary and desired-index pods a0, b0 are not Ready but
	// serving; the extra b1 is a committed member that is not serving. Its
	// restore in flight does not hold it: an extra is removed, not replaced.
	status("b", 1).Status.PostgresReady = false
	status("b", 1).Status.PostgresAction = md.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	b1 := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	if !drainRequested(t, r, b1) {
		t.Fatalf("blocking extra %s not removed (blocked: %q)", b1.Name, rollout.blocked)
	}
}

func TestFinishRollingUpdateCondition(t *testing.T) {
	blocked := metav1.Condition{
		Type: "RollingUpdate", Status: metav1.ConditionTrue, Reason: "RolloutBlocked",
		Message:            "not replacing drifted not-Ready pod p: reason",
		LastTransitionTime: metav1.NewTime(time.Unix(1000, 0)),
	}
	drifted := metav1.Condition{
		Type: "RollingUpdate", Status: metav1.ConditionTrue, Reason: "PodsDrifted",
	}
	for _, tc := range []struct {
		name       string
		rollout    shardRolloutTracker
		wantReason string
	}{
		{
			name:       "a pass that decided nothing keeps the last block",
			rollout:    shardRolloutTracker{unreadyPending: true, unreadyDeferred: true},
			wantReason: "RolloutBlocked",
		},
		{
			name:       "a pass that started a disruption reports progress",
			rollout:    shardRolloutTracker{unreadyPending: true, started: true},
			wantReason: "PodsDrifted",
		},
		{
			name:       "nothing pending reports the pools' own state",
			wantReason: "PodsDrifted",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conditions := []metav1.Condition{blocked}
			meta.SetStatusCondition(&conditions, drifted)
			finishRollingUpdateCondition(&conditions, &tc.rollout, &blocked)
			if got := conditions[0].Reason; got != tc.wantReason {
				t.Fatalf("Reason = %s, want %s", got, tc.wantReason)
			}
		})
	}
}

// moveLeader makes the named pod the committed, serving primary in topology,
// status and every pooler's committed rule.
func moveLeader(
	t *testing.T,
	r *ShardReconciler,
	shard *multigresv1alpha1.Shard,
	status func(string, int) *md.StatusResponse,
	cell string,
	index int,
) {
	t.Helper()
	leaderName := BuildPoolPodName(shard, "main", cell, index)
	leaderID := &cm.ID{Cell: cell, Name: leaderName}
	store, err := r.CreateTopoStore(shard)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range store.(*disruptionTopo).poolers {
		p.RoutingState.Role = cm.RoutingRole_ROUTING_ROLE_REPLICA
		if p.Id.GetName() == leaderName {
			p.RoutingState.Role = cm.RoutingRole_ROUTING_ROLE_PRIMARY
		}
	}
	shard.Status.PodRoles = map[string]string{leaderName: "PRIMARY"}
	var followers []*cm.ID
	for _, c := range []string{"a", "b"} {
		for i := range 2 {
			resp := status(c, i)
			resp.ConsensusStatus.CurrentPosition.Position.Decision.LeaderId = leaderID
			resp.Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_STANDBY
			resp.Status.PrimaryStatus = nil
			if c != cell || i != index {
				followers = append(followers, resp.ConsensusStatus.Id)
			}
		}
	}
	leader := status(cell, index)
	leader.Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_PRIMARY
	leader.Status.PrimaryStatus = &md.PrimaryStatus{Ready: true, ConnectedFollowers: followers}
}

// A pod of a pool or cell no longer in the spec is not an extra this path may
// drain: nothing would delete it, and its drain annotation would block every
// later disruption in the shard.
func TestUnreadyExtrasOnlyFromSpec(t *testing.T) {
	_, shard, groups, _, _ := unreadyTwoCellFixture(t)
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	b1 := *groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	removed := *b1.DeepCopy()
	removed.Name = "removed-pool-0"
	removed.Labels = maps.Clone(b1.Labels)
	removed.Labels[metadata.LabelMultigresPool] = "removed"
	got := podNames(unreadyExtras(shard, []corev1.Pod{b1, removed}))
	if !slices.Equal(got, []string{b1.Name}) {
		t.Fatalf("extras %v, want only %s", got, b1.Name)
	}
}

func TestFinishRollingUpdateConditionRepeatedRefusal(t *testing.T) {
	before := metav1.Condition{
		Type: "RollingUpdate", Status: metav1.ConditionTrue, Reason: "RolloutBlocked",
		Message: "not replacing pod p: reason",
	}
	for _, tc := range []struct {
		blocked string
		want    bool
	}{
		{blocked: before.Message, want: true},
		{blocked: "not replacing pod p: another reason", want: false},
	} {
		conditions := []metav1.Condition{before}
		rollout := &shardRolloutTracker{blocked: tc.blocked}
		finishRollingUpdateCondition(&conditions, rollout, &before)
		if rollout.blockedAgain != tc.want {
			t.Errorf(
				"blocked %q: blockedAgain = %v, want %v",
				tc.blocked,
				rollout.blockedAgain,
				tc.want,
			)
		}
	}
}

// A surge is asked for only when nothing could be replaced: here a0 wants one
// (cell a has no other serving pooler), but b0 can go, so b0 is drained and
// no surge is created. The Ready extra a1 is not a candidate.
func TestUnreadySurgeNotRequestedWhenAnotherPodDrains(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	a1 := groups["a"][BuildPoolPodName(shard, "main", "a", 1)]
	b1 := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	for _, pod := range []*corev1.Pod{a1, b1} {
		setReady(pod, true)
		if err := r.Status().Update(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
	}
	moveLeader(t, r, shard, status, "b", 1)
	setCohort(shard, status, [2]any{"a", 0}, [2]any{"b", 0}, [2]any{"b", 1})
	status("a", 1).Status.PostgresReady = false
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	if !drainRequested(t, r, groups["b"][BuildPoolPodName(shard, "main", "b", 0)]) {
		t.Fatalf("b0 not drained (blocked: %q)", rollout.blocked)
	}
	if drainRequested(t, r, a1) {
		t.Fatal("Ready extra drained")
	}
	// A surge request would create one at ordinal 1, adopting a1.
	got := &corev1.Pod{}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(a1), got); err != nil {
		t.Fatal(err)
	}
	if isMaintenanceSurge(got) {
		t.Fatal("maintenance surge requested although a pod was drained")
	}
}

// A refusal no surge can fix, such as no serving primary, asks for none.
func TestUnreadySurgeNotRequestedWithoutPrimary(t *testing.T) {
	r, shard, groups, status, publish := unreadyTwoCellFixture(t)
	pool := shard.Spec.Pools["main"]
	pool.ReplicasPerCell = ptr.To(int32(1))
	shard.Spec.Pools["main"] = pool
	for _, cell := range []string{"a", "b"} {
		if err := r.Delete(
			t.Context(),
			groups[cell][BuildPoolPodName(shard, "main", cell, 1)],
		); err != nil {
			t.Fatal(err)
		}
		status(cell, 1).Status.PostgresReady = false
	}
	status("a", 0).Status.PostgresReady = false
	moveLeader(t, r, shard, status, "b", 0)
	setCohort(shard, status, [2]any{"a", 0}, [2]any{"b", 0})
	status("b", 0).Status.PrimaryStatus.Ready = false
	publish()
	rollout := &shardRolloutTracker{}
	if _, err := r.replaceUnreadyDriftedPod(t.Context(), shard, rollout); err != nil {
		t.Fatal(err)
	}
	surge := &corev1.Pod{}
	key := client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      BuildPoolPodName(shard, "main", "a", 1),
	}
	if err := r.Get(t.Context(), key, surge); err == nil {
		t.Fatalf(
			"maintenance surge created for a refusal it cannot fix (blocked: %q)",
			rollout.blocked,
		)
	}
}
