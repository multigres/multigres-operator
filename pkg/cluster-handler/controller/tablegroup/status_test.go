package tablegroup

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

// TestStepComputeStatus pins the status aggregation branches. Status must come
// from observed child status, while TotalShards comes from the desired spec.
func TestStepComputeStatus(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	const (
		tgName    = "test-tg"
		namespace = "default"
	)

	tests := map[string]struct {
		desiredShards int
		children      []multigresv1alpha1.Shard
		wantPhase     multigresv1alpha1.Phase
		wantTotal     int32
		wantReady     int32
		wantMessage   string
		wantAvailable metav1.ConditionStatus
		wantReason    string
		wantCondMsg   string
	}{
		// A healthy child whose ObservedGeneration matches counts as ready.
		"healthy steady state": {
			desiredShards: 1,
			children: []multigresv1alpha1.Shard{
				{
					ObjectMeta: metav1.ObjectMeta{Generation: 1},
					Status: multigresv1alpha1.ShardStatus{
						Phase:              multigresv1alpha1.PhaseHealthy,
						ObservedGeneration: 1,
					},
				},
			},
			wantPhase:     multigresv1alpha1.PhaseHealthy,
			wantTotal:     1,
			wantReady:     1,
			wantMessage:   "Ready",
			wantAvailable: metav1.ConditionTrue,
			wantReason:    "ShardsReady",
			wantCondMsg:   "1/1 shards ready",
		},
		// A healthy child with a stale ObservedGeneration isn't ready, so the group
		// is Progressing.
		"child observedGeneration stale": {
			desiredShards: 1,
			children: []multigresv1alpha1.Shard{
				{
					ObjectMeta: metav1.ObjectMeta{Generation: 2},
					Status: multigresv1alpha1.ShardStatus{
						Phase:              multigresv1alpha1.PhaseHealthy,
						ObservedGeneration: 1,
					},
				},
			},
			wantPhase:     multigresv1alpha1.PhaseProgressing,
			wantTotal:     1,
			wantReady:     0,
			wantMessage:   "0/1 shards ready",
			wantAvailable: metav1.ConditionFalse,
			wantReason:    "ShardsNotReady",
			wantCondMsg:   "0/1 shards ready",
		},
		// A degraded child makes the whole group Degraded.
		"degraded child": {
			desiredShards: 1,
			children: []multigresv1alpha1.Shard{
				{
					ObjectMeta: metav1.ObjectMeta{Generation: 1},
					Status: multigresv1alpha1.ShardStatus{
						Phase:              multigresv1alpha1.PhaseDegraded,
						ObservedGeneration: 1,
					},
				},
			},
			wantPhase:     multigresv1alpha1.PhaseDegraded,
			wantTotal:     1,
			wantReady:     0,
			wantMessage:   "At least one shard is degraded",
			wantAvailable: metav1.ConditionFalse,
			wantReason:    "ShardDegraded",
			wantCondMsg:   "At least one shard is degraded",
		},
		// Two desired but only one healthy, so the group is Progressing.
		"partial ready": {
			desiredShards: 2,
			children: []multigresv1alpha1.Shard{
				{
					ObjectMeta: metav1.ObjectMeta{Generation: 1},
					Status: multigresv1alpha1.ShardStatus{
						Phase:              multigresv1alpha1.PhaseHealthy,
						ObservedGeneration: 1,
					},
				},
			},
			wantPhase:     multigresv1alpha1.PhaseProgressing,
			wantTotal:     2,
			wantReady:     1,
			wantMessage:   "1/2 shards ready",
			wantAvailable: metav1.ConditionFalse,
			wantReason:    "ShardsNotReady",
			wantCondMsg:   "1/2 shards ready",
		},
		// With no shards the group is vacuously Available and Initializing.
		"zero desired": {
			desiredShards: 0,
			children:      nil,
			wantPhase:     multigresv1alpha1.PhaseInitializing,
			wantTotal:     0,
			wantReady:     0,
			wantMessage:   "No Shards",
			wantAvailable: metav1.ConditionTrue,
			wantReason:    "NoShards",
			wantCondMsg:   "No Shards",
		},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			t.Parallel()
			ck := assert.NewCollecting(t)

			tg := &multigresv1alpha1.TableGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:       tgName,
					Namespace:  namespace,
					Generation: 1,
				},
				Spec: multigresv1alpha1.TableGroupSpec{
					Shards: make([]multigresv1alpha1.ShardResolvedSpec, tc.desiredShards),
				},
			}

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tg).
				WithStatusSubresource(&multigresv1alpha1.TableGroup{}).
				Build()

			reconciler := &TableGroupReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(100),
			}

			observed := &multigresv1alpha1.ShardList{Items: tc.children}

			// Name each observed child and mark it desired and applied at its own
			// generation, which is the steady-state setup these cases describe.
			// The orphan and spec-change cases are covered by dedicated tests
			// below that set these inputs differently.
			active := make(map[string]bool, len(observed.Items))
			appliedGen := make(map[string]int64, len(observed.Items))
			for i := range observed.Items {
				childName := fmt.Sprintf("shard-%d", i)
				observed.Items[i].Name = childName
				active[childName] = true
				appliedGen[childName] = observed.Items[i].Generation
			}

			rc := &reconcileContext{
				r:                 reconciler,
				tg:                tg,
				observedShards:    observed,
				activeShardNames:  active,
				appliedGeneration: appliedGen,
				start:             time.Now(),
				oldPhase:          tg.Status.Phase,
			}
			if _, err := stepComputeStatus(t.Context(), rc); err != nil {
				t.Fatalf("stepComputeStatus returned error: %v", err)
			}
			res, err := stepPatchStatus(t.Context(), rc)
			ck.Require().NoError(err, "stepPatchStatus returned error")
			// A successful status update never requeues on its own.
			ck.Eq(0, res.result.RequeueAfter, "expected no requeue, got %+v", res.result)

			updated := &multigresv1alpha1.TableGroup{}
			ck.Require().NoError(c.Get(
				t.Context(),
				types.NamespacedName{Name: tgName, Namespace: namespace},
				updated,
			), "failed to get tablegroup")

			ck.Eq(tc.wantPhase, updated.Status.Phase, "Phase mismatch: got")
			ck.Eq(tc.wantTotal, updated.Status.TotalShards, "TotalShards mismatch: got")
			ck.Eq(tc.wantReady, updated.Status.ReadyShards, "ReadyShards mismatch: got")
			ck.Eq(tc.wantMessage, updated.Status.Message, "Message mismatch: got")

			cond := meta.FindStatusCondition(updated.Status.Conditions, "Available")
			ck.Require().NotNil(cond, "expected an Available condition to be set")
			ck.Eq(tc.wantAvailable, cond.Status, "Available condition status mismatch: got")
			ck.Eq(tc.wantReason, cond.Reason, "Available condition reason mismatch: got")
			ck.Eq(tc.wantCondMsg, cond.Message, "Available condition message mismatch: got")
			ck.Eq(
				tg.Generation,
				cond.ObservedGeneration,
				"Available condition observedGeneration mismatch: got",
			)
		})
	}
}

// TestStepComputeStatus_IgnoresUndesiredOrphans verifies that pruned orphans do
// not drive parent status, even if the observed snapshot still has stale child
// status.
func TestStepComputeStatus_IgnoresUndesiredOrphans(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "tg", Namespace: "default", Generation: 1},
		Spec: multigresv1alpha1.TableGroupSpec{
			Shards: make([]multigresv1alpha1.ShardResolvedSpec, 1),
		},
	}

	observed := &multigresv1alpha1.ShardList{Items: []multigresv1alpha1.Shard{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "desired", Generation: 1},
			Status: multigresv1alpha1.ShardStatus{
				Phase:              multigresv1alpha1.PhaseHealthy,
				ObservedGeneration: 1,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "orphan", Generation: 1},
			Status: multigresv1alpha1.ShardStatus{
				Phase:              multigresv1alpha1.PhaseDegraded,
				ObservedGeneration: 1,
			},
		},
	}}

	rc := &reconcileContext{
		tg:                tg,
		observedShards:    observed,
		activeShardNames:  map[string]bool{"desired": true},
		appliedGeneration: map[string]int64{"desired": 1},
	}

	_, err := stepComputeStatus(t.Context(), rc)
	c.Require().NoError(err, "stepComputeStatus returned error")

	c.Eq(multigresv1alpha1.PhaseHealthy, tg.Status.Phase, "Phase mismatch: got")
	c.Eq(1, tg.Status.ReadyShards, "ReadyShards mismatch: got")
	c.Eq(1, tg.Status.TotalShards, "TotalShards mismatch: got")
}

// TestStepComputeStatus_PendingDeletionPreventsHealthy verifies that pending
// cleanup keeps the parent Progressing even when all desired children are ready.
func TestStepComputeStatus_PendingDeletionPreventsHealthy(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "tg", Namespace: "default", Generation: 2},
		Spec: multigresv1alpha1.TableGroupSpec{
			Shards: make([]multigresv1alpha1.ShardResolvedSpec, 1),
		},
	}

	observed := &multigresv1alpha1.ShardList{Items: []multigresv1alpha1.Shard{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "desired", Generation: 1},
			Status: multigresv1alpha1.ShardStatus{
				Phase:              multigresv1alpha1.PhaseHealthy,
				ObservedGeneration: 1,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "orphan", Generation: 1},
			Status: multigresv1alpha1.ShardStatus{
				Phase:              multigresv1alpha1.PhaseHealthy,
				ObservedGeneration: 1,
			},
		},
	}}

	rc := &reconcileContext{
		tg:                tg,
		observedShards:    observed,
		activeShardNames:  map[string]bool{"desired": true},
		appliedGeneration: map[string]int64{"desired": 1},
		pendingDeletion:   true,
	}

	_, err := stepComputeStatus(t.Context(), rc)
	c.Require().NoError(err, "stepComputeStatus returned error")

	c.Eq(multigresv1alpha1.PhaseProgressing, tg.Status.Phase, "Phase mismatch: got")
	got, want := tg.Status.Message, "Waiting for shard cleanup to finish"
	assert.NewCollecting(t).Eq(want, got, "Message mismatch: got")
	c.Eq(1, tg.Status.ReadyShards, "ReadyShards mismatch: got")

	cond := meta.FindStatusCondition(tg.Status.Conditions, "Available")
	c.Require().NotNil(cond, "expected an Available condition to be set")
	c.Eq(metav1.ConditionFalse, cond.Status, "Available status mismatch: got")
	c.Eq("CleanupPending", cond.Reason, "Available reason mismatch: got")
	c.Eq(tg.Generation, cond.ObservedGeneration, "Available observedGeneration mismatch: got")
}

// TestStepComputeStatus_IgnoresChildUntilSpecChangeObserved verifies that a
// just-changed child is not ready until its observed generation catches up.
func TestStepComputeStatus_IgnoresChildUntilSpecChangeObserved(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	newTG := func() *multigresv1alpha1.TableGroup {
		return &multigresv1alpha1.TableGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "tg", Namespace: "default", Generation: 1},
			Spec: multigresv1alpha1.TableGroupSpec{
				Shards: make([]multigresv1alpha1.ShardResolvedSpec, 1),
			},
		}
	}
	observed := func() *multigresv1alpha1.ShardList {
		return &multigresv1alpha1.ShardList{Items: []multigresv1alpha1.Shard{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "desired", Generation: 1},
				Status: multigresv1alpha1.ShardStatus{
					Phase:              multigresv1alpha1.PhaseHealthy,
					ObservedGeneration: 1,
				},
			},
		}}
	}

	// This reconcile applied generation 2, so the child's observed generation 1
	// is stale and it must not be counted as ready.
	changed := &reconcileContext{
		tg:                newTG(),
		observedShards:    observed(),
		activeShardNames:  map[string]bool{"desired": true},
		appliedGeneration: map[string]int64{"desired": 2},
	}
	if _, err := stepComputeStatus(t.Context(), changed); err != nil {
		t.Fatalf("stepComputeStatus returned error: %v", err)
	}
	c.Eq(
		multigresv1alpha1.PhaseProgressing,
		changed.tg.Status.Phase,
		"Phase mismatch after spec change: got",
	)
	c.Eq(0, changed.tg.Status.ReadyShards, "ReadyShards mismatch after spec change: got")

	// Once applied and observed generations match, the child counts as ready.
	caughtUp := &reconcileContext{
		tg:                newTG(),
		observedShards:    observed(),
		activeShardNames:  map[string]bool{"desired": true},
		appliedGeneration: map[string]int64{"desired": 1},
	}
	_, err := stepComputeStatus(t.Context(), caughtUp)
	c.Require().NoError(err, "stepComputeStatus returned error")
	c.Eq(
		multigresv1alpha1.PhaseHealthy,
		caughtUp.tg.Status.Phase,
		"Phase mismatch once observed: got",
	)
	c.Eq(1, caughtUp.tg.Status.ReadyShards, "ReadyShards mismatch once observed: got")
}
