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
			if err != nil {
				t.Fatalf("stepPatchStatus returned error: %v", err)
			}
			// A successful status update never requeues on its own.
			if res.result.RequeueAfter != 0 {
				t.Errorf("expected no requeue, got %+v", res.result)
			}

			updated := &multigresv1alpha1.TableGroup{}
			if err := c.Get(
				t.Context(),
				types.NamespacedName{Name: tgName, Namespace: namespace},
				updated,
			); err != nil {
				t.Fatalf("failed to get tablegroup: %v", err)
			}

			if got := updated.Status.Phase; got != tc.wantPhase {
				t.Errorf("Phase mismatch: got %q, want %q", got, tc.wantPhase)
			}
			if got := updated.Status.TotalShards; got != tc.wantTotal {
				t.Errorf("TotalShards mismatch: got %d, want %d", got, tc.wantTotal)
			}
			if got := updated.Status.ReadyShards; got != tc.wantReady {
				t.Errorf("ReadyShards mismatch: got %d, want %d", got, tc.wantReady)
			}
			if got := updated.Status.Message; got != tc.wantMessage {
				t.Errorf("Message mismatch: got %q, want %q", got, tc.wantMessage)
			}

			cond := meta.FindStatusCondition(updated.Status.Conditions, "Available")
			if cond == nil {
				t.Fatal("expected an Available condition to be set")
			}
			if cond.Status != tc.wantAvailable {
				t.Errorf(
					"Available condition status mismatch: got %q, want %q",
					cond.Status,
					tc.wantAvailable,
				)
			}
			if cond.Reason != tc.wantReason {
				t.Errorf(
					"Available condition reason mismatch: got %q, want %q",
					cond.Reason,
					tc.wantReason,
				)
			}
			if cond.Message != tc.wantCondMsg {
				t.Errorf(
					"Available condition message mismatch: got %q, want %q",
					cond.Message,
					tc.wantCondMsg,
				)
			}
			if cond.ObservedGeneration != tg.Generation {
				t.Errorf(
					"Available condition observedGeneration mismatch: got %d, want %d",
					cond.ObservedGeneration,
					tg.Generation,
				)
			}
		})
	}
}

// TestStepComputeStatus_IgnoresUndesiredOrphans verifies that pruned orphans do
// not drive parent status, even if the observed snapshot still has stale child
// status.
func TestStepComputeStatus_IgnoresUndesiredOrphans(t *testing.T) {
	t.Parallel()

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

	if _, err := stepComputeStatus(t.Context(), rc); err != nil {
		t.Fatalf("stepComputeStatus returned error: %v", err)
	}

	if got := tg.Status.Phase; got != multigresv1alpha1.PhaseHealthy {
		t.Errorf(
			"Phase mismatch: got %q, want %q (orphan must not count)",
			got,
			multigresv1alpha1.PhaseHealthy,
		)
	}
	if got := tg.Status.ReadyShards; got != 1 {
		t.Errorf("ReadyShards mismatch: got %d, want 1", got)
	}
	if got := tg.Status.TotalShards; got != 1 {
		t.Errorf("TotalShards mismatch: got %d, want 1", got)
	}
}

// TestStepComputeStatus_PendingDeletionPreventsHealthy verifies that pending
// cleanup keeps the parent Progressing even when all desired children are ready.
func TestStepComputeStatus_PendingDeletionPreventsHealthy(t *testing.T) {
	t.Parallel()

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

	if _, err := stepComputeStatus(t.Context(), rc); err != nil {
		t.Fatalf("stepComputeStatus returned error: %v", err)
	}

	if got := tg.Status.Phase; got != multigresv1alpha1.PhaseProgressing {
		t.Errorf("Phase mismatch: got %q, want %q", got, multigresv1alpha1.PhaseProgressing)
	}
	if got, want := tg.Status.Message, "Waiting for removed shards to drain"; got != want {
		t.Errorf("Message mismatch: got %q, want %q", got, want)
	}
	if got := tg.Status.ReadyShards; got != 1 {
		t.Errorf("ReadyShards mismatch: got %d, want 1", got)
	}

	cond := meta.FindStatusCondition(tg.Status.Conditions, "Available")
	if cond == nil {
		t.Fatal("expected an Available condition to be set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Available status mismatch: got %q, want %q", cond.Status, metav1.ConditionFalse)
	}
	if cond.Reason != "CleanupPending" {
		t.Errorf("Available reason mismatch: got %q, want CleanupPending", cond.Reason)
	}
	if cond.ObservedGeneration != tg.Generation {
		t.Errorf(
			"Available observedGeneration mismatch: got %d, want %d",
			cond.ObservedGeneration,
			tg.Generation,
		)
	}
}

// TestStepComputeStatus_IgnoresChildUntilSpecChangeObserved verifies that a
// just-changed child is not ready until its observed generation catches up.
func TestStepComputeStatus_IgnoresChildUntilSpecChangeObserved(t *testing.T) {
	t.Parallel()

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
	if got := changed.tg.Status.Phase; got != multigresv1alpha1.PhaseProgressing {
		t.Errorf(
			"Phase mismatch after spec change: got %q, want %q",
			got,
			multigresv1alpha1.PhaseProgressing,
		)
	}
	if got := changed.tg.Status.ReadyShards; got != 0 {
		t.Errorf("ReadyShards mismatch after spec change: got %d, want 0", got)
	}

	// Once applied and observed generations match, the child counts as ready.
	caughtUp := &reconcileContext{
		tg:                newTG(),
		observedShards:    observed(),
		activeShardNames:  map[string]bool{"desired": true},
		appliedGeneration: map[string]int64{"desired": 1},
	}
	if _, err := stepComputeStatus(t.Context(), caughtUp); err != nil {
		t.Fatalf("stepComputeStatus returned error: %v", err)
	}
	if got := caughtUp.tg.Status.Phase; got != multigresv1alpha1.PhaseHealthy {
		t.Errorf(
			"Phase mismatch once observed: got %q, want %q",
			got,
			multigresv1alpha1.PhaseHealthy,
		)
	}
	if got := caughtUp.tg.Status.ReadyShards; got != 1 {
		t.Errorf("ReadyShards mismatch once observed: got %d, want 1", got)
	}
}
