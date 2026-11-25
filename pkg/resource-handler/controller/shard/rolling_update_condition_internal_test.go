package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// TestFinishRollingUpdateConditionAcrossPools checks that the RollingUpdate
// condition written at the end of a pass describes every pool noted, in pool
// name order, whatever order the pool/cells were noted in.
func TestFinishRollingUpdateConditionAcrossPools(t *testing.T) {
	t.Parallel()

	type note struct {
		pool       string
		drifted    int
		inProgress bool
	}
	for name, tc := range map[string]struct {
		notes       []note
		wantStatus  metav1.ConditionStatus
		wantReason  string
		wantMessage string
	}{
		"one pool drifted": {
			notes:       []note{{"default", 2, true}},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  "PodsDrifted",
			wantMessage: "2 pods need update in pool default",
		},
		"one pool up to date": {
			notes:       []note{{"default", 0, false}},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  "PodsUpToDate",
			wantMessage: "All pods up to date in pool default",
		},
		"drifted pool reconciled before an up-to-date one": {
			notes:       []note{{"alpha", 1, true}, {"beta", 0, false}},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  "PodsDrifted",
			wantMessage: "1 pod needs update in pool alpha",
		},
		"two pools drifted, cells summed": {
			notes: []note{
				{"zeta", 1, true}, {"alpha", 2, true}, {"zeta", 1, true}, {"mid", 0, false},
			},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  "PodsDrifted",
			wantMessage: "2 pods need update in pool alpha; 2 pods need update in pool zeta",
		},
		"draining with nothing left drifted": {
			notes:       []note{{"alpha", 0, true}},
			wantStatus:  metav1.ConditionTrue,
			wantReason:  "PodsDrifted",
			wantMessage: "0 pods need update in pool alpha",
		},
		"every pool up to date": {
			notes:       []note{{"zeta", 0, false}, {"alpha", 0, false}, {"mid", 0, false}},
			wantStatus:  metav1.ConditionFalse,
			wantReason:  "PodsUpToDate",
			wantMessage: "All pods up to date in pools alpha, mid, zeta",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// The message must not depend on map iteration order, so build
			// it many times.
			for range 50 {
				rollout := &shardRolloutTracker{}
				var conditions []metav1.Condition
				for _, n := range tc.notes {
					rollout.notePool(n.pool, n.drifted, n.inProgress)
				}
				finishRollingUpdateCondition(&conditions, rollout, nil)
				got := meta.FindStatusCondition(conditions, "RollingUpdate")
				if got == nil || got.Status != tc.wantStatus || got.Reason != tc.wantReason ||
					got.Message != tc.wantMessage {
					t.Fatalf("RollingUpdate = %+v, want %s %s %q",
						got, tc.wantStatus, tc.wantReason, tc.wantMessage)
				}
			}
		})
	}
}

// TestFinishRollingUpdateConditionBlockedWins checks that a refusal recorded
// this pass still replaces the per-pool condition.
func TestFinishRollingUpdateConditionBlockedWins(t *testing.T) {
	t.Parallel()

	rollout := &shardRolloutTracker{}
	rollout.notePool("alpha", 1, true)
	rollout.block("pod x is not Ready")
	var conditions []metav1.Condition
	finishRollingUpdateCondition(&conditions, rollout, nil)
	got := meta.FindStatusCondition(conditions, "RollingUpdate")
	if got == nil || got.Reason != "RolloutBlocked" || got.Message != "pod x is not Ready" {
		t.Fatalf("RollingUpdate = %+v, want RolloutBlocked", got)
	}
}

// TestSurgeActionNotesItsDrift reconciles a two-cell shard's pool/cell whose
// drifted pod needs a maintenance surge. The pass ends there for that
// pool/cell, after an up-to-date sibling has been noted, and the shard's
// RollingUpdate condition must still report the drift.
func TestSurgeActionNotesItsDrift(t *testing.T) {
	scheme := maintenanceSurgeTestScheme(t)
	shard := maintenanceSurgeTestShard()
	poolName, cellName := "primary", "zone-a"
	pool := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	target, err := BuildPoolPod(shard, poolName, cellName, pool, 0, scheme)
	if err != nil {
		t.Fatal(err)
	}
	target.Annotations[metadata.AnnotationSpecHash] = "stale"
	setReady(target, true)
	r := &ShardReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&corev1.Pod{}).
			WithObjects(shard, target).
			Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	rollout := &shardRolloutTracker{}
	rollout.notePool(poolName, 0, false)
	err = r.reconcilePoolPods(t.Context(), shard, poolName, cellName, pool, rollout)
	if err != nil {
		t.Fatal(err)
	}
	if !rollout.HasSurgeStarted(cellName) {
		t.Fatal("precondition: the pool/cell did not create a maintenance surge")
	}
	condition, ok := rollout.poolsRollingUpdate()
	if !ok || condition.Reason != "PodsDrifted" ||
		condition.Message != "1 pod needs update in pool primary" {
		t.Fatalf("RollingUpdate = %+v (%v), want 1 drifted pod in pool primary", condition, ok)
	}
}
