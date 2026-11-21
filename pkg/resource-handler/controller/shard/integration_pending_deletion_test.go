//go:build integration

package shard_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestPendingDeletionBeforeFirstStatusWrite covers a Shard whose first
// reconcile already sees the PendingDeletion annotation, so the status
// subresource has never been written. orchReady and poolsReady are required
// by the CRD, so the ReadyForDeletion write must carry them; otherwise the API
// server rejects it on every pass and the owning TableGroup never deletes the
// Shard. The reconciler is driven directly, with no manager, so no normal
// reconcile can write status first.
func TestPendingDeletionBeforeFirstStatusWrite(t *testing.T) {
	t.Parallel()

	c, r := pendingDeletionEnv(t)
	ctx := t.Context()

	shard := gatingTestShard("pending-before-status", "cluster-pending")
	setTestPostgresPasswordSecretRef(shard)
	shard.Annotations = map[string]string{
		multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
	}
	require.NoError(t, c.Create(ctx, shard))

	key := types.NamespacedName{Name: shard.Name, Namespace: shard.Namespace}

	// Two passes: the first may only add the finalizer, the second must not
	// fail on a condition the first already wrote.
	for range 2 {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
	}

	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, key, got))
	require.True(
		t,
		meta.IsStatusConditionTrue(
			got.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion,
		),
		"shard never reported ReadyForDeletion; conditions: %+v",
		got.Status.Conditions,
	)
}

// TestPendingDeletionKeepsWrittenStatus covers the ReadyForDeletion write on a
// Shard whose status has already been written: the write must keep the stored
// orchReady, poolsReady, phase and other conditions, not reset them.
func TestPendingDeletionKeepsWrittenStatus(t *testing.T) {
	t.Parallel()

	c, r := pendingDeletionEnv(t)
	ctx := t.Context()

	shard := gatingTestShard("pending-after-status", "cluster-pending-status")
	setTestPostgresPasswordSecretRef(shard)
	require.NoError(t, c.Create(ctx, shard))
	key := types.NamespacedName{Name: shard.Name, Namespace: shard.Namespace}

	// The values the ReadyForDeletion write must keep. They differ from the
	// zero values, so a write that resets them is seen.
	written := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, key, written))
	written.Status = multigresv1alpha1.ShardStatus{
		ObservedGeneration: written.Generation,
		OrchReady:          true,
		PoolsReady:         true,
		Phase:              multigresv1alpha1.PhaseHealthy,
		Message:            "Ready",
		Conditions: []metav1.Condition{{
			Type:               "Available",
			Status:             metav1.ConditionTrue,
			Reason:             "AllPodsReady",
			Message:            "All 1 pods are ready",
			ObservedGeneration: written.Generation,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second)),
		}},
	}
	require.NoError(t, c.Status().Update(ctx, written))

	annotated := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, key, annotated))
	patch := client.MergeFrom(annotated.DeepCopy())
	annotated.Annotations = map[string]string{
		multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
	}
	require.NoError(t, c.Patch(ctx, annotated, patch))

	for range 2 {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
	}

	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, key, got))
	require.True(
		t,
		meta.IsStatusConditionTrue(
			got.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion,
		),
		"shard never reported ReadyForDeletion; conditions: %+v",
		got.Status.Conditions,
	)
	require.True(t, got.Status.OrchReady, "orchReady was reset")
	require.True(t, got.Status.PoolsReady, "poolsReady was reset")
	require.Equal(t, multigresv1alpha1.PhaseHealthy, got.Status.Phase)
	require.Equal(t, written.Status.ObservedGeneration, got.Status.ObservedGeneration)
	available := meta.FindStatusCondition(got.Status.Conditions, "Available")
	require.NotNil(t, available, "the Available condition was dropped")
	require.True(t,
		written.Status.Conditions[0].LastTransitionTime.Equal(&available.LastTransitionTime),
		"the Available condition's transition time changed")
	require.Equal(t, metav1.ConditionTrue, available.Status)
}

// pendingDeletionEnv starts envtest and returns a client that reads the API
// server directly and a Shard reconciler using it, driven by calling
// Reconcile, with no manager.
func pendingDeletionEnv(t *testing.T) (client.Client, *shardcontroller.ShardReconciler) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))

	cfg := testutil.SetUpEnvtest(t,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	c := testutil.SetUpClient(t, cfg, scheme)
	return c, &shardcontroller.ShardReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}
}
