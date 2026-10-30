//go:build integration
// +build integration

package shard

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/testkit/managedfields"
)

// TestReconcilePoolerReadiness_SurvivesConcurrentKubeletWrite proves the fix
// against a real API server: kubelet changing a condition it owns, between
// reconcilePoolerReadiness's List and its write, must survive that write.
// It also covers the no-op skip (a repeat call over a settled Pod writes
// nothing) and a same-status reason/message change (which must still write,
// but must carry lastTransitionTime over rather than reset it).
//
// The interleaving is made deterministic with an interceptor.Funcs hook on
// List: once reconcilePoolerReadiness has read its (now stale) snapshot, and
// before it can act on it, the hook drives a second, direct client through
// the same Update kubelet itself would issue, under field manager "kubelet".
// reconcilePoolerReadiness then writes from the stale snapshot it already
// holds, exactly as it does outside the test.
func TestReconcilePoolerReadiness_SurvivesConcurrentKubeletWrite(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Pod scheme: %v", err)
	}

	cfg := testutil.SetUpEnvtest(t)
	ctx := t.Context()

	// A plain client plays both "the real API server state" for assertions
	// and the kubelet actor racing the reconciler.
	kubelet := testutil.SetUpClient(t, cfg, scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}
	labels := shardPDBLabels(shard)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pooler-0", Namespace: shard.Namespace, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "multipooler", Image: "busybox"}},
			ReadinessGates: []corev1.PodReadinessGate{
				{ConditionType: PoolerDataReadyCondition},
			},
		},
	}
	if err := kubelet.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	// Seed the baseline kubelet owns, matching a healthy running pod, so the
	// race below is a transition kubelet makes, not a first write.
	pod.Status.Conditions = []corev1.PodCondition{
		{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			Reason:             "PodReady",
			Message:            "containers are ready",
			LastTransitionTime: metav1.Now(),
		},
	}
	if err := kubelet.Status().Update(ctx, pod, client.FieldOwner("kubelet")); err != nil {
		t.Fatalf("seed kubelet condition: %v", err)
	}

	opClient, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build operator client: %v", err)
	}

	var triggered atomic.Bool
	var statusPatches atomic.Int32
	wrapped := interceptor.NewClient(opClient, interceptor.Funcs{
		List: func(
			ctx context.Context,
			c client.WithWatch,
			list client.ObjectList,
			opts ...client.ListOption,
		) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if _, ok := list.(*corev1.PodList); !ok || !triggered.CompareAndSwap(false, true) {
				return nil
			}
			// The race: kubelet observes a container go unready and writes
			// that after the operator's List above already returned.
			var latest corev1.Pod
			if err := kubelet.Get(ctx, client.ObjectKeyFromObject(pod), &latest); err != nil {
				return err
			}
			latest.Status.Conditions = []corev1.PodCondition{
				{
					Type:               corev1.PodReady,
					Status:             corev1.ConditionFalse,
					Reason:             "ContainersNotReady",
					Message:            "containers with unready status: [multipooler]",
					LastTransitionTime: metav1.Now(),
				},
			}
			return kubelet.Status().Update(ctx, &latest, client.FieldOwner("kubelet"))
		},
		SubResourcePatch: func(
			ctx context.Context,
			c client.Client,
			subResourceName string,
			obj client.Object,
			patch client.Patch,
			opts ...client.SubResourcePatchOption,
		) error {
			if subResourceName == "status" {
				statusPatches.Add(1)
			}
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	})

	r := &ShardReconciler{Client: wrapped, Scheme: scheme}
	observations := map[string]posture.Readiness{
		pod.Name: {
			Ready:   true,
			Reason:  "DataPlaneReady",
			Message: "multipooler reports data plane ready",
		},
	}
	if err := r.reconcilePoolerReadiness(ctx, shard, observations); err != nil {
		t.Fatalf("reconcile readiness: %v", err)
	}
	if got := statusPatches.Load(); got != 1 {
		t.Fatalf("status patches = %d, want 1", got)
	}

	var final corev1.Pod
	if err := kubelet.Get(ctx, client.ObjectKeyFromObject(pod), &final); err != nil {
		t.Fatalf("get final pod: %v", err)
	}

	readiness := findPoolerReadinessCondition(final.Status.Conditions)
	if readiness == nil ||
		readiness.Status != corev1.ConditionTrue ||
		readiness.Reason != "DataPlaneReady" {
		t.Fatalf("PoolerDataReady condition = %#v, want true DataPlaneReady", readiness)
	}

	var podReady *corev1.PodCondition
	for i := range final.Status.Conditions {
		if final.Status.Conditions[i].Type == corev1.PodReady {
			podReady = &final.Status.Conditions[i]
		}
	}
	if podReady == nil ||
		podReady.Status != corev1.ConditionFalse ||
		podReady.Reason != "ContainersNotReady" {
		t.Fatalf("kubelet's Ready condition was clobbered, got %#v", podReady)
	}

	// No-op path: the same observation over the now-settled Pod must not
	// write again.
	if err := r.reconcilePoolerReadiness(ctx, shard, observations); err != nil {
		t.Fatalf("reconcile readiness (no-op): %v", err)
	}
	if got := statusPatches.Load(); got != 1 {
		t.Fatalf("status patches after no-op reconcile = %d, want 1 (no additional write)", got)
	}

	// Update path: same Ready status, a new reason/message. This must still
	// write (conditionMatches compares reason and message too), but since
	// Status did not transition, lastTransitionTime must carry over from the
	// existing entry rather than reset to the write's own time.
	previousTransition := readiness.LastTransitionTime
	updated := map[string]posture.Readiness{
		pod.Name: {
			Ready:   true,
			Reason:  "DataPlaneReady",
			Message: "multipooler still reports data plane ready",
		},
	}
	if err := r.reconcilePoolerReadiness(ctx, shard, updated); err != nil {
		t.Fatalf("reconcile readiness (reason change): %v", err)
	}
	if got := statusPatches.Load(); got != 2 {
		t.Fatalf("status patches after reason-change reconcile = %d, want 2", got)
	}
	if err := kubelet.Get(ctx, client.ObjectKeyFromObject(pod), &final); err != nil {
		t.Fatalf("get pod after reason change: %v", err)
	}
	readiness = findPoolerReadinessCondition(final.Status.Conditions)
	if readiness == nil ||
		readiness.Status != corev1.ConditionTrue ||
		readiness.Message != "multipooler still reports data plane ready" {
		t.Fatalf("PoolerDataReady condition after reason change = %#v", readiness)
	}
	if !readiness.LastTransitionTime.Equal(&previousTransition) {
		t.Fatalf(
			"lastTransitionTime changed on a same-status update: was %v, now %v",
			previousTransition, readiness.LastTransitionTime,
		)
	}
}

// TestReconcilePoolerReadiness_SurvivesMergeFromUpgrade proves why
// ForceOwnership is required rather than defensive. It seeds a Pod whose
// PoolerDataReady condition was last written exactly as the pre-upgrade
// binary wrote it: a JSON merge patch (client.MergeFrom) under the same
// field manager name the new server-side apply uses. The API server
// records that patch as an Update-operation managedFields entry, a
// different owner from an Apply-operation entry even though the manager
// name is identical, so the first apply from this manager after an
// upgrade conflicts with its own leftover entry unless forced. This test
// asserts the apply succeeds over that leftover entry and that kubelet's
// own condition, owned by a different manager, is untouched.
func TestReconcilePoolerReadiness_SurvivesMergeFromUpgrade(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Pod scheme: %v", err)
	}

	cfg := testutil.SetUpEnvtest(t)
	ctx := t.Context()

	kubelet := testutil.SetUpClient(t, cfg, scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}
	labels := shardPDBLabels(shard)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pooler-0", Namespace: shard.Namespace, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "multipooler", Image: "busybox"}},
			ReadinessGates: []corev1.PodReadinessGate{
				{ConditionType: PoolerDataReadyCondition},
			},
		},
	}
	if err := kubelet.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	// kubelet's own condition, owned by field manager "kubelet", must come
	// through the reconciler's apply untouched.
	pod.Status.Conditions = []corev1.PodCondition{
		{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			Reason:             "PodReady",
			Message:            "containers are ready",
			LastTransitionTime: metav1.Now(),
		},
	}
	if err := kubelet.Status().Update(ctx, pod, client.FieldOwner("kubelet")); err != nil {
		t.Fatalf("seed kubelet condition: %v", err)
	}

	// Reproduce what the pre-upgrade binary did: read-modify-write the
	// PoolerDataReady condition with a JSON merge patch under the same
	// field manager name this fix's apply now uses.
	base := pod.DeepCopy()
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               PoolerDataReadyCondition,
		Status:             corev1.ConditionFalse,
		Reason:             "ObservationUnavailable",
		Message:            "Multigres data-plane readiness has not been observed",
		LastTransitionTime: metav1.Now(),
	})
	if err := kubelet.Status().Patch(
		ctx,
		pod,
		client.MergeFrom(base),
		client.FieldOwner(ownership.LegacyResourceHandlerReadiness),
	); err != nil {
		t.Fatalf("seed pre-upgrade PoolerDataReady condition: %v", err)
	}

	opClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build operator client: %v", err)
	}

	r := &ShardReconciler{Client: opClient, Scheme: scheme}
	observations := map[string]posture.Readiness{
		pod.Name: {
			Ready:   true,
			Reason:  "DataPlaneReady",
			Message: "multipooler reports data plane ready",
		},
	}
	if err := r.reconcilePoolerReadiness(ctx, shard, observations); err != nil {
		t.Fatalf("reconcile readiness over a pre-upgrade MergeFrom-owned condition: %v", err)
	}

	var final corev1.Pod
	if err := kubelet.Get(ctx, client.ObjectKeyFromObject(pod), &final); err != nil {
		t.Fatalf("get final pod: %v", err)
	}

	readiness := findPoolerReadinessCondition(final.Status.Conditions)
	if readiness == nil ||
		readiness.Status != corev1.ConditionTrue ||
		readiness.Reason != "DataPlaneReady" {
		t.Fatalf("PoolerDataReady condition = %#v, want true DataPlaneReady", readiness)
	}

	var podReady *corev1.PodCondition
	for i := range final.Status.Conditions {
		if final.Status.Conditions[i].Type == corev1.PodReady {
			podReady = &final.Status.Conditions[i]
		}
	}
	if podReady == nil ||
		podReady.Status != corev1.ConditionTrue ||
		podReady.Reason != "PodReady" {
		t.Fatalf("kubelet's Ready condition was disturbed, got %#v", podReady)
	}
	requireNoLegacyManager(t, &final)
}

func requireNoLegacyManager(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if i := slices.IndexFunc(managedfields.Managers(pod), ownership.IsLegacy); i >= 0 {
		t.Fatalf(
			"pod %s still carries legacy field manager %q",
			pod.Name,
			managedfields.Managers(pod)[i],
		)
	}
}

func hasLegacyManager(pod *corev1.Pod) bool {
	return slices.ContainsFunc(managedfields.Managers(pod), ownership.IsLegacy)
}

// seedLegacyReadinessPod creates a pooler Pod whose PoolerDataReady condition
// was written by a real server-side apply under the legacy readiness manager,
// the way the pre-rename operator wrote it, next to a kubelet-owned Ready
// condition. The legacy entry is checked to exist before the test acts.
func seedLegacyReadinessPod(
	t *testing.T,
	c client.Client,
	shard *multigresv1alpha1.Shard,
	name string,
	cond corev1.PodCondition,
) *corev1.Pod {
	t.Helper()
	ctx := t.Context()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: shard.Namespace,
			Labels:    shardPDBLabels(shard),
		},
		Spec: corev1.PodSpec{
			Containers:     []corev1.Container{{Name: "multipooler", Image: "busybox"}},
			ReadinessGates: []corev1.PodReadinessGate{{ConditionType: PoolerDataReadyCondition}},
		},
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodReady, Status: corev1.ConditionTrue, Reason: "PodReady",
		LastTransitionTime: metav1.Now(),
	}}
	if err := c.Status().Update(ctx, pod, client.FieldOwner("kubelet")); err != nil {
		t.Fatalf("seed kubelet condition: %v", err)
	}
	legacy := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: shard.Namespace},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{cond}},
	}
	if err := managedfields.Seed(
		ctx, c, legacy, ownership.LegacyResourceHandlerReadiness, managedfields.OnStatus(),
	); err != nil {
		t.Fatalf("seed legacy apply: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatalf("get seeded pod: %v", err)
	}
	if !hasLegacyManager(pod) {
		t.Fatalf("seed left no legacy entry on pod %s", name)
	}
	return pod
}

// TestReconcilePoolerReadiness_DropsLegacyWhenConditionAlreadyMatches covers
// the skip branch: the condition already holds the observed value, so no apply
// follows and only the tidy-up can retire the legacy entry.
func TestReconcilePoolerReadiness_DropsLegacyWhenConditionAlreadyMatches(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Pod scheme: %v", err)
	}
	cfg := testutil.SetUpEnvtest(t)
	ctx := t.Context()
	c := testutil.SetUpClient(t, cfg, scheme)

	shard := testReadinessShard()
	pod := seedLegacyReadinessPod(t, c, shard, "pooler-0", corev1.PodCondition{
		Type: PoolerDataReadyCondition, Status: corev1.ConditionTrue,
		Reason: "DataPlaneReady", Message: "ready", LastTransitionTime: metav1.Now(),
	})
	before := findPoolerReadinessCondition(pod.Status.Conditions)

	var patches atomic.Int32
	wrapped := interceptor.NewClient(mustWatchClient(t, cfg, scheme), interceptor.Funcs{
		SubResourcePatch: func(
			ctx context.Context, c client.Client, sub string, obj client.Object,
			patch client.Patch, opts ...client.SubResourcePatchOption,
		) error {
			patches.Add(1)
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
	r := &ShardReconciler{Client: wrapped, Scheme: scheme}
	observations := map[string]posture.Readiness{
		pod.Name: {Ready: true, Reason: "DataPlaneReady", Message: "ready"},
	}
	if err := r.reconcilePoolerReadiness(ctx, shard, observations); err != nil {
		t.Fatalf("reconcile readiness: %v", err)
	}
	if got := patches.Load(); got != 0 {
		t.Fatalf("status patches = %d, want 0 (the condition already matched)", got)
	}

	var final corev1.Pod
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &final); err != nil {
		t.Fatalf("get final pod: %v", err)
	}
	requireNoLegacyManager(t, &final)
	after := findPoolerReadinessCondition(final.Status.Conditions)
	if after == nil || after.Status != before.Status || after.Reason != before.Reason ||
		after.Message != before.Message {
		t.Fatalf("condition changed: before %#v, after %#v", before, after)
	}
}

// TestReconcilePoolerReadiness_DropLegacyFailureDoesNotAbortOtherPods proves
// the legacy tidy-up cannot gate readiness: a DropLegacy request that fails for
// the first pod must not stop the remaining pods from getting their readiness
// applied, nor fail the reconcile.
func TestReconcilePoolerReadiness_DropLegacyFailureDoesNotAbortOtherPods(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Pod scheme: %v", err)
	}
	cfg := testutil.SetUpEnvtest(t)
	ctx := t.Context()
	c := testutil.SetUpClient(t, cfg, scheme)

	shard := testReadinessShard()
	names := []string{"pooler-0", "pooler-1", "pooler-2"}
	for _, name := range names {
		seedLegacyReadinessPod(t, c, shard, name, corev1.PodCondition{
			Type: PoolerDataReadyCondition, Status: corev1.ConditionFalse,
			Reason: "ObservationUnavailable", Message: "not observed",
			LastTransitionTime: metav1.Now(),
		})
	}

	// DropLegacy is the only non-apply Patch on the main resource.
	wrapped := interceptor.NewClient(mustWatchClient(t, cfg, scheme), interceptor.Funcs{
		Patch: func(
			ctx context.Context, c client.WithWatch, obj client.Object,
			patch client.Patch, opts ...client.PatchOption,
		) error {
			if obj.GetName() == names[0] {
				return errors.New("injected drop failure")
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	r := &ShardReconciler{Client: wrapped, Scheme: scheme}
	observations := map[string]posture.Readiness{}
	for _, name := range names {
		observations[name] = posture.Readiness{
			Ready:   true,
			Reason:  "DataPlaneReady",
			Message: "ready",
		}
	}
	if err := r.reconcilePoolerReadiness(ctx, shard, observations); err != nil {
		t.Fatalf("a failed legacy tidy-up failed the reconcile: %v", err)
	}

	for _, name := range names {
		var got corev1.Pod
		if err := c.Get(
			ctx,
			client.ObjectKey{Namespace: shard.Namespace, Name: name},
			&got,
		); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		cond := findPoolerReadinessCondition(got.Status.Conditions)
		if cond == nil || cond.Status != corev1.ConditionTrue {
			t.Fatalf("%s readiness not applied: %#v", name, cond)
		}
		if name == names[0] {
			if !hasLegacyManager(&got) {
				t.Fatalf("%s lost its legacy entry despite the injected failure", name)
			}
			continue
		}
		requireNoLegacyManager(t, &got)
	}
}

func testReadinessShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}
}

func mustWatchClient(t *testing.T, cfg *rest.Config, scheme *runtime.Scheme) client.WithWatch {
	t.Helper()
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build operator client: %v", err)
	}
	return c
}
