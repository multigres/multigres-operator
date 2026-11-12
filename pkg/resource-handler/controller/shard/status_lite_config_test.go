package shard

import (
	"errors"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// litePodWithConfig is the ready Lite pod built while the shard carried the
// given restart and reload hashes, as the reconcile stamps them.
func litePodWithConfig(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	restartHash, reloadHash string,
) *corev1.Pod {
	t.Helper()
	stamped := shard.DeepCopy()
	if stamped.Annotations == nil {
		stamped.Annotations = map[string]string{}
	}
	stamped.Annotations[metadata.AnnotationPostgresConfigHash] = restartHash
	stamped.Annotations[metadata.AnnotationPostgresReloadHash] = reloadHash
	return liteStatusTestPod(t, stamped, true)
}

func litePrimaryRPC(shard *multigresv1alpha1.Shard) *rpcclient.FakeClient {
	rpc := rpcclient.NewFakeClient()
	rpc.SetStatusResponse(
		litePoolerID(shard),
		liteStatusResponse(clustermetadata.PoolerType_PRIMARY),
	)
	return rpc
}

// liteStatusHarness runs Lite status passes against one fake API server, so
// a rollout can be followed across passes with the pod swapped in between.
type liteStatusHarness struct {
	t *testing.T
	r *ShardReconciler
	c client.Client
}

func newLiteStatusHarness(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
) *liteStatusHarness {
	t.Helper()
	var objects []client.Object
	if pod != nil {
		objects = append(objects, pod)
	}
	r, c := postureTestReconciler(t, shard, litePrimaryRPC(shard), objects...)
	return &liteStatusHarness{t: t, r: r, c: c}
}

// pass runs one Lite status pass on the stored shard and returns it after.
func (h *liteStatusHarness) pass(
	shard *multigresv1alpha1.Shard,
	rendered renderedConfig,
) *multigresv1alpha1.Shard {
	h.t.Helper()
	current := &multigresv1alpha1.Shard{}
	if err := h.c.Get(h.t.Context(), client.ObjectKeyFromObject(shard), current); err != nil {
		h.t.Fatalf("get shard: %v", err)
	}
	if err := h.r.updateStatus(h.t.Context(), current, rendered); err != nil {
		h.t.Fatalf("updateStatus() error = %v", err)
	}
	got := &multigresv1alpha1.Shard{}
	if err := h.c.Get(h.t.Context(), client.ObjectKeyFromObject(shard), got); err != nil {
		h.t.Fatalf("get shard: %v", err)
	}
	return got
}

// removePod deletes the Lite pod, as the Lite controller does before it
// creates a replacement.
func (h *liteStatusHarness) removePod(pod *corev1.Pod) {
	h.t.Helper()
	old := &corev1.Pod{}
	if err := h.c.Get(h.t.Context(), client.ObjectKeyFromObject(pod), old); err != nil {
		h.t.Fatalf("get pod: %v", err)
	}
	if err := h.c.Delete(h.t.Context(), old); err != nil {
		h.t.Fatalf("delete pod: %v", err)
	}
}

// createPod creates the replacement Lite pod.
func (h *liteStatusHarness) createPod(pod *corev1.Pod) {
	h.t.Helper()
	if err := h.c.Create(h.t.Context(), pod); err != nil {
		h.t.Fatalf("create pod: %v", err)
	}
}

// replacePod swaps the Lite pod in one step, as the reload reconcile does, in
// effect, when it stamps a new reload hash.
func (h *liteStatusHarness) replacePod(pod *corev1.Pod) {
	h.t.Helper()
	h.removePod(pod)
	h.createPod(pod)
}

// runLiteStatus runs a single Lite status pass and returns the stored shard.
func runLiteStatus(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	pod *corev1.Pod,
	rendered renderedConfig,
) *multigresv1alpha1.Shard {
	t.Helper()
	return newLiteStatusHarness(t, shard, pod).pass(shard, rendered)
}

func assertLiteUpToDate(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	status metav1.ConditionStatus,
	reason string,
) {
	t.Helper()
	cond := meta.FindStatusCondition(shard.Status.Conditions, multigresv1alpha1.ConditionUpToDate)
	if cond == nil {
		t.Fatal("UpToDate condition missing")
	}
	if cond.Status != status || cond.Reason != reason {
		t.Fatalf(
			"UpToDate = %s/%s, want %s/%s (%s)",
			cond.Status,
			cond.Reason,
			status,
			reason,
			cond.Message,
		)
	}
	if cond.ObservedGeneration != shard.Generation {
		t.Errorf(
			"UpToDate observedGeneration = %d, want %d",
			cond.ObservedGeneration,
			shard.Generation,
		)
	}
	if shard.Status.ObservedGeneration != shard.Generation {
		t.Errorf(
			"observedGeneration = %d, want %d",
			shard.Status.ObservedGeneration,
			shard.Generation,
		)
	}
}

func TestUpdateLiteStatusPostgresConfig(t *testing.T) {
	applied := renderedConfig{restartHash: "restart-1", reloadHash: "reload-1"}

	t.Run("settled when the pod runs the rendered config", func(t *testing.T) {
		shard := liteStatusTestShard()
		got := runLiteStatus(
			t,
			shard,
			litePodWithConfig(t, shard, "restart-1", "reload-1"),
			applied,
		)

		assertLiteUpToDate(t, got, metav1.ConditionTrue, "SpecApplied")
		st := got.Status.PostgresConfig
		if st == nil || st.InProgress || st.Error != "" {
			t.Fatalf("postgresConfig = %+v, want settled", st)
		}
		if st.LastAppliedAt == nil {
			t.Error("lastAppliedAt should be stamped when the config settles")
		}
	})

	// A reload-only change keeps the pod and waits for the pooler to confirm
	// the SIGHUP, which stamps the new reload hash on the pod.
	t.Run("reload-only change is in progress until the reload is confirmed", func(t *testing.T) {
		past := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
		shard := liteStatusTestShard()
		shard.Status.PostgresConfig = &multigresv1alpha1.PostgresConfigStatus{LastAppliedAt: &past}
		reloaded := renderedConfig{restartHash: "restart-1", reloadHash: "reload-2"}

		h := newLiteStatusHarness(t, shard, litePodWithConfig(t, shard, "restart-1", "reload-1"))
		pending := h.pass(shard, reloaded)
		assertLiteUpToDate(t, pending, metav1.ConditionFalse, "ReloadPending")
		st := pending.Status.PostgresConfig
		if st == nil || !st.InProgress || st.Error != "" {
			t.Fatalf("postgresConfig = %+v, want in progress", st)
		}
		if st.LastAppliedAt == nil || !st.LastAppliedAt.Equal(&past) {
			t.Errorf(
				"lastAppliedAt = %v, want the previous %v kept during the rollout",
				st.LastAppliedAt,
				past,
			)
		}

		h.replacePod(litePodWithConfig(t, shard, "restart-1", "reload-2"))
		done := h.pass(shard, reloaded)
		assertLiteUpToDate(t, done, metav1.ConditionTrue, "SpecApplied")
		st = done.Status.PostgresConfig
		if st == nil || st.InProgress || st.Error != "" {
			t.Fatalf("postgresConfig = %+v, want settled", st)
		}
		if st.LastAppliedAt == nil || !st.LastAppliedAt.After(past.Time) {
			t.Errorf("lastAppliedAt = %v, want restamped after %v", st.LastAppliedAt, past)
		}
	})

	// A restart change replaces the pod: the controller deletes the old pod,
	// which no longer matches the desired spec hash, then creates one that
	// does. The rollout stays in progress while no pod exists.
	t.Run("restart change is in progress until the pod is replaced", func(t *testing.T) {
		shard := liteStatusTestShard()
		restarted := renderedConfig{restartHash: "restart-2", reloadHash: "reload-1"}

		old := litePodWithConfig(t, shard, "restart-1", "reload-1")
		h := newLiteStatusHarness(t, shard, old)
		pending := h.pass(shard, restarted)
		assertLiteUpToDate(t, pending, metav1.ConditionFalse, "SpecChanged")
		if st := pending.Status.PostgresConfig; st == nil || !st.InProgress || st.Error != "" {
			t.Fatalf("postgresConfig = %+v, want in progress", st)
		}

		h.removePod(old)
		gone := h.pass(shard, restarted)
		assertLiteUpToDate(t, gone, metav1.ConditionFalse, "PodMissing")
		if st := gone.Status.PostgresConfig; st == nil || !st.InProgress {
			t.Fatalf("postgresConfig = %+v, want still in progress without a pod", st)
		}

		h.createPod(litePodWithConfig(t, shard, "restart-2", "reload-1"))
		done := h.pass(shard, restarted)
		assertLiteUpToDate(t, done, metav1.ConditionTrue, "SpecApplied")
		st := done.Status.PostgresConfig
		if st == nil || st.InProgress || st.Error != "" || st.LastAppliedAt == nil {
			t.Fatalf("postgresConfig = %+v, want settled with lastAppliedAt", st)
		}
	})

	t.Run("render error is reported without settling", func(t *testing.T) {
		past := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
		shard := liteStatusTestShard()
		shard.Status.PostgresConfig = &multigresv1alpha1.PostgresConfigStatus{LastAppliedAt: &past}
		broken := renderedConfig{err: errors.New("unknown parameter \"nope\"")}

		got := runLiteStatus(t, shard, litePodWithConfig(t, shard, "restart-1", "reload-1"), broken)
		assertLiteUpToDate(t, got, metav1.ConditionFalse, "ConfigError")
		cond := meta.FindStatusCondition(got.Status.Conditions, multigresv1alpha1.ConditionUpToDate)
		if cond.Message != "Failed to render postgresql.conf: unknown parameter \"nope\"" {
			t.Errorf("UpToDate message = %q", cond.Message)
		}
		st := got.Status.PostgresConfig
		if st == nil || st.InProgress || st.Error != "unknown parameter \"nope\"" {
			t.Fatalf("postgresConfig = %+v, want the render error", st)
		}
		if st.LastAppliedAt == nil || !st.LastAppliedAt.Equal(&past) {
			t.Errorf("lastAppliedAt = %v, want the previous %v kept", st.LastAppliedAt, past)
		}
	})

	// An image or resource change replaces the pod but rolls out no config:
	// through the old pod, the gap without a pod and the new pod,
	// status.postgresConfig stays settled and lastAppliedAt does not move.
	t.Run("spec change without a config change stays settled", func(t *testing.T) {
		past := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
		shard := liteStatusTestShard()
		shard.Status.PostgresConfig = &multigresv1alpha1.PostgresConfigStatus{LastAppliedAt: &past}
		old := litePodWithConfig(t, shard, "restart-1", "reload-1")
		old.Annotations[metadata.AnnotationSpecHash] = "older-image"
		h := newLiteStatusHarness(t, shard, old)

		settled := func(got *multigresv1alpha1.Shard, stage string) {
			t.Helper()
			st := got.Status.PostgresConfig
			if st == nil || st.InProgress || st.Error != "" {
				t.Fatalf("%s: postgresConfig = %+v, want settled", stage, st)
			}
			if st.LastAppliedAt == nil || !st.LastAppliedAt.Equal(&past) {
				t.Errorf(
					"%s: lastAppliedAt = %v, want the previous %v kept",
					stage,
					st.LastAppliedAt,
					past,
				)
			}
		}

		got := h.pass(shard, applied)
		assertLiteUpToDate(t, got, metav1.ConditionFalse, "SpecChanged")
		settled(got, "old pod")

		h.removePod(old)
		got = h.pass(shard, applied)
		assertLiteUpToDate(t, got, metav1.ConditionFalse, "PodMissing")
		settled(got, "no pod")

		h.createPod(litePodWithConfig(t, shard, "restart-1", "reload-1"))
		got = h.pass(shard, applied)
		assertLiteUpToDate(t, got, metav1.ConditionTrue, "SpecApplied")
		settled(got, "new pod")
	})

	t.Run("missing pod keeps the previous value", func(t *testing.T) {
		got := runLiteStatus(t, liteStatusTestShard(), nil, applied)
		assertLiteUpToDate(t, got, metav1.ConditionFalse, "PodMissing")
		if st := got.Status.PostgresConfig; st != nil {
			t.Fatalf("postgresConfig = %+v, want unset before the first pod", st)
		}
	})

	t.Run("crash-looping pod on the rendered config is in progress", func(t *testing.T) {
		shard := liteStatusTestShard()
		pod := litePodWithConfig(t, shard, "restart-1", "reload-1")
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{
			{
				Name: "postgres",
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
			},
		}

		got := runLiteStatus(t, shard, pod, applied)
		assertLiteUpToDate(t, got, metav1.ConditionTrue, "SpecApplied")
		if st := got.Status.PostgresConfig; st == nil || !st.InProgress || st.LastAppliedAt != nil {
			t.Fatalf("postgresConfig = %+v, want in progress and never applied", st)
		}
	})
}

func TestLiteConfigInProgress(t *testing.T) {
	rendered := renderedConfig{restartHash: "restart-1", reloadHash: "reload-1"}
	withHashes := func(restart, reload string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			metadata.AnnotationPostgresConfigHash: restart,
			metadata.AnnotationPostgresReloadHash: reload,
		}}}
	}
	crashLooping := withHashes("restart-1", "reload-1")
	crashLooping.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
			},
		},
	}

	tests := map[string]struct {
		reason         string
		pod            *corev1.Pod
		wantInProgress bool
		wantKnown      bool
	}{
		"applied":                {"SpecApplied", withHashes("restart-1", "reload-1"), false, true},
		"applied but crash-loop": {"SpecApplied", crashLooping, true, true},
		"restart change":         {"SpecChanged", withHashes("restart-0", "reload-1"), true, true},
		// An image or resource change replaces the pod without a config change.
		"spec change without config change": {
			"SpecChanged",
			withHashes("restart-1", "reload-1"),
			false,
			true,
		},
		"reload pending": {
			"ReloadPending",
			withHashes("restart-1", "reload-0"),
			true,
			true,
		},
		// The controller deletes the pod before replacing it, for any spec change.
		"pod missing": {"PodMissing", nil, false, false},
		"config error": {
			"ConfigError",
			withHashes("restart-1", "reload-1"),
			false,
			true,
		},
		// BuildFailed is not a config state, so the previous status stays.
		"build failed": {"BuildFailed", withHashes("restart-0", "reload-0"), false, false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			inProgress, known := liteConfigInProgress(
				metav1.Condition{Reason: tc.reason},
				tc.pod,
				rendered,
			)
			if inProgress != tc.wantInProgress || known != tc.wantKnown {
				t.Errorf("liteConfigInProgress(%s) = %v, %v; want %v, %v",
					tc.reason, inProgress, known, tc.wantInProgress, tc.wantKnown)
			}
		})
	}
}
