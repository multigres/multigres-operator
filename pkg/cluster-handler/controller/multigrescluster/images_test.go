package multigrescluster

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/images"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

type patchCountingClient struct {
	client.Client
	patches int
}

func (c *patchCountingClient) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	c.patches++
	return c.Client.Patch(ctx, obj, patch, opts...)
}

type countingLogSink struct {
	entries int
}

func (*countingLogSink) Init(logr.RuntimeInfo) {}

func (*countingLogSink) Enabled(int) bool { return true }

func (s *countingLogSink) Info(int, string, ...any) { s.entries++ }

func (s *countingLogSink) Error(error, string, ...any) { s.entries++ }

func (s *countingLogSink) WithValues(...any) logr.LogSink { return s }

func (s *countingLogSink) WithName(string) logr.LogSink { return s }

func testImagesConfig(strategy images.UpdateStrategy) images.Config {
	return images.Config{
		Defaults: multigresv1alpha1.ComponentImages{
			Postgres:      "test/pgctld:v2",
			Multiadmin:    "test/multigres:v2",
			MultiadminWeb: "test/web:v2",
			Multiorch:     "test/multigres:v2",
			Multipooler:   "test/multigres:v2",
			Multigateway:  "test/multigres:v2",
		},
		Strategy: strategy,
	}
}

func olderApplied() multigresv1alpha1.ComponentImages {
	return multigresv1alpha1.ComponentImages{
		Postgres:      "test/pgctld:v1",
		Multiadmin:    "test/multigres:v1",
		MultiadminWeb: "test/web:v1",
		Multiorch:     "test/multigres:v1",
		Multipooler:   "test/multigres:v1",
		Multigateway:  "test/multigres:v1",
	}
}

// withPgBackRestd is the set as it appears in status.images.effective, which
// alone carries the sidecar image, derived from the set's multipooler.
func withPgBackRestd(set multigresv1alpha1.ComponentImages) multigresv1alpha1.ComponentImages {
	set.PgBackRestd = set.Multipooler
	return set
}

func mustJSON(t *testing.T, set multigresv1alpha1.ComponentImages) string {
	t.Helper()
	raw, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func clusterImages(set multigresv1alpha1.ComponentImages) multigresv1alpha1.ClusterImages {
	return multigresv1alpha1.ClusterImages{
		Postgres:      set.Postgres,
		Multiadmin:    set.Multiadmin,
		MultiadminWeb: set.MultiadminWeb,
		Multiorch:     set.Multiorch,
		Multipooler:   set.Multipooler,
		Multigateway:  set.Multigateway,
		PgBackRestd:   set.PgBackRestd,
	}
}

// hasEvent drains the fake recorder and reports whether any buffered event
// mentions the given reason.
func hasEvent(t *testing.T, rec *record.FakeRecorder, reason string) bool {
	t.Helper()
	for {
		select {
		case ev := <-rec.Events:
			if strings.Contains(ev, reason) {
				return true
			}
		default:
			return false
		}
	}
}

func TestResolveImages(t *testing.T) {
	key := types.NamespacedName{Name: "c1", Namespace: "default"}

	// newHarness returns a reconciler whose fake client stores the given
	// cluster, so resolveImages can patch the applied-images annotation.
	newHarness := func(t *testing.T, strategy images.UpdateStrategy, cluster *multigresv1alpha1.MultigresCluster) *MultigresClusterReconciler {
		t.Helper()
		scheme := setupScheme()
		baseClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cluster).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
			Build()
		c := &patchCountingClient{Client: baseClient}
		return &MultigresClusterReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
			Images:   testImagesConfig(strategy),
		}
	}

	newCluster := func(annotations map[string]string, status *multigresv1alpha1.ImagesStatus) *multigresv1alpha1.MultigresCluster {
		return &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "c1",
				Namespace:   "default",
				Annotations: annotations,
			},
			Status: multigresv1alpha1.MultigresClusterStatus{Images: status},
		}
	}

	pendingCond := func(cluster *multigresv1alpha1.MultigresCluster) *metav1.Condition {
		return meta.FindStatusCondition(
			cluster.Status.Conditions,
			multigresv1alpha1.ConditionImageRolloutPending,
		)
	}

	t.Run("immediate strategy adopts current defaults", func(t *testing.T) {
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		r := newHarness(t, images.UpdateImmediate, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v2" {
			t.Errorf("expected current default, got %s", cluster.Spec.Images.Postgres)
		}
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceDefaults ||
			cluster.Status.Images.Effective != withPgBackRestd(
				testImagesConfig(images.UpdateImmediate).Defaults,
			) {
			t.Errorf("unexpected effective image status: %+v", cluster.Status.Images)
		}
		if cond := pendingCond(cluster); cond == nil || cond.Status != metav1.ConditionFalse {
			t.Errorf("expected ImageRolloutPending=False, got %+v", cond)
		}
	})

	t.Run("lazy strategy holds recorded set without acknowledgement", func(t *testing.T) {
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v1" {
			t.Errorf("expected held image, got %s", cluster.Spec.Images.Postgres)
		}
		if cluster.Status.Images.AppliedRevision == cluster.Status.Images.AvailableRevision {
			t.Error("expected a pending rollout (applied != available)")
		}
		if cluster.Status.Images.Applied != old ||
			cluster.Status.Images.Available != testImagesConfig(images.UpdateLazy).Defaults {
			t.Errorf(
				"status does not expose applied and available sets: %+v",
				cluster.Status.Images,
			)
		}
		if cluster.Status.Images.UpdateStrategy != string(images.UpdateLazy) {
			t.Errorf("status must report the running strategy, got %q",
				cluster.Status.Images.UpdateStrategy)
		}
		cond := pendingCond(cluster)
		if cond == nil || cond.Status != metav1.ConditionTrue ||
			cond.Reason != multigresv1alpha1.ReasonAwaitingAcknowledgement {
			t.Errorf("expected ImageRolloutPending=True/AwaitingAcknowledgement, got %+v", cond)
		}
	})

	t.Run(
		"lazy record without pgbackrestd resolves it to the record's multipooler",
		func(t *testing.T) {
			// The record holds an older multipooler while the defaults moved on:
			// pgbackrestd must follow the held multipooler, not the new default.
			old := olderApplied()
			cluster := newCluster(map[string]string{
				metadata.AnnotationAppliedImages: mustJSON(t, old),
			}, nil)
			r := newHarness(t, images.UpdateLazy, cluster)

			if err := r.resolveImages(context.Background(), cluster); err != nil {
				t.Fatal(err)
			}
			if cluster.Spec.Images.PgBackRestd != old.Multipooler {
				t.Errorf("pgbackrestd = %q, want record multipooler %q",
					cluster.Spec.Images.PgBackRestd, old.Multipooler)
			}
			if cond := pendingCond(cluster); cond == nil || cond.Status != metav1.ConditionTrue {
				t.Errorf("expected ImageRolloutPending=True while held, got %+v", cond)
			}
			if got := cluster.Annotations[metadata.AnnotationAppliedImages]; got != mustJSON(
				t,
				old,
			) {
				t.Errorf("record was rewritten: %s", got)
			}
		},
	)

	t.Run(
		"operator upgrade moving multipooler moves pgbackrestd in the same step",
		func(t *testing.T) {
			cluster := newCluster(map[string]string{
				metadata.AnnotationAppliedImages: mustJSON(t, olderApplied()),
			}, nil)
			cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
				AcknowledgedRevision: images.Revision(testImagesConfig(images.UpdateLazy).Defaults),
			}
			r := newHarness(t, images.UpdateLazy, cluster)

			if err := r.resolveImages(context.Background(), cluster); err != nil {
				t.Fatal(err)
			}
			want := testImagesConfig(images.UpdateLazy).Defaults.Multipooler
			if cluster.Spec.Images.Multipooler != want || cluster.Spec.Images.PgBackRestd != want {
				t.Errorf("multipooler=%q pgbackrestd=%q, want both %q",
					cluster.Spec.Images.Multipooler, cluster.Spec.Images.PgBackRestd, want)
			}
		},
	)

	t.Run("immediate default set derives pgbackrestd from its multipooler", func(t *testing.T) {
		cluster := newCluster(nil, nil)
		r := newHarness(t, images.UpdateImmediate, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		want := testImagesConfig(images.UpdateImmediate).Defaults.Multipooler
		if cluster.Spec.Images.PgBackRestd != want ||
			cluster.Status.Images.Effective.PgBackRestd != want {
			t.Errorf("spec=%q effective=%q, want %q", cluster.Spec.Images.PgBackRestd,
				cluster.Status.Images.Effective.PgBackRestd, want)
		}
		if cluster.Status.Images.Applied.PgBackRestd != "" {
			t.Errorf("applied set must not carry pgbackrestd, got %q",
				cluster.Status.Images.Applied.PgBackRestd)
		}
	})

	t.Run("explicit pgbackrestd wins and is not rewritten by the set", func(t *testing.T) {
		cluster := newCluster(nil, nil)
		cluster.Spec.Images.PgBackRestd = "custom/multigres:v9"
		r := newHarness(t, images.UpdateImmediate, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.PgBackRestd != "custom/multigres:v9" ||
			cluster.Status.Images.Effective.PgBackRestd != "custom/multigres:v9" {
			t.Errorf("explicit value lost: spec=%q effective=%q", cluster.Spec.Images.PgBackRestd,
				cluster.Status.Images.Effective.PgBackRestd)
		}
	})

	t.Run("fully pinned cluster follows its pinned multipooler, not defaults", func(t *testing.T) {
		cluster := newCluster(nil, nil)
		pinned := olderApplied()
		cluster.Spec.Images = clusterImages(pinned)
		r := newHarness(t, images.UpdateImmediate, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.PgBackRestd != pinned.Multipooler ||
			cluster.Status.Images.Effective.PgBackRestd != pinned.Multipooler {
			t.Errorf("pgbackrestd not derived from pin: spec=%q effective=%q",
				cluster.Spec.Images.PgBackRestd, cluster.Status.Images.Effective.PgBackRestd)
		}
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceExplicit {
			t.Errorf("source = %q, want explicit", cluster.Status.Images.Source)
		}
	})

	t.Run("lazy strategy survives status loss via annotation", func(t *testing.T) {
		// Status wiped (backup/restore, kubectl replace); only the
		// annotation remains. The cluster must NOT roll to new defaults.
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v1" {
			t.Errorf("status loss rolled the cluster: %s", cluster.Spec.Images.Postgres)
		}
	})

	t.Run("lazy strategy adopts when acknowledged revision matches", func(t *testing.T) {
		old := olderApplied()
		cfg := testImagesConfig(images.UpdateLazy)
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			AcknowledgedRevision: images.Revision(cfg.Defaults),
		}
		r := newHarness(t, images.UpdateLazy, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v2" {
			t.Errorf("expected adopted image, got %s", cluster.Spec.Images.Postgres)
		}
		if cond := pendingCond(cluster); cond == nil || cond.Status != metav1.ConditionFalse {
			t.Errorf("expected ImageRolloutPending=False after adoption, got %+v", cond)
		}
		// The durable record must move with the adoption.
		stored := &multigresv1alpha1.MultigresCluster{}
		if err := r.Get(context.Background(), key, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Annotations[metadata.AnnotationAppliedImages] != mustJSON(t, cfg.Defaults) {
			t.Errorf("applied-images annotation not updated: %s",
				stored.Annotations[metadata.AnnotationAppliedImages])
		}
	})

	t.Run("consumed acknowledgement is awaiting, not a mismatch", func(t *testing.T) {
		// The normal state between rollouts: the previous acknowledgment was
		// adopted and is still in the spec when a newer set becomes available.
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			AcknowledgedRevision: images.Revision(old),
		}
		r := newHarness(t, images.UpdateLazy, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v1" {
			t.Errorf("consumed acknowledgement rolled the cluster: %s",
				cluster.Spec.Images.Postgres)
		}
		cond := pendingCond(cluster)
		if cond == nil || cond.Reason != multigresv1alpha1.ReasonAwaitingAcknowledgement {
			t.Errorf("expected AwaitingAcknowledgement, got %+v", cond)
		}
		if hasEvent(t, rec, "ImagesRevisionMismatch") {
			t.Error("consumed acknowledgement must not warn as a mismatch")
		}
	})

	t.Run("mismatched acknowledgement holds and reports RevisionMismatch", func(t *testing.T) {
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			AcknowledgedRevision: "not-a-real-revision",
		}
		r := newHarness(t, images.UpdateLazy, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v1" {
			t.Errorf("mismatched acknowledgement rolled the cluster: %s",
				cluster.Spec.Images.Postgres)
		}
		cond := pendingCond(cluster)
		if cond == nil || cond.Reason != multigresv1alpha1.ReasonRevisionMismatch {
			t.Errorf("expected RevisionMismatch reason, got %+v", cond)
		}
	})

	t.Run("new cluster adopts immediately and records the set", func(t *testing.T) {
		cluster := newCluster(nil, nil)
		r := newHarness(t, images.UpdateLazy, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v2" {
			t.Errorf("expected current default, got %s", cluster.Spec.Images.Postgres)
		}
		stored := &multigresv1alpha1.MultigresCluster{}
		if err := r.Get(context.Background(), key, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Annotations[metadata.AnnotationAppliedImages] == "" {
			t.Error("applied-images annotation not recorded for new cluster")
		}
	})

	t.Run("explicit spec pins always win", func(t *testing.T) {
		cluster := newCluster(nil, nil)
		cluster.Spec.Images.Postgres = "pinned/pgctld:v0"
		r := newHarness(t, images.UpdateLazy, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "pinned/pgctld:v0" {
			t.Errorf("explicit pin overwritten: %s", cluster.Spec.Images.Postgres)
		}
		if cluster.Spec.Images.Multiorch != "test/multigres:v2" {
			t.Errorf("unset field not resolved: %s", cluster.Spec.Images.Multiorch)
		}
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceMixed ||
			cluster.Status.Images.Effective.Postgres != "pinned/pgctld:v0" {
			t.Errorf("unexpected mixed image status: %+v", cluster.Status.Images)
		}
	})

	t.Run("partial unpin after fully pinned adopts current defaults", func(t *testing.T) {
		cluster := newCluster(nil, nil)
		r := newHarness(t, images.UpdateLazy, cluster)
		counter := r.Client.(*patchCountingClient)

		old := olderApplied()
		r.Images.Defaults = old
		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if counter.patches != 1 {
			t.Fatalf("initial default record patches = %d, want 1", counter.patches)
		}

		pinned := multigresv1alpha1.ComponentImages{
			Postgres:      "pinned/pgctld:v3",
			Multiadmin:    "pinned/multigres:v3",
			MultiadminWeb: "pinned/web:v3",
			Multiorch:     "pinned/multigres:v3",
			Multipooler:   "pinned/multigres:v3",
			Multigateway:  "pinned/multigres:v3",
		}
		cluster.Spec.Images = clusterImages(pinned)
		r.Images = testImagesConfig(images.UpdateLazy)
		sink := &countingLogSink{}
		pinnedCtx := logr.NewContext(context.Background(), logr.New(sink))
		rec := r.Recorder.(*record.FakeRecorder)

		if err := r.resolveImages(pinnedCtx, cluster); err != nil {
			t.Fatal(err)
		}
		if counter.patches != 2 {
			t.Fatalf("pin transition patches = %d, want exactly 2 total", counter.patches)
		}
		if got := cluster.Annotations[metadata.AnnotationAppliedImages]; got != appliedImagesFullyPinned {
			t.Fatalf("applied-images annotation = %q, want fully-pinned tombstone", got)
		}
		if sink.entries != 0 || hasEvent(t, rec, "") {
			t.Fatalf("pin transition emitted image activity: logs=%d", sink.entries)
		}
		if got := cluster.Status.Images; got.Source != multigresv1alpha1.ImageSourceExplicit ||
			got.Effective != withPgBackRestd(
				pinned,
			) || got.Applied != (multigresv1alpha1.ComponentImages{}) ||
			got.AppliedRevision != "" || got.Available != (multigresv1alpha1.ComponentImages{}) ||
			got.AvailableRevision != "" || got.UpdateStrategy != "" {
			t.Fatalf("unexpected fully pinned status: %+v", got)
		}
		cond := pendingCond(cluster)
		if cond == nil || cond.Status != metav1.ConditionFalse ||
			cond.Reason != multigresv1alpha1.ReasonFullyPinned {
			t.Fatalf("expected FullyPinned condition, got %+v", cond)
		}

		if err := r.resolveImages(pinnedCtx, cluster); err != nil {
			t.Fatal(err)
		}
		if counter.patches != 2 || sink.entries != 0 || hasEvent(t, rec, "") {
			t.Fatalf("steady pinned reconcile was not quiet: patches=%d logs=%d",
				counter.patches, sink.entries)
		}

		cluster.Spec.Images.Postgres = ""
		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		current := testImagesConfig(images.UpdateLazy).Defaults
		if cluster.Spec.Images.Postgres != current.Postgres {
			t.Fatalf("partial unpin restored %q, want current default %q",
				cluster.Spec.Images.Postgres, current.Postgres)
		}
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceMixed ||
			cluster.Status.Images.Effective.Postgres != current.Postgres {
			t.Fatalf("unexpected partial-unpin status: %+v", cluster.Status.Images)
		}
		stored := &multigresv1alpha1.MultigresCluster{}
		if err := r.Get(context.Background(), key, stored); err != nil {
			t.Fatal(err)
		}
		wantAnnotation := mustJSON(t, current)
		if got := stored.Annotations[metadata.AnnotationAppliedImages]; got != wantAnnotation {
			t.Fatalf("partial unpin recorded %q, want current defaults", got)
		}
	})

	t.Run("fully pinned tombstone survives interrupted status update", func(t *testing.T) {
		old := olderApplied()
		pinned := multigresv1alpha1.ComponentImages{
			Postgres:      "pinned/pgctld:v3",
			Multiadmin:    "pinned/multigres:v3",
			MultiadminWeb: "pinned/web:v3",
			Multiorch:     "pinned/multigres:v3",
			Multipooler:   "pinned/multigres:v3",
			Multigateway:  "pinned/multigres:v3",
		}
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, &multigresv1alpha1.ImagesStatus{
			Applied:         old,
			AppliedRevision: images.Revision(old),
		})
		cluster.Spec.Images = clusterImages(pinned)
		r := newHarness(t, images.UpdateLazy, cluster)

		// Simulate reconciliation stopping after image resolution but before
		// the in-memory fully pinned status is persisted.
		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		stored := &multigresv1alpha1.MultigresCluster{}
		if err := r.Get(context.Background(), key, stored); err != nil {
			t.Fatal(err)
		}
		if stored.Status.Images.Applied != old {
			t.Fatalf("test setup lost stale status: %+v", stored.Status.Images)
		}
		stored.Spec.Images.Postgres = ""
		if err := r.Update(context.Background(), stored); err != nil {
			t.Fatal(err)
		}
		fresh := &multigresv1alpha1.MultigresCluster{}
		if err := r.Get(context.Background(), key, fresh); err != nil {
			t.Fatal(err)
		}
		if err := r.resolveImages(context.Background(), fresh); err != nil {
			t.Fatal(err)
		}
		if fresh.Spec.Images.Postgres != testImagesConfig(images.UpdateLazy).Defaults.Postgres {
			t.Fatalf("unpin restored stale image %q", fresh.Spec.Images.Postgres)
		}
	})

	t.Run("fully pinned to fully unpinned adopts current defaults", func(t *testing.T) {
		pinned := multigresv1alpha1.ComponentImages{
			Postgres:      "pinned/pgctld:v3",
			Multiadmin:    "pinned/multigres:v3",
			MultiadminWeb: "pinned/web:v3",
			Multiorch:     "pinned/multigres:v3",
			Multipooler:   "pinned/multigres:v3",
			Multigateway:  "pinned/multigres:v3",
		}
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.Images = clusterImages(pinned)
		r := newHarness(t, images.UpdateLazy, cluster)
		counter := r.Client.(*patchCountingClient)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if counter.patches != 1 {
			t.Fatalf("pin transition patches = %d, want 1", counter.patches)
		}

		cluster.Spec.Images = multigresv1alpha1.ClusterImages{}
		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		defaults := testImagesConfig(images.UpdateLazy).Defaults
		current := withPgBackRestd(defaults)
		if got := images.FromSpec(cluster.Spec.Images); got != current {
			t.Fatalf("full unpin resolved %+v, want current defaults %+v", got, current)
		}
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceDefaults ||
			cluster.Status.Images.Effective != current {
			t.Fatalf("unexpected full-unpin status: %+v", cluster.Status.Images)
		}
		stored := &multigresv1alpha1.MultigresCluster{}
		if err := r.Get(context.Background(), key, stored); err != nil {
			t.Fatal(err)
		}
		// The record never carries the derived sidecar image.
		wantAnnotation := mustJSON(t, defaults)
		if got := stored.Annotations[metadata.AnnotationAppliedImages]; got != wantAnnotation {
			t.Fatalf("full unpin recorded %q, want current defaults", got)
		}
	})

	t.Run("corrupt state fails closed under lazy strategy", func(t *testing.T) {
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: "{not json",
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)
		rec := r.Recorder.(*record.FakeRecorder)
		counter := r.Client.(*patchCountingClient)

		err := r.resolveImages(context.Background(), cluster)
		if err == nil || !strings.Contains(err.Error(), "lazy update strategy") {
			t.Fatalf("resolveImages() error = %v, want lazy state error", err)
		}
		if cluster.Spec.Images.Postgres != "" {
			t.Errorf("corrupt lazy state resolved image %q", cluster.Spec.Images.Postgres)
		}
		if counter.patches != 0 {
			t.Errorf("corrupt lazy state performed %d patches, want 0", counter.patches)
		}
		if !hasEvent(t, rec, "ImagesRecordInvalid") {
			t.Error("expected ImagesRecordInvalid warning event")
		}
	})

	t.Run("empty annotation fails closed under lazy strategy", func(t *testing.T) {
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: "",
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		if err := r.resolveImages(context.Background(), cluster); err == nil {
			t.Fatal("resolveImages() error = nil, want invalid lazy state error")
		}
		if !hasEvent(t, rec, "ImagesRecordInvalid") {
			t.Error("expected ImagesRecordInvalid warning event")
		}
	})

	t.Run("corrupted annotation falls back to status and holds", func(t *testing.T) {
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: "{not json",
		}, &multigresv1alpha1.ImagesStatus{
			Applied:         old,
			AppliedRevision: images.Revision(old),
		})
		r := newHarness(t, images.UpdateLazy, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v1" {
			t.Errorf("expected hold via status fallback, got %s", cluster.Spec.Images.Postgres)
		}
	})

	t.Run("partial annotation fails closed under lazy strategy", func(t *testing.T) {
		// A recorded set missing components must not resolve some components
		// from the record and drop the rest to compiled-in fallbacks.
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: `{"postgres":"test/pgctld:v1"}`,
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		if err := r.resolveImages(context.Background(), cluster); err == nil {
			t.Fatal("resolveImages() error = nil, want invalid lazy state error")
		}
		if cluster.Spec.Images.Postgres != "" || cluster.Spec.Images.Multiorch != "" {
			t.Errorf("partial lazy record resolved images: %+v", cluster.Spec.Images)
		}
		if !hasEvent(t, rec, "ImagesRecordInvalid") {
			t.Error("expected ImagesRecordInvalid warning event")
		}
	})

	t.Run("corrupt state recovers under immediate strategy", func(t *testing.T) {
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: "{not json",
		}, nil)
		r := newHarness(t, images.UpdateImmediate, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v2" {
			t.Errorf("expected current defaults, got %s", cluster.Spec.Images.Postgres)
		}
		if !hasEvent(t, rec, "ImagesRecordInvalid") {
			t.Error("expected ImagesRecordInvalid warning event")
		}
	})

	t.Run("acknowledgement with immediate strategy warns that it is ignored", func(t *testing.T) {
		cluster := newCluster(nil, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			AcknowledgedRevision: "abcdef123456",
		}
		r := newHarness(t, images.UpdateImmediate, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if !hasEvent(t, rec, "ImagesAcknowledgementIgnored") {
			t.Error("expected ImagesAcknowledgementIgnored warning event")
		}
	})

	t.Run("per-cluster lazy overrides an immediate operator", func(t *testing.T) {
		// A single cluster can be frozen while the fleet follows the operator.
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			Strategy: string(images.UpdateLazy),
		}
		r := newHarness(t, images.UpdateImmediate, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v1" {
			t.Errorf("per-cluster lazy did not hold: %s", cluster.Spec.Images.Postgres)
		}
		if cluster.Status.Images.UpdateStrategy != string(images.UpdateLazy) {
			t.Errorf("status must report the effective strategy, got %q",
				cluster.Status.Images.UpdateStrategy)
		}
	})

	t.Run("per-cluster immediate overrides a lazy operator", func(t *testing.T) {
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			Strategy: string(images.UpdateImmediate),
		}
		r := newHarness(t, images.UpdateLazy, cluster)

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != "test/pgctld:v2" {
			t.Errorf("per-cluster immediate did not adopt: %s", cluster.Spec.Images.Postgres)
		}
	})

	t.Run("zero-value config falls back to compiled defaults", func(t *testing.T) {
		cluster := newCluster(nil, nil)
		r := newHarness(t, "", cluster)
		r.Images = images.Config{}

		if err := r.resolveImages(context.Background(), cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Spec.Images.Postgres != multigresv1alpha1.DefaultPostgresImage {
			t.Errorf("expected compiled default, got %s", cluster.Spec.Images.Postgres)
		}
	})
}

// TestReconcile_LazyImageRollout runs the full Reconcile loop and asserts the
// contract that matters: under the lazy strategy, child CRs keep the recorded
// image set until spec.imageUpdatePolicy.acknowledgedRevision names the new
// revision, and follow it once it does.
func TestReconcile_LazyImageRollout(t *testing.T) {
	coreTpl, cellTpl, shardTpl, baseCluster, clusterName, namespace := setupFixtures(t)
	// The fixture pins spec.images; this test is about operator defaults.
	baseCluster.Spec.Images = multigresv1alpha1.ClusterImages{}
	old := olderApplied()
	baseCluster.Annotations = map[string]string{
		metadata.AnnotationAppliedImages: mustJSON(t, old),
	}

	scheme := setupScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(coreTpl, cellTpl, shardTpl, baseCluster).
		WithStatusSubresource(
			&multigresv1alpha1.MultigresCluster{},
			&multigresv1alpha1.Cell{},
			&multigresv1alpha1.TableGroup{},
			&multigresv1alpha1.TopoServer{},
		).
		Build()
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
		Images:   testImagesConfig(images.UpdateLazy),
		CreateTopoStore: func(_ multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			_, factory := memorytopo.NewServerAndFactory(t.Context())
			return topoclient.NewWithFactory(
				factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
			), nil
		},
	}
	req := ctrl.Request{
		NamespacedName: types.NamespacedName{Name: clusterName, Namespace: namespace},
	}

	assertChildImages := func(t *testing.T, wantGateway, wantOrch multigresv1alpha1.ImageRef) {
		t.Helper()
		cells := &multigresv1alpha1.CellList{}
		if err := c.List(t.Context(), cells); err != nil {
			t.Fatal(err)
		}
		if len(cells.Items) == 0 {
			t.Fatal("no Cell children created")
		}
		for _, cell := range cells.Items {
			if cell.Spec.Images.Multigateway != wantGateway {
				t.Errorf("cell %s multigateway = %s, want %s",
					cell.Name, cell.Spec.Images.Multigateway, wantGateway)
			}
		}
		tgs := &multigresv1alpha1.TableGroupList{}
		if err := c.List(t.Context(), tgs); err != nil {
			t.Fatal(err)
		}
		if len(tgs.Items) == 0 {
			t.Fatal("no TableGroup children created")
		}
		for _, tg := range tgs.Items {
			if tg.Spec.Images.Multiorch != wantOrch {
				t.Errorf("tablegroup %s multiorch = %s, want %s",
					tg.Name, tg.Spec.Images.Multiorch, wantOrch)
			}
		}
	}

	// Pass 1: nothing acknowledged — children must be built from the recorded
	// old set, not the operator's current defaults.
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	assertChildImages(t, "test/multigres:v1", "test/multigres:v1")

	// Pass 2: available revision acknowledged in the spec — children must follow.
	cluster := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(t.Context(), req.NamespacedName, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
		AcknowledgedRevision: images.Revision(testImagesConfig(images.UpdateLazy).Defaults),
	}
	if err := c.Update(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	assertChildImages(t, "test/multigres:v2", "test/multigres:v2")
}
