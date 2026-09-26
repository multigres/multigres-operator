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

	"github.com/multigres/testkit/assert"
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

func mustJSON(t *testing.T, set multigresv1alpha1.ComponentImages) string {
	t.Helper()
	raw, err := json.Marshal(set)
	assert.NewAborting(t).NoError(err)
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
		c := assert.NewCollecting(t)
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		r := newHarness(t, images.UpdateImmediate, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("test/pgctld:v2", cluster.Spec.Images.Postgres, "expected current default, got")
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceDefaults ||
			cluster.Status.Images.Effective != testImagesConfig(images.UpdateImmediate).Defaults {
			t.Errorf("unexpected effective image status: %+v", cluster.Status.Images)
		}
		if cond := pendingCond(cluster); cond == nil || cond.Status != metav1.ConditionFalse {
			t.Errorf("expected ImageRolloutPending=False, got %+v", cond)
		}
	})

	t.Run("lazy strategy holds recorded set without acknowledgement", func(t *testing.T) {
		c := assert.NewCollecting(t)
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("test/pgctld:v1", cluster.Spec.Images.Postgres, "expected held image, got")
		c.NotEq(
			cluster.Status.Images.AvailableRevision,
			cluster.Status.Images.AppliedRevision,
			"expected a pending rollout (applied != available)",
		)
		if cluster.Status.Images.Applied != old ||
			cluster.Status.Images.Available != testImagesConfig(images.UpdateLazy).Defaults {
			t.Errorf(
				"status does not expose applied and available sets: %+v",
				cluster.Status.Images,
			)
		}
		c.Eq(
			string(images.UpdateLazy),
			cluster.Status.Images.UpdateStrategy,
			"status must report the running strategy, got",
		)
		cond := pendingCond(cluster)
		c.False(cond == nil || cond.Status != metav1.ConditionTrue ||
			cond.Reason != multigresv1alpha1.ReasonAwaitingAcknowledgement, "expected ImageRolloutPending=True/AwaitingAcknowledgement, got %+v", cond)
	})

	t.Run("lazy strategy survives status loss via annotation", func(t *testing.T) {
		c := assert.NewCollecting(t)
		// Status wiped (backup/restore, kubectl replace); only the
		// annotation remains. The cluster must NOT roll to new defaults.
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("test/pgctld:v1", cluster.Spec.Images.Postgres, "status loss rolled the cluster")
	})

	t.Run("lazy strategy adopts when acknowledged revision matches", func(t *testing.T) {
		c := assert.NewCollecting(t)
		old := olderApplied()
		cfg := testImagesConfig(images.UpdateLazy)
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			AcknowledgedRevision: images.Revision(cfg.Defaults),
		}
		r := newHarness(t, images.UpdateLazy, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("test/pgctld:v2", cluster.Spec.Images.Postgres, "expected adopted image, got")
		if cond := pendingCond(cluster); cond == nil || cond.Status != metav1.ConditionFalse {
			t.Errorf("expected ImageRolloutPending=False after adoption, got %+v", cond)
		}
		// The durable record must move with the adoption.
		stored := &multigresv1alpha1.MultigresCluster{}
		c.Require().NoError(r.Get(context.Background(), key, stored))
		c.Eq(
			mustJSON(t, cfg.Defaults),
			stored.Annotations[metadata.AnnotationAppliedImages],
			"applied-images annotation not updated",
		)
	})

	t.Run("consumed acknowledgement is awaiting, not a mismatch", func(t *testing.T) {
		c := assert.NewCollecting(t)
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

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq(
			"test/pgctld:v1",
			cluster.Spec.Images.Postgres,
			"consumed acknowledgement rolled the cluster",
		)
		cond := pendingCond(cluster)
		c.False(
			cond == nil || cond.Reason != multigresv1alpha1.ReasonAwaitingAcknowledgement,
			"expected AwaitingAcknowledgement, got %+v",
			cond,
		)
		c.False(
			hasEvent(t, rec, "ImagesRevisionMismatch"),
			"consumed acknowledgement must not warn as a mismatch",
		)
	})

	t.Run("mismatched acknowledgement holds and reports RevisionMismatch", func(t *testing.T) {
		c := assert.NewCollecting(t)
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			AcknowledgedRevision: "not-a-real-revision",
		}
		r := newHarness(t, images.UpdateLazy, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq(
			"test/pgctld:v1",
			cluster.Spec.Images.Postgres,
			"mismatched acknowledgement rolled the cluster",
		)
		cond := pendingCond(cluster)
		c.False(
			cond == nil || cond.Reason != multigresv1alpha1.ReasonRevisionMismatch,
			"expected RevisionMismatch reason, got %+v",
			cond,
		)
	})

	t.Run("new cluster adopts immediately and records the set", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cluster := newCluster(nil, nil)
		r := newHarness(t, images.UpdateLazy, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("test/pgctld:v2", cluster.Spec.Images.Postgres, "expected current default, got")
		stored := &multigresv1alpha1.MultigresCluster{}
		c.Require().NoError(r.Get(context.Background(), key, stored))
		c.NotEq(
			"",
			stored.Annotations[metadata.AnnotationAppliedImages],
			"applied-images annotation not recorded for new cluster",
		)
	})

	t.Run("explicit spec pins always win", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cluster := newCluster(nil, nil)
		cluster.Spec.Images.Postgres = "pinned/pgctld:v0"
		r := newHarness(t, images.UpdateLazy, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("pinned/pgctld:v0", cluster.Spec.Images.Postgres, "explicit pin overwritten")
		c.Eq("test/multigres:v2", cluster.Spec.Images.Multiorch, "unset field not resolved")
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceMixed ||
			cluster.Status.Images.Effective.Postgres != "pinned/pgctld:v0" {
			t.Errorf("unexpected mixed image status: %+v", cluster.Status.Images)
		}
	})

	t.Run("partial unpin after fully pinned adopts current defaults", func(t *testing.T) {
		c := assert.NewAborting(t)
		cluster := newCluster(nil, nil)
		r := newHarness(t, images.UpdateLazy, cluster)
		counter := r.Client.(*patchCountingClient)

		old := olderApplied()
		r.Images.Defaults = old
		c.NoError(r.resolveImages(context.Background(), cluster))
		c.Eq(1, counter.patches, "initial default record patches")

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

		c.NoError(r.resolveImages(pinnedCtx, cluster))
		c.Eq(2, counter.patches, "pin transition patches")
		c.Eq(
			appliedImagesFullyPinned,
			cluster.Annotations[metadata.AnnotationAppliedImages],
			"applied-images annotation",
		)
		if sink.entries != 0 || hasEvent(t, rec, "") {
			t.Fatalf("pin transition emitted image activity: logs=%d", sink.entries)
		}
		if got := cluster.Status.Images; got.Source != multigresv1alpha1.ImageSourceExplicit ||
			got.Effective != pinned || got.Applied != (multigresv1alpha1.ComponentImages{}) ||
			got.AppliedRevision != "" || got.Available != (multigresv1alpha1.ComponentImages{}) ||
			got.AvailableRevision != "" || got.UpdateStrategy != "" {
			t.Fatalf("unexpected fully pinned status: %+v", got)
		}
		cond := pendingCond(cluster)
		c.False(cond == nil || cond.Status != metav1.ConditionFalse ||
			cond.Reason != multigresv1alpha1.ReasonFullyPinned, "expected FullyPinned condition, got %+v", cond)

		c.NoError(r.resolveImages(pinnedCtx, cluster))
		if counter.patches != 2 || sink.entries != 0 || hasEvent(t, rec, "") {
			t.Fatalf("steady pinned reconcile was not quiet: patches=%d logs=%d",
				counter.patches, sink.entries)
		}

		cluster.Spec.Images.Postgres = ""
		c.NoError(r.resolveImages(context.Background(), cluster))
		current := testImagesConfig(images.UpdateLazy).Defaults
		c.Eq(current.Postgres, cluster.Spec.Images.Postgres, "partial unpin restored")
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceMixed ||
			cluster.Status.Images.Effective.Postgres != current.Postgres {
			t.Fatalf("unexpected partial-unpin status: %+v", cluster.Status.Images)
		}
		stored := &multigresv1alpha1.MultigresCluster{}
		c.NoError(r.Get(context.Background(), key, stored))
		wantAnnotation := mustJSON(t, current)
		c.Eq(
			wantAnnotation,
			stored.Annotations[metadata.AnnotationAppliedImages],
			"partial unpin recorded",
		)
	})

	t.Run("fully pinned tombstone survives interrupted status update", func(t *testing.T) {
		c := assert.NewAborting(t)
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
		c.NoError(r.resolveImages(context.Background(), cluster))
		stored := &multigresv1alpha1.MultigresCluster{}
		c.NoError(r.Get(context.Background(), key, stored))
		c.Eq(
			old,
			stored.Status.Images.Applied,
			"test setup lost stale status: %+v",
			stored.Status.Images,
		)
		stored.Spec.Images.Postgres = ""
		c.NoError(r.Update(context.Background(), stored))
		fresh := &multigresv1alpha1.MultigresCluster{}
		c.NoError(r.Get(context.Background(), key, fresh))
		c.NoError(r.resolveImages(context.Background(), fresh))
		c.Eq(
			testImagesConfig(images.UpdateLazy).Defaults.Postgres,
			fresh.Spec.Images.Postgres,
			"unpin restored stale image",
		)
	})

	t.Run("fully pinned to fully unpinned adopts current defaults", func(t *testing.T) {
		c := assert.NewAborting(t)
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

		c.NoError(r.resolveImages(context.Background(), cluster))
		c.Eq(1, counter.patches, "pin transition patches")

		cluster.Spec.Images = multigresv1alpha1.ClusterImages{}
		c.NoError(r.resolveImages(context.Background(), cluster))
		current := testImagesConfig(images.UpdateLazy).Defaults
		c.Eq(current, images.FromSpec(cluster.Spec.Images), "full unpin resolved")
		if cluster.Status.Images.Source != multigresv1alpha1.ImageSourceDefaults ||
			cluster.Status.Images.Effective != current {
			t.Fatalf("unexpected full-unpin status: %+v", cluster.Status.Images)
		}
		stored := &multigresv1alpha1.MultigresCluster{}
		c.NoError(r.Get(context.Background(), key, stored))
		wantAnnotation := mustJSON(t, current)
		c.Eq(
			wantAnnotation,
			stored.Annotations[metadata.AnnotationAppliedImages],
			"full unpin recorded",
		)
	})

	t.Run("corrupt state fails closed under lazy strategy", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: "{not json",
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)
		rec := r.Recorder.(*record.FakeRecorder)
		counter := r.Client.(*patchCountingClient)

		err := r.resolveImages(context.Background(), cluster)
		c.Require().
			False(err == nil || !strings.Contains(err.Error(), "lazy update strategy"), "resolveImages() error = %v, want lazy state error", err)
		c.Eq("", cluster.Spec.Images.Postgres, "corrupt lazy state resolved image")
		c.Eq(0, counter.patches, "corrupt lazy state performed")
		c.True(
			hasEvent(t, rec, "ImagesRecordInvalid"),
			"expected ImagesRecordInvalid warning event",
		)
	})

	t.Run("empty annotation fails closed under lazy strategy", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: "",
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		c.Require().
			Error(r.resolveImages(context.Background(), cluster), "resolveImages() error = nil, want invalid lazy state error")
		c.True(
			hasEvent(t, rec, "ImagesRecordInvalid"),
			"expected ImagesRecordInvalid warning event",
		)
	})

	t.Run("corrupted annotation falls back to status and holds", func(t *testing.T) {
		c := assert.NewCollecting(t)
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: "{not json",
		}, &multigresv1alpha1.ImagesStatus{
			Applied:         old,
			AppliedRevision: images.Revision(old),
		})
		r := newHarness(t, images.UpdateLazy, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq(
			"test/pgctld:v1",
			cluster.Spec.Images.Postgres,
			"expected hold via status fallback, got",
		)
	})

	t.Run("partial annotation fails closed under lazy strategy", func(t *testing.T) {
		c := assert.NewCollecting(t)
		// A recorded set missing components must not resolve some components
		// from the record and drop the rest to compiled-in fallbacks.
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: `{"postgres":"test/pgctld:v1"}`,
		}, nil)
		r := newHarness(t, images.UpdateLazy, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		c.Require().
			Error(r.resolveImages(context.Background(), cluster), "resolveImages() error = nil, want invalid lazy state error")
		if cluster.Spec.Images.Postgres != "" || cluster.Spec.Images.Multiorch != "" {
			t.Errorf("partial lazy record resolved images: %+v", cluster.Spec.Images)
		}
		c.True(
			hasEvent(t, rec, "ImagesRecordInvalid"),
			"expected ImagesRecordInvalid warning event",
		)
	})

	t.Run("corrupt state recovers under immediate strategy", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: "{not json",
		}, nil)
		r := newHarness(t, images.UpdateImmediate, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("test/pgctld:v2", cluster.Spec.Images.Postgres, "expected current defaults, got")
		c.True(
			hasEvent(t, rec, "ImagesRecordInvalid"),
			"expected ImagesRecordInvalid warning event",
		)
	})

	t.Run("acknowledgement with immediate strategy warns that it is ignored", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cluster := newCluster(nil, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			AcknowledgedRevision: "abcdef123456",
		}
		r := newHarness(t, images.UpdateImmediate, cluster)
		rec := r.Recorder.(*record.FakeRecorder)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.True(
			hasEvent(t, rec, "ImagesAcknowledgementIgnored"),
			"expected ImagesAcknowledgementIgnored warning event",
		)
	})

	t.Run("per-cluster lazy overrides an immediate operator", func(t *testing.T) {
		c := assert.NewCollecting(t)
		// A single cluster can be frozen while the fleet follows the operator.
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			Strategy: string(images.UpdateLazy),
		}
		r := newHarness(t, images.UpdateImmediate, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("test/pgctld:v1", cluster.Spec.Images.Postgres, "per-cluster lazy did not hold")
		c.Eq(
			string(images.UpdateLazy),
			cluster.Status.Images.UpdateStrategy,
			"status must report the effective strategy, got",
		)
	})

	t.Run("per-cluster immediate overrides a lazy operator", func(t *testing.T) {
		c := assert.NewCollecting(t)
		old := olderApplied()
		cluster := newCluster(map[string]string{
			metadata.AnnotationAppliedImages: mustJSON(t, old),
		}, nil)
		cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
			Strategy: string(images.UpdateImmediate),
		}
		r := newHarness(t, images.UpdateLazy, cluster)

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq("test/pgctld:v2", cluster.Spec.Images.Postgres, "per-cluster immediate did not adopt")
	})

	t.Run("zero-value config falls back to compiled defaults", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cluster := newCluster(nil, nil)
		r := newHarness(t, "", cluster)
		r.Images = images.Config{}

		c.Require().NoError(r.resolveImages(context.Background(), cluster))
		c.Eq(
			multigresv1alpha1.DefaultPostgresImage,
			cluster.Spec.Images.Postgres,
			"expected compiled default, got",
		)
	})
}

// TestReconcile_LazyImageRollout runs the full Reconcile loop and asserts the
// contract that matters: under the lazy strategy, child CRs keep the recorded
// image set until spec.imageUpdatePolicy.acknowledgedRevision names the new
// revision, and follow it once it does.
func TestReconcile_LazyImageRollout(t *testing.T) {
	ck := assert.NewAborting(t)
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
		ck := assert.NewCollecting(t)
		cells := &multigresv1alpha1.CellList{}
		ck.Require().NoError(c.List(t.Context(), cells))
		ck.Require().NotEmpty(cells.Items, "no Cell children created")
		for _, cell := range cells.Items {
			ck.Eq(
				wantGateway,
				cell.Spec.Images.Multigateway,
				"cell %s multigateway = %s, want",
				cell.Name,
				cell.Spec.Images.Multigateway,
			)
		}
		tgs := &multigresv1alpha1.TableGroupList{}
		ck.Require().NoError(c.List(t.Context(), tgs))
		ck.Require().NotEmpty(tgs.Items, "no TableGroup children created")
		for _, tg := range tgs.Items {
			ck.Eq(
				wantOrch,
				tg.Spec.Images.Multiorch,
				"tablegroup %s multiorch = %s, want",
				tg.Name,
				tg.Spec.Images.Multiorch,
			)
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
	ck.NoError(c.Get(t.Context(), req.NamespacedName, cluster))
	cluster.Spec.ImageUpdatePolicy = &multigresv1alpha1.ImageUpdatePolicy{
		AcknowledgedRevision: images.Revision(testImagesConfig(images.UpdateLazy).Defaults),
	}
	ck.NoError(c.Update(t.Context(), cluster))
	_, err := r.Reconcile(t.Context(), req)
	ck.NoError(err, "second reconcile")
	assertChildImages(t, "test/multigres:v2", "test/multigres:v2")
}
