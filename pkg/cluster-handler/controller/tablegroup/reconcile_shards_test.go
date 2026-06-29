package tablegroup

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// TestShardStepsPrune covers the retained PendingDeletion handshake for child
// Shards no longer in the spec. It verifies pruning annotates once, waits for
// ReadyForDeletion, deletes only after drain, and skips desired children.
func TestShardStepsPrune(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	baseTG, _, namespace, clusterName, dbName, tgLabelName := setupFixtures(t)

	shardName := name.JoinWithConstraints(
		name.DefaultConstraints,
		clusterName,
		dbName,
		tgLabelName,
		"shard-0",
	)

	childShardLabels := map[string]string{
		"multigres.com/cluster":    clusterName,
		"multigres.com/database":   dbName,
		"multigres.com/tablegroup": tgLabelName,
	}

	// annotation stamped by a prior reconcile; must be preserved, not re-stamped.
	const oldAnnotation = "2026-01-01T00:00:00Z"

	// child builds a no-longer-desired child Shard. withAnnotation seeds the
	// PendingDeletion annotation; ready additionally sets ReadyForDeletion.
	child := func(withAnnotation, ready bool) *multigresv1alpha1.Shard {
		s := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      shardName,
				Namespace: namespace,
				Labels:    childShardLabels,
			},
			Spec: multigresv1alpha1.ShardSpec{ShardName: "shard-0"},
		}
		if withAnnotation {
			s.Annotations = map[string]string{
				multigresv1alpha1.AnnotationPendingDeletion: oldAnnotation,
			}
		}
		if ready {
			meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{
				Type:    multigresv1alpha1.ConditionReadyForDeletion,
				Status:  metav1.ConditionTrue,
				Reason:  "Drained",
				Message: "All pods drained",
			})
		}
		return s
	}

	tests := map[string]struct {
		child                  *multigresv1alpha1.Shard
		stillDesired           bool // when true the spec keeps shard-0 so pruning must skip it
		wantPatches            int64
		wantDeletes            int64
		wantPending            bool
		wantDeleted            bool
		wantEvent              string
		wantAnnotation         string // exact PendingDeletion value expected when child survives
		wantAnnotationNonEmpty bool   // when set, only require a (freshly stamped) non-empty value
	}{
		// With no annotation yet, pruning annotates the child, marks it pending,
		// and emits an event without deleting.
		"orphan without annotation gets annotated and pends": {
			child:                  child(false, false),
			wantPatches:            1,
			wantDeletes:            0,
			wantPending:            true,
			wantDeleted:            false,
			wantEvent:              "Normal PendingDeletion",
			wantAnnotationNonEmpty: true,
		},
		// Already annotated but not ready, so it stays pending without re-stamping
		// or deleting.
		"orphan annotated but not ready stays pending, not double-driven": {
			child:          child(true, false),
			wantPatches:    0,
			wantDeletes:    0,
			wantPending:    true,
			wantDeleted:    false,
			wantAnnotation: oldAnnotation,
		},
		// Annotated and drained, so it's deleted once without re-stamping.
		"orphan annotated and ReadyForDeletion is deleted": {
			child:       child(true, true),
			wantPatches: 0,
			wantDeletes: 1,
			wantPending: false,
			wantDeleted: true,
		},
		// A child that's still desired is applied but never pruned: the apply is
		// the single Patch, pruning leaves it untouched with no PendingDeletion.
		"desired child is applied and skipped by pruning": {
			child:          child(false, false),
			stillDesired:   true,
			wantPatches:    1,
			wantDeletes:    0,
			wantPending:    false,
			wantDeleted:    false,
			wantAnnotation: "",
		},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			t.Parallel()

			var shardPatchCount atomic.Int64
			var shardDeleteCount atomic.Int64

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.child).
				WithStatusSubresource(
					&multigresv1alpha1.TableGroup{},
					&multigresv1alpha1.Shard{},
				).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(
						ctx context.Context,
						cli client.WithWatch,
						obj client.Object,
						patch client.Patch,
						opts ...client.PatchOption,
					) error {
						if _, ok := obj.(*multigresv1alpha1.Shard); ok {
							shardPatchCount.Add(1)
						}
						return cli.Patch(ctx, obj, patch, opts...)
					},
					Delete: func(
						ctx context.Context,
						cli client.WithWatch,
						obj client.Object,
						opts ...client.DeleteOption,
					) error {
						if _, ok := obj.(*multigresv1alpha1.Shard); ok {
							shardDeleteCount.Add(1)
						}
						return cli.Delete(ctx, obj, opts...)
					},
				}).
				Build()

			recorder := record.NewFakeRecorder(100)
			reconciler := &TableGroupReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: recorder,
			}

			tg := baseTG.DeepCopy()
			if !tc.stillDesired {
				// Drop the desired shard so the seeded child becomes an orphan.
				tg.Spec.Shards = []multigresv1alpha1.ShardResolvedSpec{}
			}

			rc := &reconcileContext{
				r:  reconciler,
				tg: tg,
			}
			for _, s := range []step{
				stepListChildShards,
				stepApplyDesiredShards,
				stepReconcileUndesired,
			} {
				if _, err := s(t.Context(), rc); err != nil {
					t.Fatalf("shard step returned error: %v", err)
				}
			}

			if got := shardPatchCount.Load(); got != tc.wantPatches {
				t.Errorf("Patch count mismatch: got %d, want %d", got, tc.wantPatches)
			}
			if got := shardDeleteCount.Load(); got != tc.wantDeletes {
				t.Errorf("Delete count mismatch: got %d, want %d", got, tc.wantDeletes)
			}
			if rc.pendingDeletion != tc.wantPending {
				t.Errorf("pending mismatch: got %t, want %t", rc.pendingDeletion, tc.wantPending)
			}

			if tc.wantEvent != "" {
				close(recorder.Events)
				found := false
				for evt := range recorder.Events {
					if strings.Contains(evt, tc.wantEvent) {
						found = true
					}
				}
				if !found {
					t.Errorf("expected an event containing %q", tc.wantEvent)
				}
			}

			fetched := &multigresv1alpha1.Shard{}
			getErr := c.Get(
				t.Context(),
				types.NamespacedName{Name: shardName, Namespace: namespace},
				fetched,
			)

			if tc.wantDeleted {
				if !errors.IsNotFound(getErr) {
					t.Errorf(
						"expected child Shard to be deleted, but it still exists (err: %v)",
						getErr,
					)
				}
				return
			}

			if getErr != nil {
				t.Fatalf("expected child Shard to still exist: %v", getErr)
			}
			got := fetched.Annotations[multigresv1alpha1.AnnotationPendingDeletion]
			if tc.wantAnnotationNonEmpty {
				if got == "" {
					t.Error("expected a freshly stamped PendingDeletion annotation, got empty")
				}
			} else if got != tc.wantAnnotation {
				t.Errorf(
					"PendingDeletion annotation mismatch: got %q, want %q",
					got,
					tc.wantAnnotation,
				)
			}
		})
	}
}
