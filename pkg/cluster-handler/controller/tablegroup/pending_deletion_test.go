package tablegroup

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestPendingDeletionWaitsForUnobservedChildren pins defect 5.
//
// handlePendingDeletion concluded "every child is ready" from a list of zero
// children: the loop never ran, allReady stayed true, and the TableGroup
// reported ReadyForDeletion having drained nothing. Every step reported
// success, which is what made it dangerous, because the protocol it short
// circuits exists to prevent data loss.
//
// Reachable through a spec edit inside the window before children are created,
// and through plain cache lag, since the list comes from the cached client.
// The second needs no unusual behaviour at all.
func TestPendingDeletionWaitsForUnobservedChildren(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	now := metav1.Now()
	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "tg",
			Namespace:         "ns",
			DeletionTimestamp: &now,
			Finalizers:        []string{"multigres.com/tablegroup-cleanup"},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			// One declared child, and deliberately none created, which is what
			// both reachable paths look like from here.
			Shards: []multigresv1alpha1.ShardResolvedSpec{{Name: "0-inf"}},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tg).
		WithStatusSubresource(&multigresv1alpha1.TableGroup{}).
		Build()
	r := &TableGroupReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	res, err := r.handlePendingDeletion(context.Background(), tg)
	if err != nil {
		t.Fatalf("handlePendingDeletion() error = %v", err)
	}

	if meta.IsStatusConditionTrue(
		tg.Status.Conditions, multigresv1alpha1.ConditionReadyForDeletion,
	) {
		t.Error(
			"TableGroup reported ReadyForDeletion while its only declared Shard " +
				"had not been observed, so the parent would be deleted having " +
				"drained nothing",
		)
	}
	if res.RequeueAfter == 0 {
		t.Error("no requeue, so nothing would ever re-examine the children")
	}
}
