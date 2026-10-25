package cell_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/multigres/testkit/ctrltest"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
)

// legacyDropFailingClient makes every status apply return an object that still
// carries a legacy managedFields entry, and every Cell patch (the DropLegacy
// request) fail with a 422. The fake does not model managedFields, so the
// legacy entry is injected on the status response here; the 422 comes from
// testkit.
func legacyDropFailingClient(base client.WithWatch) client.Client {
	funcs := ctrltest.InvalidPatch(func(obj client.Object) bool {
		_, ok := obj.(*multigresv1alpha1.Cell)
		return ok
	}, 0)
	funcs.SubResourcePatch = func(
		ctx context.Context,
		c client.Client,
		sub string,
		obj client.Object,
		patch client.Patch,
		opts ...client.SubResourcePatchOption,
	) error {
		if err := c.SubResource(sub).Patch(ctx, obj, patch, opts...); err != nil {
			return err
		}
		obj.SetManagedFields([]metav1.ManagedFieldsEntry{
			{Manager: ownership.LegacyOperator, Operation: metav1.ManagedFieldsOperationApply},
			{Manager: ownership.ManagerCell, Operation: metav1.ManagedFieldsOperationApply},
		})
		return nil
	}
	return interceptor.NewClient(base, funcs)
}

// TestCellReconcile_LegacyDropFailureIsNotAReconcileError pins the tidy-up as
// best-effort: a failing DropLegacy neither fails the reconcile nor changes the
// result it returns, and the status write stands.
func TestCellReconcile_LegacyDropFailureIsNotAReconcileError(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	reconcile := func(wrap func(client.WithWatch) client.Client) (ctrl.Result, client.Client, error) {
		obj := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{Name: "tidy-cell", Namespace: "default"},
			Spec:       multigresv1alpha1.CellSpec{Name: "zone1"},
		}
		c := cell.NewTestClient(
			t,
			[]client.Object{obj},
			cell.WithTestScheme(scheme),
			cell.WithTestWrap(wrap),
		)
		r := &cell.CellReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
		res, err := r.Reconcile(
			t.Context(),
			ctrl.Request{NamespacedName: client.ObjectKeyFromObject(obj)},
		)
		return res, c, err
	}

	want, _, err := reconcile(func(base client.WithWatch) client.Client { return base })
	require.NoError(t, err)

	got, c, err := reconcile(legacyDropFailingClient)
	require.NoError(t, err)
	require.Equal(t, want, got, "a failed tidy-up must not force a requeue")

	stored := &multigresv1alpha1.Cell{}
	require.NoError(
		t,
		c.Get(t.Context(), client.ObjectKey{Name: "tidy-cell", Namespace: "default"}, stored),
	)
	require.NotEmpty(t, stored.Status.Phase, "the status write must stand")
	require.Equal(t, stored.Generation, stored.Status.ObservedGeneration)
}
