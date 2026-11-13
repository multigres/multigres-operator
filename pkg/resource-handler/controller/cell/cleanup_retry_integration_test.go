//go:build integration
// +build integration

package cell_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// The controller is driven directly so unrelated watch events cannot rescue a
// lost retry. The cleanup races a real API write; the healthy reconcile must
// return that conflict to the work queue, and its next pass must finish cleanup.
func TestCell_HealthyCleanupConflictRequestsRetry(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	k8s := testutil.SetUpClient(t, cfg, scheme)
	ctx := t.Context()
	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cleanup-retry", Namespace: "default",
			Labels: map[string]string{"multigres.com/cluster": "cleanup-retry"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone-a", ZoneID: "zone-a",
			Images: multigresv1alpha1.CellImages{Multigateway: "ghcr.io/multigres/multigres:main"},
			Multigateway: multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "global-topo:2379", RootPath: "/multigres/global", Implementation: "etcd",
			},
		},
	}
	require.NoError(t, managedfields.Seed(ctx, k8s, cell, ownership.ManagerChildWriter))
	r := &cellcontroller.CellReconciler{
		Client: k8s, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cell)}
	_, err := r.Reconcile(ctx, request)
	require.NoError(t, err)
	deployment := &appsv1.Deployment{}
	require.NoError(t, k8s.Get(ctx, client.ObjectKey{
		Namespace: cell.Namespace, Name: cellcontroller.BuildMultigatewayDeploymentName(cell),
	}, deployment))
	deployment.Status.Replicas = 1
	deployment.Status.ReadyReplicas = 1
	deployment.Status.ObservedGeneration = deployment.Generation
	require.NoError(t, k8s.Status().Update(ctx, deployment))
	result, err := r.Reconcile(ctx, request)
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result, "a healthy Cell has no periodic requeue")
	require.NoError(t, k8s.Get(ctx, request.NamespacedName, cell))
	require.Equal(t, multigresv1alpha1.PhaseHealthy, cell.Status.Phase)
	wantStatus := cell.DeepCopy().Status

	// A rollback wrote the same status under the old shared name. The current
	// entry still covers it, so cleanup should remove exactly that co-owner.
	require.NoError(t, managedfields.Seed(ctx, k8s, cell, ownership.ManagerChildWriter,
		managedfields.OnStatus()))
	racing := &cleanupRaceClient{Client: k8s}
	r.Client = racing
	_, err = r.Reconcile(ctx, request)
	require.True(t, racing.injected, "cleanup never attempted a guarded patch")
	require.ErrorIs(t, err, ownership.ErrManagedFieldsChanged,
		"a cleanup conflict must reach the work queue even though the Cell is healthy")
	require.NoError(t, k8s.Get(ctx, request.NamespacedName, cell))
	require.Equal(t, wantStatus, cell.Status, "cleanup must not undo the successful status apply")
	require.True(t, slices.ContainsFunc(cell.ManagedFields, ownership.IsLegacyEntry))

	result, err = r.Reconcile(ctx, request)
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, result)
	require.NoError(t, k8s.Get(ctx, request.NamespacedName, cell))
	require.Equal(t, wantStatus, cell.Status)
	require.False(t, slices.ContainsFunc(cell.ManagedFields, ownership.IsLegacyEntry))
	require.True(t, slices.ContainsFunc(cell.ManagedFields, func(e metav1.ManagedFieldsEntry) bool {
		return e.Manager == ownership.ManagerChildWriter && e.Subresource == ""
	}), "the parent's spec ownership must survive cleanup")
}

type cleanupRaceClient struct {
	client.Client
	injected bool
}

func (c *cleanupRaceClient) Patch(
	ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption,
) error {
	if _, ok := obj.(*multigresv1alpha1.Cell); ok && patch.Type() == types.JSONPatchType &&
		!c.injected {
		current := &multigresv1alpha1.Cell{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
			return err
		}
		i := slices.IndexFunc(current.ManagedFields, ownership.IsLegacyEntry)
		if i < 0 {
			return fmt.Errorf("cleanup race requires a legacy entry")
		}
		// Change only the legacy entry's timestamp after cleanup has read it.
		// The API server must reject the stale whole-entry JSON patch test.
		body, err := json.Marshal([]map[string]any{{
			"op": "add", "path": fmt.Sprintf("/metadata/managedFields/%d/time", i),
			"value": metav1.NewTime(time.Now().Add(time.Hour)),
		}})
		if err != nil {
			return err
		}
		if err := c.Client.Patch(
			ctx,
			current,
			client.RawPatch(types.JSONPatchType, body),
		); err != nil {
			return err
		}
		c.injected = true
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}
