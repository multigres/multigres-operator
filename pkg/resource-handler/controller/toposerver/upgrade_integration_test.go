//go:build integration
// +build integration

package toposerver_test

import (
	"context"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	toposervercontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestTopoServer_Upgrade drives the TopoServer controller over objects in the
// state an upgrade leaves them in: spec under the child writer's name, status
// under the shared legacy name, and a StorageClassValid condition that already
// holds the value the guard computes but is owned only by the retired guard
// name.
//
// The guard must claim that condition even though nothing about it changed,
// or the tidy-up after the status apply could never retire the old entry
// without leaving the condition with no owner. The etcd StatefulSet, last
// written under the child writer's name, must then follow a spec change that
// raises the CPU request past the old limit and removes the limit.
func TestTopoServer_Upgrade(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	k8s := testutil.SetUpClient(t, cfg, scheme)
	mgr := testutil.SetUpManager(t, cfg, scheme)
	require.NoError(t, (&toposervercontroller.TopoServerReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("toposerver-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}))

	cpu := func(v string) corev1.ResourceList {
		return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(v)}
	}
	ts := &multigresv1alpha1.TopoServer{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "TopoServer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "upgrade-topo", Namespace: "default",
			Labels: map[string]string{"multigres.com/cluster": "upgrade-cluster"},
		},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{
				Resources: corev1.ResourceRequirements{Requests: cpu("100m"), Limits: cpu("100m")},
			},
		},
	}
	ctx := t.Context()
	require.NoError(t, managedfields.Seed(ctx, k8s, ts, ownership.ManagerChildWriter))
	cur := &multigresv1alpha1.TopoServer{}
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(ts), cur))

	status := ts.DeepCopy()
	status.Status = multigresv1alpha1.TopoServerStatus{ObservedGeneration: cur.Generation}
	require.NoError(t, managedfields.Seed(ctx, k8s, status, ownership.ManagerChildWriter,
		managedfields.OnStatus()))
	guard := ts.DeepCopy()
	guard.Status = multigresv1alpha1.TopoServerStatus{Conditions: []metav1.Condition{{
		Type:               "StorageClassValid",
		Status:             metav1.ConditionTrue,
		Reason:             "StorageClassNotSpecified",
		Message:            "No explicit etcd StorageClass configured; using cluster default",
		ObservedGeneration: cur.Generation,
		LastTransitionTime: metav1.Now(),
	}}}
	require.NoError(t, managedfields.Seed(ctx, k8s, guard, ownership.LegacyOperatorGuard,
		managedfields.OnStatus()))
	testutil.StartManager(t, mgr)

	condPath := ownership.ConditionPath("StorageClassValid")
	got := &multigresv1alpha1.TopoServer{}
	require.Eventually(t, func() bool {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(ts), got); err != nil {
			return false
		}
		return slices.Equal(managedfields.Owners(got, condPath),
			[]string{ownership.ManagerTopoServerGuard}) &&
			!slices.ContainsFunc(got.ManagedFields, ownership.IsLegacyEntry)
	}, 20*time.Second, 100*time.Millisecond,
		"the guard never took the unchanged condition over from the legacy entry: %+v",
		got.ManagedFields)
	require.True(t, slices.ContainsFunc(got.ManagedFields, func(e metav1.ManagedFieldsEntry) bool {
		return e.Manager == ownership.ManagerChildWriter && e.Subresource == ""
	}), "the parent's spec entry was removed: %+v", got.ManagedFields)

	sts := &appsv1.StatefulSet{}
	key := client.ObjectKeyFromObject(ts)
	require.Eventually(t, func() bool {
		return k8s.Get(ctx, key, sts) == nil
	}, 15*time.Second, 100*time.Millisecond, "etcd StatefulSet never created")
	testutil.RewriteAsChildWriter(t, k8s, sts)

	require.Eventually(t, func() bool {
		cur := &multigresv1alpha1.TopoServer{}
		if err := k8s.Get(ctx, key, cur); err != nil {
			return false
		}
		cur.Spec.Etcd.Resources = corev1.ResourceRequirements{Requests: cpu("200m")}
		return k8s.Update(ctx, cur, client.FieldOwner(ownership.ManagerChildWriter)) == nil
	}, 10*time.Second, 100*time.Millisecond, "update the TopoServer spec")

	require.Eventually(t, func() bool {
		if err := k8s.Get(ctx, key, sts); err != nil {
			return false
		}
		c := sts.Spec.Template.Spec.Containers[0]
		_, limited := c.Resources.Limits[corev1.ResourceCPU]
		return !limited && c.Resources.Requests.Cpu().Cmp(resource.MustParse("200m")) == 0
	}, 20*time.Second, 100*time.Millisecond,
		"etcd StatefulSet never took the new request without the old limit: %+v",
		sts.Spec.Template.Spec.Containers)
	require.Equal(t, []string{ownership.ManagerChildWriter}, testutil.OperatorManagers(sts))
}

// failFirstDrop fails the first legacy tidy-up patch it sees, the JSON patch
// DropLegacy sends, with the 422 a failed test op returns, and passes every
// other write through.
type failFirstDrop struct {
	client.Client
	drops atomic.Int64
}

func (c *failFirstDrop) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	if _, ok := obj.(*multigresv1alpha1.TopoServer); ok && patch.Type() == types.JSONPatchType {
		if c.drops.Add(1) == 1 {
			return apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "patch",
				schema.GroupResource{}, "", "", 0, false)
		}
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// TestTopoServer_TidyRetriesAfterAFailure drives a TopoServer whose StorageClass
// is missing, so every pass stops at the guard and never reaches the main
// status apply. The guard's first pass claims the condition and its tidy-up
// fails once. Every later pass finds the condition unchanged and owned, so the
// tidy-up must run on that skip path too, or the legacy entry it failed to
// remove would stay until the StorageClass appeared.
func TestTopoServer_TidyRetriesAfterAFailure(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	k8s := testutil.SetUpClient(t, cfg, scheme)
	mgr := testutil.SetUpManager(t, cfg, scheme)
	failing := &failFirstDrop{Client: mgr.GetClient()}
	require.NoError(t, (&toposervercontroller.TopoServerReconciler{
		Client:   failing,
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("toposerver-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}))

	ts := &multigresv1alpha1.TopoServer{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "TopoServer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "retry-topo", Namespace: "default",
			Labels: map[string]string{"multigres.com/cluster": "retry-cluster"},
		},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{
				Storage: multigresv1alpha1.StorageSpec{Class: "absent"},
			},
		},
	}
	ctx := t.Context()
	require.NoError(t, managedfields.Seed(ctx, k8s, ts, ownership.ManagerChildWriter))
	cur := &multigresv1alpha1.TopoServer{}
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(ts), cur))
	guard := ts.DeepCopy()
	guard.Status = multigresv1alpha1.TopoServerStatus{Conditions: []metav1.Condition{{
		Type:               "StorageClassValid",
		Status:             metav1.ConditionFalse,
		Reason:             "StorageClassNotFound",
		Message:            `StorageClass "absent" not found for etcd PVCs`,
		ObservedGeneration: cur.Generation,
		LastTransitionTime: metav1.Now(),
	}}}
	require.NoError(t, managedfields.Seed(ctx, k8s, guard, ownership.LegacyOperatorGuard,
		managedfields.OnStatus()))
	testutil.StartManager(t, mgr)

	got := &multigresv1alpha1.TopoServer{}
	legacyLeft := func() bool {
		require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(ts), got))
		return slices.ContainsFunc(got.ManagedFields, ownership.IsLegacyEntry)
	}
	for deadline := time.Now().Add(40 * time.Second); legacyLeft() && time.Now().Before(deadline); {
		time.Sleep(200 * time.Millisecond)
	}
	require.False(t, legacyLeft(),
		"the legacy entry survived a single failed tidy-up (%d attempts): %+v",
		failing.drops.Load(), got.ManagedFields)
	require.GreaterOrEqual(t, failing.drops.Load(), int64(2), "the injected failure never ran")
	require.Equal(t, []string{ownership.ManagerTopoServerGuard},
		managedfields.Owners(got, ownership.ConditionPath("StorageClassValid")))
}
