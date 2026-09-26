package cell

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestBuildLocalTopoServer(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	c.Require().NoError(multigresv1alpha1.AddToScheme(scheme), "AddToScheme() error =")

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-zone-a",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "cluster",
			},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone-a",
			TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					RootPath: "/multigres/zone-a",
					PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
						WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
					},
				},
			},
		},
	}

	got, err := BuildLocalTopoServer(cell, scheme)
	c.Require().NoError(err, "BuildLocalTopoServer() error =")
	c.Require().NotNil(got, "BuildLocalTopoServer() = nil, want TopoServer")
	c.Eq(BuildLocalTopoServerName(cell), got.Name, "name")
	c.Eq("default", got.Namespace, "namespace")
	c.Eq("cluster", got.Labels[metadata.LabelMultigresCluster], "cluster label")
	c.Eq("zone-a", got.Labels[metadata.LabelMultigresCell], "cell label")
	if got.Spec.Etcd == nil || got.Spec.Etcd.RootPath != "/multigres/zone-a" {
		t.Fatalf("etcd spec = %#v, want root path /multigres/zone-a", got.Spec.Etcd)
	}
	if got.Spec.PVCDeletionPolicy == nil ||
		got.Spec.PVCDeletionPolicy.WhenDeleted != multigresv1alpha1.DeletePVCRetentionPolicy {
		t.Fatalf("PVC deletion policy = %#v, want Delete", got.Spec.PVCDeletionPolicy)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].Name != cell.Name {
		t.Fatalf("owner references = %#v, want Cell owner", got.OwnerReferences)
	}
}

func TestBuildLocalTopoServerExternalReturnsNil(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-zone-a", Namespace: "default"},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone-a",
			TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://local:2379"},
					RootPath:  "/multigres/zone-a",
				},
			},
		},
	}

	got, err := BuildLocalTopoServer(cell, runtime.NewScheme())
	c.NoError(err, "BuildLocalTopoServer() error =")
	c.Nil(got, "BuildLocalTopoServer()")
}

func TestBuildLocalTopoServerNameIsSafeForTopoServerChildren(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: strings.Repeat("very-long-cell-name-", 20),
		},
	}

	got := BuildLocalTopoServerName(cell)
	c.LessOrEqual(
		52,
		len(got),
		"managed TopoServer name length = %d, want <= 52: %q",
		len(got),
		got,
	)
	c.LessOrEqual(
		63,
		len(got+"-headless"),
		"managed TopoServer headless service name length = %d, want <= 63: %q",
		len(got+"-headless"),
		got+"-headless",
	)
}

func TestCellReconcilerWaitsForManagedLocalTopoServer(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	})
	c.NoError(err, "Reconcile() error =")
	c.Eq(localTopoServerRecheckDelay, result.RequeueAfter, "RequeueAfter")

	toposerver := &multigresv1alpha1.TopoServer{}
	c.NoError(fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      BuildLocalTopoServerName(cell),
		Namespace: cell.Namespace,
	}, toposerver), "managed TopoServer should exist")

	deployment := &appsv1.Deployment{}
	err = fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      "test-cluster-zone-a-multigateway",
		Namespace: cell.Namespace,
	}, deployment)
	c.True(errors.IsNotFound(err), "Multigateway Deployment get error = %v, want NotFound", err)
}

func TestCellReconcilerSetsWaitingStatusForManagedLocalTopoServer(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell).
		Build()
	cell.Status.Phase = multigresv1alpha1.PhaseHealthy
	cell.Status.ObservedGeneration = cell.Generation
	meta.SetStatusCondition(&cell.Status.Conditions, metav1.Condition{
		Type:               "Available",
		Status:             metav1.ConditionTrue,
		Reason:             "MultigatewayAvailable",
		ObservedGeneration: cell.Generation,
	})
	meta.SetStatusCondition(&cell.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "MultigatewayReady",
		ObservedGeneration: cell.Generation,
	})
	c.NoError(fakeClient.Status().Update(t.Context(), cell), "failed to seed Cell status")

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	})
	assert.NewAborting(t).NoError(err, "Reconcile() error =")

	updatedCell := &multigresv1alpha1.Cell{}
	c.NoError(fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      cell.Name,
		Namespace: cell.Namespace,
	}, updatedCell), "Cell get error =")
	c.Eq(multigresv1alpha1.PhaseProgressing, updatedCell.Status.Phase, "Phase")
	for _, conditionType := range []string{"Available", "Ready"} {
		condition := meta.FindStatusCondition(updatedCell.Status.Conditions, conditionType)
		c.NotNil(condition, "%s condition not found", conditionType)
		if condition.Status != metav1.ConditionFalse ||
			condition.Reason != "LocalTopoServerNotReady" {
			t.Fatalf("%s = %s/%s, want False/LocalTopoServerNotReady",
				conditionType, condition.Status, condition.Reason)
		}
		c.Eq(
			updatedCell.Generation,
			condition.ObservedGeneration,
			"%s observedGeneration = %d, want",
			conditionType,
			condition.ObservedGeneration,
		)
	}
}

func TestCellReconcilerDeletesStaleManagedLocalTopoServer(t *testing.T) {
	t.Parallel()

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	cell.Spec.TopoServer = &multigresv1alpha1.LocalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{"http://local-topo:2379"},
			RootPath:  "/multigres/zone-a",
		},
	}
	toposerver := controlledTopoServer(cell)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell, toposerver).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	got := &multigresv1alpha1.TopoServer{}
	err := fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      toposerver.Name,
		Namespace: toposerver.Namespace,
	}, got)
	assert.NewAborting(t).
		True(errors.IsNotFound(err), "stale local TopoServer get error = %v, want NotFound", err)
}

func TestCellReconcilerIgnoresUnownedLocalTopoServerWhenNoManagedTopoDesired(t *testing.T) {
	t.Parallel()

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	cell.Spec.TopoServer = &multigresv1alpha1.LocalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{"http://local-topo:2379"},
			RootPath:  "/multigres/zone-a",
		},
	}
	toposerver := controlledTopoServer(cell)
	toposerver.OwnerReferences = nil
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell, toposerver).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	})
	assert.NewAborting(t).NoError(err, "Reconcile() error =")

	got := &multigresv1alpha1.TopoServer{}
	assert.NewAborting(t).NoError(fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      toposerver.Name,
		Namespace: toposerver.Namespace,
	}, got), "unowned local TopoServer should be left alone")
}

func TestCellReconcilerRefusesManagedLocalTopoServerNameConflict(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	toposerver := controlledTopoServer(cell)
	toposerver.OwnerReferences = nil
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell, toposerver).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	})
	c.Error(err, "Reconcile() error = nil, want name conflict")
	c.StrContains(
		err.Error(),
		"not controlled by Cell",
		"Reconcile() error = %v, want not controlled by Cell",
		err,
	)
}

func TestCellReconcilerPendingDeletionWaitsForManagedLocalTopoServer(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	cell.Annotations = map[string]string{
		multigresv1alpha1.AnnotationPendingDeletion: "true",
	}
	toposerver := controlledTopoServer(cell)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell, toposerver).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	})
	c.NoError(err, "Reconcile() error =")
	c.Eq(localTopoServerRecheckDelay, result.RequeueAfter, "RequeueAfter")

	updatedCell := &multigresv1alpha1.Cell{}
	c.NoError(fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      cell.Name,
		Namespace: cell.Namespace,
	}, updatedCell), "Cell get error =")
	condition := meta.FindStatusCondition(
		updatedCell.Status.Conditions,
		multigresv1alpha1.ConditionReadyForDeletion,
	)
	c.NotNil(condition, "ReadyForDeletion condition not found")
	if condition.Status != metav1.ConditionFalse || condition.Reason != "LocalTopoServerDeleting" {
		t.Fatalf("ReadyForDeletion = %s/%s, want False/LocalTopoServerDeleting",
			condition.Status, condition.Reason)
	}
}

func TestCellReconcilerPendingDeletionDeletesObservedLocalTopoServerWhenSpecNoLongerManaged(
	t *testing.T,
) {
	t.Parallel()
	c := assert.NewAborting(t)

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	cell.Annotations = map[string]string{
		multigresv1alpha1.AnnotationPendingDeletion: "true",
	}
	cell.Spec.TopoServer = &multigresv1alpha1.LocalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{"http://local-topo:2379"},
			RootPath:  "/multigres/zone-a",
		},
	}
	toposerver := controlledTopoServer(cell)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell, toposerver).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	})
	c.NoError(err, "Reconcile() error =")
	c.Eq(localTopoServerRecheckDelay, result.RequeueAfter, "RequeueAfter")

	got := &multigresv1alpha1.TopoServer{}
	err = fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      toposerver.Name,
		Namespace: toposerver.Namespace,
	}, got)
	c.True(errors.IsNotFound(err), "local TopoServer get error = %v, want NotFound", err)
}

func TestCellReconcilerLocalTopoServerReadyRequiresObservedGeneration(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	toposerver := controlledTopoServer(cell)
	toposerver.Generation = 2
	toposerver.Status.ObservedGeneration = 1
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell, toposerver).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	ready, err := reconciler.localTopoServerReady(t.Context(), cell)
	c.NoError(err, "localTopoServerReady() error =")
	c.False(ready, "localTopoServerReady() = true, want false for stale observedGeneration")
}

func TestCellReconcilerPendingDeletionReadyAfterManagedLocalTopoServerDeleted(t *testing.T) {
	t.Parallel()
	c := assert.NewAborting(t)

	scheme := cellTestScheme(t)
	cell := managedLocalTopoCell("test-cell")
	cell.Annotations = map[string]string{
		multigresv1alpha1.AnnotationPendingDeletion: "true",
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithObjects(cell).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	})
	c.NoError(err, "Reconcile() error =")
	c.Eq(0, result.RequeueAfter, "RequeueAfter")

	updatedCell := &multigresv1alpha1.Cell{}
	c.NoError(fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      cell.Name,
		Namespace: cell.Namespace,
	}, updatedCell), "Cell get error =")
	condition := meta.FindStatusCondition(
		updatedCell.Status.Conditions,
		multigresv1alpha1.ConditionReadyForDeletion,
	)
	c.NotNil(condition, "ReadyForDeletion condition not found")
	if condition.Status != metav1.ConditionTrue || condition.Reason != "LocalTopoServerDeleted" {
		t.Fatalf("ReadyForDeletion = %s/%s, want True/LocalTopoServerDeleted",
			condition.Status, condition.Reason)
	}
}

func cellTestScheme(t testing.TB) *runtime.Scheme {
	t.Helper()
	c := assert.NewAborting(t)
	scheme := runtime.NewScheme()
	c.NoError(multigresv1alpha1.AddToScheme(scheme), "AddToScheme(multigres) error =")
	c.NoError(appsv1.AddToScheme(scheme), "AddToScheme(apps) error =")
	c.NoError(corev1.AddToScheme(scheme), "AddToScheme(core) error =")
	return scheme
}

func managedLocalTopoCell(name string) *multigresv1alpha1.Cell {
	return &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name + "-uid"),
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone-a",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					RootPath: "/multigres/zone-a",
				},
			},
		},
	}
}

func controlledTopoServer(cell *multigresv1alpha1.Cell) *multigresv1alpha1.TopoServer {
	trueValue := true
	return &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:              BuildLocalTopoServerName(cell),
			Namespace:         cell.Namespace,
			Generation:        1,
			CreationTimestamp: metav1.NewTime(time.Now()),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         multigresv1alpha1.GroupVersion.String(),
					Kind:               "Cell",
					Name:               cell.Name,
					UID:                cell.UID,
					Controller:         &trueValue,
					BlockOwnerDeletion: &trueValue,
				},
			},
		},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{
				RootPath: "/multigres/zone-a",
			},
		},
		Status: multigresv1alpha1.TopoServerStatus{
			ObservedGeneration: 1,
			Phase:              multigresv1alpha1.PhaseHealthy,
		},
	}
}
