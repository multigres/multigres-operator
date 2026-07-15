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
)

func TestBuildLocalTopoServer(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}

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
	if err != nil {
		t.Fatalf("BuildLocalTopoServer() error = %v", err)
	}
	if got == nil {
		t.Fatal("BuildLocalTopoServer() = nil, want TopoServer")
	}
	if got.Name != BuildLocalTopoServerName(cell) {
		t.Errorf("name = %q, want %q", got.Name, BuildLocalTopoServerName(cell))
	}
	if got.Namespace != "default" {
		t.Errorf("namespace = %q, want default", got.Namespace)
	}
	if got.Labels[metadata.LabelMultigresCluster] != "cluster" {
		t.Errorf("cluster label = %q, want cluster", got.Labels[metadata.LabelMultigresCluster])
	}
	if got.Labels[metadata.LabelMultigresCell] != "zone-a" {
		t.Errorf("cell label = %q, want zone-a", got.Labels[metadata.LabelMultigresCell])
	}
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
	if err != nil {
		t.Fatalf("BuildLocalTopoServer() error = %v", err)
	}
	if got != nil {
		t.Fatalf("BuildLocalTopoServer() = %#v, want nil", got)
	}
}

func TestBuildLocalTopoServerNameIsSafeForTopoServerChildren(t *testing.T) {
	t.Parallel()

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: strings.Repeat("very-long-cell-name-", 20),
		},
	}

	got := BuildLocalTopoServerName(cell)
	if len(got) > 52 {
		t.Fatalf("managed TopoServer name length = %d, want <= 52: %q", len(got), got)
	}
	if len(got+"-headless") > 63 {
		t.Fatalf("managed TopoServer headless service name length = %d, want <= 63: %q",
			len(got+"-headless"), got+"-headless")
	}
}

func TestCellReconcilerWaitsForManagedLocalTopoServer(t *testing.T) {
	t.Parallel()

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
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != localTopoServerRecheckDelay {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, localTopoServerRecheckDelay)
	}

	toposerver := &multigresv1alpha1.TopoServer{}
	if err := fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      BuildLocalTopoServerName(cell),
		Namespace: cell.Namespace,
	}, toposerver); err != nil {
		t.Fatalf("managed TopoServer should exist: %v", err)
	}

	deployment := &appsv1.Deployment{}
	err = fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      "test-cluster-zone-a-multigateway",
		Namespace: cell.Namespace,
	}, deployment)
	if !errors.IsNotFound(err) {
		t.Fatalf("Multigateway Deployment get error = %v, want NotFound", err)
	}
}

func TestCellReconcilerSetsWaitingStatusForManagedLocalTopoServer(t *testing.T) {
	t.Parallel()

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
	if err := fakeClient.Status().Update(t.Context(), cell); err != nil {
		t.Fatalf("failed to seed Cell status: %v", err)
	}

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

	updatedCell := &multigresv1alpha1.Cell{}
	if err := fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      cell.Name,
		Namespace: cell.Namespace,
	}, updatedCell); err != nil {
		t.Fatalf("Cell get error = %v", err)
	}
	if updatedCell.Status.Phase != multigresv1alpha1.PhaseProgressing {
		t.Fatalf("Phase = %s, want Progressing", updatedCell.Status.Phase)
	}
	for _, conditionType := range []string{"Available", "Ready"} {
		condition := meta.FindStatusCondition(updatedCell.Status.Conditions, conditionType)
		if condition == nil {
			t.Fatalf("%s condition not found", conditionType)
		}
		if condition.Status != metav1.ConditionFalse ||
			condition.Reason != "LocalTopoServerNotReady" {
			t.Fatalf("%s = %s/%s, want False/LocalTopoServerNotReady",
				conditionType, condition.Status, condition.Reason)
		}
		if condition.ObservedGeneration != updatedCell.Generation {
			t.Fatalf("%s observedGeneration = %d, want %d",
				conditionType, condition.ObservedGeneration, updatedCell.Generation)
		}
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
	if !errors.IsNotFound(err) {
		t.Fatalf("stale local TopoServer get error = %v, want NotFound", err)
	}
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

	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cell.Name, Namespace: cell.Namespace},
	}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	got := &multigresv1alpha1.TopoServer{}
	if err := fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      toposerver.Name,
		Namespace: toposerver.Namespace,
	}, got); err != nil {
		t.Fatalf("unowned local TopoServer should be left alone: %v", err)
	}
}

func TestCellReconcilerRefusesManagedLocalTopoServerNameConflict(t *testing.T) {
	t.Parallel()

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
	if err == nil {
		t.Fatal("Reconcile() error = nil, want name conflict")
	}
	if !strings.Contains(err.Error(), "not controlled by Cell") {
		t.Fatalf("Reconcile() error = %v, want not controlled by Cell", err)
	}
}

func TestCellReconcilerPendingDeletionWaitsForManagedLocalTopoServer(t *testing.T) {
	t.Parallel()

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
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != localTopoServerRecheckDelay {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, localTopoServerRecheckDelay)
	}

	updatedCell := &multigresv1alpha1.Cell{}
	if err := fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      cell.Name,
		Namespace: cell.Namespace,
	}, updatedCell); err != nil {
		t.Fatalf("Cell get error = %v", err)
	}
	condition := meta.FindStatusCondition(
		updatedCell.Status.Conditions,
		multigresv1alpha1.ConditionReadyForDeletion,
	)
	if condition == nil {
		t.Fatal("ReadyForDeletion condition not found")
	}
	if condition.Status != metav1.ConditionFalse || condition.Reason != "LocalTopoServerDeleting" {
		t.Fatalf("ReadyForDeletion = %s/%s, want False/LocalTopoServerDeleting",
			condition.Status, condition.Reason)
	}
}

func TestCellReconcilerPendingDeletionDeletesObservedLocalTopoServerWhenSpecNoLongerManaged(
	t *testing.T,
) {
	t.Parallel()

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
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != localTopoServerRecheckDelay {
		t.Fatalf("RequeueAfter = %v, want %v", result.RequeueAfter, localTopoServerRecheckDelay)
	}

	got := &multigresv1alpha1.TopoServer{}
	err = fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      toposerver.Name,
		Namespace: toposerver.Namespace,
	}, got)
	if !errors.IsNotFound(err) {
		t.Fatalf("local TopoServer get error = %v, want NotFound", err)
	}
}

func TestCellReconcilerLocalTopoServerReadyRequiresObservedGeneration(t *testing.T) {
	t.Parallel()

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
	if err != nil {
		t.Fatalf("localTopoServerReady() error = %v", err)
	}
	if ready {
		t.Fatal("localTopoServerReady() = true, want false for stale observedGeneration")
	}
}

func TestCellReconcilerPendingDeletionReadyAfterManagedLocalTopoServerDeleted(t *testing.T) {
	t.Parallel()

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
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("RequeueAfter = %v, want 0", result.RequeueAfter)
	}

	updatedCell := &multigresv1alpha1.Cell{}
	if err := fakeClient.Get(t.Context(), client.ObjectKey{
		Name:      cell.Name,
		Namespace: cell.Namespace,
	}, updatedCell); err != nil {
		t.Fatalf("Cell get error = %v", err)
	}
	condition := meta.FindStatusCondition(
		updatedCell.Status.Conditions,
		multigresv1alpha1.ConditionReadyForDeletion,
	)
	if condition == nil {
		t.Fatal("ReadyForDeletion condition not found")
	}
	if condition.Status != metav1.ConditionTrue || condition.Reason != "LocalTopoServerDeleted" {
		t.Fatalf("ReadyForDeletion = %s/%s, want True/LocalTopoServerDeleted",
			condition.Status, condition.Reason)
	}
}

func cellTestScheme(t testing.TB) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(multigres) error = %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(apps) error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme(core) error = %v", err)
	}
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
