package cell

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// TestReconcileMultigatewayDeployment_InvalidScheme tests the error path when BuildMultigatewayDeployment fails.
// This should never happen in production - scheme is properly set up in main.go.
// Test exists for coverage of defensive error handling.
func TestReconcileMultigatewayDeployment_InvalidScheme(t *testing.T) {
	// Empty scheme without Cell type registered
	invalidScheme := runtime.NewScheme()

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(invalidScheme).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   invalidScheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.reconcileMultigatewayDeployment(context.Background(), cell)
	if err == nil {
		t.Error("reconcileMultigatewayDeployment() should error with invalid scheme")
	}
}

// TestReconcileMultigatewayService_InvalidScheme tests the error path when BuildMultigatewayService fails.
func TestReconcileMultigatewayService_InvalidScheme(t *testing.T) {
	invalidScheme := runtime.NewScheme()

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(invalidScheme).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   invalidScheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.reconcileMultigatewayService(context.Background(), cell)
	if err == nil {
		t.Error("reconcileMultigatewayService() should error with invalid scheme")
	}
}

// TestUpdateStatus_MultigatewayDeploymentNotFound tests the NotFound path in updateStatus.
func TestUpdateStatus_MultigatewayDeploymentNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme) // Need Deployment type registered for Get to work

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cell).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	// Call updateStatus when Multigateway Deployment doesn't exist yet
	err := reconciler.updateStatus(context.Background(), cell)
	if err != nil {
		t.Errorf(
			"updateStatus() should not error when Multigateway Deployment not found, got: %v",
			err,
		)
	}
}

// TestReconcileMultigatewayDeployment_PatchError tests error path on Patch Multigateway Deployment.
func TestReconcileMultigatewayDeployment_PatchError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
		},
	}

	// Create client with failure injection
	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cell).
		Build()

	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnPatch: func(obj client.Object) error {
			if strings.Contains(obj.GetName(), "multigateway") {
				return testutil.ErrNetworkTimeout
			}
			return nil
		},
	})

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.reconcileMultigatewayDeployment(context.Background(), cell)
	if err == nil {
		t.Error("reconcileMultigatewayDeployment() should error on Patch failure")
	}
}

// TestReconcileMultigatewayService_PatchError tests error path on Patch Multigateway Service.
func TestReconcileMultigatewayService_PatchError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
		},
	}

	// Create client with failure injection
	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cell).
		Build()

	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnPatch: func(obj client.Object) error {
			if strings.Contains(obj.GetName(), "multigateway") {
				return testutil.ErrNetworkTimeout
			}
			return nil
		},
	})

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.reconcileMultigatewayService(context.Background(), cell)
	if err == nil {
		t.Error("reconcileMultigatewayService() should error on Patch failure")
	}
}

// TestUpdateStatus_GetError tests error path on Get Multigateway Deployment (not NotFound).
func TestUpdateStatus_GetError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cell).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		Build()

	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnGet: func(key client.ObjectKey) error {
			if strings.Contains(key.Name, "multigateway") {
				return testutil.ErrNetworkTimeout
			}
			return nil
		},
	})

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.updateStatus(context.Background(), cell)
	if err == nil {
		t.Error("updateStatus() should error on Get failure")
	}
}

// TestSetConditions_ZeroReplicas tests setConditions when deployments have zero replicas.
func TestSetConditions_ZeroReplicas(t *testing.T) {
	reconciler := &CellReconciler{
		Recorder: record.NewFakeRecorder(100),
	}

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cell",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
		},
	}

	mgDeploy := &appsv1.Deployment{
		Status: appsv1.DeploymentStatus{
			Replicas:      0,
			ReadyReplicas: 0,
		},
	}

	reconciler.setConditions(cell, mgDeploy)
	conditions := cell.Status.Conditions

	if len(conditions) != 2 {
		t.Fatalf("setConditions() should set 2 conditions, got %d", len(conditions))
	}

	availCond := conditions[0]
	if availCond.Type != "Available" {
		t.Errorf("Condition type = %s, want Available", availCond.Type)
	}
	if availCond.Status != metav1.ConditionFalse {
		t.Errorf("Condition status = %s, want False (zero replicas)", availCond.Status)
	}
	if availCond.Reason != "MultigatewayUnavailable" {
		t.Errorf("Condition reason = %s, want MultigatewayUnavailable", availCond.Reason)
	}

	readyCond := conditions[1]
	if readyCond.Type != "Ready" {
		t.Errorf("Condition type = %s, want Ready", readyCond.Type)
	}
	if readyCond.Status != metav1.ConditionFalse {
		t.Errorf("Condition status = %s, want False", readyCond.Status)
	}
	if readyCond.Reason != "MultigatewayNotReady" {
		t.Errorf("Condition reason = %s, want MultigatewayNotReady", readyCond.Reason)
	}
}

// TestSetupWithManager tests the manager setup function.
func TestSetupWithManager(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// dummy config
	cfg := &rest.Config{Host: "http://localhost:8080"}

	createMgr := func() ctrl.Manager {
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  scheme,
			Metrics: metricsserver.Options{BindAddress: "0"},
		})
		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}
		return mgr
	}

	t.Run("default options", func(t *testing.T) {
		mgr := createMgr()
		r := &CellReconciler{
			Client:   mgr.GetClient(),
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}
		if err := r.SetupWithManager(mgr); err != nil {
			t.Errorf("SetupWithManager() error = %v", err)
		}
	})

	t.Run("with options", func(t *testing.T) {
		mgr := createMgr()
		r := &CellReconciler{
			Client:   mgr.GetClient(),
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}
		if err := r.SetupWithManager(mgr, controller.Options{
			MaxConcurrentReconciles: 1,
			SkipNameValidation:      ptr.To(true),
		}); err != nil {
			t.Errorf("SetupWithManager() with opts error = %v", err)
		}
	})
}

func TestUpdateStatus_DegradedOnCrashLoop(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
		},
	}

	deployName := BuildMultigatewayDeploymentName(cell)
	mgLabels := metadata.BuildStandardLabels("test-cluster", MultigatewayComponentName)
	metadata.AddCellLabel(mgLabels, cell.Spec.Name)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       deployName,
			Namespace:  "default",
			Generation: 1,
		},
		Spec:   appsv1.DeploymentSpec{Replicas: ptr.To(int32(1))},
		Status: appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 0, ObservedGeneration: 1},
	}

	crashPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName + "-abc",
			Namespace: "default",
			Labels:    metadata.GetSelectorLabels(mgLabels),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "multigateway",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cell, deploy, crashPod).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		Build()

	reconciler := &CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	if err := reconciler.updateStatus(context.Background(), cell); err != nil {
		t.Fatalf("updateStatus() unexpected error: %v", err)
	}
	if cell.Status.Phase != multigresv1alpha1.PhaseDegraded {
		t.Errorf("expected PhaseDegraded, got %q", cell.Status.Phase)
	}
}

func TestProjectRefOrGenerationChangedPredicate_Update(t *testing.T) {
	newCell := func(annotations map[string]string) *multigresv1alpha1.Cell {
		return &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-cell",
				Namespace:   "default",
				Generation:  1,
				Annotations: annotations,
			},
		}
	}

	tests := []struct {
		name string
		old  *multigresv1alpha1.Cell
		new  *multigresv1alpha1.Cell
		want bool
	}{
		{
			name: "pending-deletion annotation added",
			old:  newCell(nil),
			new:  newCell(map[string]string{multigresv1alpha1.AnnotationPendingDeletion: "now"}),
			want: true,
		},
		{
			name: "pending-deletion annotation changed",
			old:  newCell(map[string]string{multigresv1alpha1.AnnotationPendingDeletion: "then"}),
			new:  newCell(map[string]string{multigresv1alpha1.AnnotationPendingDeletion: "now"}),
			want: true,
		},
		{
			name: "pending-deletion annotation removed",
			old:  newCell(map[string]string{multigresv1alpha1.AnnotationPendingDeletion: "now"}),
			new:  newCell(nil),
			want: true,
		},
		{
			name: "pending-deletion annotation added alongside an unrelated annotation",
			old:  newCell(map[string]string{"example.com/unrelated": "a"}),
			new: newCell(map[string]string{
				"example.com/unrelated":                     "b",
				multigresv1alpha1.AnnotationPendingDeletion: "now",
			}),
			want: true,
		},
		{
			name: "unrelated annotation changed",
			old:  newCell(map[string]string{"example.com/unrelated": "a"}),
			new:  newCell(map[string]string{"example.com/unrelated": "b"}),
			want: false,
		},
		{
			name: "no change",
			old:  newCell(nil),
			new:  newCell(nil),
			want: false,
		},
	}

	pred := projectRefOrGenerationChangedPredicate()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Update(event.UpdateEvent{ObjectOld: tt.old, ObjectNew: tt.new})
			if got != tt.want {
				t.Errorf("Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHandlePendingDeletion_AnswersOnTheVersionItRead pins that the drain's
// ReadyForDeletion write is conditioned on the resourceVersion the drain
// read, which is what makes it conflict when MultigresClusterReconciler
// cancels the stamp while the drain is in flight.
//
// Pinned at the write rather than by interleaving the cancel in test/suite:
// testkit's ctrltest has no hook to hold one controller's write mid-pass
// while another controller's lands, which is the only way to put a cancel
// between the drain's read and its write deterministically.
func TestHandlePendingDeletion_AnswersOnTheVersionItRead(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	stored := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
			Annotations: map[string]string{
				multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
			},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "zone1"},
	}
	var appliedVersions []string
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(stored).
		WithStatusSubresource(&multigresv1alpha1.Cell{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(
				_ context.Context,
				_ client.Client,
				subResource string,
				obj client.Object,
				_ client.Patch,
				_ ...client.SubResourcePatchOption,
			) error {
				if subResource == "status" {
					appliedVersions = append(appliedVersions, obj.GetResourceVersion())
				}
				return nil
			},
		}).
		Build()
	r := &CellReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	cell := &multigresv1alpha1.Cell{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(stored), cell); err != nil {
		t.Fatal(err)
	}
	if _, err := r.handlePendingDeletion(context.Background(), cell); err != nil {
		t.Fatal(err)
	}
	if len(appliedVersions) != 1 {
		t.Fatalf("want one status write, got %d", len(appliedVersions))
	}
	if appliedVersions[0] == "" || appliedVersions[0] != cell.ResourceVersion {
		t.Errorf("ReadyForDeletion applied at resourceVersion %q, want the version read, %q",
			appliedVersions[0], cell.ResourceVersion)
	}
}
