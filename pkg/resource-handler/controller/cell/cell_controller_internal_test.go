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
	"sigs.k8s.io/controller-runtime/pkg/controller"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
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
	assert.NewCollecting(t).
		Error(err, "reconcileMultigatewayDeployment() should error with invalid scheme")
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
	assert.NewCollecting(t).
		Error(err, "reconcileMultigatewayService() should error with invalid scheme")
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
	assert.NewCollecting(t).
		NoError(err, "updateStatus() should not error when Multigateway Deployment not found, got")
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
	assert.NewCollecting(t).
		Error(err, "reconcileMultigatewayDeployment() should error on Patch failure")
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
	assert.NewCollecting(t).
		Error(err, "reconcileMultigatewayService() should error on Patch failure")
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
	assert.NewCollecting(t).Error(err, "updateStatus() should error on Get failure")
}

// TestSetConditions_ZeroReplicas tests setConditions when deployments have zero replicas.
func TestSetConditions_ZeroReplicas(t *testing.T) {
	c := assert.NewCollecting(t)
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

	c.Require().
		Len(conditions, 2, "setConditions() should set 2 conditions, got %d", len(conditions))

	availCond := conditions[0]
	c.Eq("Available", availCond.Type, "Condition type")
	c.Eq(metav1.ConditionFalse, availCond.Status, "Condition status")
	c.Eq("MultigatewayUnavailable", availCond.Reason, "Condition reason")

	readyCond := conditions[1]
	c.Eq("Ready", readyCond.Type, "Condition type")
	c.Eq(metav1.ConditionFalse, readyCond.Status, "Condition status")
	c.Eq("MultigatewayNotReady", readyCond.Reason, "Condition reason")
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
		assert.NewAborting(t).NoError(err, "Failed to create manager")
		return mgr
	}

	t.Run("default options", func(t *testing.T) {
		mgr := createMgr()
		r := &CellReconciler{
			Client:   mgr.GetClient(),
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}
		assert.NewCollecting(t).NoError(r.SetupWithManager(mgr), "SetupWithManager() error =")
	})

	t.Run("with options", func(t *testing.T) {
		mgr := createMgr()
		r := &CellReconciler{
			Client:   mgr.GetClient(),
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}
		assert.NewCollecting(t).NoError(r.SetupWithManager(mgr, controller.Options{
			MaxConcurrentReconciles: 1,
			SkipNameValidation:      ptr.To(true),
		}), "SetupWithManager() with opts error =")
	})
}

func TestUpdateStatus_DegradedOnCrashLoop(t *testing.T) {
	c := assert.NewCollecting(t)
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

	c.Require().
		NoError(reconciler.updateStatus(context.Background(), cell), "updateStatus() unexpected error")
	c.Eq(multigresv1alpha1.PhaseDegraded, cell.Status.Phase, "expected PhaseDegraded, got")
}
