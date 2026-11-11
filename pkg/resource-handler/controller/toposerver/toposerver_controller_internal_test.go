package toposerver

import (
	"context"
	"net/http"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
)

// TestReconcileStatefulSet_InvalidScheme tests the error path when BuildStatefulSet fails.
// This should never happen in production - scheme is properly set up in main.go.
// Test exists for coverage of defensive error handling.
func TestReconcileStatefulSet_InvalidScheme(t *testing.T) {
	// Empty scheme without TopoServer type registered
	invalidScheme := runtime.NewScheme()

	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.TopoServerSpec{},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(invalidScheme).
		Build()

	reconciler := &TopoServerReconciler{
		Client:   fakeClient,
		Scheme:   invalidScheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.reconcileStatefulSet(context.Background(), toposerver)
	if err == nil {
		t.Error("reconcileStatefulSet() should error with invalid scheme")
	}
}

// TestReconcileHeadlessService_InvalidScheme tests the error path when BuildHeadlessService fails.
func TestReconcileHeadlessService_InvalidScheme(t *testing.T) {
	invalidScheme := runtime.NewScheme()

	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.TopoServerSpec{},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(invalidScheme).
		Build()

	reconciler := &TopoServerReconciler{
		Client:   fakeClient,
		Scheme:   invalidScheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.reconcileHeadlessService(context.Background(), toposerver)
	if err == nil {
		t.Error("reconcileHeadlessService() should error with invalid scheme")
	}
}

// TestReconcileClientService_InvalidScheme tests the error path when BuildClientService fails.
func TestReconcileClientService_InvalidScheme(t *testing.T) {
	invalidScheme := runtime.NewScheme()

	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.TopoServerSpec{},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(invalidScheme).
		Build()

	reconciler := &TopoServerReconciler{
		Client:   fakeClient,
		Scheme:   invalidScheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.reconcileClientService(context.Background(), toposerver)
	if err == nil {
		t.Error("reconcileClientService() should error with invalid scheme")
	}
}

// TestUpdateStatus_StatefulSetNotFound tests the NotFound path in updateStatus.
func TestUpdateStatus_StatefulSetNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme) // Need StatefulSet type registered for Get to work

	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.TopoServerSpec{},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(toposerver).
		WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
		Build()

	reconciler := &TopoServerReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	// Call updateStatus when StatefulSet doesn't exist yet
	err := reconciler.updateStatus(context.Background(), toposerver)
	if err != nil {
		t.Errorf("updateStatus() should not error when StatefulSet not found, got: %v", err)
	}
}

// TestReconcileClientService_PatchError tests error path on Patch client Service.
func TestReconcileClientService_PatchError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.TopoServerSpec{},
	}

	// Create client with failure injection
	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(toposerver).
		Build()

	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnPatch: func(obj client.Object) error {
			if obj.GetName() == "test-toposerver" {
				return testutil.ErrNetworkTimeout
			}
			return nil
		},
	})

	reconciler := &TopoServerReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.reconcileClientService(context.Background(), toposerver)
	if err == nil {
		t.Error("reconcileClientService() should error on Patch failure")
	}
}

// TestUpdateStatus_GetError tests error path on Get StatefulSet (not NotFound).
func TestUpdateStatus_GetError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.TopoServerSpec{},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(toposerver).
		WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
		Build()

	fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
		OnGet: testutil.FailOnKeyName("test-toposerver", testutil.ErrNetworkTimeout),
	})

	reconciler := &TopoServerReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	err := reconciler.updateStatus(context.Background(), toposerver)
	if err == nil {
		t.Error("updateStatus() should error on Get failure")
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
			MapperProvider: func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
				mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{
					multigresv1alpha1.GroupVersion,
					appsv1.SchemeGroupVersion,
					corev1.SchemeGroupVersion,
					policyv1.SchemeGroupVersion,
				})
				mapper.Add(multigresv1alpha1.GroupVersion.WithKind("Cell"), meta.RESTScopeNamespace)
				mapper.Add(
					multigresv1alpha1.GroupVersion.WithKind("MultigresCluster"),
					meta.RESTScopeNamespace,
				)
				mapper.Add(
					multigresv1alpha1.GroupVersion.WithKind("TopoServer"),
					meta.RESTScopeNamespace,
				)
				mapper.Add(
					appsv1.SchemeGroupVersion.WithKind("StatefulSet"),
					meta.RESTScopeNamespace,
				)
				mapper.Add(corev1.SchemeGroupVersion.WithKind("Service"), meta.RESTScopeNamespace)
				mapper.Add(
					policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"),
					meta.RESTScopeNamespace,
				)
				return mapper, nil
			},
		})
		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}
		return mgr
	}

	t.Run("default options", func(t *testing.T) {
		mgr := createMgr()
		r := &TopoServerReconciler{
			Client:   mgr.GetClient(),
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(100),
		}
		if err := r.SetupWithManager(mgr); err != nil {
			t.Errorf("SetupWithManager() error = %v", err)
		}
		if r.APIReader != mgr.GetAPIReader() {
			t.Error("maintenance must use the manager's uncached API reader")
		}
	})

	t.Run("with options", func(t *testing.T) {
		mgr := createMgr()
		r := &TopoServerReconciler{
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

	for name, reader := range map[string]client.Reader{
		"shared setup defaults to uncached reader": nil,
		"shared setup preserves injected reader":   fake.NewClientBuilder().WithScheme(scheme).Build(),
	} {
		t.Run(name, func(t *testing.T) {
			mgr := createMgr()
			r := &TopoServerReconciler{
				Client:    mgr.GetClient(),
				APIReader: reader,
				Scheme:    scheme,
				Recorder:  record.NewFakeRecorder(100),
			}
			if err := r.SetupWithManagerReconciler(mgr, r, controller.Options{
				SkipNameValidation: ptr.To(true),
			}); err != nil {
				t.Fatalf("SetupWithManagerReconciler() error = %v", err)
			}
			want := reader
			if want == nil {
				want = mgr.GetAPIReader()
			}
			if r.APIReader != want {
				t.Error("shared setup selected the wrong maintenance reader")
			}
		})
	}
}

func TestUpdateStatus_DegradedOnCrashLoop(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-topo",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       toposerver.Name,
			Namespace:  "default",
			Generation: 1,
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: ptr.To(int32(1))},
		Status: appsv1.StatefulSetStatus{Replicas: 1, ReadyReplicas: 0, ObservedGeneration: 1},
	}

	tsLabels := metadata.BuildStandardLabels("test-cluster", ComponentName)
	tsLabels = metadata.MergeLabels(tsLabels, toposerver.GetObjectMeta().GetLabels())

	crashPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toposerver.Name + "-0",
			Namespace: "default",
			Labels:    metadata.GetSelectorLabels(tsLabels),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "etcd",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(toposerver, sts, crashPod).
		WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
		Build()

	reconciler := &TopoServerReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	if err := reconciler.updateStatus(context.Background(), toposerver); err != nil {
		t.Fatalf("updateStatus() unexpected error: %v", err)
	}
	if toposerver.Status.Phase != multigresv1alpha1.PhaseDegraded {
		t.Errorf("expected PhaseDegraded, got %q", toposerver.Status.Phase)
	}
}
