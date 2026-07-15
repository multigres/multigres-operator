package cell_test

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// buildHashedName helper to generate the expected hashed name for tests
// mimicking BuildMultigatewayName logic
func buildHashedName(clusterName, cellName string) string {
	return name.JoinWithConstraints(name.ServiceConstraints, clusterName, cellName, "multigateway")
}

// conditionAssertion defines expected condition state for testing
type conditionAssertion struct {
	Type   string
	Status metav1.ConditionStatus
	Reason string
}

// assertConditions verifies conditions match expectations
func assertConditions(t testing.TB, got []metav1.Condition, want ...conditionAssertion) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("condition count = %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.Type != w.Type {
			t.Errorf("condition[%d].Type = %q, want %q", i, g.Type, w.Type)
		}
		if g.Status != w.Status {
			t.Errorf("condition[%d].Status = %q, want %q", i, g.Status, w.Status)
		}
		if g.Reason != w.Reason {
			t.Errorf("condition[%d].Reason = %q, want %q", i, g.Reason, w.Reason)
		}
	}
}

func titleContains(s, substrate string) bool {
	return strings.Contains(s, substrate)
}

func TestCellReconciler_Reconcile(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		cell            *multigresv1alpha1.Cell
		existingObjects []client.Object
		failureConfig   *testutil.FailureConfig
		wantErr         bool
		wantRequeue     bool
		assertFunc      func(t *testing.T, c client.Client, cell *multigresv1alpha1.Cell)
	}{
		////----------------------------------------
		///   Success
		//------------------------------------------
		"create all resources for new Cell": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			existingObjects: []client.Object{},
			wantRequeue:     true,
			assertFunc: func(t *testing.T, c client.Client, cell *multigresv1alpha1.Cell) {
				hashedName := buildHashedName(
					cell.Labels["multigres.com/cluster"],
					string(cell.Spec.Name),
				)
				// Verify Multigateway Deployment was created
				mgDeploy := &appsv1.Deployment{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedName, Namespace: "default"},
					mgDeploy); err != nil {
					t.Errorf("Multigateway Deployment should exist: %v", err)
				}

				// Verify Multigateway Service was created
				mgSvc := &corev1.Service{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedName, Namespace: "default"},
					mgSvc); err != nil {
					t.Errorf("Multigateway Service should exist: %v", err)
				}

				// Verify defaults
				const wantReplicas int32 = 1
				if *mgDeploy.Spec.Replicas != wantReplicas {
					t.Errorf(
						"Multigateway Deployment replicas = %d, want %d",
						*mgDeploy.Spec.Replicas,
						wantReplicas,
					)
				}
			},
		},
		"update existing resources": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-cell",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone2",
					Images: multigresv1alpha1.CellImages{
						Multigateway: "custom/multigateway:v1.0.0",
					},
					Multigateway: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(5)),
					},
				},
			},
			existingObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "existing-cell-multigateway",
						Namespace: "default",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)), // will be updated to 5
					},
					Status: appsv1.DeploymentStatus{
						Replicas:      2,
						ReadyReplicas: 2,
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "existing-cell-multigateway",
						Namespace: "default",
					},
				},
			},
			assertFunc: func(t *testing.T, c client.Client, cell *multigresv1alpha1.Cell) {
				hashedName := buildHashedName(
					cell.Labels["multigres.com/cluster"],
					string(cell.Spec.Name),
				)
				mgDeploy := &appsv1.Deployment{}
				err := c.Get(t.Context(), types.NamespacedName{
					Name:      hashedName,
					Namespace: "default",
				}, mgDeploy)
				if err != nil {
					t.Fatalf("Failed to get Multigateway Deployment: %v", err)
				}

				if *mgDeploy.Spec.Replicas != 5 {
					t.Errorf(
						"Multigateway Deployment replicas = %d, want 5",
						*mgDeploy.Spec.Replicas,
					)
				}

				if len(mgDeploy.Spec.Template.Spec.Containers) == 0 {
					t.Fatal("Multigateway Deployment has no containers")
				}
				if mgDeploy.Spec.Template.Spec.Containers[0].Image != "custom/multigateway:v1.0.0" {
					t.Errorf(
						"Multigateway image = %s, want custom/multigateway:v1.0.0",
						mgDeploy.Spec.Template.Spec.Containers[0].Image,
					)
				}
			},
		},

		"deletion - early exit": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-cell-deletion",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone3",
				},
			},
			existingObjects: []client.Object{
				&multigresv1alpha1.Cell{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-cell-deletion",
						Namespace:         "default",
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
						Finalizers:        []string{"testing"},
					},
					Spec: multigresv1alpha1.CellSpec{
						Name: "zone3",
					},
				},
			},
			assertFunc: func(t *testing.T, c client.Client, cell *multigresv1alpha1.Cell) {
				// Reconcile should just return without error and without creating resources
				hashedName := buildHashedName(
					cell.Labels["multigres.com/cluster"],
					string(cell.Spec.Name),
				)
				mgDeploy := &appsv1.Deployment{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: hashedName, Namespace: "default"},
					mgDeploy); err == nil {
					t.Errorf("Multigateway Deployment should NOT exist")
				}
			},
		},

		"all replicas ready status": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell-ready",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone4",
					Multigateway: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(2)),
					},
				},
			},
			existingObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cell-ready-multigateway",
						Namespace: "default",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)),
					},
					Status: appsv1.DeploymentStatus{
						Replicas:      2,
						ReadyReplicas: 2,
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cell-ready-multigateway",
						Namespace: "default",
					},
				},
			},
			assertFunc: func(t *testing.T, c client.Client, cell *multigresv1alpha1.Cell) {
				updatedCell := &multigresv1alpha1.Cell{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: "test-cell-ready", Namespace: "default"},
					updatedCell); err != nil {
					t.Fatalf("Failed to get Cell: %v", err)
				}

				assertConditions(
					t,
					updatedCell.Status.Conditions,
					conditionAssertion{
						Type:   "Available",
						Status: metav1.ConditionTrue,
						Reason: "MultigatewayAvailable",
					},
					conditionAssertion{
						Type:   "Ready",
						Status: metav1.ConditionTrue,
						Reason: "MultigatewayReady",
					},
				)

				if got, want := updatedCell.Status.GatewayReplicas, int32(2); got != want {
					t.Errorf("GatewayReplicas = %d, want %d", got, want)
				}
				if got, want := updatedCell.Status.GatewayReadyReplicas, int32(2); got != want {
					t.Errorf("GatewayReadyReplicas = %d, want %d", got, want)
				}
			},
		},
		"not ready status - partial replicas": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell-partial",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone5",
					Multigateway: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(3)),
					},
				},
			},
			wantRequeue: true,
			existingObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cell-partial-multigateway",
						Namespace: "default",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(3)),
					},
					Status: appsv1.DeploymentStatus{
						Replicas:      3,
						ReadyReplicas: 2, // not all ready
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cell-partial-multigateway",
						Namespace: "default",
					},
				},
			},
			assertFunc: func(t *testing.T, c client.Client, cell *multigresv1alpha1.Cell) {
				updatedCell := &multigresv1alpha1.Cell{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: "test-cell-partial", Namespace: "default"},
					updatedCell); err != nil {
					t.Fatalf("Failed to get Cell: %v", err)
				}

				// With 2/3 replicas ready: Available=True (service is up), Ready=False (not converged)
				assertConditions(
					t,
					updatedCell.Status.Conditions,
					conditionAssertion{
						Type:   "Available",
						Status: metav1.ConditionTrue,
						Reason: "MultigatewayAvailable",
					},
					conditionAssertion{
						Type:   "Ready",
						Status: metav1.ConditionFalse,
						Reason: "MultigatewayNotReady",
					},
				)
			},
		},
		"pending deletion annotation sets ReadyForDeletion status": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell-pending-deletion",
					Namespace: "default",
					Annotations: map[string]string{
						multigresv1alpha1.AnnotationPendingDeletion: "true",
					},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone-pending",
					Multigateway: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(1)),
					},
				},
			},
			existingObjects: []client.Object{},
			assertFunc: func(t *testing.T, c client.Client, cell *multigresv1alpha1.Cell) {
				updatedCell := &multigresv1alpha1.Cell{}
				if err := c.Get(t.Context(),
					types.NamespacedName{Name: "test-cell-pending-deletion", Namespace: "default"},
					updatedCell); err != nil {
					t.Fatalf("Failed to get Cell: %v", err)
				}

				found := false
				for _, cond := range updatedCell.Status.Conditions {
					if cond.Type == multigresv1alpha1.ConditionReadyForDeletion {
						found = true
						if cond.Status != metav1.ConditionTrue {
							t.Errorf("expected ReadyForDeletion=True, got %s", cond.Status)
						}
					}
				}
				if !found {
					t.Errorf("expected ConditionReadyForDeletion to be present")
				}
			},
		},
		////----------------------------------------
		///   Error
		//------------------------------------------
		"error on status update": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnStatusPatch: testutil.FailOnObjectName("test-cell", testutil.ErrInjected),
			},
			wantErr: true,
		},
		"error on get cell status patch for pending deletion": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell-pd-patch-err",
					Namespace: "default",
					Annotations: map[string]string{
						multigresv1alpha1.AnnotationPendingDeletion: "true",
					},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone-pd-err",
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnStatusPatch: testutil.FailOnObjectName(
					"test-cell-pd-patch-err",
					testutil.ErrInjected,
				),
			},
			wantErr: true,
		},
		"error on Get Multigateway Deployment in updateStatus (network error)": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell-status",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			existingObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cell-status-multigateway",
						Namespace: "default",
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cell-status-multigateway",
						Namespace: "default",
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				// Fail Multigateway Deployment Get in updateStatus.
				// Since we switched to SSA, there are no Gets in reconcile loop.
				// The only Get is in updateStatus.
				OnGet: testutil.FailKeyAfterNCalls(0, testutil.ErrNetworkTimeout),
			},
			wantErr: true,
		},
		"error on List Pods in updateStatus": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell-list-pods",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			existingObjects: []client.Object{
				&appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cell-list-pods-multigateway",
						Namespace: "default",
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cell-list-pods-multigateway",
						Namespace: "default",
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				OnList: func(list client.ObjectList) error {
					if _, ok := list.(*corev1.PodList); ok {
						return testutil.ErrNetworkTimeout
					}
					return nil
				},
			},
			wantErr: true,
		},
		"error on Multigateway Deployment patch": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if deploy, ok := obj.(*appsv1.Deployment); ok &&
						titleContains(deploy.Name, "multigateway") {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},
		"error on Multigateway Service patch": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if svc, ok := obj.(*corev1.Service); ok &&
						titleContains(svc.Name, "multigateway") {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},

		"error on Get Cell (network error)": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName("test-cell", testutil.ErrNetworkTimeout),
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Calculate hashed name
			clusterName := tc.cell.Labels["multigres.com/cluster"]
			hashedName := buildHashedName(clusterName, string(tc.cell.Spec.Name))
			t.Logf("Computed hashedName: %s", hashedName)

			// Patch existing objects names
			for i, obj := range tc.existingObjects {
				if deploy, ok := obj.(*appsv1.Deployment); ok &&
					titleContains(deploy.Name, "multigateway") {
					t.Logf("Renaming deployment %s to %s", deploy.Name, hashedName)
					deploy.Name = hashedName
					// Also update selector/labels if needed to match
					if deploy.Labels != nil {
						deploy.Labels["app.kubernetes.io/instance"] = hashedName
					}
					// Update match labels
					if deploy.Spec.Selector != nil {
						deploy.Spec.Selector.MatchLabels["app.kubernetes.io/instance"] = hashedName
					}
				}
				if svc, ok := obj.(*corev1.Service); ok && titleContains(svc.Name, "multigateway") {
					svc.Name = hashedName
				}
				tc.existingObjects[i] = obj
			}

			// Create base fake client
			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.existingObjects...).
				WithStatusSubresource(&multigresv1alpha1.Cell{}).
				Build()

			// Wrap fake client with failures if needed
			fakeClient := testutil.NewFakeClientWithFailures(baseClient, tc.failureConfig)

			reconciler := &cell.CellReconciler{
				Client:   fakeClient,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			// Create the Cell resource if not in existing objects
			cellInExisting := false
			for _, obj := range tc.existingObjects {
				if cell, ok := obj.(*multigresv1alpha1.Cell); ok && cell.Name == tc.cell.Name {
					cellInExisting = true
					break
				}
			}
			if !cellInExisting {
				err := fakeClient.Create(t.Context(), tc.cell)
				if err != nil {
					t.Fatalf("Failed to create Cell: %v", err)
				}
			}

			// Reconcile
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tc.cell.Name,
					Namespace: tc.cell.Namespace,
				},
			}

			result, err := reconciler.Reconcile(t.Context(), req)
			if (err != nil) != tc.wantErr {
				t.Errorf("Reconcile() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if tc.wantErr {
				return
			}

			if (result.RequeueAfter != 0) != tc.wantRequeue {
				t.Errorf(
					"Reconcile() requeue = %v, want requeue = %v",
					result.RequeueAfter,
					tc.wantRequeue,
				)
			}

			// Run custom assertions if provided
			if tc.assertFunc != nil {
				tc.assertFunc(t, fakeClient, tc.cell)
			}
		})
	}
}

func TestCellReconciler_ReconcileNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	reconciler := &cell.CellReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	// Reconcile non-existent resource
	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "nonexistent-cell",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(t.Context(), req)
	if err != nil {
		t.Errorf("Reconcile() should not error on NotFound, got: %v", err)
	}
	if result.RequeueAfter > 0 {
		t.Errorf("Reconcile() should not requeue on NotFound")
	}
}
