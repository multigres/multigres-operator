package toposerver

import (
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"

	"github.com/multigres/testkit/assert"
)

func TestTopoServerReconciler_Reconcile(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		toposerver      *multigresv1alpha1.TopoServer
		existingObjects []client.Object
		failureConfig   *testutil.FailureConfig
		// TODO: If wantErr is false but failureConfig is set, assertions may fail
		// due to failure injection. This should be addressed when we need to test
		// partial failures that don't prevent reconciliation success.
		wantErr     bool
		wantRequeue bool
		assertFunc  func(t *testing.T, c client.Client, toposerver *multigresv1alpha1.TopoServer)
	}{
		////----------------------------------------
		///   Success
		//------------------------------------------
		"create all resources for new TopoServer": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			existingObjects: []client.Object{},
			wantRequeue:     true,
			assertFunc: func(t *testing.T, c client.Client, toposerver *multigresv1alpha1.TopoServer) {
				ck := assert.NewCollecting(t)
				// Verify all workload, disruption, and service resources were created.
				sts := &appsv1.StatefulSet{}
				ck.NoError(c.Get(t.Context(),
					types.NamespacedName{Name: "test-toposerver", Namespace: "default"},
					sts), "StatefulSet should exist")

				pdb := &policyv1.PodDisruptionBudget{}
				ck.NoError(c.Get(t.Context(),
					types.NamespacedName{Name: "test-toposerver", Namespace: "default"},
					pdb), "PodDisruptionBudget should exist")

				headlessSvc := &corev1.Service{}
				ck.NoError(c.Get(t.Context(),
					types.NamespacedName{Name: "test-toposerver-headless", Namespace: "default"},
					headlessSvc), "Headless Service should exist")

				clientSvc := &corev1.Service{}
				ck.NoError(c.Get(t.Context(),
					types.NamespacedName{Name: "test-toposerver", Namespace: "default"},
					clientSvc), "Client Service should exist")

				// Verify defaults
				ck.Eq(int32(3), *sts.Spec.Replicas, "StatefulSet replicas")
			},
		},
		"update existing resources": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "existing-toposerver",
					Namespace: "default",

					Labels: map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Replicas: ptr.To(int32(5)),
						Image:    "quay.io/coreos/etcd:v3.5.15",
					},
				},
			},
			wantRequeue: true,
			existingObjects: []client.Object{
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "existing-toposerver",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: ptr.To(int32(3)), // will be updated to 5
					},
					Status: appsv1.StatefulSetStatus{
						Replicas:      3,
						ReadyReplicas: 3,
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "existing-toposerver-headless",
						Namespace: "default",
					},
				},
				&corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "existing-toposerver",
						Namespace: "default",
					},
				},
			},
			assertFunc: func(t *testing.T, c client.Client, toposerver *multigresv1alpha1.TopoServer) {
				ck := assert.NewCollecting(t)
				sts := &appsv1.StatefulSet{}
				err := c.Get(t.Context(), types.NamespacedName{
					Name:      "existing-toposerver",
					Namespace: "default",
				}, sts)
				ck.Require().NoError(err, "Failed to get StatefulSet")

				ck.Eq(5, *sts.Spec.Replicas, "StatefulSet replicas")

				ck.Eq(
					"quay.io/coreos/etcd:v3.5.15",
					sts.Spec.Template.Spec.Containers[0].Image,
					"StatefulSet image",
				)
			},
		},

		////----------------------------------------
		///   StorageClass Guard
		//------------------------------------------
		"missing StorageClass returns RequeueAfter=10s and no StatefulSet": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-missing-sc",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{Class: "nonexistent-sc"},
					},
				},
			},
			existingObjects: []client.Object{},
			wantRequeue:     true,
			assertFunc: func(t *testing.T, c client.Client, toposerver *multigresv1alpha1.TopoServer) {
				// StatefulSet should not have been created.
				sts := &appsv1.StatefulSet{}
				err := c.Get(t.Context(),
					types.NamespacedName{Name: "test-missing-sc", Namespace: "default"},
					sts)
				assert.NewCollecting(t).
					Error(err, "StatefulSet should not exist when StorageClass is missing")
			},
		},
		"existing StorageClass proceeds normally and creates StatefulSet": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-existing-sc",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{Class: "fast-ssd"},
					},
				},
			},
			existingObjects: []client.Object{
				&storagev1.StorageClass{
					ObjectMeta:        metav1.ObjectMeta{Name: "fast-ssd"},
					VolumeBindingMode: ptr.To(storagev1.VolumeBindingWaitForFirstConsumer),
				},
			},
			wantRequeue: true,
			assertFunc: func(t *testing.T, c client.Client, toposerver *multigresv1alpha1.TopoServer) {
				sts := &appsv1.StatefulSet{}
				assert.NewCollecting(t).NoError(c.Get(t.Context(),
					types.NamespacedName{Name: "test-existing-sc", Namespace: "default"},
					sts), "StatefulSet should exist when StorageClass is present")
			},
		},
		"immediate StorageClass requeues without creating StatefulSet": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-immediate-sc",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{Class: "immediate-sc"},
					},
				},
			},
			existingObjects: []client.Object{
				&storagev1.StorageClass{
					ObjectMeta:        metav1.ObjectMeta{Name: "immediate-sc"},
					VolumeBindingMode: ptr.To(storagev1.VolumeBindingImmediate),
				},
			},
			wantRequeue: true,
			assertFunc: func(t *testing.T, c client.Client, toposerver *multigresv1alpha1.TopoServer) {
				sts := &appsv1.StatefulSet{}
				err := c.Get(t.Context(), client.ObjectKeyFromObject(toposerver), sts)
				assert.NewCollecting(t).
					Error(err, "StatefulSet should not exist with immediate volume binding")
			},
		},

		////----------------------------------------
		///   Error
		//------------------------------------------
		"error on status update": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnStatusPatch: testutil.FailOnObjectName("test-toposerver", testutil.ErrInjected),
			},
			wantErr: true,
		},
		"error on Get StatefulSet in updateStatus (network error)": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver-status",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			existingObjects: []client.Object{
				&appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-toposerver-status",
						Namespace: "default",
					},
				},
			},
			failureConfig: &testutil.FailureConfig{
				// Fail StatefulSet Get in updateStatus.
				// No Gets in reconcile loop with SSA.
				OnGet: testutil.FailKeyAfterNCalls(0, testutil.ErrNetworkTimeout),
			},
			wantErr: true,
		},
		"error on client Service patch": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if svc, ok := obj.(*corev1.Service); ok && svc.Name == "test-toposerver" {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},
		"error on headless Service patch": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if svc, ok := obj.(*corev1.Service); ok &&
						svc.Name == "test-toposerver-headless" {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},
		"error on StatefulSet patch": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnPatch: func(obj client.Object) error {
					if _, ok := obj.(*appsv1.StatefulSet); ok {
						return testutil.ErrPermissionError
					}
					return nil
				},
			},
			wantErr: true,
		},

		"error on Get TopoServer (network error)": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			existingObjects: []client.Object{},
			failureConfig: &testutil.FailureConfig{
				OnGet: testutil.FailOnKeyName("test-toposerver", testutil.ErrNetworkTimeout),
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := assert.NewCollecting(t)

			// Fake clients register unstructured certificate types lazily. Each
			// parallel subtest must own its scheme to avoid concurrent mutation.
			scheme := runtime.NewScheme()
			_ = multigresv1alpha1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)
			_ = corev1.AddToScheme(scheme)
			_ = policyv1.AddToScheme(scheme)
			_ = storagev1.AddToScheme(scheme)

			// Create base fake client
			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.existingObjects...).
				WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
				WithStatusSubresource(&appsv1.StatefulSet{}).
				Build()

			fakeClient := client.Client(baseClient)
			// Wrap with failure injection if configured
			if tc.failureConfig != nil {
				fakeClient = testutil.NewFakeClientWithFailures(baseClient, tc.failureConfig)
			}

			reconciler := &TopoServerReconciler{
				Client:   fakeClient,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			// Create the TopoServer resource if not in existing objects
			toposerverInExisting := false
			for _, obj := range tc.existingObjects {
				if toposerver, ok := obj.(*multigresv1alpha1.TopoServer); ok &&
					toposerver.Name == tc.toposerver.Name {
					toposerverInExisting = true
					break
				}
			}
			if !toposerverInExisting {
				err := fakeClient.Create(t.Context(), tc.toposerver)
				c.Require().NoError(err, "Failed to create TopoServer")
			}

			// Reconcile
			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tc.toposerver.Name,
					Namespace: tc.toposerver.Namespace,
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

			c.Eq(
				tc.wantRequeue,
				(result.RequeueAfter != 0),
				"Reconcile() requeue = %v, want requeue =",
				result.RequeueAfter,
			)

			// Run custom assertions if provided
			if tc.assertFunc != nil {
				tc.assertFunc(t, fakeClient, tc.toposerver)
			}
		})
	}
}

func TestTopoServerReconciler_ReconcileNotFound(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	reconciler := &TopoServerReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	// Reconcile non-existent resource
	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "nonexistent-toposerver",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(t.Context(), req)
	c.NoError(err, "Reconcile() should not error on NotFound, got")
	c.LessOrEqual(0, result.RequeueAfter, "Reconcile() should not requeue on NotFound")
}

func TestTopoServerReconciler_UpdateStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	t.Run("all_replicas_ready_status", func(t *testing.T) {
		c := assert.NewCollecting(t)
		toposerver := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-toposerver-ready",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: multigresv1alpha1.TopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Replicas: ptr.To(int32(3)),
				},
			},
		}

		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-toposerver-ready",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: ptr.To(int32(3)),
			},
			Status: appsv1.StatefulSetStatus{
				Replicas:      3,
				ReadyReplicas: 3,
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, sts).
			WithStatusSubresource(toposerver, sts).
			Build()

		r := &TopoServerReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		c.Require().NoError(r.updateStatus(t.Context(), toposerver), "updateStatus failed")

		updatedTopoServer := &multigresv1alpha1.TopoServer{}
		c.Require().NoError(fakeClient.Get(
			t.Context(),
			client.ObjectKeyFromObject(toposerver),
			updatedTopoServer,
		), "Failed to get TopoServer")

		if len(updatedTopoServer.Status.Conditions) == 0 {
			t.Error("Status.Conditions should not be empty")
		} else {
			readyCondition := updatedTopoServer.Status.Conditions[0]
			c.Eq("Ready", readyCondition.Type, "Condition type")
			c.Eq(metav1.ConditionTrue, readyCondition.Status, "Condition status")
		}
	})

	t.Run("statefulset_update_in_progress", func(t *testing.T) {
		toposerver := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{Name: "topo-update", Namespace: "default"},
		}
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "topo-update",
				Namespace:  "default",
				Generation: 2,
			},
			Spec: appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3))},
			Status: appsv1.StatefulSetStatus{
				ObservedGeneration: 1, // trigger the 'else if'
			},
		}
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, sts).
			WithStatusSubresource(toposerver).
			Build()
		r := &TopoServerReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		assert.NewAborting(t).
			NoError(r.updateStatus(t.Context(), toposerver), "updateStatus failed")
		toposerverUpdated := &multigresv1alpha1.TopoServer{}
		_ = fakeClient.Get(t.Context(), client.ObjectKeyFromObject(toposerver), toposerverUpdated)
		if len(toposerverUpdated.Status.Conditions) > 0 &&
			toposerverUpdated.Status.Conditions[0].Message != "StatefulSet update in progress" {
			t.Errorf(
				"Expected 'StatefulSet update in progress' message, got %s",
				toposerverUpdated.Status.Conditions[0].Message,
			)
		}
	})
}

func TestTopoServerReconciler_FieldOwnershipIsolation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	t.Run("updateStatus patch contains exactly one condition (Ready)", func(t *testing.T) {
		t.Parallel()
		c := assert.NewAborting(t)

		ts := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-field-owner",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: multigresv1alpha1.TopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Replicas: ptr.To(int32(3)),
				},
			},
		}
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-field-owner",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3))},
			Status: appsv1.StatefulSetStatus{
				Replicas:      3,
				ReadyReplicas: 3,
			},
		}

		baseClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(ts, sts).
			WithStatusSubresource(&multigresv1alpha1.TopoServer{}, sts).
			Build()

		// Intercept the status patch to inspect the patch object.
		var capturedPatchObj client.Object
		fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnStatusPatch: func(obj client.Object) error {
				capturedPatchObj = obj
				return nil
			},
		})

		r := &TopoServerReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		c.NoError(r.updateStatus(t.Context(), ts), "updateStatus")

		patchTS, ok := capturedPatchObj.(*multigresv1alpha1.TopoServer)
		c.True(ok, "expected *TopoServer patch, got %T", capturedPatchObj)

		// Exactly one condition: Ready.
		c.Len(
			patchTS.Status.Conditions,
			1,
			"updateStatus patch must contain exactly 1 condition, got %d",
			len(patchTS.Status.Conditions),
		)
		c.Eq("Ready", patchTS.Status.Conditions[0].Type, "expected Ready condition, got")
	})

	t.Run(
		"guard patch contains exactly one condition (StorageClassValid) and no other status fields",
		func(t *testing.T) {
			t.Parallel()
			c := assert.NewAborting(t)

			ts := &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-field-owner-2",
					Namespace: "default",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{Class: "fast-ssd"},
					},
				},
			}
			sc := &storagev1.StorageClass{
				ObjectMeta:        metav1.ObjectMeta{Name: "fast-ssd"},
				VolumeBindingMode: ptr.To(storagev1.VolumeBindingWaitForFirstConsumer),
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(ts, sc).
				WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
				Build()

			// Intercept the status patch to inspect the patch object.
			var capturedPatchObj client.Object
			fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
				OnStatusPatch: func(obj client.Object) error {
					capturedPatchObj = obj
					return nil
				},
			})

			r := &TopoServerReconciler{
				Client:   fakeClient,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			c.NoError(r.validateEtcdStorageClassDependency(t.Context(), ts), "guard")

			patchTS, ok := capturedPatchObj.(*multigresv1alpha1.TopoServer)
			c.True(ok, "expected *TopoServer patch, got %T", capturedPatchObj)

			// Exactly one condition: StorageClassValid.
			c.Len(
				patchTS.Status.Conditions,
				1,
				"guard patch must contain exactly 1 condition, got %d",
				len(patchTS.Status.Conditions),
			)
			scCond := &patchTS.Status.Conditions[0]
			c.Eq(conditionStorageClassValid, scCond.Type, "expected")
			if scCond.Status != metav1.ConditionTrue || scCond.Reason != storageClassReadyReason {
				t.Fatalf("unexpected condition: status=%s reason=%s", scCond.Status, scCond.Reason)
			}

			// No other status fields should be set in the guard patch.
			c.Eq("", patchTS.Status.Phase, "guard patch must not set Phase, got")
			c.Eq("", patchTS.Status.Message, "guard patch must not set Message, got")
			c.Eq("", patchTS.Status.ClientService, "guard patch must not set ClientService, got")
			c.Eq("", patchTS.Status.PeerService, "guard patch must not set PeerService, got")
			c.Eq(
				0,
				patchTS.Status.ObservedGeneration,
				"guard patch must not set ObservedGeneration, got",
			)
		},
	)
}

func TestTopoServerReconciler_StandaloneMocks(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	t.Run("ignore_deleted", func(t *testing.T) {
		c := assert.NewAborting(t)
		toposerver := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-deleted",
				Namespace: "default",
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(toposerver).Build()
		// Now delete it so DeletionTimestamp is set
		c.NoError(fakeClient.Delete(t.Context(), toposerver), "failed to delete")

		r := &TopoServerReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}
		res, err := r.Reconcile(
			t.Context(),
			ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "test-deleted", Namespace: "default"},
			},
		)
		c.NoError(err, "unexpected error")
		c.Eq(0, res.RequeueAfter, "expected no requeue")
	})

	t.Run("healthy_phase", func(t *testing.T) {
		c := assert.NewAborting(t)
		toposerver := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-healthy",
				Namespace: "default",
			},
			Status: multigresv1alpha1.TopoServerStatus{
				Phase: multigresv1alpha1.PhaseHealthy,
			},
		}

		// Build the exact STS and Pods needed so reconcile reaches healthy phase.
		sts, err := BuildStatefulSet(toposerver, scheme)
		c.NoError(err, "failed to build sts")
		sts.Generation = 1
		sts.Status = appsv1.StatefulSetStatus{
			Replicas:           *sts.Spec.Replicas,
			ReadyReplicas:      *sts.Spec.Replicas,
			ObservedGeneration: 1,
		}

		var existing []client.Object
		existing = append(existing, toposerver, sts)

		for i := int32(0); i < *sts.Spec.Replicas; i++ {
			existing = append(existing, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("test-healthy-%d", i),
					Namespace: "default",
					Labels:    sts.Spec.Selector.MatchLabels,
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			})
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
			WithStatusSubresource(&appsv1.StatefulSet{}).
			WithObjects(existing...).
			Build()
		r := &TopoServerReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		res, reconcileErr := r.Reconcile(
			t.Context(),
			ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "test-healthy", Namespace: "default"},
			},
		)
		c.NoError(reconcileErr, "unexpected error")

		updatedTopo := &multigresv1alpha1.TopoServer{}
		_ = fakeClient.Get(t.Context(), client.ObjectKeyFromObject(toposerver), updatedTopo)

		c.Eq(
			0,
			res.RequeueAfter,
			"expected no requeue, but got requeueAfter %v, phase is %s, msg is %s",
			res.RequeueAfter,
			updatedTopo.Status.Phase,
			updatedTopo.Status.Message,
		)
	})

	t.Run("list_error_in_updateStatus", func(t *testing.T) {
		toposerver := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-list-err",
				Namespace: "default",
			},
		}
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "test-list-err", Namespace: "default"},
			Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3))},
			Status:     appsv1.StatefulSetStatus{Replicas: 3, ReadyReplicas: 3},
		}

		baseClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(toposerver, sts).
			WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
			Build()
		fails := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnList: func(list client.ObjectList) error { return testutil.ErrNetworkTimeout },
		})

		r := &TopoServerReconciler{
			Client:   fails,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		_, reconcileErr := r.Reconcile(
			t.Context(),
			ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "test-list-err", Namespace: "default"},
			},
		)
		assert.NewAborting(t).False(reconcileErr == nil ||
			!strings.Contains(
				reconcileErr.Error(),
				"failed to list toposerver pods",
			), "expected list pods error, got %v", reconcileErr)
	})
}
