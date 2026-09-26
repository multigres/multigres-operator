package toposerver

import (
	"errors"
	"testing"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"

	"github.com/multigres/testkit/assert"
)

func TestValidateEtcdStorageClassDependency(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	t.Run("empty storage class sets True/NotSpecified condition", func(t *testing.T) {
		t.Parallel()
		ck := assert.NewAborting(t)
		ts := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{Name: "test-ts", Namespace: "default"},
			Spec:       multigresv1alpha1.TopoServerSpec{},
		}
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(ts).
			WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
			Build()
		r := &TopoServerReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		ck.NoError(r.validateEtcdStorageClassDependency(t.Context(), ts), "unexpected error")

		var updated multigresv1alpha1.TopoServer
		ck.NoError(
			c.Get(t.Context(), client.ObjectKeyFromObject(ts), &updated),
			"failed to read toposerver",
		)
		cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
		ck.False(cond == nil || cond.Status != metav1.ConditionTrue ||
			cond.Reason != storageClassNotSpecifiedReason, "unexpected condition: %#v", cond)
	})

	t.Run("ready storage class sets True/Ready condition", func(t *testing.T) {
		t.Parallel()
		ck := assert.NewAborting(t)
		ts := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{Name: "test-ts", Namespace: "default"},
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
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(ts, sc).
			WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
			Build()
		r := &TopoServerReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

		ck.NoError(r.validateEtcdStorageClassDependency(t.Context(), ts), "unexpected error")

		var updated multigresv1alpha1.TopoServer
		ck.NoError(
			c.Get(t.Context(), client.ObjectKeyFromObject(ts), &updated),
			"failed to read toposerver",
		)
		cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
		ck.False(cond == nil || cond.Status != metav1.ConditionTrue ||
			cond.Reason != storageClassReadyReason, "unexpected condition: %#v", cond)
	})

	t.Run(
		"immediate binding mode sets False condition and returns dependency error",
		func(t *testing.T) {
			t.Parallel()
			ck := assert.NewAborting(t)
			ts := &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "test-ts", Namespace: "default"},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{Class: "immediate-sc"},
					},
				},
			}
			sc := &storagev1.StorageClass{
				ObjectMeta:        metav1.ObjectMeta{Name: "immediate-sc"},
				VolumeBindingMode: ptr.To(storagev1.VolumeBindingImmediate),
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(ts, sc).
				WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
				Build()
			r := &TopoServerReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			err := r.validateEtcdStorageClassDependency(t.Context(), ts)
			ck.False(
				err == nil || !isStorageClassDependencyError(err),
				"expected StorageClass dependency error, got: %v",
				err,
			)

			var updated multigresv1alpha1.TopoServer
			ck.NoError(c.Get(
				t.Context(),
				client.ObjectKeyFromObject(ts),
				&updated,
			), "failed to read toposerver")
			cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
			ck.False(cond == nil || cond.Status != metav1.ConditionFalse ||
				cond.Reason != storageClassBindingModeReason, "unexpected condition: %#v", cond)
		},
	)

	t.Run(
		"missing storage class sets False/NotFound condition and returns dependency error",
		func(t *testing.T) {
			t.Parallel()
			ck := assert.NewAborting(t)
			ts := &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "test-ts", Namespace: "default"},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{Class: "missing-sc"},
					},
				},
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(ts).
				WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
				Build()
			r := &TopoServerReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			err := r.validateEtcdStorageClassDependency(t.Context(), ts)
			ck.False(
				err == nil || !isMissingStorageClassDependency(err),
				"expected missing dependency error, got: %v",
				err,
			)

			var updated multigresv1alpha1.TopoServer
			ck.NoError(c.Get(
				t.Context(),
				client.ObjectKeyFromObject(ts),
				&updated,
			), "failed to read toposerver")
			cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
			ck.False(cond == nil || cond.Status != metav1.ConditionFalse ||
				cond.Reason != storageClassNotFoundReason, "unexpected condition: %#v", cond)
		},
	)

	t.Run("API error propagates without setting condition", func(t *testing.T) {
		t.Parallel()
		c := assert.NewAborting(t)
		ts := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{Name: "test-ts", Namespace: "default"},
			Spec: multigresv1alpha1.TopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Storage: multigresv1alpha1.StorageSpec{Class: "some-sc"},
				},
			},
		}
		baseClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(ts).
			WithStatusSubresource(&multigresv1alpha1.TopoServer{}).
			Build()
		fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnGet: testutil.FailOnKeyName("some-sc", testutil.ErrNetworkTimeout),
		})
		r := &TopoServerReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.validateEtcdStorageClassDependency(t.Context(), ts)
		c.Error(err, "expected error, got nil")
		c.False(
			isMissingStorageClassDependency(err),
			"expected non-dependency error, got dependency error",
		)

		var updated multigresv1alpha1.TopoServer
		c.NoError(baseClient.Get(
			t.Context(),
			client.ObjectKeyFromObject(ts),
			&updated,
		), "failed to read toposerver")
		cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
		c.False(
			cond != nil && cond.Status == metav1.ConditionFalse,
			"condition should not be False on API error, got: %#v",
			cond,
		)
	})
}

func TestIsMissingStorageClassDependencyWrapped(t *testing.T) {
	c := assert.NewAborting(t)
	err := errors.New("other")
	c.False(isMissingStorageClassDependency(err), "expected false for non-dependency error")

	wrapped := errors.Join(errors.New("outer"), &missingStorageClassDependencyError{className: "x"})
	c.True(
		isMissingStorageClassDependency(wrapped),
		"expected true for wrapped missing dependency error",
	)
}

// findCondition returns the condition with the given type, or nil if not found.
func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}
