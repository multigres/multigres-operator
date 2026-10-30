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

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

func TestValidateEtcdStorageClassDependency(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	t.Run("empty storage class sets True/NotSpecified condition", func(t *testing.T) {
		t.Parallel()
		ts := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{Name: "test-ts", Namespace: "default"},
			Spec:       multigresv1alpha1.TopoServerSpec{},
		}
		c := newTestClient(t, scheme, ts)
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.validateEtcdStorageClassDependency(t.Context(), ts); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated multigresv1alpha1.TopoServer
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(ts), &updated); err != nil {
			t.Fatalf("failed to read toposerver: %v", err)
		}
		cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
		if cond == nil || cond.Status != metav1.ConditionTrue ||
			cond.Reason != storageClassNotSpecifiedReason {
			t.Fatalf("unexpected condition: %#v", cond)
		}
	})

	t.Run("ready storage class sets True/Ready condition", func(t *testing.T) {
		t.Parallel()
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
		c := newTestClient(t, scheme, ts, sc)
		r := &TopoServerReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		if err := r.validateEtcdStorageClassDependency(t.Context(), ts); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated multigresv1alpha1.TopoServer
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(ts), &updated); err != nil {
			t.Fatalf("failed to read toposerver: %v", err)
		}
		cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
		if cond == nil || cond.Status != metav1.ConditionTrue ||
			cond.Reason != storageClassReadyReason {
			t.Fatalf("unexpected condition: %#v", cond)
		}
	})

	t.Run(
		"immediate binding mode sets False condition and returns dependency error",
		func(t *testing.T) {
			t.Parallel()
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
			c := newTestClient(t, scheme, ts, sc)
			r := &TopoServerReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			err := r.validateEtcdStorageClassDependency(t.Context(), ts)
			if err == nil || !isStorageClassDependencyError(err) {
				t.Fatalf("expected StorageClass dependency error, got: %v", err)
			}

			var updated multigresv1alpha1.TopoServer
			if getErr := c.Get(
				t.Context(),
				client.ObjectKeyFromObject(ts),
				&updated,
			); getErr != nil {
				t.Fatalf("failed to read toposerver: %v", getErr)
			}
			cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
			if cond == nil || cond.Status != metav1.ConditionFalse ||
				cond.Reason != storageClassBindingModeReason {
				t.Fatalf("unexpected condition: %#v", cond)
			}
		},
	)

	t.Run(
		"missing storage class sets False/NotFound condition and returns dependency error",
		func(t *testing.T) {
			t.Parallel()
			ts := &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{Name: "test-ts", Namespace: "default"},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{Class: "missing-sc"},
					},
				},
			}
			c := newTestClient(t, scheme, ts)
			r := &TopoServerReconciler{
				Client:   c,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			err := r.validateEtcdStorageClassDependency(t.Context(), ts)
			if err == nil || !isMissingStorageClassDependency(err) {
				t.Fatalf("expected missing dependency error, got: %v", err)
			}

			var updated multigresv1alpha1.TopoServer
			if getErr := c.Get(
				t.Context(),
				client.ObjectKeyFromObject(ts),
				&updated,
			); getErr != nil {
				t.Fatalf("failed to read toposerver: %v", getErr)
			}
			cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
			if cond == nil || cond.Status != metav1.ConditionFalse ||
				cond.Reason != storageClassNotFoundReason {
				t.Fatalf("unexpected condition: %#v", cond)
			}
		},
	)

	t.Run("API error propagates without setting condition", func(t *testing.T) {
		t.Parallel()
		ts := &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{Name: "test-ts", Namespace: "default"},
			Spec: multigresv1alpha1.TopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{
					Storage: multigresv1alpha1.StorageSpec{Class: "some-sc"},
				},
			},
		}
		baseClient := newTestClient(t, scheme, ts)
		fakeClient := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnGet: testutil.FailOnKeyName("some-sc", testutil.ErrNetworkTimeout),
		})
		r := &TopoServerReconciler{
			Client:   fakeClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		err := r.validateEtcdStorageClassDependency(t.Context(), ts)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if isMissingStorageClassDependency(err) {
			t.Fatal("expected non-dependency error, got dependency error")
		}

		var updated multigresv1alpha1.TopoServer
		if getErr := baseClient.Get(
			t.Context(),
			client.ObjectKeyFromObject(ts),
			&updated,
		); getErr != nil {
			t.Fatalf("failed to read toposerver: %v", getErr)
		}
		cond := findCondition(updated.Status.Conditions, conditionStorageClassValid)
		if cond != nil && cond.Status == metav1.ConditionFalse {
			t.Fatalf("condition should not be False on API error, got: %#v", cond)
		}
	})
}

func TestIsMissingStorageClassDependencyWrapped(t *testing.T) {
	err := errors.New("other")
	if isMissingStorageClassDependency(err) {
		t.Fatal("expected false for non-dependency error")
	}

	wrapped := errors.Join(errors.New("outer"), &missingStorageClassDependencyError{className: "x"})
	if !isMissingStorageClassDependency(wrapped) {
		t.Fatal("expected true for wrapped missing dependency error")
	}
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
