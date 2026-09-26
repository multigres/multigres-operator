package testutil

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/multigres/testkit/assert"
)

func TestFakeClientWithFailures_Get(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		key     client.ObjectKey
		wantErr bool
	}{
		"no failure - get succeeds": {
			config: nil,
			key: client.ObjectKey{
				Name:      "test-pod",
				Namespace: "default",
			},
			wantErr: false,
		},
		"fail on specific name": {
			config: &FailureConfig{
				OnGet: FailOnKeyName("test-pod", ErrInjected),
			},
			key: client.ObjectKey{
				Name:      "test-pod",
				Namespace: "default",
			},
			wantErr: true,
		},
		"no failure on different name": {
			config: &FailureConfig{
				OnGet: FailOnKeyName("other-pod", ErrInjected),
			},
			key: client.ObjectKey{
				Name:      "test-pod",
				Namespace: "default",
			},
			wantErr: false,
		},
		"always fail": {
			config: &FailureConfig{
				OnGet: func(key client.ObjectKey) error {
					return ErrInjected
				},
			},
			key: client.ObjectKey{
				Name:      "test-pod",
				Namespace: "default",
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			result := &corev1.Pod{}
			err := fakeClient.Get(context.Background(), tc.key, result)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "Get() error")
		})
	}
}

func TestFakeClientWithFailures_Create(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		obj     *corev1.Pod
		wantErr bool
	}{
		"no failure - create succeeds": {
			config: nil,
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "new-pod",
					Namespace: "default",
				},
			},
			wantErr: false,
		},
		"fail on specific object name": {
			config: &FailureConfig{
				OnCreate: FailOnObjectName("new-pod", ErrPermissionError),
			},
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "new-pod",
					Namespace: "default",
				},
			},
			wantErr: true,
		},
		"no failure on different object name": {
			config: &FailureConfig{
				OnCreate: FailOnObjectName("other-pod", ErrPermissionError),
			},
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "new-pod",
					Namespace: "default",
				},
			},
			wantErr: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			err := fakeClient.Create(context.Background(), tc.obj)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "Create() error")
		})
	}
}

func TestFakeClientWithFailures_Update(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		wantErr bool
	}{
		"no failure - update succeeds": {
			config:  nil,
			wantErr: false,
		},
		"fail on update": {
			config: &FailureConfig{
				OnUpdate: FailOnObjectName("test-pod", ErrInjected),
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			err := fakeClient.Update(context.Background(), pod)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "Update() error")
		})
	}
}

func TestFakeClientWithFailures_Delete(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		wantErr bool
	}{
		"no failure - delete succeeds": {
			config:  nil,
			wantErr: false,
		},
		"fail on delete": {
			config: &FailureConfig{
				OnDelete: FailOnObjectName("test-pod", ErrInjected),
			},
			wantErr: true,
		},
		"fail on namespace": {
			config: &FailureConfig{
				OnDelete: FailOnNamespace("default", ErrPermissionError),
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod.DeepCopy()).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			err := fakeClient.Delete(context.Background(), pod)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "Delete() error")
		})
	}
}

func TestFakeClientWithFailures_StatusUpdate(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		wantErr bool
	}{
		"no failure - status update succeeds": {
			config:  nil,
			wantErr: false,
		},
		"fail on status update": {
			config: &FailureConfig{
				OnStatusUpdate: FailOnObjectName("test-pod", ErrInjected),
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				WithStatusSubresource(&corev1.Pod{}).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			err := fakeClient.Status().Update(context.Background(), pod)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "Status().Update() error")
		})
	}
}

func TestFakeClientWithFailures_List(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		wantErr bool
	}{
		"no failure - list succeeds": {
			config:  nil,
			wantErr: false,
		},
		"fail on list": {
			config: &FailureConfig{
				OnList: func(list client.ObjectList) error {
					return ErrInjected
				},
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			podList := &corev1.PodList{}
			err := fakeClient.List(context.Background(), podList)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "List() error")
		})
	}
}

func TestFakeClientWithFailures_Patch(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		wantErr bool
	}{
		"no failure - patch succeeds": {
			config:  nil,
			wantErr: false,
		},
		"fail on patch": {
			config: &FailureConfig{
				OnPatch: FailOnObjectName("test-pod", ErrInjected),
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod.DeepCopy()).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			patch := client.MergeFrom(pod.DeepCopy())
			err := fakeClient.Patch(context.Background(), pod, patch)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "Patch() error")
		})
	}
}

func TestFakeClientWithFailures_DeleteAllOf(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		wantErr bool
	}{
		"no failure - deleteAllOf succeeds": {
			config:  nil,
			wantErr: false,
		},
		"fail on deleteAllOf": {
			config: &FailureConfig{
				OnDeleteAllOf: func(obj client.Object) error {
					return ErrInjected
				},
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			err := fakeClient.DeleteAllOf(
				context.Background(),
				&corev1.Pod{},
				client.InNamespace("default"),
			)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "DeleteAllOf() error")
		})
	}
}

func TestFakeClientWithFailures_StatusPatch(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := map[string]struct {
		config  *FailureConfig
		wantErr bool
	}{
		"no failure - status patch succeeds": {
			config:  nil,
			wantErr: false,
		},
		"fail on status patch": {
			config: &FailureConfig{
				OnStatusPatch: FailOnObjectName("test-pod", ErrInjected),
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			baseClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pod.DeepCopy()).
				WithStatusSubresource(&corev1.Pod{}).
				Build()

			fakeClient := NewFakeClientWithFailures(baseClient, tc.config)

			patch := client.MergeFrom(pod.DeepCopy())
			err := fakeClient.Status().Patch(context.Background(), pod, patch)

			assert.NewCollecting(t).ErrorWhen(tc.wantErr, err, "Status().Patch() error")
		})
	}
}

func TestHelperFunctions_ObjectMatchers(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	tests := map[string]struct {
		setupFn func() func(client.Object) error
		wantErr error
	}{
		"FailOnObjectName - matching name": {
			setupFn: func() func(client.Object) error {
				return FailOnObjectName("test-pod", ErrInjected)
			},
			wantErr: ErrInjected,
		},
		"FailOnObjectName - different name": {
			setupFn: func() func(client.Object) error {
				return FailOnObjectName("other-pod", ErrInjected)
			},
			wantErr: nil,
		},
		"FailOnNamespace - matching namespace": {
			setupFn: func() func(client.Object) error {
				return FailOnNamespace("default", ErrInjected)
			},
			wantErr: ErrInjected,
		},
		"FailOnNamespace - different namespace": {
			setupFn: func() func(client.Object) error {
				return FailOnNamespace("other-ns", ErrInjected)
			},
			wantErr: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fn := tc.setupFn()
			err := fn(pod)

			assert.NewCollecting(t).
				False(err != tc.wantErr, "Expected error %v, got %v", tc.wantErr, err)
		})
	}
}

func TestHelperFunctions_KeyMatchers(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setupFn func() func(client.ObjectKey) error
		key     client.ObjectKey
		wantErr error
	}{
		"FailOnKeyName - matching name": {
			setupFn: func() func(client.ObjectKey) error {
				return FailOnKeyName("test-pod", ErrInjected)
			},
			key:     client.ObjectKey{Name: "test-pod", Namespace: "default"},
			wantErr: ErrInjected,
		},
		"FailOnKeyName - different name": {
			setupFn: func() func(client.ObjectKey) error {
				return FailOnKeyName("other-pod", ErrInjected)
			},
			key:     client.ObjectKey{Name: "test-pod", Namespace: "default"},
			wantErr: nil,
		},
		"FailOnNamespacedKeyName - matching name and namespace": {
			setupFn: func() func(client.ObjectKey) error {
				return FailOnNamespacedKeyName("test-pod", "default", ErrInjected)
			},
			key:     client.ObjectKey{Name: "test-pod", Namespace: "default"},
			wantErr: ErrInjected,
		},
		"FailOnNamespacedKeyName - matching name but different namespace": {
			setupFn: func() func(client.ObjectKey) error {
				return FailOnNamespacedKeyName("test-pod", "default", ErrInjected)
			},
			key:     client.ObjectKey{Name: "test-pod", Namespace: "kube-system"},
			wantErr: nil,
		},
		"FailOnNamespacedKeyName - different name but matching namespace": {
			setupFn: func() func(client.ObjectKey) error {
				return FailOnNamespacedKeyName("test-pod", "default", ErrInjected)
			},
			key:     client.ObjectKey{Name: "other-pod", Namespace: "default"},
			wantErr: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fn := tc.setupFn()
			err := fn(tc.key)

			assert.NewCollecting(t).
				False(err != tc.wantErr, "Expected error %v, got %v", tc.wantErr, err)
		})
	}
}

func TestHelperFunctions_CallCounters(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		nCalls  int
		wantErr error
		calls   []error
	}{
		"FailKeyAfterNCalls - 2 successful then fail": {
			nCalls:  2,
			wantErr: ErrInjected,
			calls:   []error{nil, nil, ErrInjected},
		},
		"FailKeyAfterNCalls - 0 always fails": {
			nCalls:  0,
			wantErr: ErrInjected,
			calls:   []error{ErrInjected, ErrInjected},
		},
		"FailKeyAfterNCalls - 1 successful then fail": {
			nCalls:  1,
			wantErr: ErrPermissionError,
			calls:   []error{nil, ErrPermissionError, ErrPermissionError},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fn := FailKeyAfterNCalls(tc.nCalls, tc.wantErr)
			key := client.ObjectKey{Name: "test", Namespace: "default"}

			for i, wantErr := range tc.calls {
				err := fn(key)
				if err != wantErr {
					t.Errorf("Call %d: expected error %v, got %v", i+1, wantErr, err)
				}
			}
		})
	}
}

func TestHelperFunctions_ObjCallCounters(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	tests := map[string]struct {
		nCalls  int
		wantErr error
		calls   []error
	}{
		"FailObjAfterNCalls - 1 successful then fail": {
			nCalls:  1,
			wantErr: ErrPermissionError,
			calls:   []error{nil, ErrPermissionError},
		},
		"FailObjAfterNCalls - 0 always fails": {
			nCalls:  0,
			wantErr: ErrInjected,
			calls:   []error{ErrInjected, ErrInjected},
		},
		"FailObjAfterNCalls - 2 successful then fail": {
			nCalls:  2,
			wantErr: ErrNetworkTimeout,
			calls:   []error{nil, nil, ErrNetworkTimeout},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fn := FailObjAfterNCalls(tc.nCalls, tc.wantErr)

			for i, wantErr := range tc.calls {
				err := fn(pod)
				if err != wantErr {
					t.Errorf("Call %d: expected error %v, got %v", i+1, wantErr, err)
				}
			}
		})
	}
}

func TestHelperFunctions_ObjListCallCounters(t *testing.T) {
	t.Parallel()

	podList := &corev1.PodList{}

	tests := map[string]struct {
		nCalls  int
		wantErr error
		calls   []error
	}{
		"FailObjListAfterNCalls - 1 successful then fail": {
			nCalls:  1,
			wantErr: ErrNetworkTimeout,
			calls:   []error{nil, ErrNetworkTimeout},
		},
		"FailObjListAfterNCalls - 0 always fails": {
			nCalls:  0,
			wantErr: ErrInjected,
			calls:   []error{ErrInjected, ErrInjected},
		},
		"FailObjListAfterNCalls - 2 successful then fail": {
			nCalls:  2,
			wantErr: ErrPermissionError,
			calls:   []error{nil, nil, ErrPermissionError},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fn := FailObjListAfterNCalls(tc.nCalls, tc.wantErr)

			for i, wantErr := range tc.calls {
				err := fn(podList)
				if err != wantErr {
					t.Errorf("Call %d: expected error %v, got %v", i+1, wantErr, err)
				}
			}
		})
	}
}

func TestHelperFunctions_AlwaysFail(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	key := client.ObjectKey{Name: "test", Namespace: "default"}

	tests := map[string]struct {
		input   any
		wantErr error
	}{
		"AlwaysFail with object": {
			input:   pod,
			wantErr: ErrInjected,
		},
		"AlwaysFail with key": {
			input:   key,
			wantErr: ErrNetworkTimeout,
		},
		"AlwaysFail with list": {
			input:   &corev1.PodList{},
			wantErr: ErrPermissionError,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fn := AlwaysFail(tc.wantErr)
			err := fn(tc.input)

			assert.NewCollecting(t).
				False(err != tc.wantErr, "Expected error %v, got %v", tc.wantErr, err)
		})
	}
}

func TestHelperFunctions_Panic(t *testing.T) {
	t.Parallel()

	t.Run("FailOnObjectName - panics on nil object", func(t *testing.T) {
		t.Parallel()

		defer func() {
			assert.NewCollecting(t).
				NotNil(recover(), "Expected panic when meta.Accessor fails on nil")
		}()

		fn := FailOnObjectName("test", ErrInjected)
		_ = fn(nil) // Should panic
	})

	t.Run("FailOnNamespace - panics on nil object", func(t *testing.T) {
		t.Parallel()

		defer func() {
			assert.NewCollecting(t).
				NotNil(recover(), "Expected panic when meta.Accessor fails on nil")
		}()

		fn := FailOnNamespace("default", ErrInjected)
		_ = fn(nil) // Should panic
	})
}
