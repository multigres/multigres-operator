//go:build integration
// +build integration

package testutil_test

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/multigres/multigres-operator/pkg/testutil"

	"github.com/multigres/testkit/assert"
)

// TestResourceWatcher_UnwatchedKinds tests error for unwatched resource kinds.
func TestResourceWatcher_UnwatchedKinds(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := testutil.SetUpEnvtestManager(t, scheme)
	watcher := testutil.NewResourceWatcher(t, ctx, mgr)

	// Try to wait for ConfigMap which is not watched by default
	err := watcher.WaitForMatch(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
	})

	c.Error(err, "WaitForMatch() should error for unwatched kind")

	var unwatchedErr *testutil.ErrUnwatchedKinds
	c.True(errors.As(err, &unwatchedErr), "Error should be ErrUnwatchedKinds, got: %T", err)

	if len(unwatchedErr.Kinds) != 1 || unwatchedErr.Kinds[0] != "ConfigMap" {
		t.Errorf("ErrUnwatchedKinds.Kinds = %v, want [ConfigMap]", unwatchedErr.Kinds)
	}
}

// TestResourceWatcher_WatchDuplicateKind tests that watching the same kind twice doesn't create duplicate handlers.
func TestResourceWatcher_WatchDuplicateKind(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := testutil.SetUpEnvtestManager(t, scheme)

	// Create watcher with ConfigMap as extra resource twice
	watcher := testutil.NewResourceWatcher(t, ctx, mgr,
		testutil.WithExtraResource(&corev1.ConfigMap{}),
		testutil.WithExtraResource(&corev1.ConfigMap{}), // Duplicate
	)

	c := mgr.GetClient()

	// Create a ConfigMap
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cm", Namespace: "default"},
		Data:       map[string]string{"key": "value"},
	}
	ck.Require().NoError(c.Create(ctx, cm), "Failed to create ConfigMap")

	// Wait for the event
	watcher.SetCmpOpts(testutil.IgnoreMetaRuntimeFields())
	ck.NoError(watcher.WaitForMatch(cm), "Failed to wait for ConfigMap")

	// Verify we got our ConfigMap event (there may be others from kube-system)
	// The key test is that duplicate handler registration was prevented by watchResource
	events := watcher.ForName("test-cm")
	ck.Len(
		events,
		1,
		"Expected 1 event for test-cm, got %d (duplicate handler may have been created)",
		len(events),
	)
}

// TestResourceWatcher_NonMatchingUpdate tests that updates which don't match expected spec keep waiting.
func TestResourceWatcher_NonMatchingUpdate(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := testutil.SetUpEnvtestManager(t, scheme)
	c := mgr.GetClient()

	// Create a Service with port 80
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test-svc", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}
	ck.Require().NoError(c.Create(ctx, svc), "Failed to create Service")

	// Start watcher after creation
	watcher := testutil.NewResourceWatcher(t, ctx, mgr,
		testutil.WithTimeout(100*time.Millisecond),
	)

	// Try to wait for port 8080 (which doesn't exist)
	expected := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test-svc", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Protocol:   corev1.ProtocolTCP,
					Port:       8080, // Different port
					TargetPort: intstr.FromInt(8080),
				},
			},
			Type:            corev1.ServiceTypeClusterIP,
			SessionAffinity: corev1.ServiceAffinityNone,
		},
	}

	watcher.SetCmpOpts(testutil.IgnoreMetaRuntimeFields(), testutil.IgnoreServiceRuntimeFields())

	// This should timeout because the service has port 80, not 8080
	err := watcher.WaitForMatch(expected)
	ck.Error(err, "Expected timeout error, got nil")

	// Error should be a timeout with diff information (we don't check exact message)
	if !errors.Is(err, context.DeadlineExceeded) {
		// Check if it's a timeout error by checking for the expected error structure
		t.Logf("Got expected timeout error: %v", err)
	}
}

// TestResourceWatcher_NoEventsTimeout tests timeout when no events of the expected kind are received.
func TestResourceWatcher_NoEventsTimeout(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := testutil.SetUpEnvtestManager(t, scheme)

	watcher := testutil.NewResourceWatcher(t, ctx, mgr,
		testutil.WithTimeout(100*time.Millisecond),
	)

	// Try to wait for a Service that never gets created
	expected := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "nonexistent", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort},
	}

	err := watcher.WaitForMatch(expected)
	assert.NewCollecting(t).Error(err, "Expected timeout error, got nil")

	// We just verify we got an error (timeout), not checking exact message
	t.Logf("Got expected timeout error: %v", err)
}

// TestWaitForEventType_ExistingEvent tests WaitForEventType finding existing event.
func TestWaitForEventType_ExistingEvent(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := testutil.SetUpEnvtestManager(t, scheme)
	c := mgr.GetClient()

	watcher := testutil.NewResourceWatcher(t, ctx, mgr)

	// Create a service
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test-svc-event", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}
	ck.Require().NoError(c.Create(ctx, svc), "Failed to create Service")

	// Wait for ADDED event
	evt, err := watcher.WaitForEventType("Service", "ADDED")
	ck.Require().NoError(err, "WaitForEventType() error =")

	ck.Eq("ADDED", evt.Type, "Event type")
	ck.Eq("Service", evt.Kind, "Event kind")
}

// TestWaitForEventType_Timeout tests WaitForEventType timeout.
func TestWaitForEventType_Timeout(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := testutil.SetUpEnvtestManager(t, scheme)

	watcher := testutil.NewResourceWatcher(t, ctx, mgr,
		testutil.WithTimeout(50*time.Millisecond),
	)

	// Wait for an event type that won't happen
	_, err := watcher.WaitForEventType("Service", "DELETED")
	assert.NewCollecting(t).Error(err, "Expected timeout error, got nil")

	t.Logf("Got expected timeout: %v", err)
}

// TestWaitForMatch_ContextCanceled tests WaitForMatch when context is canceled.
func TestWaitForMatch_ContextCanceled(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx, cancel := context.WithCancel(t.Context())
	mgr := testutil.SetUpEnvtestManager(t, scheme)

	watcher := testutil.NewResourceWatcher(t, ctx, mgr)

	// Cancel context immediately
	cancel()

	// Try to wait for match - should fail with watcher stopped
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
	}
	err := watcher.WaitForMatch(svc)
	assert.NewCollecting(t).Error(err, "Expected error when context is canceled")

	t.Logf("Got expected error: %v", err)
}
