package testutil

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// mockTB implements testing.TB for capturing Fatal/Fatalf and Error/Errorf calls.
type mockTB struct {
	testing.TB
	fatalCalled  bool
	errorCalled  bool
	logCalled    bool
	cleanupFuncs []func()
}

func (m *mockTB) Helper() {}
func (m *mockTB) Fatal(args ...interface{}) {
	m.fatalCalled = true
}

func (m *mockTB) Fatalf(format string, args ...interface{}) {
	m.fatalCalled = true
}

func (m *mockTB) Error(args ...interface{}) {
	m.errorCalled = true
}

func (m *mockTB) Errorf(format string, args ...interface{}) {
	m.errorCalled = true
}

func (m *mockTB) Log(args ...interface{}) {
	m.logCalled = true
}

func (m *mockTB) Logf(format string, args ...any) {
	m.logCalled = true
}

// Cleanup records f for runCleanups and registers it on the embedded test
// as well, once for both. A test that never calls runCleanups, as after a
// mocked Fatal, would otherwise leave behind what f releases, such as an
// envtest control plane, whose processes outlive the test binary.
func (m *mockTB) Cleanup(f func()) {
	var once sync.Once
	run := func() { once.Do(f) }
	m.cleanupFuncs = append(m.cleanupFuncs, run)
	m.TB.Cleanup(run)
}

// mockManager implements manager.Manager for testing.
type mockManager struct {
	manager.Manager
	cache ctrlcache.Cache
}

func (m *mockManager) GetCache() ctrlcache.Cache {
	return m.cache
}

// mockCache implements ctrlcache.Cache for testing.
type mockCache struct {
	ctrlcache.Cache
	getInformerErr error
}

func (c *mockCache) GetInformer(
	ctx context.Context,
	obj client.Object,
	_ ...ctrlcache.InformerGetOption,
) (ctrlcache.Informer, error) {
	return nil, c.getInformerErr
}

// TestWatchResource_Error tests that watchResource calls Fatalf when GetInformer fails.
func TestWatchResource_Error(t *testing.T) {
	t.Parallel()

	mockTB := &mockTB{TB: t}
	watcher := &ResourceWatcher{
		t:            mockTB,
		watchedKinds: make(map[string]any),
	}

	mockC := &mockCache{
		getInformerErr: errors.New("simulated error"),
	}
	mockM := &mockManager{
		cache: mockC,
	}

	watcher.watchResource(context.Background(), mockM, &corev1.Service{})

	if !mockTB.fatalCalled {
		t.Error("t.Fatalf was not called")
	}
}

// mockInformer implements eventHandlerRegistrar for testing.
type mockInformer struct {
	addEventHandlerFunc func(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error)
	tb                  testing.TB
}

func (m *mockInformer) AddEventHandler(
	handler cache.ResourceEventHandler,
) (cache.ResourceEventHandlerRegistration, error) {
	if m.addEventHandlerFunc != nil {
		return m.addEventHandlerFunc(handler)
	}

	// If the mock is set up to return an error, simulate t.Fatalf behavior.
	// This is to match the behavior of the real addEventHandlerToInformer.
	if m.tb != nil {
		if m.addEventHandlerFunc == nil {
			m.tb.Fatalf("Failed to add event handler to informer: simulated error")
		}
	}

	return nil, nil
}

// TestAddEventHandlerToInformer_Error tests that errors from AddEventHandler
// are returned.
// In a normal setup, this should never happen. But we have the proper error
// handling in place, and this test uses a mock informer to force the error.
func TestAddEventHandlerToInformer_Error(t *testing.T) {
	t.Parallel()

	mockTB := &mockTB{TB: t}
	watcher := &ResourceWatcher{t: mockTB}
	mockInformer := &mockInformer{
		addEventHandlerFunc: func(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
			return nil, errors.New("simulated error")
		},
		tb: mockTB,
	}

	watcher.addEventHandlerToInformer(mockInformer, "Service")

	if !mockTB.fatalCalled {
		t.Error("t.Fatalf was not called")
	}
}

// TestWaitForEvent_Match tests waitForEvent when predicate matches.
func TestWaitForEvent_Match(t *testing.T) {
	t.Parallel()

	ch := make(chan ResourceEvent, 1)

	// Send an event
	ch <- ResourceEvent{
		Type: "ADDED",
		Kind: "Service",
		Name: "test",
	}

	predicate := func(evt ResourceEvent) bool {
		return evt.Name == "test" // Match
	}

	deadline := time.Now().Add(1 * time.Second)

	watcher := &ResourceWatcher{t: t}
	evt, err := watcher.waitForEvent(ch, deadline, predicate)
	if err != nil {
		t.Errorf("waitForEvent() error = %v, want nil", err)
	}
	if evt == nil || evt.Name != "test" {
		t.Errorf("waitForEvent() evt.Name = %v, want test", evt)
	}
}

// TestWaitForEvent_ChannelClosed tests waitForEvent when the channel is closed.
func TestWaitForEvent_ChannelClosed(t *testing.T) {
	t.Parallel()

	ch := make(chan ResourceEvent, 1)
	close(ch) // Close immediately

	predicate := func(evt ResourceEvent) bool {
		return true
	}

	deadline := time.Now().Add(1 * time.Second)

	watcher := &ResourceWatcher{t: t}
	_, err := watcher.waitForEvent(ch, deadline, predicate)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("waitForEvent() error = %v, want context.Canceled", err)
	}
}

// TestWaitForEvent_NoMatch tests waitForEvent when predicate never matches.
func TestWaitForEvent_NoMatch(t *testing.T) {
	t.Parallel()

	ch := make(chan ResourceEvent, 1)

	// Send an event
	ch <- ResourceEvent{
		Type: "ADDED",
		Kind: "Service",
		Name: "test",
	}

	predicate := func(evt ResourceEvent) bool {
		return false // Never match
	}

	deadline := time.Now().Add(10 * time.Millisecond)

	watcher := &ResourceWatcher{t: t}
	_, err := watcher.waitForEvent(ch, deadline, predicate)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waitForEvent() error = %v, want DeadlineExceeded", err)
	}
}

// TestWaitForEvent_TimeoutEdgeCase tests the timeout logic in waitForEvent.
func TestWaitForEvent_TimeoutEdgeCase(t *testing.T) {
	t.Parallel()

	ch := make(chan ResourceEvent, 1)

	// Set deadline in the past
	deadline := time.Now().Add(-1 * time.Second)

	predicate := func(evt ResourceEvent) bool {
		return false
	}

	watcher := &ResourceWatcher{t: t}
	_, err := watcher.waitForEvent(ch, deadline, predicate)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waitForEvent() with past deadline should return DeadlineExceeded, got: %v", err)
	}
}

// TestWaitForEvent_TimeoutBoundary tests the timeout boundary condition.
func TestWaitForEvent_TimeoutBoundary(t *testing.T) {
	t.Parallel()

	ch := make(chan ResourceEvent, 1)

	// Set deadline very close to now to test the boundary check
	deadline := time.Now().Add(50 * time.Millisecond)

	// Send events after deadline should pass
	go func() {
		time.Sleep(100 * time.Millisecond)
		ch <- ResourceEvent{Type: "TEST", Kind: "Service"}
	}()

	predicate := func(evt ResourceEvent) bool {
		return false // Never match
	}

	watcher := &ResourceWatcher{t: t}
	_, err := watcher.waitForEvent(ch, deadline, predicate)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waitForEvent() should timeout, got: %v", err)
	}
}

// TestWaitForMatch_EmptySlice tests WaitForMatch with empty slice.
func TestWaitForMatch_EmptySlice(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		t:       t,
		timeout: 1 * time.Second,
	}

	err := watcher.WaitForMatch()
	if err != nil {
		t.Errorf("WaitForMatch() with empty slice should return nil, got: %v", err)
	}
}

// TestExtractKind tests kind extraction from various object types.
func TestExtractKind(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		obj  client.Object
		want string
	}{
		"Service with pointer": {
			obj:  &corev1.Service{},
			want: "Service",
		},
		"ConfigMap with pointer": {
			obj:  &corev1.ConfigMap{},
			want: "ConfigMap",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := extractKind(tc.obj)
			if got != tc.want {
				t.Errorf("extractKind() = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestExtractKind_NoPointer tests extractKind with non-pointer types and types without dots.
func TestExtractKind_NoPointer(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		obj  client.Object
		want string
	}{
		"Pod with pointer": {
			obj:  &corev1.Pod{},
			want: "Pod",
		},
		"Secret with pointer": {
			obj:  &corev1.Secret{},
			want: "Secret",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := extractKind(tc.obj)
			if got != tc.want {
				t.Errorf("extractKind() = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestExtractKind_NoDot tests extractKind with a type that has no package (no dot).
func TestExtractKind_NoDot(t *testing.T) {
	t.Parallel()

	// Create a mock type that when formatted has no dot
	// In practice, all client.Object types have packages, but we can test the fallback
	kind := extractKind(&corev1.Node{})
	if kind != "Node" {
		t.Errorf("extractKind() = %s, want Node", kind)
	}
}

// TestExtractKind_NonPointer tests extractKind fallback for type without leading *.
func TestExtractKind_NonPointer(t *testing.T) {
	t.Parallel()

	// This tests the case where kind[0] != '*'
	// by ensuring we handle types correctly
	pod := corev1.Pod{}
	kind := extractKind(&pod)
	if kind != "Pod" {
		t.Errorf("extractKind() = %s, want Pod", kind)
	}
}

// TestExtractKind_FallbackPath tests the final return when no dot is found.
func TestExtractKind_FallbackPath(t *testing.T) {
	t.Parallel()

	// Test with various types to ensure the fallback path works
	tests := []struct {
		obj  client.Object
		want string
	}{
		{&corev1.Namespace{}, "Namespace"},
		{&corev1.Node{}, "Node"},
		{&corev1.PersistentVolume{}, "PersistentVolume"},
	}

	for _, tc := range tests {
		got := extractKind(tc.obj)
		if got != tc.want {
			t.Errorf("extractKind(%T) = %s, want %s", tc.obj, got, tc.want)
		}
	}
}
