package testutil

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/multigres/testkit/assert"
)

// TestSendEvent_ChannelFull tests the default branch when event channel is full.
func TestSendEvent_ChannelFull(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	watcher := &ResourceWatcher{
		t:       t,
		eventCh: make(chan ResourceEvent, 1), // Buffer size 1
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
	}

	// Fill the channel
	watcher.sendEvent("ADDED", "Service", svc)

	// This should hit the default branch (channel full, event dropped)
	watcher.sendEvent("UPDATED", "Service", svc)

	// Verify first event is in channel
	evt := <-watcher.eventCh
	c.Eq("test", evt.Name, "Event in channel has Name")
	c.Eq("ADDED", evt.Type, "Event in channel has Type")

	// Second event was dropped (channel was full)
	select {
	case <-watcher.eventCh:
		t.Error("Channel should be empty (second event was dropped)")
	default:
		// Expected - channel is empty because second event was dropped
	}
}

// TestCollectEvents_SubscriberChannelFull tests that events are dropped when subscriber channel is full.
func TestCollectEvents_SubscriberChannelFull(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)

	ctx := t.Context()

	watcher := &ResourceWatcher{
		t:       t,
		eventCh: make(chan ResourceEvent, 10),
		events:  []ResourceEvent{},
	}

	// Start collectEvents
	go watcher.collectEvents(ctx)

	// Create a subscriber with small buffer
	subCh := make(chan ResourceEvent, 1)
	watcher.mu.Lock()
	watcher.subscribers = append(watcher.subscribers, subCh)
	watcher.mu.Unlock()

	// Fill the subscriber channel
	watcher.eventCh <- ResourceEvent{Type: "ADDED", Kind: "Service", Name: "svc1"}
	time.Sleep(10 * time.Millisecond) // Give collectEvents time to process

	// Send another event - this should trigger the default branch (subscriber channel full)
	watcher.eventCh <- ResourceEvent{Type: "ADDED", Kind: "Service", Name: "svc2"}
	time.Sleep(10 * time.Millisecond) // Give collectEvents time to process

	// Verify the main events slice has both events
	watcher.mu.RLock()
	eventsCount := len(watcher.events)
	watcher.mu.RUnlock()

	c.Eq(2, eventsCount, "watcher.events length")

	// Verify subscriber channel only has first event
	select {
	case evt := <-subCh:
		c.Eq("svc1", evt.Name, "First event Name")
	default:
		t.Error("Expected first event in subscriber channel")
	}

	// Second event should have been dropped due to full channel
	select {
	case evt := <-subCh:
		t.Errorf("Unexpected second event in subscriber channel: %+v", evt)
	default:
		// Expected - channel doesn't have second event
	}
}

// TestUnsubscribe_AlreadyClosed tests unsubscribe when subscribers is nil.
func TestUnsubscribe_AlreadyClosed(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		t:           t,
		subscribers: nil, // Simulate collectEvents already closed
	}

	ch := make(chan ResourceEvent, 1)

	// Should not panic
	watcher.unsubscribe(ch)
}

// TestUnsubscribe_NotFound tests unsubscribe with channel not in list.
func TestUnsubscribe_NotFound(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		t:           t,
		subscribers: []chan ResourceEvent{make(chan ResourceEvent, 1)},
	}

	differentCh := make(chan ResourceEvent, 1)

	// Should not panic
	watcher.unsubscribe(differentCh)

	assert.NewCollecting(t).
		Len(watcher.subscribers, 1, "unsubscribe() should not remove other channels, got %d subscribers", len(watcher.subscribers))
}
