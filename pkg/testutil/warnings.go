package testutil

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LogWarnings logs the Warning events in namespace, those about the object
// named involved when it is not empty, for a test explaining why something
// did not converge. Listing has its own deadline, so a slow API server cannot
// hold a test that has already timed out, and a failed list is logged rather
// than read as no warnings.
func LogWarnings(t testing.TB, c client.Client, namespace, involved string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events := &corev1.EventList{}
	if err := c.List(ctx, events, client.InNamespace(namespace)); err != nil {
		t.Logf("listing events to explain the failure: %v", err)
		return
	}
	for _, e := range events.Items {
		if e.Type == corev1.EventTypeWarning &&
			(involved == "" || e.InvolvedObject.Name == involved) {
			t.Logf("event %s: %q", e.Reason, e.Message)
		}
	}
}
