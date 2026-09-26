//go:build integration
// +build integration

package testutil

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/assert"
)

// TestWaitForDeletion_EmptySlice tests WaitForDeletion with empty slice.
func TestWaitForDeletion_EmptySlice(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		t:       t,
		timeout: 1 * time.Second,
	}

	err := watcher.WaitForDeletion()
	assert.NewCollecting(t).
		NoError(err, "WaitForDeletion() with empty slice should return nil, got")
}

// TestDeletionPredicate_NonMatchingEvents tests deletion predicate with various
// event types.
func TestDeletionPredicate_NonMatchingEvents(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := SetUpEnvtestManager(t, scheme)
	c := mgr.GetClient()

	watcher := NewResourceWatcher(t, ctx, mgr)
	watcher.SetCmpOpts(IgnoreMetaRuntimeFields(), IgnoreServiceRuntimeFields())
	watcher.SetTimeout(500 * time.Millisecond)

	go func() {
		time.Sleep(100 * time.Millisecond)

		// Create and update svc1 (UPDATED events won't match deletion predicate)
		svc1 := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "svc-update", Namespace: "default"},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
		}
		_ = c.Create(ctx, svc1)

		time.Sleep(80 * time.Millisecond)

		key := client.ObjectKey{Name: "svc-update", Namespace: "default"}
		if err := c.Get(ctx, key, svc1); err == nil {
			svc1.Spec.Ports[0].Port = 81
			_ = c.Update(ctx, svc1)
		}

		time.Sleep(80 * time.Millisecond)

		// Create and delete svc2 (DELETED event with wrong name)
		svc2 := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "svc-other", Namespace: "default"},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 82}}},
		}
		_ = c.Create(ctx, svc2)
		time.Sleep(80 * time.Millisecond)
		_ = c.Delete(ctx, svc2)
	}()

	// Wait for deletion of non-existent service (times out)
	err := watcher.WaitForDeletion(Obj[corev1.Service]("svc-target", "default"))
	assert.NewCollecting(t).Error(err, "Expected timeout")
}

// TestWaitForSingleDeletion_SuccessWithUpdates tests deletion success after
// seeing UPDATED events.
func TestWaitForSingleDeletion_SuccessWithUpdates(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := SetUpEnvtestManager(t, scheme)
	c := mgr.GetClient()

	watcher := NewResourceWatcher(t, ctx, mgr)
	watcher.SetCmpOpts(IgnoreMetaRuntimeFields(), IgnoreServiceRuntimeFields())

	go func() {
		time.Sleep(100 * time.Millisecond)

		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "svc-upd-del", Namespace: "default"},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
		}
		_ = c.Create(ctx, svc)

		time.Sleep(80 * time.Millisecond)

		// Update multiple times
		for i := range 2 {
			key := client.ObjectKey{Name: "svc-upd-del", Namespace: "default"}
			if err := c.Get(ctx, key, svc); err == nil {
				svc.Spec.Ports[0].Port = int32(81 + i)
				_ = c.Update(ctx, svc)
				time.Sleep(80 * time.Millisecond)
			}
		}

		// Finally delete
		_ = c.Delete(ctx, svc)
	}()

	err := watcher.WaitForDeletion(Obj[corev1.Service]("svc-upd-del", "default"))
	assert.NewCollecting(t).NoError(err, "WaitForDeletion() should succeed, got")
}
