//go:build integration && verbose
// +build integration,verbose

// Edge case tests for match functions that require the verbose build tag.

package testutil

import (
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/assert"
)

var testError = errors.New("test error for coverage")

// TestVerboseDiffs_ExistingNonMatch tests verbose diff logging when existing event doesn't match.
func TestVerboseDiffs_ExistingNonMatch(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := SetUpEnvtestManager(t, scheme)
	c := mgr.GetClient()

	// Create Service before watcher starts
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-svc", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}
	ck.Require().NoError(c.Create(ctx, svc), "Failed to create Service")

	time.Sleep(100 * time.Millisecond)

	watcher := NewResourceWatcher(t, ctx, mgr,
		WithTimeout(100*time.Millisecond),
	)

	time.Sleep(100 * time.Millisecond)

	// Try to match different spec - logs verbose diffs for existing event
	expected := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-svc", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Protocol: corev1.ProtocolTCP, Port: 9999, TargetPort: intstr.FromInt(9999)},
			},
			Type:            corev1.ServiceTypeClusterIP,
			SessionAffinity: corev1.ServiceAffinityNone,
		},
	}

	watcher.SetCmpOpts(IgnoreMetaRuntimeFields(), IgnoreServiceRuntimeFields())

	err := watcher.WaitForMatch(expected)
	ck.Error(err, "Expected timeout error")
}

// TestVerboseDiffs_IncomingEvents tests verbose diff logging for incoming events.
func TestVerboseDiffs_IncomingEvents(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := SetUpEnvtestManager(t, scheme)
	c := mgr.GetClient()

	watcher := NewResourceWatcher(t, ctx, mgr)
	watcher.SetCmpOpts(IgnoreMetaRuntimeFields(), IgnoreServiceRuntimeFields(),
		IgnoreStatefulSetRuntimeFields(), IgnoreDeploymentRuntimeFields())
	watcher.SetTimeout(600 * time.Millisecond)

	// Create resources while WaitForMatch is running
	go func() {
		time.Sleep(100 * time.Millisecond)

		// Create StatefulSet (different kind - will be skipped)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "test-sts", Namespace: "default"},
			Spec: appsv1.StatefulSetSpec{
				Replicas: ptr.To(int32(1)),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "nginx", Image: "nginx"}},
					},
				},
			},
		}
		c.Create(ctx, sts)

		time.Sleep(80 * time.Millisecond)

		// Create Service with wrong spec
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "test-svc-v", Namespace: "default"},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
		}
		c.Create(ctx, svc)

		// Update it multiple times (logs verbose diffs each time)
		for i := 0; i < 3; i++ {
			time.Sleep(80 * time.Millisecond)
			key := client.ObjectKey{Name: "test-svc-v", Namespace: "default"}
			if err := c.Get(ctx, key, svc); err == nil {
				svc.Spec.Ports[0].Port = int32(81 + i)
				c.Update(ctx, svc)
			}
		}
	}()

	// Wait for Service with spec that never matches
	expected := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test-svc-v", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Protocol: corev1.ProtocolTCP, Port: 9999, TargetPort: intstr.FromInt(9999)},
			},
			Type:            corev1.ServiceTypeClusterIP,
			SessionAffinity: corev1.ServiceAffinityNone,
		},
	}

	err := watcher.WaitForMatch(expected)
	assert.NewCollecting(t).Error(err, "Expected timeout")
}

// TestWatchResource_AlreadyWatched tests early return when kind already watched.
func TestWatchResource_AlreadyWatched(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := SetUpEnvtestManager(t, scheme)

	watcher := &ResourceWatcher{
		t:            t,
		watchedKinds: make(map[string]any),
	}

	watcher.watchedKinds["Service"] = nil

	watcher.watchResource(ctx, mgr, &corev1.Service{})

	// Already watched kind should not fail.
}

// TestWaitForEventType_NonMatchingEvents tests predicate returning false for non-matching events.
func TestWaitForEventType_NonMatchingEvents(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := t.Context()
	mgr := SetUpEnvtestManager(t, scheme)
	c := mgr.GetClient()

	watcher := NewResourceWatcher(t, ctx, mgr,
		WithTimeout(300*time.Millisecond),
	)

	// Create a Service (ADDED event)
	go func() {
		time.Sleep(100 * time.Millisecond)
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "test-svc-evt", Namespace: "default"},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
		}
		c.Create(ctx, svc)
	}()

	// Wait for DELETED event (will see ADDED but predicate returns false)
	_, err := watcher.WaitForEventType("Service", "DELETED")
	assert.NewCollecting(t).Error(err, "Expected timeout error")
}
