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
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/testutil"

	"github.com/multigres/testkit/assert"
)

// TestResourceWatcher_BeforeCreation tests that watcher can subscribe to events
// that haven't happened yet (watcher started before resource creation).
func TestResourceWatcher_BeforeCreation(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setup      func(ctx context.Context, c client.Client) error
		assertFunc func(t *testing.T, watcher *testutil.ResourceWatcher)
	}{
		"single service created": {
			setup: func(ctx context.Context, c client.Client) error {
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-svc",
						Namespace: "default",
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{{Port: 80}},
					},
				}
				return c.Create(ctx, svc)
			},
			assertFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) {
				// Expected object with Kubernetes defaults explicitly set
				expected := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-svc",
						Namespace: "default",
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{
								Protocol:   corev1.ProtocolTCP, // Kubernetes default
								Port:       80,
								TargetPort: intstr.FromInt(80), // Defaults to Port
							},
						},
						Type:            corev1.ServiceTypeClusterIP, // Kubernetes default
						SessionAffinity: corev1.ServiceAffinityNone,  // Kubernetes default
					},
				}

				// Configure watcher with comparison options
				opts := append(
					testutil.IgnoreMetaRuntimeFields(),
					testutil.IgnoreStatus(),
					testutil.IgnoreServiceRuntimeFields(),
				)
				watcher.SetCmpOpts(opts...)

				err := watcher.WaitForMatch(expected)
				assert.NewCollecting(t).NoError(err, "Failed to wait for Service")
			},
		},
		"statefulset created": {
			setup: func(ctx context.Context, c client.Client) error {
				sts := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-sts",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: ptr.To(int32(2)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": "test"},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "nginx",
										Image: "nginx:latest",
									},
								},
							},
						},
					},
				}
				return c.Create(ctx, sts)
			},
			assertFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) {
				expected := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-sts",
						Namespace: "default",
					},
					Spec: appsv1.StatefulSetSpec{
						Replicas: ptr.To(int32(2)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "test"},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": "test"},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "nginx",
										Image: "nginx:latest",
									},
								},
							},
						},
					},
				}

				opts := append(
					testutil.IgnoreMetaRuntimeFields(),
					testutil.IgnoreStatefulSetRuntimeFields(),
					testutil.IgnorePodSpecDefaults(),
					testutil.IgnoreStatefulSetSpecDefaults(),
				)
				watcher.SetCmpOpts(opts...)

				err := watcher.WaitForMatch(expected)
				assert.NewCollecting(t).NoError(err, "Failed to wait for StatefulSet")
			},
		},
		"deployment created": {
			setup: func(ctx context.Context, c client.Client) error {
				deploy := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-deploy",
						Namespace: "default",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "nginx"},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": "nginx"},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "nginx",
										Image: "nginx:1.25",
									},
								},
							},
						},
					},
				}
				return c.Create(ctx, deploy)
			},
			assertFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) {
				expected := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-deploy",
						Namespace: "default",
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "nginx"},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": "nginx"},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:            "nginx",
										Image:           "nginx:1.25",
										ImagePullPolicy: corev1.PullIfNotPresent, // Default for non-:latest tag
									},
								},
							},
						},
					},
				}

				opts := append(
					testutil.IgnoreMetaRuntimeFields(),
					testutil.IgnoreDeploymentRuntimeFields(),
					testutil.IgnorePodSpecDefaultsExceptPullPolicy(), // Keep ImagePullPolicy for verification
					testutil.IgnoreDeploymentSpecDefaults(),
				)
				watcher.SetCmpOpts(opts...)

				err := watcher.WaitForMatch(expected)
				assert.NewCollecting(t).NoError(err, "Failed to wait for Deployment")
			},
		},
		"multiple unwatched kinds fail immediately": {
			setup: func(ctx context.Context, c client.Client) error {
				// No setup needed - we're testing validation before waiting
				return nil
			},
			assertFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) {
				// Try to wait for ConfigMap and Secret which are not being watched
				err := watcher.WaitForMatch(&corev1.ConfigMap{}, &corev1.Secret{})
				if err == nil {
					t.Errorf("Expected error for unwatched kinds, but got nil")
					return
				}

				want := &testutil.ErrUnwatchedKinds{Kinds: []string{"ConfigMap", "Secret"}}
				var got *testutil.ErrUnwatchedKinds
				if !errors.As(err, &got) {
					t.Errorf("Expected ErrUnwatchedKinds, got: %T - %v", err, err)
					return
				}

				assert.NewCollecting(t).EqDiff(want.Kinds, got.Kinds, "Unwatched kinds mismatch")
			},
		},
	}

	for name, tc := range tests {
		name, tc := name, tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)

			ctx := t.Context()
			mgr := testutil.SetUpEnvtestManager(t, scheme)

			watcher := testutil.NewResourceWatcher(t, ctx, mgr)
			c := mgr.GetClient()

			// Start assertion in background FIRST
			done := make(chan error, 1)
			go func() {
				defer close(done)
				tc.assertFunc(t, watcher)
			}()

			// THEN create resources (tests subscription path)
			assert.NewAborting(t).NoError(tc.setup(ctx, c), "Setup failed")

			// Wait for assertion to complete
			<-done
		})
	}
}

// TestResourceWatcher_AfterCreation tests that watcher correctly handles resources
// that were created before the watcher started, and subsequent updates.
func TestResourceWatcher_AfterCreation(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setup      func(ctx context.Context, c client.Client) error
		update     func(ctx context.Context, c client.Client) error
		assertFunc func(t *testing.T, watcher *testutil.ResourceWatcher)
	}{
		"service updated after watcher starts": {
			setup: func(ctx context.Context, c client.Client) error {
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-svc",
						Namespace: "default",
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{{Port: 80}},
					},
				}
				return c.Create(ctx, svc)
			},
			update: func(ctx context.Context, c client.Client) error {
				svc := &corev1.Service{}
				if err := c.Get(ctx, client.ObjectKey{
					Name:      "test-svc",
					Namespace: "default",
				}, svc); err != nil {
					return err
				}

				svc.Spec.Ports[0].Port = 8080
				return c.Update(ctx, svc)
			},
			assertFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) {
				// Expected object with Kubernetes defaults and updated port
				expected := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-svc",
						Namespace: "default",
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{
								Protocol:   corev1.ProtocolTCP, // Kubernetes default
								Port:       8080,               // Updated value
								TargetPort: intstr.FromInt(80), // Original TargetPort preserved
							},
						},
						Type:            corev1.ServiceTypeClusterIP, // Kubernetes default
						SessionAffinity: corev1.ServiceAffinityNone,  // Kubernetes default
					},
				}

				opts := append(
					testutil.IgnoreMetaRuntimeFields(),
					testutil.IgnoreStatus(),
					testutil.IgnoreServiceRuntimeFields(),
				)
				watcher.SetCmpOpts(opts...)

				err := watcher.WaitForMatch(expected)
				assert.NewCollecting(t).NoError(err, "Failed to wait for updated Service")
			},
		},
		"service deleted after watcher starts": {
			setup: func(ctx context.Context, c client.Client) error {
				svc := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-svc-delete",
						Namespace: "default",
					},
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{{Port: 80}},
					},
				}
				return c.Create(ctx, svc)
			},
			update: func(ctx context.Context, c client.Client) error {
				svc := &corev1.Service{}
				if err := c.Get(ctx, client.ObjectKey{
					Name:      "test-svc-delete",
					Namespace: "default",
				}, svc); err != nil {
					return err
				}
				return c.Delete(ctx, svc)
			},
			assertFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) {
				// Wait specifically for DELETED event
				evt, err := watcher.WaitForEventType("Service", "DELETED")
				if err != nil {
					t.Errorf("Failed to wait for Service DELETED event: %v", err)
					return
				}

				assert.NewCollecting(t).
					Eq("test-svc-delete", evt.Name, "Expected test-svc-delete, got")

				t.Logf("Successfully detected Service deletion")
			},
		},
	}

	for name, tc := range tests {
		name, tc := name, tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ck := assert.NewAborting(t)

			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)

			ctx := context.Background()
			mgr := testutil.SetUpEnvtestManager(t, scheme)
			c := mgr.GetClient()

			// Create resources FIRST
			ck.NoError(tc.setup(ctx, c), "Setup failed")

			// THEN start watcher (won't see initial creation)
			watcher := testutil.NewResourceWatcher(t, ctx, mgr)

			// Start assertion in background
			done := make(chan error, 1)
			go func() {
				defer close(done)
				tc.assertFunc(t, watcher)
			}()

			// Trigger update (this should be picked up by watcher)
			ck.NoError(tc.update(ctx, c), "Update failed")

			// Wait for assertion to complete
			<-done
		})
	}
}

// TestResourceWatcherEventUtilities tests event inspection utility functions.
func TestResourceWatcherEventUtilities(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	ctx := context.Background()
	mgr := testutil.SetUpEnvtestManager(t, scheme)

	watcher := testutil.NewResourceWatcher(t, ctx, mgr,
		testutil.WithExtraResource(&corev1.ConfigMap{}),
	)

	c := mgr.GetClient()

	// Create multiple resources
	svc1 := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-1", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}
	svc2 := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-2", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "deploy-1", Namespace: "default"},
		Spec: appsv1.DeploymentSpec{
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

	ck.Require().NoError(c.Create(ctx, svc1), "Failed to create svc-1")
	ck.Require().NoError(c.Create(ctx, svc2), "Failed to create svc-2")
	ck.Require().NoError(c.Create(ctx, deploy), "Failed to create deploy-1")

	// Wait for all resources
	watcher.SetCmpOpts(
		testutil.IgnoreMetaRuntimeFields(),
		testutil.IgnoreServiceRuntimeFields(),
		testutil.IgnoreDeploymentRuntimeFields(),
		testutil.IgnoreDeploymentSpecDefaults(),
		testutil.IgnorePodSpecDefaults(),
	)
	ck.Require().NoError(watcher.WaitForMatch(svc1, svc2, deploy), "Failed to wait for resources")

	// Test Events() returns all collected events
	events := watcher.Events()
	ck.GreaterOrEqual(3, len(events), "Events()")

	// Verify Events() contains expected resources
	foundSvc1, foundSvc2, foundDeploy := false, false, false
	for _, evt := range events {
		if evt.Name == "svc-1" && evt.Kind == "Service" && evt.Type == "ADDED" {
			foundSvc1 = true
		}
		if evt.Name == "svc-2" && evt.Kind == "Service" && evt.Type == "ADDED" {
			foundSvc2 = true
		}
		if evt.Name == "deploy-1" && evt.Kind == "Deployment" && evt.Type == "ADDED" {
			foundDeploy = true
		}
	}
	ck.True(foundSvc1, "Events() should contain ADDED event for svc-1")
	ck.True(foundSvc2, "Events() should contain ADDED event for svc-2")
	ck.True(foundDeploy, "Events() should contain ADDED event for deploy-1")

	// Test ForKind() filters by resource kind
	svcEvents := watcher.ForKind("Service")
	ck.GreaterOrEqual(2, len(svcEvents), "ForKind(Service)")
	for _, evt := range svcEvents {
		ck.Eq("Service", evt.Kind, "ForKind(Service) returned event with Kind =")
	}

	deployEvents := watcher.ForKind("Deployment")
	ck.GreaterOrEqual(1, len(deployEvents), "ForKind(Deployment)")
	for _, evt := range deployEvents {
		ck.Eq("Deployment", evt.Kind, "ForKind(Deployment) returned event with Kind =")
	}

	// Test ForName() filters by resource name
	svc2Events := watcher.ForName("svc-2")
	ck.NotEmpty(svc2Events, "ForName(svc-2) returned no events")
	for _, evt := range svc2Events {
		ck.Eq("svc-2", evt.Name, "ForName(svc-2) returned event with Name =")
	}

	// Test Count() matches Events() length
	count := watcher.Count()
	ck.Eq(len(events), count, "Count()")

	// Test EventCh() provides direct access to event channel
	ch := watcher.EventCh()
	ck.Require().NotNil(ch, "EventCh() returned nil")

	// Verify EventCh returns same channel on multiple calls
	ch2 := watcher.EventCh()
	ck.Eq(ch2, ch, "EventCh() should return the same channel instance")

	// Create another resource and verify the event count increases
	// (can't consume from channel as that would interfere with collectEvents)
	countBefore := watcher.Count()

	svc3 := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-3", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80}}},
	}
	ck.Require().NoError(c.Create(ctx, svc3), "Failed to create svc-3")

	// Wait for event to be collected
	watcher.SetCmpOpts(testutil.IgnoreMetaRuntimeFields(), testutil.IgnoreServiceRuntimeFields())
	ck.Require().NoError(watcher.WaitForMatch(svc3), "Failed to wait for svc-3")

	// Verify event was collected through the channel
	countAfter := watcher.Count()
	ck.Greater(countBefore, countAfter, "Count after creating svc-3")

	// Verify the new event is in Events()
	foundSvc3 := false
	for _, evt := range watcher.Events() {
		if evt.Name == "svc-3" && evt.Kind == "Service" && evt.Type == "ADDED" {
			foundSvc3 = true
			break
		}
	}
	ck.True(foundSvc3, "Events() should contain ADDED event for svc-3 (received via EventCh)")
}

// TestErrUnwatchedKinds_Error tests the Error() method.
func TestErrUnwatchedKinds_Error(t *testing.T) {
	err := &testutil.ErrUnwatchedKinds{
		Kinds: []string{"ConfigMap", "Secret"},
	}

	got := err.Error()
	want := "the following kinds are not being watched by this ResourceWatcher: [ConfigMap Secret]"

	assert.NewCollecting(t).Eq(want, got, "Error()")
}

// TestResourceWatcher_ContextCancellation tests behavior when context is canceled.
func TestResourceWatcher_ContextCancellation(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := map[string]struct {
		testFunc func(t *testing.T, watcher *testutil.ResourceWatcher) error
	}{
		"WaitForMatch": {
			testFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) error {
				expected := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				}
				return watcher.WaitForMatch(expected)
			},
		},
		"WaitForDeletion": {
			testFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) error {
				return watcher.WaitForDeletion(testutil.Obj[corev1.Service]("test", "default"))
			},
		},
		"WaitForEventType": {
			testFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) error {
				_, err := watcher.WaitForEventType("Service", "DELETED")
				return err
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			mgr := testutil.SetUpEnvtestManager(t, scheme)
			watcher := testutil.NewResourceWatcher(t, ctx, mgr)

			cancel() // Cancel context to trigger error paths

			err := tc.testFunc(t, watcher)
			assert.NewCollecting(t).Error(err, "Function should error when context is canceled")
		})
	}
}

// TestResourceWatcher_Timeouts tests timeout scenarios with 1ms timeout.
func TestResourceWatcher_Timeouts(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	tests := map[string]struct {
		testFunc func(t *testing.T, watcher *testutil.ResourceWatcher) error
	}{
		"WaitForMatch timeout": {
			testFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) error {
				expected := &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Name: "nonexistent", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort},
				}
				return watcher.WaitForMatch(expected)
			},
		},
		"WaitForDeletion timeout": {
			testFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) error {
				return watcher.WaitForDeletion(
					testutil.Obj[corev1.Service]("nonexistent", "default"),
				)
			},
		},
		"WaitForEventType timeout": {
			testFunc: func(t *testing.T, watcher *testutil.ResourceWatcher) error {
				_, err := watcher.WaitForEventType("Service", "DELETED")
				return err
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			mgr := testutil.SetUpEnvtestManager(t, scheme)
			watcher := testutil.NewResourceWatcher(t, ctx, mgr,
				testutil.WithTimeout(1*time.Millisecond), // 1ms timeout
			)

			err := tc.testFunc(t, watcher)
			assert.NewCollecting(t).Error(err, "Function should timeout")
		})
	}
}
