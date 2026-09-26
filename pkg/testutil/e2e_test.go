//go:build e2e

package testutil

import (
	"context"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	"github.com/multigres/testkit/assert"
)

// ---------------------------------------------------------------------------
// defaultE2EOpts
// ---------------------------------------------------------------------------

func TestDefaultE2EOpts(t *testing.T) {
	c := assert.NewCollecting(t)
	o := defaultE2EOpts()
	c.True(o.parallel, "parallel should be true by default")
	c.Eq(5*time.Minute, o.clusterWaitTime, "clusterWaitTime")
	c.Eq("", o.kindConfigPath, "kindConfigPath")
	c.Empty(o.images, "images")
	c.Empty(o.setupFuncs, "setupFuncs should be empty")
	c.Empty(o.finishFuncs, "finishFuncs should be empty")
}

// ---------------------------------------------------------------------------
// E2EOption functions
// ---------------------------------------------------------------------------

func TestWithSequential(t *testing.T) {
	o := defaultE2EOpts()
	WithSequential()(o)
	assert.NewCollecting(t).False(o.parallel, "parallel should be false after WithSequential")
}

func TestWithE2EKindConfig(t *testing.T) {
	o := defaultE2EOpts()
	WithE2EKindConfig("/path/to/config.yaml")(o)
	assert.NewCollecting(t).Eq("/path/to/config.yaml", o.kindConfigPath, "kindConfigPath")
}

func TestWithImage(t *testing.T) {
	c := assert.NewCollecting(t)
	o := defaultE2EOpts()
	WithImage("img1:latest")(o)
	WithImage("img2:v2")(o)

	c.Require().Len(o.images, 2, "len(images) = %d, want 2", len(o.images))
	c.Eq("img1:latest", o.images[0], "images[0]")
	c.Eq("img2:v2", o.images[1], "images[1]")
}

func TestWithSetup(t *testing.T) {
	o := defaultE2EOpts()
	WithSetup(func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
		return ctx, nil
	})(o)
	assert.NewCollecting(t).Len(o.setupFuncs, 1, "len(setupFuncs) = %d, want 1", len(o.setupFuncs))
}

func TestWithFinish(t *testing.T) {
	o := defaultE2EOpts()
	WithFinish(func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
		return ctx, nil
	})(o)
	assert.NewCollecting(t).
		Len(o.finishFuncs, 1, "len(finishFuncs) = %d, want 1", len(o.finishFuncs))
}

func TestWithClusterWait(t *testing.T) {
	o := defaultE2EOpts()
	WithClusterWait(10 * time.Minute)(o)
	assert.NewCollecting(t).Eq(10*time.Minute, o.clusterWaitTime, "clusterWaitTime")
}

// ---------------------------------------------------------------------------
// sanitizeE2EName
// ---------------------------------------------------------------------------

func TestSanitizeE2EName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"e2e-test-12345", "e2e-test-12345"},
		{"TestFoo/Bar", "testfoo-bar"},
		{"Test_Foo_Bar", "test-foo-bar"},
		{"Test Foo Bar", "test-foo-bar"},
		{"UPPER", "upper"},
		// Truncated to 50 chars
		{
			"a-very-long-name-that-exceeds-fifty-characters-and-should-be-truncated",
			"a-very-long-name-that-exceeds-fifty-characters-and",
		},
		// Trailing dashes stripped
		{"-leading-and-trailing-", "leading-and-trailing"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeE2EName(tt.input)
			assert.NewCollecting(t).
				Eq(tt.want, got, "sanitizeE2EName(%q) = %q, want", tt.input, got)
		})
	}
}

// ---------------------------------------------------------------------------
// isPodReady
// ---------------------------------------------------------------------------

func TestIsPodReady(t *testing.T) {
	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{
			name: "ready pod",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			},
			want: true,
		},
		{
			name: "not ready pod",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionFalse},
					},
				},
			},
			want: false,
		},
		{
			name: "no conditions",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{},
			},
			want: false,
		},
		{
			name: "other conditions only",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
						{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
					},
				},
			},
			want: false,
		},
		{
			name: "ready among multiple conditions",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
						{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
					},
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPodReady(tt.pod)
			assert.NewCollecting(t).Eq(tt.want, got, "isPodReady()")
		})
	}
}

// ---------------------------------------------------------------------------
// selectorFromDeployment
// ---------------------------------------------------------------------------

func TestSelectorFromDeployment(t *testing.T) {
	tests := []struct {
		name string
		dep  *appsv1.Deployment
		want string
	}{
		{
			name: "nil selector",
			dep:  &appsv1.Deployment{},
			want: "",
		},
		{
			name: "single label",
			dep: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "test"},
					},
				},
			},
			want: "app=test",
		},
		{
			name: "multiple labels",
			dep: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app":     "test",
							"version": "v1",
						},
					},
				},
			},
			want: "app=test,version=v1",
		},
		{
			name: "empty match labels",
			dep: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{},
					},
				},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectorFromDeployment(tt.dep)
			assert.NewCollecting(t).Eq(tt.want, got, "selectorFromDeployment()")
		})
	}
}

// ---------------------------------------------------------------------------
// int64Ptr
// ---------------------------------------------------------------------------

func TestInt64Ptr(t *testing.T) {
	c := assert.NewCollecting(t)
	p := int64Ptr(42)
	c.Eq(42, *p, "int64Ptr(42)")
	p2 := int64Ptr(0)
	c.Eq(0, *p2, "int64Ptr(0)")
}

// ---------------------------------------------------------------------------
// DefaultOperatorPreset
// ---------------------------------------------------------------------------

func TestDefaultOperatorPreset_FromEnv(t *testing.T) {
	t.Setenv("OPERATOR_IMG", "my-registry/my-operator:v1.2.3")
	c := assert.NewCollecting(t)

	p := DefaultOperatorPreset()
	c.Eq("my-registry/my-operator:v1.2.3", p.Image, "Image")
	c.Eq("multigres-operator", p.Namespace, "Namespace")
	c.Eq("multigres-operator-controller-manager", p.DeploymentName, "DeploymentName")
	c.Eq(3*time.Minute, p.ReadyTimeout, "ReadyTimeout")
}

func TestDefaultOperatorPreset_Fallback(t *testing.T) {
	t.Setenv("OPERATOR_IMG", "")

	p := DefaultOperatorPreset()
	assert.NewCollecting(t).Eq("ghcr.io/multigres/multigres-operator:dev", p.Image, "Image")
}

// ---------------------------------------------------------------------------
// MultigresImages
// ---------------------------------------------------------------------------

func TestMultigresImages(t *testing.T) {
	c := assert.NewCollecting(t)
	c.Len(MultigresImages, 4, "len(MultigresImages) = %d, want 4", len(MultigresImages))
	// Verify all entries are non-empty
	for i, img := range MultigresImages {
		c.NotEq("", img, "MultigresImages[%d] is empty", i)
	}
}

// ---------------------------------------------------------------------------
// TestCluster accessors
// ---------------------------------------------------------------------------

func TestTestClusterAccessors(t *testing.T) {
	tc := &TestCluster{
		clusterName: "test-cluster-123",
	}
	assert.NewCollecting(t).Eq("test-cluster-123", tc.ClusterName(), "ClusterName()")
}

// ---------------------------------------------------------------------------
// PortForwardResult.Stop
// ---------------------------------------------------------------------------

func TestPortForwardResultStop(t *testing.T) {
	stopCh := make(chan struct{})
	pf := &PortForwardResult{
		LocalPort: 12345,
		StopCh:    stopCh,
	}

	pf.Stop()

	// Verify channel is closed (receive returns immediately).
	select {
	case <-stopCh:
		// OK — closed
	default:
		t.Error("Stop() did not close StopCh")
	}
}

// ---------------------------------------------------------------------------
// maybeDestroyCluster (without a real cluster, we test the branching logic)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Option composition
// ---------------------------------------------------------------------------

func TestOptionsCompose(t *testing.T) {
	c := assert.NewCollecting(t)
	o := defaultE2EOpts()
	opts := []E2EOption{
		WithSequential(),
		WithImage("a:1"),
		WithImage("b:2"),
		WithE2EKindConfig("/kind.yaml"),
		WithClusterWait(7 * time.Minute),
	}
	for _, opt := range opts {
		opt(o)
	}

	c.False(o.parallel, "parallel should be false")
	c.Require().Len(o.images, 2, "len(images) = %d, want 2", len(o.images))
	c.Eq("/kind.yaml", o.kindConfigPath, "kindConfigPath =")
	c.Eq(7*time.Minute, o.clusterWaitTime, "clusterWaitTime =")
}

// ---------------------------------------------------------------------------
// TestCluster accessors — remaining ones
// ---------------------------------------------------------------------------

func TestTestCluster_NilAccessors(t *testing.T) {
	c := assert.NewCollecting(t)
	// Accessors should not panic even with nil fields — they just return nil.
	tc := &TestCluster{
		clusterName: "x",
	}
	c.Nil(tc.Env(), "Env() should be nil")
	c.Nil(tc.Config(), "Config() should be nil")
	c.Nil(tc.RESTConfig(), "RESTConfig() should be nil")
	c.Nil(tc.Clientset(), "Clientset() should be nil")
}

func TestTestCluster_WithConfig(t *testing.T) {
	c := assert.NewCollecting(t)
	cfg := envconf.New()
	testEnv := env.NewWithConfig(cfg)
	tc := &TestCluster{
		clusterName: "test",
		cfg:         cfg,
		env:         testEnv,
	}
	c.Eq(cfg, tc.Config(), "Config() should return the assigned config")
	c.Eq(cfg.KubeconfigFile(), tc.KubeconfigFile(), "KubeconfigFile()")
	c.NotNil(tc.Env(), "Env() should not be nil")
	// KlientClient() requires a real kubeconfig — tested via e2e tests only.
}

// ---------------------------------------------------------------------------
// runFinish — exercises the loop
// ---------------------------------------------------------------------------

func TestRunFinish(t *testing.T) {
	called := false
	tc := &TestCluster{
		t:   t,
		cfg: envconf.New(),
	}
	tc.runFinish([]env.Func{
		func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			called = true
			return ctx, nil
		},
	})
	assert.NewCollecting(t).True(called, "runFinish did not call the func")
}

func TestRunFinish_Empty(t *testing.T) {
	tc := &TestCluster{
		t:   t,
		cfg: envconf.New(),
	}
	// Should not panic with empty slice.
	tc.runFinish(nil)
}

// ---------------------------------------------------------------------------
// clusterCleanupAction — pure decision logic
// ---------------------------------------------------------------------------

func TestClusterCleanupAction(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		failed bool
		want   cleanupDecision
	}{
		// Default (empty/never) — always destroy
		{"default passing", "", false, cleanupDestroy},
		{"default failed", "", true, cleanupDestroyWithHint},
		{"never passing", "never", false, cleanupDestroy},
		{"never failed", "never", true, cleanupDestroyWithHint},

		// on-failure — keep only on failure
		{"on-failure passing", "on-failure", false, cleanupDestroy},
		{"on-failure failed", "on-failure", true, cleanupKeep},

		// always — always keep
		{"always passing", "always", false, cleanupKeep},
		{"always failed", "always", true, cleanupKeep},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clusterCleanupAction(tt.policy, tt.failed)
			assert.NewCollecting(t).
				Eq(tt.want, got, "clusterCleanupAction(%q, %v) = %d, want", tt.policy, tt.failed, got)
		})
	}
}

// ---------------------------------------------------------------------------
// maybeDestroyCluster — exercises the method with a real (fake) cluster
// ---------------------------------------------------------------------------

func TestMaybeDestroyCluster_Always(t *testing.T) {
	t.Setenv("E2E_KEEP_CLUSTERS", "always")

	tc := &TestCluster{
		t:           t,
		clusterName: "fake-cluster-always",
	}
	// Should not try to destroy — returns early with log.
	tc.maybeDestroyCluster()
}

func TestMaybeDestroyCluster_DefaultDestroy(t *testing.T) {
	t.Setenv("E2E_KEEP_CLUSTERS", "")

	tc := &TestCluster{
		t:           t,
		clusterName: "fake-cluster-default",
		cfg:         envconf.New(),
	}
	// Should try to destroy (will log but not crash since cluster doesn't exist).
	tc.maybeDestroyCluster()
}

func TestMaybeDestroyCluster_OnFailurePassingTest(t *testing.T) {
	t.Setenv("E2E_KEEP_CLUSTERS", "on-failure")

	tc := &TestCluster{
		t:           t,
		clusterName: "fake-cluster-onfailure",
		cfg:         envconf.New(),
	}
	// Test is passing, so it should destroy.
	tc.maybeDestroyCluster()
}

// Ensure unused imports are consumed.
var (
	_ = os.Getenv
	_ = ptr.To[int32]
)
