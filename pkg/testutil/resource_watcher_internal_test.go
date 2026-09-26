package testutil

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/assert"
)

// TestSetTimeout verifies SetTimeout updates the timeout field.
func TestSetTimeout(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		t:       t,
		timeout: 5 * time.Second,
	}

	newTimeout := 20 * time.Second
	watcher.SetTimeout(newTimeout)

	assert.NewCollecting(t).Eq(newTimeout, watcher.timeout, "SetTimeout() timeout")
}

// TestResetTimeout verifies ResetTimeout restores default (5 seconds).
func TestResetTimeout(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		t:       t,
		timeout: 20 * time.Second,
	}

	watcher.ResetTimeout()

	expectedDefault := 5 * time.Second
	assert.NewCollecting(t).Eq(expectedDefault, watcher.timeout, "ResetTimeout() timeout")
}

// TestSetCmpOpts verifies SetCmpOpts updates the cmpOpts field.
func TestSetCmpOpts(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		t:       t,
		cmpOpts: nil,
	}

	newOpts := []cmp.Option{IgnoreMetaRuntimeFields(), IgnoreStatus()}
	watcher.SetCmpOpts(newOpts...)

	assert.NewCollecting(t).
		Len(watcher.cmpOpts, 2, "SetCmpOpts() cmpOpts length = %d, want 2", len(watcher.cmpOpts))
}

// TestResetCmpOpts verifies ResetCmpOpts clears the cmpOpts field.
func TestResetCmpOpts(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		t:       t,
		cmpOpts: []cmp.Option{IgnoreStatus()},
	}

	watcher.ResetCmpOpts()

	assert.NewCollecting(t).Nil(watcher.cmpOpts, "ResetCmpOpts() cmpOpts")
}

// TestWithTimeout verifies WithTimeout option.
func TestWithTimeout(t *testing.T) {
	t.Parallel()

	customTimeout := 30 * time.Second
	watcher := &ResourceWatcher{
		t:       t,
		timeout: 5 * time.Second,
	}

	opt := WithTimeout(customTimeout)
	opt(watcher)

	assert.NewCollecting(t).Eq(customTimeout, watcher.timeout, "WithTimeout() set timeout")
}

// TestWithCmpOpts verifies WithCmpOpts option.
func TestWithCmpOpts(t *testing.T) {
	t.Parallel()

	opts := []cmp.Option{IgnoreMetaRuntimeFields(), IgnoreStatus()}
	watcher := &ResourceWatcher{
		t:       t,
		cmpOpts: nil,
	}

	option := WithCmpOpts(opts...)
	option(watcher)

	assert.NewCollecting(t).
		Len(watcher.cmpOpts, 2, "WithCmpOpts() set cmpOpts length = %d, want 2", len(watcher.cmpOpts))
}

// TestWithExtraResource verifies WithExtraResource option.
func TestWithExtraResource(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		extraResources: []client.Object{},
	}

	configMap := &corev1.ConfigMap{}
	secret := &corev1.Secret{}

	option := WithExtraResource(configMap, secret)
	option(watcher)

	assert.NewCollecting(t).
		Len(watcher.extraResources, 2, "WithExtraResource() set extraResources length = %d, want 2", len(watcher.extraResources))
}

// TestWithExtraResource_Duplicates verifies duplicate kinds are handled.
func TestWithExtraResource_Duplicates(t *testing.T) {
	t.Parallel()

	watcher := &ResourceWatcher{
		extraResources: []client.Object{},
	}

	cm1 := &corev1.ConfigMap{}
	cm2 := &corev1.ConfigMap{} // Same kind

	// Call twice with same kind
	opt1 := WithExtraResource(cm1)
	opt2 := WithExtraResource(cm2)

	opt1(watcher)
	opt2(watcher)

	// Both are added to extraResources slice
	assert.NewCollecting(t).
		Len(watcher.extraResources, 2, "WithExtraResource() called twice set extraResources length = %d, want 2", len(watcher.extraResources))

	// Note: watchResource() deduplicates by checking watchedKinds map,
	// so the second ConfigMap won't create duplicate event handlers.
}
