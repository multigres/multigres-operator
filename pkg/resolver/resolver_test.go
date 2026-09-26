package resolver

import (
	"errors"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/multigres/testkit/assert"
)

// setupFixtures helper returns a fresh set of test objects.
func setupFixtures(t testing.TB) (
	*multigresv1alpha1.CoreTemplate,
	*multigresv1alpha1.CellTemplate,
	*multigresv1alpha1.ShardTemplate,
	string,
) {
	t.Helper()
	namespace := "default"

	coreTpl := &multigresv1alpha1.CoreTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace},
		Spec: multigresv1alpha1.CoreTemplateSpec{
			GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Image: "core-default"},
			},
			Multiadmin: &multigresv1alpha1.StatelessSpec{
				// Image is not in StatelessSpec, it is global
			},
			MultiadminWeb: &multigresv1alpha1.StatelessSpec{
				Replicas: ptr.To(int32(3)),
			},
		},
	}

	cellTpl := &multigresv1alpha1.CellTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace},
		Spec: multigresv1alpha1.CellTemplateSpec{
			Multigateway: &multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{
					Replicas: ptr.To(int32(3)),
				},
			},
			LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				Etcd: &multigresv1alpha1.EtcdSpec{Image: "local-etcd-default"},
			},
		},
	}

	shardTpl := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace},
		Spec: multigresv1alpha1.ShardTemplateSpec{
			Multiorch: &multigresv1alpha1.MultiorchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{
					Replicas: ptr.To(int32(3)),
				},
			},
		},
	}

	return coreTpl, cellTpl, shardTpl, namespace
}

func TestNewResolver(t *testing.T) {
	t.Parallel()

	c := fake.NewClientBuilder().Build()
	r := NewResolver(c, "ns")

	if got, want := r.Client, c; got != want {
		t.Errorf("Client mismatch: got %v, want %v", got, want)
	}
	got, want := r.Namespace, "ns"
	assert.NewCollecting(t).Eq(want, got, "Namespace mismatch: got")
}

// setupScheme creates a new scheme with all required types registered
func setupScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

// TestResolver_TemplateExists covers the helpers called by the Defaulter/Validator.
func TestResolver_TemplateExists(t *testing.T) {
	t.Parallel()
	scheme := setupScheme()

	coreTpl, cellTpl, shardTpl, ns := setupFixtures(t)
	objs := []client.Object{coreTpl, cellTpl, shardTpl}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	r := NewResolver(c, ns)

	// 1. CoreTemplateExists
	t.Run("CoreTemplateExists", func(t *testing.T) {
		// Found
		if exists, err := r.CoreTemplateExists(t.Context(), "default"); err != nil || !exists {
			t.Errorf("Expected found, got %v, %v", exists, err)
		}
		// Not Found
		if exists, err := r.CoreTemplateExists(t.Context(), "missing"); err != nil || exists {
			t.Errorf("Expected not found, got %v, %v", exists, err)
		}
		// Empty Name
		if exists, err := r.CoreTemplateExists(t.Context(), ""); err != nil || exists {
			t.Errorf("Expected false for empty name, got %v, %v", exists, err)
		}
	})

	// 2. CellTemplateExists
	t.Run("CellTemplateExists", func(t *testing.T) {
		if exists, err := r.CellTemplateExists(t.Context(), "default"); err != nil || !exists {
			t.Errorf("Expected found, got %v, %v", exists, err)
		}
		if exists, err := r.CellTemplateExists(t.Context(), "missing"); err != nil || exists {
			t.Errorf("Expected not found, got %v, %v", exists, err)
		}
		if exists, err := r.CellTemplateExists(t.Context(), ""); err != nil || exists {
			t.Errorf("Expected false for empty name, got %v, %v", exists, err)
		}
	})

	// 3. ShardTemplateExists
	t.Run("ShardTemplateExists", func(t *testing.T) {
		if exists, err := r.ShardTemplateExists(t.Context(), "default"); err != nil || !exists {
			t.Errorf("Expected found, got %v, %v", exists, err)
		}
		if exists, err := r.ShardTemplateExists(t.Context(), "missing"); err != nil || exists {
			t.Errorf("Expected not found, got %v, %v", exists, err)
		}
		if exists, err := r.ShardTemplateExists(t.Context(), ""); err != nil || exists {
			t.Errorf("Expected false for empty name, got %v, %v", exists, err)
		}
	})

	// 4. Error Case (Simulate DB failure)
	t.Run("ClientFailure", func(t *testing.T) {
		errSim := testutil.ErrInjected
		failClient := testutil.NewFakeClientWithFailures(c, &testutil.FailureConfig{
			OnGet: func(_ client.ObjectKey) error { return errSim },
		})
		rFail := NewResolver(failClient, ns)

		if _, err := rFail.CoreTemplateExists(t.Context(), "any"); err == nil ||
			!errors.Is(err, errSim) {
			t.Error("Expected error for CoreTemplateExists")
		}
		if _, err := rFail.CellTemplateExists(t.Context(), "any"); err == nil ||
			!errors.Is(err, errSim) {
			t.Error("Expected error for CellTemplateExists")
		}
		if _, err := rFail.ShardTemplateExists(t.Context(), "any"); err == nil ||
			!errors.Is(err, errSim) {
			t.Error("Expected error for ShardTemplateExists")
		}
	})
}

// TestResolver_ValidateReference checks Validat*TemplateReference logic (including fallback).
func TestResolver_ValidateReference(t *testing.T) {
	t.Parallel()
	scheme := setupScheme()

	// Fixtures have names like "default" (FallbackCoreTemplate)
	coreTpl, cellTpl, shardTpl, ns := setupFixtures(t)
	objs := []client.Object{coreTpl, cellTpl, shardTpl} // "default" exists here

	// Add "custom" templates to bypass fallback logic and hit "Exists" branch
	customCore := &multigresv1alpha1.CoreTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "custom", Namespace: ns},
	}
	customCell := &multigresv1alpha1.CellTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "custom", Namespace: ns},
	}
	customShard := &multigresv1alpha1.ShardTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "custom", Namespace: ns},
	}
	objs = append(objs, customCore, customCell, customShard)

	cWithDefaults := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	cEmpty := fake.NewClientBuilder().WithScheme(scheme).Build() // "default" is missing here

	// Case 1: Core Template
	t.Run("Core", func(t *testing.T) {
		c := assert.NewCollecting(t)
		r := NewResolver(cEmpty, ns)

		// Empty name -> Valid (no explicit reference)
		c.NoError(
			r.ValidateCoreTemplateReference(t.Context(), ""),
			"Empty name should be valid, got",
		)
		// Explicit "default" reference with missing template -> Invalid
		c.Error(
			r.ValidateCoreTemplateReference(t.Context(), FallbackCoreTemplate),
			"Explicit 'default' reference should error when template is missing",
		)
		// Random missing -> Invalid
		c.Error(
			r.ValidateCoreTemplateReference(t.Context(), "missing"),
			"Missing template should error",
		)

		// Real existence (Implicit Default)
		rExists := NewResolver(cWithDefaults, ns)
		c.NoError(
			rExists.ValidateCoreTemplateReference(t.Context(), "default"),
			"Existing template should be valid, got",
		)
		// Real existence (Explicit Custom) - Hits "exists" branch
		c.NoError(
			rExists.ValidateCoreTemplateReference(t.Context(), "custom"),
			"Custom template should be valid, got",
		)
	})

	// Case 2: Cell Template
	t.Run("Cell", func(t *testing.T) {
		c := assert.NewCollecting(t)
		r := NewResolver(cEmpty, ns)

		c.NoError(
			r.ValidateCellTemplateReference(t.Context(), ""),
			"Empty name should be valid, got",
		)
		// Explicit "default" reference with missing template -> Invalid
		c.Error(
			r.ValidateCellTemplateReference(t.Context(), FallbackCellTemplate),
			"Explicit 'default' reference should error when template is missing",
		)
		c.Error(
			r.ValidateCellTemplateReference(t.Context(), "missing"),
			"Missing template should error",
		)
		rExists := NewResolver(cWithDefaults, ns)
		c.NoError(
			rExists.ValidateCellTemplateReference(t.Context(), "default"),
			"Existing template should be valid, got",
		)
		c.NoError(
			rExists.ValidateCellTemplateReference(t.Context(), "custom"),
			"Custom template should be valid, got",
		)
	})

	// Case 3: Shard Template
	t.Run("Shard", func(t *testing.T) {
		c := assert.NewCollecting(t)
		r := NewResolver(cEmpty, ns)

		c.NoError(
			r.ValidateShardTemplateReference(t.Context(), ""),
			"Empty name should be valid, got",
		)
		// Explicit "default" reference with missing template -> Invalid
		c.Error(
			r.ValidateShardTemplateReference(t.Context(), FallbackShardTemplate),
			"Explicit 'default' reference should error when template is missing",
		)
		c.Error(
			r.ValidateShardTemplateReference(t.Context(), "missing"),
			"Missing template should error",
		)
		rExists := NewResolver(cWithDefaults, ns)
		c.NoError(
			rExists.ValidateShardTemplateReference(t.Context(), "default"),
			"Existing template should be valid, got",
		)
		c.NoError(
			rExists.ValidateShardTemplateReference(t.Context(), "custom"),
			"Custom template should be valid, got",
		)
	})

	// Case 4: Client Failure
	t.Run("ClientFailure", func(t *testing.T) {
		c := assert.NewCollecting(t)
		errSim := testutil.ErrInjected
		failClient := testutil.NewFakeClientWithFailures(
			fake.NewClientBuilder().Build(),
			&testutil.FailureConfig{
				OnGet: func(_ client.ObjectKey) error { return errSim },
			},
		)
		rFail := NewResolver(failClient, ns)

		// Should propagate error
		c.ErrorIs(
			rFail.ValidateCoreTemplateReference(t.Context(), "any"),
			errSim,
			"Expected error propagation for Core, got",
		)
		c.ErrorIs(
			rFail.ValidateCellTemplateReference(t.Context(), "any"),
			errSim,
			"Expected error propagation for Cell, got",
		)
		c.ErrorIs(
			rFail.ValidateShardTemplateReference(t.Context(), "any"),
			errSim,
			"Expected error propagation for Shard, got",
		)
	})
}

// TestResolver_Caching verifies that the resolver caches templates to prevent N+1 API calls.
func TestResolver_Caching(t *testing.T) {
	t.Parallel()
	scheme := setupScheme()

	coreTpl, cellTpl, shardTpl, ns := setupFixtures(t)
	objs := []client.Object{coreTpl, cellTpl, shardTpl}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	t.Run("CoreTemplate", func(t *testing.T) {
		c := assert.NewCollecting(t)
		// Use a counter to track Get calls
		var getCalls int
		clientWithCounter := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnGet: func(_ client.ObjectKey) error {
				getCalls++
				return nil
			},
		})

		r := NewResolver(clientWithCounter, ns)

		// First call - should hit the API
		tpl1, err := r.ResolveCoreTemplate(t.Context(), "default")
		c.Require().NoError(err, "First ResolveCoreTemplate failed")
		c.Require().NotNil(tpl1, "Expected non-nil template")
		c.Eq(1, getCalls, "Expected 1 Get call after first resolve, got")

		// Second call - should use cache
		tpl2, err := r.ResolveCoreTemplate(t.Context(), "default")
		c.Require().NoError(err, "Second ResolveCoreTemplate failed")
		c.Require().NotNil(tpl2, "Expected non-nil template")
		c.Eq(1, getCalls, "Expected 1 Get call after second resolve (cached), got")

		// Verify DeepCopy (forward): modifying first result shouldn't affect second
		if tpl1.Spec.GlobalTopoServer != nil && tpl1.Spec.GlobalTopoServer.Etcd != nil {
			tpl1.Spec.GlobalTopoServer.Etcd.Image = "modified"
			c.NotEq(
				"modified",
				tpl2.Spec.GlobalTopoServer.Etcd.Image,
				"DeepCopy failed - modifications leaked from tpl1 to tpl2",
			)
		}

		// Verify DeepCopy (reverse): modifying a cached result must not
		// corrupt subsequent resolves. This is the critical direction that
		// catches missing DeepCopy on the cache-hit return path.
		if tpl2.Spec.GlobalTopoServer != nil && tpl2.Spec.GlobalTopoServer.Etcd != nil {
			tpl2.Spec.GlobalTopoServer.Etcd.Image = "corrupted"
			tpl3, err := r.ResolveCoreTemplate(t.Context(), "default")
			c.Require().NoError(err, "Third ResolveCoreTemplate failed")
			c.NotEq(
				"corrupted",
				tpl3.Spec.GlobalTopoServer.Etcd.Image,
				"Cache corruption - mutating a cached result polluted subsequent resolves",
			)
		}
	})

	t.Run("CellTemplate", func(t *testing.T) {
		c := assert.NewCollecting(t)
		var getCalls int
		clientWithCounter := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnGet: func(_ client.ObjectKey) error {
				getCalls++
				return nil
			},
		})

		r := NewResolver(clientWithCounter, ns)

		// First call
		tpl1, err := r.ResolveCellTemplate(t.Context(), "default")
		c.Require().NoError(err, "First ResolveCellTemplate failed")
		c.Require().NotNil(tpl1, "Expected non-nil template")
		c.Eq(1, getCalls, "Expected 1 Get call after first resolve, got")

		// Second call - should use cache
		tpl2, err := r.ResolveCellTemplate(t.Context(), "default")
		c.Require().NoError(err, "Second ResolveCellTemplate failed")
		c.Require().NotNil(tpl2, "Expected non-nil template")
		c.Eq(1, getCalls, "Expected 1 Get call after second resolve (cached), got")

		// Verify DeepCopy (forward)
		if tpl1.Spec.Multigateway != nil {
			tpl1.Spec.Multigateway.Replicas = ptr.To(int32(999))
			c.False(
				tpl2.Spec.Multigateway.Replicas != nil && *tpl2.Spec.Multigateway.Replicas == 999,
				"DeepCopy failed - modifications leaked from tpl1 to tpl2",
			)
		}

		// Verify DeepCopy (reverse): mutating a cached result must not
		// corrupt subsequent resolves.
		if tpl2.Spec.Multigateway != nil {
			tpl2.Spec.Multigateway.Replicas = ptr.To(int32(777))
			tpl3, err := r.ResolveCellTemplate(t.Context(), "default")
			c.Require().NoError(err, "Third ResolveCellTemplate failed")
			c.False(
				tpl3.Spec.Multigateway.Replicas != nil && *tpl3.Spec.Multigateway.Replicas == 777,
				"Cache corruption - mutating a cached result polluted subsequent resolves",
			)
		}
	})

	t.Run("ShardTemplate", func(t *testing.T) {
		c := assert.NewCollecting(t)
		var getCalls int
		clientWithCounter := testutil.NewFakeClientWithFailures(baseClient, &testutil.FailureConfig{
			OnGet: func(_ client.ObjectKey) error {
				getCalls++
				return nil
			},
		})

		r := NewResolver(clientWithCounter, ns)

		// First call
		tpl1, err := r.ResolveShardTemplate(t.Context(), "default")
		c.Require().NoError(err, "First ResolveShardTemplate failed")
		c.Require().NotNil(tpl1, "Expected non-nil template")
		c.Eq(1, getCalls, "Expected 1 Get call after first resolve, got")

		// Second call - should use cache
		tpl2, err := r.ResolveShardTemplate(t.Context(), "default")
		c.Require().NoError(err, "Second ResolveShardTemplate failed")
		c.Require().NotNil(tpl2, "Expected non-nil template")
		c.Eq(1, getCalls, "Expected 1 Get call after second resolve (cached), got")

		// Verify DeepCopy (forward)
		if tpl1.Spec.Multiorch != nil {
			tpl1.Spec.Multiorch.Replicas = ptr.To(int32(999))
			c.False(
				tpl2.Spec.Multiorch.Replicas != nil && *tpl2.Spec.Multiorch.Replicas == 999,
				"DeepCopy failed - modifications leaked from tpl1 to tpl2",
			)
		}

		// Verify DeepCopy (reverse): mutating a cached result must not
		// corrupt subsequent resolves.
		if tpl2.Spec.Multiorch != nil {
			tpl2.Spec.Multiorch.Replicas = ptr.To(int32(777))
			tpl3, err := r.ResolveShardTemplate(t.Context(), "default")
			c.Require().NoError(err, "Third ResolveShardTemplate failed")
			c.False(
				tpl3.Spec.Multiorch.Replicas != nil && *tpl3.Spec.Multiorch.Replicas == 777,
				"Cache corruption - mutating a cached result polluted subsequent resolves",
			)
		}
	})

	t.Run("FallbackNotCached", func(t *testing.T) {
		c := assert.NewCollecting(t)
		// Verify that fallback empty templates are NOT cached
		var getCalls int
		emptyClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		clientWithCounter := testutil.NewFakeClientWithFailures(
			emptyClient,
			&testutil.FailureConfig{
				OnGet: func(_ client.ObjectKey) error {
					getCalls++
					return nil
				},
			},
		)

		r := NewResolver(clientWithCounter, ns)

		// Call with empty name (triggers fallback)
		_, err := r.ResolveShardTemplate(t.Context(), "")
		c.Require().NoError(err, "Fallback resolve failed")

		// Should have attempted Get once and got NotFound
		c.Eq(1, getCalls, "Expected 1 Get call for fallback, got")

		// Second call - fallback should NOT be cached, so another Get attempt
		_, err = r.ResolveShardTemplate(t.Context(), "")
		c.Require().NoError(err, "Second fallback resolve failed")

		// Since fallback is not cached, we expect another Get call
		c.Eq(2, getCalls, "Expected 2 Get calls (fallback not cached), got")
	})
}

func TestSharedHelpers(t *testing.T) {
	t.Parallel()

	t.Run("isResourcesZero", func(t *testing.T) {
		tests := []struct {
			name string
			res  corev1.ResourceRequirements
			want bool
		}{
			{"Zero", corev1.ResourceRequirements{}, true},
			{"Requests Set", corev1.ResourceRequirements{Requests: corev1.ResourceList{}}, false},
			{"Limits Set", corev1.ResourceRequirements{Limits: corev1.ResourceList{}}, false},
			{"Claims Set", corev1.ResourceRequirements{Claims: []corev1.ResourceClaim{}}, false},
		}
		for _, tc := range tests {
			got := isResourcesZero(tc.res)
			assert.NewCollecting(t).Eq(tc.want, got, "%s: got %v, want", tc.name, got)
		}
	})

	t.Run("defaultEtcdSpec", func(t *testing.T) {
		c := assert.NewCollecting(t)
		spec := &multigresv1alpha1.EtcdSpec{}
		defaultEtcdSpec(spec, "/test/global")

		c.Eq(DefaultEtcdImage, spec.Image, "Image: got")
		c.Eq(DefaultEtcdStorageSize, spec.Storage.Size, "Storage: got")
		c.Eq(DefaultEtcdReplicas, *spec.Replicas, "Replicas: got")
		c.False(isResourcesZero(spec.Resources), "Resources should be defaulted")

		// Test Preservation
		spec2 := &multigresv1alpha1.EtcdSpec{Image: "custom"}
		defaultEtcdSpec(spec2, "/test/global")
		c.Eq("custom", spec2.Image, "Should preserve existing image")
	})

	t.Run("defaultStatelessSpec", func(t *testing.T) {
		c := assert.NewCollecting(t)
		spec := &multigresv1alpha1.StatelessSpec{}
		res := corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("1")},
		}
		defaultStatelessSpec(spec, res, 5)

		c.Eq(5, *spec.Replicas, "Replicas: got")
		c.EqDiff(spec.Resources, res, "Resources not copied correctly")

		// Test DeepCopy independence
		res.Requests[corev1.ResourceCPU] = parseQty("999")
		// Cannot call String() directly on map index result in one line effectively if we want addressability
		// But String() is on *Quantity usually? Actually Quantity is a struct.
		// Wait, Requests[...] returns a Value.
		// We need to capture it to check it.
		val := spec.Resources.Requests[corev1.ResourceCPU]
		c.NotEq("999", val.String(), "Shared pointer detected in defaultStatelessSpec")

		// Test Preservation
		spec2 := &multigresv1alpha1.StatelessSpec{
			Replicas: ptr.To(int32(10)),
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("5m")},
			},
		}
		defaultStatelessSpec(spec2, res, 5)
		c.Eq(10, *spec2.Replicas, "Should preserve existing Replicas")
		c.Eq("5m", spec2.Resources.Requests.Cpu().String(), "Should preserve existing Resources")
	})

	t.Run("mergeStatelessSpec", func(t *testing.T) {
		c := assert.NewCollecting(t)
		base := &multigresv1alpha1.StatelessSpec{
			PodAnnotations: map[string]string{"a": "1"},
			PodLabels:      map[string]string{"l1": "v1"},
		}
		override := &multigresv1alpha1.StatelessSpec{
			PodAnnotations: map[string]string{"b": "2"},
			PodLabels:      map[string]string{"l2": "v2"},
			Replicas:       ptr.To(int32(3)),
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("100m")},
			},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{},
			},
		}
		mergeStatelessSpec(base, override)

		c.Len(base.PodAnnotations, 2, "Map merge failed (Annotations), got")
		c.Len(base.PodLabels, 2, "Map merge failed (Labels), got")
		c.Eq(3, *base.Replicas, "Replicas not merged")
		c.NotNil(base.Resources.Requests, "Resources not merged")
		c.NotNil(base.Affinity, "Affinity not merged")
	})

	t.Run("mergePodPlacementSpec", func(t *testing.T) {
		c := assert.NewCollecting(t)
		override := &multigresv1alpha1.PodPlacementSpec{
			Tolerations: []corev1.Toleration{
				{
					Key:      "workload",
					Operator: corev1.TolerationOpEqual,
					Value:    "customer-pg",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
		}

		var base *multigresv1alpha1.PodPlacementSpec
		mergePodPlacementSpec(&base, override)

		c.Require().NotNil(base, "Placement not initialized")
		c.Require().Len(base.Tolerations, 1, "Tolerations not merged, got")

		override.Tolerations[0].Value = "changed"
		c.Eq(
			"customer-pg",
			base.Tolerations[0].Value,
			"Tolerations should be deep-copied during merge",
		)
	})

	t.Run("mergePodPlacementSpec clears inherited tolerations", func(t *testing.T) {
		c := assert.NewAborting(t)
		base := &multigresv1alpha1.PodPlacementSpec{
			Tolerations: []corev1.Toleration{
				{
					Key:      "workload",
					Operator: corev1.TolerationOpEqual,
					Value:    "customer-pg",
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
		}

		override := &multigresv1alpha1.PodPlacementSpec{}
		mergePodPlacementSpec(&base, override)

		c.NotNil(base, "Placement should remain initialized")
		c.Empty(base.Tolerations, "Expected inherited tolerations to be cleared, got")
	})
}

func parseQty(s string) resource.Quantity {
	return resource.MustParse(s)
}
