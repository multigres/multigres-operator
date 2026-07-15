package resolver

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
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
			Multigateway: &multigresv1alpha1.StatelessSpec{
				Replicas: ptr.To(int32(3)),
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
	if got, want := r.Namespace, "ns"; got != want {
		t.Errorf("Namespace mismatch: got %q, want %q", got, want)
	}
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
		r := NewResolver(cEmpty, ns)

		// Empty name -> Valid (no explicit reference)
		if err := r.ValidateCoreTemplateReference(t.Context(), ""); err != nil {
			t.Errorf("Empty name should be valid, got %v", err)
		}
		// Explicit "default" reference with missing template -> Invalid
		if err := r.ValidateCoreTemplateReference(t.Context(), FallbackCoreTemplate); err == nil {
			t.Error("Explicit 'default' reference should error when template is missing")
		}
		// Random missing -> Invalid
		if err := r.ValidateCoreTemplateReference(t.Context(), "missing"); err == nil {
			t.Error("Missing template should error")
		}

		// Real existence (Implicit Default)
		rExists := NewResolver(cWithDefaults, ns)
		if err := rExists.ValidateCoreTemplateReference(t.Context(), "default"); err != nil {
			t.Errorf("Existing template should be valid, got %v", err)
		}
		// Real existence (Explicit Custom) - Hits "exists" branch
		if err := rExists.ValidateCoreTemplateReference(t.Context(), "custom"); err != nil {
			t.Errorf("Custom template should be valid, got %v", err)
		}
	})

	// Case 2: Cell Template
	t.Run("Cell", func(t *testing.T) {
		r := NewResolver(cEmpty, ns)

		if err := r.ValidateCellTemplateReference(t.Context(), ""); err != nil {
			t.Errorf("Empty name should be valid, got %v", err)
		}
		// Explicit "default" reference with missing template -> Invalid
		if err := r.ValidateCellTemplateReference(t.Context(), FallbackCellTemplate); err == nil {
			t.Error("Explicit 'default' reference should error when template is missing")
		}
		if err := r.ValidateCellTemplateReference(t.Context(), "missing"); err == nil {
			t.Error("Missing template should error")
		}
		rExists := NewResolver(cWithDefaults, ns)
		if err := rExists.ValidateCellTemplateReference(t.Context(), "default"); err != nil {
			t.Errorf("Existing template should be valid, got %v", err)
		}
		if err := rExists.ValidateCellTemplateReference(t.Context(), "custom"); err != nil {
			t.Errorf("Custom template should be valid, got %v", err)
		}
	})

	// Case 3: Shard Template
	t.Run("Shard", func(t *testing.T) {
		r := NewResolver(cEmpty, ns)

		if err := r.ValidateShardTemplateReference(t.Context(), ""); err != nil {
			t.Errorf("Empty name should be valid, got %v", err)
		}
		// Explicit "default" reference with missing template -> Invalid
		if err := r.ValidateShardTemplateReference(t.Context(), FallbackShardTemplate); err == nil {
			t.Error("Explicit 'default' reference should error when template is missing")
		}
		if err := r.ValidateShardTemplateReference(t.Context(), "missing"); err == nil {
			t.Error("Missing template should error")
		}
		rExists := NewResolver(cWithDefaults, ns)
		if err := rExists.ValidateShardTemplateReference(t.Context(), "default"); err != nil {
			t.Errorf("Existing template should be valid, got %v", err)
		}
		if err := rExists.ValidateShardTemplateReference(t.Context(), "custom"); err != nil {
			t.Errorf("Custom template should be valid, got %v", err)
		}
	})

	// Case 4: Client Failure
	t.Run("ClientFailure", func(t *testing.T) {
		errSim := testutil.ErrInjected
		failClient := testutil.NewFakeClientWithFailures(
			fake.NewClientBuilder().Build(),
			&testutil.FailureConfig{
				OnGet: func(_ client.ObjectKey) error { return errSim },
			},
		)
		rFail := NewResolver(failClient, ns)

		// Should propagate error
		if err := rFail.ValidateCoreTemplateReference(t.Context(), "any"); !errors.Is(err, errSim) {
			t.Errorf("Expected error propagation for Core, got %v", err)
		}
		if err := rFail.ValidateCellTemplateReference(t.Context(), "any"); !errors.Is(err, errSim) {
			t.Errorf("Expected error propagation for Cell, got %v", err)
		}
		if err := rFail.ValidateShardTemplateReference(t.Context(), "any"); !errors.Is(
			err,
			errSim,
		) {
			t.Errorf("Expected error propagation for Shard, got %v", err)
		}
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
		if err != nil {
			t.Fatalf("First ResolveCoreTemplate failed: %v", err)
		}
		if tpl1 == nil {
			t.Fatal("Expected non-nil template")
		}
		if getCalls != 1 {
			t.Errorf("Expected 1 Get call after first resolve, got %d", getCalls)
		}

		// Second call - should use cache
		tpl2, err := r.ResolveCoreTemplate(t.Context(), "default")
		if err != nil {
			t.Fatalf("Second ResolveCoreTemplate failed: %v", err)
		}
		if tpl2 == nil {
			t.Fatal("Expected non-nil template")
		}
		if getCalls != 1 {
			t.Errorf("Expected 1 Get call after second resolve (cached), got %d", getCalls)
		}

		// Verify DeepCopy (forward): modifying first result shouldn't affect second
		if tpl1.Spec.GlobalTopoServer != nil && tpl1.Spec.GlobalTopoServer.Etcd != nil {
			tpl1.Spec.GlobalTopoServer.Etcd.Image = "modified"
			if tpl2.Spec.GlobalTopoServer.Etcd.Image == "modified" {
				t.Error("DeepCopy failed - modifications leaked from tpl1 to tpl2")
			}
		}

		// Verify DeepCopy (reverse): modifying a cached result must not
		// corrupt subsequent resolves. This is the critical direction that
		// catches missing DeepCopy on the cache-hit return path.
		if tpl2.Spec.GlobalTopoServer != nil && tpl2.Spec.GlobalTopoServer.Etcd != nil {
			tpl2.Spec.GlobalTopoServer.Etcd.Image = "corrupted"
			tpl3, err := r.ResolveCoreTemplate(t.Context(), "default")
			if err != nil {
				t.Fatalf("Third ResolveCoreTemplate failed: %v", err)
			}
			if tpl3.Spec.GlobalTopoServer.Etcd.Image == "corrupted" {
				t.Error("Cache corruption - mutating a cached result polluted subsequent resolves")
			}
		}
	})

	t.Run("CellTemplate", func(t *testing.T) {
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
		if err != nil {
			t.Fatalf("First ResolveCellTemplate failed: %v", err)
		}
		if tpl1 == nil {
			t.Fatal("Expected non-nil template")
		}
		if getCalls != 1 {
			t.Errorf("Expected 1 Get call after first resolve, got %d", getCalls)
		}

		// Second call - should use cache
		tpl2, err := r.ResolveCellTemplate(t.Context(), "default")
		if err != nil {
			t.Fatalf("Second ResolveCellTemplate failed: %v", err)
		}
		if tpl2 == nil {
			t.Fatal("Expected non-nil template")
		}
		if getCalls != 1 {
			t.Errorf("Expected 1 Get call after second resolve (cached), got %d", getCalls)
		}

		// Verify DeepCopy (forward)
		if tpl1.Spec.Multigateway != nil {
			tpl1.Spec.Multigateway.Replicas = ptr.To(int32(999))
			if tpl2.Spec.Multigateway.Replicas != nil && *tpl2.Spec.Multigateway.Replicas == 999 {
				t.Error("DeepCopy failed - modifications leaked from tpl1 to tpl2")
			}
		}

		// Verify DeepCopy (reverse): mutating a cached result must not
		// corrupt subsequent resolves.
		if tpl2.Spec.Multigateway != nil {
			tpl2.Spec.Multigateway.Replicas = ptr.To(int32(777))
			tpl3, err := r.ResolveCellTemplate(t.Context(), "default")
			if err != nil {
				t.Fatalf("Third ResolveCellTemplate failed: %v", err)
			}
			if tpl3.Spec.Multigateway.Replicas != nil && *tpl3.Spec.Multigateway.Replicas == 777 {
				t.Error("Cache corruption - mutating a cached result polluted subsequent resolves")
			}
		}
	})

	t.Run("ShardTemplate", func(t *testing.T) {
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
		if err != nil {
			t.Fatalf("First ResolveShardTemplate failed: %v", err)
		}
		if tpl1 == nil {
			t.Fatal("Expected non-nil template")
		}
		if getCalls != 1 {
			t.Errorf("Expected 1 Get call after first resolve, got %d", getCalls)
		}

		// Second call - should use cache
		tpl2, err := r.ResolveShardTemplate(t.Context(), "default")
		if err != nil {
			t.Fatalf("Second ResolveShardTemplate failed: %v", err)
		}
		if tpl2 == nil {
			t.Fatal("Expected non-nil template")
		}
		if getCalls != 1 {
			t.Errorf("Expected 1 Get call after second resolve (cached), got %d", getCalls)
		}

		// Verify DeepCopy (forward)
		if tpl1.Spec.Multiorch != nil {
			tpl1.Spec.Multiorch.Replicas = ptr.To(int32(999))
			if tpl2.Spec.Multiorch.Replicas != nil && *tpl2.Spec.Multiorch.Replicas == 999 {
				t.Error("DeepCopy failed - modifications leaked from tpl1 to tpl2")
			}
		}

		// Verify DeepCopy (reverse): mutating a cached result must not
		// corrupt subsequent resolves.
		if tpl2.Spec.Multiorch != nil {
			tpl2.Spec.Multiorch.Replicas = ptr.To(int32(777))
			tpl3, err := r.ResolveShardTemplate(t.Context(), "default")
			if err != nil {
				t.Fatalf("Third ResolveShardTemplate failed: %v", err)
			}
			if tpl3.Spec.Multiorch.Replicas != nil && *tpl3.Spec.Multiorch.Replicas == 777 {
				t.Error("Cache corruption - mutating a cached result polluted subsequent resolves")
			}
		}
	})

	t.Run("FallbackNotCached", func(t *testing.T) {
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
		if err != nil {
			t.Fatalf("Fallback resolve failed: %v", err)
		}

		// Should have attempted Get once and got NotFound
		if getCalls != 1 {
			t.Errorf("Expected 1 Get call for fallback, got %d", getCalls)
		}

		// Second call - fallback should NOT be cached, so another Get attempt
		_, err = r.ResolveShardTemplate(t.Context(), "")
		if err != nil {
			t.Fatalf("Second fallback resolve failed: %v", err)
		}

		// Since fallback is not cached, we expect another Get call
		if getCalls != 2 {
			t.Errorf("Expected 2 Get calls (fallback not cached), got %d", getCalls)
		}
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
			if got := isResourcesZero(tc.res); got != tc.want {
				t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		}
	})

	t.Run("defaultEtcdSpec", func(t *testing.T) {
		spec := &multigresv1alpha1.EtcdSpec{}
		defaultEtcdSpec(spec, DefaultTopoRootPath)

		if spec.Image != DefaultEtcdImage {
			t.Errorf("Image: got %q, want %q", spec.Image, DefaultEtcdImage)
		}
		if spec.Storage.Size != DefaultEtcdStorageSize {
			t.Errorf("Storage: got %q, want %q", spec.Storage.Size, DefaultEtcdStorageSize)
		}
		if *spec.Replicas != DefaultEtcdReplicas {
			t.Errorf("Replicas: got %d, want %d", *spec.Replicas, DefaultEtcdReplicas)
		}
		if isResourcesZero(spec.Resources) {
			t.Error("Resources should be defaulted")
		}

		// Test Preservation
		spec2 := &multigresv1alpha1.EtcdSpec{Image: "custom"}
		defaultEtcdSpec(spec2, DefaultTopoRootPath)
		if spec2.Image != "custom" {
			t.Error("Should preserve existing image")
		}
	})

	t.Run("defaultStatelessSpec", func(t *testing.T) {
		spec := &multigresv1alpha1.StatelessSpec{}
		res := corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("1")},
		}
		defaultStatelessSpec(spec, res, 5)

		if *spec.Replicas != 5 {
			t.Errorf("Replicas: got %d, want 5", *spec.Replicas)
		}
		if cmp.Diff(spec.Resources, res) != "" {
			t.Error("Resources not copied correctly")
		}

		// Test DeepCopy independence
		res.Requests[corev1.ResourceCPU] = parseQty("999")
		// Cannot call String() directly on map index result in one line effectively if we want addressability
		// But String() is on *Quantity usually? Actually Quantity is a struct.
		// Wait, Requests[...] returns a Value.
		// We need to capture it to check it.
		val := spec.Resources.Requests[corev1.ResourceCPU]
		if val.String() == "999" {
			t.Error("Shared pointer detected in defaultStatelessSpec")
		}

		// Test Preservation
		spec2 := &multigresv1alpha1.StatelessSpec{
			Replicas: ptr.To(int32(10)),
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: parseQty("5m")},
			},
		}
		defaultStatelessSpec(spec2, res, 5)
		if *spec2.Replicas != 10 {
			t.Error("Should preserve existing Replicas")
		}
		if spec2.Resources.Requests.Cpu().String() != "5m" {
			t.Error("Should preserve existing Resources")
		}
	})

	t.Run("mergeStatelessSpec", func(t *testing.T) {
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

		if len(base.PodAnnotations) != 2 {
			t.Errorf("Map merge failed (Annotations), got %v", base.PodAnnotations)
		}
		if len(base.PodLabels) != 2 {
			t.Errorf("Map merge failed (Labels), got %v", base.PodLabels)
		}
		if *base.Replicas != 3 {
			t.Error("Replicas not merged")
		}
		if base.Resources.Requests == nil {
			t.Error("Resources not merged")
		}
		if base.Affinity == nil {
			t.Error("Affinity not merged")
		}
	})

	t.Run("mergePodPlacementSpec", func(t *testing.T) {
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

		if base == nil {
			t.Fatal("Placement not initialized")
		}
		if len(base.Tolerations) != 1 {
			t.Fatalf("Tolerations not merged, got %v", base.Tolerations)
		}

		override.Tolerations[0].Value = "changed"
		if base.Tolerations[0].Value != "customer-pg" {
			t.Error("Tolerations should be deep-copied during merge")
		}
	})

	t.Run("mergePodPlacementSpec clears inherited tolerations", func(t *testing.T) {
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

		if base == nil {
			t.Fatal("Placement should remain initialized")
		}
		if len(base.Tolerations) != 0 {
			t.Fatalf("Expected inherited tolerations to be cleared, got %v", base.Tolerations)
		}
	})
}

func parseQty(s string) resource.Quantity {
	return resource.MustParse(s)
}
