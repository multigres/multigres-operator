package testutil

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
)

// EmptyResourcesShape is one non-zero way a resources block can ask for
// nothing, as the unstructured value written into a custom resource.
type EmptyResourcesShape struct {
	Name  string
	Shape map[string]any
}

// EmptyResourcesShapes are the shapes a resources block with entries can be
// edited down to while still being present. `{limits: {}}` is what kubectl edit
// leaves after the last limit is deleted.
var EmptyResourcesShapes = []EmptyResourcesShape{
	{"empty requests", map[string]any{"requests": map[string]any{}}},
	{"empty limits", map[string]any{"limits": map[string]any{}}},
	{"empty claims", map[string]any{"claims": []any{}}},
	{"all empty", map[string]any{
		"requests": map[string]any{},
		"limits":   map[string]any{},
		"claims":   []any{},
	}},
}

// EmptyResourcesShapesAndBlock is EmptyResourcesShapes plus the bare `{}`
// block, for resources whose bare block means no resources rather than
// inheriting a default.
func EmptyResourcesShapesAndBlock() []EmptyResourcesShape {
	return append(append([]EmptyResourcesShape{}, EmptyResourcesShapes...),
		EmptyResourcesShape{"empty block", map[string]any{}})
}

// PopulatedResources is the resources block the empty-shape tests start from
// and return to: 123m cpu requested, 321Mi memory limited.
func PopulatedResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("123m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("321Mi")},
	}
}

// IsPopulatedResources reports whether r carries the PopulatedResources entries.
func IsPopulatedResources(r corev1.ResourceRequirements) bool {
	return r.Requests.Cpu().String() == "123m" && r.Limits.Memory().String() == "321Mi"
}

// IsEmptyResources reports whether r has no requests, limits or claims.
func IsEmptyResources(r corev1.ResourceRequirements) bool {
	return len(r.Requests) == 0 && len(r.Limits) == 0 && len(r.Claims) == 0
}

// RoundTripEmptyResources drives each shape through a resource that starts
// with PopulatedResources: set writes the shape (or the populated block) into
// the parent object, and converged blocks until the children it follows carry
// entries (populated true) or none (populated false). The populated block is
// checked first, and again after each shape, so the trip back is covered.
func RoundTripEmptyResources(
	t *testing.T,
	what string,
	shapes []EmptyResourcesShape,
	set func(shape map[string]any),
	converged func(populated bool),
) {
	t.Helper()
	populated, err := runtime.DefaultUnstructuredConverter.ToUnstructured(
		new(PopulatedResources()),
	)
	if err != nil {
		t.Fatalf("converting the populated resources block: %v", err)
	}

	converged(true)
	for _, s := range shapes {
		set(s.Shape)
		t.Logf("shape %s: waiting for %s to drop its resources", s.Name, what)
		converged(false)

		set(populated)
		t.Logf("shape %s: waiting for %s to carry resources again", s.Name, what)
		converged(true)
	}
}
