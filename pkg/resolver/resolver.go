package resolver

import (
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// Resolver handles the logic for fetching templates and calculating defaults.
// It serves as the single source of truth for defaulting logic across the operator.
type Resolver struct {
	Client             client.Client
	Namespace          string
	CoreTemplateCache  map[string]*multigresv1alpha1.CoreTemplate
	CellTemplateCache  map[string]*multigresv1alpha1.CellTemplate
	ShardTemplateCache map[string]*multigresv1alpha1.ShardTemplate
}

// NewResolver creates a new defaults.Resolver.
func NewResolver(
	c client.Client,
	namespace string,
) *Resolver {
	return &Resolver{
		Client:             c,
		Namespace:          namespace,
		CoreTemplateCache:  make(map[string]*multigresv1alpha1.CoreTemplate),
		CellTemplateCache:  make(map[string]*multigresv1alpha1.CellTemplate),
		ShardTemplateCache: make(map[string]*multigresv1alpha1.ShardTemplate),
	}
}

// ============================================================================
// Shared Merge Helpers
// ============================================================================

func mergeStatelessSpec(
	base *multigresv1alpha1.StatelessSpec,
	override *multigresv1alpha1.StatelessSpec,
) {
	if override.Replicas != nil {
		base.Replicas = override.Replicas
	}

	// Safety: Use DeepCopy to ensure we don't share mutable map references (Requests/Limits)
	if !isResourcesZero(override.Resources) {
		base.Resources = *override.Resources.DeepCopy()
	}

	// Safety: DeepCopy Affinity to avoid sharing pointers
	if override.Affinity != nil {
		base.Affinity = override.Affinity.DeepCopy()
	}

	for k, v := range override.PodAnnotations {
		if base.PodAnnotations == nil {
			base.PodAnnotations = make(map[string]string)
		}
		base.PodAnnotations[k] = v
	}
	for k, v := range override.PodLabels {
		if base.PodLabels == nil {
			base.PodLabels = make(map[string]string)
		}
		base.PodLabels[k] = v
	}
}

func mergePodPlacementSpec(
	base **multigresv1alpha1.PodPlacementSpec,
	override *multigresv1alpha1.PodPlacementSpec,
) {
	if override == nil {
		return
	}
	*base = override.DeepCopy()
}

func mergeTopoServerPlacementSpec(
	base **multigresv1alpha1.TopoServerPlacementSpec,
	override *multigresv1alpha1.TopoServerPlacementSpec,
) {
	if override == nil {
		return
	}
	*base = override.DeepCopy()
}

// isResourcesZero checks if the resource requirements are strictly the zero value (nil maps).
// This mimics reflect.DeepEqual(res, corev1.ResourceRequirements{}) but is safer and faster.
// It is used for merging logic where we want to distinguish "inherit" (nil) from "empty" (set to empty).
func isResourcesZero(res corev1.ResourceRequirements) bool {
	return res.Requests == nil && res.Limits == nil && res.Claims == nil
}

// ============================================================================
// Shared Defaulting Helpers
// ============================================================================

// defaultEtcdSpec applies hardcoded safety defaults to an inline Etcd spec.
func defaultEtcdSpec(spec *multigresv1alpha1.EtcdSpec, defaultRootPath string) {
	if spec.Image == "" {
		spec.Image = DefaultEtcdImage
	}
	if spec.Storage.Size == "" {
		spec.Storage.Size = DefaultEtcdStorageSize
	}
	if spec.Replicas == nil {
		r := DefaultEtcdReplicas
		spec.Replicas = &r
	}
	if spec.RootPath == "" {
		spec.RootPath = defaultRootPath
	}
	// Use isResourcesZero to ensure we respect overrides that only have Claims
	if isResourcesZero(spec.Resources) {
		// Safety: DefaultResourcesEtcd() returns a fresh struct, so no DeepCopy needed.
		spec.Resources = DefaultResourcesEtcd()
	}
}

// defaultExternalTopoSpec applies hardcoded safety defaults to an external topo server spec.
func defaultExternalTopoSpec(
	spec *multigresv1alpha1.ExternalTopoServerSpec,
	defaultRootPath string,
) {
	if spec.Implementation == "" {
		spec.Implementation = DefaultTopoImplementation
	}
	if spec.RootPath == "" {
		spec.RootPath = defaultRootPath
	}
}

// defaultStatelessSpec applies hardcoded safety defaults to any stateless spec.
func defaultStatelessSpec(
	spec *multigresv1alpha1.StatelessSpec,
	defaultRes corev1.ResourceRequirements,
	defaultReplicas int32,
) {
	if spec.Replicas == nil {
		spec.Replicas = &defaultReplicas
	}
	// Use isResourcesZero to ensure we respect overrides that only have Claims
	if isResourcesZero(spec.Resources) {
		// Safety: We assume defaultRes is passed by value (a fresh copy from the default function).
		// We perform a DeepCopy to ensure spec.Resources owns its own maps, independent of the input defaultRes.
		spec.Resources = *defaultRes.DeepCopy()
	}
}
