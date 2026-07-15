package resolver

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// ResolveCell determines the final configuration for a specific Cell.
// It orchestrates: Template Lookup -> Fetch -> Merge -> Defaulting.
func (r *Resolver) ResolveCell(
	ctx context.Context,
	cellSpec *multigresv1alpha1.CellConfig,
) (*multigresv1alpha1.StatelessSpec, *multigresv1alpha1.PodPlacementSpec, *multigresv1alpha1.LocalTopoServerSpec, error) {
	// 1. Fetch Template (Logic handles defaults)
	templateName := cellSpec.CellTemplate
	tpl, err := r.ResolveCellTemplate(ctx, templateName)
	if err != nil {
		return nil, nil, nil, err
	}

	// 2. Merge Logic
	gateway, placement, localTopo := mergeCellConfig(tpl, cellSpec.Overrides, cellSpec.Spec)

	// 3. Apply Deep Defaults (Level 4)
	defaultStatelessSpec(gateway, DefaultResourcesGateway(), 1)

	// Note: We do NOT default LocalTopo here because it is optional.
	if localTopo != nil {
		defaultRootPath := fmt.Sprintf("/multigres/%s", cellSpec.Name)
		if localTopo.Etcd != nil {
			defaultEtcdSpec(localTopo.Etcd, defaultRootPath)
		}
		if localTopo.External != nil {
			defaultExternalTopoSpec(localTopo.External, defaultRootPath)
		}
	}

	return gateway, placement, localTopo, nil
}

// ResolveCellTemplate fetches and resolves a CellTemplate by name.
func (r *Resolver) ResolveCellTemplate(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) (*multigresv1alpha1.CellTemplate, error) {
	resolvedName := name
	isImplicitFallback := false

	if resolvedName == "" || resolvedName == FallbackCellTemplate {
		resolvedName = FallbackCellTemplate
		isImplicitFallback = true
	}

	// Check cache first
	if cached, found := r.CellTemplateCache[string(resolvedName)]; found {
		return cached.DeepCopy(), nil
	}

	tpl := &multigresv1alpha1.CellTemplate{}
	err := r.Client.Get(
		ctx,
		types.NamespacedName{Name: string(resolvedName), Namespace: r.Namespace},
		tpl,
	)
	if err != nil {
		if errors.IsNotFound(err) {
			if isImplicitFallback {
				// Don't cache fallback empty templates
				return &multigresv1alpha1.CellTemplate{}, nil
			}
			return nil, fmt.Errorf("referenced CellTemplate '%s' not found: %w", resolvedName, err)
		}
		return nil, fmt.Errorf("failed to get CellTemplate: %w", err)
	}

	// Store in cache
	r.CellTemplateCache[string(resolvedName)] = tpl
	return tpl.DeepCopy(), nil
}

// mergeCellConfig merges a template spec with overrides and an inline spec.
func mergeCellConfig(
	template *multigresv1alpha1.CellTemplate,
	overrides *multigresv1alpha1.CellOverrides,
	inline *multigresv1alpha1.CellInlineSpec,
) (*multigresv1alpha1.StatelessSpec, *multigresv1alpha1.PodPlacementSpec, *multigresv1alpha1.LocalTopoServerSpec) {
	// Start with empty
	gateway := &multigresv1alpha1.StatelessSpec{}
	var placement *multigresv1alpha1.PodPlacementSpec
	var localTopo *multigresv1alpha1.LocalTopoServerSpec

	// 1. Apply Template (Base)
	if template != nil {
		if template.Spec.Multigateway != nil {
			gateway = template.Spec.Multigateway.DeepCopy()
		}
		if template.Spec.LocalTopoServer != nil {
			localTopo = template.Spec.LocalTopoServer.DeepCopy()
		}
		mergePodPlacementSpec(&placement, template.Spec.MultigatewayPlacement)
	}

	// 2. Apply Overrides (Explicit Template Modification)
	if overrides != nil {
		if overrides.Multigateway != nil {
			mergeStatelessSpec(gateway, overrides.Multigateway)
		}
		mergePodPlacementSpec(&placement, overrides.MultigatewayPlacement)
	}

	// 3. Apply Inline Spec (Primary Overlay)
	// This merges the inline definition on top of the template+overrides.
	if inline != nil {
		mergeStatelessSpec(gateway, &inline.Multigateway)
		mergePodPlacementSpec(&placement, inline.MultigatewayPlacement)

		if inline.LocalTopoServer != nil {
			// LocalTopo is complex (polymorphic), so we treat it as a replacement if provided
			localTopo = inline.LocalTopoServer.DeepCopy()
		}
	}

	return gateway, placement, localTopo
}
