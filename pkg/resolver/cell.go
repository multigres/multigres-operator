package resolver

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/topology"
)

// ResolveCell determines the final configuration for a specific Cell.
// It orchestrates: Template Lookup -> Fetch -> Merge -> Defaulting.
func (r *Resolver) ResolveCell(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	cellSpec *multigresv1alpha1.CellConfig,
) (*multigresv1alpha1.MultigatewaySpec, *multigresv1alpha1.PodPlacementSpec, *multigresv1alpha1.LocalTopoServerSpec, error) {
	// 1. Fetch Template (Logic handles defaults)
	templateName := cellSpec.CellTemplate
	tpl, err := r.ResolveCellTemplate(ctx, templateName)
	if err != nil {
		return nil, nil, nil, err
	}

	// 2. Merge Logic
	gateway, placement, localTopo := mergeCellConfig(tpl, cellSpec.Overrides, cellSpec.Spec)

	// 3. Apply Deep Defaults (Level 4)
	defaultStatelessSpec(&gateway.StatelessSpec, DefaultResourcesGateway(), 1)
	defaultGatewayBuffer(gateway)

	// Note: We do NOT default LocalTopo here because it is optional.
	if localTopo != nil {
		roots, err := topology.NewRoots(cluster.Annotations, cluster.Namespace, cluster.Name)
		if err != nil {
			return nil, nil, nil, fmt.Errorf(
				"deriving topology roots for cell %q: %w",
				cellSpec.Name,
				err,
			)
		}
		defaultRootPath, err := roots.Cell(string(cellSpec.Name))
		if err != nil {
			return nil, nil, nil, fmt.Errorf(
				"deriving topology root for cell %q: %w",
				cellSpec.Name,
				err,
			)
		}
		if localTopo.Etcd != nil {
			defaultEtcdSpec(localTopo.Etcd, defaultRootPath)
		}
		if localTopo.External != nil {
			defaultExternalTopoSpec(localTopo.External, defaultRootPath)
		}
	}

	return gateway, placement, localTopo, nil
}

// EffectiveCellTemplateName maps a cell's template ref to the template name
// resolution will actually use: an empty ref selects the implicit fallback.
// Shared by ResolveCellTemplate and the webhook's consumer filter so they can
// never disagree about which template a cell consumes.
func EffectiveCellTemplateName(ref multigresv1alpha1.TemplateRef) multigresv1alpha1.TemplateRef {
	if ref == "" {
		return FallbackCellTemplate
	}
	return ref
}

// ResolveCellTemplate fetches and resolves a CellTemplate by name.
func (r *Resolver) ResolveCellTemplate(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) (*multigresv1alpha1.CellTemplate, error) {
	resolvedName := EffectiveCellTemplateName(name)
	isImplicitFallback := resolvedName == FallbackCellTemplate

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
) (*multigresv1alpha1.MultigatewaySpec, *multigresv1alpha1.PodPlacementSpec, *multigresv1alpha1.LocalTopoServerSpec) {
	// Start with empty
	gateway := &multigresv1alpha1.MultigatewaySpec{}
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
			mergeMultigatewaySpec(gateway, overrides.Multigateway)
		}
		mergePodPlacementSpec(&placement, overrides.MultigatewayPlacement)
	}

	// 3. Apply Inline Spec (Primary Overlay)
	// This merges the inline definition on top of the template+overrides.
	if inline != nil {
		mergeMultigatewaySpec(gateway, &inline.Multigateway)
		mergePodPlacementSpec(&placement, inline.MultigatewayPlacement)

		if inline.LocalTopoServer != nil {
			// LocalTopo is complex (polymorphic), so we treat it as a replacement if provided
			localTopo = inline.LocalTopoServer.DeepCopy()
		}
	}

	return gateway, placement, localTopo
}

// mergeMultigatewaySpec merges an override onto a base gateway spec: the shared
// stateless fields and the buffer settings both merge per-field, so an override
// that only sets one buffer knob keeps the rest from the base.
func mergeMultigatewaySpec(
	base *multigresv1alpha1.MultigatewaySpec,
	override *multigresv1alpha1.MultigatewaySpec,
) {
	mergeStatelessSpec(&base.StatelessSpec, &override.StatelessSpec)

	if override.Buffer == nil {
		return
	}
	if base.Buffer == nil {
		base.Buffer = &multigresv1alpha1.GatewayBufferConfig{}
	}
	// Copy so the resolved spec never shares pointers with the caller's
	// (possibly informer-cached) object, matching the other merge helpers.
	ovr := override.Buffer.DeepCopy()
	if ovr.Enabled != nil {
		base.Buffer.Enabled = ovr.Enabled
	}
	if ovr.Window != nil {
		base.Buffer.Window = ovr.Window
	}
	if ovr.MaxFailoverDuration != nil {
		base.Buffer.MaxFailoverDuration = ovr.MaxFailoverDuration
	}
	if ovr.MinTimeBetweenFailovers != nil {
		base.Buffer.MinTimeBetweenFailovers = ovr.MinTimeBetweenFailovers
	}
	if ovr.Size != nil {
		base.Buffer.Size = ovr.Size
	}
	if ovr.DrainConcurrency != nil {
		base.Buffer.DrainConcurrency = ovr.DrainConcurrency
	}
}

// defaultGatewayBuffer turns failover buffering on unless the user disabled it
// explicitly. All other buffer fields stay unset so the multigateway binary's
// own defaults apply.
func defaultGatewayBuffer(spec *multigresv1alpha1.MultigatewaySpec) {
	if spec.Buffer == nil {
		spec.Buffer = &multigresv1alpha1.GatewayBufferConfig{}
	}
	if spec.Buffer.Enabled == nil {
		spec.Buffer.Enabled = ptr.To(true)
	}
}
