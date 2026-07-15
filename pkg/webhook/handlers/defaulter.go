package handlers

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

// +kubebuilder:webhook:path=/mutate-multigres-com-v1alpha1-multigrescluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=multigres.com,resources=multigresclusters,verbs=create;update,versions=v1alpha1,name=mmultigrescluster.kb.io,admissionReviewVersions=v1

// MultigresClusterDefaulter handles the mutation of MultigresCluster resources.
type MultigresClusterDefaulter struct {
	Resolver *resolver.Resolver
}

var _ webhook.CustomDefaulter = &MultigresClusterDefaulter{}

// NewMultigresClusterDefaulter creates a new defaulter handler.
func NewMultigresClusterDefaulter(r *resolver.Resolver) *MultigresClusterDefaulter {
	return &MultigresClusterDefaulter{
		Resolver: r,
	}
}

// Default implements webhook.CustomDefaulter.
func (d *MultigresClusterDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	start := time.Now()
	ctx, span := monitoring.StartChildSpan(ctx, "Webhook.Default")
	defer span.End()

	logger := log.FromContext(ctx)
	logger.V(1).Info("defaulting webhook started")

	// SAFETY CHECK
	if d.Resolver == nil {
		err := fmt.Errorf("defaulter not initialized: resolver is nil")
		monitoring.RecordSpanError(span, err)
		monitoring.RecordWebhookRequest("DEFAULT", "MultigresCluster", err, time.Since(start))
		return err
	}

	cluster, ok := obj.(*multigresv1alpha1.MultigresCluster)
	if !ok {
		err := fmt.Errorf("expected MultigresCluster, got %T", obj)
		monitoring.RecordSpanError(span, err)
		return err
	}

	// 1. Static Defaulting (Images, System Catalog)
	if _, err := d.Resolver.PopulateClusterDefaults(ctx, cluster); err != nil {
		return fmt.Errorf("failed to populate cluster defaults: %w", err)
	}

	// 2. Create a "Request Scoped" Resolver
	// We copy the resolver and point it to the Object's Namespace.
	scopedResolver := *d.Resolver
	scopedResolver.Namespace = cluster.Namespace
	scopedResolver.CoreTemplateCache = make(map[string]*multigresv1alpha1.CoreTemplate)
	scopedResolver.CellTemplateCache = make(map[string]*multigresv1alpha1.CellTemplate)
	scopedResolver.ShardTemplateCache = make(map[string]*multigresv1alpha1.ShardTemplate)

	// 2.5 Promote Implicit Defaults to Explicit
	// If the user hasn't specified a template, but a "default" one exists,
	// we explicitly set it in the Spec. This ensures the user KNOWS a template is being used
	// instead of it happening magically behind the scenes.
	{
		if cluster.Spec.TemplateDefaults.CoreTemplate == "" {
			exists, _ := scopedResolver.CoreTemplateExists(ctx, resolver.FallbackCoreTemplate)
			if exists {
				cluster.Spec.TemplateDefaults.CoreTemplate = resolver.FallbackCoreTemplate
			}
		}
		if cluster.Spec.TemplateDefaults.CellTemplate == "" {
			exists, _ := scopedResolver.CellTemplateExists(ctx, resolver.FallbackCellTemplate)
			if exists {
				cluster.Spec.TemplateDefaults.CellTemplate = resolver.FallbackCellTemplate
			}
		}
		if cluster.Spec.TemplateDefaults.ShardTemplate == "" {
			exists, _ := scopedResolver.ShardTemplateExists(ctx, resolver.FallbackShardTemplate)
			if exists {
				cluster.Spec.TemplateDefaults.ShardTemplate = resolver.FallbackShardTemplate
			}
		}
	}

	// 3. Stateful Resolution (Visible Defaults)

	// A. Resolve Global Topo Server
	// Logic:
	// 1. If explicit Inline Template -> Skip (Dynamic)
	// 2. If explicit Global Template (TemplateDefaults) -> Skip (Dynamic)
	// 3. If implicit "default" Template exists -> Skip (Dynamic)
	// 4. Else -> Materialize Hardcoded Defaults.

	// Helper to check intent
	hasGlobalCore := cluster.Spec.TemplateDefaults.CoreTemplate != ""
	hasImplicitCore, _ := scopedResolver.CoreTemplateExists(
		ctx,
		resolver.FallbackCoreTemplate,
	) // Ignore error, treat as false

	// GlobalTopo
	{
		hasInline := cluster.Spec.GlobalTopoServer != nil &&
			cluster.Spec.GlobalTopoServer.TemplateRef != ""

		isUsingTemplate := hasInline || hasGlobalCore || hasImplicitCore

		if !isUsingTemplate {
			// No template involved. Materialize defaults.
			globalTopo, err := scopedResolver.ResolveGlobalTopo(ctx, cluster)
			if err != nil {
				return fmt.Errorf("failed to resolve globalTopoServer: %w", err)
			}
			cluster.Spec.GlobalTopoServer = globalTopo
		}
	}

	// B. Resolve Multiadmin
	{
		hasInline := cluster.Spec.Multiadmin != nil && cluster.Spec.Multiadmin.TemplateRef != ""
		isUsingTemplate := hasInline || hasGlobalCore || hasImplicitCore

		if !isUsingTemplate {
			multiadmin, err := scopedResolver.ResolveMultiadmin(ctx, cluster)
			if err != nil {
				return fmt.Errorf("failed to resolve multiadmin: %w", err)
			}
			if cluster.Spec.Multiadmin == nil {
				cluster.Spec.Multiadmin = &multigresv1alpha1.MultiadminConfig{}
			}
			if multiadmin != nil {
				cluster.Spec.Multiadmin.Spec = multiadmin
			}
		}
	}

	// B2. Resolve MultiadminWeb
	{
		hasInline := cluster.Spec.MultiadminWeb != nil &&
			cluster.Spec.MultiadminWeb.TemplateRef != ""
		isUsingTemplate := hasInline || hasGlobalCore || hasImplicitCore

		if !isUsingTemplate {
			multiadminWeb, err := scopedResolver.ResolveMultiadminWeb(ctx, cluster)
			if err != nil {
				return fmt.Errorf("failed to resolve multiadmin-web: %w", err)
			}
			if cluster.Spec.MultiadminWeb == nil {
				cluster.Spec.MultiadminWeb = &multigresv1alpha1.MultiadminWebConfig{}
			}
			if multiadminWeb != nil {
				cluster.Spec.MultiadminWeb.Spec = multiadminWeb
			}
		}
	}

	// C. Resolve Cells
	hasGlobalCell := cluster.Spec.TemplateDefaults.CellTemplate != ""
	hasImplicitCell, _ := scopedResolver.CellTemplateExists(ctx, resolver.FallbackCellTemplate)

	for i := range cluster.Spec.Cells {
		cell := &cluster.Spec.Cells[i]
		hasInline := cell.CellTemplate != ""

		isUsingTemplate := hasInline || hasGlobalCell || hasImplicitCell

		if !isUsingTemplate {
			gatewaySpec, gatewayPlacement, localTopoSpec, err := scopedResolver.ResolveCell(
				ctx,
				cell,
			)
			if err != nil {
				return fmt.Errorf("failed to resolve cell '%s': %w", cell.Name, err)
			}
			cell.Spec = &multigresv1alpha1.CellInlineSpec{
				Multigateway:          *gatewaySpec,
				MultigatewayPlacement: gatewayPlacement,
				LocalTopoServer:       localTopoSpec,
			}
		}
	}

	// D. Resolve Shards
	hasGlobalShard := cluster.Spec.TemplateDefaults.ShardTemplate != ""
	hasImplicitShard, _ := scopedResolver.ShardTemplateExists(ctx, resolver.FallbackShardTemplate)
	var allCellNames []multigresv1alpha1.CellName
	for _, cell := range cluster.Spec.Cells {
		allCellNames = append(allCellNames, cell.Name)
	}

	for i := range cluster.Spec.Databases {
		dbBackup := multigresv1alpha1.MergeBackupConfig(
			cluster.Spec.Databases[i].Backup,
			cluster.Spec.Backup,
		)
		for j := range cluster.Spec.Databases[i].TableGroups {
			tgBackup := multigresv1alpha1.MergeBackupConfig(
				cluster.Spec.Databases[i].TableGroups[j].Backup,
				dbBackup,
			)
			for k := range cluster.Spec.Databases[i].TableGroups[j].Shards {
				shard := &cluster.Spec.Databases[i].TableGroups[j].Shards[k]
				hasInline := shard.ShardTemplate != ""

				isUsingTemplate := hasInline || hasGlobalShard || hasImplicitShard

				if !isUsingTemplate {
					// We pass cell names for contextual cell defaulting, but leave
					// MaterializeCellDefaults false so the stored spec remains dynamic.
					multiorchSpec, poolsSpec, resolvedPvcPolicy, resolvedBackupConfig, resolvedInitdbArgs, resolvedPostgresConfigRef, err := scopedResolver.ResolveShard(
						ctx,
						shard,
						resolver.ResolveShardOptions{
							AllCellNames:    allCellNames,
							InheritedBackup: tgBackup,
						},
					)
					if err != nil {
						return fmt.Errorf("failed to resolve shard '%s': %w", shard.Name, err)
					}

					// Preserve PVCDeletionPolicy if it was set in the original spec
					// Otherwise, use the resolved policy (from template)
					var pvcPolicy *multigresv1alpha1.PVCDeletionPolicy
					if shard.Spec != nil && shard.Spec.PVCDeletionPolicy != nil {
						pvcPolicy = shard.Spec.PVCDeletionPolicy
					} else {
						pvcPolicy = resolvedPvcPolicy
					}

					shard.Spec = &multigresv1alpha1.ShardInlineSpec{
						Multiorch:         *multiorchSpec,
						InitdbArgs:        resolvedInitdbArgs,
						PostgresConfigRef: resolvedPostgresConfigRef,
						Pools:             poolsSpec,
						PVCDeletionPolicy: pvcPolicy,
					}
					shard.Backup = resolvedBackupConfig
				}
			}
		}
	}

	// Inject trace context into annotations so the controller can continue
	// the trace started by this webhook across the async boundary.
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	monitoring.InjectTraceContext(ctx, cluster.Annotations)

	monitoring.RecordWebhookRequest("DEFAULT", "MultigresCluster", nil, time.Since(start))
	logger.V(1).Info("defaulting webhook complete", "duration", time.Since(start).String())
	return nil
}
