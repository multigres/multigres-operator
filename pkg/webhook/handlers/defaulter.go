package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

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

	// 1. Static Defaulting (Pull Policy, System Catalog)
	// Component images are resolved at reconcile time from operator
	// configuration and never persisted into the spec.
	// Defaulting fills an absent spec.backup with the filesystem default,
	// after which it cannot be told from a filesystem block the user wrote.
	// This makes Default depend on seeing the request as it was submitted:
	// called a second time on its own output, the filled-in block reads as
	// type filesystem written by the user, and dropStaleShardBackupCopies
	// removes S3 shard blocks for it. The API server calls a mutating
	// webhook once per request unless its reinvocationPolicy is IfNeeded, so
	// this webhook's configuration must keep the default, Never.
	submittedBackup := cluster.Spec.Backup.DeepCopy()
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
			multiadmin, placement, err := scopedResolver.ResolveMultiadmin(ctx, cluster)
			if err != nil {
				return fmt.Errorf("failed to resolve multiadmin: %w", err)
			}
			if cluster.Spec.Multiadmin == nil {
				cluster.Spec.Multiadmin = &multigresv1alpha1.MultiadminConfig{}
			}
			if multiadmin != nil {
				cluster.Spec.Multiadmin.Spec = multiadmin
			}
			if placement != nil {
				cluster.Spec.Multiadmin.Placement = placement
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
				cluster,
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

	dropStaleShardBackupCopies(ctx, cluster, submittedBackup)

	// A shard's backup block is left as written. The controllers merge the
	// cluster, database, tablegroup and shard blocks themselves, and a copy
	// of the merged result stored here would keep the values of the day it
	// was made: a later edit above it would be overridden by the copy.
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
					resolved, err := scopedResolver.ResolveShard(
						ctx,
						shard,
						resolver.ResolveShardOptions{
							AllCellNames:    allCellNames,
							InheritedBackup: tgBackup,
							Mode:            cluster.Spec.Mode,
						},
					)
					if err != nil {
						return fmt.Errorf("failed to resolve shard '%s': %w", shard.Name, err)
					}

					// Preserve PVCDeletionPolicy if it was set in the original spec
					// Otherwise, use the resolved policy (from template)
					pvcPolicy := resolved.PVCDeletionPolicy
					if shard.Spec != nil && shard.Spec.PVCDeletionPolicy != nil {
						pvcPolicy = shard.Spec.PVCDeletionPolicy
					}

					shard.Spec = &multigresv1alpha1.ShardInlineSpec{
						Multiorch:         resolved.Multiorch,
						InitdbArgs:        resolved.InitdbArgs,
						PostgresConfigRef: resolved.PostgresConfigRef,
						PostgresConfig:    resolved.PostgresConfig,
						Pools:             resolved.Pools,
						PVCDeletionPolicy: pvcPolicy,
					}
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

// dropStaleShardBackupCopies removes, on an update, a shard backup block that
// only repeated what the shard inherited in the stored object, when the
// update changes a level above it so that the block now disagrees.
//
// Stored clusters can carry the merged backup config in every shard block,
// written there by defaulting. Such a block keeps the values of the day it
// was written. After an edit of the bucket, region, endpoint or TLS secret at
// cluster or database level, a distributed cluster would be refused for an
// override the database location does not carry, and a Lite cluster, where
// the shard's block is the location, would keep writing backups to the old
// place with nothing reporting it. Without the block the shard inherits the
// edited values, which is what the block said before the edit.
//
// Three things must all hold, or the block is left for validation to judge:
//
//   - The update does not touch the block. A block the user adds or edits in
//     this update is what they are asking for, whatever it says.
//   - Merged over what the shard inherited in the stored object, the block
//     changes nothing. A block that did change something is an override.
//   - Against the levels above it in the incoming object, the block now asks
//     for something else: an override a distributed cluster's database
//     location would not apply, or any difference in a Lite cluster.
//
// A block that someone wrote by hand to repeat the levels above it cannot be
// told from a defaulted one and is removed the same way.
//
// An S3 block that the levels above it no longer resolve to gets a rule of
// its own, because removing it moves the shard's backups from S3 to the
// cluster's own volume. It is removed only when a level above it says type
// filesystem in this update and did not in the stored object: that is a
// location stated by the user, and the block the update leaves alone does
// not outrank it, whatever it added before. In every other case it stays.
// Deleting the S3 block above it states no location, also when a filesystem
// block that stood further up now shows through. A Lite shard then keeps
// its block and stays on S3; a distributed cluster is refused by validation
// until someone says which location is meant.
//
// The levels are compared as the update submitted them, with submittedBackup
// for spec.backup: defaulting has by now filled an absent spec.backup with
// the filesystem default, which would read as a filesystem block written in
// place of a deleted S3 one. A stored spec.backup always has a type for the
// same reason, so at that level only the change from S3 to filesystem
// counts: a filesystem block sent over the stored default is what every
// update sends that writes the stored object back.
func dropStaleShardBackupCopies(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	submittedBackup *multigresv1alpha1.BackupConfig,
) {
	req, err := admission.RequestFromContext(ctx)
	if err != nil || req.Operation != admissionv1.Update || len(req.OldObject.Raw) == 0 {
		return
	}
	old := &multigresv1alpha1.MultigresCluster{}
	if err := json.Unmarshal(req.OldObject.Raw, old); err != nil {
		return
	}
	type shardKey struct {
		db multigresv1alpha1.DatabaseName
		tg multigresv1alpha1.TableGroupName
		sh multigresv1alpha1.ShardName
	}
	// stored is each stored shard's block, the merged config of the levels
	// above it, and the blocks of those levels: cluster, database, tablegroup.
	type storedShard struct {
		block, inherited *multigresv1alpha1.BackupConfig
		above            [3]*multigresv1alpha1.BackupConfig
	}
	stored := map[shardKey]storedShard{}
	for _, db := range old.Spec.Databases {
		dbBackup := multigresv1alpha1.MergeBackupConfig(db.Backup, old.Spec.Backup)
		for _, tg := range db.TableGroups {
			tgBackup := multigresv1alpha1.MergeBackupConfig(tg.Backup, dbBackup)
			for _, shard := range tg.Shards {
				stored[shardKey{db.Name, tg.Name, shard.Name}] = storedShard{
					block:     shard.Backup,
					inherited: tgBackup,
					above: [3]*multigresv1alpha1.BackupConfig{
						old.Spec.Backup,
						db.Backup,
						tg.Backup,
					},
				}
			}
		}
	}
	lite := cluster.Spec.Mode == multigresv1alpha1.ModeLite
	// adds reports whether block changes anything when merged over inherited.
	adds := func(block, inherited *multigresv1alpha1.BackupConfig) bool {
		return !apiequality.Semantic.DeepEqual(
			resolver.InheritedShardBackup(multigresv1alpha1.MergeBackupConfig(block, inherited)),
			resolver.InheritedShardBackup(inherited),
		)
	}
	// saysFilesystem reports whether a backup block states the filesystem
	// type. No block at all resolves to the filesystem without stating it.
	saysFilesystem := func(config *multigresv1alpha1.BackupConfig) bool {
		return config != nil && config.Type == multigresv1alpha1.BackupTypeFilesystem
	}
	for i := range cluster.Spec.Databases {
		db := &cluster.Spec.Databases[i]
		location := multigresv1alpha1.MergeBackupConfig(db.Backup, cluster.Spec.Backup)
		for j := range db.TableGroups {
			tg := &db.TableGroups[j]
			inherited := multigresv1alpha1.MergeBackupConfig(tg.Backup, location)
			for k := range tg.Shards {
				shard := &tg.Shards[k]
				was, known := stored[shardKey{db.Name, tg.Name, shard.Name}]
				if shard.Backup == nil || !known ||
					!apiequality.Semantic.DeepEqual(shard.Backup, was.block) {
					continue
				}
				if shard.Backup.Type == multigresv1alpha1.BackupTypeS3 &&
					!inherited.ResolvesToS3() {
					// Removing the block would take the shard off S3.
					submitted := [3]*multigresv1alpha1.BackupConfig{
						submittedBackup, db.Backup, tg.Backup,
					}
					for level, block := range submitted {
						if saysFilesystem(block) && !saysFilesystem(was.above[level]) {
							shard.Backup = nil
							break
						}
					}
					continue
				}
				if adds(was.block, was.inherited) {
					continue
				}
				stale := adds(shard.Backup, inherited)
				if !lite {
					stale = multigresv1alpha1.UnappliedBackupOverride(shard.Backup, location) != nil
				}
				if stale {
					shard.Backup = nil
				}
			}
		}
	}
}
