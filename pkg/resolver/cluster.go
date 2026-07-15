package resolver

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// PopulateClusterDefaults applies static defaults to the Cluster Spec.
func (r *Resolver) PopulateClusterDefaults(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) ([]string, error) {
	var decisions []string
	// 1. Default Images
	if cluster.Spec.Images.Postgres == "" {
		cluster.Spec.Images.Postgres = multigresv1alpha1.DefaultPostgresImage
	}
	if cluster.Spec.Images.Multiadmin == "" {
		cluster.Spec.Images.Multiadmin = multigresv1alpha1.DefaultMultiadminImage
	}
	if cluster.Spec.Images.MultiadminWeb == "" {
		cluster.Spec.Images.MultiadminWeb = multigresv1alpha1.DefaultMultiadminWebImage
	}
	if cluster.Spec.Images.Multiorch == "" {
		cluster.Spec.Images.Multiorch = multigresv1alpha1.DefaultMultiorchImage
	}
	if cluster.Spec.Images.Multipooler == "" {
		cluster.Spec.Images.Multipooler = multigresv1alpha1.DefaultMultipoolerImage
	}
	if cluster.Spec.Images.Multigateway == "" {
		cluster.Spec.Images.Multigateway = multigresv1alpha1.DefaultMultigatewayImage
	}
	if cluster.Spec.Images.ImagePullPolicy == "" {
		cluster.Spec.Images.ImagePullPolicy = DefaultImagePullPolicy
	}

	if cluster.Spec.TopologyPruning == nil {
		cluster.Spec.TopologyPruning = &multigresv1alpha1.TopologyPruningConfig{
			Enabled: ptr.To(true),
		}
	}

	if cluster.Spec.DurabilityPolicy == "" {
		cluster.Spec.DurabilityPolicy = DefaultDurabilityPolicy
	}

	// Default Log Levels
	if cluster.Spec.LogLevels.Pgctld == "" {
		cluster.Spec.LogLevels.Pgctld = DefaultLogLevel
	}
	if cluster.Spec.LogLevels.Multipooler == "" {
		cluster.Spec.LogLevels.Multipooler = DefaultLogLevel
	}
	if cluster.Spec.LogLevels.Multiorch == "" {
		cluster.Spec.LogLevels.Multiorch = DefaultLogLevel
	}
	if cluster.Spec.LogLevels.Multiadmin == "" {
		cluster.Spec.LogLevels.Multiadmin = DefaultLogLevel
	}
	if cluster.Spec.LogLevels.Multigateway == "" {
		cluster.Spec.LogLevels.Multigateway = DefaultLogLevel
	}

	if cluster.Spec.PVCDeletionPolicy == nil {
		cluster.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
			WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
			WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
		}
	}

	if cluster.Spec.Backup == nil {
		cluster.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
			Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
				Path: DefaultBackupPath,
				Storage: multigresv1alpha1.StorageSpec{
					Size: DefaultBackupStorageSize,
				},
			},
		}
	}

	// 2. Smart Defaulting: System Catalog
	if len(cluster.Spec.Databases) == 0 {
		cluster.Spec.Databases = append(cluster.Spec.Databases, multigresv1alpha1.DatabaseConfig{
			Name:    DefaultSystemDatabaseName,
			Default: true,
		})
	}

	var defaultCells []multigresv1alpha1.CellName
	for _, c := range cluster.Spec.Cells {
		defaultCells = append(defaultCells, c.Name)
	}

	// Logic: Should we inject the "default" pool inline?
	// Rule 1: If user EXPLICITLY requested a template, NEVER inject defaults. Trust the user.
	// Rule 2: If user requested NOTHING, check if "default" template exists.
	//         If exists -> Do not inject (use implicit template).
	//         If missing -> Inject defaults (Zero Config mode).
	shouldInjectDefaults := false

	userExplicitTemplate := cluster.Spec.TemplateDefaults.ShardTemplate
	if userExplicitTemplate != "" {
		// Rule 1: Explicit template -> No defaults
		shouldInjectDefaults = false
	} else {
		// Rule 2: No explicit template. Check for implicit "default".
		implicitExists, err := r.ShardTemplateExists(ctx, "default")
		if err != nil {
			return nil, fmt.Errorf("failed to check for implicit shard template: %w", err)
		}
		if implicitExists {
			shouldInjectDefaults = false
		} else {
			shouldInjectDefaults = true
			decisions = append(
				decisions,
				"Implicitly injected default 'readWrite' pool configuration using the 'default' ShardTemplate",
			)
		}
	}

	for i := range cluster.Spec.Databases {
		if len(cluster.Spec.Databases[i].TableGroups) == 0 {
			cluster.Spec.Databases[i].TableGroups = append(
				cluster.Spec.Databases[i].TableGroups,
				multigresv1alpha1.TableGroupConfig{
					Name:    DefaultSystemTableGroupName,
					Default: true,
				},
			)
		}

		for j := range cluster.Spec.Databases[i].TableGroups {
			if len(cluster.Spec.Databases[i].TableGroups[j].Shards) == 0 {
				shardCfg := multigresv1alpha1.ShardConfig{
					Name: "0-inf",
				}

				if len(defaultCells) > 0 {
					shardCfg.Spec = &multigresv1alpha1.ShardInlineSpec{
						Multiorch: multigresv1alpha1.MultiorchSpec{
							// Clean Spec: Do not inject default cells statically.
							// This allows dynamic contextual resolution later.
							// Cells: defaultCells,
						},
						Pools: make(map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec),
					}

					// Apply the decision made above
					if shouldInjectDefaults {
						shardCfg.Spec.Pools["default"] = multigresv1alpha1.PoolSpec{
							Type: "readWrite",
							// Clean Spec: Do not inject default cells.
							// Cells: defaultCells,
						}
					}
				}

				cluster.Spec.Databases[i].TableGroups[j].Shards = append(
					cluster.Spec.Databases[i].TableGroups[j].Shards,
					shardCfg,
				)
			}
		}
	}

	return decisions, nil
}

// ResolveGlobalTopo determines the final GlobalTopoServer configuration.
func (r *Resolver) ResolveGlobalTopo(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (*multigresv1alpha1.GlobalTopoServerSpec, error) {
	var templateName multigresv1alpha1.TemplateRef
	var spec *multigresv1alpha1.GlobalTopoServerSpec

	if cluster.Spec.GlobalTopoServer != nil {
		templateName = cluster.Spec.GlobalTopoServer.TemplateRef
		spec = cluster.Spec.GlobalTopoServer
	}

	// Apply global CoreTemplate default (Level 2 in the 4-level override chain)
	if templateName == "" {
		templateName = cluster.Spec.TemplateDefaults.CoreTemplate
	}

	coreTemplate, err := r.ResolveCoreTemplate(ctx, templateName)
	if err != nil {
		return nil, err
	}

	var finalSpec *multigresv1alpha1.GlobalTopoServerSpec

	if coreTemplate != nil && coreTemplate.Spec.GlobalTopoServer != nil {
		finalSpec = &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd:              coreTemplate.Spec.GlobalTopoServer.Etcd.DeepCopy(),
			PVCDeletionPolicy: coreTemplate.Spec.GlobalTopoServer.PVCDeletionPolicy,
		}
	} else {
		finalSpec = &multigresv1alpha1.GlobalTopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{},
		}
	}

	if spec != nil {
		if spec.External != nil {
			finalSpec.External = spec.External.DeepCopy()
			finalSpec.Etcd = nil
		} else if spec.Etcd != nil {
			if finalSpec.Etcd == nil {
				finalSpec.Etcd = &multigresv1alpha1.EtcdSpec{}
			}
			mergeEtcdSpec(finalSpec.Etcd, spec.Etcd)
		}
	}

	if finalSpec.Etcd != nil {
		defaultEtcdSpec(finalSpec.Etcd, DefaultTopoRootPath)
	}
	if finalSpec.External != nil {
		defaultExternalTopoSpec(finalSpec.External, DefaultTopoRootPath)
	}

	// Merge GlobalTopoServerSpec-level PVCDeletionPolicy
	if spec != nil && spec.PVCDeletionPolicy != nil {
		finalSpec.PVCDeletionPolicy = spec.PVCDeletionPolicy
	}

	return finalSpec, nil
}

// ResolveMultiadmin determines the final Multiadmin configuration.
func (r *Resolver) ResolveMultiadmin(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (*multigresv1alpha1.StatelessSpec, error) {
	var templateName multigresv1alpha1.TemplateRef
	var spec *multigresv1alpha1.MultiadminConfig

	if cluster.Spec.Multiadmin != nil {
		templateName = cluster.Spec.Multiadmin.TemplateRef
		spec = cluster.Spec.Multiadmin
	}

	// Apply global CoreTemplate default (Level 2 in the 4-level override chain)
	if templateName == "" {
		templateName = cluster.Spec.TemplateDefaults.CoreTemplate
	}

	coreTemplate, err := r.ResolveCoreTemplate(ctx, templateName)
	if err != nil {
		return nil, err
	}

	finalSpec := &multigresv1alpha1.StatelessSpec{}

	if coreTemplate != nil && coreTemplate.Spec.Multiadmin != nil {
		finalSpec = coreTemplate.Spec.Multiadmin.DeepCopy()
	}

	if spec != nil && spec.Spec != nil {
		mergeStatelessSpec(finalSpec, spec.Spec)
	}

	defaultStatelessSpec(finalSpec, DefaultResourcesAdmin(), DefaultAdminReplicas)

	return finalSpec, nil
}

// ResolveMultiadminWeb determines the final MultiadminWeb configuration.
func (r *Resolver) ResolveMultiadminWeb(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (*multigresv1alpha1.StatelessSpec, error) {
	var templateName multigresv1alpha1.TemplateRef
	var spec *multigresv1alpha1.MultiadminWebConfig

	if cluster.Spec.MultiadminWeb != nil {
		templateName = cluster.Spec.MultiadminWeb.TemplateRef
		spec = cluster.Spec.MultiadminWeb
	}

	// Apply global CoreTemplate default (Level 2 in the 4-level override chain)
	if templateName == "" {
		templateName = cluster.Spec.TemplateDefaults.CoreTemplate
	}

	coreTemplate, err := r.ResolveCoreTemplate(ctx, templateName)
	if err != nil {
		return nil, err
	}

	finalSpec := &multigresv1alpha1.StatelessSpec{}

	if coreTemplate != nil && coreTemplate.Spec.MultiadminWeb != nil {
		finalSpec = coreTemplate.Spec.MultiadminWeb.DeepCopy()
	}

	if spec != nil && spec.Spec != nil {
		mergeStatelessSpec(finalSpec, spec.Spec)
	}

	defaultStatelessSpec(finalSpec, DefaultResourcesAdminWeb(), DefaultMultiadminWebReplicas)

	return finalSpec, nil
}

// ResolveCoreTemplate fetches and resolves a CoreTemplate by name, using the request-scoped cache.
func (r *Resolver) ResolveCoreTemplate(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) (*multigresv1alpha1.CoreTemplate, error) {
	resolvedName := name
	isImplicitFallback := false

	if resolvedName == "" || resolvedName == FallbackCoreTemplate {
		resolvedName = FallbackCoreTemplate
		isImplicitFallback = true
	}

	// Check cache first
	if cached, found := r.CoreTemplateCache[string(resolvedName)]; found {
		return cached.DeepCopy(), nil
	}

	// 2. Fetch
	tpl := &multigresv1alpha1.CoreTemplate{}
	key := types.NamespacedName{Name: string(resolvedName), Namespace: r.Namespace}
	if err := r.Client.Get(ctx, key, tpl); err != nil {
		if errors.IsNotFound(err) {
			if isImplicitFallback {
				// Don't cache fallback empty templates
				return &multigresv1alpha1.CoreTemplate{}, nil
			}
			return nil, fmt.Errorf("referenced CoreTemplate '%s' not found: %w", resolvedName, err)
		}
		return nil, fmt.Errorf("failed to get CoreTemplate: %w", err)
	}

	// 3. Cache
	r.CoreTemplateCache[string(resolvedName)] = tpl
	return tpl.DeepCopy(), nil
}

func mergeEtcdSpec(base *multigresv1alpha1.EtcdSpec, override *multigresv1alpha1.EtcdSpec) {
	if override.Image != "" {
		base.Image = override.Image
	}
	if override.Replicas != nil {
		base.Replicas = override.Replicas
	}
	if override.Storage.Size != "" {
		base.Storage.Size = override.Storage.Size
	}
	if override.Storage.Class != "" {
		base.Storage.Class = override.Storage.Class
	}
	if len(override.Storage.AccessModes) > 0 {
		base.Storage.AccessModes = override.Storage.AccessModes
	}
	if !isResourcesZero(override.Resources) {
		base.Resources = *override.Resources.DeepCopy()
	}
	if override.RootPath != "" {
		base.RootPath = override.RootPath
	}
	if override.PVCDeletionPolicy != nil {
		base.PVCDeletionPolicy = override.PVCDeletionPolicy
	}
}
