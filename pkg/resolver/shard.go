package resolver

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// ResolveShard determines the final configuration for a specific Shard.
func (r *Resolver) ResolveShard(
	ctx context.Context,
	shardSpec *multigresv1alpha1.ShardConfig,
	allCellNames []multigresv1alpha1.CellName,
	inheritedBackup *multigresv1alpha1.BackupConfig,
) (*multigresv1alpha1.MultiOrchSpec, map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec, *multigresv1alpha1.PVCDeletionPolicy, *multigresv1alpha1.BackupConfig, multigresv1alpha1.InitdbArgs, *multigresv1alpha1.PostgresConfigRef, error) {
	// 1. Fetch Template
	templateName := shardSpec.ShardTemplate
	tpl, err := r.ResolveShardTemplate(ctx, templateName)
	if err != nil {
		return nil, nil, nil, nil, "", nil, err
	}

	// 2. Merge Logic
	multiOrch, pools, pvcPolicy, backupCfg, initdbArgs, postgresConfigRef := mergeShardConfig(
		tpl,
		shardSpec.Overrides,
		shardSpec.Spec,
		shardSpec.Backup,
		inheritedBackup,
	)

	// 3. Apply Deep Defaults (Level 4)
	defaultStatelessSpec(&multiOrch.StatelessSpec, DefaultResourcesOrch(), 1)

	if backupCfg == nil {
		backupCfg = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
		}
	}
	defaultBackupConfig(backupCfg)

	// Contextual Defaulting: Lazy Cell Injection
	// If the resolved configuration has no cells defined, it means "run everywhere".
	// We inject the full list of cluster cells here.
	if len(multiOrch.Cells) == 0 && len(allCellNames) > 0 {
		for _, c := range allCellNames {
			multiOrch.Cells = append(multiOrch.Cells, multigresv1alpha1.CellName(c))
		}
		// Sort for deterministic output
		slices.Sort(multiOrch.Cells)
	}

	if len(pools) == 0 {
		pools[DefaultPoolName] = multigresv1alpha1.PoolSpec{
			Type:  "readWrite",
			Cells: multiOrch.Cells,
		}
	}

	for name := range pools {
		p := pools[name]
		defaultPoolSpec(&p)

		// Contextual Defaulting for Pools
		if len(p.Cells) == 0 && len(allCellNames) > 0 {
			for _, c := range allCellNames {
				p.Cells = append(p.Cells, multigresv1alpha1.CellName(c))
			}
			// Sort for deterministic output
			slices.Sort(p.Cells)
		}

		pools[name] = p
	}

	return &multiOrch, pools, pvcPolicy, backupCfg, initdbArgs, postgresConfigRef, nil
}

// ResolveShardTemplate fetches and resolves a ShardTemplate by name.
func (r *Resolver) ResolveShardTemplate(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) (*multigresv1alpha1.ShardTemplate, error) {
	resolvedName := name
	isImplicitFallback := false

	if resolvedName == "" || resolvedName == FallbackShardTemplate {
		resolvedName = FallbackShardTemplate
		isImplicitFallback = true
	}

	// Check cache first
	if cached, found := r.ShardTemplateCache[string(resolvedName)]; found {
		return cached.DeepCopy(), nil
	}

	// 2. Fetch
	tpl := &multigresv1alpha1.ShardTemplate{}
	key := types.NamespacedName{Name: string(resolvedName), Namespace: r.Namespace}
	if err := r.Client.Get(ctx, key, tpl); err != nil {
		if errors.IsNotFound(err) {
			if isImplicitFallback {
				return &multigresv1alpha1.ShardTemplate{}, nil
			}
			return nil, fmt.Errorf("referenced ShardTemplate '%s' not found: %w", resolvedName, err)
		}
		return nil, fmt.Errorf("failed to get ShardTemplate: %w", err)
	}

	// 3. Cache
	r.ShardTemplateCache[string(resolvedName)] = tpl
	return tpl.DeepCopy(), nil
}

func mergeShardConfig(
	template *multigresv1alpha1.ShardTemplate,
	overrides *multigresv1alpha1.ShardOverrides,
	inline *multigresv1alpha1.ShardInlineSpec,
	backupOverride *multigresv1alpha1.BackupConfig,
	inheritedBackup *multigresv1alpha1.BackupConfig,
) (multigresv1alpha1.MultiOrchSpec, map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec, *multigresv1alpha1.PVCDeletionPolicy, *multigresv1alpha1.BackupConfig, multigresv1alpha1.InitdbArgs, *multigresv1alpha1.PostgresConfigRef) {
	// 1. Start with Template (Base)
	var multiOrch multigresv1alpha1.MultiOrchSpec
	pools := make(map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec)
	var pvcPolicy *multigresv1alpha1.PVCDeletionPolicy
	var initdbArgs multigresv1alpha1.InitdbArgs
	var postgresConfigRef *multigresv1alpha1.PostgresConfigRef
	// Start with inherited backup as base
	var backupCfg *multigresv1alpha1.BackupConfig
	if inheritedBackup != nil {
		backupCfg = inheritedBackup.DeepCopy()
	}

	if template != nil {
		if template.Spec.MultiOrch != nil {
			multiOrch = *template.Spec.MultiOrch.DeepCopy()
		}
		for k, v := range template.Spec.Pools {
			pools[k] = *v.DeepCopy()
		}
		if template.Spec.PVCDeletionPolicy != nil {
			pvcPolicy = template.Spec.PVCDeletionPolicy
		}
		initdbArgs = template.Spec.InitdbArgs
		if template.Spec.PostgresConfigRef != nil {
			postgresConfigRef = template.Spec.PostgresConfigRef
		}
	}

	// 2. Apply Overrides (Explicit Template Modification)
	if overrides != nil {
		if overrides.MultiOrch != nil {
			mergeMultiOrchSpec(&multiOrch, overrides.MultiOrch)
		}
		for k, v := range overrides.Pools {
			if existingPool, exists := pools[k]; exists {
				pools[k] = mergePoolSpec(existingPool, v)
			} else {
				pools[k] = v
			}
		}
		if overrides.InitdbArgs != "" {
			initdbArgs = overrides.InitdbArgs
		}
		if overrides.PostgresConfigRef != nil {
			postgresConfigRef = overrides.PostgresConfigRef
		}
	}

	// 3. Apply Inline Spec (Primary Overlay)
	// This merges the inline definition on top of the template+overrides.
	if inline != nil {
		mergeMultiOrchSpec(&multiOrch, &inline.MultiOrch)

		for k, v := range inline.Pools {
			if existingPool, exists := pools[k]; exists {
				pools[k] = mergePoolSpec(existingPool, v)
			} else {
				pools[k] = v
			}
		}

		// Inline PVCDeletionPolicy overrides template
		if inline.PVCDeletionPolicy != nil {
			pvcPolicy = inline.PVCDeletionPolicy
		}

		if inline.InitdbArgs != "" {
			initdbArgs = inline.InitdbArgs
		}

		if inline.PostgresConfigRef != nil {
			postgresConfigRef = inline.PostgresConfigRef
		}
	}

	// 4. Apply Backup Override (from ShardConfig.Backup)
	// We use MergeBackupConfig so that ShardConfig overrides inherited config
	if backupOverride != nil {
		backupCfg = multigresv1alpha1.MergeBackupConfig(backupOverride, backupCfg)
	}

	return multiOrch, pools, pvcPolicy, backupCfg, initdbArgs, postgresConfigRef
}

func mergeMultiOrchSpec(
	base *multigresv1alpha1.MultiOrchSpec,
	override *multigresv1alpha1.MultiOrchSpec,
) {
	mergeStatelessSpec(&base.StatelessSpec, &override.StatelessSpec)
	if len(override.Cells) > 0 {
		base.Cells = override.Cells
	}
}

func mergePoolSpec(
	base multigresv1alpha1.PoolSpec,
	override multigresv1alpha1.PoolSpec,
) multigresv1alpha1.PoolSpec {
	out := base
	if override.Type != "" {
		out.Type = override.Type
	}
	if len(override.Cells) > 0 {
		out.Cells = override.Cells
	}
	if override.ReplicasPerCell != nil {
		out.ReplicasPerCell = override.ReplicasPerCell
	}
	if override.Storage.Size != "" {
		out.Storage.Size = override.Storage.Size
	}
	if override.Storage.Class != "" {
		out.Storage.Class = override.Storage.Class
	}
	if len(override.Storage.AccessModes) > 0 {
		out.Storage.AccessModes = override.Storage.AccessModes
	}
	// Safety: Use DeepCopy to avoid sharing pointers to maps within ResourceRequirements
	if !isResourcesZero(override.Postgres.Resources) {
		out.Postgres.Resources = *override.Postgres.Resources.DeepCopy()
	}
	if !isResourcesZero(override.Multipooler.Resources) {
		out.Multipooler.Resources = *override.Multipooler.Resources.DeepCopy()
	}
	if override.Affinity != nil {
		out.Affinity = override.Affinity.DeepCopy()
	}
	if len(override.Tolerations) > 0 {
		out.Tolerations = override.Tolerations
	}
	if override.PVCDeletionPolicy != nil {
		out.PVCDeletionPolicy = override.PVCDeletionPolicy
	}
	return out
}

func defaultPoolSpec(spec *multigresv1alpha1.PoolSpec) {
	if spec.ReplicasPerCell == nil {
		// Default to 3 replicas to satisfy multiorch's AT_LEAST_2 durability policy
		// requirement.
		// TODO: multiorch currently only supports AT_LEAST_2 or MULTI_CELL_AT_LEAST_2,
		// both of which require at least 2 replicas for quorum. Single-node
		// policies are not yet supported.
		spec.ReplicasPerCell = ptr.To(int32(3))
	}
	if spec.Storage.Size == "" {
		spec.Storage.Size = DefaultEtcdStorageSize
	}
	if isResourcesZero(spec.Postgres.Resources) {
		spec.Postgres.Resources = DefaultResourcesPostgres()
	}
	if isResourcesZero(spec.Multipooler.Resources) {
		spec.Multipooler.Resources = DefaultResourcesPooler()
	}
}

func defaultBackupConfig(cfg *multigresv1alpha1.BackupConfig) {
	if cfg.Type == "" {
		cfg.Type = multigresv1alpha1.BackupTypeFilesystem
	}

	if cfg.Type == multigresv1alpha1.BackupTypeFilesystem {
		if cfg.Filesystem == nil {
			cfg.Filesystem = &multigresv1alpha1.FilesystemBackupConfig{}
		}
		if cfg.Filesystem.Path == "" {
			cfg.Filesystem.Path = DefaultBackupPath
		}
		if cfg.Filesystem.Storage.Size == "" {
			cfg.Filesystem.Storage.Size = DefaultBackupStorageSize
		}
	}

	if cfg.Retention == nil {
		cfg.Retention = &multigresv1alpha1.RetentionPolicy{}
	}
	if cfg.Retention.FullCount == nil {
		cfg.Retention.FullCount = ptr.To(int32(4))
	}
	if cfg.Retention.DifferentialCount == nil {
		cfg.Retention.DifferentialCount = ptr.To(int32(1))
	}
}
