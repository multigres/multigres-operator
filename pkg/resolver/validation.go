package resolver

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// ============================================================================
// Template Reference Validation
// ============================================================================

// ValidateCoreTemplateReference checks if a CoreTemplate reference is valid.
// It returns nil if:
// 1. The name is empty (no reference).
// 2. The name matches the FallbackCoreTemplate (assumed to be a system default or implicitly allowed).
// 3. The referenced CoreTemplate exists in the Resolver's namespace.
// Otherwise, it returns an error.
func (r *Resolver) ValidateCoreTemplateReference(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) error {
	if name == "" {
		return nil
	}

	exists, err := r.CoreTemplateExists(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf(
			"referenced CoreTemplate '%s' not found in namespace '%s'",
			name,
			r.Namespace,
		)
	}
	return nil
}

// CoreTemplateExists checks if a CoreTemplate with the given name exists in the current namespace.
func (r *Resolver) CoreTemplateExists(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) (bool, error) {
	if name == "" {
		return false, nil
	}
	tpl := &multigresv1alpha1.CoreTemplate{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: string(name), Namespace: r.Namespace}, tpl)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ValidateCellTemplateReference checks if a CellTemplate reference is valid.
// See ValidateCoreTemplateReference for logic details.
func (r *Resolver) ValidateCellTemplateReference(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) error {
	if name == "" {
		return nil
	}

	exists, err := r.CellTemplateExists(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf(
			"referenced CellTemplate '%s' not found in namespace '%s'",
			name,
			r.Namespace,
		)
	}
	return nil
}

// CellTemplateExists checks if a CellTemplate with the given name exists in the current namespace.
func (r *Resolver) CellTemplateExists(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) (bool, error) {
	if name == "" {
		return false, nil
	}
	tpl := &multigresv1alpha1.CellTemplate{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: string(name), Namespace: r.Namespace}, tpl)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ValidateShardTemplateReference checks if a ShardTemplate reference is valid.
// See ValidateCoreTemplateReference for logic details.
func (r *Resolver) ValidateShardTemplateReference(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) error {
	if name == "" {
		return nil
	}

	exists, err := r.ShardTemplateExists(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf(
			"referenced ShardTemplate '%s' not found in namespace '%s'",
			name,
			r.Namespace,
		)
	}
	return nil
}

// ShardTemplateExists checks if a ShardTemplate with the given name exists in the current namespace.
func (r *Resolver) ShardTemplateExists(
	ctx context.Context,
	name multigresv1alpha1.TemplateRef,
) (bool, error) {
	if name == "" {
		return false, nil
	}
	tpl := &multigresv1alpha1.ShardTemplate{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: string(name), Namespace: r.Namespace}, tpl)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ============================================================================
// Cluster Validation
// ============================================================================

// ValidateClusterIntegrity checks that all templates referenced by the cluster actually exist.
// This corresponds to the Level 4 Referential Integrity check.
//
// This method is primarily used by the Validating Webhook to reject clusters with broken references.
func (r *Resolver) ValidateClusterIntegrity(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	// 1. Validate Core Templates
	if err := r.ValidateCoreTemplateReference(
		ctx,
		cluster.Spec.TemplateDefaults.CoreTemplate,
	); err != nil {
		return err
	}
	if cluster.Spec.Multiadmin != nil && cluster.Spec.Multiadmin.TemplateRef != "" {
		if err := r.ValidateCoreTemplateReference(
			ctx,
			cluster.Spec.Multiadmin.TemplateRef,
		); err != nil {
			return err
		}
	}
	if cluster.Spec.GlobalTopoServer != nil && cluster.Spec.GlobalTopoServer.TemplateRef != "" {
		if err := r.ValidateCoreTemplateReference(
			ctx,
			cluster.Spec.GlobalTopoServer.TemplateRef,
		); err != nil {
			return err
		}
	}
	if cluster.Spec.MultiadminWeb != nil && cluster.Spec.MultiadminWeb.TemplateRef != "" {
		if err := r.ValidateCoreTemplateReference(
			ctx,
			cluster.Spec.MultiadminWeb.TemplateRef,
		); err != nil {
			return err
		}
	}

	// 2. Validate Cell Templates
	if err := r.ValidateCellTemplateReference(
		ctx,
		cluster.Spec.TemplateDefaults.CellTemplate,
	); err != nil {
		return err
	}
	for _, cell := range cluster.Spec.Cells {
		if err := r.ValidateCellTemplateReference(ctx, cell.CellTemplate); err != nil {
			return err
		}
	}

	// 3. Validate Shard Templates
	if err := r.ValidateShardTemplateReference(
		ctx,
		cluster.Spec.TemplateDefaults.ShardTemplate,
	); err != nil {
		return err
	}
	for _, db := range cluster.Spec.Databases {
		for _, tg := range db.TableGroups {
			for _, shard := range tg.Shards {
				if err := r.ValidateShardTemplateReference(ctx, shard.ShardTemplate); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// ValidateClusterLogic performs deep logic checks and strict validation on the cluster configuration.
// It simulates the resolution process to identify broken references, empty cells, or invalid overrides.
//
// This method is primarily used by the Validating Webhook to enforce logical correctness.
func (r *Resolver) ValidateClusterLogic(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (admission.Warnings, error) {
	var warnings admission.Warnings

	// Extract all valid cell names for this cluster
	var cellNames []multigresv1alpha1.CellName
	validCells := make(map[multigresv1alpha1.CellName]bool)
	for _, c := range cluster.Spec.Cells {
		cellNames = append(cellNames, c.Name)
		validCells[c.Name] = true
	}

	// ------------------------------------------------------------------
	// 0. Cell Topology Validation (Warnings Only)
	// ------------------------------------------------------------------
	topologyWarnings := r.validateCellTopology(ctx, cluster.Spec.Cells)
	warnings = append(warnings, topologyWarnings...)

	// ------------------------------------------------------------------
	// 0b. Etcd Replicas Warning
	// ------------------------------------------------------------------
	if etcdReplicas := getEffectiveEtcdReplicas(cluster); etcdReplicas == 1 {
		warnings = append(warnings, fmt.Sprintf(
			"etcd has 1 replica — no fault tolerance. "+
				"3 replicas is recommended for production (tolerates 1 failure). "+
				"Note: topology server replicas cannot be changed after cluster creation",
		))
	}

	// ------------------------------------------------------------------
	// 0c. External Gateway Validation
	// ------------------------------------------------------------------
	if gw := cluster.Spec.ExternalGateway; gw != nil && gw.Enabled {
		if len(gw.ExternalIPs) == 0 {
			warnings = append(warnings,
				"externalGateway is enabled but no externalIPs specified; "+
					"endpoint resolution depends on an external load balancer controller "+
					"provisioning an ingress address on the global multigateway Service")
		}
		for _, ip := range gw.ExternalIPs {
			if net.ParseIP(string(ip)) == nil {
				return nil, fmt.Errorf(
					"externalGateway.externalIPs contains invalid IP address: %q", ip,
				)
			}
		}
	}

	// ------------------------------------------------------------------
	// 0d. External Admin Web Validation
	// ------------------------------------------------------------------
	if aw := cluster.Spec.ExternalAdminWeb; aw != nil && aw.Enabled {
		if len(aw.ExternalIPs) == 0 {
			warnings = append(warnings,
				"externalAdminWeb is enabled but no externalIPs specified; "+
					"endpoint resolution depends on an external load balancer controller "+
					"provisioning an ingress address on the multiadmin-web Service")
		}
		for _, ip := range aw.ExternalIPs {
			if net.ParseIP(string(ip)) == nil {
				return nil, fmt.Errorf(
					"externalAdminWeb.externalIPs contains invalid IP address: %q", ip,
				)
			}
		}
	}

	// ------------------------------------------------------------------
	// 0e. Resource Limits Validation (Top-Level Components)
	// ------------------------------------------------------------------
	if cluster.Spec.GlobalTopoServer != nil &&
		cluster.Spec.GlobalTopoServer.Etcd != nil {
		if err := validateResourceRequirements(
			cluster.Spec.GlobalTopoServer.Etcd.Resources, "etcd",
		); err != nil {
			return nil, err
		}
	}
	if cluster.Spec.Multiadmin != nil && cluster.Spec.Multiadmin.Spec != nil {
		if err := validateResourceRequirements(
			cluster.Spec.Multiadmin.Spec.Resources, "multiadmin",
		); err != nil {
			return nil, err
		}
	}
	if cluster.Spec.MultiadminWeb != nil && cluster.Spec.MultiadminWeb.Spec != nil {
		if err := validateResourceRequirements(
			cluster.Spec.MultiadminWeb.Spec.Resources, "multiadmin-web",
		); err != nil {
			return nil, err
		}
	}
	for _, cell := range cluster.Spec.Cells {
		if cell.Spec != nil {
			if err := validateResourceRequirements(
				cell.Spec.Multigateway.Resources,
				fmt.Sprintf("cell '%s' multigateway", cell.Name),
			); err != nil {
				return nil, err
			}
		}
	}

	// Iterate through every Shard and "Simulate" Resolution
	for _, db := range cluster.Spec.Databases {
		dbBackup := multigresv1alpha1.MergeBackupConfig(db.Backup, cluster.Spec.Backup)
		for _, tg := range db.TableGroups {
			tgBackup := multigresv1alpha1.MergeBackupConfig(tg.Backup, dbBackup)
			for _, shard := range tg.Shards {
				// Propagate global ShardTemplate default for accurate validation
				if shard.ShardTemplate == "" && cluster.Spec.TemplateDefaults.ShardTemplate != "" {
					shard.ShardTemplate = cluster.Spec.TemplateDefaults.ShardTemplate
				}

				// ------------------------------------------------------------------
				// 1. Orphan Override Check
				// ------------------------------------------------------------------
				if shard.Overrides != nil && len(shard.Overrides.Pools) > 0 {
					// We must resolve the template to know what pools *should* exist.
					// Pass empty string if ShardTemplate is empty to resolve default/implicit.

					tpl, err := r.ResolveShardTemplate(ctx, shard.ShardTemplate)
					if err != nil {
						// This should have been caught by ValidateClusterIntegrity, but handling it safe.
						return nil, fmt.Errorf(
							"failed to resolve template for orphan check: %w",
							err,
						)
					}

					if tpl != nil {
						for poolName := range shard.Overrides.Pools {
							if _, exists := tpl.Spec.Pools[poolName]; !exists {
								warnings = append(warnings, fmt.Sprintf(
									"Pool '%s' defined in overrides for shard '%s' does not exist in template '%s'. A new pool will be created.",
									poolName,
									shard.Name,
									tpl.Name,
								))
							}
						}
					}
				}

				// ------------------------------------------------------------------
				// 2. Logic Resolution
				// ------------------------------------------------------------------
				// Dry-Run Resolution
				// We pass allCellNames just like the Reconciler would, to simulate the final state
				orch, pools, _, backupCfg, _, _, err := r.ResolveShard(
					ctx,
					&shard,
					ResolveShardOptions{
						AllCellNames:            cellNames,
						InheritedBackup:         tgBackup,
						MaterializeCellDefaults: true,
					},
				)
				if err != nil {
					return nil, fmt.Errorf(
						"validation failed: cannot resolve shard '%s': %w",
						shard.Name,
						err,
					)
				}

				// Pool Name Format: CRD structural schema does not enforce
				// validation markers on map keys, so validate explicitly.
				for poolName := range pools {
					if err := ValidatePoolName(poolName); err != nil {
						return nil, fmt.Errorf("shard '%s': %w", shard.Name, err)
					}
				}

				// Check 1: Empty Cells (Orphaned Shard)
				// If after resolution (and defaulting), cells are STILL empty, it's a broken config.
				if len(orch.Cells) == 0 {
					return nil, fmt.Errorf(
						"shard '%s' matches NO cells (check your cell names or template configuration)",
						shard.Name,
					)
				}

				for poolName, pool := range pools {
					// Check 1b: Empty Pool cells
					if len(pool.Cells) == 0 {
						return nil, fmt.Errorf(
							"pool '%s' in shard '%s' matches NO cells",
							poolName,
							shard.Name,
						)
					}
				}

				// Check 2: Invalid Cells (Reference Validity)
				for _, c := range orch.Cells {
					if !validCells[multigresv1alpha1.CellName(c)] {
						return nil, fmt.Errorf(
							"shard '%s' is assigned to non-existent cell '%s'",
							shard.Name,
							c,
						)
					}
				}

				for poolName, pool := range pools {
					// Check 2b: Invalid Pool cells
					for _, c := range pool.Cells {
						if !validCells[multigresv1alpha1.CellName(c)] {
							return nil, fmt.Errorf(
								"pool '%s' in shard '%s' is assigned to non-existent cell '%s'",
								poolName,
								shard.Name,
								c,
							)
						}
					}
				}

				// ------------------------------------------------------------------
				// 3. Durability achievability warnings
				// ------------------------------------------------------------------
				for poolName, pool := range pools {
					durabilityPolicy := effectiveDatabaseDurabilityPolicy(db, cluster)
					if durabilityPolicy == "" {
						durabilityPolicy = DefaultDurabilityPolicy
					}
					replicas := DefaultPoolReplicasPerCell
					if pool.ReplicasPerCell != nil {
						replicas = *pool.ReplicasPerCell
					}
					cellCount := len(pool.Cells)
					if cellCount == 0 {
						cellCount = len(cellNames)
					}
					totalReplicas := int(replicas) * cellCount

					switch durabilityPolicy {
					case "MULTI_CELL_AT_LEAST_2":
						if cellCount >= 2 {
							continue
						}
						warnings = append(warnings, fmt.Sprintf(
							"pool '%s' in shard '%s' spans %d cell(s); "+
								"durability policy %q requires at least %d distinct cells.",
							poolName,
							shard.Name,
							cellCount,
							durabilityPolicy,
							2,
						))

					default:
						if totalReplicas >= 2 {
							continue
						}
						warnings = append(warnings, fmt.Sprintf(
							"pool '%s' in shard '%s' has replicasPerCell=%d across %d cell(s) (%d total); "+
								"durability policy %q requires at least %d total poolers.",
							poolName,
							shard.Name,
							replicas,
							cellCount,
							totalReplicas,
							durabilityPolicy,
							2,
						))
					}
				}

				// ------------------------------------------------------------------
				// 3b. Resource Limits Validation (Resolved Shard Components)
				// ------------------------------------------------------------------
				if err := validateResourceRequirements(
					orch.Resources,
					fmt.Sprintf("shard '%s' multiorch", shard.Name),
				); err != nil {
					return nil, err
				}
				for poolName, pool := range pools {
					if err := validateResourceRequirements(
						pool.Postgres.Resources,
						fmt.Sprintf("shard '%s' pool '%s' postgres", shard.Name, poolName),
					); err != nil {
						return nil, err
					}
					if err := validateResourceRequirements(
						pool.Multipooler.Resources,
						fmt.Sprintf("shard '%s' pool '%s' multipooler", shard.Name, poolName),
					); err != nil {
						return nil, err
					}
				}

				// ------------------------------------------------------------------
				// 4. Backup Validation
				// ------------------------------------------------------------------
				if backupCfg != nil && backupCfg.Type == multigresv1alpha1.BackupTypeFilesystem {
					// Check if any pool has >1 replicas and we are using RWO
					isRWO := true // Default is RWO

					if backupCfg.Filesystem != nil &&
						len(backupCfg.Filesystem.Storage.AccessModes) > 0 {
						for _, mode := range backupCfg.Filesystem.Storage.AccessModes {
							if mode == corev1.ReadWriteMany {
								isRWO = false
								break
							}
						}
					}

					if isRWO {
						for poolName, pool := range pools {
							replicas := int32(1)
							if pool.ReplicasPerCell != nil {
								replicas = *pool.ReplicasPerCell
							}
							if replicas > 1 {
								warnings = append(warnings, fmt.Sprintf(
									"Shard '%s' uses filesystem backups with ReadWriteOnce (RWO) storage but pool '%s' has %d replicas per cell. "+
										"This configuration may fail if pods are scheduled on different nodes. "+
										"Consider using ReadWriteMany (RWX), ensuring node affinity, or using S3 backups.",
									shard.Name,
									poolName,
									replicas,
								))
							}
						}
					}
				}
			}
		}
	}

	// ------------------------------------------------------------------
	// 5. StorageClass Validation
	// ------------------------------------------------------------------
	// Collect all PVC-generating components that have no explicit storage class.
	// If any exist, verify a default StorageClass is available in the cluster.
	var missingClass []string

	// Check global topo server etcd storage
	if cluster.Spec.GlobalTopoServer != nil &&
		cluster.Spec.GlobalTopoServer.Etcd != nil &&
		cluster.Spec.GlobalTopoServer.Etcd.Storage.Class == "" {
		missingClass = append(missingClass,
			"spec.globalTopoServer.etcd.storage.class")
	}

	// Check all resolved pool and backup storage
	for _, db := range cluster.Spec.Databases {
		dbBackup := multigresv1alpha1.MergeBackupConfig(db.Backup, cluster.Spec.Backup)
		for _, tg := range db.TableGroups {
			tgBackup := multigresv1alpha1.MergeBackupConfig(tg.Backup, dbBackup)
			for _, shard := range tg.Shards {
				_, pools, _, backupCfg, _, _, err := r.ResolveShard(
					ctx,
					&shard,
					ResolveShardOptions{
						AllCellNames:            cellNames,
						InheritedBackup:         tgBackup,
						MaterializeCellDefaults: true,
					},
				)
				if err != nil {
					continue // already validated in section 2
				}

				for poolName, pool := range pools {
					if pool.Storage.Class == "" {
						missingClass = append(missingClass, fmt.Sprintf(
							"shard '%s' pool '%s' storage.class",
							shard.Name, poolName))
					}
				}

				if backupCfg != nil &&
					backupCfg.Type == multigresv1alpha1.BackupTypeFilesystem &&
					backupCfg.Filesystem != nil &&
					backupCfg.Filesystem.Storage.Class == "" {
					missingClass = append(missingClass, fmt.Sprintf(
						"shard '%s' backup filesystem storage.class",
						shard.Name))
				}
			}
		}
	}

	if len(missingClass) > 0 {
		hasDefault, err := r.hasDefaultStorageClass(ctx)
		if err != nil {
			return warnings, fmt.Errorf("failed to check for default StorageClass: %w", err)
		}
		if !hasDefault {
			return nil, fmt.Errorf(
				"no default StorageClass found in the cluster, and no explicit storage class is set for: %s. "+
					"PVCs will be stuck in Pending. Either: "+
					"(1) Set a default StorageClass: kubectl annotate sc <name> storageclass.kubernetes.io/is-default-class=true, or "+
					"(2) Set the storage class explicitly in the MultigresCluster spec for each component listed above",
				strings.Join(missingClass, ", "),
			)
		}
	}

	return warnings, nil
}

// hasDefaultStorageClass checks if the cluster has at least one StorageClass
// annotated as the default.
func (r *Resolver) hasDefaultStorageClass(ctx context.Context) (bool, error) {
	var scList storagev1.StorageClassList
	if err := r.Client.List(ctx, &scList); err != nil {
		return false, err
	}
	for i := range scList.Items {
		if scList.Items[i].Annotations["storageclass.kubernetes.io/is-default-class"] == "true" ||
			scList.Items[i].Annotations["storageclass.beta.kubernetes.io/is-default-class"] == "true" {
			return true, nil
		}
	}
	return false, nil
}

// getEffectiveEtcdReplicas returns the etcd replica count for a cluster,
// resolving nil fields to the default (3). Returns 0 if the cluster uses
// an external topology server (no managed etcd).
func getEffectiveEtcdReplicas(cluster *multigresv1alpha1.MultigresCluster) int32 {
	if cluster.Spec.GlobalTopoServer != nil && cluster.Spec.GlobalTopoServer.External != nil {
		return 0
	}
	if cluster.Spec.GlobalTopoServer != nil &&
		cluster.Spec.GlobalTopoServer.Etcd != nil &&
		cluster.Spec.GlobalTopoServer.Etcd.Replicas != nil {
		return *cluster.Spec.GlobalTopoServer.Etcd.Replicas
	}
	return DefaultEtcdReplicas
}

func effectiveDatabaseDurabilityPolicy(
	db multigresv1alpha1.DatabaseConfig,
	cluster *multigresv1alpha1.MultigresCluster,
) string {
	return multigresv1alpha1.MergeDurabilityPolicy(
		db.DurabilityPolicy,
		cluster.Spec.DurabilityPolicy,
	)
}

// dnsLabelRegex matches valid DNS labels per RFC 1123. This mirrors the
// kubebuilder Pattern marker on PoolName, which is not enforced for map keys.
var dnsLabelRegex = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const poolNameMaxLen = 25

// ValidatePoolName checks that a pool name is a valid DNS label within length
// limits. CRD structural schema does not enforce validation markers on map
// keys, so pool names must be validated explicitly.
func ValidatePoolName(name multigresv1alpha1.PoolName) error {
	s := string(name)
	if len(s) == 0 || len(s) > poolNameMaxLen {
		return fmt.Errorf(
			"pool name '%s' must be between 1 and %d characters",
			name, poolNameMaxLen,
		)
	}
	if !dnsLabelRegex.MatchString(s) {
		return fmt.Errorf(
			"pool name '%s' is invalid: "+
				"must consist of lowercase alphanumeric characters or '-', "+
				"and must start and end with an alphanumeric character",
			name,
		)
	}
	return nil
}

// validateResourceRequirements checks that for every resource type present in
// both Limits and Requests, the limit is >= the request. Kubernetes enforces
// this on Pods but NOT on CRDs, so we catch it early in the webhook.
func validateResourceRequirements(resources corev1.ResourceRequirements, component string) error {
	for resourceName, limit := range resources.Limits {
		if request, ok := resources.Requests[resourceName]; ok {
			if limit.Cmp(request) < 0 {
				return fmt.Errorf(
					"%s: resource %s limit (%s) must be >= request (%s)",
					component,
					resourceName,
					limit.String(),
					request.String(),
				)
			}
		}
	}
	return nil
}

// validateCellTopology checks if nodes exist in the cluster matching each cell's
// zone or region topology label. Returns warnings (not errors) for any cells
// whose topology labels don't match any nodes — pods targeting those cells will
// remain Pending until matching nodes appear.
func (r *Resolver) validateCellTopology(
	ctx context.Context,
	cells []multigresv1alpha1.CellConfig,
) admission.Warnings {
	var warnings admission.Warnings

	// Collect unique topology checks needed
	type topoCheck struct {
		key   string
		value string
		cells []string
	}
	checks := make(map[string]*topoCheck)
	for _, cell := range cells {
		var key, value string
		switch {
		case cell.ZoneID != "":
			key = metadata.NodeLabelZoneID
			value = string(cell.ZoneID)
		case cell.Region != "":
			key = "topology.kubernetes.io/region"
			value = string(cell.Region)
		default:
			continue
		}
		k := key + "=" + value
		if tc, ok := checks[k]; ok {
			tc.cells = append(tc.cells, string(cell.Name))
		} else {
			checks[k] = &topoCheck{
				key:   key,
				value: value,
				cells: []string{string(cell.Name)},
			}
		}
	}

	if len(checks) == 0 {
		return nil
	}

	// List all nodes once
	var nodeList corev1.NodeList
	if err := r.Client.List(ctx, &nodeList); err != nil {
		// If we can't list nodes, skip topology validation silently.
		// RBAC might not be configured yet.
		return nil
	}

	// Build a set of available topology values
	availableZoneIDs := make(map[string]bool)
	availableRegions := make(map[string]bool)
	for i := range nodeList.Items {
		labels := nodeList.Items[i].Labels
		if zoneID, ok := labels[metadata.NodeLabelZoneID]; ok {
			availableZoneIDs[zoneID] = true
		}
		if reg, ok := labels["topology.kubernetes.io/region"]; ok {
			availableRegions[reg] = true
		}
	}

	for _, tc := range checks {
		var found bool
		switch tc.key {
		case metadata.NodeLabelZoneID:
			found = availableZoneIDs[tc.value]
		case "topology.kubernetes.io/region":
			found = availableRegions[tc.value]
		}
		if !found {
			for _, cellName := range tc.cells {
				warnings = append(warnings, fmt.Sprintf(
					"cell '%s': no nodes currently match %s=%s; pods will be Pending until matching nodes are available",
					cellName,
					tc.key,
					tc.value,
				))
			}
		}
	}

	return warnings
}
