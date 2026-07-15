package metadata

import (
	"maps"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// Standard Kubernetes label keys following kubernetes.io conventions.
//
// See: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
const (
	// LabelAppName is the standard label key for the application name.
	LabelAppName = "app.kubernetes.io/name"

	// LabelAppInstance is the standard label key for the unique instance name.
	LabelAppInstance = "app.kubernetes.io/instance"

	// LabelAppVersion is the standard label key for the application version.
	LabelAppVersion = "app.kubernetes.io/version"

	// LabelAppComponent is the standard label key for the component within the
	// application.
	LabelAppComponent = "app.kubernetes.io/component"

	// LabelAppPartOf is the standard label key for the name of a higher level
	// application this one is part of.
	LabelAppPartOf = "app.kubernetes.io/part-of"

	// LabelAppManagedBy is the standard label key for the tool managing the
	// resource.
	LabelAppManagedBy = "app.kubernetes.io/managed-by"
)

const (
	// AppNameMultigres is the fixed application name for all Multigres resources.
	AppNameMultigres = "multigres"

	// ManagedByMultigres identifies the operator managing these resources.
	ManagedByMultigres = "multigres-operator"
)

const (
	// ComponentMultiadmin identifies the multiadmin component.
	ComponentMultiadmin = "multiadmin"

	// ComponentMultiadminWeb identifies the multiadmin-web component.
	ComponentMultiadminWeb = "multiadmin-web"

	// ComponentMultigateway identifies the multigateway component.
	ComponentMultigateway = "multigateway"

	// ComponentGlobalTopo identifies the global-topo component.
	ComponentGlobalTopo = "global-topo"

	// ComponentCell identifies the cell component.
	ComponentCell = "cell"

	// ComponentShard identifies the shard component.
	ComponentShard = "shard"

	// ComponentTableGroup identifies the table group component.
	ComponentTableGroup = "tablegroup"
)

const (
	// LabelMultigresCell identifies which cell a resource belongs to.
	LabelMultigresCell = "multigres.com/cell"

	// LabelMultigresCluster identifies which cluster a resource belongs to.
	LabelMultigresCluster = "multigres.com/cluster"

	// LabelMultigresShard identifies which shard a resource belongs to.
	LabelMultigresShard = "multigres.com/shard"

	// LabelMultigresDatabase identifies which database a resource belongs to.
	LabelMultigresDatabase = "multigres.com/database"

	// LabelMultigresTableGroup identifies which table group a resource belongs to.
	LabelMultigresTableGroup = "multigres.com/tablegroup"

	// LabelMultigresPool identifies which pool a resource belongs to.
	LabelMultigresPool = "multigres.com/pool"

	// LabelMultigresZoneID identifies which zone ID a resource belongs to.
	LabelMultigresZoneID = "multigres.com/zone-id"

	// NodeLabelZoneID is the AWS node label key for the availability zone ID.
	// Zone IDs (e.g. use1-az1) are consistent across accounts, unlike zone names.
	NodeLabelZoneID = "topology.k8s.aws/zone-id"

	// LabelMultigresRegion identifies which region a resource belongs to.
	LabelMultigresRegion = "multigres.com/region"

	// DefaultCellName is the default cell name when none is specified.
	DefaultCellName = "multigres-global-topo"

	// AnnotationSpecHash stores the hash of operator-managed pod spec fields.
	AnnotationSpecHash = "multigres.com/spec-hash"

	// AnnotationPostgresConfigHash stores the SHA-256 hash of the referenced
	// postgres config ConfigMap data. Changes to the ConfigMap content produce
	// a different hash, which changes the spec-hash and triggers a rolling update.
	AnnotationPostgresConfigHash = "multigres.com/postgres-config-hash"

	// AnnotationDrainState is used to coordinate graceful scale down between
	// the resource-handler (Kubernetes) and data-handler (etcd).
	AnnotationDrainState = "drain.multigres.com/state"

	// DrainStateRequested indicates the resource-handler wants this pod removed.
	DrainStateRequested = "requested"
	// DrainStateDraining indicates the data-handler is removing the pod from synchronous standby.
	DrainStateDraining = "draining"
	// DrainStateAcknowledged indicates the data-handler verified removal from synchronous standby.
	DrainStateAcknowledged = "acknowledged"
	// DrainStateReadyForDeletion indicates the data-handler unregistered the pod from etcd.
	DrainStateReadyForDeletion = "ready-for-deletion"

	// AnnotationDrainRequestedAt stores the RFC3339 timestamp of when the drain
	// was first requested. Used to detect failover timeouts.
	AnnotationDrainRequestedAt = "drain.multigres.com/requested-at"

	// LabelPodRole reflects the pod's topology role (e.g. "DRAINED").
	// Set by the resource-handler when the topology store reports a notable role.
	// Used as the durable signal for DRAINED PVC cleanup, since PodRoles may be
	// cleared by the data-handler during drain before cleanup runs.
	LabelPodRole = "multigres.com/role"

	// LabelOrphan marks a resource as orphaned (no longer referenced by an owner)
	// and eligible for garbage collection. Value is the UTC timestamp when the
	// resource became orphaned, formatted as OrphanTimestampFormat.
	LabelOrphan = "multigres.com/orphan-since"

	// OrphanTimestampFormat is the layout used for LabelOrphan values (RFC 3339).
	OrphanTimestampFormat = "2006-01-02T15-04-05Z"
)

const (
	// LabelMultigresCellTemplate identifies which cell template was used for
	// the given resource. This is needed for the operator to efficiently find
	// all the relevant resources using the CellTemplate.
	LabelMultigresCellTemplate = "multigres.com/cell-template"

	// LabelMultigresShardTemplate identifies which shard template was used for
	// the given resource. This is needed for the operator to efficiently find
	// all the relevant resources using the ShardTemplate.
	LabelMultigresShardTemplate = "multigres.com/shard-template"
)

const (
	// LabelUsesCoreTemplate is set to "true" on clusters that reference any CoreTemplate.
	LabelUsesCoreTemplate = "multigres.com/uses-core-template"

	// LabelUsesCellTemplate is set to "true" on clusters that reference any CellTemplate.
	LabelUsesCellTemplate = "multigres.com/uses-cell-template"

	// LabelUsesShardTemplate is set to "true" on clusters that reference any ShardTemplate.
	LabelUsesShardTemplate = "multigres.com/uses-shard-template"
)

// BuildStandardLabels returns a map of standard kubernetes labels.
// clusterName should be the name of the MultigresCluster CR (used for instance label).
// component is the name of the component (e.g. multigateway, multiorch, pool).
func BuildStandardLabels(clusterName, component string) map[string]string {
	return map[string]string{
		LabelAppName:      AppNameMultigres,
		LabelAppInstance:  clusterName,
		LabelAppComponent: component,
		LabelAppPartOf:    AppNameMultigres,
		LabelAppManagedBy: ManagedByMultigres,
	}
}

// AddCellLabel adds the cell label to the provided labels map.
func AddCellLabel(labels map[string]string, cellName multigresv1alpha1.CellName) map[string]string {
	labels[LabelMultigresCell] = string(cellName)
	return labels
}

// AddClusterLabel adds the cluster label to the provided labels map.
func AddClusterLabel(labels map[string]string, clusterName string) map[string]string {
	labels[LabelMultigresCluster] = clusterName
	return labels
}

// AddShardLabel adds the shard label to the provided labels map.
func AddShardLabel(
	labels map[string]string,
	shardName multigresv1alpha1.ShardName,
) map[string]string {
	labels[LabelMultigresShard] = string(shardName)
	return labels
}

// AddDatabaseLabel adds the database label to the provided labels map.
func AddDatabaseLabel(
	labels map[string]string,
	databaseName multigresv1alpha1.DatabaseName,
) map[string]string {
	labels[LabelMultigresDatabase] = string(databaseName)
	return labels
}

// AddTableGroupLabel adds the table group label to the provided labels map.
func AddTableGroupLabel(
	labels map[string]string,
	tableGroupName multigresv1alpha1.TableGroupName,
) map[string]string {
	labels[LabelMultigresTableGroup] = string(tableGroupName)
	return labels
}

// AddPoolLabel adds the pool label to the provided labels map.
func AddPoolLabel(
	labels map[string]string,
	poolName multigresv1alpha1.PoolName,
) map[string]string {
	labels[LabelMultigresPool] = string(poolName)
	return labels
}

// AddZoneIDLabel adds the zone ID label to the provided labels map.
func AddZoneIDLabel(
	labels map[string]string,
	zoneID multigresv1alpha1.ZoneID,
) map[string]string {
	labels[LabelMultigresZoneID] = string(zoneID)
	return labels
}

// AddRegionLabel adds the region label to the provided labels map.
func AddRegionLabel(
	labels map[string]string,
	regionName multigresv1alpha1.Region,
) map[string]string {
	labels[LabelMultigresRegion] = string(regionName)
	return labels
}

// selectorLabelsAllowList contains the keys that are allowed in label selectors.
// These must be stable identity labels, not mutable metadata.
var selectorLabelsAllowList = map[string]bool{
	LabelAppComponent:        true,
	LabelAppInstance:         true,
	LabelMultigresCluster:    true,
	LabelMultigresDatabase:   true,
	LabelMultigresTableGroup: true,
	LabelMultigresShard:      true,
	LabelMultigresPool:       true,
	LabelMultigresCell:       true,
}

// GetSelectorLabels filters the provided labels map to return only those keys
// allowed in resource selectors (Identity Labels).
//
// This separates stable identity labels from mutable metadata labels like
// versions or location tags, ensuring that changes to mutable metadata do not
// trigger unnecessary recreation of immutable resources (like StatefulSets).
func GetSelectorLabels(labels map[string]string) map[string]string {
	selectorLabels := make(map[string]string)
	for k, v := range labels {
		if selectorLabelsAllowList[k] {
			selectorLabels[k] = v
		}
	}
	return selectorLabels
}

// MergeLabels merges custom labels with standard labels.
//
// Note that standard labels take precedence over custom labels to prevent users
// from overriding critical operator-managed labels.
func MergeLabels(standardLabels, customLabels map[string]string) map[string]string {
	merged := make(map[string]string)

	// Copy custom labels first (if provided)
	maps.Copy(merged, customLabels)

	// Copy standard labels (overwriting any duplicates from custom)
	maps.Copy(merged, standardLabels)

	return merged
}
