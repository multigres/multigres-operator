/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ============================================================================
// RBAC Markers
// ============================================================================

// -- Standard CRD Permissions --
// +kubebuilder:rbac:groups=multigres.com,resources=multigresclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=multigresclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multigres.com,resources=multigresclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=multigres.com,resources=coretemplates;celltemplates;shardtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=multigres.com,resources=cells;tablegroups;toposervers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// -- Certificate Manager Permissions (ADDED) --
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations,resourceNames=multigres-operator-mutating-webhook-configuration,verbs=get;update;patch
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingwebhookconfigurations,resourceNames=multigres-operator-validating-webhook-configuration,verbs=get;update;patch
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=list;watch

// ============================================================================
// MultigresClusterSpec Spec (User-editable API)
// ============================================================================

// MultigresClusterSpec defines the desired state of MultigresCluster.
type MultigresClusterSpec struct {
	// Images defines the container images for all components in the cluster.
	// +optional
	Images ClusterImages `json:"images,omitempty"`

	// ImageUpdatePolicy controls when the cluster adopts a changed
	// operator-default image set. Explicit spec.images values are unaffected.
	// +optional
	ImageUpdatePolicy *ImageUpdatePolicy `json:"imageUpdatePolicy,omitempty"`

	// LogLevels configures the --log-level flag for each multigres data-plane component.
	// All fields default to "info" if not specified.
	// +optional
	LogLevels ComponentLogLevels `json:"logLevels,omitempty"`

	// TemplateDefaults defines the default templates to use for components
	// that do not have explicit specs.
	// +optional
	TemplateDefaults TemplateDefaults `json:"templateDefaults,omitempty"`

	// GlobalTopoServer defines the cluster-wide global topology server.
	// +optional
	GlobalTopoServer *GlobalTopoServerSpec `json:"globalTopoServer,omitempty"`

	// Multiadmin defines the configuration for the Multiadmin component.
	// +optional
	Multiadmin *MultiadminConfig `json:"multiadmin,omitempty"`

	// MultiadminWeb defines the configuration for the MultiadminWeb component.
	// +optional
	MultiadminWeb *MultiadminWebConfig `json:"multiadminWeb,omitempty"`

	// Cells defines the list of cells (failure domains) in the cluster.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=50
	// +kubebuilder:validation:XValidation:rule="oldSelf.all(c, c.name in self.map(x, x.name))",message="Cells cannot be removed or renamed (Append-Only)"
	Cells []CellConfig `json:"cells,omitempty"`

	// Databases defines the logical databases, table groups, and sharding.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=1
	// +kubebuilder:validation:XValidation:rule="self.all(db, db.name == 'postgres' && db.default == true)",message="in v1alpha1, only the single system database named 'postgres' (marked default: true) is supported"
	Databases []DatabaseConfig `json:"databases,omitempty"`

	// PVCDeletionPolicy controls PVC lifecycle management for all stateful components.
	// Defaults to Delete/Delete — PVC lifecycle is handled by the operator.
	// +optional
	// +kubebuilder:default={whenDeleted: "Delete", whenScaled: "Delete"}
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`

	// Observability configures OpenTelemetry for data-plane components.
	// When nil, data-plane pods inherit the operator's own OTEL environment
	// variables at reconcile time. Set fields to override or disable.
	// +optional
	Observability *ObservabilityConfig `json:"observability,omitempty"`

	// Backup configures the default backup settings for the entire cluster.
	// +optional
	Backup *BackupConfig `json:"backup,omitempty"`

	// ExternalGateway controls external exposure of the global multigateway Service.
	// When nil or enabled: false, the Service remains ClusterIP.
	// +optional
	ExternalGateway *ExternalGatewayConfig `json:"externalGateway,omitempty"`

	// ExternalAdminWeb controls external exposure of the global multiadmin-web Service.
	// When nil or enabled: false, the Service remains ClusterIP.
	// +optional
	ExternalAdminWeb *ExternalAdminWebConfig `json:"externalAdminWeb,omitempty"`

	// TopologyPruning controls whether stale topology entries are pruned.
	// Default: enabled (nil means pruning is on).
	// +optional
	TopologyPruning *TopologyPruningConfig `json:"topologyPruning,omitempty"`

	// DurabilityPolicy sets the default durability policy for all databases in the cluster.
	// This value is written to the topology and determines how multiorch enforces
	// synchronous replication acknowledgment during failover.
	//
	// Currently supported values (upstream multiorch):
	//   - "AT_LEAST_2": any 2 nodes must acknowledge writes (single-cell quorum)
	//   - "MULTI_CELL_AT_LEAST_2": any 2 nodes from different cells must acknowledge (cross-AZ quorum)
	//
	// Additional user-defined policies may be supported in future upstream releases.
	// Defaults to "AT_LEAST_2" if not set.
	// +optional
	DurabilityPolicy string `json:"durabilityPolicy,omitempty"`

	// PostgresSuperuser is the name of the Postgres superuser role used by pgctld
	// (during initdb) and by multipooler/multiadmin when connecting as admin.
	// Defaults to "postgres".
	// +optional
	// +kubebuilder:default="postgres"
	// +kubebuilder:validation:MaxLength=63
	PostgresSuperuser string `json:"postgresSuperuser,omitempty"`

	// PostgresPasswordSecretRef references a Secret containing the final
	// Postgres superuser password.
	PostgresPasswordSecretRef PostgresPasswordSecretRef `json:"postgresPasswordSecretRef"`

	// PostgresInitSecretsRef provides optional day-0 settings inherited by TableGroups and Shards.
	// +optional
	PostgresInitSecretsRef *PostgresInitSecretsRef `json:"postgresInitSecretsRef,omitempty"`

	// InternalTLS configures mutual TLS for communication between internal
	// Multigres components. When omitted or enabled is false, internal mTLS is disabled.
	// +optional
	InternalTLS *InternalTLSConfig `json:"internalTLS,omitempty"`

	// CertCommonName is the DNS name used as the Common Name and SAN for the
	// public PostgreSQL TLS certificate terminated by multigateway
	// (e.g., "db.abc123.supabase.red"). It does not enable internal component mTLS.
	// When set, the cluster controller creates a cert-manager Certificate resource
	// and the cell controller mounts the resulting TLS secret into multigateway pods.
	// When empty, public PostgreSQL connections use no TLS.
	// +optional
	CertCommonName string `json:"certCommonName,omitempty"`

	// IssuerName is the cert-manager ClusterIssuer used for TLS certificates
	// created by this cluster. When empty, defaults to the Supabase issuer.
	// +optional
	IssuerName string `json:"issuerName,omitempty"`
}

// ============================================================================
// Images Config Section Specs
// ============================================================================

// ClusterImages defines the container images for all components in the cluster.
type ClusterImages struct {
	// ImagePullPolicy overrides the default image pull policy.
	// +optional
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets is a list of references to secrets in the same namespace.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Component Images
	// +optional
	Multigateway ImageRef `json:"multigateway,omitempty"`
	// +optional
	Multiorch ImageRef `json:"multiorch,omitempty"`
	// +optional
	Multipooler ImageRef `json:"multipooler,omitempty"`
	// +optional
	Multiadmin ImageRef `json:"multiadmin,omitempty"`
	// +optional
	MultiadminWeb ImageRef `json:"multiadminWeb,omitempty"`
	// +optional
	Postgres ImageRef `json:"postgres,omitempty"`
}

// ImageUpdatePolicy controls when a cluster adopts a changed operator-default
// image set.
type ImageUpdatePolicy struct {
	// Strategy overrides the operator's --image-update-strategy for this
	// cluster: "immediate" adopts a changed default image set on the next
	// reconcile; "lazy" holds the current set until acknowledgedRevision
	// names the new revision. Empty inherits the operator's flag. This lets
	// individual clusters be frozen or advanced independently of the fleet.
	// +optional
	// +kubebuilder:validation:Enum=immediate;lazy
	Strategy string `json:"strategy,omitempty"`

	// AcknowledgedRevision authorizes the cluster to adopt the default image
	// set with this revision. Under the lazy update strategy, a cluster keeps
	// the set it is running until this field names the revision published in
	// status.images.availableRevision. A value matching no known revision has
	// no effect and is reported through the ImageRolloutPending condition.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern="^[0-9a-f]*$"
	AcknowledgedRevision string `json:"acknowledgedRevision,omitempty"`
}

// ComponentImages is a concrete image for every Multigres component. It
// describes a fully resolved image set.
type ComponentImages struct {
	// +optional
	Multigateway ImageRef `json:"multigateway,omitempty"`
	// +optional
	Multiorch ImageRef `json:"multiorch,omitempty"`
	// +optional
	Multipooler ImageRef `json:"multipooler,omitempty"`
	// +optional
	Multiadmin ImageRef `json:"multiadmin,omitempty"`
	// +optional
	MultiadminWeb ImageRef `json:"multiadminWeb,omitempty"`
	// +optional
	Postgres ImageRef `json:"postgres,omitempty"`
}

// ImageSource describes how the effective component images were selected.
// +kubebuilder:validation:Enum=explicit;defaults;mixed
type ImageSource string

const (
	// ImageSourceExplicit means every component image is explicitly pinned in
	// spec.images, so operator defaults do not participate.
	ImageSourceExplicit ImageSource = "explicit"

	// ImageSourceDefaults means every component image comes from the
	// operator-managed default set.
	ImageSourceDefaults ImageSource = "defaults"

	// ImageSourceMixed means spec.images pins some components while the
	// operator-managed default set supplies the rest.
	ImageSourceMixed ImageSource = "mixed"
)

// ImagesStatus reports the effective component images and, when defaults
// participate, the operator-default rollout state.
type ImagesStatus struct {
	// UpdateStrategy is the strategy in effect for this cluster:
	// spec.imageUpdatePolicy.strategy when set, otherwise the operator's
	// --image-update-strategy flag. It determines whether
	// spec.imageUpdatePolicy.acknowledgedRevision has any effect: "lazy"
	// consults it, "immediate" ignores it.
	// +optional
	UpdateStrategy string `json:"updateStrategy,omitempty"`

	// Effective is the complete image set currently selected for child
	// resources after explicit pins and operator defaults are combined.
	Effective ComponentImages `json:"effective"`

	// Source identifies whether effective images come from explicit spec pins,
	// operator defaults, or a mixture of both.
	Source ImageSource `json:"source"`

	// Applied is the operator-default image set committed for components that
	// are not explicitly set in spec.images. It is empty when source is
	// explicit because defaults do not participate.
	// +optional
	Applied ComponentImages `json:"applied,omitempty"`

	// AppliedRevision identifies the applied default image set.
	// +optional
	AppliedRevision string `json:"appliedRevision,omitempty"`

	// Available is the operator's current default image set. Under the lazy
	// update strategy, this is the set that AvailableRevision authorizes.
	// +optional
	Available ComponentImages `json:"available,omitempty"`

	// AvailableRevision identifies the operator's current default image set.
	// When it differs from appliedRevision, a new set is available but has not
	// been adopted yet; with the lazy update strategy, adoption waits for
	// spec.imageUpdatePolicy.acknowledgedRevision to be set to this value.
	// +optional
	AvailableRevision string `json:"availableRevision,omitempty"`
}

// ============================================================================
// Template Defaults Config Section Specs
// ============================================================================

// TemplateDefaults defines the default templates to use for components.
type TemplateDefaults struct {
	// CoreTemplate is the default template for global components.
	// +optional
	CoreTemplate TemplateRef `json:"coreTemplate,omitempty"`
	// CellTemplate is the default template for cells.
	// +optional
	CellTemplate TemplateRef `json:"cellTemplate,omitempty"`
	// ShardTemplate is the default template for shards.
	// +optional
	ShardTemplate TemplateRef `json:"shardTemplate,omitempty"`
}

// EffectiveCellTemplate returns ref with the cluster-level TemplateDefaults
// fallback applied (level 2 of the override chain). The reconcilers and the
// webhook dry-run must share this rule so they resolve the same template.
func (s *MultigresClusterSpec) EffectiveCellTemplate(ref TemplateRef) TemplateRef {
	if ref == "" {
		return s.TemplateDefaults.CellTemplate
	}
	return ref
}

// ============================================================================
// Multiadmin Config Section Specs
// ============================================================================

// MultiadminConfig defines the configuration for Multiadmin in the Cluster.
// It allows either an inline spec OR a reference to a CoreTemplate.
// +kubebuilder:validation:XValidation:rule="has(self.spec) || has(self.templateRef)",message="must specify either 'spec' or 'templateRef'"
// +kubebuilder:validation:XValidation:rule="!(has(self.spec) && has(self.templateRef))",message="cannot specify both 'spec' and 'templateRef'"
type MultiadminConfig struct {
	// Spec defines the inline configuration.
	// +optional
	Spec *StatelessSpec `json:"spec,omitempty"`

	// TemplateRef refers to a CoreTemplate to load configuration from.
	// +optional
	TemplateRef TemplateRef `json:"templateRef,omitempty"`

	// Placement defines optional scheduling settings for multiadmin pods.
	// Applies whether Spec or TemplateRef is used.
	// +optional
	Placement *PodPlacementSpec `json:"placement,omitempty"`
}

// MultiadminWebConfig defines the configuration for MultiadminWeb in the Cluster.
// It allows either an inline spec OR a reference to a CoreTemplate.
// +kubebuilder:validation:XValidation:rule="has(self.spec) || has(self.templateRef)",message="must specify either 'spec' or 'templateRef'"
// +kubebuilder:validation:XValidation:rule="!(has(self.spec) && has(self.templateRef))",message="cannot specify both 'spec' and 'templateRef'"
type MultiadminWebConfig struct {
	// Spec defines the inline configuration.
	// +optional
	Spec *StatelessSpec `json:"spec,omitempty"`

	// TemplateRef refers to a CoreTemplate to load configuration from.
	// +optional
	TemplateRef TemplateRef `json:"templateRef,omitempty"`
}

// ============================================================================
// Cell Config Section Specs
// ============================================================================

// CellConfig defines a cell in the cluster.
// +kubebuilder:validation:XValidation:rule="!(has(self.spec) && has(self.cellTemplate))",message="cannot specify both 'spec' and 'cellTemplate'"
// +kubebuilder:validation:XValidation:rule="!(has(self.spec) && has(self.overrides))",message="cannot specify both 'spec' and 'overrides'"
// +kubebuilder:validation:XValidation:rule="!(has(self.zoneId) && has(self.region))",message="cannot specify both 'zoneId' and 'region'"
// +kubebuilder:validation:XValidation:rule="has(self.zoneId) || has(self.region)",message="at least one of 'zoneId' or 'region' must be specified"
type CellConfig struct {
	// Name is the logical name of the cell.
	Name CellName `json:"name"`

	// ZoneID indicates the physical availability zone ID (e.g. use1-az1).
	// Zone IDs are consistent across AWS accounts, unlike zone names.
	// +optional
	ZoneID ZoneID `json:"zoneId,omitempty"`
	// Region indicates the physical region (mutually exclusive with zoneId via CEL validation).
	// +optional
	Region Region `json:"region,omitempty"`

	// Metadata is an opaque document describing this cell, copied verbatim into
	// the cell's topology record for administrative clients to read. Whoever
	// creates the cluster owns its contents; the operator does not interpret it.
	// +optional
	// +kubebuilder:validation:MaxLength=4096
	Metadata string `json:"metadata,omitempty"`

	// CellTemplate refers to a CellTemplate CR.
	// +optional
	CellTemplate TemplateRef `json:"cellTemplate,omitempty"`

	// Overrides are applied on top of the template.
	// +optional
	Overrides *CellOverrides `json:"overrides,omitempty"`

	// Spec defines the inline configuration if no template is used.
	// +optional
	Spec *CellInlineSpec `json:"spec,omitempty"`
}

// CellOverrides defines overrides for a CellTemplate.
type CellOverrides struct {
	// Multigateway overrides.
	// +optional
	Multigateway *MultigatewaySpec `json:"multigateway,omitempty"`

	// MultigatewayPlacement overrides.
	// +optional
	MultigatewayPlacement *PodPlacementSpec `json:"multigatewayPlacement,omitempty"`
}

// CellInlineSpec defines the inline configuration for a Cell.
type CellInlineSpec struct {
	// Multigateway configuration.
	// +optional
	Multigateway MultigatewaySpec `json:"multigateway,omitempty"`

	// MultigatewayPlacement defines optional scheduling settings for multigateway pods.
	// +optional
	MultigatewayPlacement *PodPlacementSpec `json:"multigatewayPlacement,omitempty"`

	// LocalTopoServer configuration (optional).
	// +optional
	LocalTopoServer *LocalTopoServerSpec `json:"localTopoServer,omitempty"`
}

// ============================================================================
// Database Config Section Specs
// ============================================================================

// DatabaseConfig defines a logical database.
type DatabaseConfig struct {
	// Name is the logical name of the database.
	Name DatabaseName `json:"name"`

	// Default indicates if this is the system default database.
	// +optional
	Default bool `json:"default,omitempty"`

	// TableGroups is a list of table groups.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:XValidation:rule="self.filter(x, has(x.default) && x.default).size() == 1",message="every database must have exactly one tablegroup marked as default"
	// +kubebuilder:validation:MaxItems=20
	TableGroups []TableGroupConfig `json:"tablegroups,omitempty"`

	// Backup overrides the global backup configuration for this specific database.
	// +optional
	Backup *BackupConfig `json:"backup,omitempty"`

	// DurabilityPolicy overrides the cluster-level durability policy for this database.
	// See MultigresClusterSpec.DurabilityPolicy for supported values.
	// +optional
	DurabilityPolicy string `json:"durabilityPolicy,omitempty"`
}

// TableGroupConfig defines a table group within a database.
// +kubebuilder:validation:XValidation:rule="!has(self.default) || !self.default || self.name == 'default'",message="the default tablegroup must be named 'default'"
type TableGroupConfig struct {
	// Name is the logical name of the table group.
	Name TableGroupName `json:"name"`

	// Default indicates if this is the default/unsharded group.
	// +optional
	Default bool `json:"default,omitempty"`

	// Shards defines the list of shards.
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=32
	Shards []ShardConfig `json:"shards,omitempty"`

	// Backup overrides the database backup configuration for this table group.
	// +optional
	Backup *BackupConfig `json:"backup,omitempty"`

	// PVCDeletionPolicy controls PVC lifecycle for shards in this table group.
	// Overrides MultigresCluster setting.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`
}

// ShardConfig defines a specific shard.
// +kubebuilder:validation:XValidation:rule="!(has(self.spec) && has(self.shardTemplate))",message="cannot specify both 'spec' and 'shardTemplate'"
// +kubebuilder:validation:XValidation:rule="!(has(self.spec) && has(self.overrides))",message="cannot specify both 'spec' and 'overrides'"
type ShardConfig struct {
	// Name is the identifier of the shard (e.g., "0", "1").
	// +kubebuilder:validation:XValidation:rule="self == '0-inf'",message="shardName must be strictly equal to '0-inf' in this version"
	Name ShardName `json:"name"`

	// ShardTemplate refers to a ShardTemplate CR.
	// +optional
	ShardTemplate TemplateRef `json:"shardTemplate,omitempty"`

	// Overrides are applied on top of the template.
	// +optional
	Overrides *ShardOverrides `json:"overrides,omitempty"`

	// Spec defines the inline configuration if no template is used.
	// +optional
	Spec *ShardInlineSpec `json:"spec,omitempty"`

	// Backup overrides the table group backup configuration for this specific shard.
	// +optional
	Backup *BackupConfig `json:"backup,omitempty"`
}

// ShardOverrides defines overrides for a ShardTemplate.
type ShardOverrides struct {
	// Multiorch overrides.
	// +optional
	Multiorch *MultiorchSpec `json:"multiorch,omitempty"`

	// InitdbArgs overrides the template's initdb arguments.
	// +optional
	InitdbArgs InitdbArgs `json:"initdbArgs,omitempty"`

	// PostgresConfigRef references a ConfigMap whose postgresql.conf lines are
	// merged into the operator-rendered config for this shard's pools. This field
	// is deprecated in favor of the inline PostgresConfig map; it remains
	// supported for backward compatibility and will be removed in a future
	// version.
	// +optional
	PostgresConfigRef *PostgresConfigRef `json:"postgresConfigRef,omitempty"`

	// PostgresConfig is a map of PostgreSQL parameter (GUC) names to string
	// values, merged into the operator-rendered postgresql.conf. It takes
	// precedence over PostgresConfigRef and the operator's built-in defaults.
	// +optional
	// +kubebuilder:validation:MaxProperties=200
	PostgresConfig map[string]string `json:"postgresConfig,omitempty"`

	// Pools overrides. Keyed by pool name.
	// +optional
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(key, size(key) <= 25)",message="pool names must be <= 25 chars"
	// +kubebuilder:validation:XValidation:rule="oldSelf.all(k, k in self)",message="Pools cannot be removed or renamed in this version (Append-Only)"
	Pools map[PoolName]PoolSpec `json:"pools,omitempty"`
}

// ShardInlineSpec defines the inline configuration for a Shard.
type ShardInlineSpec struct {
	// Multiorch configuration.
	// +optional
	Multiorch MultiorchSpec `json:"multiorch,omitempty"`

	// InitdbArgs specifies extra arguments passed to initdb during PostgreSQL
	// data directory initialization (e.g., "--locale-provider=icu --icu-locale=en_US.UTF-8").
	// Applied uniformly to all pool pods in this shard.
	// Only takes effect when a pod initializes a new data directory.
	// +optional
	InitdbArgs InitdbArgs `json:"initdbArgs,omitempty"`

	// PostgresConfigRef references a ConfigMap whose postgresql.conf lines are
	// merged into the operator-rendered config for this shard's pools. This field
	// is deprecated in favor of the inline PostgresConfig map; it remains
	// supported for backward compatibility and will be removed in a future
	// version.
	// +optional
	PostgresConfigRef *PostgresConfigRef `json:"postgresConfigRef,omitempty"`

	// PostgresConfig is a map of PostgreSQL parameter (GUC) names to string
	// values, merged into the operator-rendered postgresql.conf. It takes
	// precedence over PostgresConfigRef and the operator's built-in defaults.
	// +optional
	// +kubebuilder:validation:MaxProperties=200
	PostgresConfig map[string]string `json:"postgresConfig,omitempty"`

	// Pools configuration. Keyed by pool name.
	// +optional
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(key, size(key) <= 25)",message="pool names must be <= 25 chars"
	// +kubebuilder:validation:XValidation:rule="oldSelf.all(k, k in self)",message="Pools cannot be removed or renamed in this version (Append-Only)"
	Pools map[PoolName]PoolSpec `json:"pools,omitempty"`

	// PVCDeletionPolicy controls PVC lifecycle for pools in this shard.
	// Overrides TableGroup and MultigresCluster settings.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`
}

// ============================================================================
// External Gateway Configuration
// ============================================================================

// ExternalGatewayConfig controls external exposure of the global multigateway Service.
//
// Annotations under the multigres.com/ prefix are rejected to avoid
// conflicts with operator-managed metadata.
// +kubebuilder:validation:XValidation:rule="!has(self.annotations) || self.annotations.all(k, !k.startsWith('multigres.com/'))",message="annotations must not use multigres.com/ prefix (reserved for operator)"
type ExternalGatewayConfig struct {
	// Enabled controls whether external gateway exposure is enabled.
	// The global Service remains ClusterIP; external reachability is provided
	// via explicitly assigned external IPs and platform networking.
	Enabled bool `json:"enabled"`

	// ExternalIPs are externally routable addresses assigned to the global
	// multigateway Service. These are surfaced via Service.spec.externalIPs.
	// +optional
	// +kubebuilder:validation:MaxItems=10
	ExternalIPs []IPAddress `json:"externalIPs,omitempty"`

	// Annotations are applied to the global multigateway Service metadata.
	// The operator owns exactly these annotation keys via SSA field ownership.
	// +optional
	// +kubebuilder:validation:MaxProperties=20
	Annotations map[string]string `json:"annotations,omitempty"`
}

// GatewayStatus reports the external gateway state.
type GatewayStatus struct {
	// ExternalEndpoint is the hostname or IP for the external gateway endpoint,
	// sourced from externalIPs or load balancer ingress (in that priority).
	// Empty when no external endpoint has been provisioned.
	ExternalEndpoint string `json:"externalEndpoint"`
}

const (
	// ConditionGatewayExternalReady indicates whether the external endpoint
	// is provisioned and backed by ready gateway pods.
	ConditionGatewayExternalReady = "GatewayExternalReady"

	// ReasonAwaitingEndpoint indicates the service endpoint has not
	// yet been provisioned.
	ReasonAwaitingEndpoint = "AwaitingEndpoint"

	// ReasonNoReadyGateways indicates the external endpoint is assigned but
	// no multigateway pods are ready.
	ReasonNoReadyGateways = "NoReadyGateways"

	// ReasonEndpointReady indicates the external endpoint is serving traffic.
	ReasonEndpointReady = "EndpointReady"

	// ConditionImageRolloutPending indicates whether a newer operator-default
	// image set is available that this cluster has not adopted yet.
	ConditionImageRolloutPending = "ImageRolloutPending"

	// ReasonAwaitingAcknowledgement indicates a new default image set is
	// available and the cluster is holding its current set until
	// spec.imageUpdatePolicy.acknowledgedRevision names the new revision.
	ReasonAwaitingAcknowledgement = "AwaitingAcknowledgement"

	// ReasonRevisionMismatch indicates the acknowledged revision does not
	// match the available revision, so it has no effect.
	ReasonRevisionMismatch = "RevisionMismatch"

	// ReasonImagesUpToDate indicates the cluster runs the operator's current
	// default image set.
	ReasonImagesUpToDate = "UpToDate"

	// ReasonFullyPinned indicates every component image is explicitly set and
	// operator-default rollout state does not apply.
	ReasonFullyPinned = "FullyPinned"
)

// ============================================================================
// External Admin Web Configuration
// ============================================================================

// ExternalAdminWebConfig controls external exposure of the global multiadmin-web Service.
//
// Annotations under the multigres.com/ prefix are rejected to avoid
// conflicts with operator-managed metadata.
// +kubebuilder:validation:XValidation:rule="!has(self.annotations) || self.annotations.all(k, !k.startsWith('multigres.com/'))",message="annotations must not use multigres.com/ prefix (reserved for operator)"
type ExternalAdminWebConfig struct {
	// Enabled controls whether external admin web exposure is enabled.
	// The global Service remains ClusterIP; external reachability is provided
	// via explicitly assigned external IPs and platform networking.
	Enabled bool `json:"enabled"`

	// ExternalIPs are externally routable addresses assigned to the global
	// multiadmin-web Service. These are surfaced via Service.spec.externalIPs.
	// +optional
	// +kubebuilder:validation:MaxItems=10
	ExternalIPs []IPAddress `json:"externalIPs,omitempty"`

	// Annotations are applied to the global multiadmin-web Service metadata.
	// The operator owns exactly these annotation keys via SSA field ownership.
	// +optional
	// +kubebuilder:validation:MaxProperties=20
	Annotations map[string]string `json:"annotations,omitempty"`
}

// AdminWebStatus reports the external admin web state.
type AdminWebStatus struct {
	// ExternalEndpoint is the hostname or IP for the external admin web endpoint,
	// sourced from externalIPs or load balancer ingress (in that priority).
	// Empty when no external endpoint has been provisioned.
	ExternalEndpoint string `json:"externalEndpoint"`
}

const (
	// ConditionAdminWebExternalReady indicates whether the external endpoint
	// is provisioned and backed by ready multiadmin-web pods.
	ConditionAdminWebExternalReady = "AdminWebExternalReady"

	// ReasonNoReadyAdminWeb indicates the external endpoint is assigned but
	// no multiadmin-web pods are ready.
	ReasonNoReadyAdminWeb = "NoReadyAdminWeb"
)

// ============================================================================
// CR Controller Status Specs
// ============================================================================

// MultigresClusterStatus defines the observed state.
type MultigresClusterStatus struct {
	// ObservedGeneration is the most recent generation observed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the aggregated lifecycle state of the cluster.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Message provides details about the current phase (e.g. error messages).
	// +optional
	Message string `json:"message,omitempty"`

	// Cells status summary.
	// +optional
	// +kubebuilder:validation:MaxProperties=50
	Cells map[CellName]CellStatusSummary `json:"cells,omitempty"`

	// Gateway reports the external gateway state.
	// Nil when spec.externalGateway is nil or disabled.
	// +optional
	Gateway *GatewayStatus `json:"gateway,omitempty"`

	// AdminWeb reports the external admin web state.
	// Nil when spec.externalAdminWeb is nil or disabled.
	// +optional
	AdminWeb *AdminWebStatus `json:"adminWeb,omitempty"`

	// Databases status summary.
	// +optional
	// +kubebuilder:validation:MaxProperties=50
	Databases map[DatabaseName]DatabaseStatusSummary `json:"databases,omitempty"`

	// ResolvedTemplates records which templates were resolved during the last
	// successful reconciliation. Used for targeted template-change enqueuing.
	// +optional
	ResolvedTemplates *ResolvedTemplates `json:"resolvedTemplates,omitempty"`

	// Images reports the effective component images and operator-default
	// rollout state when defaults participate.
	// +optional
	Images *ImagesStatus `json:"images,omitempty"`
}

// ResolvedTemplates tracks the template names resolved during reconciliation.
type ResolvedTemplates struct {
	// CoreTemplates is the deduplicated set of CoreTemplate names resolved.
	// +optional
	CoreTemplates []TemplateRef `json:"coreTemplates,omitempty"`

	// CellTemplates is the deduplicated set of CellTemplate names resolved.
	// +optional
	CellTemplates []TemplateRef `json:"cellTemplates,omitempty"`

	// ShardTemplates is the deduplicated set of ShardTemplate names resolved.
	// +optional
	ShardTemplates []TemplateRef `json:"shardTemplates,omitempty"`
}

// CellStatusSummary provides a high-level status of a cell.
type CellStatusSummary struct {
	Ready           bool  `json:"ready"`
	GatewayReplicas int32 `json:"gatewayReplicas"`
}

// DatabaseStatusSummary provides a high-level status of a database.
type DatabaseStatusSummary struct {
	ReadyShards int32 `json:"readyShards"`
	TotalShards int32 `json:"totalShards"`
}

// ============================================================================
// Kind Definition and registration
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Available",type="string",JSONPath=".status.conditions[?(@.type=='Available')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// MultigresCluster represents a distributed database cluster managed by the operator.
// +kubebuilder:resource:shortName=mgc
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Images",type="string",JSONPath=".status.images.appliedRevision",priority=1
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 25",message="MultigresCluster name must be at most 25 characters"
type MultigresCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MultigresClusterSpec   `json:"spec,omitempty"`
	Status MultigresClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MultigresClusterList contains a list of MultigresCluster
type MultigresClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MultigresCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MultigresCluster{}, &MultigresClusterList{})
}
