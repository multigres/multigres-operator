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
//
// +kubebuilder:rbac:groups=multigres.com,resources=shards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=shards/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multigres.com,resources=shards/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// ============================================================================
// Shard Component Specs (Reusable)
// ============================================================================
//
// These specs define the specific components of a shard (Orchestration and Data Pools).
// They are used by ShardTemplate, TableGroup, and the Shard Child CR.

// MultiorchSpec defines the configuration specifically for Multiorch,
// which requires placement logic (cell targeting).
type MultiorchSpec struct {
	StatelessSpec `json:",inline"`

	// Placement defines optional scheduling settings for multiorch pods.
	// +optional
	Placement *PodPlacementSpec `json:"placement,omitempty"`

	// Cells defines the list of cells where this Multiorch should be deployed.
	// If empty, it defaults to all cells where pools are defined.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=50
	Cells []CellName `json:"cells,omitempty"`
}

// PoolSpec defines the configuration for a data pool (StatefulSet).
type PoolSpec struct {
	// Type of the pool. Currently only "readWrite" is supported.
	// +kubebuilder:validation:Enum=readWrite
	// +optional
	Type PoolType `json:"type,omitempty"`

	// Cells defines the list of cells where this Pool should be deployed.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=10
	// +kubebuilder:validation:XValidation:rule="oldSelf.all(c, c in self)",message="Cells cannot be removed from a pool (Append-Only)"
	Cells []CellName `json:"cells,omitempty"`

	// ReplicasPerCell is the desired number of pool pods per selected cell.
	// The default and minimum is 1.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=32
	// +optional
	ReplicasPerCell *int32 `json:"replicasPerCell,omitempty"`

	// Storage defines the storage configuration for the pool's data volumes.
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// Postgres container configuration.
	// +optional
	Postgres ContainerConfig `json:"postgres,omitempty"`

	// Multipooler container configuration.
	// +optional
	Multipooler ContainerConfig `json:"multipooler,omitempty"`

	// Affinity defines the pod's scheduling constraints.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations defines the pod tolerations for scheduling onto tainted nodes.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// FSGroup sets the pod-level fsGroup for shared volume and socket access across
	// pool containers. When unset, runtime defaults apply.
	// +optional
	// +kubebuilder:validation:Minimum=1
	FSGroup *int64 `json:"fsGroup,omitempty"`

	// PVCDeletionPolicy controls PVC lifecycle for this pool.
	// Overrides Shard, TableGroup, and MultigresCluster settings.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`
}

// ============================================================================
// Shard Spec (Read-only API)
// ============================================================================
//
// Shard is a child CR managed by TableGroup.
// Represents a single logical shard with its orchestration and pools.

// ShardSpec defines the desired state of Shard.
type ShardSpec struct {
	// DatabaseName is the name of the logical database this shard belongs to.
	DatabaseName DatabaseName `json:"databaseName"`

	// TableGroupName is the name of the table group this shard belongs to.
	TableGroupName TableGroupName `json:"tableGroupName"`

	// ShardName is the specific identifier for this shard (e.g. "0").
	ShardName ShardName `json:"shardName"`

	// Images defines the container images to be used by this shard (defined globally at MultigresCluster).
	Images ShardImages `json:"images"`

	// GlobalTopoServer is a reference to the global topology server.
	GlobalTopoServer GlobalTopoServerRef `json:"globalTopoServer"`

	// Multiorch is the fully resolved configuration for the shard orchestrator.
	Multiorch MultiorchSpec `json:"multiorch"`

	// InitdbArgs specifies extra arguments for initdb, propagated from the cluster spec.
	// +optional
	InitdbArgs InitdbArgs `json:"initdbArgs,omitempty"`

	// PostgresConfigRef references a ConfigMap containing extra postgresql.conf
	// lines appended to pgctld's auto-tuned defaults. The operator mounts it and
	// sets POSTGRES_INITDB_EXTRA_CONF on pgctld; PostgreSQL's last-write-wins
	// rule lets you override specific params without replacing the whole config.
	// +optional
	PostgresConfigRef *PostgresConfigRef `json:"postgresConfigRef,omitempty"`

	// Pools is the map of fully resolved data pool configurations.
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(key, size(key) < 63)",message="pool names must be < 63 chars"
	Pools map[PoolName]PoolSpec `json:"pools"`

	// PVCDeletionPolicy controls PVC lifecycle for this shard's pools.
	// Inherited from MultigresCluster.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`

	// Observability configures OpenTelemetry for shard-level data-plane components.
	// Inherited from MultigresCluster.Spec.Observability by the resolver.
	// +optional
	Observability *ObservabilityConfig `json:"observability,omitempty"`

	// LogLevels is the resolved log level configuration for shard-level components.
	// Inherited from MultigresCluster.Spec.LogLevels.
	// +optional
	LogLevels ComponentLogLevels `json:"logLevels,omitempty"`

	// Backup is the fully resolved backup configuration for this shard.
	// Inherited from MultigresCluster.Spec.Backup by the resolver.
	// +optional
	Backup *BackupConfig `json:"backup,omitempty"`

	// TopologyPruning controls whether stale topology entries are pruned.
	// Inherited from MultigresCluster.
	// +optional
	TopologyPruning *TopologyPruningConfig `json:"topologyPruning,omitempty"`

	// DurabilityPolicy is the resolved durability policy for this shard's database.
	// Inherited from MultigresCluster → Database.
	// +optional
	DurabilityPolicy string `json:"durabilityPolicy,omitempty"`

	// PostgresSuperuser is the resolved Postgres superuser name propagated from
	// MultigresCluster. Used to populate POSTGRES_USER on pgctld and multipooler.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	PostgresSuperuser string `json:"postgresSuperuser,omitempty"`

	// PostgresPasswordSecretRef is the resolved Postgres superuser password
	// Secret.
	PostgresPasswordSecretRef PostgresPasswordSecretRef `json:"postgresPasswordSecretRef"`

	// CellTopologyLabels maps cell names to their topology nodeSelector labels.
	// Each entry is a map like {"topology.kubernetes.io/zone": "us-east-1a"}.
	// Propagated from the cluster's cell configs so the shard controller
	// can inject nodeSelector without looking up Cell CRs.
	// +optional
	CellTopologyLabels map[CellName]map[string]string `json:"cellTopologyLabels,omitempty"`

	// Replicas is the total number of desired replicas across all pools in this shard.
	// This field is used by the scale subresource to support HPA and PDBs.
	// It is automatically populated by the cluster-handler based on the sum of ReplicasPerCell.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
}

// ShardImages defines the images required for a Shard.
type ShardImages struct {
	// ImagePullPolicy overrides the default image pull policy.
	// +optional
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets is a list of references to secrets in the same namespace.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Multiorch is the image for the shard orchestrator.
	Multiorch ImageRef `json:"multiorch"`

	// Multipooler is the image for the connection pooler sidecar.
	Multipooler ImageRef `json:"multipooler"`

	// Postgres is the image for the postgres database.
	Postgres ImageRef `json:"postgres"`
}

// ============================================================================
// CR Controller Status Specs
// ============================================================================

// ShardStatus defines the observed state of Shard.
type ShardStatus struct {
	// ObservedGeneration is the most recent generation observed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the aggregated lifecycle state of the shard.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Message provides details about the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// Cells is a list of cells this shard is currently deployed to.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=50
	Cells []CellName `json:"cells,omitempty"`

	// OrchReady indicates if the Multiorch component is ready.
	OrchReady bool `json:"orchReady"`

	// PoolsReady indicates if all data pools are ready.
	PoolsReady bool `json:"poolsReady"`

	// LastBackupTime is the timestamp of the most recent completed backup.
	// +optional
	LastBackupTime *metav1.Time `json:"lastBackupTime,omitempty"`

	// LastBackupType is the type of the most recent completed backup (full, diff, incr).
	// +optional
	// +kubebuilder:validation:Enum=full;diff;incr
	LastBackupType string `json:"lastBackupType,omitempty"`

	// PodRoles maps pod names to their database roles (e.g. PRIMARY, REPLICA, DRAINED).
	// +optional
	PodRoles map[string]string `json:"podRoles,omitempty"`

	// ReadyReplicas is the total number of ready pods across all pools in this shard.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
}

// ============================================================================
// Kind Definition and registration
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Available')].status"

// Shard represents a horizontal partition of a table group, managing pool pods and data-plane components.
// +kubebuilder:resource:shortName=srd
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.readyReplicas
type Shard struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ShardSpec   `json:"spec,omitempty"`
	Status ShardStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ShardList contains a list of Shard
type ShardList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Shard `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Shard{}, &ShardList{})
}
