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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ============================================================================
// RBAC Markers
// ============================================================================
//
// +kubebuilder:rbac:groups=multigres.com,resources=tablegroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=tablegroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multigres.com,resources=shards,verbs=get;list;watch;create;update;patch;delete

// ============================================================================
// TableGroup Spec (Read-only API)
// ============================================================================
//
// TableGroup is a child CR managed by MultigresCluster.
// It acts as the middle-manager for Shards, holding fully resolved specs.

// TableGroupSpec defines the desired state of TableGroup.
type TableGroupSpec struct {
	// DatabaseName is the name of the logical database.
	DatabaseName DatabaseName `json:"databaseName"`

	// TableGroupName is the name of this table group.
	TableGroupName TableGroupName `json:"tableGroupName"`

	// IsDefault indicates if this is the default/unsharded group for the database.
	// +optional
	IsDefault bool `json:"default,omitempty"`

	// Images defines the container images used for child shards - defined globally in MultigresCluster.
	Images ShardImages `json:"images"`

	// GlobalTopoServer is a reference to the global topology server.
	GlobalTopoServer GlobalTopoServerRef `json:"globalTopoServer"`

	// Shards is the list of FULLY RESOLVED shard specifications.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=32
	Shards []ShardResolvedSpec `json:"shards"`

	// PVCDeletionPolicy controls PVC lifecycle for all shards in this table group.
	// Inherited from MultigresCluster.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`

	// Observability configures OpenTelemetry for shard-level data-plane components.
	// Inherited from MultigresCluster.Spec.Observability.
	// +optional
	Observability *ObservabilityConfig `json:"observability,omitempty"`

	// LogLevels is the resolved log level configuration for shard-level components.
	// Inherited from MultigresCluster.Spec.LogLevels.
	// +optional
	LogLevels ComponentLogLevels `json:"logLevels,omitempty"`

	// Backup is the backup configuration inherited from MultigresCluster -> Database -> TableGroup.
	// +optional
	Backup *BackupConfig `json:"backup,omitempty"`

	// CellTopologyLabels maps cell names to their topology nodeSelector labels.
	// Propagated from the cluster's cell configs.
	// +optional
	CellTopologyLabels map[CellName]map[string]string `json:"cellTopologyLabels,omitempty"`

	// TopologyPruning controls whether stale topology entries are pruned.
	// Inherited from MultigresCluster.
	// +optional
	TopologyPruning *TopologyPruningConfig `json:"topologyPruning,omitempty"`

	// DurabilityPolicy is the resolved durability policy for this database.
	// Inherited from MultigresCluster → Database.
	// +optional
	DurabilityPolicy string `json:"durabilityPolicy,omitempty"`

	// PostgresSuperuser is the resolved Postgres superuser name.
	// Inherited from MultigresCluster.Spec.PostgresSuperuser.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	PostgresSuperuser string `json:"postgresSuperuser,omitempty"`

	// PostgresPasswordSecretRef is inherited from the MultigresCluster.
	PostgresPasswordSecretRef PostgresPasswordSecretRef `json:"postgresPasswordSecretRef"`
}

// ShardResolvedSpec represents the fully calculated spec for a shard,
// pushed down to the TableGroup.
type ShardResolvedSpec struct {
	// Name is the identifier of the shard.
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Multiorch is the fully resolved configuration for the orchestrator.
	Multiorch MultiorchSpec `json:"multiorch"`

	// InitdbArgs is the resolved initdb arguments for this shard.
	// +optional
	InitdbArgs InitdbArgs `json:"initdbArgs,omitempty"`

	// PostgresConfigRef references a ConfigMap containing extra postgresql.conf
	// lines appended to pgctld's auto-tuned defaults. When set, the operator
	// mounts it and sets POSTGRES_INITDB_EXTRA_CONF on pgctld.
	// +optional
	PostgresConfigRef *PostgresConfigRef `json:"postgresConfigRef,omitempty"`

	// Pools is the map of fully resolved data pool configurations.
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(key, size(key) < 63)",message="pool names must be < 63 chars"
	Pools map[PoolName]PoolSpec `json:"pools"`

	// PVCDeletionPolicy controls PVC lifecycle for pools in this shard.
	// Overrides TableGroup and MultigresCluster settings.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`

	// Backup is the backup configuration inherited from MultigresCluster.
	// +optional
	Backup *BackupConfig `json:"backup,omitempty"`
}

// ============================================================================
// CR Controller Status Specs
// ============================================================================

// TableGroupStatus defines the observed state of TableGroup.
type TableGroupStatus struct {
	// ObservedGeneration is the most recent generation observed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the aggregated lifecycle state of the table group.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Message provides details about the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// ReadyShards is the number of shards that have reached the Healthy phase.
	ReadyShards int32 `json:"readyShards"`
	// TotalShards is the total number of shards declared for this table group.
	TotalShards int32 `json:"totalShards"`
}

// ============================================================================
// Kind Definition and registration
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Shards",type="integer",JSONPath=".status.readyShards"

// TableGroup represents a logical grouping of tables that are sharded together within a database.
// +kubebuilder:resource:shortName=tbg
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
type TableGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TableGroupSpec   `json:"spec,omitempty"`
	Status TableGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TableGroupList contains a list of TableGroup
type TableGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TableGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TableGroup{}, &TableGroupList{})
}
