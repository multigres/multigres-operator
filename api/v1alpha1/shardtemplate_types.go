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
// ShardTemplateSpec Spec
// ============================================================================

// ShardTemplateSpec defines reusable config for Shard components (Multiorch, Pools).
type ShardTemplateSpec struct {
	// Multiorch configuration.
	// +optional
	Multiorch *MultiorchSpec `json:"multiorch,omitempty"`

	// InitdbArgs specifies extra arguments passed to initdb during PostgreSQL
	// data directory initialization (e.g., "--locale-provider=icu --icu-locale=en_US.UTF-8").
	// Applied to all pool pods in shards using this template.
	// Only takes effect when a pod initializes a new data directory.
	// +optional
	InitdbArgs InitdbArgs `json:"initdbArgs,omitempty"`

	// PostgresConfigRef references a ConfigMap containing extra postgresql.conf
	// lines appended to pgctld's auto-tuned defaults. The ConfigMap must exist in
	// the same namespace. When set, the operator mounts it and sets
	// POSTGRES_INITDB_EXTRA_CONF on pgctld.
	// +optional
	PostgresConfigRef *PostgresConfigRef `json:"postgresConfigRef,omitempty"`

	// +optional
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(key, size(key) < 63)",message="pool names must be < 63 chars"
	// +kubebuilder:validation:XValidation:rule="oldSelf.all(k, k in self)",message="Pools cannot be removed or renamed in this version (Append-Only)"
	Pools map[PoolName]PoolSpec `json:"pools,omitempty"`

	// PVCDeletionPolicy controls PVC lifecycle management for this shard template.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`
}

// ============================================================================
// Kind Definition and registration
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced

// ShardTemplate provides reusable shard-level configuration (pools, storage, resources) for MultigresCluster shards.
// +kubebuilder:resource:shortName=sht
type ShardTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ShardTemplateSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// ShardTemplateList contains a list of ShardTemplate
type ShardTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShardTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ShardTemplate{}, &ShardTemplateList{})
}
