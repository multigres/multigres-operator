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
// +kubebuilder:rbac:groups=multigres.com,resources=toposervers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=toposervers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multigres.com,resources=toposervers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get

// ============================================================================
// TopoServer Spec (Read-only API)
// ============================================================================
//
// TopoServer is a child CR managed by MultigresCluster (Global) or Cell (Local).

// EndpointUrl is a string restricted to 2048 characters for strict validation budgeting.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=2048
type EndpointUrl string

// TopoServerSpec defines the desired state of TopoServer (Child CR).
// +kubebuilder:validation:XValidation:rule="has(self.etcd)",message="must specify 'etcd' configuration"
type TopoServerSpec struct {
	// Etcd defines the configuration if using Etcd.
	Etcd *EtcdSpec `json:"etcd,omitempty"`

	// PVCDeletionPolicy controls PVC lifecycle for etcd.
	// Inherited from MultigresCluster.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`

	// Placement defines optional scheduling settings for the etcd pods.
	// +optional
	Placement *TopoServerPlacementSpec `json:"placement,omitempty"`

	// TLS controls whether the controller issues a serving certificate for
	// this topology server. Inherited from MultigresCluster.
	// +optional
	TLS *TopoTLSConfig `json:"tls,omitempty"`
}

// ============================================================================
// CR Controller Status Specs
// ============================================================================

// TopoServerStatus defines the observed state of TopoServer.
type TopoServerStatus struct {
	// HealthCheckedAt is the completion time of the latest direct etcd health probe.
	// +optional
	HealthCheckedAt *metav1.Time `json:"healthCheckedAt,omitempty"`

	// ObservedGeneration is the most recent generation observed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the aggregated lifecycle state of the toposerver.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Message provides details about the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// ClientService is the name of the service for clients.
	// +optional
	ClientService string `json:"clientService,omitempty"`

	// PeerService is the name of the service for peers.
	// +optional
	PeerService string `json:"peerService,omitempty"`

	// EtcdMaintenance records a maintenance reservation before contacting etcd,
	// preventing overlapping defragmentation across reconciles and restarts.
	// +optional
	EtcdMaintenance *EtcdMaintenanceStatus `json:"etcdMaintenance,omitempty"`
}

// EtcdMaintenanceStatus tracks the most recent automatic defragmentation.
type EtcdMaintenanceStatus struct {
	// LastAttemptTime starts the one-hour minimum interval between members.
	LastAttemptTime metav1.Time `json:"lastAttemptTime"`
	// Endpoint identifies the member reserved for defragmentation.
	Endpoint string `json:"endpoint"`
	// InProgress remains true after an interrupted or uncertain operation until
	// all members pass health checks again. While true, pod rollouts are paused.
	InProgress bool `json:"inProgress"`
}

// ============================================================================
// TopoServer Component Specs
// ============================================================================
//
// These components are not directly used in the formation of this child CR,
// but they are used to configure the toposerver via the MultigresCluster

// EtcdSpec defines the configuration for a managed Etcd cluster.
type EtcdSpec struct {
	// Image is the Etcd container image.
	// +optional
	Image ImageRef `json:"image,omitempty"`

	// Replicas is the desired number of etcd members.
	// Immutable after cluster creation — etcd does not support dynamic member addition
	// with static bootstrap. Must be an odd number for proper quorum fault tolerance.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:XValidation:rule="self % 2 == 1",message="etcd replicas must be an odd number (1, 3, 5); even numbers provide no additional fault tolerance over the next lower odd number"
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Storage configuration for Etcd data.
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// Resources defines the compute resource requirements.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Maintenance configures MVCC history retention, backend quota, and optional
	// defragmentation. Applies only to operator-managed etcd. Omitted fields use
	// the defaults documented on EtcdMaintenanceConfig.
	// +optional
	Maintenance *EtcdMaintenanceConfig `json:"maintenance,omitempty"`

	// RootPath is the etcd prefix for this cluster.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	RootPath string `json:"rootPath,omitempty"`

	// PVCDeletionPolicy controls PVC lifecycle for etcd volumes.
	// Overrides GlobalTopoServerSpec and MultigresCluster settings.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`
}

// EtcdMaintenanceConfig controls storage maintenance independently of logical
// topology pruning. Defaults are applied at rendering time so template values
// remain inheritable. Changing compaction or quota rolls the etcd pods.
// +kubebuilder:validation:XValidation:rule="!has(self.autoCompactionRetention) || ((has(self.autoCompactionMode) && self.autoCompactionMode == 'revision') ? self.autoCompactionRetention.matches('^[1-9][0-9]{0,9}$') : self.autoCompactionRetention.matches('^([0-9]{1,6}h)?([0-9]{1,6}m)?([0-9]{1,6}s)?$'))",message="compaction retention must be a positive revision count in revision mode or a duration using h, m, s in periodic mode"
type EtcdMaintenanceConfig struct {
	// AutoCompactionMode defaults to periodic (time-based retention).
	// +kubebuilder:validation:Enum=periodic;revision
	// +optional
	AutoCompactionMode string `json:"autoCompactionMode,omitempty"`

	// AutoCompactionRetention defaults to 1h in periodic mode or 10000 in
	// revision mode. Periodic values must be positive durations (e.g. 30m, 1h).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:XValidation:rule="self.matches('[1-9]')",message="compaction retention must be positive"
	// +optional
	AutoCompactionRetention string `json:"autoCompactionRetention,omitempty"`

	// QuotaBackendBytes defaults to 2147483648 (2 GiB), preserving etcd's
	// existing default. This is a storage quota, not a memory limit: provision
	// memory for measured restore-time usage and disk for the backend and WAL.
	// Do not lower the quota below an existing backend's size.
	// +kubebuilder:validation:Minimum=1048576
	// +kubebuilder:validation:Maximum=8589934592
	// +optional
	QuotaBackendBytes *int64 `json:"quotaBackendBytes,omitempty"`

	// DefragmentationEnabled opts into hourly, health-gated maintenance of at
	// most one member. Requires at least three healthy members, no rollout,
	// and at least 100 MiB and 30% reclaimable space. Disabled by default.
	// +optional
	DefragmentationEnabled *bool `json:"defragmentationEnabled,omitempty"`
}

// GlobalTopoServerSpec defines the configuration for the global topology server.
// It can be either an inline Etcd spec, an External reference, or a Template reference.
// +kubebuilder:validation:XValidation:rule="[has(self.etcd), has(self.external), has(self.templateRef)].filter(x, x).size() == 1",message="must specify exactly one of 'etcd', 'external', or 'templateRef'"
type GlobalTopoServerSpec struct {
	// Etcd defines an inline managed Etcd cluster.
	// +optional
	Etcd *EtcdSpec `json:"etcd,omitempty"`

	// External defines connection details for an unmanaged, external topo server.
	// +optional
	External *ExternalTopoServerSpec `json:"external,omitempty"`

	// TemplateRef refers to a CoreTemplate to load configuration from.
	// +optional
	TemplateRef TemplateRef `json:"templateRef,omitempty"`

	// PVCDeletionPolicy controls PVC lifecycle for topology server.
	// Overrides MultigresCluster setting.
	// +optional
	PVCDeletionPolicy *PVCDeletionPolicy `json:"pvcDeletionPolicy,omitempty"`

	// Placement defines optional scheduling settings for the global topo server pods.
	// +optional
	Placement *TopoServerPlacementSpec `json:"placement,omitempty"`
}

// ExternalTopoServerSpec defines connection details for an external system.
type ExternalTopoServerSpec struct {
	// Endpoints is a list of client URLs.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=20
	// +kubebuilder:validation:XValidation:rule="self.all(x, x.matches('^https?://'))",message="endpoints must be valid URLs"
	// +listType=set
	Endpoints []EndpointUrl `json:"endpoints"`

	// Implementation is the topology implementation type (e.g., "etcd", "memory").
	// Defaults to "etcd" if not specified.
	// +optional
	Implementation string `json:"implementation,omitempty"`

	// CASecret is the name of the secret containing the CA certificate.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	CASecret string `json:"caSecret,omitempty"`

	// ClientCertSecret is the name of the secret containing the client cert/key.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ClientCertSecret string `json:"clientCertSecret,omitempty"`

	// RootPath is the etcd prefix for this cluster.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	RootPath string `json:"rootPath,omitempty"`
}

// LocalTopoServerSpec defines configuration for Cell-local topology.
// +kubebuilder:validation:XValidation:rule="has(self.etcd) || has(self.external)",message="must specify either 'etcd' or 'external'"
// +kubebuilder:validation:XValidation:rule="!(has(self.etcd) && has(self.external))",message="only one of 'etcd' or 'external' can be set"
type LocalTopoServerSpec struct {
	// Etcd defines an inline managed Etcd cluster.
	// +optional
	Etcd *EtcdSpec `json:"etcd,omitempty"`

	// External defines connection details for an unmanaged, external topo server.
	// +optional
	External *ExternalTopoServerSpec `json:"external,omitempty"`
}

// GlobalTopoServerRef defines a reference to the global topo server.
// Used by Cell, TableGroup, and Shard.
type GlobalTopoServerRef struct {
	// Address is the DNS address of the topology server.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Address string `json:"address"`

	// RootPath is the etcd prefix for this cluster.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	RootPath string `json:"rootPath"`

	// Implementation defines the client implementation (e.g. "etcd").
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Implementation string `json:"implementation"`

	// CASecret is the name of the Secret holding the CA bundle that verifies
	// the topology server's certificate. Empty when the topology connection
	// carries no TLS material.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	CASecret string `json:"caSecret,omitempty"`

	// ClientCertSecret is the name of the Secret holding the certificate and
	// key this cluster presents to the topology server. Empty when the
	// topology connection carries no TLS material.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ClientCertSecret string `json:"clientCertSecret,omitempty"`
}

// ============================================================================
// Kind Definition and registration
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Available')].status"

// TopoServer represents the topology server (etcd) that stores cluster metadata and routing state.
// +kubebuilder:resource:shortName=tps
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
type TopoServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TopoServerSpec   `json:"spec,omitempty"`
	Status TopoServerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TopoServerList contains a list of TopoServer
type TopoServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TopoServer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TopoServer{}, &TopoServerList{})
}
