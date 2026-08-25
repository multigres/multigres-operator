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
// +kubebuilder:rbac:groups=multigres.com,resources=cells,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=cells/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multigres.com,resources=cells/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

// ============================================================================
// Cell Component Specs (Reusable)
// ============================================================================
//
// These specs are used by CellTemplate, MultigresCluster, and the Cell child CR.

// MultigatewaySpec defines the configuration specifically for Multigateway,
// which adds gateway-only settings (failover request buffering) on top of the
// shared stateless spec.
type MultigatewaySpec struct {
	StatelessSpec `json:",inline"`

	// Buffer configures request buffering during planned failovers.
	// +optional
	Buffer *GatewayBufferConfig `json:"buffer,omitempty"`
}

// GatewayBufferConfig controls multigateway request buffering during planned
// failovers. While a shard has no serving primary, the gateway holds incoming
// requests and drains them once the new primary serves, masking the failover
// from clients instead of surfacing errors.
//
// Fields left unset emit no flag, so the multigateway binary's own defaults
// apply.
type GatewayBufferConfig struct {
	// Enabled controls failover buffering (--buffer-enabled).
	// The resolver defaults it to true when building Cells from a
	// MultigresCluster: the operator provisions HA clusters where transparent
	// planned failover is expected. On a hand-written Cell CR the field stays
	// as authored (unset emits no flag; the binary default applies).
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Window is the per-request buffering cap (--buffer-window); requests
	// buffered longer than this fail. Binary default: 10s.
	// Sizing note: on a 3-pooler test cluster the gateway-perceived failover
	// duration averaged ~8.3s (consensus plus topology propagation) with a
	// longer tail, so the 10s default can still evict a request or two per
	// planned failover; 20s masked all of them.
	// +optional
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|μs|ms|s|m|h))+$`
	Window *metav1.Duration `json:"window,omitempty"`

	// MaxFailoverDuration is the session-level cap on how long one failover
	// may keep requests buffered (--buffer-max-failover-duration). Must be
	// >= window. Binary default: 20s.
	// +optional
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|μs|ms|s|m|h))+$`
	MaxFailoverDuration *metav1.Duration `json:"maxFailoverDuration,omitempty"`

	// MinTimeBetweenFailovers is the minimum interval between two buffering
	// events for the same shard (--buffer-min-time-between-failovers).
	// Binary default: 1m.
	// +optional
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|μs|ms|s|m|h))+$`
	MinTimeBetweenFailovers *metav1.Duration `json:"minTimeBetweenFailovers,omitempty"`

	// Size is the maximum number of concurrently buffered requests
	// (--buffer-size). Binary default: 1000.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Size *int32 `json:"size,omitempty"`

	// DrainConcurrency is the number of buffered requests drained in
	// parallel after the failover completes (--buffer-drain-concurrency).
	// Binary default: 1.
	// +optional
	// +kubebuilder:validation:Minimum=1
	DrainConcurrency *int32 `json:"drainConcurrency,omitempty"`
}

// ============================================================================
// Cell Spec (Read-only API)
// ============================================================================
//
// Cell is a child CR managed by MultigresCluster.

// CellSpec defines the desired state of Cell.
// +kubebuilder:validation:XValidation:rule="!(has(self.zoneId) && has(self.region))",message="cannot specify both 'zoneId' and 'region'"
// +kubebuilder:validation:XValidation:rule="has(self.zoneId) || has(self.region)",message="at least one of 'zoneId' or 'region' must be specified"
type CellSpec struct {
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
	// the cell's topology record. Set from the parent cluster's cells[].metadata.
	// +optional
	// +kubebuilder:validation:MaxLength=4096
	Metadata string `json:"metadata,omitempty"`

	// Images defines the container images used in this cell.
	Images CellImages `json:"images"`

	// Multigateway fully resolved config.
	Multigateway MultigatewaySpec `json:"multigateway"`

	// MultigatewayPlacement defines optional scheduling settings for multigateway pods.
	// +optional
	MultigatewayPlacement *PodPlacementSpec `json:"multigatewayPlacement,omitempty"`

	// GlobalTopoServer reference (always populated).
	GlobalTopoServer GlobalTopoServerRef `json:"globalTopoServer"`

	// TopoServer defines the local topology config.
	// +optional
	TopoServer *LocalTopoServerSpec `json:"topoServer,omitempty"`

	// AllCells list for discovery.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=50
	AllCells []CellName `json:"allCells,omitempty"`

	// TopologyReconciliation flags.
	// +optional
	TopologyReconciliation TopologyReconciliation `json:"topologyReconciliation,omitempty"`

	// Observability configures OpenTelemetry for cell-level data-plane components.
	// Inherited from MultigresCluster.Spec.Observability by the resolver.
	// +optional
	Observability *ObservabilityConfig `json:"observability,omitempty"`

	// LogLevels is the resolved log level configuration for cell-level components.
	// Inherited from MultigresCluster.Spec.LogLevels.
	// +optional
	LogLevels ComponentLogLevels `json:"logLevels,omitempty"`

	// InternalTLS is the resolved internal component mTLS configuration.
	// When omitted or enabled is false, internal mTLS is disabled.
	// +optional
	InternalTLS *InternalTLSConfig `json:"internalTLS,omitempty"`

	// CertCommonName is the DNS name for the public PostgreSQL TLS certificate
	// terminated by multigateway. It does not enable internal component mTLS.
	// Inherited from MultigresCluster.Spec.CertCommonName.
	// +optional
	CertCommonName string `json:"certCommonName,omitempty"`
}

// CellImages defines the images required for a Cell.
type CellImages struct {
	// ImagePullPolicy overrides the default image pull policy.
	// +optional
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets is a list of references to secrets in the same namespace.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Multigateway is the image used for the gateway.
	Multigateway ImageRef `json:"multigateway"`
}

// TopologyReconciliation defines flags for the cell controller.
type TopologyReconciliation struct {
	// RegisterCell indicates if the cell should register itself in the topology.
	RegisterCell bool `json:"registerCell"`

	// PrunePoolers indicates if dead poolers (topology entries with no backing
	// pod) should be marked LIFECYCLE_SHUTDOWN so the orchestrator clears them
	// from the cohort.
	PrunePoolers bool `json:"prunePoolers"`
}

// ============================================================================
// CR Controller Status Specs
// ============================================================================

// CellStatus defines the observed state of Cell.
type CellStatus struct {
	// ObservedGeneration is the most recent generation observed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Phase represents the aggregated lifecycle state of the cell.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// Message provides details about the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// GatewayReplicas is the total number of gateway pods.
	GatewayReplicas int32 `json:"gatewayReplicas"`

	// GatewayReadyReplicas is the number of gateway pods that are ready.
	GatewayReadyReplicas int32 `json:"gatewayReadyReplicas"`

	// GatewayServiceName is the DNS name of the gateway service.
	// +kubebuilder:validation:MaxLength=253
	GatewayServiceName string `json:"gatewayServiceName,omitempty"`
}

// ============================================================================
// Kind Definition and registration
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Gateway",type="integer",JSONPath=".status.gatewayReadyReplicas"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Available')].status"

// Cell represents a failure-domain unit within a MultigresCluster (typically one per availability zone).
// +kubebuilder:resource:shortName=cel
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
type Cell struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CellSpec   `json:"spec,omitempty"`
	Status CellStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CellList contains a list of Cell
type CellList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cell `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Cell{}, &CellList{})
}
