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

	// Images defines the container images used in this cell.
	Images CellImages `json:"images"`

	// Multigateway fully resolved config.
	Multigateway StatelessSpec `json:"multigateway"`

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

	// CertCommonName is the DNS name for the multigateway TLS certificate.
	// When set, the cluster controller creates a cert-manager Certificate and
	// the cell controller mounts the TLS secret into multigateway pods.
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
