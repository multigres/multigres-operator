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
// CellTemplateSpec Spec
// ============================================================================

// CellTemplateSpec defines reusable config for Cell components (Gateway, LocalTopo).
type CellTemplateSpec struct {
	// Multigateway configuration.
	// +optional
	Multigateway *StatelessSpec `json:"multigateway,omitempty"`

	// MultigatewayPlacement defines optional scheduling settings for multigateway pods.
	// +optional
	MultigatewayPlacement *PodPlacementSpec `json:"multigatewayPlacement,omitempty"`

	// LocalTopoServer configuration (optional).
	// +optional
	LocalTopoServer *LocalTopoServerSpec `json:"localTopoServer,omitempty"`
}

// ============================================================================
// Kind Definition and registration
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced

// CellTemplate provides reusable cell-level configuration (images, resources) for MultigresCluster cells.
// +kubebuilder:resource:shortName=cet
type CellTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CellTemplateSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// CellTemplateList contains a list of CellTemplate
type CellTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CellTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CellTemplate{}, &CellTemplateList{})
}
