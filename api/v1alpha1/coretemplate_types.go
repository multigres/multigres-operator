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
// CoreTemplateSpec Spec
// ============================================================================

// CoreTemplateSpec defines reusable config for core components (GlobalTopo, Multiadmin).
type CoreTemplateSpec struct {
	// GlobalTopoServer configuration.
	// Uses TopoServerSpec directly (Managed Etcd only).
	// +optional
	GlobalTopoServer *TopoServerSpec `json:"globalTopoServer,omitempty"`

	// Multiadmin configuration.
	// +optional
	Multiadmin *StatelessSpec `json:"multiadmin,omitempty"`

	// MultiadminWeb configuration.
	// +optional
	MultiadminWeb *StatelessSpec `json:"multiadminWeb,omitempty"`
}

// ============================================================================
// Kind Definition and registration
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced

// CoreTemplate provides reusable infrastructure configuration (pod resources, scheduling) for cluster core components.
// +kubebuilder:resource:shortName=cot
type CoreTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CoreTemplateSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// CoreTemplateList contains a list of CoreTemplate
type CoreTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CoreTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CoreTemplate{}, &CoreTemplateList{})
}
