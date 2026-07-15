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

// Package cell implements the controller for the Cell resource.
//
// The Cell controller manages the Multigateway component, which serves as the
// entry point for client connections within a failure domain (cell). Its
// responsibilities include:
//
//   - Multigateway Deployment: Creates and maintains a Deployment that runs the
//     Multigateway proxy. This component routes client queries to the appropriate
//     shards based on the topology configuration.
//
//   - Multigateway Service: Exposes the Multigateway Deployment via a Kubernetes
//     Service, providing a stable endpoint for clients within the cell.
//
//   - Status Aggregation: Monitors the Deployment's availability and updates the
//     Cell's status conditions accordingly. Reports ready replica counts and
//     overall health phase.
//
// The Cell controller is part of the resource-handler module and operates at the
// "leaf" level of the resource hierarchy, directly managing Kubernetes-native
// resources (Deployments and Services) rather than other Custom Resources.
package cell
