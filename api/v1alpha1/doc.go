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

// Package v1alpha1 defines the API types for the Multigres Operator.
//
// This package contains the Go type definitions for all Custom Resources in the
// multigres.com API group. These types are used by kubebuilder to generate:
//   - CustomResourceDefinitions (CRDs)
//   - DeepCopy methods
//   - Client code
//
// # Custom Resources
//
// The API defines a hierarchical structure of resources:
//
// User-Facing Resources:
//   - MultigresCluster: The root resource representing a complete distributed database cluster.
//     Users define cells, databases, and configuration here.
//   - CoreTemplate: Reusable configuration template for core components (Multiadmin).
//   - CellTemplate: Reusable configuration template for cell components (Multigateway).
//   - ShardTemplate: Reusable configuration template for shards (pools, orchestrator).
//
// Operator-Managed Resources (child resources created by the operator):
//   - Cell: Represents a failure domain with its own Multigateway deployment.
//   - TableGroup: Groups shards belonging to the same database table group.
//   - Shard: A database shard with PostgreSQL pools and orchestrator.
//   - TopoServer: Etcd-based topology storage server.
//
// # Resource Hierarchy
//
//	MultigresCluster
//	├── TopoServer (global topology)
//	├── Cell (per failure domain)
//	│   └── Multigateway Deployment
//	└── TableGroup (per database.tablegroup)
//	    └── Shard (per shard)
//	        ├── Pool StatefulSets (PostgreSQL replicas)
//	        └── Multiorch Deployment
//
// # Versioning
//
// This is the v1alpha1 version, indicating the API is in early development
// and may change in backward-incompatible ways.
package v1alpha1
