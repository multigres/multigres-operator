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

// Package shard implements the controller for the Shard resource.
//
// The Shard controller is the most complex controller in the operator, responsible
// for managing all infrastructure required to run a database shard. It creates and
// maintains the following components:
//
// # PostgreSQL Pools
//
// For each pool defined in the Shard spec, the controller creates:
//   - Pod: Runs PostgreSQL replicas (operator-managed) with proper volume claims and configuration.
//   - Headless Service: Enables direct pod addressing for replication.
//   - Backup PVC: Shared persistent volume for backup storage (when configured).
//
// Pools can span multiple cells for high availability. In multi-cell configurations,
// the controller creates separate Pods per cell while maintaining a unified
// view in the Shard status.
//
// # Multiorch Orchestrator
//
// The controller manages the Multiorch component which handles:
//   - Leader election and failover for PostgreSQL
//   - Replication topology management
//   - Health monitoring and automatic recovery
//
// For each cell where the shard operates, a Multiorch Deployment and Service are created.
//
// # Configuration Management
//
// The controller creates ConfigMaps for PostgreSQL configuration:
//   - pg_hba.conf template: Authentication rules for client connections.
//
// # Status Aggregation
//
// The controller continuously monitors all managed resources and aggregates their
// status into the Shard's status fields, including:
//   - Ready/Total pod counts across all pools
//   - List of cells where the shard is deployed
//   - Orchestrator readiness per cell
//   - Overall phase (Initializing, Progressing, Healthy)
package shard
