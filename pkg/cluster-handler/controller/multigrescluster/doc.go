// Package multigrescluster implements the controller for the root MultigresCluster resource.
//
// The MultigresCluster controller acts as the central orchestrator for the database system.
// It is responsible for translating the high-level user intent into specific child resources.
// Its primary responsibilities include:
//
//  1. Global Component Management:
//     It directly manages singleton resources defined at the cluster level, such as the
//     Multiadmin deployment. It also manages the Global TopoServer (via a child TopoServer CR)
//     when the cluster is configured for managed topology (Etcd).
//
//  2. Resource Fan-Out (Child CR Management):
//     It projects the configuration defined in the MultigresCluster spec (Cells and Databases)
//     into discrete child Custom Resources (Cell and TableGroup). These child resources are
//     then reconciled by their own respective controllers.
//
//  3. Defaulting and Template Resolution:
//     It applies in-memory defaults (robustness against webhook unavailability) and leverages
//     the 'pkg/resolver' module to fetch CoreTemplates, CellTemplates, and ShardTemplates,
//     merging them with user-defined overrides to produce the final specifications for
//     child resources.
//
//  4. Status Aggregation:
//     It continually observes the status of its child resources to produce a high-level
//     summary of the cluster's health (e.g., "All cells ready", "Database X has Y/Z shards ready").
//
//  5. Lifecycle Management:
//     It relies on Kubernetes owner references for garbage collection of child resources
//     and cleaned up before the parent MultigresCluster is removed.
package multigrescluster
