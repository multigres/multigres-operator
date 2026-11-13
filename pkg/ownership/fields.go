package ownership

const (
	kindCluster     = "MultigresCluster"
	kindTableGroup  = "TableGroup"
	kindCell        = "Cell"
	kindTopoServer  = "TopoServer"
	kindShard       = "Shard"
	kindPod         = "Pod"
	conditionsOwner = ".status.conditions"
)

// Fields has one row per status field or keyed-list element, the manager that
// writes it, and its class. OwnerOf resolves a path by longest prefix at a
// path-element boundary.
var Fields = []Field{
	// MultigresCluster
	// multigrescluster_controller.go Reconcile emits implicit-default events when
	// this is stale or phase is empty. retire_global_topo.go moveIncomplete compares
	// the in-memory value with generation; updateStatus refreshes it in the pass.
	{Kind: kindCluster, Path: ".status.observedGeneration", Manager: ManagerCluster, Class: Gate},
	{Kind: kindCluster, Path: conditionsOwner, Manager: ManagerCluster, Class: Observation},
	// reconcile_topology.go reconcileTopology writes False through
	// markTopologyFailed when it cannot open the topology store for a reason other
	// than unavailability, or certificate.go cannot issue the topology client
	// certificate. Registration and pruning failures do not set it. A completed
	// reconcileTopology registration sets it True. retire_global_topo.go
	// moveIncomplete defensively holds retirement while it is False because each
	// False write returns an error before retirement runs.
	{
		Kind:    kindCluster,
		Path:    `.status.conditions[type="TopologyReady"]`,
		Manager: ManagerCluster,
		Class:   Gate,
	},
	// retire_global_topo.go moveIncomplete holds the managed global TopoServer until
	// Healthy. status.go updateStatus recomputes it from child resources each pass.
	{Kind: kindCluster, Path: ".status.phase", Manager: ManagerCluster, Class: Gate},
	// status.go updateStatus stamps this after the cluster becomes Healthy, and
	// SetClusterInfo reads it for the initialized metric. A missing value is stamped
	// after the next healthy reconciliation.
	{
		Kind:    kindCluster,
		Path:    ".status.initializedAt",
		Manager: ManagerCluster,
		Class:   WorkflowMemory,
	},
	{Kind: kindCluster, Path: ".status.message", Manager: ManagerCluster, Class: Observation},
	{Kind: kindCluster, Path: ".status.cells", Manager: ManagerCluster, Class: Observation},
	{Kind: kindCluster, Path: ".status.gateway", Manager: ManagerCluster, Class: Observation},
	{Kind: kindCluster, Path: ".status.adminWeb", Manager: ManagerCluster, Class: Observation},
	{Kind: kindCluster, Path: ".status.databases", Manager: ManagerCluster, Class: Observation},
	// multigrescluster_controller.go referencesTemplate maps template events using
	// this value. A missing or stale value waits for another event; updateStatus
	// rebuilds it from the spec on the next reconciliation.
	{Kind: kindCluster, Path: ".status.resolvedTemplates", Manager: ManagerCluster, Class: Gate},
	{Kind: kindCluster, Path: ".status.images", Manager: ManagerCluster, Class: Observation},
	// images.go recordedImages uses this when the applied-images annotation is
	// absent or invalid. updateStatus records the resolved default image set.
	{
		Kind:    kindCluster,
		Path:    ".status.images.applied",
		Manager: ManagerCluster,
		Class:   WorkflowMemory,
	},
	{
		Kind:    kindCluster,
		Path:    ".status.images.appliedRevision",
		Manager: ManagerCluster,
		Class:   Observation,
	},

	// TableGroup
	// status.go updateStatus aggregates shard totals only when this equals the
	// generation. A stale value keeps the cluster progressing until the
	// tablegroup controller refreshes its status.
	{
		Kind:    kindTableGroup,
		Path:    ".status.observedGeneration",
		Manager: ManagerTableGroup,
		Class:   Gate,
	},
	// status.go updateStatus keeps the cluster from Healthy for a non-Healthy
	// value. The tablegroup controller recomputes it from its shards.
	{Kind: kindTableGroup, Path: ".status.phase", Manager: ManagerTableGroup, Class: Gate},
	{Kind: kindTableGroup, Path: ".status.message", Manager: ManagerTableGroup, Class: Observation},
	{
		Kind:    kindTableGroup,
		Path:    ".status.readyShards",
		Manager: ManagerTableGroup,
		Class:   Observation,
	},
	{
		Kind:    kindTableGroup,
		Path:    ".status.totalShards",
		Manager: ManagerTableGroup,
		Class:   Observation,
	},
	{Kind: kindTableGroup, Path: conditionsOwner, Manager: ManagerTableGroup, Class: Observation},
	// reconcile_databases.go pendingDeletion holds the TableGroup until this is
	// True. The tablegroup controller sets it after shards drain; cache refreshes
	// turn a stale False into a later delete reconciliation.
	{
		Kind:    kindTableGroup,
		Path:    `.status.conditions[type="ReadyForDeletion"]`,
		Manager: ManagerTableGroup,
		Class:   Gate,
	},

	// Cell
	// status.go updateStatus trusts this before reporting the Cell available or
	// counting gateway replicas. The cell controller refreshes it each reconcile.
	{Kind: kindCell, Path: ".status.observedGeneration", Manager: ManagerCell, Class: Gate},
	{Kind: kindCell, Path: conditionsOwner, Manager: ManagerCell, Class: Observation},
	// status.go updateStatus builds cluster readiness from this condition. The cell
	// controller recomputes it from the gateway Deployment and local TopoServer.
	{
		Kind:    kindCell,
		Path:    `.status.conditions[type="Available"]`,
		Manager: ManagerCell,
		Class:   Gate,
	},
	// reconcile_cells.go pendingDeletion holds the Cell until this is True. The
	// cell controller sets it after its managed local TopoServer drains, on the
	// resourceVersion the drain read, and clears it, persisted before
	// recreating anything, when a cancelled drain returns the Cell to normal
	// reconciling; reconcile_cells.go deletes only the exact resourceVersion it
	// read the condition from.
	{
		Kind:    kindCell,
		Path:    `.status.conditions[type="ReadyForDeletion"]`,
		Manager: ManagerCell,
		Class:   Gate,
	},
	// status.go updateStatus aggregates this into cluster phase, and
	// cell_controller.go Reconcile uses its computed phase for requeueing. The cell
	// controller recomputes it from the gateway Deployment and local TopoServer.
	{Kind: kindCell, Path: ".status.phase", Manager: ManagerCell, Class: Gate},
	{Kind: kindCell, Path: ".status.message", Manager: ManagerCell, Class: Observation},
	{Kind: kindCell, Path: ".status.gatewayReplicas", Manager: ManagerCell, Class: Observation},
	{
		Kind:    kindCell,
		Path:    ".status.gatewayReadyReplicas",
		Manager: ManagerCell,
		Class:   Observation,
	},
	{Kind: kindCell, Path: ".status.gatewayServiceName", Manager: ManagerCell, Class: Observation},

	// TopoServer
	// cell_controller.go Reconcile, reconcile_topology.go reconcileTopology, and
	// status.go updateStatus require this to match generation before trusting phase.
	// The toposerver controller refreshes it during reconciliation.
	{
		Kind:    kindTopoServer,
		Path:    ".status.observedGeneration",
		Manager: ManagerTopoServer,
		Class:   Gate,
	},
	{Kind: kindTopoServer, Path: conditionsOwner, Manager: ManagerTopoServer, Class: Observation},
	// auth.go makes successful authorization activation sticky, while status.go
	// uses it to gate initial readiness. AuthorizationReady reports the current
	// reconciliation without resetting this gate.
	{Kind: kindTopoServer, Path: ".status.authEnabled", Manager: ManagerTopoServer, Class: Gate},
	// auth.go records identities before granting access and retains them on errors;
	// later reconciles prune removed identities before shrinking this list.
	{
		Kind: kindTopoServer, Path: ".status.authIdentities",
		Manager: ManagerTopoServer, Class: WorkflowMemory,
	},
	{
		Kind:    kindTopoServer,
		Path:    `.status.conditions[type="StorageClassValid"]`,
		Manager: ManagerTopoServerGuard,
		Class:   Observation,
	},
	// cell_controller.go Reconcile, reconcile_topology.go reconcileTopology, and
	// status.go updateStatus read this before trusting the topology server.
	// toposerver_controller.go Reconcile recomputes it from the etcd StatefulSet.
	{Kind: kindTopoServer, Path: ".status.phase", Manager: ManagerTopoServer, Class: Gate},
	{Kind: kindTopoServer, Path: ".status.message", Manager: ManagerTopoServer, Class: Observation},
	{
		Kind:    kindTopoServer,
		Path:    ".status.clientService",
		Manager: ManagerTopoServer,
		Class:   Observation,
	},
	{
		Kind:    kindTopoServer,
		Path:    ".status.peerService",
		Manager: ManagerTopoServer,
		Class:   Observation,
	},
	// maintenance.go resumeMaintenance and reconcileMaintenance read this to enforce
	// the maintenance interval and pause rollouts while inProgress. The timeout path
	// or disabled maintenance releases an incomplete operation; health checks clear it.
	{
		Kind:    kindTopoServer,
		Path:    ".status.etcdMaintenance",
		Manager: ManagerEtcdMaintenance,
		Class:   WorkflowMemory,
	},

	// Shard
	// tablegroup/status.go updateStatus counts a shard ready only when this matches
	// generation. The shard controller refreshes it each reconcile.
	{Kind: kindShard, Path: ".status.observedGeneration", Manager: ManagerShard, Class: Gate},
	// shard/status.go setPostgresConfigStatus preserves lastAppliedAt across config
	// rollout transitions. The shard controller rebuilds inProgress and error, and
	// stamps a missing record after configuration settles.
	{Kind: kindShard, Path: ".status.postgresConfig", Manager: ManagerShard, Class: WorkflowMemory},
	{Kind: kindShard, Path: conditionsOwner, Manager: ManagerShard, Class: Observation},
	{
		Kind:    kindShard,
		Path:    `.status.conditions[type="StorageClassValid"]`,
		Manager: ManagerShardGuard,
		Class:   Observation,
	},
	// shard/status.go updateStatus turns False into Degraded and Unknown into
	// Progressing. posture.go Apply preserves a confirmed False until a complete
	// observation, and the shard controller requeues to perform that observation.
	{
		Kind:    kindShard,
		Path:    `.status.conditions[type="PostureConsistent"]`,
		Manager: ManagerShard,
		Class:   Gate,
	},
	// reconcile_quarantine.go reconcileQuarantine defers volume remediation unless
	// this is True. The shard controller refreshes it from backup listing, independent
	// of the quarantined pod.
	{
		Kind:    kindShard,
		Path:    `.status.conditions[type="BackupHealthy"]`,
		Manager: ManagerShard,
		Class:   Gate,
	},
	// reconcile_shards.go pendingDeletion holds the Shard until this is True. The
	// shard controller sets it after drain completion; cache refreshes retry a stale
	// False through the TableGroup reconciliation.
	{
		Kind:    kindShard,
		Path:    `.status.conditions[type="ReadyForDeletion"]`,
		Manager: ManagerShard,
		Class:   Gate,
	},
	// tablegroup/status.go updateStatus aggregates this, and reconcile_data_plane.go
	// reconcileDataPlane requeues a Healthy shard without a primary. The shard
	// controller recomputes phase from pool, orch, posture, and pod roles.
	{Kind: kindShard, Path: ".status.phase", Manager: ManagerShard, Class: Gate},
	{Kind: kindShard, Path: ".status.message", Manager: ManagerShard, Class: Observation},
	{Kind: kindShard, Path: ".status.cells", Manager: ManagerShard, Class: Observation},
	{Kind: kindShard, Path: ".status.orchReady", Manager: ManagerShard, Class: Observation},
	{Kind: kindShard, Path: ".status.poolsReady", Manager: ManagerShard, Class: Observation},
	{Kind: kindShard, Path: ".status.lastBackupTime", Manager: ManagerShard, Class: Observation},
	{Kind: kindShard, Path: ".status.lastBackupType", Manager: ManagerShard, Class: Observation},
	// drain_helpers.go resolvePodRole chooses the primary or replica removal path,
	// reconcile_quarantine.go reconcileQuarantine protects recorded primaries, and
	// disruption.go CheckDisruption blocks unsafe drains. status.go hasPrimary and
	// reconcile_data_plane.go reconcileDataPlane treat missing roles as no primary;
	// reconcilePodRoles refreshes them from topology before those checks.
	{Kind: kindShard, Path: ".status.podRoles", Manager: ManagerShard, Class: Gate},
	{Kind: kindShard, Path: ".status.podPostures", Manager: ManagerShard, Class: Observation},
	{Kind: kindShard, Path: ".status.readyReplicas", Manager: ManagerShard, Class: Observation},

	// Pod
	// reconcile_pool_pods.go podTopoDrifted and disruption.go canStartDisruption
	// read this through Pod readiness. A missing value blocks disruption and permits
	// replacement of a drifted pod; reconcilePoolerReadiness refreshes it from pooler status.
	{
		Kind:    kindPod,
		Path:    `.status.conditions[type="multigres.com/pooler-data-ready"]`,
		Manager: ManagerShardReadiness,
		Class:   Gate,
	},
}
