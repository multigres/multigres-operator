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

// Fields is the contract: one row per status field (or keyed-list element)
// and the manager that writes it. OwnerOf resolves a path against it by
// longest prefix at a path-element boundary, so a row for
// `.status.conditions[type="X"]` covers every sub-field of that condition.
//
// Managers are the target names, not what the code writes under today.
var Fields = []Field{
	// MultigresCluster
	{
		Kind:    kindCluster,
		Path:    ".status.observedGeneration",
		Manager: ManagerCluster,
		Class:   Gate,
		Changes: "every reconcile, to the generation the status was computed from",
		Readers: "multigrescluster_controller.go:196 reads the persisted value before updateStatus runs, to decide whether to emit the ImplicitDefault events (observedGeneration != generation, or phase empty); a wiped or stale one re-emits them once and nothing else. " +
			"retire_global_topo.go moveIncomplete (retireManagedGlobalTopo) also compares it with metadata.generation, but updateStatus has set it to the current generation earlier in the same pass (controller.go ~362, then moveIncomplete ~381), so that comparison reads the in-memory value and always passes. " +
			"The reconcile that rewrites it does not wait on either reader.",
	},
	{
		Kind:    kindCluster,
		Path:    conditionsOwner,
		Manager: ManagerCluster,
		Class:   Observation,
		Changes: "every reconcile for Available, Gateway and AdminWeb conditions; ImageRolloutPending when images resolve",
	},
	{
		Kind:    kindCluster,
		Path:    `.status.conditions[type="TopologyReady"]`,
		Manager: ManagerCluster,
		Class:   Gate,
		Changes: "set False when registering cells or databases in the topology server fails, True again after a registration that reaches it",
		Readers: "retire_global_topo.go moveIncomplete (:304) holds retirement while it is False. That check is defensive today: every False write (markTopologyFailed) is followed by an error return from reconcileTopology, so the pass never reaches retirement with it False, and the real hold is that early return. " +
			"The class stays Gate so the row stays honest if a False ever lets the pass continue. " +
			"reconcileTopology runs every reconcile and rewrites it, and nothing the retirement waits on feeds that write, so a recovered external topology clears it.",
	},
	{
		Kind:    kindCluster,
		Path:    ".status.phase",
		Manager: ManagerCluster,
		Class:   Gate,
		Changes: "every reconcile, aggregated from child Cells, TableGroups and the global TopoServer",
		Readers: "retire_global_topo.go moveIncomplete holds the managed global TopoServer until Healthy, reading the value updateStatus computed earlier in the same pass. " +
			"status.go skips the retiring TopoServer when aggregating, so its health cannot hold back its own retirement. " +
			"multigrescluster_controller.go:196 reads the persisted value before updateStatus runs, to decide whether to emit the ImplicitDefault events (phase empty, or observedGeneration != generation); a wiped one re-emits them once and nothing else.",
	},
	{
		Kind:    kindCluster,
		Path:    ".status.initializedAt",
		Manager: ManagerCluster,
		Class:   WorkflowMemory,
		Changes: "stamped once, the first time the cluster is Healthy; never cleared",
		Readers: "multigrescluster/status.go:312 reads it as the latch: it stamps only when nil. " +
			"multigrescluster_controller.go SetClusterInfo reads it for the initialized metric label. " +
			"Wiped, the metric reads false and the stamp is reissued, with a later time, at the next Healthy.",
	},
	{
		Kind: kindCluster, Path: ".status.message", Manager: ManagerCluster, Class: Observation,
		Changes: "every reconcile, with the phase",
	},
	{
		Kind: kindCluster, Path: ".status.cells", Manager: ManagerCluster, Class: Observation,
		Changes: "every reconcile, one summary per Cell read back from the Cell statuses",
	},
	{
		Kind: kindCluster, Path: ".status.gateway", Manager: ManagerCluster, Class: Observation,
		Changes: "every reconcile, aggregated from the Cells' gateway replica counts",
	},
	{
		Kind: kindCluster, Path: ".status.adminWeb", Manager: ManagerCluster, Class: Observation,
		Changes: "every reconcile, read from the admin web Deployment",
	},
	{
		Kind: kindCluster, Path: ".status.databases", Manager: ManagerCluster, Class: Observation,
		Changes: "every reconcile, shard counts summed from the TableGroup statuses",
	},
	{
		Kind:    kindCluster,
		Path:    ".status.resolvedTemplates",
		Manager: ManagerCluster,
		Class:   Gate,
		Changes: "every reconcile, from the spec; changes when a template reference is added or removed",
		Readers: "multigrescluster_controller.go referencesTemplate maps a template event to the clusters that use it. " +
			"Missing or stale means a template edit does not enqueue the cluster until some other event does; the next reconcile rebuilds it from the spec.",
	},
	{
		Kind: kindCluster, Path: ".status.images", Manager: ManagerCluster, Class: Observation,
		Changes: "every reconcile, from the spec, the operator defaults and the applied record",
	},
	{
		Kind:    kindCluster,
		Path:    ".status.images.applied",
		Manager: ManagerCluster,
		Class:   WorkflowMemory,
		Changes: "when the operator adopts a new default image set, or an acknowledged revision is applied",
		Readers: "images.go recordedImages falls back to it when the applied-images annotation is absent or invalid. " +
			"Missing too means no applied record: the operator resolves from the current defaults, which is the only path that can adopt a new default set unacknowledged.",
	},
	{
		Kind:    kindCluster,
		Path:    ".status.images.appliedRevision",
		Manager: ManagerCluster,
		Class:   Observation,
		Changes: "with images.applied",
	},

	// TableGroup
	{
		Kind:    kindTableGroup,
		Path:    ".status.observedGeneration",
		Manager: ManagerTableGroup,
		Class:   Gate,
		Changes: "every reconcile, to the generation the status was computed from",
		Readers: "multigrescluster/status.go aggregation counts a TableGroup's shard totals only when it equals metadata.generation. " +
			"Stale means the cluster stays Progressing; the TableGroup controller rewrites it on its own schedule and does not wait on the cluster.",
	},
	{
		Kind:    kindTableGroup,
		Path:    ".status.phase",
		Manager: ManagerTableGroup,
		Class:   Gate,
		Changes: "every reconcile, aggregated from the child Shards",
		Readers: "multigrescluster/status.go aggregation: Degraded or non-Healthy keeps the cluster from Healthy. " +
			"Unreadable or missing is not Healthy; the TableGroup controller rewrites it each reconcile.",
	},
	{
		Kind:    kindTableGroup,
		Path:    ".status.message",
		Manager: ManagerTableGroup,
		Class:   Observation,
		Changes: "every reconcile, with the phase",
	},
	{
		Kind:    kindTableGroup,
		Path:    ".status.readyShards",
		Manager: ManagerTableGroup,
		Class:   Observation,
		Changes: "every reconcile, counted from child Shard phases; feeds only the published cluster database summary",
	},
	{
		Kind:    kindTableGroup,
		Path:    ".status.totalShards",
		Manager: ManagerTableGroup,
		Class:   Observation,
		Changes: "every reconcile, the number of child Shards; feeds only the published cluster database summary and metrics",
	},
	{
		Kind: kindTableGroup, Path: conditionsOwner, Manager: ManagerTableGroup, Class: Observation,
		Changes: "every reconcile for Available",
	},
	{
		Kind:    kindTableGroup,
		Path:    `.status.conditions[type="ReadyForDeletion"]`,
		Manager: ManagerTableGroup,
		Class:   Gate,
		Changes: "set True once every child Shard reports ReadyForDeletion, during a graceful delete",
		Readers: "multigrescluster/reconcile_databases.go holds the TableGroup (pendingDeletion) until it is True, then deletes it. " +
			"Missing means the cluster keeps waiting and requeues; it is set by the TableGroup controller, which waits only on its Shards, and their drain does not wait on the cluster. " +
			"The reader sees the value through the informer cache, so a stale False delays the delete by one event.",
	},

	// Cell
	{
		Kind:    kindCell,
		Path:    ".status.observedGeneration",
		Manager: ManagerCell,
		Class:   Gate,
		Changes: "every reconcile, to the generation the status was computed from",
		Readers: "multigrescluster/status.go treats a Cell as Available or counts its gateway replicas only when it equals metadata.generation. " +
			"Stale means the cluster reports the Cell as not ready; the Cell controller rewrites it every reconcile.",
	},
	{
		Kind: kindCell, Path: conditionsOwner, Manager: ManagerCell, Class: Observation,
		Changes: "every reconcile for Ready",
	},
	{
		Kind:    kindCell,
		Path:    `.status.conditions[type="Available"]`,
		Manager: ManagerCell,
		Class:   Gate,
		Changes: "every reconcile, from the multigateway Deployment (or Waiting while the local TopoServer is not ready)",
		Readers: "multigrescluster/status.go builds the cluster's per-Cell Ready and, from it, the cluster Available condition. " +
			"Missing is not Ready; the Cell controller rewrites it every reconcile.",
	},
	{
		Kind:    kindCell,
		Path:    `.status.conditions[type="ReadyForDeletion"]`,
		Manager: ManagerCell,
		Class:   Gate,
		Changes: "set True once the Cell's gateway is gone and any managed local TopoServer is deleted, during a graceful delete",
		Readers: "multigrescluster/reconcile_cells.go holds the Cell (pendingDeletion) until it is True, then deletes it. " +
			"Missing means the cluster requeues and waits; the Cell controller sets it without waiting on the cluster. " +
			"The reader sees the value through the informer cache, so a stale False delays the delete by one event.",
	},
	{
		Kind:    kindCell,
		Path:    ".status.phase",
		Manager: ManagerCell,
		Class:   Gate,
		Changes: "every reconcile, from the multigateway Deployment and the local TopoServer",
		Readers: "multigrescluster/status.go aggregation: non-Healthy keeps the cluster from Healthy. " +
			"cell_controller.go reads its own just-computed value to choose a requeue. " +
			"Missing is not Healthy; the Cell controller rewrites it every reconcile.",
	},
	{
		Kind: kindCell, Path: ".status.message", Manager: ManagerCell, Class: Observation,
		Changes: "every reconcile, with the phase",
	},
	{
		Kind: kindCell, Path: ".status.gatewayReplicas", Manager: ManagerCell, Class: Observation,
		Changes: "every reconcile, from the multigateway Deployment",
	},
	{
		Kind:    kindCell,
		Path:    ".status.gatewayReadyReplicas",
		Manager: ManagerCell,
		Class:   Observation,
		Changes: "every reconcile, from the multigateway Deployment; feeds only the cluster Gateway condition",
	},
	{
		Kind:    kindCell,
		Path:    ".status.gatewayServiceName",
		Manager: ManagerCell,
		Class:   Observation,
		Changes: "every reconcile; changes only if the Cell is renamed",
	},

	// TopoServer
	{
		Kind:    kindTopoServer,
		Path:    ".status.observedGeneration",
		Manager: ManagerTopoServer,
		Class:   Gate,
		Changes: "every reconcile, to the generation the status was computed from",
		Readers: "cell_controller.go and multigrescluster/reconcile_topology.go (topology readiness) and multigrescluster/status.go require it to equal metadata.generation before trusting phase. " +
			"Stale means the Cell or cluster requeues; the TopoServer controller rewrites it every reconcile and does not wait on its readers.",
	},
	{
		Kind: kindTopoServer, Path: conditionsOwner, Manager: ManagerTopoServer, Class: Observation,
		Changes: "every reconcile for Ready",
	},
	{
		Kind:    kindTopoServer,
		Path:    `.status.conditions[type="StorageClassValid"]`,
		Manager: ManagerTopoServerGuard,
		Class:   Observation,
		Changes: "when the storage class guard re-evaluates a pending or failed volume claim",
	},
	{
		Kind:    kindTopoServer,
		Path:    ".status.phase",
		Manager: ManagerTopoServer,
		Class:   Gate,
		Changes: "every reconcile, from the etcd StatefulSet",
		Readers: "cell_controller.go (local topology ready), multigrescluster/reconcile_topology.go and multigrescluster/status.go read it before trusting the topology server. " +
			"toposerver_controller.go reads its own just-computed value to decide whether to run etcd maintenance. " +
			"Missing is not Healthy; the TopoServer controller rewrites it every reconcile.",
	},
	{
		Kind:    kindTopoServer,
		Path:    ".status.message",
		Manager: ManagerTopoServer,
		Class:   Observation,
		Changes: "every reconcile, with the phase",
	},
	{
		Kind:    kindTopoServer,
		Path:    ".status.clientService",
		Manager: ManagerTopoServer,
		Class:   Observation,
		Changes: "every reconcile; changes only if the TopoServer is renamed",
	},
	{
		Kind:    kindTopoServer,
		Path:    ".status.peerService",
		Manager: ManagerTopoServer,
		Class:   Observation,
		Changes: "every reconcile; changes only if the TopoServer is renamed",
	},
	{
		Kind:    kindTopoServer,
		Path:    ".status.etcdMaintenance",
		Manager: ManagerEtcdMaintenance,
		Class:   WorkflowMemory,
		Changes: "reserved before each defragmentation, cleared or settled when it ends; inProgress stays true after an interrupted or uncertain operation",
		Readers: "toposerver/maintenance.go resumeMaintenance and reconcileMaintenance read it from an uncached reader to enforce the one-hour interval and to pause pod rollouts while inProgress. " +
			"A wiped record permits a new defragmentation at once. A stuck inProgress is released by the maintenanceTimeout path or by disabling maintenance, and member health checks passing again clears it.",
	},

	// Shard
	{
		Kind:    kindShard,
		Path:    ".status.observedGeneration",
		Manager: ManagerShard,
		Class:   Gate,
		Changes: "every reconcile, to the generation the status was computed from",
		Readers: "tablegroup/status.go counts a Shard Ready only when it equals the generation the TableGroup applied. " +
			"Stale means the TableGroup stays Progressing; the Shard controller rewrites it every reconcile.",
	},
	{
		Kind:    kindShard,
		Path:    ".status.postgresConfig",
		Manager: ManagerShard,
		Class:   WorkflowMemory,
		Changes: "every reconcile; inProgress during a config rollout, lastAppliedAt stamped when it settles",
		Readers: "shard/status.go setPostgresConfigStatus reads the previous value to keep lastAppliedAt and to stamp it only on an unsettled-to-settled transition. " +
			"Missing is treated as unsettled and stamps now, so the next reconcile rebuilds it; inProgress and error are recomputed every reconcile.",
	},
	{
		Kind: kindShard, Path: conditionsOwner, Manager: ManagerShard, Class: Observation,
		Changes: "every reconcile for Available and RollingUpdate",
	},
	{
		Kind:    kindShard,
		Path:    `.status.conditions[type="StorageClassValid"]`,
		Manager: ManagerShardGuard,
		Class:   Observation,
		Changes: "when the storage class guard re-evaluates a pending or failed volume claim",
	},
	{
		Kind:    kindShard,
		Path:    `.status.conditions[type="PostureConsistent"]`,
		Manager: ManagerShard,
		Class:   Gate,
		Changes: "every data-plane reconcile; False on multiple primaries or a role mismatch, Unknown when observation is incomplete",
		Readers: "shard/status.go turns False into phase Degraded and Unknown into Progressing. " +
			"data-handler/posture/posture.go reads it so a partial scan cannot clear a confirmed False, so it clears only on a complete observation, and the rescan is a timed requeue that does not wait on the condition.",
	},
	{
		Kind:    kindShard,
		Path:    `.status.conditions[type="BackupHealthy"]`,
		Manager: ManagerShard,
		Class:   Gate,
		Changes: "every data-plane reconcile, from the newest backup in the pooler",
		Readers: "shard/reconcile_quarantine.go defers quarantine remediation (a hard delete of a data volume) unless it is True. " +
			"Missing or False blocks, and it is rewritten each reconcile from the backup listing, which does not depend on the quarantined pod.",
	},
	{
		Kind:    kindShard,
		Path:    `.status.conditions[type="ReadyForDeletion"]`,
		Manager: ManagerShard,
		Class:   Gate,
		Changes: "set True once every pod is drained and deleted, during a graceful delete",
		Readers: "tablegroup/reconcile_shards.go holds the Shard (pendingDeletion) until it is True, then deletes it. " +
			"Missing means the TableGroup requeues every 5s; the Shard controller sets it after its own drain, which does not wait on the TableGroup. " +
			"The reader sees the value through the informer cache, so a stale False delays the delete by one event.",
	},
	{
		Kind: kindShard, Path: ".status.phase", Manager: ManagerShard, Class: Gate,
		Changes: "every reconcile, from pool and multiorch pods, posture and PodRoles",
		Readers: "tablegroup/status.go aggregates it into the TableGroup phase. " +
			"shard/reconcile_data_plane.go requeues to re-read topology when Healthy without a primary. " +
			"Missing is not Healthy; the Shard controller rewrites it every reconcile.",
	},
	{
		Kind: kindShard, Path: ".status.message", Manager: ManagerShard, Class: Observation,
		Changes: "every reconcile, with the phase",
	},
	{
		Kind: kindShard, Path: ".status.cells", Manager: ManagerShard, Class: Observation,
		Changes: "every reconcile, the sorted cells that have observed pods",
	},
	{
		Kind: kindShard, Path: ".status.orchReady", Manager: ManagerShard, Class: Observation,
		Changes: "every reconcile, from the multiorch Deployments",
	},
	{
		Kind: kindShard, Path: ".status.poolsReady", Manager: ManagerShard, Class: Observation,
		Changes: "every reconcile, from the pool pods",
	},
	{
		Kind: kindShard, Path: ".status.lastBackupTime", Manager: ManagerShard, Class: Observation,
		Changes: "when a newer backup is observed in the pooler",
	},
	{
		Kind: kindShard, Path: ".status.lastBackupType", Manager: ManagerShard, Class: Observation,
		Changes: "when a newer backup is observed in the pooler",
	},
	{
		Kind:    kindShard,
		Path:    ".status.podRoles",
		Manager: ManagerShard,
		Class:   Gate,
		Changes: "every data-plane reconcile, read from the topology server",
		Readers: "shard/drain_helpers.go resolvePodRole (used in reconcile_pool_pods.go) picks the primary or replica path for a pod being removed, and reconcile_quarantine.go (~:134) skips a pod recorded PRIMARY, a defensive guard that is lost when the role is missing (the quarantined list itself comes from topology); " +
			"data-handler/posture/disruption.go refuses to proceed when the target is primary but not recorded so; " +
			"shard/status.go:86 and reconcile_data_plane.go:137 read it through hasPrimary, so a missing map reads as no primary: the phase is Degraded when pools and orchestration are ready, and a Healthy shard requeues every 10s. " +
			"Missing or stale reads as no role: a pod is routed down the replica path, which changes only ordering, and posture.CheckDisruption still blocks the drain (fail closed) when the target is primary, unreachable or missing from topology. " +
			"reconcilePodRoles refills it from topology earlier in the same data-plane pass (reconcile_data_plane.go:53), before status and the requeue read it, and reads the topology, not the drain, so the wait ends.",
	},
	{
		Kind: kindShard, Path: ".status.podPostures", Manager: ManagerShard, Class: Observation,
		Changes: "every data-plane reconcile, read from the pooler observations",
	},
	{
		Kind: kindShard, Path: ".status.readyReplicas", Manager: ManagerShard, Class: Observation,
		Changes: "every reconcile, the count of ready pool pods",
	},

	// Pod
	{
		Kind:    kindPod,
		Path:    `.status.conditions[type="multigres.com/pooler-data-ready"]`,
		Manager: ManagerShardReadiness,
		Class:   Gate,
		Changes: "when the pooler reports its data readiness; the pod readiness gate follows it",
		Readers: "shard/reconcile_pool_pods.go podTopoDrifted excludes a pod whose condition is True from topology-drift replacement, so it gates a drain. " +
			"It is a Pod readiness gate (pool_pod.go), so it feeds Pod Ready, which the disruption gates read (shard/disruption.go canStartDisruption via isAvailablePooler in maintenance_surge.go, and isShardHealthy in reconcile_pool_pods.go). " +
			"Missing reads as not ready: a drifted pod becomes a replacement candidate (topoStrandedPod decides against live topology) and disruption gates wait. " +
			"A stale True holds the previous value, so the pod still reads as ready and is not a candidate, until the next data-plane pass rewrites it. " +
			"reconcilePoolerReadiness rewrites it from pooler status independent of those gates, so either way the wait ends.",
	},
}
