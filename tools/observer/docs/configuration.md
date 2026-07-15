# Configuration Reference

All flags, environment variables, thresholds, and tunable parameters for the observer.

---

## CLI Flags

| Flag | Default | Env Override | Description |
|------|---------|-------------|-------------|
| `--namespace` | `""` (all) | `NAMESPACE` | Namespace to observe for Multigres workloads. Empty = all namespaces |
| `--operator-namespace` | `multigres-operator` | `OPERATOR_NAMESPACE` | Namespace where the operator is running (for monitoring operator pod/logs) |
| `--interval` | `10s` | — | Time between observer cycles |
| `--kubeconfig` | `""` | `KUBECONFIG` | Path to kubeconfig. Empty = in-cluster config |
| `--once` | `false` | — | Run one cycle and exit (useful for CI) |
| `--metrics-addr` | `:9090` | — | Address for Prometheus metrics, health, and API endpoints (`/api/status`, `/api/history`, `/api/check`) |
| `--log-tail-lines` | `100` | — | Lines to tail from each container per cycle |
| `--enable-sql-probe` | `true` | — | Enable SQL probes for replication health and gateway connectivity |
| `--history-capacity` | `30` | — | Number of observer cycles to retain in finding history (30 = ~5 min at 10s interval) |

Environment variables override the corresponding flag only when the flag is at its default value. The flag always takes precedence.

---

## Observer Thresholds

These constants are defined in `pkg/common/constants.go` and control when findings are emitted.

### Timing Thresholds

| Constant | Value | Used by | Description |
|----------|-------|---------|-------------|
| `DefaultInterval` | 10s | Observer loop | Default cycle interval |
| `PendingTimeout` | 60s | Pod health | Pending pods flagged after this |
| `NotReadyTimeout` | 30s | Pod health | Not-ready containers flagged after this |
| `TerminatingTimeout` | 90s | Pod health | Stuck terminating pods flagged after this |
| `DrainRequestedTimeout` | 30s | Drain state | `requested` state stuck timeout |
| `DrainDrainingTimeout` | 5min | Drain state | `draining` state stuck timeout |
| `DrainAcknowledgedTimeout` | 30s | Drain state | `acknowledged` state stuck timeout |
| `PhaseDegradedTimeout` | 5min | CRD status | Degraded/Unknown phase flagged after this |
| `GenerationDivergeTimeout` | 60s | CRD status | Generation mismatch flagged after this |
| `PrimaryGracePeriod` | 60s | CRD status | Grace period before flagging wrong primary count |
| `StaleStatusEntryGracePeriod` | 30s | CRD status | Grace for stale podRoles entries after pod deletion |
| `TerminatingResourceTimeout` | 5min | Resources | Stuck terminating CRDs flagged after this |
| `ConnectivityTimeout` | 5s | Connectivity | TCP/HTTP probe timeout |
| `GRPCHealthTimeout` | 3s | Connectivity | gRPC health check timeout |
| `ConnectivityLatencyThreshold` | 500ms | Connectivity | Latency warning threshold |
| `MetricsProbeTimeout` | 5s | Connectivity | Operator metrics endpoint probe timeout |
| `PodStartupGracePeriod` | 2min | All checks | Suppress findings for newly started pool pods |
| `PhaseProgressingTimeout` | 10min | CRD status | Stuck-in-Progressing detection threshold |
| `BackupStalenessWarnAge` | 25h | CRD status | Backup age above this emits a warning |
| `BackupStalenessErrorAge` | 49h | CRD status | Backup age above this emits an error |

### Restart Thresholds

| Constant | Value | Description |
|----------|-------|-------------|
| `RestartWarnThreshold` | 1 | Any restart emits a warning |
| `RestartErrorThreshold` | 3 | ≥3 restarts in the error window emits an error |
| `RestartErrorWindow` | 5min | Window for counting restarts toward error threshold |

### Replication Thresholds

| Constant | Value | Description |
|----------|-------|-------------|
| `ReplicationLagWarnSecs` | 10 | Replay lag above this emits a warning |
| `ReplicationLagErrorSecs` | 60 | Replay lag above this emits an error |
| `PGNameDataLen` | 63 | PostgreSQL NAMEDATALEN; exact-match flags truncation |

---

## Well-Known Ports

The observer probes these ports on their respective services:

| Port | Component | Protocol | Constant |
|------|-----------|----------|----------|
| 15100 | Multigateway HTTP | HTTP | `PortMultigatewayHTTP` |
| 15170 | Multigateway gRPC | gRPC | `PortMultigatewayGRPC` |
| 5432 | Multigateway PostgreSQL | PG wire | `PortMultigatewayPG` |
| 15200 | Multipooler HTTP | HTTP | `PortMultipoolerHTTP` |
| 15270 | Multipooler gRPC | gRPC | `PortMultipoolerGRPC` |
| 15300 | Multiorch HTTP | HTTP | `PortMultiorchHTTP` |
| 15370 | Multiorch gRPC | gRPC | `PortMultiorchGRPC` |
| 2379 | etcd (TopoServer) client | HTTP | `PortEtcdClient` |
| 2380 | etcd (TopoServer) peer | HTTP | `PortEtcdPeer` |
| 5432 | PostgreSQL (pool pods) | PG wire | `PortPostgres` |
| 8081 | Operator health | HTTP | `PortOperatorHealth` |
| 8443 | Operator metrics | HTTPS | `PortOperatorMetrics` |

---

## Label Keys

The observer uses these labels to discover and classify Multigres resources:

| Constant | Label Key | Description |
|----------|-----------|-------------|
| `LabelAppManagedBy` | `app.kubernetes.io/managed-by` | Must be `multigres-operator` |
| `LabelAppComponent` | `app.kubernetes.io/component` | Component type (see below) |
| `LabelAppInstance` | `app.kubernetes.io/instance` | Instance name |
| `LabelMultigresCluster` | `multigres.com/cluster` | Parent cluster name |
| `LabelMultigresCell` | `multigres.com/cell` | Parent cell name |
| `LabelMultigresShard` | `multigres.com/shard` | Parent shard name |
| `LabelMultigresPool` | `multigres.com/pool` | Pool name |
| `LabelMultigresDatabase` | `multigres.com/database` | Database name |
| `LabelMultigresTableGroup` | `multigres.com/tablegroup` | TableGroup name |

### Component Values

| Constant | Value | Description |
|----------|-------|-------------|
| `ComponentPool` | `shard-pool` | Pool pods (PostgreSQL + multipooler) |
| `ComponentMultiorch` | `multiorch` | Orchestrator |
| `ComponentMultigateway` | `multigateway` | Gateway |
| `ComponentGlobalTopo` | `toposerver` | TopoServer (etcd) |

---

## Drain State Annotations

| Annotation | Description |
|------------|-------------|
| `drain.multigres.com/state` | Current drain state of the pod |
| `drain.multigres.com/requested-at` | Timestamp when drain was requested |

### Valid Drain States

| State | Constant | Description |
|-------|----------|-------------|
| `requested` | `DrainStateRequested` | Operator has requested this pod to drain |
| `draining` | `DrainStateDraining` | Drain is in progress (deregistering from topology) |
| `acknowledged` | `DrainStateAcknowledged` | Upstream has acknowledged the drain |
| `ready-for-deletion` | `DrainStateReadyForDeletion` | Pod can be safely deleted |

---

## Monitored Event Reasons

The observer flags Warning Kubernetes events matching these reasons:

### Operator Events

| Constant | Reason | Description |
|----------|--------|-------------|
| `EventReasonBackupStale` | `BackupStale` | Last backup older than 25h |
| `EventReasonConfigError` | `ConfigError` | Invalid configuration |
| `EventReasonExpandPVCFailed` | `ExpandPVCFailed` | PVC resize failed |
| `EventReasonPodReplaced` | `PodReplaced` | A drained pod was replaced |
| `EventReasonStatusError` | `StatusError` | Status update failed |
| `EventReasonStuckTerminating` | `StuckTerminating` | Pod stuck terminating |
| `EventReasonTopologyError` | `TopologyError` | etcd topology operation failed |
