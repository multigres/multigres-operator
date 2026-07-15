# Operator Knowledge Reference

Use this when investigating observer findings. It maps observer checks to operator/upstream code paths.

## Architecture

```
MultigresCluster (user-created)
├── TopoServer (etcd StatefulSet — topology registry)
├── Cell (one per region)
│   └── Multigateway (Deployment — SQL routing proxy)
└── Database → TableGroup → Shard (logical hierarchy)
    ├── Multiorch (Deployment — orchestration, failover, primary election)
    └── Pool pods (managed directly, not via StatefulSet)
        ├── postgres (pgctld — PG lifecycle manager)
        └── multipooler (connection pooling, gRPC health)
```

### Two Controllers
- **resource-handler** (`pkg/resource-handler/`): Reconciles Shards → creates/updates/deletes pool pods, PVCs, services, deployments. Manages physical resources.
- **data-handler** (`pkg/data-handler/`): Reconciles topology → etcd registration, drain state machine, podRoles assignment. Manages logical state.

### Key Design Decisions
- **No finalizers** — relies on ownerReferences for cascade deletion
- **Status via Server-Side Apply (SSA)** — field owner `multigres-resource-handler`
- **Drain state machine** for graceful scale-down: `Requested` → `Draining` → `Acknowledged` → `ReadyForDeletion`

## Where Bugs Hide — By Observer Check

### `replication` errors
| Code Path | What to Check |
|-----------|--------------|
| `pkg/resource-handler/controller/shard/containers.go` | Pod names, `application_name` in PG config |
| `pkg/resource-handler/controller/shard/pool_pod.go` | Pod spec construction, replication config |
| `pkg/data-handler/controller/shard/drain.go` | `podMatchesPooler()` — was broken before (matched proto text instead of pod name) |
| Upstream: `go/cmd/multiorch/` | Failover logic, primary election, quorum decisions |
| Upstream: `go/cmd/pgctld/` | PG startup, replication slot setup, WAL config |

### `connectivity` errors
| Code Path | What to Check |
|-----------|--------------|
| `pkg/resource-handler/controller/shard/services.go` | Service creation, port definitions |
| `pkg/resource-handler/controller/multigateway/` | Gateway Deployment spec, port config |
| `pkg/resource-handler/controller/cell/` | Cell-level resource creation |
| `cmd/multigres-operator/main.go` | Operator metrics server configuration (port 8443, HTTPS) |
| Upstream: `go/cmd/multigateway/` | Routing logic, port binding, connection lifecycle |
| Upstream: `go/cmd/multipooler/` | gRPC server (port 15270), health endpoint, connection pool |
| Upstream: `go/cmd/multiorch/` | Pooler polling, gRPC Status RPCs to multipooler |

**Common connectivity chain:** Gateway SQL timeout → check multiorch logs for "pooler poll failed" → check multipooler gRPC health → check if port 15270 is actually listening.

**Operator metrics probe:** The observer probes `https://{podIP}:8443/metrics` on operator pods. Uses TLS with certificate verification disabled (cluster-internal). Checks for `multigres_operator_cluster_info` and `multigres_operator_webhook_request_total` metrics. Failures are `warn` severity since metrics are auxiliary.

### `drain-state` errors
| Code Path | What to Check |
|-----------|--------------|
| `pkg/data-handler/drain/drain.go` | State machine implementation, timeout handling |
| `pkg/data-handler/controller/shard/drain.go` | Drain orchestration, `handleScaleDown`, `inProgress` flag for concurrent prevention |

**Known bug area:** The `inProgress` flag prevents concurrent drains. If a drain gets stuck, check if `inProgress` is blocking subsequent operations.

### `crd-status` errors (generation divergence, phase, backup, cell fields)
| Code Path | What to Check |
|-----------|--------------|
| `pkg/resource-handler/controller/shard/shard_controller.go` | Status reconciliation, SSA apply, phase transitions |
| `api/v1alpha1/` | Status subresource definitions, phase constants |
| `api/v1alpha1/shard_types.go` | `LastBackupTime`, `LastBackupType` fields |
| `api/v1alpha1/cell_types.go` | `GatewayReplicas`, `GatewayReadyReplicas`, `GatewayServiceName` |
| `pkg/resource-handler/controller/cell/` | Cell status reconciliation, gateway replica counting |

**Known bug area:** SSA status updates can nil out fields (PodRoles, LastBackupTime) if the apply object doesn't include them. Check the SSA field owner and apply scope.

**Phase progression:** The observer tracks phase transitions across cycles. `Healthy→Initializing` is flagged as `fatal` (impossible under normal operation). Stuck-in-`Progressing` >10min indicates reconciliation is blocked.

**Backup staleness:** Checked when shard has `BackupHealthy` condition. `LastBackupTime` nil with backup configured → warn. Age >25h → warn, >49h → error. Unknown `LastBackupType` → warn.

**Cell status fields:** `GatewayReadyReplicas > GatewayReplicas` is impossible and indicates a status update bug. Cross-checked against actual gateway Deployment readyReplicas.

### `topology` errors
| Code Path | What to Check |
|-----------|--------------|
| `pkg/data-handler/topo/` | etcd registration, topology CRUD |
| `pkg/resource-handler/controller/toposerver/` | TopoServer CR reconciliation, etcd StatefulSet |

### `pod-health` errors
| Code Path | What to Check |
|-----------|--------------|
| `pkg/resource-handler/controller/shard/pool_pod.go` | Pod spec, readiness/liveness probes |
| `pkg/resource-handler/controller/shard/containers.go` | Container definitions, image refs |

### `resource-validation` errors
| Code Path | What to Check |
|-----------|--------------|
| `pkg/resource-handler/controller/` | ownerReference setup on all child resources |
| `pkg/resource-handler/controller/shard/pool_pvc.go` | PVC creation, naming (`BuildPoolDataPVCName`), labeling |
| `pkg/resource-handler/controller/shard/pool_pod.go` | Pod volume references to PVCs |
| `pkg/resource-handler/controller/shard/services.go` | Service creation, headless vs ClusterIP |
| `api/v1alpha1/` | CRD type definitions, webhook validation |

**PVC validation:** The observer lists all running pool pods and PVCs. For each pod, it resolves PVC references from `pod.spec.volumes`. Missing PVC → `fatal`, unbound PVC → `error`, orphaned PVC (multigres labels, no matching pod, beyond grace period) → `info`.

**Service endpoint validation:** Lists all multigres-managed Services (skips headless). Gets matching Endpoints object by name. Missing endpoints → `warn`, zero ready addresses → `warn`.

### `operator-logs` / `dataplane-logs` errors
These surface errors from component logs directly. Trace the error message to the specific component's code.

## Upstream vs Operator — Decision Tree

```
Observer reports an error
│
├─ Error about resource creation/deletion/status/ownership?
│  └─ Operator bug (resource-handler or data-handler)
│
├─ Error about connectivity/protocol/gRPC?
│  ├─ Service exists with correct ports? ─ No → Operator bug (service creation)
│  └─ Yes → Check component logs for actual error
│     ├─ "method not implemented" → Version mismatch (operator sets wrong image)
│     ├─ "DeadlineExceeded" on gRPC → Upstream bug (multipooler/multiorch)
│     └─ Connection refused → Check if port is bound (upstream startup issue)
│
├─ Error about replication/failover/split-brain?
│  ├─ Pod spec has correct replication config? ─ No → Operator bug (containers.go)
│  └─ Yes → Upstream bug (pgctld or multiorch)
│     ├─ "PrimaryIsDead" + "insufficient poolers" → multiorch quorum issue
│     └─ Split-brain → multiorch failover logic or fencing failure
│
└─ Error about topology/etcd?
   ├─ TopoServer pods running? ─ No → Operator bug (toposerver controller)
   └─ Yes → Check etcd health directly, may be upstream config
```

## Version Checking

Default image tags are compiled into `api/v1alpha1/image_defaults.go`. Tags use format `sha-XXXXXXX` (multigres repo commit SHA).

```bash
# What images are running
KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl get pods -A \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .spec.containers[*]}{.image}{" "}{end}{"\n"}{end}' \
  | grep multigres

# What the operator defaults to
cat api/v1alpha1/image_defaults.go

# Compare with upstream HEAD
cd /tmp/multigres && git log --oneline -5
```

If a bug exists on an older upstream SHA but is already fixed on `main`, report this — the operator may need an image bump. Do not change image defaults yourself.

## Key Ports

| Component | Port | Protocol | Purpose |
|-----------|------|----------|---------|
| Multigateway | 5432 | TCP | SQL proxy (client connections) |
| Multigateway | 15100 | HTTP | Health/readiness |
| Multiorch | 15300 | HTTP | Health/readiness |
| Multiorch | 15370 | gRPC | Orchestration RPCs |
| Multipooler | 15200 | HTTP | Health/readiness |
| Multipooler | 15270 | gRPC | Pool management, Status RPCs |
| PostgreSQL | 5432 | TCP | Direct PG connections |
| etcd | 2379 | HTTP | Client API |
| etcd | 2380 | HTTP | Peer communication |
| Operator | 8081 | HTTP | Health/readiness |
| Operator | 8443 | HTTPS | Prometheus metrics |
