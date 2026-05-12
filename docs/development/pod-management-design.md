# Pod Management Design: StatefulSet → Direct Pod Management

> **Status**: Approved and Implemented
>
> **Goal**: Document the design for replacing StatefulSets with direct pod management in the operator, covering pod identity, decommissioning, failure scenarios, and scope limitations.
>
> **Implementation reference**: See [pod-management-architecture.md](./pod-management-architecture.md) for the current implementation details.

---

## Table of Contents

1. [Background](#1-background)
2. [Resource Topology: Before & After](#2-resource-topology-before--after)
3. [What We Gain & Lose (StatefulSet → Pods)](#3-what-we-gain--lose-statefulset--pods)
4. [Scope Limitations (v1alpha1)](#4-scope-limitations-v1alpha1)
5. [Pod Identity: Primary vs Replica](#5-pod-identity-primary-vs-replica)
6. [Pod Lifecycle & Decommissioning](#6-pod-lifecycle--decommissioning)
7. [Failure Scenarios](#7-failure-scenarios)
8. [Scaling Operations](#8-scaling-operations)
9. [Rolling Updates](#9-rolling-updates)
10. [pgBackRest & Backup Infrastructure](#10-pgbackrest--backup-infrastructure)
11. [Rename Prevention](#11-rename-prevention)
12. [Responsibility Matrix & Gaps](#12-responsibility-matrix--gaps-for-multigres-team-discussion)

---

## 1. Background

The operator currently uses StatefulSets to manage pool pods. However, multigres has its own identity and discovery system (via etcd topology) that is completely independent of Kubernetes StatefulSet identity. The operator uses `ParallelPodManagement` and does not leverage ordered deployment/scaling — the two main features that StatefulSets provide over Deployments.

The key motivation is that StatefulSets introduce limitations that conflict with multigres's own management model:
- Cannot target individual pods for decommissioning (must scale from the tail)
- Cannot manage pods across zones or cells independently
- Rename/replace operations become ambiguous with StatefulSet ordinal identity
- `PersistentVolumeClaimRetentionPolicy` is the only mechanism for PVC lifecycle

### How Multigres Works (Key Concepts)

Multigres manages its own cluster state through an **etcd topology store**. Each component discovers others by reading etcd — not through Kubernetes pod names or services:

- **Multipooler**: Registers itself in etcd at startup using `ts.RegisterMultiPooler()` with its `--service-id` (currently set to `$(POD_NAME)`). On shutdown, it sets `ServingStatus = NOT_SERVING` in etcd but **never deletes its registration** — the code comment says: *"If they are actually deleted, they need to be cleaned up outside the lifecycle of starting/stopping."*

- **Multiorch**: Discovers poolers by querying etcd (`GetMultiPoolersByCell()`), not the Kubernetes API. It health-checks them via gRPC, detects problems (dead primary, broken replication), and executes recovery actions (failover, replication fix). It does **not** know what a StatefulSet or a K8s Pod is.

- **Multigateway**: Discovers poolers via etcd topology watches (`WatchRecursive("poolers")`). It connects to poolers using the `Hostname:GRPCPort` from their etcd registration (`MultiPoolerInfo.Addr()`), completely independent of Kubernetes service discovery or DNS.

---

## 2. Resource Topology: Before & After

### Current (StatefulSets)

```ascii
[MultigresCluster] 🚀 (Root CR - user-editable)
      │
      ├── 📍 Defines [TemplateDefaults] (Cluster-wide default templates)
      │
      ├── 🌍 [GlobalTopoServer] (Child CR) ← 📄 Uses [CoreTemplate] OR inline [spec]
      │
      ├── 🤖 MultiAdmin Resources ← 📄 Uses [CoreTemplate] OR inline [spec]
      │
      ├── 💠 [Cell] (Child CR) ← 📄 Uses [CellTemplate] OR inline [spec]
      │    │
      │    ├── 🚪 MultiGateway Resources (Deployment + Service)
      │    └── 📡 [LocalTopoServer] (Child CR, optional)
      │
      └── 🗃️ [TableGroup] (Child CR)
           │
           └── 📦 [Shard] (Child CR) ← 📄 Uses [ShardTemplate] OR inline [spec]
                │
                ├── 🧠 MultiOrch Resources (Deployment)
                └── 🏊 Pools (per cell):
                     ├── StatefulSet  ← owns pods, owns data PVCs
                     ├── Headless Service  ← required by StatefulSet spec
                     └── Backup PVC (shared across pods in cell)
```

### After (Direct Pod Management)

```ascii
[MultigresCluster] 🚀 (Root CR - user-editable)
      │
      ├── 📍 Defines [TemplateDefaults] (Cluster-wide default templates)
      │
      ├── 🌍 [GlobalTopoServer] (Child CR) ← 📄 Uses [CoreTemplate] OR inline [spec]
      │
      ├── 🤖 MultiAdmin Resources ← 📄 Uses [CoreTemplate] OR inline [spec]
      │
      ├── 💠 [Cell] (Child CR) ← 📄 Uses [CellTemplate] OR inline [spec]
      │    │
      │    ├── 🚪 MultiGateway Resources (Deployment + Service)
      │    └── 📡 [LocalTopoServer] (Child CR, optional)
      │
      └── 🗃️ [TableGroup] (Child CR)
           │
           └── 📦 [Shard] (Child CR) ← 📄 Uses [ShardTemplate] OR inline [spec]
                │
                ├── 🧠 MultiOrch Resources (Deployment)
                └── 🏊 Pools (per cell):
                     ├── Pod-0  ← operator-managed, owns data PVC-0
                     ├── Pod-1  ← operator-managed, owns data PVC-1
                     ├── PVC-0  ← operator-managed (data)
                     ├── PVC-1  ← operator-managed (data)
                     └── Backup PVC (shared across pods in cell)
```

### Key Differences

| Aspect | Before (StatefulSet) | After (Direct Pods) |
|---|---|---|
| **Pod creation** | StatefulSet controller creates pods | Operator reconciler creates pods directly |
| **Pod recreation** | StatefulSet auto-recreates on deletion | Operator reconciler detects missing pods and recreates |
| **Data PVC creation** | StatefulSet `volumeClaimTemplates` | Operator creates PVCs explicitly |
| **PVC retention** | `PersistentVolumeClaimRetentionPolicy` | Operator decides based on `PVCDeletionPolicy` |
| **Headless Service** | Required by `spec.serviceName` | **Still needed for hostname resolution** — see note below |
| **Scaling** | Change `replicas` on StatefulSet | Operator creates/deletes individual pods |
| **Target-specific deletion** | Not possible (must scale from tail) | Delete any specific pod |

> [!WARNING]
> **Headless service is still needed for hostname resolution.** While multigateway discovers poolers via etcd (not K8s DNS), it connects using the `Hostname:GRPCPort` from the topology. The `Hostname` value comes from multipooler's `FullyQualifiedHostname()`, which resolves via DNS: `os.Hostname()` → `net.LookupHost()` → `net.LookupAddr()` → FQDN. With a headless service, this produces a resolvable FQDN (e.g., `pod-0.headless-svc.ns.svc.cluster.local`). Without it, the hostname may not be DNS-resolvable from other pods.
>
> **Decision**: Keep the headless service. Using `--hostname=$(POD_IP)` to register IP addresses instead of DNS names is not viable — Kubernetes recycles IP addresses, so stale etcd entries would point to the wrong pod after rescheduling.

---

## 3. What We Gain & Lose (StatefulSet → Pods)

### What We Gain

| Benefit | Detail |
|---|---|
| **Targeted decommissioning** | Can delete specific pods without affecting others. StatefulSets can only scale from ordinal N-1 downward. |
| **Independent zone/cell management** | Each cell's pods can be managed independently. No need for one StatefulSet per cell (which is what we already do). |
| **Simpler rename/replace** | Pods can be replaced with new names without fighting StatefulSet ordinal conventions. |
| **Direct PVC lifecycle control** | Operator explicitly creates and deletes PVCs instead of relying on `PersistentVolumeClaimRetentionPolicy` semantics. |
| **No GitOps drift** | StatefulSets report `replicas: N` in status which causes constant drift. Direct pods + operator-managed count avoids this. |
| **Better observability** | Operator has full visibility into which specific pods exist and their states, rather than relying on StatefulSet status. |
| **Headless service only for DNS** | Without StatefulSets, the headless service is only needed for pod hostname resolution (see [§2](#2-resource-topology-before--after) note). It is no longer a StatefulSet requirement — it is purely a DNS requirement. |

### What We Lose

| Loss | Mitigation |
|---|---|
| **Automatic pod recreation** | Operator's reconcile loop must detect missing pods and recreate them. This is straightforward — reconciler compares desired vs actual pod count. |
| **Rolling update coordination** | Operator must implement its own rolling update logic. However, since multigres already handles replication and failover, the operator just needs to delete/recreate pods one at a time. |
| **Stable network identity** | Not needed for identity — multigres uses etcd, not K8s DNS. However, the hostname registered in etcd must be resolvable (see [§2](#2-resource-topology-before--after) note on headless service). |
| **Ordered startup** | Not used — we already use `ParallelPodManagement`. |
| **PVC template auto-creation** | Operator must explicitly create PVCs for each pod. |

### Impact Assessment

**Low risk.** The critical functionality (replication, failover, backup/restore) is entirely within multigres and etcd — not in Kubernetes StatefulSet features. The operator's role is limited to:
1. Creating/deleting pods and PVCs
2. Passing the correct configuration to each pod
3. Reconciling desired state vs actual state

---

## 4. Scope Limitations (v1alpha1)

These constraints are already enforced or need to be enforced via CEL validation rules and webhook logic. We have enforced these to prevent users from doing things not supported by multigres or the operator at the current stage. For example, we don't support resharding or even multiple shards yet, so we prevent users from doing it.

### Already Enforced

| Constraint | Enforcement | CEL Rule |
|---|---|---|
| **Single database: `postgres`** | CEL on `databases` array | `self.all(db, db.name == 'postgres' && db.default == true)` |
| **Single shard: `0-inf`** | CEL on `shardName` | `self == '0-inf'` |
| **Pools are append-only** | CEL on pools map | `oldSelf.all(k, k in self)` — prevents removal or rename |

### Not Yet Enforced (Needs Review)

| Constraint | Status | Risk |
|---|---|---|
| **Cell rename prevention** | ✅ **Implemented** | CEL append-only rule added to `MultigresCluster` and `Shard` CRDs. Cells cannot be removed or renamed after creation. |
| **Pool replica count floor** | ✅ **Validated** | CRD enforces minimum=1. Webhook warns when a resolved pool cannot satisfy its durability policy (`AT_LEAST_2` needs 2 total poolers; `MULTI_CELL_AT_LEAST_2` needs 2 cells). |

### Explicitly NOT Supported (v1alpha1)

These operations are **not supported by the operator** and should be documented as such:

| Operation | Why Not Supported |
|---|---|
| **Resharding** | Not currently supported by multigres upstream either. The focus is exclusively on single shard for now. |
| **Adding new shards** | Not supported upstream. Multigres MVP enforces single shard `0-inf` via `ValidateMVPTableGroupAndShard()` in `constants/multischema.go`. |
| **Multiple databases** | Not supported upstream. Multigres MVP enforces single `default` tablegroup with shard `0-inf`. |
| **Renaming databases** | Would require re-registering the database in etcd under a new name and migrating all topology references. |
| **Renaming shards** | Same issue — all topology references are keyed by shard name. |
| **Renaming pools** | Already blocked by CEL append-only rule. Renaming would orphan pods and their PVCs. |
| **Renaming cells** | Not yet blocked but must be (see above). Same orphaning problem. |
| **Removing pools** | Already blocked by CEL. Would require draining connections, removing pods, cleaning etcd topology entries, and cleaning PVCs. |
| **Removing cells from a pool** | Not yet blocked. Would orphan pods and etcd registrations. |

### What Multigres Upstream Supports vs What the Operator Supports

> [!IMPORTANT]
> Multiple shards, multiple databases, and resharding are **not currently supported by multigres upstream** either. The upstream codebase enforces single shard `0-inf` and single tablegroup `default` via `ValidateMVPTableGroupAndShard()` in `go/common/constants/multischema.go`. The operator's CEL rules mirror this upstream MVP constraint. S3 backup support was added upstream recently (commit `62e1d94`).

| Feature | Multigres Upstream | Operator (v1alpha1) |
|---|---|---|
| Multiple databases | ❌ (MVP: single `default` tablegroup) | ❌ (locked to `postgres`) |
| Multiple shards | ❌ (MVP: locked to `0-inf`) | ❌ (locked to `0-inf`) |
| Multiple cells/zones | ✅ | ✅ |
| Multiple pools | ✅ | ✅ (append-only) |
| Resharding | ❌ (not in current roadmap) | ❌ |
| Backup to S3 | ✅ (recently added) | ✅ (via `credentialsSecret` or `useEnvCredentials`) |
| Backup to filesystem | ✅ | ✅ (via shared PVC) |
| Failover (auto) | ✅ (via multiorch) | ✅ (handled by multiorch) |
| pgBackRest TLS certs | ✅ (via cert-manager in demo) | ✅ (via auto-generated or user-provided Secret) |

---

## 5. Pod Identity: Primary vs Replica

### How Multigres Determines Primary vs Replica

**The operator does NOT need to distinguish primary from replica in pod naming.** Multigres determines roles internally:

1. **During bootstrap**: `multiorch`'s `BootstrapShardAction` acquires a distributed lock, selects a candidate from reachable poolers, and calls `InitializeEmptyPrimary` on it (with `ConsensusTerm: 1`). The other pods become standbys.
2. **After bootstrap**: Each multipooler reports its role to etcd (PRIMARY, REPLICA, DRAINED). Multiorch reads this from etcd. The Kubernetes pod name is irrelevant to the role assignment.
3. **During failover**: `multiorch`'s `AppointLeaderAction` promotes a standby to primary. Pod names don't change — only the `PoolerType` in etcd changes.

### Does the Operator Need to Know the Primary?

**Yes, in specific situations.** While the operator doesn't need to know for normal operations (multigres handles replication and failover internally), the operator DOES need primary awareness for:

1. **Scale-down**: To avoid accidentally deleting the primary (which would trigger an unnecessary failover)
2. **Rolling updates**: To update replicas first and the primary last (minimizing downtime)
3. **Status reporting**: To report primary/replica counts and roles in the MultigresCluster status

**How the operator can determine primary/replica**: The shard controller has an etcd client connection. It can read the pooler topology to check each pooler's `PoolerType` (PRIMARY, REPLICA, DRAINED). The `--service-id` registered in etcd matches the pod name (`$(POD_NAME)`), so the operator can correlate etcd entries with K8s pods.

### Pod Naming Scheme

The operator currently uses a **hierarchical naming scheme** with safety hashes defined in `pkg/util/name`. The naming pattern for pool resources is:

```
{cluster}-{db}-{tg}-{shard}-pool-{pool}-{cell}-{hash}
```

With StatefulSets, pods get an ordinal suffix appended by Kubernetes: `{sts-name}-{ordinal}`. StatefulSet names are constrained to 52 characters (reserving 11 chars for pod ordinal suffix and controller hash). When the name exceeds 52 chars, it's truncated with a `---` marker followed by the FNV-1a hash.

**Examples from the operator's naming package:**
- **StatefulSet**: `inline-postgres-default-0-inf---d8f1a` (truncated)
- **Pod (from STS)**: `inline-postgres-default-0-inf---d8f1a-0` (ordinal appended)
- **Short cluster**: `mycluster-postgres-default-0inf-pool-main-z1-a7c3d9e2`

**After removing StatefulSets**, pods can use the full 63-character DNS label limit since there's no STS ordinal suffix. The operator would create pods directly with names like:

```
{cluster}-{db}-{tg}-{shard}-pool-{pool}-{cell}-{hash}-{index}
```

Where `{index}` is a numeric index (0, 1, 2...) that the operator tracks. The hash ensures uniqueness across different pool/cell combinations even after truncation (see [Naming Strategy](naming-strategy.md) for the full naming design).

### Do NOT Name Pods by Role

Naming pods like `primary-0` or `replica-1` would be misleading because:
- After a failover, `replica-1` would actually be the primary
- The operator would need to rename pods after failover, which means deleting and recreating the pod (and potentially losing data if PVC is tied to the old name)
- Multigres does not use pod names to determine roles

---

## 6. Pod Lifecycle & Decommissioning

### Container Shutdown Ordering

In Kubernetes native sidecar semantics (see [KEP-753](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/753-sidecar-containers/README.md)), regular containers receive SIGTERM first and shut down before sidecars. For the pooler pod, this ordering
is important: multipooler's shutdown sequence drains in-flight queries,
closes the connection pool, and then calls `pgctld.Stop()` over gRPC to stop
PostgreSQL cleanly before announcing `STOPPED` to the orchestrator.

The operator therefore runs:

- **pgctld** as a native sidecar (init container with `restartPolicy: Always`)
- **multipooler** as a regular container

This guarantees pgctld outlives multipooler during pod termination: multipooler
receives SIGTERM first, runs its full graceful shutdown against a live pgctld
gRPC server, and only after multipooler exits does Kubernetes send SIGTERM to
pgctld. No preStop hooks, polling, or external coordination is required —
ordering is enforced by Kubernetes itself.

`TerminationGracePeriodSeconds` on the pool pod is sized to fit
multipooler's full shutdown sequence (drain + close pool + `pgctld.Stop()` +
announce STOPPED + multipooler's own safety-net timer) plus pgctld's own
`onterm-timeout` and headroom for slow `pg_ctl stop` before SIGKILL fires.

### What Happens on Multipooler Shutdown (SIGTERM)

When a pool pod receives SIGTERM (e.g., from `kubectl delete pod` or
operator-initiated deletion), the sequence is:

1. Kubernetes sends SIGTERM to **multipooler** only (the regular container).
   pgctld is a native sidecar and does not receive SIGTERM yet.
2. Multipooler runs its graceful shutdown.
3. The multipooler process exits.
4. **Only now** does Kubernetes send SIGTERM to pgctld. pgctld runs its
   `OnTermSync` hooks (gRPC `GracefulStop`, etc.) with no in-flight RPCs
   outstanding and exits.
5. Kubernetes terminates the pod.

**Critical: the multipooler does NOT delete its registration from etcd.** The entry persists with `ServingStatus = NOT_SERVING`. The rationale in the code is: *"For poolers, we don't un-register them on shutdown (they are persistent component). If they are actually deleted, they need to be cleaned up outside the lifecycle of starting/stopping."*

### Stale Etcd Entry Cleanup

There is **no active cleanup mechanism** for etcd topology entries of deleted pods:

| Mechanism | What It Cleans | Scope | Timing |
|---|---|---|---|
| `forgetLongUnseenInstances()` | Multiorch's **internal** `poolerStore` (in-memory) | Per-multiorch instance | 4 hours after last health check |
| Etcd topology entries | **Nothing cleans them automatically** | Global | Never |

This means:
- When the operator deletes a pod permanently (scale-down), the etcd topology still has an entry for that pod's service ID
- Multiorch will keep trying to health-check that entry (it'll fail with connection refused)
- After 4 hours, multiorch removes it from its internal store and stops health-checking
- But the etcd entry remains forever unless explicitly deleted

### Current Behavior with StatefulSets

> [!NOTE]
> The stale etcd entry problem **already exists today with StatefulSets**. When a user reduces `replicasPerCell` on a pool, the StatefulSet controller deletes the tail pod, but the operator does nothing to clean up that pod's etcd registration. The same 4-hour stale window applies. Moving to direct pod management does not make this worse — it just gives us the opportunity to do better.
>
> **Update (post-implementation)**: The drain state machine now calls `UnregisterMultiPooler` during the `ACKNOWLEDGED` state, fully cleaning up etcd entries on scale-down. Stale entries are no longer an issue.

### Implications for the Operator

> [!IMPORTANT]
> The operator SHOULD clean up etcd topology entries when permanently removing a pod. This is an improvement over the current StatefulSet behavior.

Options:
1. **Operator calls `ts.UnregisterMultiPooler()` on scale-down** — this function already exists in multigres's `CellStore` interface (`go/common/topoclient/store.go:170`, implementation in `multipooler.go:283`). It deletes the pooler's etcd entry by path. The shard controller has a topology store client and can call this directly.
2. **Operator calls `ts.UpdateMultiPoolerFields()` to set type to DRAINED** — this exists, but the stale entry still persists
3. **Wait for upstream multigres to add a decommission RPC** — the multipooler could accept a "decommission" command via gRPC before being killed
4. **Accept the 4-hour stale window** — multiorch handles it eventually, but during those 4 hours it'll log errors trying to reach the dead pooler

**Recommendation**: Use option 1. The `UnregisterMultiPooler` API already exists and is tested. The operator should call it during the `FINISHED` state of the drain state machine, after the pod's containers have fully terminated.

### Graceful Scale-Down Sequence

Scale-down is a multi-step process that spans several reconcile loops. Because the operator must coordinate with the Postgres replication layer (removing a standby from `synchronous_standby_names` before deleting its pod), doing everything in a single reconcile pass is fragile — if the operator crashes mid-sequence, the cluster could be left in an inconsistent state.

#### Drain State Machine

The recommended approach uses **pod annotations** to track the decommissioning state across reconcile loops. This ensures that each step is durable and resumable:

```
                    ┌──────────────────────────────────────┐
                    │          Scale-down detected         │
                    │ (desired replicas < actual replicas) │
                    └──────────────┬───────────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────────────┐
                    │          Select pod to drain         │
                    │ (see Pod Selection Algorithm below)  │
                    └──────────────┬───────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────┐
                    │  State: REQUESTED                    │
                    │  Annotation: drain.multigres.com/    │
                    │    state=requested                   │
                    │                                      │
                    │  Set by resource-handler to signal   │
                    │  that drain should begin             │
                    │  Then: Requeue                       │
                    └──────────────┬───────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────┐
                    │  State: DRAINING                     │
                    │  Annotation: drain.multigres.com/    │
                    │    state=draining                    │
                    │                                      │
                    │  Action: Call UpdateSynchronous-     │
                    │    StandbyList(REMOVE) on primary    │
                    │  Then: Requeue                       │
                    └──────────────┬───────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────┐
                    │  State: ACKNOWLEDGED                 │
                    │  Annotation: drain.multigres.com/    │
                    │    state=acknowledged                │
                    │                                      │
                    │  Guard: Verify the standby is        │
                    │    actually removed from sync list   │
                    │  Then: Requeue                       │
                    └──────────────┬───────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────┐
                    │  State: READY-FOR-DELETION           │
                    │  Annotation: drain.multigres.com/    │
                    │    state=ready-for-deletion          │
                    │                                      │
                    │  Action: Unregister from etcd,       │
                    │    delete PVC (if policy),           │
                    │    pod is garbage collected          │
                    └──────────────────────────────────────┘
```

**Key invariant**: At most one pod per pool should be in `FINISHED` state at a time. This guarantees that scale-down cannot accidentally delete multiple pods in one reconcile if the operator processes events faster than expected.

**Why annotation-based?** Each state transition is persisted to the Kubernetes API before proceeding. If the operator crashes at any point, the next reconcile reads the annotation and resumes from the correct state — no in-memory state is lost.

> [!NOTE]
> `UpdateSynchronousStandbyList` with `STANDBY_UPDATE_OPERATION_REMOVE` exists in the multipooler gRPC API. However, **multiorch never calls it** — its `FixReplicationAction` only handles ADD operations. This means the operator is the only component that performs standby removal today.

#### Scale-Down of a Primary Pod

When the operator wants to scale down and the target IS the primary:
1. Trigger a failover first (tell multiorch to appoint a new leader)
2. Wait for failover to complete
3. Then follow the drain state machine above for the now-demoted replica

---

## 7. Failure Scenarios

### 7.1 Replica Pod Deleted (PVC Retained)

**Trigger**: `kubectl delete pod <replica-pod>` or node failure with PVC on networked storage

**What happens**:
1. Pod is deleted → multipooler sets `NOT_SERVING` in etcd (if graceful shutdown)
2. Operator's reconcile loop detects missing pod
3. Operator recreates the pod with the same PVC
4. Pod starts → multipooler discovers existing PGDATA on the PVC
5. Multipooler's `MonitorPostgres` finds `dirInitialized = true` → starts PostgreSQL
6. Multiorch detects the pooler is back, configures streaming replication if needed
7. **Result: automatic recovery, no data loss, fast (seconds)**

### 7.2 Replica Pod + PVC Deleted

**Trigger**: `kubectl delete pod <replica-pod> && kubectl delete pvc <replica-pvc>` or node permanent failure with local storage

**What happens**:
1. Pod and PVC are gone
2. Operator's reconcile loop detects missing pod
3. Operator creates a new PVC and a new pod
4. Pod starts with empty PGDATA
5. Multipooler's `MonitorPostgres` finds `dirInitialized = false`, checks for backups
6. If backups exist → `pgbackrest restore --type=standby` → restores from latest backup
7. PostgreSQL starts, streaming replication catches up from primary
8. **Result: automatic recovery, no data loss, slower (depends on backup size)**

### 7.3 Primary Pod Deleted (PVC Retained)

**Trigger**: `kubectl delete pod <primary-pod>`

**What happens**:
1. Pod goes down → multipooler sets `NOT_SERVING` (if graceful)
2. Multiorch's `PrimaryIsDeadAnalyzer` detects the primary is unreachable
3. **Smart check**: If replicas are still connected to the primary's Postgres (verified via `PrimaryConnInfo` + `LastReceiveLsn` from health checks), multiorch assumes Postgres is still running and waits — it logs *"operator should restart pooler process"*
4. If Postgres is truly down: after the **failover grace period** (configurable, default ~4-12 seconds with jitter), multiorch triggers `AppointLeaderAction`
5. `AppointLeaderAction` selects the most up-to-date replica and promotes it to primary via consensus protocol
6. Meanwhile, operator's reconcile loop recreates the old primary pod with the same PVC
7. The old primary pod restarts — but by now a new primary exists
8. Multiorch detects `ProblemReplicaNotReplicating` on the old primary → configures it as a standby
9. If timeline divergence prevents direct replication: multiorch attempts `pg_rewind`. If that fails, marks the pooler as `DRAINED`.
10. **Result: automatic failover + recovery, potential brief write interruption**

### 7.4 Primary Pod + PVC Deleted

**Trigger**: Catastrophic loss of primary

**What happens**:
1. Same as 7.3 for failover (steps 2-5)
2. Operator creates a new PVC and new pod
3. New pod starts with empty PGDATA
4. Restore from pgBackRest backup → become standby of the new primary
5. **Result: automatic failover + restore from backup, data loss limited to unreplicated transactions**

### 7.5 All Pods Deleted (All PVCs Retained)

**What happens**:
1. Operator recreates all pods
2. All pods start with existing PGDATA
3. Multiorch detects `StalePrimary` or similar → bootstraps the shard again
4. Previous primary is typically re-elected if it has the most recent data

### 7.6 All Pods + All PVCs Deleted

**What happens**:
1. Operator recreates all pods with new PVCs
2. All pods start with empty PGDATA
3. If the **shared backup PVC** still exists → multiorch runs `BootstrapShardAction` → picks one as primary → `InitializeEmptyPrimary` → backup → others restore
4. If the backup PVC is also deleted → same bootstrap but **from an empty database** — all data is lost
5. **Result: full cluster re-bootstrap, data loss depends on backup PVC survival**

> [!CAUTION]
> The shared backup PVC is critical for disaster recovery when using filesystem-based backups. Losing the backup PVC means total data loss. S3-based backups eliminate this single point of failure and are the recommended production choice (see [§10](#10-pgbackrest--backup-infrastructure)).

---

## 8. Scaling Operations

### Scale Up (Add Replicas)

1. Operator creates a new PVC for the new replica
2. Operator creates a new pod referencing the new PVC and the shared backup PVC
3. Pod starts → multipooler registers in etcd → `MonitorPostgres` detects no PGDATA
4. Checks backup availability → restores from latest backup → starts as standby
5. Multiorch discovers new pooler → configures streaming replication → adds to `synchronous_standby_names`
6. **No operator-driven orchestration needed** — multigres handles everything automatically

### Scale Down (Remove Replicas)

Scale-down uses the drain state machine described in [§6](#graceful-scale-down-sequence):

1. Operator selects which pod to remove using the pod selection algorithm below
2. Operator annotates the pod to begin the drain sequence
3. Drain progresses over multiple reconcile loops: remove from sync standby list → verify → unregister from etcd → delete
4. Operator deletes the PVC (if `PVCDeletionPolicy` says so)
5. Etcd topology entry is cleaned up during the drain flow (via `UnregisterMultiPooler`)

### Pod Selection Algorithm

When scaling down, the operator must choose which pod to delete. The selection algorithm is:

1. **Never select the primary.** Read etcd topology to identify the current primary's `service-id` (which matches the pod name).
2. **Prefer pods that are not ready.** If any non-primary pods are in a non-ready state, prefer those (they're already not serving traffic).
3. **Among ready, non-primary pods: select the highest-index pod.** This makes the selection deterministic and predictable. The index refers to the numeric suffix in the pod name (e.g., pod `...-2` is preferred over `...-1`).
4. **If no deletable pod is found** (e.g., all pods are the primary or not ready), **do not scale down.** Requeue and try again later.

> [!WARNING]
> The operator must avoid deleting the primary pod during scale-down. Since multigres determines primary/replica roles internally, the operator must query the etcd topology to identify the current primary before choosing which pod to delete. If the operator accidentally deletes the primary, multiorch will handle failover automatically (see [§7.3](#73-primary-pod-deleted-pvc-retained)), but this causes unnecessary downtime.

### DRAINED Pooler Replacement

Multiorch may mark a pooler as `DRAINED` in etcd independently of the operator (e.g., during internal recovery or rebalancing). When this happens, the operator must:

1. **Detect the DRAINED pooler** by reading etcd topology during reconciliation and comparing `PoolerType` for each pod's `service-id`
2. **Create a replacement pod** to maintain the `replicasPerCell` count — the DRAINED pod no longer participates in quorum, so a new replica is needed
3. **Clean up the DRAINED pod** using the drain state machine from [§6](#graceful-scale-down-sequence) — since the pooler is already DRAINED, it can skip straight to removing from `synchronous_standby_names` (if still listed) and then deleting the pod and PVC

The key insight is that the operator's reconcile loop compares the **desired healthy replica count** (`replicasPerCell`) against the **actual healthy count** (pods whose etcd entry is PRIMARY or REPLICA, not DRAINED). A DRAINED pod counts as a deficit, triggering both scale-up (create replacement) and cleanup (remove the drained pod).

---

## 9. Rolling Updates

When the operator detects that a pod's spec has drifted from the desired state (e.g., image change, environment variable change, resource limit change), it must perform a rolling update. Since pod specs are **immutable** after creation, the only way to update a pod is to delete it and recreate it with the new spec.

### Drift Detection

The operator detects drift by comparing the **running pod's spec** against the **desired pod spec** generated from the Shard CR. The comparison should be limited to operator-managed fields:

| Field | Triggers Rolling Update? |
|---|---|
| Container images | Yes |
| Container command/args | Yes |
| Environment variables (operator-set) | Yes |
| Resource requests/limits | Yes |
| Volume mounts | Yes |
| Security context | Yes |
| Affinity/tolerations/node selector | Yes |
| Labels/annotations | No (updated in-place via patch) |
| Fields set by admission controllers (sidecars, injected env vars) | No |

> [!NOTE]
> Only fields that the operator explicitly sets should be compared. Admission controllers (e.g., Istio, Linkerd, Vault Agent) may inject sidecars, environment variables, or volume mounts that the operator didn't set. Comparing these would trigger unnecessary rollouts every time the operator reconciles. If this becomes a problem in practice, a hash-based approach (hashing only operator-controlled fields into a pod annotation at creation time) can be adopted to make detection reliable.

### Update Order: Replicas First, Primary Last

To minimize downtime during rolling updates:

1. **Collect all pods** in the pool that need updating (drift detected)
2. **Exclude the primary** from the initial pass
3. **Update replicas** one at a time, starting with the most lagged replica (least valuable data):
   - Delete the replica pod
   - Requeue (return from reconcile)
   - On next reconcile: the operator detects the missing pod and recreates it with the new spec
   - Wait for the new pod to become ready before proceeding to the next replica
4. **Update the primary** last:
   - Trigger a controlled switchover: tell multiorch to promote a healthy, up-to-date replica to primary
   - Wait for the switchover to complete (old primary becomes a replica)
   - Delete the old primary pod (now a replica) and recreate it with the new spec

### Delete-Then-Recreate Pattern

The operator does **not** attempt in-place pod updates. The flow is:

1. Delete the pod (Kubernetes terminates it; multipooler runs its graceful shutdown → sets `NOT_SERVING` in etcd)
2. The reconciler detects the pod is missing on the next loop
3. The reconciler creates a new pod with the updated spec, referencing the existing PVC
4. The new pod starts → multipooler discovers existing PGDATA → PostgreSQL starts → streaming replication resumes

This is safe because PVCs persist across pod deletions. The data is never lost during a rolling update.

### One Pod at a Time

The operator should update **at most one pod per reconcile loop** per pool. After deleting a pod, the reconciler returns early (requeue). On the next reconcile, it verifies the replacement pod is ready before moving to the next pod. This natural rate-limiting prevents cascading failures from updating too many pods simultaneously.

---

## 10. pgBackRest & Backup Infrastructure

### 10.1 Backup Architecture: Per-Shard & MultiAdmin Appointed

Backups in Multigres are **per-shard**. Each shard has its own pgBackRest "stanza" (configuration section) and repository.

**How a backup is triggered and executed:**

1.  **Trigger**: MultiAdmin receives a backup request (via CLI, API, or future scheduler).
2.  **Appointment**: MultiAdmin **selects a specific pod** to perform the backup.
    -   It iterates through all pods in the shard.
    -   It prefers a **REPLICA** to offload the backup overhead from the primary.
    -   If no replica is available (or `forcePrimary=true`), it selects the PRIMARY.
    -   *Logic location*: `go/services/multiadmin/backup.go:findPoolerForBackup`
3.  **Execution**: MultiAdmin sends a gRPC `Backup` request to the selected pooler's `multipooler` process.
4.  **Action**: The `multipooler` runs `pgbackrest backup` locally.
    -   It uses the shard-specific stanza name.
    -   It connects to the repository (Filesystem PVC or S3).
    -   If running on a replica, it connects to the primary's Postgres (via `pg2-host`) to fetch WAL files if needed.

> [!NOTE]
> **One pod does the work.** The backup does not run on all pods simultaneously. MultiAdmin orchestrates this uniqueness by appointing a single executor for each backup job.

### 10.2 Backup Storage Backends

The operator's `BackupConfig` API (v1alpha1) supports two storage backends:

#### A. Filesystem (Shared PVC)
-   **Mechanism**: A single PersistentVolumeClaim is mounted to `/backups` on **all pods** in the shard.
-   **Requirement**: The underlying storage class must support **ReadWriteMany (RWX)** if pods are scheduled on different nodes.
-   **Pros**: Simple for single-node testing (Kind) or NFS-backed clusters.
-   **Cons**: Single point of failure; performance bottlenecks; requires RWX storage.

#### B. S3 (Object Storage)
-   **Mechanism**: All pods connect directly to an S3 bucket. The backup volume uses `EmptyDir` instead of a PVC.
-   **Requirement**: AWS credentials via one of three mutually exclusive methods: `serviceAccountName` (IRSA — recommended for EKS), `credentialsSecret` (K8s Secret with `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), or EC2 instance metadata (default). Requires internet/VPC connectivity to S3.
-   **Pros**: No shared volume required; works across zones/regions; highly durable; standard production choice.
-   **Cons**: Requires external service setup (S3 bucket + credentials).

> **Note on the EmptyDir in S3 mode:** When using S3, pgbackrest writes directly to S3 and does not use the `/backups` mount path for its repository. The `EmptyDir` volume mounted at `/backups` is an **unused placeholder**. It exists because `buildSharedBackupVolume()` always returns a volume (PVC for filesystem, EmptyDir for S3) and the container spec always includes the corresponding mount — this keeps the pod spec and spec-hash logic unconditional, avoiding conditional volume mounts and the complexity they would introduce. In the future, we may want to remove the EmptyDir mount entirely for S3 mode to avoid confusion, but the current approach is simpler and has no runtime cost.

### 10.3 Configuration API

The `MultigresCluster` CRD now includes a `spec.backup` section used to configure the backend:

```yaml
spec:
  backup:
    type: "filesystem" # or "s3"
    filesystem:
      path: "/backups"
      storage:
        size: "10Gi"
        class: "standard-rwx"
    s3:
      bucket: "my-backups"
      region: "us-east-1"
      endpoint: "http://minio:9000"  # optional, for S3-compatible stores
      keyPrefix: "my-cluster"        # optional
      useEnvCredentials: true         # optional, for static creds from credentialsSecret
      credentialsSecret: "aws-creds" # optional, K8s Secret with AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
      serviceAccountName: "multigres-backup" # optional, for IRSA-based auth (mutually exclusive with credentialsSecret/useEnvCredentials)
```

This configuration is propagated to:
1.  **Etcd Topology**: The operator writes the backup configuration (S3 credentials, paths) into the `ClusterMetadata` or `ShardMetadata` in etcd.
2.  **PgBackRest Config**: The `multipooler` generates `pgbackrest.conf` based on the topology data.

### Understanding the pgBackRest Repository

A pgBackRest **repository** is the storage location where backups and WAL archives are kept. It is a **directory tree** (on a filesystem PVC or in an S3 bucket), not a single file. The internal structure looks like:

```
<repo-path>/
├── archive/multigres/          # Archived WAL segments
│   └── 16-1/                   # Timeline directory
│       ├── 0000000100000000/   # WAL segment files (compressed with zstandard)
│       └── ...
└── backup/multigres/           # Base backup sets
    ├── 20260101-120000F/       # Full backup (manifest + data files)
    ├── 20260108-120000D/       # Differential backup (changes since last full)
    └── ...
```

Two fundamentally different types of data live in this repository:

| | Base Backups (`backup/`) | WAL Archives (`archive/`) |
|---|---|---|
| **What** | Snapshot of the entire PostgreSQL data directory | Individual transaction log segments (16 MB each) |
| **When created** | On-demand: at bootstrap, or via explicit `pgbackrest backup` | Continuously: PostgreSQL pushes WAL segments via `archive_command` as transactions commit |
| **Purpose** | Starting point for a restore | Replay changes on top of a base backup to reach a specific point in time |
| **Required for restore?** | Yes — at least one base backup must exist | Yes — all WAL from the base backup to the target recovery point |

> [!IMPORTANT]
> **WAL archiving ≠ backups.** WAL archiving is continuous and automatic (driven by PostgreSQL's `archive_command`), but it only produces WAL segment files — not base backups. A restore requires **both**: a base backup as the starting point, plus WAL to replay forward. Without at least one base backup in the repository, WAL archives alone are useless for restore.

### When Base Backups Are Taken

Multigres does **not** have automatic periodic backup scheduling (no cron, no timer):

1. **Shard bootstrap** — `InitializeEmptyPrimary` takes a single `full` base backup after initializing the data directory, creating the stanza, and starting PostgreSQL. This is the **only** automatic base backup.
2. **On-demand** — The multipooler's `Backup` gRPC RPC can be called by multiadmin or the CLI (`multigres cluster backup`). Supports `full`, `diff` (differential), and `incr` (incremental) types. No component schedules these automatically.

#### How Multiple Base Backups Coexist

pgBackRest maintains a **catalog** of all backup sets. New backups **never** overwrite old ones — they are simply added to the catalog alongside existing ones:

```
backup/multigres/
├── 20260101-120000F         # Full backup from Jan 1 (bootstrap)
├── 20260108-120000D         # Differential from Jan 8 (changes since Jan 1 full)
├── 20260115-120000D         # Differential from Jan 15 (changes since Jan 1 full)
└── 20260201-120000F         # New full backup from Feb 1
```

- A **full** backup is a complete snapshot — independent of all others
- A **differential** backup stores only the data pages that changed since the **last full** backup. It references its parent full backup in its manifest.
- An **incremental** backup stores only changes since the **last backup of any type**. It references its parent (which could be full, diff, or incr).

When restoring, pgBackRest automatically resolves the dependency chain. For example, restoring from `20260115-120000D` means pgBackRest first restores `20260101-120000F` (the parent full), then applies the differential changes.

#### Retention and Cleanup

Retention is a **pgBackRest** feature (not an S3 lifecycle policy). pgBackRest has an `expire` command that deletes old backups exceeding the retention policy. This is configured via pgBackRest config settings, not cloud provider policies.

Multigres configures retention **differently** depending on the backup location:

| Setting | Filesystem (PVC) | S3 |
|---|---|---|
| `expire-auto` | `n` (disabled globally in the pgBackRest config template `config/pgbackrest/pgbackrest_template.conf`) | `n` (same template) |
| `repo1-retention-full` | Not set (no retention configured) | `28` days (hardcoded in `go/common/backup/constants.go`, applied in `config.go:PgBackRestConfig()`) |
| `repo1-retention-diff` | Not set | `1` (keep only the most recent differential) |
| `repo1-retention-full-type` | Not set | `time` (retention measured in days, not count) |

Because `expire-auto=n` globally, the `expire` command does **not** run automatically after backups in either case. Retention only takes effect if `pgbackrest expire` is called explicitly. This means on filesystem repos, old backups accumulate indefinitely.

> [!NOTE]
> **pgBackRest never deletes the last remaining full backup.** Even if retention says "keep 28 days" and the only full backup is 60 days old, pgBackRest will keep it. This is a built-in safety guarantee: at least one full backup always remains so that restore is possible. WAL archives needed by remaining backups are also preserved.

### When WAL Archiving Runs

WAL archiving is configured during `InitializeEmptyPrimary` in `postgresql.auto.conf`:

```
archive_mode = 'on'
archive_command = 'pgbackrest --stanza=<stanza> --config=<path> archive-push %p'
```

PostgreSQL has a built-in **archiver process** that runs continuously as a background worker. When a WAL segment is complete (16 MB of transaction log data), the archiver invokes the `archive_command` to ship it. In this case, the `archive_command` calls pgBackRest's `archive-push` subcommand, which handles compression (zstandard), transfer to the repository (filesystem or S3), and integrity verification.

In other words: **PostgreSQL decides WHEN to archive** (when a segment fills up), and **pgBackRest handles HOW** (compression, transport, storage). Multigres's role is simply to configure this at bootstrap by writing the `archive_mode` and `archive_command` settings — after that, the archiving runs autonomously without any multigres involvement.

### Replica Auto-Restore (MonitorPostgres)

Each multipooler runs a periodic `monitorPostgresIteration` loop. For new/uninitialized standbys (no PGDATA directory):

1. Calls `hasCompleteBackups()` to check if any complete **base backups** exist in the repository
2. If **yes**: restores from the latest complete base backup (`pgbackrest restore --type=standby`), replays archived WAL to catch up, then starts PostgreSQL in standby mode and connects to the primary for streaming replication
3. If **no**: logs `"directory not initialized and no backups available, waiting"` and does **nothing** — the standby waits indefinitely until a base backup appears

### Scenario: Adding Replicas After Months of Operation

**Situation**: The shard was bootstrapped 6 months ago. Only the initial bootstrap base backup exists. WAL has been archiving continuously. A scale-up adds a new replica.

**What happens**: The new standby restores the 6-month-old base backup, then replays **6 months of archived WAL segments**. This works — the data will be correct — but replaying months of WAL is extremely slow (potentially hours or days depending on write volume).

> [!WARNING]
> Without periodic base backups, replica initialization time grows unboundedly. The restore time is proportional to the WAL volume that must be replayed (all writes since the last base backup). A weekly full backup limits worst-case replay to ~7 days of WAL. A daily differential limits it further.
>
> **Recommendation**: The operator should implement scheduled base backups (via CronJob or shard controller timer) to keep restore times predictable. This also benefits disaster recovery, not just scale-up.

### Scenario: Accidental Data Deletion (Dropped Table)

**Situation**: A user accidentally drops a table at 14:30. Can the backup system recover the data?

**Answer**: pgBackRest natively supports **Point-in-Time Recovery (PITR)** — restoring to a specific timestamp before the destructive operation. However, **multigres does not currently expose PITR**. The `executePgBackrestRestore` function uses `--type=standby` exclusively, meaning it always restores to the latest state (including the dropped table).

#### How PITR Works Mechanically

PITR is a restore to a **single PostgreSQL instance**, not to all nodes at once. Here is how it works step by step:

1. **Stop PostgreSQL** on the instance you want to restore
2. **Run pgBackRest restore** with `--type=time --target="2026-01-15 14:29:00" --target-action=promote`
3. pgBackRest **automatically selects the best base backup** — it picks the most recent backup that was completed *before* the target timestamp. You do not need to specify `--set` (though you can). For example, if you have full backups from Jan 1 and Jan 8, and the target is Jan 15, pgBackRest picks the Jan 8 backup.
4. pgBackRest **restores the base backup** — the data directory is overwritten with the snapshot from Jan 8
5. pgBackRest writes `postgresql.auto.conf` with `restore_command` (to fetch WAL from the repository) and `recovery_target_time`
6. **PostgreSQL starts in recovery mode**, fetching archived WAL segments from the repository and replaying them one by one. It replays transactions from Jan 8 through Jan 15 at 14:29, then **stops** — the `DROP TABLE` at 14:30 is never replayed
7. PostgreSQL promotes to primary (because of `--target-action=promote`) and the database is usable with the table intact

#### Why WAL Is Not Tied to a Specific Backup

WAL is a **continuous, linear stream** of transaction log segments. It is not partitioned per backup — all backups share the same WAL archive. Any base backup can serve as a starting point, and WAL from that point forward can reach any subsequent moment in time. This is why:

- The base backup from Jan 1 + WAL from Jan 1 to Jan 15 = database at Jan 15
- The base backup from Jan 8 + WAL from Jan 8 to Jan 15 = same database at Jan 15
- The only difference is **how much WAL must be replayed** (7 days vs 14 days)

#### What Is the pgBackRest Repository Format?

The pgBackRest repository is **not** a generic folder of backup files. It is a highly structured, opinionated format managed entirely by pgBackRest itself. The repository contains:

- A **backup catalog** with manifests that record every file, its checksum, size, and which backup set it belongs to
- **Named backup sets** following a strict naming convention (e.g., `20260101-120000F` = full backup taken at 2026-01-01 12:00:00, suffix `F`=full / `D`=diff / `I`=incr)
- **Dependency metadata** linking differential and incremental backups to their parent backup
- **WAL archive segments** organized by PostgreSQL timeline and LSN

You **cannot** substitute your own backup files into a pgBackRest repository. All backup and restore operations must go through pgBackRest's own commands. The repository format enables pgBackRest to automatically resolve dependency chains, verify backup integrity, and select the correct backup for PITR.

#### Current Gap in Multigres

Multigres does not expose PITR. There are two distinct PITR use cases, each with a different procedure:

**Use Case A: Full Cluster Rollback** — Roll back the entire database to before the destructive operation. All data written after the target timestamp is **lost**.

1. Stop PostgreSQL on the primary
2. Delete the primary's PGDATA directory
3. Run `pgbackrest restore --type=time --target="<timestamp>" --target-action=promote` on the primary
4. Start PostgreSQL — it replays WAL to the target time, then promotes. The database is now as it was at the target timestamp.
5. **All replicas must be reinitialized** — their PGDATA is now diverged because the primary is on a new PostgreSQL timeline. Delete their PGDATA and let `MonitorPostgres` re-bootstrap them via backup restore.

**Use Case B: Selective Data Recovery** — Recover specific data (e.g., a dropped table) without losing changes made after the destructive operation.

1. Create a **separate, temporary pod** (e.g., a Kubernetes Job) with PostgreSQL + pgBackRest and access to the pgBackRest repository (backup PVC or S3 credentials). This pod is **not** part of the multigres cluster — it doesn't join consensus, doesn't register in etcd.
2. Run `pgbackrest restore --type=time --target="<timestamp>" --target-action=promote` inside this pod
3. Start PostgreSQL in the recovery pod — WAL replays to the target time
4. Connect to the recovery pod's database, `pg_dump` or `COPY` the recovered data
5. Re-insert the recovered data into the **running primary** of the actual cluster
6. Delete the temporary recovery pod

The existing cluster replicas are **not affected** by Use Case B — they continue streaming from the primary as normal.

Neither use case is currently automatable through multigres. Future versions could expose PITR via the multiadmin API or as an operator CRD feature (e.g., a `MultigresRestore` custom resource).

### Scenario: WAL Archiving Failure

**Situation**: The `archive_command` starts failing (e.g., backup PVC is full, S3 credentials expired, network issue).

**What happens**:

1. PostgreSQL detects the failed `archive_command` and **retries indefinitely** at a configurable interval (default: `archive_timeout` = 60 seconds)
2. Unarchived WAL segments accumulate in PostgreSQL's local `pg_wal/` directory
3. If `pg_wal/` fills the data PVC, PostgreSQL **stops accepting writes** to prevent data loss — it refuses to generate WAL it cannot archive
4. The primary logs archive failures, but **multigres has no specific monitoring for this condition** — there is no health check that alerts on sustained archive failure
5. During this window, any replica initialization will still work with the existing base backup and whatever WAL was archived *before* the failure — but the restored replica will be missing recent data

> [!WARNING]
> **WAL archiving failure should be detected and surfaced.** As discussed in Gap 1, the multipooler is the right component to monitor this (via `pg_stat_archiver`), since the operator has no direct connection to PostgreSQL inside the container. Once multigres reports archive health via etcd, the operator should surface it through status conditions, events, metrics, and logs — but must **never** trigger pod restarts or affect scheduling. Sustained archive failure silently degrades backup coverage and can eventually halt writes.

### Scenario: No Backup Exists At All

**Situation**: pgBackRest is configured but the stanza was never created, or the backup PVC was lost/corrupted, or S3 credentials were never valid.

**What happens**:

1. The primary operates normally — PostgreSQL runs, the `archive_command` fails on every WAL segment (logged as errors), but PostgreSQL itself continues serving reads/writes
2. `pg_wal/` grows continuously on the primary's data PVC since WAL cannot be archived
3. Any standby with no PGDATA enters the `reasonWaitingForBackup` state and **waits indefinitely** — there is no fallback mechanism (e.g., `pg_basebackup` from the primary)
4. Scale-up is effectively **blocked**: new pods are created but never become functional replicas
5. The operator should detect this condition (pods stuck without PGDATA for extended periods) and surface it as a cluster health warning



## 11. Rename Prevention

### Currently Blocked

| Entity | How Prevented | Where |
|---|---|---|
| **Pools** | CEL: `oldSelf.all(k, k in self)` | `multigrescluster_types.go`, `shardtemplate_types.go` |
| **Shard name** | CEL: `self == '0-inf'` | `multigrescluster_types.go` |
| **Database name** | CEL: `self.all(db, db.name == 'postgres' && db.default == true)` | `multigrescluster_types.go` |

### NOT Blocked (Action Required)

| Entity | Risk | Recommendation |
|---|---|---|
| **Cell names** | ✅ **Implemented** — CEL append-only rule added to `MultigresCluster` and `Shard` CRDs. |
| **Removing cells from a pool** | ✅ **Implemented** — Covered by the same CEL append-only rule. |

---

## 12. Responsibility Matrix & Gaps (For Multigres Team Discussion)

> [!IMPORTANT]
> This section consolidates all identified gaps from the design review. Each item assigns a **recommended owner** (multigres or operator) with rationale. Items where the operator *could* implement a workaround but *should not* are called out explicitly — we prefer to wait for proper upstream support rather than implement brittle hacks.

### Gap 1: WAL Archiving Failure Detection

**Status**: GH Issue filed ([multigres/multigres#654](https://github.com/multigres/multigres/issues/654)). In the 2026-02-23 meeting, Sugu confirmed this is part of a much larger "observability hole" in multigres that will be addressed upstream. It does not block the StatefulSet to Pods migration.

**Problem**: When the `archive_command` fails (PVC full, S3 credentials expired, network issue), PostgreSQL retries indefinitely and `pg_wal/` grows until the data PVC fills up, at which point PostgreSQL **stops accepting writes**. Neither multigres nor the operator currently monitors for this condition.

**Recommended owner**: **Multigres** (multipooler)

**Rationale**: The multipooler already runs a `monitorPostgresIteration` loop that periodically checks PostgreSQL health. It has direct access to the running PostgreSQL instance and can query `pg_stat_archiver` for `failed_count` and `last_archived_time`. However, **today the multipooler is completely unaware of WAL archiving failures** — the monitor loop only checks whether PostgreSQL is running, whether PGDATA is initialized, and whether backups exist. When `archive_command` fails, PostgreSQL retries it internally and the multipooler continues serving traffic normally with `ServingStatus = SERVING`. The readiness probe (`/ready`) stays healthy.

When sustained archive failure is detected, the multipooler should:
- Report a **degraded condition** via etcd (e.g., `ArchiveStatus: FAILING`, `LastArchivedTime`, `FailedCount`) so multiorch and the operator can observe it
- Log an explicit warning

> [!CAUTION]
> **This must NOT affect liveness or readiness probes.** WAL archiving failure is a repository/infrastructure issue (PVC full, S3 creds expired, network). Restarting the pod will not fix the root cause and will cause unnecessary downtime for a database that is otherwise serving queries correctly. The correct response is to alert the operator (human), not to restart the pod.

**Why not the operator?** The operator does not have a direct connection to PostgreSQL inside the container. It would need to either:
- SSH/exec into the container and run SQL queries — fragile, not idiomatic for a K8s operator
- Add a sidecar that periodically checks `pg_stat_archiver` — introduces unnecessary complexity
- Monitor `pg_wal/` directory size from outside the container — not possible without exec

All of these are inferior to the multipooler doing it internally, where it already has a PostgreSQL connection.

**Operator action**: Once multigres exposes this as a health signal (via etcd or gRPC), the operator should surface it through the full observability stack:
- **Status condition** on the Shard CR: `type: WALArchivingHealthy`, `status: False`, `reason: ArchiveCommandFailing`
- **Kubernetes Warning event** on the Shard CR when the condition transitions to `False`
- **Structured log** at warn level with archive failure details
- **Prometheus metric**: `multigres_operator_wal_archive_failed_count{cluster, shard}` gauge
- **Trace span annotation** on the shard reconciliation span when archive failure is detected

It should **never** trigger a pod restart or affect pod scheduling.

---

### Gap 2: Scheduled Base Backups

**Status**: Sugu noted in the 2026-02-23 meeting that backups are "very policy bound" and should be kept peripheral to the operator. The operator should at most be responsible for launching a command line at regular intervals, but the scheduling should be simple. We will NOT be implementing this.

**Problem**: Multigres only takes one automatic base backup — at shard bootstrap. After that, no base backups are ever taken unless someone manually triggers one via the CLI or gRPC API. Over time, this means:
- Replica initialization replays unbounded amounts of WAL (hours/days for long-running clusters)
- Disaster recovery restore time grows linearly with cluster age
- If WAL archive has a gap (even briefly), restore becomes impossible

**Recommended owner**: **Operator** (with multigres support for backup trigger)

**Rationale**: The multigres `Backup` gRPC RPC already exists and works. The operator can schedule periodic backups by:
- Creating a Kubernetes CronJob that calls the multipooler's `Backup` RPC
- Or implementing a timer in the shard controller that periodically triggers a backup

**Suggested schedule**: Weekly `full` backup + daily `differential` backup. This caps worst-case WAL replay at ~1 day for replica initialization.

**Multigres team input needed**: Is there a preference for where scheduling lives? Options:
1. **Operator CronJob** — operator creates a CronJob per shard that calls the `Backup` RPC on the primary's multipooler
2. **Operator controller timer** — shard controller tracks last backup time and triggers backup via gRPC when overdue
3. **Multigres internal** — multipooler or multiorch adds its own backup timer (would not require operator involvement)

We recommend **option 2 (controller timer)**. Here is a comparison:

| Aspect | CronJob (option 1) | Controller timer (option 2) | Multigres internal (option 3) |
|---|---|---|---|
| **Coordination** | Fires blindly — can trigger mid-rolling-update or mid-scale-down, conflicting with ongoing operations | Controller checks cluster state before triggering — can skip/defer if a rolling update, scale-down, or failover is in progress | No operator coordination — multigres has no visibility into K8s operations |
| **Extra resources** | Creates a CronJob + backup pods per shard, each needing gRPC connectivity, TLS certs, network policies | No extra resources — the operator controller already has gRPC access to the multipooler | No extra resources |
| **Status visibility** | Operator must poll CronJob/Job status separately to know backup state | Controller tracks `lastBackupTime` natively in cluster status conditions | Operator has no visibility — must query multigres for backup state |
| **CRD configurability** | Schedule in CRD, operator creates CronJobs | Schedule in CRD, operator reconciles directly | Not configurable via CRD unless multigres adds its own config |
| **Operator restart** | CronJob survives operator restart (K8s-managed) | Timer resets, but reconciliation re-checks `lastBackupTime` and triggers if overdue — self-healing | Independent of operator |

**Precedent**: CloudNativePG (CNPG), the gold-standard PostgreSQL operator, uses the controller timer approach. Their `ScheduledBackup` CRD triggers backup creation through the operator's reconciliation loop, not via CronJobs. CrunchyData PGO and Percona use CronJobs. Both patterns are valid Kubernetes practices, but the controller approach provides better coordination with other operator operations.

**Implementation requirements if we go with option 2**:
- Add `backupSchedule` field to the CRD (cron expression for full and differential backups)
- Track `lastFullBackupTime` and `lastDiffBackupTime` in the shard status
- On each reconciliation, check if the next scheduled backup is overdue
- Before triggering, verify the cluster is in a stable state (no rolling update, no scale operation, no failover in progress)
- Call the existing multigres `Backup` gRPC RPC on the primary's multipooler
- Surface backup schedule and health as cluster status conditions (e.g., `BackupScheduleActive: True`, `LastBackupTime: <timestamp>`)

---

### Gap 3: S3 Backup Support in the Operator — ✅ Resolved

**Status**: Fully implemented and verified with end-to-end Kind deployment.

All three required changes are complete:
1.  ✅ **`registerDatabaseInTopology`** in the shard controller's `shard_controller.go` now writes `BackupLocation_S3{Bucket, Region, Endpoint, KeyPrefix, UseEnvCredentials}` to etcd when backup type is S3.
2.  ✅ **AWS credentials injected** via the `s3EnvVars()` helper in resource-handler `containers.go`. Both `postgres` and `multipooler` containers receive `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` (from a referenced K8s Secret via `credentialsSecret`, or from pod-level env via `useEnvCredentials`).
3.  ✅ **Shared backup PVC skipped** for S3 — `reconcileSharedBackupPVC` returns early, and `buildSharedBackupVolume` uses `EmptyDir` instead of a PVC claim.

**API additions**: `S3BackupConfig` now includes `credentialsSecret` (name of a K8s Secret containing `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`).

---

### Gap 4: Standby Stuck Waiting for Backup

**Status**: GH Issue filed ([multigres/multigres#652](https://github.com/multigres/multigres/issues/652)). Sugu confirmed in the 2026-02-23 meeting that this intended behavior, but similarly to Gap 1, the lack of observability is a known issue. The operator should treat this as a reporting gap.

**Problem**: When a standby has no PGDATA and no base backup exists in the repository, it enters `reasonWaitingForBackup` and **waits indefinitely**. The multipooler's `ServingStatus` stays at `NOT_SERVING` — the same status it has during normal initialization, restore-in-progress, or when pgctld is unavailable. The reason string (`reasonWaitingForBackup`) is stored only in a private field (`pgMonitorLastLoggedReason`) used for log deduplication. It is **not** exposed via gRPC, etcd, or any health endpoint.

This means the operator sees a pod that is `NOT_SERVING` but has no way to distinguish between:
- **Normal startup** — PostgreSQL is initializing or restoring from backup (transient, will resolve)
- **Stuck waiting for backup** — no base backup exists, the pod will wait **forever** until one is created (requires intervention)

**Recommended owner**: **Multigres** (multipooler)

**Current probe behavior**: The Kubernetes readiness probe (`/ready`) checks only whether the multipooler's init completed without error — it does **not** check PostgreSQL state. A pod stuck in `reasonWaitingForBackup` passes the readiness probe (HTTP 200) because the multipooler itself initialized fine. Kubernetes probes are binary (pass/fail) and cannot convey a reason, so they are not the right mechanism for this.

**What we're asking multigres to do**: Write the monitor reason into the pooler's etcd topology entry alongside the existing `ServingStatus`. When `monitorPostgresIteration` calls `setMonitorReason()`, it should also update the etcd entry with a field like `monitor_reason: "waiting_for_backup"` (or `"restoring"`, `"starting"`, `"postgres_running"`). This is a small change — the multipooler already writes to its etcd entry for `ServingStatus` updates.

**How the operator reads it**: The shard controller already watches etcd topology for each shard. No new mechanism is needed — the `monitor_reason` field would appear in the existing topology data the operator already reads on every reconciliation.

**What the operator does with it** (concrete actions on the **Shard** resource — following the established pattern where conditions and events are set on the resource the controller directly manages):

1. **Status condition** on the `Shard` resource: set `type: StandbyWaitingForBackup`, `status: True`, `reason: NoBaseBackupAvailable`, `message: "Pod <pod-name> has been waiting for a base backup for <duration>. Run a manual backup or enable scheduled backups to resolve."` This is visible via `kubectl get shard -o yaml`. The shard controller already reads etcd topology during reconciliation, so this is a natural fit.

2. **Kubernetes Warning event** on the `Shard` resource: `reason: StandbyWaitingForBackup`, `message: "Standby pod <pod-name> cannot initialize — no base backup exists in the pgBackRest repository"`. This is visible via `kubectl describe shard` and `kubectl get events`.

3. **Metric**: Expose a gauge `multigres_operator_standby_waiting_for_backup{cluster, shard, pod}` (value 1 when stuck, 0 otherwise) for Prometheus alerting.

4. **Structured log**: `logger.Warn("standby waiting for backup", "pod", podName, "duration", timeSinceCreation, "shard", shardName)` — emitted once when the condition is first detected, not on every reconciliation.

**Why not the operator?** The operator can detect that a pod has been running for a long time without becoming Ready, but it cannot distinguish between "waiting for backup" and "restore in progress" or "PostgreSQL starting slowly" without the multipooler explicitly reporting its internal state via etcd.

---

### Gap 5: Etcd Topology Cleanup on Scale-Down — ✅ Resolved (API Exists)

**Status**: The required API already exists in multigres. No upstream changes needed.

**Problem**: When a pod is permanently deleted (scale-down), its etcd topology entry persists forever with `ServingStatus = NOT_SERVING`. Multiorch's `forgetLongUnseenInstances()` removes it from its internal in-memory store after 4 hours, but the etcd entry is never deleted.

**Resolution**: The `UnregisterMultiPooler` function already exists in multigres's `CellStore` interface (`go/common/topoclient/store.go:170`, implementation in `multipooler.go:283`). It deletes the pooler's etcd entry by path. It is part of the public `Store` interface and has test coverage. The multipooler's shutdown path deliberately does NOT call it (the comment says: *"If they are actually deleted, they need to be cleaned up outside the lifecycle of starting/stopping."*) — but this is by design: shutdown ≠ permanent removal.

**Operator action**: The operator should verify that the pod containers have terminated (after the pod's `DeletionTimestamp` is set), call `ts.UnregisterMultiPooler()`, and delete the PVC (if policy requires). The shard controller has a topology store client, so no new infrastructure is needed.

---

### Gap 6: Backup Health Reporting

**Status**: ✅ **API Exists** — The operator can call `multiadmin` `GetBackups` RPC directly.

**Problem**: The operator has no way to know whether backups are sufficient for replica initialization and disaster recovery. Specifically:

1. **Last backup status** — Did the most recent backup complete successfully, or did it fail mid-way? A failed backup (`BackupMetadata.Status == INCOMPLETE`) means the repository may not have a usable restore point.
2. **Last backup recency** — How old is the last successful base backup? If the only backup is from shard bootstrap (potentially months ago), replica initialization will replay unbounded WAL (see Gap 2). The operator needs to know if backups are stale.

> [!NOTE]
> **WAL archiving health** (is the `archive_command` keeping up?) is a separate concern covered by Gap 1. Gap 6 is strictly about base backup metadata — whether usable backups exist and how old they are.

**Recommended owner**: **Operator** (using existing `GetBackups` RPC)

**What already exists**: The `GetBackups` gRPC RPC already returns `BackupMetadata` with `status` (COMPLETE/INCOMPLETE), `backup_id` (which encodes the timestamp in pgBackRest format, e.g., `20260101-120000F`), `type` (full/diff/incr), and `backup_size_bytes`. The data is available — it's just not exposed proactively.

**Resolution**: The operator can call `GetBackups(limit=1)` on the `multiadmin` service during each shard reconciliation to get the latest backup metadata. This requires no upstream multigres changes.

**What the operator does with it** (concrete actions on the **Shard** resource):

1. **Status fields**: Add `lastBackupTime`, `lastBackupType`, and `lastBackupStatus` to `ShardStatus`. The operator parses the timestamp from the pgBackRest backup ID (format: `YYYYMMDD-HHMMSSF`).

2. **Status condition**: Set `type: BackupHealthy`, `status: True/False`:
   - `True` + `reason: BackupCurrent` when the last complete backup is within the configured threshold (default: 7 days for full)
   - `False` + `reason: BackupStale` + `message: "Last complete full backup was <duration> ago (threshold: 7d). Run a manual backup or check scheduled backup configuration."` when the backup is too old
   - `False` + `reason: BackupFailed` + `message: "Last backup attempt failed (status: INCOMPLETE). Check pgBackRest logs."` when the most recent backup is incomplete

3. **Kubernetes Warning event**: Emitted when the condition transitions to `False` — `reason: BackupStale` or `reason: BackupFailed`.

4. **Metric**: Expose `multigres_operator_last_backup_age_seconds{cluster, shard}` as a gauge for Prometheus alerting.

---

### Gap 7: pgBackRest TLS Certificate Handling

**Problem**: The multipooler's pgBackRest server requires TLS certificates for inter-node communication. Specifically, when a **standby** takes a backup, it connects to the pgBackRest TLS server running on the **primary** via `pg2-host-type=tls`, passing `--pg2-host-cert-file`, `--pg2-host-key-file`, and `--pg2-host-ca-file`. This requires server cert/key on the primary and matching client cert/key + CA cert on the standby. Without TLS certs provisioned, standby-initiated backups fail. In our current kind setup, this isn't an issue because we only run single-replica shards (primary takes its own backup locally), but it is a **blocker for multi-replica production deployments**.

**Recommended owner**: **Operator**

**When TLS is needed**: Only when pgBackRest communicates between pods. The pgBackRest config template (`pgbackrest_template.conf`) always configures `tls-server-*` settings, and multipooler passes `pg2-host-type=tls` when a standby connects to a primary for backup. So TLS certs must be provisioned for any shard with `replicas > 1`.

**Current state: `pkg/cert` is already implemented**

The generic `pkg/cert` module (previously `pkg/webhook/cert/`) is now fully operational and handles the webhook cert lifecycle. It already supports all the features needed for pgBackRest:

| Feature | Status | How it helps pgBackRest |
|---|---|---|
| `ExtKeyUsages` option | ✅ Implemented | pgBackRest requires `ServerAuth + ClientAuth` (mutual TLS) |
| `Organization` option | ✅ Implemented | Certs can use `"Multigres"` to match upstream convention |
| `AdditionalDNSNames` option | ✅ Implemented | Add pod-specific DNS SANs if needed |
| `PostReconcileHook` | ✅ Implemented | Not needed for pgBackRest (no webhook patching) |
| `Owner` reference | ✅ Implemented | Set to Shard CR for garbage collection |
| Background rotation | ✅ Implemented | Auto-rotates before expiry |

**Upstream pgBackRest TLS details** (from `config/pgbackrest/pgbackrest_template.conf` and `go/provisioner/local/tls.go`):

- **Expected cert files on disk**: `pgbackrest.crt`, `pgbackrest.key`, `ca.crt` in a configurable `CertDir`
- **Auth model**: `tls-server-auth=pgbackrest=*` — pgBackRest authorizes by Common Name. Any cert with CN `pgbackrest` is accepted. This means **all pods in a shard can share the same cert** — per-pod certs are not required.
- **ExtKeyUsage**: `ServerAuth + ClientAuth` (line 127 of upstream `tls.go`). Both sides (pgctld server on primary, multipooler client on standby) use the same cert for mutual TLS.
- **Command-line flags**: multipooler accepts `--pgbackrest-cert-file`, `--pgbackrest-key-file`, `--pgbackrest-ca-file`; pgctld reads from `CertDir` config.

**Current state: ✅ Fully Implemented**

The operator now fully supports pgBackRest TLS certificates with a dual-mode design:

1. **Auto-Generated Certificates (Default Mode)**
   - Uses the built-in `pkg/cert` module to generate a per-shard CA Secret (`{shard}-pgbackrest-ca`) and server TLS Secret (`{shard}-pgbackrest-tls`).
   - The certificates use `ExtKeyUsages: ServerAuth + ClientAuth` (mutual TLS) and Common Name `pgbackrest` (matching upstream `tls-server-auth=pgbackrest=*`).
   - Certificates are automatically rotated by the operator before expiration.

2. **User-Provided Certificates (cert-manager compatible)**
   - Users can provide an existing Secret via `spec.backup.pgbackrestTLS.secretName`.
   - The operator validates the Secret contains `ca.crt`, `tls.crt`, and `tls.key`.
   - **Crucial Detail**: Because external Secrets (like those created by cert-manager) lack the `app.kubernetes.io/managed-by: multigres-operator` label, they are invisible to the operator's default filtered informer cache. The shard controller uses an uncached `APIReader` to fetch and validate these Secrets directly from the API server.

**Volume Mounting Engine (Projected Volumes)**

Both modes use a unified volume mounting strategy:
- The operator creates a `projected` volume referencing the CA Secret and the TLS Secret.
- The TLS Secret items (`tls.crt` and `tls.key`) are explicitly renamed in the projection to `pgbackrest.crt` and `pgbackrest.key`.
- This renaming is necessary because pgctld strictly expects these specific filenames, while cert-manager outputs `tls.crt`/`tls.key`. By using projected volumes, the operator provides seamless cert-manager compatibility without requiring users to manually rename keys.

**CRD Additions**:
- `spec.backup.pgbackrestTLS.secretName` (optional) — reference to a user-provided TLS Secret. If unset, operator auto-generates.

---

### Gap 8: Graceful Decommission RPC — Closed

**Status**: GH Issue closed ([multigres/multigres#653](https://github.com/multigres/multigres/issues/653)). The operator's drain state machine now handles all safety-critical decommission steps (sync standby removal, etcd cleanup, PVC deletion). The remaining benefit (graceful connection draining) is marginal — by the time the pod is deleted, it is already unregistered from etcd and `terminationGracePeriodSeconds` (600s) gives in-flight queries time to complete.

**Problem**: Before the operator deletes a pod during scale-down, it would be ideal to tell the multipooler to decommission itself gracefully: drain active connections, remove itself from `synchronous_standby_names`, set type to `DRAINED` in etcd, and then exit cleanly. Currently the operator must do this piecemeal by calling `UpdateSynchronousStandbyList(REMOVE)` on the primary, then deleting the pod.

**Recommended owner**: **Multigres** (add a `Decommission` gRPC RPC)

**Rationale**: The multipooler is in the best position to perform a graceful decommission because it owns the PostgreSQL process, the query connection pool, and the etcd registration. A single `Decommission` RPC that performs all cleanup steps atomically is cleaner than the operator orchestrating multiple calls from outside.

**This is a nice-to-have, not a blocker.** The operator's drain state machine ([§6](#graceful-scale-down-sequence)) already handles the safety-critical part — removing the standby from `synchronous_standby_names` before deletion. A `Decommission` RPC would add two additional benefits, both trivial:

1. **Graceful connection draining** — the multipooler would close its query connection pool before shutdown, giving active queries a chance to complete instead of being severed on pod deletion. In practice, client applications should already handle reconnections, and the `terminationGracePeriodSeconds` window provides some buffer.
2. **Explicit DRAINED status in etcd** — the multipooler would set its type to `DRAINED` instead of `NOT_SERVING` on shutdown, giving multiorch a clearer signal. In practice, multiorch makes no distinction between the two — it forgets unreachable poolers after 4 hours regardless.

Neither benefit addresses a user-visible problem today. The investment is likely not worth it at this stage — the operator's existing drain state machine covers the critical safety invariants.

---

### Gap 9: Point-in-Time Recovery (PITR)

**Status**: Skipped. Per the 2026-02-23 meeting, this is a distinct product line that is far out right now.

**Problem**: pgBackRest natively supports PITR, but multigres `executePgBackrestRestore` always uses `--type=standby` (restore to latest state). There is no way to restore to a specific timestamp.

**Recommended owner**: **Multigres** (with operator CRD exposure later)

**Rationale**: PITR requires intimate knowledge of PostgreSQL recovery internals — setting `recovery_target_time`, `target_action`, managing timeline divergence, and handling the post-recovery state. This belongs in the multipooler, which already manages PostgreSQL lifecycle.

The multigres team should consider:
1. Adding a `--target-time` parameter to `executePgBackrestRestore` and exposing it via the `Restore` gRPC RPC
2. Adding a `--type=time` option alongside the existing `--type=standby`

Once multigres exposes PITR via gRPC, the operator could surface it as a `MultigresRestore` CRD or a CLI command.

**This is not urgent for v1alpha1** — PITR is an advanced feature. The priority should be getting scheduled backups working first (Gap 2) to ensure restore is fast and reliable.

---

### Summary Table

| # | Gap | Owner | Priority | Status |
|---|---|---|---|---|
| 1 | WAL archiving failure detection | Multigres | **High** | [Issue #654](https://github.com/multigres/multigres/issues/654) |
| 2 | Scheduled base backups | Operator (using multigres `Backup` RPC) | **High** | Will NOT implement with the operator |
| 3 | ~~S3 backup support in operator~~ | Operator | ✅ **Done** | Resolved — API, data handler, and resource handler fully wired |
| 4 | Standby stuck without backup (not surfaced) | Multigres | **Medium** | [Issue #652](https://github.com/multigres/multigres/issues/652) (Upstream observability improvement needed) |
| 5 | ~~Etcd cleanup on scale-down~~ | Operator (using multigres `UnregisterMultiPooler` API) | ✅ **Done** | Implemented — drain state machine calls `UnregisterMultiPooler` in `ACKNOWLEDGED` state |
| 6 | ~~Backup health reporting~~ | Operator (calling `GetBackups` RPC on multipooler) | ✅ **Done** | Implemented — `backup_health.go` calls `GetBackups`, sets `BackupHealthy` condition and metrics |
| 7 | ~~pgBackRest TLS certificate handling~~ | Operator | ✅ **Done** | Resolved — `pkg/cert` infra, volume builder, and APIReader fully wired |
| 8 | ~~Graceful decommission RPC~~ | Multigres | ✅ **Closed** | [Issue #653](https://github.com/multigres/multigres/issues/653) — operator drain state machine covers all safety-critical steps |
| 9 | Point-in-Time Recovery | Multigres | Low | Skipped |

> [!NOTE]
> During the 2026-02-23 design review meeting, it was approved for the operator to **read from the etcd topology** in order to correctly report status and correctly sequence operations like scale-down. The operator should be thoughtful to avoid excessive coupling, and interactions should be cataloged and audited. None of these upstream gaps block the StatefulSet-to-Direct-Pod migration.
