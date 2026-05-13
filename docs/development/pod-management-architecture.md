# Pod Management Architecture

> This document explains how the multigres-operator manages PostgreSQL pool pods directly, replacing the original StatefulSet-based approach. It covers the "why", "how", code structure, current feature set, and known gaps.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Why Direct Pod Management](#2-why-direct-pod-management)
3. [Resource Topology](#3-resource-topology)
4. [Pod Lifecycle](#4-pod-lifecycle)
5. [Drain State Machine](#5-drain-state-machine)
6. [Rolling Updates](#6-rolling-updates)
7. [Spec-Hash Drift Detection](#7-spec-hash-drift-detection)
8. [PVC Management](#8-pvc-management)
9. [Pod Disruption Budgets](#9-pod-disruption-budgets)
10. [Status Aggregation](#10-status-aggregation)
11. [Feature Summary](#11-feature-summary)
12. [Code Organization](#12-code-organization)
13. [Naming Conventions](#13-naming-conventions)
14. [Interaction with Multigres](#14-interaction-with-multigres)
15. [Known Gaps and Future Work](#15-known-gaps-and-future-work)

---

## 1. Overview

The operator manages **individual Pods and PersistentVolumeClaims** for each pool replica instead of delegating to Kubernetes StatefulSets. The shard controller (`pkg/resource-handler/controller/shard/`) owns Pod, PVC, PDB, headless Service, and ConfigMap resources directly, reconciling them on every loop.

This architectural change was motivated by the fact that multigres has its own identity and discovery system (etcd topology) that is completely independent of Kubernetes StatefulSet identity. The operator previously used `ParallelPodManagement` and did not leverage ordered deployment or scaling — the two main features that StatefulSets provide over Deployments.
> **Scheduling Note:** With parallel pod creation, all pods for a pool are submitted in the same reconcile pass. For backup volumes using `WaitForFirstConsumer` + RWO storage (e.g., EBS gp2), if multiple replicas are created simultaneously, the scheduler may place them on different nodes, causing `Multi-Attach` errors on the shared backup PVC. See [PVC Management § Shared Backup PVC](#shared-backup-pvc) for details and recommended solutions (S3 or RWX storage).

---

## 2. Why Direct Pod Management

### Problems with StatefulSets

| Problem | Impact |
|---|---|
| **No targeted decommissioning** | StatefulSets can only scale from ordinal N-1 downward. Deleting a specific replica (e.g., one whose data is corrupt or whose zone is draining) required external hacks. |
| **No independent zone/cell management** | Each cell already needed its own StatefulSet. The operator gained no simplification from StatefulSet identity guarantees. |
| **PVC lifecycle is opaque** | The only mechanism was `PersistentVolumeClaimRetentionPolicy`, which is limited and version-dependent. |
| **Rename/replace ambiguity** | StatefulSet ordinal identity conflicted with multigres's own etcd-based identity. Replacing a pod meant fighting the controller. |
| **GitOps drift** | StatefulSets report `replicas: N` in status, causing constant "configured" drift when using `kubectl apply`. |
| **Blocked rolling updates** | StatefulSet rolling update strategy (`OnDelete` or `RollingUpdate`) doesn't coordinate with multigres replication — it can't remove a standby from `synchronous_standby_names` before killing the pod. |

### What We Gained

| Benefit | Detail |
|---|---|
| **Targeted decommissioning** | Any specific pod can be drained and removed without affecting others. |
| **Simpler PVC lifecycle** | PVC creation/deletion is handled directly in the reconcile loop. Previously delegated to StatefulSet's `PersistentVolumeClaimRetentionPolicy`, which had version and behavior quirks. |
| **Drain state machine** | Scale-down coordinates drain and etcd cleanup within the shard controller to safely remove standbys from the sync standby list. |
| **Rolling updates with primary awareness** | Updates replicas first, primary last, with switchover coordination. |
| **Parallel creation** | New pods for a pool are all created in a single reconcile pass. This enables faster bootstrap by allowing multiple replicas to restore from backup simultaneously. Rolling updates and scale-down remain strictly one-at-a-time. |
| **Pod Disruption Budgets** | Per-pool PDBs with `maxUnavailable: 1` protect against voluntary evictions. |
| **Better observability** | Status aggregation directly from pod readiness conditions; no intermediate StatefulSet `.status` layer. |

### What We Didn't Lose

- **Automatic pod recreation**: The reconcile loop detects missing pods and recreates them.
- **Stable network identity**: Pods still get DNS-resolvable names via the headless service (hostname + subdomain).
- **Ordered startup**: Was never needed — `ParallelPodManagement` was already in use. Parallel creation restores this behavior for initial bootstrap. However, it introduces a scheduling trade-off for shared RWO volumes — see [PVC Management § Shared Backup PVC](#shared-backup-pvc).
- **PVC auto-creation**: Operator creates PVCs for each pod index, mirroring `volumeClaimTemplates` behavior.

---

## 3. Resource Topology

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
                ├── 🧠 MultiOrch (Deployment + Service, per-cell)
                └── 🏊 Pools (per pool × per cell):
                     ├── Pod-0  ← operator-managed
                     ├── Pod-1  ← operator-managed
                     ├── PVC-0  ← operator-managed (per-pod data)
                     ├── PVC-1  ← operator-managed (per-pod data)
                     ├── Backup PVC (shared across pods in cell)
                     ├── Headless Service (DNS resolution)
                     └── PodDisruptionBudget (maxUnavailable: 1)
```

Key points:
- **No StatefulSets** in the resource tree for pool pods.
- **Headless Service** is still required for DNS resolution. Multigres's `FullyQualifiedHostname()` uses DNS reverse lookup, so pods need resolvable FQDNs.
- **Shared Backup PVC** is per-shard-per-cell for filesystem backups; replaced with EmptyDir for S3. **Requires RWX storage or S3 for multi-replica cells** — see [PVC Management § Shared Backup PVC](#shared-backup-pvc).
- **PDB** is per-pool-per-cell to limit voluntary disruption.

---

## 4. Pod Lifecycle

### Creation (Scale-Up)

The `createMissingResources` function handles pod creation:

1. For each desired index `0..replicasPerCell-1`, check if a PVC and Pod exist.
2. Create missing PVCs first, then Pods that reference them.
3. **Parallel creation**: All missing PVCs and pods are created in a single reconcile pass. This enables faster bootstrap by allowing standbys to restore from pgBackRest (S3/repo) in parallel without I/O impact on the primary. Multiorch dynamically manages synchronous replication membership via `FixReplicationAction`, so parallel creation is safe from a quorum perspective.
4. Terminal pods (`Failed` or `Succeeded`) are cleaned up sequentially — one per reconcile — to avoid cascading deletions.

### Deletion (Scale-Down)

Scale-down uses the [drain state machine](#5-drain-state-machine). Extra pods beyond the desired count are identified, and the highest-index non-primary pod is selected for draining. Before initiating a new drain, the operator verifies that all non-draining, non-terminating pods in the pool are Ready. If the pool is already degraded, the scale-down is deferred and a `ScaleDownBlocked` warning event is emitted to prevent cascading failures.

### External Deletion

When a pod is deleted externally (e.g., `kubectl delete pod`), it enters the drain state machine to ensure etcd cleanup before the pod is fully removed.

### PVC Lifecycle via Owner References

PVC lifecycle is managed through **conditional owner references** based on the `PVCDeletionPolicy`:

- When `WhenDeleted` is `Delete`: PVCs are created with an ownerRef pointing to the Shard CR, enabling Kubernetes garbage collection to cascade-delete them when the Shard is removed.
- When `WhenDeleted` is `Retain`: PVCs are created without ownerRefs, ensuring they persist after Shard deletion.
- The shard controller's `reconcilePVCOwnerRefs` function ensures existing PVCs stay in sync with the current policy — adding or removing ownerRefs as the policy changes mid-lifecycle.
- During scale-down, `cleanupDrainedPod` checks `WhenScaled` and deletes data PVCs directly if the policy is `Delete` (the default). For DRAINED pods (identified by the `multigres.com/role=DRAINED` label), PVCs are always deleted regardless of the `WhenScaled` policy because DRAINED pod data is known-bad.

---

## 5. Drain State Machine

The drain state machine coordinates pod removal within the shard controller. The controller handles the full drain lifecycle: selecting which pod to drain, initiating the drain (removing from `synchronous_standby_names`, unregistering from etcd), and cleaning up the pod and PVC afterward.

### State Flow

```
                    ┌──────────────────────────────────┐
                    │       Scale-down detected        │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │     Pod selected for drain       │
                    │  (non-ready > non-primary >      │
                    │   highest index)                 │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │  state=requested                 │
                    │  + drain-requested-at timestamp  │
                    │  (set by resource-handler)       │
                    │                                  │
                    │  ⚠ Cancellable: if the desired   │
                    │  state reverts (e.g., scale-down │
                    │  reversed), the drain is cleared │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │  IsPrimaryNotReady guard         │
                    │  If the primary pod's containers │
                    │  are not ready, delay and        │
                    │  requeue (do not send RPCs)      │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │  state=draining                  │
                    │  Remove pod from synchronous     │
                    │  standby list on the primary     │
                    │  (with ReloadConfig: true)       │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │  IsPrimaryNotReady guard         │
                    │  (same check as above)           │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │  state=acknowledged              │
                    │  Verify standby removal is       │
                    │  reflected in pg_stat_replication │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │  state=ready-for-deletion        │
                    │  Unregister from etcd topology   │
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │  Shard controller cleans up:     │
                    │  1. Delete data PVC (if policy)  │
                    │  2. Pod is garbage collected     │
                    └──────────────────────────────────┘
```

### Pod Selection Algorithm

When choosing which pod to remove during scale-down:

1. **Strongly disfavor the primary (score penalty −1000).** The pod role is read from `shard.Status.PodRoles`, which the shard controller populates by reading etcd topology. The primary is never the *preferred* target, but it is **not hard-excluded**: if the primary is the only extra pod remaining (e.g. after all replicas have already been removed), it can still be selected to avoid deadlocking the scale-down. Rolling updates handle the primary separately via `handleRollingUpdates` (replicas first, primary last with switchover).
2. **Prefer non-ready pods.** Pods that are already failing are better candidates.
3. **Among ready non-primary pods, select the highest index.** This makes order deterministic and predictable.
4. **If no suitable pod is found, defer.** Requeue and try again.

### Safety Guarantees

- At most **one pod per pool** can be in the drain state at any time.
- Scale-down and rolling-update operations do not run concurrently — if a drain is in progress, rolling updates are deferred.
- If the topology store is temporarily unreachable, the drain annotation sits untouched and the shard controller retries on the next reconcile.
- **Health gate**: Scale-down drains are deferred when any non-draining pod is not Ready, preventing removal of pods from an already degraded pool.
- **Primary readiness guard**: The `IsPrimaryNotReady` check prevents sending `UpdateSynchronousStandbyList` RPCs to a primary whose containers are not ready. This avoids failed gRPC calls that could leave the drain in an inconsistent state.
- **Config reload on standby removal**: `UpdateSynchronousStandbyList` requests include `ReloadConfig: true`, ensuring PostgreSQL reloads `synchronous_standby_names` immediately after the standby is removed.
- **Stale drain cancellation**: If the desired state reverts while a drain is in the `requested` state (e.g., user reverses a scale-down), the drain annotations are cleared and the pod is kept. Once a drain enters the `draining` state, it cannot be cancelled because the RPC has already been sent.

### Shard/Cluster Deletion (No Drain)

The drain state machine is **not used during shard or cluster deletion**. When a Shard is deleted, the shard controller's `handleDeletion` runs: it unregisters the database from the global topology (`DeleteDatabase`) and deletes pods directly. PVCs are garbage-collected via owner references (when `WhenDeleted` is `Delete`) or retained (when `Retain`).

This is safe for full cluster deletion because the topo server is also being destroyed. For individual shard deletion within a live cluster, residual pooler entries may remain in topo — see [Known Gaps](#15-known-gaps-and-future-work).

The drain state machine remains fully functional for **rolling updates** and **scale-down**, where the cluster is alive and the shard controller is actively reconciling.

### Graceful Orphan Deletion (TableGroups and Cells)

When a database or cell is removed from the `MultigresCluster` spec, the resulting orphan resource is **not** deleted immediately. Instead, the MultigresCluster controller follows a 3-step `PendingDeletion` flow that routes through the drain state machine to prevent data loss:

1. **Annotate**: The orphan resource receives a `multigres.com/pending-deletion` annotation.
2. **Wait for ReadyForDeletion**: The child controller detects the annotation and takes appropriate cleanup action:
   - **TableGroup**: Propagates `PendingDeletion` to all child Shards and waits for all to reach `ReadyForDeletion` (i.e., all pods drained).
   - **Cell**: Sets `ReadyForDeletion` immediately since gateways are stateless.
3. **Delete**: Once the `ReadyForDeletion` condition is true, the MultigresCluster controller deletes the resource.

#### Topology Cleanup Timeout

During deletion, both Cell and Shard controllers use a `TopologyRegistered` / `DatabaseRegistered` status condition to determine whether topology cleanup is needed. This prevents two failure modes:

- **Never initialized**: If the condition was never set (e.g., the topology server was unreachable since creation), cleanup is skipped immediately — there's nothing to clean up.
- **Transient failure**: If the condition is `True` but the topology is temporarily unreachable, the controller retries for up to `topoCleanupTimeout` (currently 2 minutes). After the timeout, the controller force-skips cleanup with a `CleanupSkipped` warning event and proceeds with deletion.

> **Future consideration**: The 2-minute timeout is conservative for the current deployment model. As real-world usage patterns emerge, this value may need tuning. It could also be made configurable via the MultigresCluster spec if operators need per-cluster control.

---

## 6. Rolling Updates

When a pod's spec has drifted from the desired state (detected via spec-hash mismatch), the reconciler performs a rolling update:

1. **Identify drifted pods** — `podNeedsUpdate` compares each pod's `multigres.com/spec-hash` annotation against the hash of the currently desired spec.
2. **Skip if drain in progress** — Rolling updates are deferred if any pod is currently being drained (scale-down takes precedence).
3. **Update replicas first** — Non-primary drifted pods are selected first.
4. **Primary last** — When only the primary remains, a controlled switchover is needed before draining and recreating. (Note: the switchover is coordinated via the shard controller using the same drain annotation mechanism.)
5. **One at a time** — Only one pod is drained per reconcile cycle. The reconciler returns early after initiating a drain, waiting for the pod to reach `ready-for-deletion` before proceeding to the next.
6. **RollingUpdate status condition** — A `RollingUpdate` condition is set on the Shard to track progress (e.g., `"2/5 pods updated"`).

---

## 7. Spec-Hash Drift Detection

Since most pod spec fields are immutable after creation, the operator uses a **hash-based approach** to detect drift:

1. At pod creation, `ComputeSpecHash` produces an FNV-1a hex string over all operator-managed fields:
   - Container images, commands, args, env vars, resources, volume mounts, security contexts
   - Pod affinity, node selector, volume definitions, termination grace period
   - The `multigres.com/postgres-config-hash` annotation value (SHA-256 of the referenced ConfigMap key data), when a `postgresConfigRef` is set. This ensures ConfigMap content changes trigger rolling updates even though the pod spec itself is unchanged.
2. The hash is stored as annotation `multigres.com/spec-hash` on the pod.
3. On each reconcile, `podNeedsUpdate` builds the desired pod and computes its hash. If it doesn't match the existing pod's annotation, the pod is flagged for update.

This avoids false positives from admission controllers (Istio, Linkerd, Vault) that inject sidecars or environment variables — only fields the operator explicitly sets are included in the hash.

---

## 8. PVC Management

### Data PVCs

Each pool pod gets its own data PVC named `data-{base-name}-{index}`. These PVCs:
- Are created before the pod (if missing).
- Persist across pod deletions — a restarted pod reattaches to its existing PVC and finds its PGDATA intact.
- Are deleted during scale-down only if `PVCDeletionPolicy.WhenScaled` is `Delete`.
- During cluster/shard deletion, PVCs are garbage-collected by Kubernetes via conditional owner references when `PVCDeletionPolicy.WhenDeleted` is `Delete`. When the policy is `Retain`, PVCs have no ownerRef and persist after deletion.

### Shared Backup PVC

One shared PVC per shard per cell for filesystem-based backups, mounted at `/backups` on all pods. For S3 backups, this is replaced with an `EmptyDir` volume.

> **⚠️ Multi-Attach Limitation with RWO Storage:**
>
> When using standard block storage (EBS gp2/gp3) with `ReadWriteOnce` access mode and `WaitForFirstConsumer` binding, the sequential pod creation model causes `Multi-Attach` errors for multi-replica cells:
>
> 1. Pod-0 is created, scheduled to Node A. The CSI driver provisions the EBS volume in Node A's AZ and attaches it.
> 2. Pod-0 becomes Ready. The operator creates Pod-1 on the next reconcile.
> 3. Pod-1 hits the scheduler. The PV's `nodeAffinity` constrains it to the same AZ, but the scheduler does not detect the active RWO attachment on Node A. If a second node exists in the AZ, the scheduler may place Pod-1 there.
> 4. The `attachdetach-controller` attempts to attach the volume to Node B and fails with `Multi-Attach error`.
>
> **Why this didn't happen with StatefulSets (v0.2.6):** The old architecture used `ParallelPodManagement`, which submitted all pods to the scheduler simultaneously. With `WaitForFirstConsumer`, the PVC was still unbound when all pods entered the scheduling queue. The first pod to be processed annotated the PVC with `volume.kubernetes.io/selected-node`, constraining subsequent pods to the same AZ. With one node per AZ (common in dev/test EKS clusters), all pods landed on the same node deterministically. This prevented Multi-Attach errors but caused **silent HA loss** — all replicas on a single node. The current parallel creation approach has the same scheduling characteristic.
>
> **Solutions:**
> - **S3 (Recommended for production):** No shared PVC needed — the operator uses `EmptyDir` for scratch space and all pods connect to S3 independently.
> - **RWX Storage:** Use a StorageClass that supports `ReadWriteMany` (NFS, EFS, CephFS).
> - **Single replica per cell:** With `replicasPerCell: 1`, only one pod mounts the PVC and the issue does not arise.

See the [Backup Architecture](backup-architecture.md) document for the shared PVC design rationale.

### PVC Deletion Policy

The `PVCDeletionPolicy` type (`whenDeleted`, `whenScaled`) controls lifecycle. `WhenDeleted` defaults to `Retain` for data safety; `WhenScaled` defaults to `Delete` so pgbackrest is the source of truth for recovery. This is no longer tied to Kubernetes's `StatefulSetPersistentVolumeClaimRetentionPolicy` — the operator manages PVC deletion directly.

### PVC Volume Expansion

The operator supports in-place PVC volume expansion for data and backup PVCs. When a user increases `storage.size` on a pool, the operator patches the existing PVC's `spec.resources.requests.storage` to the new value. Kubernetes and the CSI driver handle the actual block device expansion.

**Requirements:**
- The `StorageClass` used by the PVC must have `allowVolumeExpansion: true`. Without this, the Kubernetes API server will reject the PVC patch and the operator will emit a `Warning` event.
- Volume expansion is **grow-only**. Decreasing `storage.size` is rejected at admission time — PVC shrinks are not supported by Kubernetes.

**How it works:**
1. The operator detects that the desired `storage.size` exceeds the current PVC spec.
2. The PVC spec is patched in-place (no pod restart needed for block device expansion).
3. For CSI drivers that support **online filesystem expansion** (EBS CSI ≥ v1.5, GCE PD CSI), the filesystem grows live without pod restart.
4. For CSI drivers that require pod restart for filesystem expansion, the operator detects the `FileSystemResizePending` PVC condition and initiates a drain on the affected pod via the existing drain state machine.

> **Note:** The same mechanism applies to shared backup PVCs when using filesystem-based backups.

---

## 9. Pod Disruption Budgets

For each pool-cell combination, the operator creates a `PodDisruptionBudget` with:
- `maxUnavailable: 1` — At most one pod can be voluntarily evicted at a time.
- Label selector matching pool pods in that cell.

This protects against node drains or Kubernetes upgrades taking down too many replicas simultaneously. The PDB is owned by the Shard CR for garbage collection.

---

## 10. Status Aggregation

The `updatePoolsStatus` function directly lists pods by label selector and aggregates:

| Status Field | Source |
|---|---|
| `shard.Status.PoolsReady` | `true` when `readyPods == totalPods > 0` |
| `shard.Status.ReadyReplicas` | Count of pods with `PodReady=True` condition |
| `shard.Status.OrchReady` | MultiOrch deployment readiness |
| `shard.Status.Phase` | `Degraded` if any pod is crash-looping; `Healthy` when both pools and orch are ready; `Progressing` otherwise. See [Phase Lifecycle](phase-lifecycle.md). |
| `shard.Status.Cells` | Observed cells from pod labels |
| `shard.Status.PodRoles` | Pod → role mapping (populated by the shard controller from etcd) |
| `shard.Status.LastBackupTime` | Timestamp of last completed backup (from `GetBackups` RPC) |
| `shard.Status.LastBackupType` | Type of last backup (full/diff/incr) |
| `shard.Status.Conditions` | `Available`, `BackupHealthy`, `DatabaseRegistered`, `RollingUpdate` |

Terminating pods (with a non-zero `DeletionTimestamp`) and draining pods (with drain annotations) are excluded from ready counts.

Metrics are emitted per pool via `monitoring.SetShardPoolReplicas()`. A `PoolEmpty` warning event is emitted when a cell has zero ready replicas but the desired count is > 0. Backup age is emitted via `monitoring.SetLastBackupAge()`.

---

## 11. Feature Summary

### Implemented

| Feature | Details |
|---|---|
| **Direct pod creation and recreation** | Pods created per-index, auto-recreated on failure |
| **Parallel pod creation** | All missing pods created in a single reconcile pass for fast bootstrap |
| **Per-pod data PVCs** | Explicitly created, named by index |
| **Shared backup PVC** | Per-shard-per-cell, skipped for S3 |
| **Headless Service for DNS** | Pod hostname + subdomain for FQDN resolution |
| **PodDisruptionBudgets** | `maxUnavailable: 1` per pool per cell |
| **Drain state machine** | Annotation-based, coordinated within the shard controller |
| **Pod selection for scale-down** | Primary avoidance, prefer non-ready, highest index |
| **Rolling updates** | Spec-hash drift detection, replicas first, primary last |
| **Owner-reference-based PVC cleanup** | Conditional ownerRefs on PVCs enable Kubernetes GC cascade-delete per `PVCDeletionPolicy` |
| **Shard deletion handling** | Deletes pods and deployments directly (no drain); PVCs garbage-collected via ownerRefs per policy |
| **Status aggregation from pods** | Direct pod count, no StatefulSet intermediary |
| **Zone/region scheduling** | `nodeSelector` injection from `CellTopologyLabels` |
| **Cell topology propagation** | Labels carry cluster/db/tg/shard/pool/cell hierarchy |
| **pgBackRest TLS certificates** | Auto-generated or user-provided (cert-manager compatible) |
| **S3 and filesystem backup support** | Full backup configuration propagation |
| **PVC deletion policy** | Hierarchical merge, Retain/Delete per whenDeleted/whenScaled |
| **Etcd topology cleanup** | `UnregisterMultiPooler` called during drain flow; stale entries removed on pod termination |
| **Topology registration & pruning** | Cell and database registration centralized in MultigresCluster controller; stale entries pruned when `topologyPruning.enabled` (default) |
| **Backup health reporting** | Shard controller calls `GetBackups` RPC, sets `BackupHealthy` condition and `LastBackupTime` status |
| **DRAINED pod handling** | DRAINED pods (diverged data, pg_rewind failure) are kept alive for admin investigation. Stand-in replicas created at next index for availability. Admin discards via `kubectl delete pod`, triggering drain + PVC deletion |
| **Scale-down health gate** | Drains deferred when pool has non-ready pods to prevent cascading failures |
| **Observability** | Events, conditions, metrics, tracing spans |

### Not Yet Implemented (Blocked on Upstream Multigres)

These features are designed and documented in `pod-management-design.md` but require upstream multigres changes:

| Feature | Gap | Notes |
|---|---|---|
| **WAL archiving failure detection** | Gap 1 | Needs multipooler to expose `pg_stat_archiver` via etcd |
| **Standby waiting-for-backup surfacing** | Gap 4 | Needs multipooler to expose `monitor_reason` via etcd |
| **Point-in-Time Recovery** | Gap 9 | Separate product feature, not urgent |

### Not Implemented (Design Decision)

| Feature | Reason |
|---|---|
| **Scheduled base backups** | Per design review meeting, kept peripheral to operator |

---

## 12. Code Organization

All pod management code lives in `pkg/resource-handler/controller/shard/`:

| File | Purpose |
|---|---|
| `shard_controller.go` | Main reconciler. Orchestrates pool reconciliation, sets up watches (including ConfigMap watches via `enqueueFromPostgresConfigMap` for `postgresConfigRef` changes), PVC owner-reference management. |
| `reconcile_pool_pods.go` | Core pod lifecycle: `reconcilePoolPods`, `createMissingResources`, `handleScaleDown`, `handleRollingUpdates`, `selectPodToDrain`, `cleanupDrainedPod`, `podNeedsUpdate`. |
| `reconcile_deletion.go` | Shard deletion: `handleDeletion`, topology unregistration, child resource cleanup. |
| `reconcile_multiorch.go` | MultiOrch deployment and service reconciliation. |
| `reconcile_shared_infra.go` | Shared infrastructure: pgBackRest TLS certs, pg_hba ConfigMap, postgres password Secret, shared backup PVC, PDB, headless service. |
| `reconcile_data_plane.go` | Data-plane reconciliation: pod role reporting, drain state machine execution, backup health evaluation. |
| `pool_pod.go` | Pod builder: `BuildPoolPod`, `BuildPoolPodName`, `ComputeSpecHash`. |
| `pool_pvc.go` | PVC builders: `BuildPoolDataPVC`, `BuildPoolDataPVCName`, `BuildSharedBackupPVC` (with conditional ownerRef support). |
| `pool_pdb.go` | PDB builder: `BuildPoolPodDisruptionBudget`. |
| `pool_service.go` | Headless service builder for DNS resolution. |
| `drain_helpers.go` | Drain utilities: `resolvePodRole` (reads `PodRoles` from shard status), `initiateDrain` (sets drain annotation). |
| `labels.go` | Label builder: `buildPoolLabelsWithCell` — creates the standard label set for pool resources. |
| `status.go` | Status aggregation: `updateStatus`, `updatePoolsStatus` (counts pods directly), `updateMultiOrchStatus`, `setConditions`. |
| `containers.go` | Container builders for pgctld and multipooler. Injects `POSTGRES_INITDB_ARGS` env var on pgctld when `shard.Spec.InitdbArgs` is set. Mounts the postgresql.conf ConfigMap and sets `POSTGRES_INITDB_EXTRA_CONF` on pgctld so the user-supplied lines are appended to pgctld's auto-tuned config. |
| `configmap.go` | ConfigMap builders: `BuildPgHbaConfigMap` (pg_hba template), `BuildPostgresConfigConfigMap` (postgresql.conf template with user overrides from `shard.Spec.PostgresConfig` appended). |
| `multiorch.go` | MultiOrch deployment and service builders. |
| `doc.go` | Package documentation. |

### Test Files

| File | Coverage |
|---|---|
| `pool_pod_test.go` | Pod builder, spec-hash computation, naming |
| `pool_pvc_test.go` | PVC builder, naming, storage class handling, ownerRef tests |
| `pool_service_test.go` | Headless service labels and selector |
| `shard_controller_test.go` | Unit tests for reconcile behavior (envtest-based) |
| `shard_controller_internal_test.go` | Internal function tests (podNeedsUpdate, etc.) |
| `reconcile_deletion_test.go` | Deletion handler envtest tests |
| `reconcile_deletion_internal_test.go` | Deletion handler internal tests |
| `integration_test.go` | Full integration tests: scale up/down, PDB creation, ConfigMap, services |
| `containers_test.go` | Container builder tests |
| `configmap_test.go` | pg_hba ConfigMap generation |
| `multiorch_test.go` | MultiOrch deployment/service builders |
| `secret_test.go` | Postgres password secret |
| `ports_test.go` | Port constant tests |

---

## 13. Naming Conventions

### Pod Names

```
{cluster}-{db}-{tg}-{shard}-pool-{pool}-{cell}-{hash}-{index}
```

Generated by `BuildPoolPodName` using `PodConstraints` (60 chars max for the base name, then `-{index}` suffix appended). The hash is an 8-character FNV-1a hex suffix ensuring uniqueness even after truncation.

### Data PVC Names

```
data-{cluster}-{db}-{tg}-{shard}-pool-{pool}-{cell}-{hash}-{index}
```

Same structure as pod names with a `data-` prefix.

### Shared Backup PVC Names

```
backup-data-{cluster}-{db}-{tg}-{shard}-{cell}-{hash}
```

### PDB Names

```
{cluster}-{db}-{tg}-{shard}-pool-{pool}-{cell}-{hash}-pdb
```

### Headless Service Names

```
{cluster}-{db}-{tg}-{shard}-pool-{pool}-{cell}-{hash}-headless
```

Pods reference this via `spec.subdomain`, combined with `spec.hostname` (set to the pod name) to produce FQDNs like `pod-name.headless-svc.namespace.svc.cluster.local`.

---

## 14. Interaction with Multigres

### Etcd Topology

The operator reads from etcd topology (via the shard controller) to:
- Determine pod roles (`PRIMARY`, `REPLICA`, `DRAINED`) for scale-down and rolling-update decisions.
- Clean up stale topology entries on permanent pod removal (`UnregisterMultiPooler`).

The operator writes to etcd topology (via the MultigresCluster controller) to:
- Register cells and databases during initial setup and on every reconcile.
- Prune stale cells and databases that no longer exist in the spec (when `topologyPruning.enabled`, the default).

The operator writes to etcd topology (via the shard controller) to:
- Unregister poolers during drain cleanup.

### gRPC Operations

Through the shard controller:
- `UpdateSynchronousStandbyList(REMOVE)` — Removes a standby before pod deletion.
- `UnregisterMultiPooler` — Deletes the etcd entry for a permanently removed pod.

### What Multigres Handles Autonomously

| Operation | Component | Notes |
|---|---|---|
| Primary election (bootstrap) | multiorch | `BootstrapShardAction` via consensus |
| Failover | multiorch | `AppointLeaderAction` on primary failure |
| Replication setup | multiorch | Detects new poolers, configures streaming replication |
| Sync standby list (ADD) | multiorch | `FixReplicationAction` |
| WAL archiving | PostgreSQL | `archive_command` → pgBackRest |
| Backup execution | multiadmin | Selects a replica, calls `Backup` RPC |
| Replica restore | multipooler | `MonitorPostgres` detects empty PGDATA, restores from backup |

### What the Operator Handles

| Operation | How |
|---|---|
| Pod creation and recreation | Direct pod management via reconcile loop |
| Scale-up (new replicas) | Create PVC + Pod, multigres handles the rest |
| Scale-down | Drain state machine with standby removal + etcd unregistration |
| Rolling update | Spec-hash detection, ordered recreation |
| DRAINED pod handling | Detects DRAINED role from etcd, keeps pod alive for investigation, creates stand-in replica |
| Backup health reporting | Calls `GetBackups` RPC, sets `BackupHealthy` condition |
| PVC lifecycle | Direct creation/deletion per policy |
| Certificate provisioning | `pkg/cert` for pgBackRest TLS |
| Status reporting | Aggregate from pod conditions, etcd roles, and backup metadata |

---

## 15. Known Gaps and Future Work

### Upstream Dependencies (GitHub Issues Filed)

| Gap | Issue | Description |
|---|---|---|
| WAL archiving failure detection | [#654](https://github.com/multigres/multigres/issues/654) | Multipooler should report `pg_stat_archiver` status via etcd |
| Standby stuck waiting for backup | [#652](https://github.com/multigres/multigres/issues/652) | Multipooler should expose `monitor_reason` in etcd |

### Operator-Side Future Work

- **Scheduled base backups**: Designed as a controller-timer approach (like CloudNativePG) but deferred per team decision.
- **Individual shard deletion drain cleanup**: When a single shard is deleted within a live cluster (not full cluster teardown), pooler entries may be left in topo because `handleDeletion` skips the drain state machine and deletes pods directly. Addressing this would require running the drain flow during deletion to unregister individual poolers before pod removal. Not addressed now because the operator currently only supports single-shard, single-database clusters — shards are only ever deleted as part of full cluster teardown where the topo server is also destroyed.

### Design Constraints (v1alpha1)

These constraints are enforced via CEL validation and prevent unsupported operations:

| Constraint | Enforcement |
|---|---|
| Single database (`postgres`) | CEL on databases array |
| Single shard (`0-inf`) | CEL on shard name |
| Pools are append-only | CEL prevents pool removal or rename |
| Cells are append-only | CEL prevents cell removal (on MultigresCluster and Shard) |
| Zone/region immutability | CEL on Cell spec fields |
