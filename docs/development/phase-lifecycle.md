# Phase Lifecycle

## Overview

Every Multigres CRD exposes a `.status.phase` field representing its lifecycle state. The phase system enables operators and monitoring tools (including the [Observer](../../tools/observer/docs/observer.md)) to quickly assess cluster health without inspecting individual pods.

## Phase Values

| Phase | Description |
|:---|:---|
| `Initializing` | Resource has been created but has not yet reached its desired state for the first time. |
| `Progressing` | Resource exists but the observed state does not match the desired state (e.g., rolling update in progress, generation mismatch). |
| `Healthy` | Desired state reached — all replicas ready, generation current. |
| `Degraded` | One or more pods are crash-looping or in an unrecoverable error state. **Takes priority over all other non-Healthy phases.** |
| `Unknown` | Phase could not be determined. |

Phase values are defined in `api/v1alpha1/common_types.go`.

---

## Phase Computation by Resource

### Shard

The shard controller computes its phase in `updatePoolsStatus` and `updateMultiOrchStatus`:

1. **Degraded** — if any pool or multiorch pod is crash-looping (detected via `status.IsCrashLooping`).
2. **Healthy** — when `PoolsReady == true` AND `OrchReady == true` (all pool pods and the multiorch deployment are ready).
3. **Progressing** — otherwise (pods starting, rolling update in progress, etc.).

**Note:** Pods with drain annotations and terminating pods (non-nil `DeletionTimestamp`) are excluded from ready counts. This prevents draining pods from inflating readiness metrics.

### Cell

The cell controller uses `status.ComputeWorkloadPhase` on the MultiGateway Deployment:

1. **Degraded** — if any gateway pod is crash-looping.
2. **Healthy** — when `readyReplicas == desiredReplicas` and `observedGeneration == generation`.
3. **Progressing** — when generation is stale or not all replicas are ready.

The cell controller requeues after 30 seconds when not Healthy (see [Periodic Re-reconciliation](#periodic-re-reconciliation)).

### TopoServer

The TopoServer controller uses `status.ComputeWorkloadPhase` on the etcd StatefulSet:

1. **Degraded** — if any etcd pod is crash-looping.
2. **Healthy** — when `readyReplicas == desiredReplicas` and `observedGeneration == generation`.
3. **Progressing** — when generation is stale or not all replicas are ready.

The TopoServer controller requeues after 30 seconds when not Healthy (see [Periodic Re-reconciliation](#periodic-re-reconciliation)).

### MultigresCluster

The cluster controller aggregates phases from all children in `updateStatus`:

1. **Degraded** — if **any** child Cell, TableGroup, or TopoServer has `Phase == Degraded`.
2. **Healthy** — if **all** children have `Phase == Healthy` and current `observedGeneration`.
3. **Progressing** — otherwise.

Children with stale `observedGeneration` (spec updated but not yet reconciled) are treated as non-Healthy but not Degraded.

---

## Crash-Loop Detection

The `pkg/util/status` package provides two functions for detecting crash-looping pods:

### `IsCrashLooping(pod)`

Returns `true` if any container (regular or init) in the pod matches one of:

| Condition | Container State |
|:---|:---|
| `CrashLoopBackOff` | `Waiting` with reason `CrashLoopBackOff` |
| `OOMKilled` | `Waiting` with reason `OOMKilled` |
| `ImagePullBackOff` | `Waiting` with reason `ImagePullBackOff` |
| Repeated termination | `Terminated` with `RestartCount ≥ 3` |

The restart threshold (3) catches the gap between backoff restarts when the container is briefly in `Completed`/`Error` state before Kubernetes starts the next attempt.

**Note:** Both `ContainerStatuses` and `InitContainerStatuses` are checked. This is necessary because native sidecars (init containers with `restartPolicy: Always`, such as pgctld) report their status under `InitContainerStatuses`, not `ContainerStatuses`.

### `AnyCrashLooping(pods)`

Iterates over a pod slice, skipping pods with non-nil `DeletionTimestamp` (terminating pods), and returns `true` if any remaining pod is crash-looping.

### Why Degraded Trumps Other Phases

In `ComputeWorkloadPhase`, crash-loop detection runs **before** any other phase logic. This ensures that a resource with 2/3 replicas ready but one pod in CrashLoopBackOff reports `Degraded` rather than `Progressing`. The distinction matters because:

- **Progressing** implies the system is converging toward healthy — no action needed.
- **Degraded** implies the system is stuck and likely needs human intervention (bad image, misconfigured env vars, OOM).

---

## Periodic Re-reconciliation

Cell and TopoServer controllers implement a periodic requeue mechanism for non-Healthy states. When the computed phase is not `Healthy`, the controller returns `RequeueAfter: 30s` even if no watch event triggered the reconciliation.

**Why this is needed:** CrashLoopBackOff transitions are pod-level status changes that update `pod.status.containerStatuses[].state`. These changes do not trigger the `GenerationChangedPredicate` on the parent resource (which only watches for spec changes). Without periodic requeuing, a Cell or TopoServer could remain stuck in `Progressing` indefinitely while its pods are crash-looping, because no watch event would trigger re-evaluation.

The 30-second interval balances timely detection against unnecessary API server load.

---

## Phase Transition Diagram

```
                    ┌───────────────┐
                    │ Initializing  │
                    └───────┬───────┘
                            │ first reconcile
                            ▼
              ┌─────────────────────────────┐
              │                             │
              ▼                             ▼
     ┌────────────────┐          ┌──────────────────┐
     │  Progressing   │◀────────▶│     Healthy      │
     │ (converging)   │          │ (desired = actual)│
     └───────┬────────┘          └────────┬─────────┘
             │                            │
             │   crash-loop detected      │  crash-loop detected
             ▼                            ▼
     ┌────────────────────────────────────────────┐
     │                 Degraded                   │
     │  (pods crash-looping — needs intervention) │
     └────────────────────────────────────────────┘
             │
             │  crash-loop resolved
             ▼
     ┌────────────────┐          ┌──────────────────┐
     │  Progressing   │─────────▶│     Healthy      │
     └────────────────┘          └──────────────────┘
```

**Key transitions:**
- `Initializing → Progressing`: First reconcile begins.
- `Progressing → Healthy`: All replicas ready, generation current.
- `Healthy → Progressing`: Spec change triggers rolling update.
- `Any → Degraded`: Crash-looping pod detected (overrides all other phases).
- `Degraded → Progressing → Healthy`: Crash-loop resolves, pods recover.
