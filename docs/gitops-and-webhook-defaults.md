# Webhook Default Materialisation & GitOps

This document explains how the Multigres Operator uses its **Mutating Webhook** to materialise defaults into the `MultigresCluster` spec, why certain fields are intentionally left dynamic, and how to work with this behavior in GitOps workflows.

## Why Materialise Defaults?

Many Kubernetes operators apply defaults **in-memory** during reconciliation. This approach has a major user-facing problem: running `kubectl get multigrescluster -o yaml` will show a sparse spec that doesn't reflect what the operator is actually enforcing. Image versions, replica counts, resource limits, backup paths — all hidden.

The Multigres Operator solves this with a **Mutating Webhook** that writes defaults directly into the stored spec in etcd. When you run `kubectl get multigrescluster -o yaml`, you see *exactly* what the operator is using — no guessing, no hidden state.

### What Gets Materialised

On every `CREATE` and `UPDATE` of a `MultigresCluster`, the webhook materialises:

| Field | Default Value |
| :--- | :--- |
| `spec.images.*` (all 7 component images) | Compiled-in image tags (SHA-pinned) |
| `spec.images.imagePullPolicy` | `IfNotPresent` |
| `spec.topologyPruning.enabled` | `true` |
| `spec.durabilityPolicy` | `AT_LEAST_2` |
| `spec.logLevels.*` | `info` (all 5 components) |
| `spec.pvcDeletionPolicy` | `Retain / Delete` |
| `spec.backup` | Filesystem, `/backups`, `10Gi` |
| `spec.databases` (system catalog) | `postgres` database, `default` table group, `0-inf` shard |
| `spec.globalTopoServer` | Managed etcd (3 replicas, 1Gi storage) |
| `spec.multiAdmin` | 1 replica, default resources |
| `spec.multiAdminWeb` | 1 replica, default resources |
| Per-cell `spec.multiGateway` | 1 replica, default resources |
| Per-shard `spec.multiOrch` | 1 replica, default resources |
| Per-shard `spec.pools` | `default` pool with 1 replica per selected cell, 1Gi storage, default container resources |
| Per-shard `spec.backup` | Filesystem, default path and storage |
| `spec.templateDefaults` | Promoted to `"default"` if a matching template exists in the namespace |

All of this is visible via `kubectl get multigrescluster -o yaml`.

### Fallback Path

The webhook is the **primary** path, but the operator does not depend on it. The reconciler applies the exact same defaults in-memory using the same shared `resolver` package. If the webhook is unavailable (e.g., during local development without cert-manager), the operator still functions — defaults are just "invisible" to `kubectl get`.

---

## Intentionally Dynamic Fields

Some fields are **intentionally NOT materialised** into the parent `MultigresCluster` spec because they are resolved dynamically at reconcile time.

### 1. Shard Cell Assignments (`multiOrch.cells`, `pool.cells`)

### Why They Stay Empty

When a user defines a shard without specifying cells, the intent is **"run on all cells"**. This is a *dynamic* intent that should adapt as the cluster changes.

If the webhook baked in the cell list at admission time, it would create a problem:

**Initial apply** — cluster has cells `us-east` and `eu-west`:
```yaml
spec:
  cells:
    - name: us-east
    - name: eu-west
  databases:
    - name: postgres
      tablegroups:
        - name: default
          shards:
            - name: "0-inf"
              # User leaves cells empty — means "all cells"
```

If the webhook materialised cells, the stored spec would become:
```yaml
shards:
  - name: "0-inf"
    spec:
      multiOrch:
        cells: [eu-west, us-east]   # ← Frozen snapshot
      pools:
        default:
          cells: [eu-west, us-east] # ← Frozen snapshot
```

**Later, user adds a third cell** (`ap-south`). The shard's stored cells are now stale — `ap-south` is missing. The user would have to manually update every shard to add the new cell.

By keeping cells as `nil` in the stored spec, the operator resolves them **dynamically at reconcile time** from the current cell list. Adding a new cell automatically expands all shards to include it.

> **Rule of thumb:** `cells: nil` means "run everywhere" (dynamic). `cells: [us-east]` means "run only here" (static, user-specified).

### Viewing the Resolved State

The resolved cell assignments are visible on the **child CRs**, not the parent. The reconciler writes the fully resolved configuration into these resources:

**On the Shard child CR** (`kubectl get shard <name> -o yaml`):
```yaml
spec:
  multiorch:
    cells: [ap-south, eu-west, us-east]   # ← Fully resolved
  pools:
    default:
      cells: [ap-south, eu-west, us-east] # ← Fully resolved
status:
  cells: [ap-south, eu-west, us-east]     # ← Current deployment state
  podRoles:
    my-cluster-...-ap-south-0: PRIMARY
    my-cluster-...-eu-west-0: PRIMARY
    my-cluster-...-us-east-0: PRIMARY
```

**On the TableGroup child CR** (`kubectl get tablegroup <name> -o yaml`):
```yaml
spec:
  shards:
    - name: "0-inf"
      multiorch:
        cells: [ap-south, eu-west, us-east]
      pools:
        default:
          cells: [ap-south, eu-west, us-east]
```

| Resource | Where to look | What it shows |
| :--- | :--- | :--- |
| `MultigresCluster` | `spec.databases[].tableGroups[].shards[]` | User intent (`nil` = all cells) |
| `Shard` child CR | `spec.multiorch.cells`, `spec.pools.*.cells` | Resolved cell list |
| `Shard` child CR | `status.cells` | Currently deployed cells |
| `TableGroup` child CR | `spec.shards[].multiorch.cells` | Resolved cell list |

### 2. Observability Configuration (`spec.observability`)

The `ObservabilityConfig` in the `MultigresCluster` spec controls OpenTelemetry settings (OTLP endpoint, exporters, sampling) for data-plane components. These fields have a **dual-source fallback**: if a field is empty in the CRD, the operator falls back to its own `OTEL_*` environment variables at reconcile time.

For example, if the operator is deployed with:
```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
OTEL_TRACES_EXPORTER=otlp
```

And the `MultigresCluster` has no `spec.observability` section, the reconciler will still inject these values as environment variables into data-plane pods (multiorch, multipooler, multigateway, etc.) — inherited from the operator's own environment.

**Why it can't be materialised:** The webhook runs in the same process as the operator, so it *could* read `os.Getenv()`. However, these values are **operator deployment configuration**, not cluster-specific defaults. Materialising them into the CRD spec would incorrectly couple the operator's deployment config to the user's cluster manifest. If the platform team later changes the operator's collector endpoint, they'd have to update every previously-materialised `MultigresCluster` spec too.

**How to see the resolved state:** The observability config is propagated to child CRs. Check any `Shard` or `Cell` child CR:
```bash
kubectl get shard <name> -o yaml
```
```yaml
spec:
  observability:
    otlpEndpoint: "http://otel-collector:4318"
    tracesExporter: "otlp"
```

If `spec.observability` is absent on the child CR, telemetry is disabled for that component.

> [!NOTE]
> To explicitly **disable** telemetry regardless of what the operator's environment says, set `spec.observability.otlpEndpoint: "disabled"` in the `MultigresCluster` spec.

---

## GitOps Compatibility

### The Core Issue

GitOps tools (ArgoCD, Flux) compare the **desired state in Git** with the **live state in the cluster**. The webhook adds fields to the live object that aren't in the Git manifest, creating a diff.

For example, your Git manifest might look like:
```yaml
spec:
  cells:
    - name: us-east
```

But the live object (after webhook materialisation) includes images, resources, backup config, and more. This causes ArgoCD to show the resource as "OutOfSync" on every sync.

### Recommendations for GitOps Users

> [!IMPORTANT]
> **Be explicit in your manifests.** Instead of relying on webhook defaults, specify the values you care about directly in your Git manifests. This eliminates diff noise and makes your desired state self-documenting.

#### ArgoCD

Configure `ignoreDifferences` in your Application to suppress webhook-materialised fields:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
spec:
  ignoreDifferences:
    - group: multigres.com
      kind: MultigresCluster
      jsonPointers:
        - /spec/images
        - /spec/topologyPruning
        - /spec/durabilityPolicy
        - /spec/pvcDeletionPolicy
        - /spec/backup
        - /spec/databases
        - /spec/globalTopoServer
        - /spec/multiAdmin
        - /spec/multiAdminWeb
        - /spec/templateDefaults
        - /spec/externalGateway
```

Or use the broader `managedFieldsManagers` approach:
```yaml
spec:
  ignoreDifferences:
    - group: multigres.com
      kind: MultigresCluster
      managedFieldsManagers:
        - multigres-operator
```

#### Flux

Flux uses Server-Side Apply by default, which respects field ownership. Webhook-materialised fields are owned by the webhook's field manager and don't conflict with Flux's field manager. **No special configuration is needed.**

### Template Fallback Warning

> [!WARNING]
> If you deploy a `MultigresCluster` without setting `spec.templateDefaults` and a template named `"default"` (e.g., a `CoreTemplate` called `default`) exists in the same namespace, the webhook will **automatically promote** it to an explicit reference in `spec.templateDefaults.coreTemplate: "default"`.
>
> This means the live object in the cluster will have `templateDefaults` set even though your Git manifest does not. This is intentional — it makes implicit template usage visible — but it can cause unexpected diffs in GitOps workflows.

**Mitigation:** Always set `spec.templateDefaults` explicitly in your Git manifests, even if the value is the same as the namespace default:

```yaml
spec:
  templateDefaults:
    coreTemplate: "default"     # Explicit — no surprise webhook mutation
    cellTemplate: "default"
    shardTemplate: "default"
```

Or, if you don't want templates at all, don't create templates named `"default"` in the namespace.
