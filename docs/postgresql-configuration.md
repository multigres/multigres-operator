# PostgreSQL Configuration

The operator renders each shard's `postgresql.conf` from a built-in baseline plus resource-derived sizing, then layers your overrides on top. You provide overrides with the inline `spec.postgresConfig` map — a set of PostgreSQL parameter (GUC) names to values.

> **Deprecation:** the older `postgresConfigRef` field (a reference to a ConfigMap of `postgresql.conf` lines) is deprecated in favor of `spec.postgresConfig`. It is still honored for backward compatibility and will be removed in a future version. See [Legacy: postgresConfigRef](#legacy-postgresconfigref).

## Configuration

Set the parameters you want to override inline on the shard:

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: my-cluster
spec:
  databases:
    - name: "postgres"
      default: true
      tablegroups:
        - name: "default"
          default: true
          shards:
            - name: "0-inf"
              spec:
                postgresConfig:
                  max_connections: "200"
                  work_mem: "16MB"
                pools:
                  main-rw:
                    type: readWrite
                    cells: ["us-east-1"]
                    replicasPerCell: 3
```

Or in a ShardTemplate for cluster-wide consistency:

```yaml
apiVersion: multigres.com/v1alpha1
kind: ShardTemplate
metadata:
  name: production
spec:
  postgresConfig:
    max_connections: "200"
  pools:
    main-rw:
      type: readWrite
      replicasPerCell: 3
      storage:
        size: "100Gi"
```

Values are strings in PostgreSQL's own representation (the operator quotes them when rendering, so `"200"` and `"16MB"` are both fine). The field is available at the `ShardTemplate`, `overrides`, and inline `spec` levels and **merges per key** through that chain, with the inline `spec` winning.

## How It Works

For every shard, the operator renders `postgresql.conf` from these layers, each overriding the one before it (PostgreSQL applies later assignments last-write-wins):

1. the operator's built-in baseline — a complete small-instance `postgresql.conf` (SSL, logging, locales, `wal_level`, and the tunable defaults), so the operator owns the whole file, not just the knobs you override,
2. resource-derived sizing computed from the shard's CPU/memory and storage (e.g. `shared_buffers`, `effective_cache_size`, WAL sizing) — see [Resource-derived sizing](#resource-derived-sizing),
3. the deprecated `postgresConfigRef` content, if set,
4. the inline `spec.postgresConfig` map — highest precedence.

The result is written to an operator-owned ConfigMap (`<shard>-postgres-config`) mounted into every pool pod; pgctld reads it via the `POSTGRES_INITDB_EXTRA_CONF` env var. Because rendering is always on, every shard has this ConfigMap. pgctld reads the file only when PostgreSQL **starts**, so a config change takes effect by rolling the pods (see [Rolling updates](#rolling-updates)).

## Resource-derived sizing

Layer 2 above: the operator sizes the memory-, CPU-, and disk-sensitive parameters from the shard's
resources, so `shared_buffers` and friends scale with the pod instead of sitting at the small-instance
baseline. You don't set these directly — but you can override any of them with `postgresConfig`, which
is higher precedence.

**Inputs.** Sizing reads each pool's `resources` and `storage.size` and reduces them to one per-shard
basis (config is shard-level — see [Why shard-level?](#why-shard-level)):

- **memory** and **CPU** — the **maximum** across the shard's pools, taking each pool's **limit** and
  falling back to its **request** when no limit is set. Taking the max keeps the replication-sensitive
  settings valid on every pod.
- **disk** — the **minimum** `storage.size` across the pools, so WAL budgeting never overfills the
  smallest volume.

An input that is unset leaves its parameters at the baseline.

**What gets sized:**

| Parameter                                                                 | From   | Formula                                                                       |
| :------------------------------------------------------------------------ | :----- | :---------------------------------------------------------------------------- |
| `shared_buffers`                                                          | memory | `mem / 4`                                                                     |
| `effective_cache_size`                                                    | memory | `mem × 3/4`                                                                   |
| `maintenance_work_mem`                                                    | memory | `min(mem / 16, 2GB)`                                                          |
| `wal_buffers`                                                             | memory | `clamp(shared_buffers × 3%, 32kB, 16MB)`                                      |
| `work_mem`                                                                | memory | `(mem − shared_buffers) / (max_connections × 3) / parallel_workers`, min 64kB |
| `max_worker_processes`, `max_parallel_workers`                            | CPU    | `= cores`                                                                     |
| `max_parallel_workers_per_gather`                                         | CPU    | `= cores / 2`                                                                 |
| `max_parallel_maintenance_workers`                                        | CPU    | `= min(cores / 2, 4)`                                                         |
| `min_wal_size`, `max_wal_size`, `wal_keep_size`, `max_slot_wal_keep_size` | disk   | scaled down from the volume size                                              |

Notes:

- **Parallel-worker settings are tuned only at ≥ 4 CPU cores.** Below that the baseline is kept, so
  small pods aren't starved of worker slots.
- **`max_connections` is not resource-derived.** It stays at the baseline so it remains above the
  connection pooler's capacity. Raise it explicitly with `postgresConfig` if you need more (and size
  the pooler to match).
- These are pgtune-style heuristics; override any of them with `postgresConfig` when your workload
  needs something different.

## Override precedence

`postgresConfig` **merges per key** through the shard template override chain, so you can set a
baseline in a `ShardTemplate` and override individual parameters lower down:

1. **ShardTemplate** — base map
2. **ShardConfig.overrides** — merged on top, per key
3. **ShardConfig.spec** (inline) — merged on top, per key; wins on conflicts

```yaml
# ShardTemplate "production" sets a baseline
spec:
  postgresConfig:
    max_connections: "200"
    work_mem: "16MB"

---
# A shard overrides just one parameter; the rest are inherited
spec:
  databases:
    - name: postgres
      tablegroups:
        - name: default
          shards:
            - name: "0-inf"
              shardTemplate: production
              overrides:
                postgresConfig:
                  work_mem: "64MB"
```

## Why shard-level?

Configuration is shard-level because all pods in a shard replicate from the same primary. A primary
and its replicas must have compatible settings — hot standby requires several parameters (e.g.
`max_connections`, `max_worker_processes`) on a replica to be at least the primary's, so a uniform
per-shard config keeps failover predictable. Different shards are independent and can differ.

## Rolling updates

Changing the effective config triggers a rolling update of the shard's pods. The operator hashes the
rendered `postgresql.conf` each reconcile and stores it as a pod annotation; when the hash changes,
the pod's spec-hash changes and the operator recreates pods one at a time through the drain state
machine (replicas first, primary last).

## Status

Each `Shard` reports config rollout state under `status.postgresConfig`, so you can tell whether a
config change has finished rolling out without inspecting pods:

```bash
kubectl get shard <shard> -o jsonpath='{.status.postgresConfig}'
```

- `inProgress` — `true` while the desired rendered config has not yet landed on every pool pod
  (a config rollout is under way). This signal is **content-based**: the operator compares the
  hash of the config effective on the pods against the hash of the desired render, so it reflects
  any config change — a spec edit, a `postgresConfigRef` ConfigMap edit, or a new operator baseline
  on upgrade — none of which are captured by the shard generation alone.
- `lastAppliedAt` — when the rendered config last settled onto every pool pod. It is (re)stamped
  only on the transition into "settled", so it stays stable while nothing is changing and advances
  for any change.
- `error` — non-empty when the config could not be rendered or read (e.g. a missing
  `postgresConfigRef` ConfigMap).

The config is **settled** when `inProgress == false && error == ""`. For a spec change
specifically, use the shard's top-level `status.observedGeneration` as the freshness watermark to
confirm the operator has observed your edit:

```bash
kubectl get shard <shard> -o jsonpath='{.status.observedGeneration}'
```

## Validation

The validating webhook checks `postgresConfig` at admission time: each parameter name must be a
known PostgreSQL parameter (or a namespaced extension parameter such as `cron.database_name`), and
the value must roughly match the parameter's type (bool / integer / real). Unknown names and gross
type mismatches are rejected before the resource is accepted, so a typo can't reach the pods.

The check is deliberately rough — it does not validate every value (for example, specific enum values
or unit correctness). PostgreSQL performs authoritative validation when pgctld starts; an invalid
value that slips through causes the pod to fail at startup with a clear error in the pgctld logs:

```bash
kubectl logs <pool-pod> -c postgres | grep -i 'error\|invalid\|unrecognized'
```

The parameter catalog is generated from PostgreSQL 17's `guc_tables.c` and bundled with the operator.

## Relationship to initdbArgs

|                      | `postgresConfig`                | `initdbArgs`                                             |
| :------------------- | :------------------------------ | :------------------------------------------------------- |
| **When it applies**  | Every server start              | First initialization only                                |
| **What it controls** | Runtime PostgreSQL parameters   | Data directory initialization options (locale, encoding) |
| **Type**             | Key/value map (per-key merge)   | Single string (replacement)                              |
| **Use case**         | Tuning performance, connections | Setting ICU locale, encoding at init time                |

Use `initdbArgs` for one-time initialization options and `postgresConfig` for ongoing runtime tuning.

## Legacy: postgresConfigRef

> **Deprecated.** `postgresConfigRef` predates `postgresConfig`. It is still honored — its content is
> merged in just below the inline map (so `postgresConfig` wins on conflicts) — but it will be removed
> in a future version. Prefer `postgresConfig` for new configuration.

`postgresConfigRef` points at a ConfigMap holding raw `postgresql.conf` lines:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-postgres-config
data:
  custom.conf: |
    shared_buffers = '8GB'
    max_connections = 200
---
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
spec:
  databases:
    - name: "postgres"
      tablegroups:
        - name: "default"
          shards:
            - name: "0-inf"
              spec:
                postgresConfigRef:
                  name: my-postgres-config
                  key: custom.conf
```

Unlike the inline map, the reference is an atomic replacement (the whole file is one layer) and
follows **last-non-nil-wins** through the template chain rather than a per-key merge. To migrate,
move each `postgresql.conf` line into the `postgresConfig` map as a key/value pair.
