# PostgreSQL Configuration

The operator supports custom PostgreSQL runtime configuration via the `postgresConfigRef` field. This lets you provide a ConfigMap containing extra postgresql.conf lines that are appended to pgctld's auto-tuned defaults — PostgreSQL's last-write-wins rule then lets you override specific params without replacing the whole config.

## Configuration

Create a ConfigMap with your postgresql.conf overrides, then reference it from the shard spec:

```yaml
# User creates their own ConfigMap with postgresql.conf overrides
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-postgres-config
data:
  custom.conf: |
    shared_buffers = '8GB'
    max_connections = 200
    work_mem = '256MB'

---
# CRD references it
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
                postgresConfigRef:
                  name: my-postgres-config
                  key: custom.conf
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
  postgresConfigRef:
    name: production-pg-config
    key: postgresql.conf
  pools:
    main-rw:
      type: readWrite
      replicasPerCell: 3
      storage:
        size: "100Gi"
```

## How It Works

When `postgresConfigRef` is set:

1. The operator mounts the referenced ConfigMap into every pool pod as a read-only volume
2. The specified key is projected to the `postgresql.conf` filename inside the mount
3. pgctld reads its path from the `POSTGRES_INITDB_EXTRA_CONF` env var and appends the content to its auto-tuned `postgresql.conf`
4. PostgreSQL applies the merged config using last-write-wins, so your overrides win for any param you set

When `postgresConfigRef` is not set, pgctld uses only its auto-tuned values based on available resources. No env var is set and no extra volume is mounted.

## Override Chain

`postgresConfigRef` uses **last non-nil wins** through the shard template override chain:

1. **ShardTemplate** -- base reference
2. **ShardConfig.overrides** -- replaces the reference if set
3. **ShardConfig.spec** (inline) -- replaces the reference if set

Unlike a key-value map, the ConfigMap reference is an atomic replacement. If you need different parameters for different shards, create separate ConfigMaps.

### Example

```yaml
# ShardTemplate "production" sets baseline
spec:
  postgresConfigRef:
    name: production-pg-config
    key: postgresql.conf

---
# Shard overrides to point at a different ConfigMap
spec:
  databases:
    - name: postgres
      tablegroups:
        - name: default
          shards:
            - name: "0-inf"
              shardTemplate: production
              overrides:
                postgresConfigRef:
                  name: high-memory-pg-config
                  key: postgresql.conf
```

## Why Shard-Level?

PostgreSQL configuration is defined at the shard level because all pods in a shard replicate from the same primary. A primary and its replicas should have compatible settings -- different `shared_buffers` or `max_connections` across replicas in the same shard creates unpredictable failover behavior.

Different shards can have different configurations since they are independent PostgreSQL clusters.

## ConfigMap Contents

The ConfigMap value is plain `postgresql.conf` syntax. pgctld appends it verbatim to its auto-tuned config, so you can include just the params you want to override — there is no template processing and no need to restate auto-tuned defaults.

### Common Parameters

| Parameter | Description | Default |
|:---|:---|:---|
| `shared_buffers` | Shared memory for caching | Auto-tuned by pgctld |
| `work_mem` | Per-operation sort/hash memory | Auto-tuned |
| `max_connections` | Maximum concurrent connections | Auto-tuned |
| `effective_cache_size` | Planner's estimate of OS cache | Auto-tuned |
| `wal_buffers` | WAL write buffer size | Auto-tuned |

For a complete list of PostgreSQL parameters, see the [PostgreSQL documentation](https://www.postgresql.org/docs/current/runtime-config.html).

## Rolling Updates

Changing the referenced ConfigMap's content triggers a rolling update of all pool pods in the shard. The operator computes a SHA-256 hash of the referenced key's data during every reconciliation and stores it as a pod annotation. When the hash changes, the pod's spec-hash changes, and the operator recreates pods one at a time through the drain state machine.

## Validation

The operator does not validate the contents of the referenced ConfigMap. PostgreSQL validates the parameters itself when pgctld starts -- invalid parameters will cause the pod to fail at startup with a clear error in the pgctld logs.

To debug configuration issues:

```bash
kubectl logs <pool-pod> -c postgres | grep -i 'error\|invalid\|unrecognized'
```

## Relationship to initdbArgs

| | `postgresConfigRef` | `initdbArgs` |
|:---|:---|:---|
| **When it applies** | Every server start | First initialization only |
| **What it controls** | Runtime PostgreSQL parameters | Data directory initialization options (locale, encoding) |
| **Type** | ConfigMap reference (atomic replacement) | Single string (replacement) |
| **Use case** | Tuning performance, connections, WAL | Setting ICU locale, encoding at init time |

Both are shard-level settings with the same override chain. Use `initdbArgs` for one-time initialization options and `postgresConfigRef` for ongoing runtime tuning.

## No Defaulting

When `postgresConfigRef` is nil (the default), pgctld uses only its auto-tuned values. There is no webhook materialization -- the field stays nil unless you set it. This is intentional: the auto-tuned defaults are appropriate for most workloads.
