# PostgreSQL Initialization

> For runtime PostgreSQL tuning (`shared_buffers`, `max_connections`, `work_mem`, etc.), see [PostgreSQL Configuration](postgresql-configuration.md). This page covers one-time initialization options only.

## InitDB Arguments

The operator supports passing custom arguments to PostgreSQL's `initdb` command during data directory initialization. This is useful for configuring locale settings, encoding, and other initialization-time options.

### Configuration

Set `initdbArgs` at the shard level in your MultigresCluster spec:

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
                initdbArgs: "--locale-provider=icu --icu-locale=en_US.UTF-8"
                pools:
                  main-rw:
                    type: readWrite
                    cells: ["us-east-1"]
                    replicasPerCell: 3
```

Or in a ShardTemplate for reuse across shards:

```yaml
apiVersion: multigres.com/v1alpha1
kind: ShardTemplate
metadata:
  name: icu-locale
spec:
  initdbArgs: "--locale-provider=icu --icu-locale=en_US.UTF-8"
  pools:
    main-rw:
      type: readWrite
      replicasPerCell: 3
      storage:
        size: "50Gi"
```

### How It Works

The operator injects the value as the `POSTGRES_INITDB_ARGS` environment variable on the pgctld container. This follows the standard Docker/Supabase convention — pgctld splits the string on whitespace and appends each argument to the `initdb` command.

### Override Chain

`initdbArgs` participates in the shard template override chain:

1. **ShardTemplate** — base value
2. **ShardConfig.overrides** — overrides the template
3. **ShardConfig.spec** (inline) — overrides everything

The first non-empty value wins. If no level sets `initdbArgs`, the field is empty and no extra arguments are passed to `initdb`.

### Why Shard-Level?

`initdbArgs` is defined at the shard level (not per-pool or per-cluster) because:

- **All pods in a shard replicate from the same primary.** A primary initialized with `--locale-provider=icu` must have replicas initialized the same way, or replication breaks.
- **Each shard is an independent PostgreSQL cluster.** Different shards could theoretically use different locale settings.
- **The ShardTemplate provides cluster-wide consistency.** Set it once in a template and every shard inherits it — no need for per-cluster or per-database nesting.

### Common Use Cases

#### ICU Locale Provider

PostgreSQL 16+ supports ICU as an alternative locale provider. ICU locales are platform-independent and produce consistent collation across operating systems:

```yaml
initdbArgs: "--locale-provider=icu --icu-locale=en_US.UTF-8"
```

#### Custom Encoding

Force a specific character encoding for the template databases:

```yaml
initdbArgs: "--encoding=UTF-8"
```

#### WAL Segment Size

Customize WAL segment size for high-throughput workloads:

```yaml
initdbArgs: "--wal-segsize=64"
```

### Important Caveats

**Only affects new data directories.** `initdbArgs` only takes effect when a pod initializes a fresh data directory. Existing pods with already-initialized data directories will ignore the setting — `initdb` is skipped when `PGDATA` already exists.

This means:
- Changing `initdbArgs` triggers a rolling update (the pod spec hash changes)
- The recreated pods reuse existing PVCs with already-initialized data
- The new `initdbArgs` have **no effect** on the existing data
- Only pods with new PVCs (scale-up, PVC deletion) will use the updated args

To apply new initdb args to existing data, you must delete the PVCs and let the pods restore from pgbackrest backup.

**No defaulting.** When `initdbArgs` is empty (the default), pgctld runs `initdb` with its standard arguments (`--data-checksums --auth-local=trust --auth-host=scram-sha-256`). There is no webhook materialization — the field stays empty unless you set it.

**Validation.** The field accepts a free-form string (max 512 characters). The operator passes it through to `initdb` as-is. Invalid arguments will cause pgctld to fail at pod startup with an `initdb` error in the logs.

## Init Secrets

As the last step of `initdb`, pgctld can set role passwords and database settings from a JSON document. The operator supplies this document through an optional Secret-mounted file, keeping credentials out of arguments and environment variable values.

### Configuration

Set `postgresInitSecretsRef` at the cluster level (it is inherited by every TableGroup and Shard the cluster creates):

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: my-cluster
spec:
  postgresPasswordSecretRef:
    name: multigres-admin-password
    key: password
  postgresInitSecretsRef:
    name: multigres-init-secrets
    key: init-secrets.json  # Optional: defaults to "init-secrets.json"
  cells:
    - name: "zone-a"
      zoneId: "use1-az1"
```

The referenced Secret must exist in the same namespace as the cluster, and its data key must hold a JSON document of the form:

```json
{
  "roles": {
    "app": "some-password"
  },
  "database_settings": {
    "postgres": {
      "work_mem": "64MB"
    }
  }
}
```

See `config/samples/postgres-init-secrets.yaml` for a complete example.

Database setting values must be JSON strings. Quote numeric GUC values too, for example `"max_connections": "100"`.

### How It Works

When `postgresInitSecretsRef` is set, the operator mounts the Secret read-only into pgctld and points `POSTGRES_INIT_SECRETS_FILE` to it. pgctld reads the file after `initdb`. When the reference is unset, the operator adds no volume, mount, or environment variable.

### Important Caveats

**Only affects new data directories.** pgctld reads the file only during `initdb`. Adding the reference later recreates pool pods, but pods that reuse initialized PVCs do not apply the roles or settings. Configure it when creating the cluster. Applying it later requires fresh data directories, such as replacement PVCs.

**Restores skip it.** Pools restored from pgBackRest do not run `initdb` and retain the roles and settings from the backup.

**Secret edits do not restart pods.** The operator does not watch Secret data. Updated content affects only pools that later initialize a fresh data directory.

**Retain the Secret while referenced.** The operator does not own or delete it. A missing Secret, key, or value produces a `Warning`/`ConfigError` event and blocks shard reconciliation. Recreated pods also remain in `ContainerCreating` while the volume is missing.

**Every login role needs a password.** When an init-secrets file is configured, pgctld rejects initialization if its payload omits a login role's password.

**Mounted on every pool pod.** Any replica may initialize a fresh PVC, so every pool pod mounts the Secret into pgctld.

**The payload stays out of arguments and logs.** `POSTGRES_INIT_SECRETS_FILE` contains only the mounted file path. The payload does not appear in container arguments, pod events, or operator logs.
