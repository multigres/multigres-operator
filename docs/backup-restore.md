# Backup & Restore

The operator integrates **pgBackRest** to handle automated backups, WAL archiving, and point-in-time recovery (PITR). Backup configuration is fully declarative and propagates from the Cluster level down to individual Shards. These features are part of the operator's [Level III (Full Lifecycle)](operator-capability-levels.md#level-3-full-lifecycle) capabilities.

## Architecture

Every Shard in the cluster has its own independent backup repository.
- **Replica-Based Backups:** To avoid impacting the primary's performance, backups are always performed by a **replica**. The operator's MultiAdmin component selects a healthy replica (typically in the primary zone/cell) to execute the backup.
- **Universal Availability:** While only one replica performs the backup, **all replicas** (current and future) need access to the backup repository to:
  1.  Bootstrap new replicas (via `pgbackrest restore`).
  2.  Perform Point-in-Time Recovery (PITR).
  3.  Catch up if they fall too far behind (WAL replay).

## Supported Storage Backends

### 1. S3 (Recommended for Production)

S3 (or any S3-compatible object storage) is the **only supported method for multi-cell / multi-zone clusters**.
- **Why:** All replicas across all failure domains (zones/regions) can access the same S3 bucket.
- **Behavior:** The operator configures all pods to read/write to the specified bucket and path.

**S3 Credential Options** (mutually exclusive):

| Option | Field | Description |
|--------|-------|-------------|
| **IRSA** (recommended for EKS) | `serviceAccountName` | User creates a ServiceAccount annotated with `eks.amazonaws.com/role-arn`. The EKS pod identity webhook injects OIDC tokens automatically. |
| **Static credentials** | `credentialsSecret` (optionally `useEnvCredentials: true`) | Operator mounts the K8s Secret read-only into the pgBackRest sidecar; credentials are not injected into the other containers. |

Node/EC2 instance-profile fallback and EKS Pod Identity are not supported. See [Credentials](#pgbackrest-sidecar) below for admission and migration behavior.

```yaml
spec:
  backup:
    type: s3
    s3:
      bucket: my-database-backups
      region: us-east-1
      endpoint: https://s3.us-east-1.amazonaws.com  # Optional, for S3-compatible stores (MinIO, etc.)
      keyPrefix: prod/cluster-1

      # Option 1: IRSA (recommended for EKS)
      # User creates the SA externally with eks.amazonaws.com/role-arn annotation
      serviceAccountName: "multigres-backup"

      # Option 2: Static credentials from a Secret
      # (mutually exclusive with serviceAccountName)
      # credentialsSecret: "my-aws-secret"
      # useEnvCredentials: true

```

### 2. Filesystem

The `filesystem` backend uses one PVC per shard. Every pooler mounts that PVC, so the shard has one backup repository.

> [!WARNING]
> The PVC must support `ReadWriteMany` (RWX) when a shard has poolers in more than one cell or more than one replica. If you do not have shared RWX storage available to every cell, use S3.

RWO block storage such as EBS is suitable only when one pod mounts the repository. It cannot be shared safely across nodes.

```yaml
spec:
  backup:
    type: filesystem
    filesystem:
      path: /backups
      storage:
        size: 10Gi
        class: "nfs-client" # Must support RWX for multi-pod shards
```

## pgBackRest TLS Certificates

pgBackRest uses TLS for secure inter-node communication between replicas in a shard. The operator supports two modes for certificate provisioning:

### Auto-Generated Certificates (Default)

When no `pgbackrestTLS` configuration is specified, the operator automatically generates and rotates a CA and server certificate per Shard using the built-in `pkg/cert` module. No user action is required.

### User-Provided Certificates (cert-manager)

To use certificates from [cert-manager](https://cert-manager.io/) or any external PKI, provide the Secret name in the backup configuration:

```yaml
spec:
  backup:
    type: filesystem
    filesystem:
      path: /backups
      storage:
        size: 10Gi
    pgbackrestTLS:
      secretName: my-pgbackrest-certs  # Must contain ca.crt, tls.crt, tls.key
```

The referenced Secret must contain three keys: `ca.crt`, `tls.crt`, and `tls.key`. This is directly compatible with cert-manager's default Certificate output:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: pgbackrest-tls
spec:
  secretName: my-pgbackrest-certs
  commonName: pgbackrest
  usages: [server auth, client auth]
  issuerRef:
    name: my-issuer
    kind: Issuer
```

> [!NOTE]
> The operator internally renames `tls.crt` → `pgbackrest.crt` and `tls.key` → `pgbackrest.key` via projected volumes to match upstream pgBackRest expectations. Users do not need to perform any manual key renaming.

## pgBackRest Sidecar

pgBackRest runs in its own `pgbackrest` container in every shard pod, Lite pods included, and not in the `postgres`, `multipooler` or `minigres` containers. That container alone holds repository credentials, TLS keys and the cipher key. There is no switch for it.

**PID 1 and restart safety.** The operator starts `/multigres/bin/pgbackrestd serve` directly, without `tini` or another init wrapper. pgbackrestd itself must be PID 1 in its dedicated container PID namespace; the pod must not share process namespaces or use host PID mode. Its StateDir (`/var/lib/pgbackrestd`) is a private `emptyDir` mounted only by the sidecar. Automatic stale-worker-marker recovery depends on this contract: teardown of the previous container PID namespace must terminate its workers before a replacement sidecar reuses that state. Use a compatible pgbackrestd build that handles PID 1 signals and child reaping itself.

An init-wrapper or shared-PID deployment does not qualify for automatic stale-worker-marker recovery. Restarting only pgbackrestd is not proof that its old workers are gone; stale markers must fail closed until controlled recovery establishes worker quiescence. Do not delete StateDir, worker markers, or PGDATA to bypass that protection.

**Compatible image set.** The sidecar needs an image that ships `/multigres/bin/pgbackrestd`. The final `multipooler` and `minigres` binaries require `--pgbackrest-control-socket` and `--pgbackrest-wal-socket`; `pgctld` no longer owns the in-process pgBackRest server. The legacy `--pgbackrest-sidecar` flag is removed, but `pgbackrestd serve --sandbox=required|auto|off` remains supported. The operator's `--pgbackrest-sandbox` option (or `MULTIGRES_PGBACKREST_SANDBOX`) selects that mode, defaulting to `required`. The sidecar image defaults to the resolved Multipooler image, but that default does not verify binary compatibility. An incompatible image can fail on an unknown or missing flag.

**Coordinated manual migration (no compatibility gate).** Multigres #1614 is additive, but staging its image with the old operator requires validating mixed-version operation and pgBackRest client/server protocol compatibility across the selected image set, including replicas that remain on the previous version during the roll. It is not a universally safe preparatory rollout. #1615 removes the legacy path: the old operator cannot drive those final binaries. Staging #1614 does **not** make the final old/new transition automatic.

1. Inventory every affected cluster's effective images, including explicit pins and pending lazy image updates. Select and record a tested, compatible **full image set** for the new operator (including Postgres/pgctld, multipooler/minigres, pgbackrestd and the other Multigres components), and retain the previous operator and image set for rollback.
2. Pause reconciliation before switching incompatible operator/image versions. Stop all operator instances that reconcile the affected clusters, and prevent automation from restarting them during the switch; do not assume a per-cluster pause annotation exists. Keep reconciliation paused while installing the new operator and setting the selected images.
3. For explicitly pinned clusters, update the pins deliberately. For lazy-update clusters, select the compatible full image set and acknowledge the pending image update as required by the image update policy. Verify the effective selection and acknowledgement **before** resuming the new operator; a pending update or a default image change alone is not sufficient.
4. Resume on a canary with the paired new operator and image versions first. Verify pod startup, backup and restore/WAL operation, credential isolation, and sidecar telemetry before expanding the rollout. Never use the old operator to roll final binaries as an intermediate step.
5. If rollback is needed, pause reconciliation again and restore a tested **paired operator + full image set** before resuming. Do not roll back only the operator or only pgbackrestd. Preserve data and backup PVCs; validate data/config compatibility separately before reverting binaries.

**Credentials.** Every S3 backup config must use exactly one of these:

| Source | How |
|--------|-----|
| IRSA | `serviceAccountName` on the S3 config, or the cluster's `shardServiceAccount` carrying the role annotation |
| Static keys | `credentialsSecret` (with or without `useEnvCredentials: true`, the documented static-key form) |

Node or instance-profile credentials (an S3 config with neither of the above), `useEnvCredentials: true` without a `credentialsSecret`, and EKS Pod Identity are not supported yet. Admission rejects them, and a shard whose configuration got past admission stops reconciling with a `ConfigError` event instead of rendering a sidecar that cannot get credentials. A cluster already stored with such a configuration stays updatable and deletable: updates that leave the violation unchanged are admitted with a warning. Filesystem repositories must use the default path `/backups`, under the same rule.

**Resources.** The sidecar defaults to requests of 100m CPU and 256Mi memory and a 1Gi memory limit, with no CPU limit because pgBackRest compression bursts. Override per pool with `pgbackrestResources`:

```yaml
spec:
  databases:
    - name: postgres
      tablegroups:
        - name: default
          shards:
            - name: 0-inf
              spec:
                pools:
                  main:
                    pgbackrestResources:
                      requests: {cpu: 500m, memory: 512Mi}
                      limits: {memory: 2Gi}
```

An override replaces the defaults as a whole, so state every request and limit you want.

**Telemetry.** The sidecar uses the same runtime OTEL configuration as the other components, including operator-environment fallback and `otlpEndpoint: disabled`, with `multigres.component=pgbackrestd`. A referenced sampling ConfigMap is mounted read-only. Collect the sidecar's telemetry for the `pgbackrest.server.*` metrics that moved out of pgctld.

**Probes.** The sidecar has a startup probe and a conservative liveness probe running `pgbackrestd health`, which only asks the sidecar's status endpoint and never waits on a backup or restore, so a long operation cannot get it restarted.

**Upgrading an existing cluster.** After the coordinated migration above, every pod of a cluster created before the sidecar rolls once to the new pod template, one at a time through the normal gated roll. The data volume and the shared backup volume keep their claims, and the shard ServiceAccount named by `shardServiceAccount` is adopted if it already exists. A shard spec stored by the previous operator version carries no sidecar image, so the shard controller does not touch its pods until the cluster controller rewrites it, which happens on the first reconcile after the upgrade.

A pool that has no `fsGroup` gains one (the postgres group, with `fsGroupChangePolicy: OnRootMismatch`), because the 8432 server must read its TLS Secret. The first time kubelet sees that `fsGroup` on an already populated data volume it recursively changes the volume's group ownership before starting the pod. On a large `PGDATA` that walk delays the start of each pod once; later starts skip it. It is safe for postgres, since `pgctld` normalises `PGDATA` to mode 0700 and rewrites the password file as 0600. Set an explicit pool `fsGroup` beforehand to take the walk separately.

**Service account.** `shardServiceAccount` is optional. When unset, shard pods run as the ServiceAccount named by an S3 config's `serviceAccountName` (the IRSA account an existing cluster already uses), or as the namespace default if there is none. Either way the pod does not mount an API token.
