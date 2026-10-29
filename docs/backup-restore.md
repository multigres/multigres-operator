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
| **Static credentials** | `credentialsSecret` + `useEnvCredentials: true` | Operator injects `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from a K8s Secret. |
| **EC2 instance metadata** | *(none)* | Default fallback — uses the node's IAM instance profile. |

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

      # Option 3: EC2 instance metadata (default, no fields needed)
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

**Minimum multigres image.** The sidecar needs a multigres image that ships `/multigres/bin/pgbackrestd`, whose `multipooler` and `minigres` require `--pgbackrest-control-socket` and `--pgbackrest-wal-socket`, and whose `pgctld` and `pgbackrestd` have dropped the in-process pgBackRest flags (`--pgbackrest-sidecar`, `--sandbox`). An older image fails to start the pod on an unknown or missing flag. The sidecar image defaults to the resolved Multipooler image, so one multigres build serves both. Release the operator only after the cluster's images are at or past that build.

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

**Probes.** The sidecar has a startup probe and a conservative liveness probe running `pgbackrestd health`, which only asks the sidecar's status endpoint and never waits on a backup or restore, so a long operation cannot get it restarted.

**Upgrading an existing cluster.** Every pod of a cluster created before the sidecar rolls once to the new pod template, one at a time through the normal gated roll. The data volume and the shared backup volume keep their claims, and the shard ServiceAccount named by `shardServiceAccount` is adopted if it already exists. A shard spec stored by the previous operator version carries no sidecar image, so the shard controller does not touch its pods until the cluster controller rewrites it, which happens on the first reconcile after the upgrade.

A pool that has no `fsGroup` gains one (the postgres group, with `fsGroupChangePolicy: OnRootMismatch`), because the 8432 server must read its TLS Secret. The first time kubelet sees that `fsGroup` on an already populated data volume it recursively changes the volume's group ownership before starting the pod. On a large `PGDATA` that walk delays the start of each pod once; later starts skip it. It is safe for postgres, since `pgctld` normalises `PGDATA` to mode 0700 and rewrites the password file as 0600. Set an explicit pool `fsGroup` beforehand to take the walk separately.

**Service account.** `shardServiceAccount` is optional. When unset, shard pods run as the ServiceAccount named by an S3 config's `serviceAccountName` (the IRSA account an existing cluster already uses), or as the namespace default if there is none. Either way the pod does not mount an API token.
