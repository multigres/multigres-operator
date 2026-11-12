# Backup & Restore

The operator integrates **pgBackRest** to handle automated backups, WAL archiving, and point-in-time recovery (PITR). Backup configuration is fully declarative and propagates from the Cluster level down to individual Shards. These features are part of the operator's [Level III (Full Lifecycle)](operator-capability-levels.md#level-3-full-lifecycle) capabilities.

## Architecture

Every Shard in the cluster has its own independent backup repository.
- **Replica-Based Backups:** To avoid impacting the primary's performance, backups in a distributed cluster are performed by a **replica**. The operator's MultiAdmin component selects a healthy replica (typically in the primary zone/cell) to execute the backup.
- **Lite clusters back up on the static leader.** A Lite shard has one pooler and no replicas, so that pooler takes its own backups and expires old ones, without the etcd backup lease a distributed cluster uses. This relies on each Lite cluster having a backup repository no other cluster writes to; see [Repository isolation](#repository-isolation).
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

#### Repository isolation

Without a `keyPrefix`, the repository path in the bucket depends only on the database layout: `/multigres` for the default table group and shard, and a path under `/tablegroups` built from the table group, its catalog ID when known, and the shard for any other shard. Two clusters with the same layout on the same bucket and endpoint and no `keyPrefix` therefore resolve to one repository. A new cluster's pooler restores from whatever complete backup it finds there, so the second cluster can start with the first cluster's data and then archive WAL into the same stanza. If the repository holds no complete backup yet, one of the two clusters fails to create its stanza instead and cannot back up or archive WAL.

- **Lite clusters must set `keyPrefix`.** Admission rejects a new Lite cluster with S3 backups and no `keyPrefix`, and an update that would put a cluster in a default repository it did not already use, including a change of bucket or endpoint. When the webhook is disabled, the shard controller refuses to provision such a Lite shard, records a `ConfigError` event, and sets the Shard's `BackupRepositoryValid` condition to `False` with the reason. The condition turns `True` once a `keyPrefix` is set.
- **Existing Lite clusters keep their repository.** A Lite cluster that never set `keyPrefix` keeps backing up to its default repository, as long as its database record in topology names the same bucket and endpoint, or its data PVC still exists. Its Shard's `BackupRepositoryValid` condition is `True` with reason `ExistingDefaultRepository`, so these clusters can be found.
- **Do not change `keyPrefix` on a cluster that has already run.** The pooler creates its stanza only when the cluster first bootstraps. After a restart with a different prefix, Postgres starts from its existing data, finds no stanza in the new location, and backups and WAL archiving fail. Admission does not block this change.
- **The repository is matched by bucket and endpoint as written.** Changing `endpoint` from empty to the explicit AWS endpoint for the same bucket counts as a move and is refused for a cluster without `keyPrefix`.
- **A refused shard is not reconciled further.** While the shard controller refuses a Lite shard, it does not create or update its pod; the `BackupRepositoryValid` condition says why. The one exception is a topology outage: outage recovery runs before the check and can still bring back a pod from an existing data PVC. For an existing cluster, restore its previous backup settings. A new cluster that was refused can set a `keyPrefix` instead.
- **A recreated cluster is a new cluster.** Admission rejects creating a Lite cluster without `keyPrefix`, even when a data PVC from an earlier cluster of the same name was retained. Backups left in the default repository are not reachable through the operator. With a `keyPrefix` and a retained data PVC, the cluster starts from the existing data with no stanza in the new location, as above, so remove the retained PVC to start fresh.
- **Distributed clusters** are not checked. Give each one its own `keyPrefix` when several share a bucket.

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
