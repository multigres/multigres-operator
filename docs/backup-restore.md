# Backup & Restore

The operator integrates **pgBackRest** to handle automated backups, WAL archiving, and point-in-time recovery (PITR). Backup configuration is fully declarative and propagates from the Cluster level down to individual Shards. These features are part of the operator's [Level III (Full Lifecycle)](operator-capability-levels.md#level-3-full-lifecycle) capabilities.

## Architecture

Every Shard in the cluster has its own independent backup repository.
- **Replica-Based Backups:** To avoid impacting the primary's performance, backups in distributed clusters are performed by a **replica**. The operator's MultiAdmin component selects a healthy replica (typically in the primary zone/cell) to execute the backup.
- **Lite clusters:** The static leader takes its own backups. Both modes need independent repositories for independent clusters; distributed backup leases only coordinate poolers within their own topology.
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
| **Static credentials** | `credentialsSecret` (optionally `useEnvCredentials: true`) | Operator mounts the keys `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and, when present, `AWS_SESSION_TOKEN` of the K8s Secret read-only into the pgBackRest sidecar, and no other key of it; credentials are not injected into the other containers. |

Node/EC2 instance-profile fallback and EKS Pod Identity are not supported. See [Credentials](#pgbackrest-sidecar) below for admission and migration behavior.

```yaml
spec:
  backup:
    type: s3
    s3:
      bucket: my-database-backups
      region: us-east-1
      endpoint: https://s3.us-east-1.amazonaws.com  # Optional, for S3-compatible stores
      keyPrefix: prod/cluster-1

      # Option 1: IRSA (recommended for EKS)
      # User creates the SA externally with eks.amazonaws.com/role-arn annotation
      serviceAccountName: "multigres-backup"

      # Option 2: Static credentials from a Secret
      # (mutually exclusive with serviceAccountName)
      # credentialsSecret: "my-aws-secret"
      # useEnvCredentials: true

      # Optional: the CA bundle that verifies the endpoint's TLS certificate,
      # for an endpoint behind a private CA. A key of a Secret or a ConfigMap.
      # ca:
      #   configMap:
      #     name: minio-ca
      #     key: ca.crt        # the default
      # Optional, default true. Mutually exclusive with ca.
      # verifyTLS: false

```

**The S3 endpoint is a host or an https URL.** `s3.endpoint` is optional; without it the sidecar uses AWS S3. It is handed to pgBackRest as `repo-s3-endpoint`, which connects with TLS to a bare `host[:port]` (pgBackRest's own form, for example `minio.storage.svc:9000`) and to `https://host[:port]`, and without TLS to an `http://` URL. Requests are signed and can carry a session token, so the operator refuses an endpoint with any scheme other than `https`, and one that is neither form (a path, a query, user information). Admission rejects such an endpoint. A stored cluster that has one is refused with `BackupRepositoryValid=False`, reason `UnsupportedEndpoint`: its shard pods keep running as they are, but a pod that is lost is not recreated until the endpoint is changed. The refusal does not repeat the endpoint, and wherever an endpoint is shown (conditions, events, admission messages) its user information, query and fragment are replaced with `<redacted>`; do not put credentials in the endpoint. An S3 service with a private or self-signed certificate is used with `ca` or `verifyTLS: false`, below, not with `http`.

**TLS verification of the S3 endpoint.** The sidecar verifies the endpoint's certificate against the trust store of its image unless the S3 block says otherwise:

- `ca` names one key of a Secret or of a ConfigMap in the cluster's namespace that holds a PEM bundle. It replaces the system trust store for the endpoint. The bundle may hold several certificates, and a self-signed server certificate works as its own CA (reference the certificate itself). The operator mounts that one key read-only into the `pgbackrest` container and into no other. For a pool it does not read or validate the bundle: a missing object or key keeps the replacement pod from starting, with the kubelet's event naming it, and a file without a PEM certificate makes the sidecar refuse to start, so the rollout stops one pod short. A Lite shard has one pod, so before that pod is replaced the operator checks that the object exists, has the key, and that the key holds at least one PEM certificate. If not, the pod is kept on its current spec and the Shard reports `PodTemplateAccepted=False` with reason `CABundleUnusable`, rechecked every 5 to 6 minutes and when `multigres.com/retry-creates` is set on the pod. The name must be a valid object name and the key a valid Secret or ConfigMap key; the API server refuses others.
- `verifyTLS: false` turns verification off. Anyone who can intercept the connection can then read and replace backups and WAL, so use `ca` for a private CA instead. It is admitted with a warning on every write to the cluster, and the cluster reports `S3TLSVerified=False` (reason `VerificationDisabled`) for as long as a location is configured that way. The condition follows the spec: a pod still running on an earlier spec, for example a Lite pod kept because its replacement was refused, verifies or skips as it was started until it is replaced. The sidecar reports the same in its own status and as the `pgbackrest.s3.tls_verify_disabled` metric.
- The two exclude each other, and the API server refuses a block with both. Across levels (cluster, database, tablegroup, shard) a block that sets neither inherits both. A `ca` set below replaces a `ca` or a `verifyTLS: false` set above, and `verifyTLS: false` below replaces a `ca` above. `verifyTLS: true` below turns an inherited `false` back on and keeps an inherited `ca`: it does not go back to the system trust store. In a distributed cluster a tablegroup or shard block that differs from the database-level location in either is refused with `OverrideNotApplied`, like the other settings of the location.
- *Rotating the bundle.* Changing the content of the referenced key needs no rollout: the kubelet replaces the mounted file, usually within a minute or two, and pgBackRest reads it when each command starts. Naming another object or key changes the pod template, so the pods are replaced one at a time as for any spec change.
- The setting is part of the location the database's topology record carries. Multipooler compares the record with what its sidecar was started with, and refuses a sidecar that differs, so the operator writes the record and the sidecar's arguments from the same resolved block. A Lite shard's record keeps the location its pod was started with for as long as that pod exists on an earlier spec, a pod kept because its replacement was refused included, and moves to the new location when the successor is created: the pod's containers can restart at any time and would not start against a record that had moved ahead of them.

#### Repository isolation

New clusters in both Lite and distributed mode receive an S3 `keyPrefix` of `clusters/<cluster UID>` when a prefix is omitted, empty, or contains multiple slashes and nothing else. Set `keyPrefix: "/"` explicitly to restore a legacy backup from the bucket root, including when the original cluster and its PVCs are gone. With the generated prefix, the default table group and shard use `/clusters/<cluster UID>/multigres` within the bucket. Kubernetes assigns a different UID to each cluster object, including a cluster recreated with the same name, so independently created clusters do not share the default repository.

The cluster controller records the allocation in the `multigres.com/default-backup-key-prefix` annotation and writes missing prefixes into the configured backup blocks in the same API update that first adds its cleanup finalizer. This happens before creating child resources or writing topology and also works with the webhook disabled. The annotation is operator-managed; omit it when creating a cluster. Admission prevents changing or removing it after initialization. It keeps the same default available on restarts and when S3 settings or shards are added later. With admission enabled, defaulted fields are also materialized on subsequent updates; without admission, the controller applies that saved default when resolving the configuration.

Backup settings inherit from cluster to database, table group, and shard. Explicit prefixes, including inherited ones, are preserved exactly. If you supply prefixes yourself, use distinct values for independent clusters sharing a bucket; the operator does not check S3 for uniqueness. Filesystem backups are unchanged.

The `pgbackrest` sidecar in each shard pod is started with the location topology records for the shard's database, with the same prefix. In a distributed cluster that is the database-level location (`spec.backup` merged with the database's `backup`); tablegroup and shard backup blocks do not select the repository. In a Lite cluster it is the shard's resolved backup, so a block at any level applies. Multipooler reads the location from topology, compares it with the one its sidecar reports at startup, and refuses a sidecar that serves a different location. `keyPrefix: "/"` reaches both as an empty prefix, because pgBackRest adds the leading slash to the repository path itself.

**S3 never falls back to the filesystem.** If any backup block of the cluster configures S3 and a shard's location still resolves to the local filesystem, the operator refuses instead of proceeding: backups on the cluster's own volume would report success and be lost with the cluster. This happens when a distributed cluster configures S3 only in a tablegroup or shard block, when a block carries `s3` settings under a filesystem `type`, and when a block below an S3 one selects `filesystem`. Admission rejects such a cluster. With the webhook disabled, the MultigresCluster reports `BackupRepositoryValid=False` (reason `FilesystemFallback`) with a `BackupRepositoryInvalid` event, reports the `Degraded` phase, and reconciles no topology records or TableGroups until the configuration is fixed. A Shard reports the same condition and creates no pod when its own spec has an S3 backup but a filesystem sidecar location, or an S3 location without a key prefix although its cluster was allocated one (reason `MissingKeyPrefix`).

**A distributed cluster's tablegroup and shard blocks cannot change the repository's encryption, certificate, bucket or key prefix.** Every sidecar of a database is configured from the database-level location, so a tablegroup or shard backup block is refused, the same way and with reason `OverrideNotApplied`, when it sets any of these to a value the database-level location does not have: `type`, `encryption`, `pgbackrestTLS.secretName` (a `pgbackrestTLS` block without a secret name asks for the generated certificate), or the S3 `bucket`, `endpoint`, `region` or `keyPrefix`. Applying the block's value is not possible, and ignoring it would leave a repository that is unencrypted, or in another bucket or under another prefix, than the block says, and a restore that followed the block would look where no backup is. A block of another `type` is refused because it replaces everything it would inherit, encryption included, while the sidecar keeps the database-level location. A block that repeats the database-level values is accepted; two spellings of one endpoint count as the same value only where they cannot differ: the letter case of the scheme and of the host name, and the single slash after a host with nothing behind it. A path, a query, a port or credentials in the endpoint are compared as written. A key prefix is compared as written too, except that `/` and no prefix both mean the bucket root; a block without a `keyPrefix` inherits the database's and is accepted. The rule does not apply to a Lite cluster, where the shard's resolved backup is the location.

A stored cluster can have shard backup blocks that only repeat what the shard inherits: the defaulting webhook of earlier operator versions copied each shard's merged backup configuration into the shard's block when the cluster used no ShardTemplate. The webhook does not write that copy. A copy already stored keeps the values of the day it was made, so it contradicts a later edit of the location above it. In a distributed cluster that makes the edit an `OverrideNotApplied` refusal. In a Lite cluster, where the shard's block is the location, backups would keep going to the old place although the cluster level says otherwise. So on an update the defaulting webhook removes a shard block when all of this holds: the update does not change the block; merged over what the shard inherited before the update, the block added nothing; and against the edited levels above it, the block now asks for something else (in a distributed cluster, something the database-level location would not apply). The shard then inherits the edited values. A block you add or edit in the same update is never removed, and neither is one that set anything of its own, with the one exception described next: those are judged by validation, and in a Lite cluster a shard block of its own remains the shard's location. The webhook cannot tell a copy it wrote from a block a person wrote to say the same thing, and removes both.

**Taking S3 away above an S3 shard block.** An S3 shard block that the levels above it no longer resolve to is handled by a rule of its own, because removing it moves the shard's backups from S3 to the cluster's own volume.

- *The S3 block above is deleted.* That states no location, so the shard block stays, also when a filesystem block further up shows through, such as the default that defaulting stores in `spec.backup`. A Lite shard keeps its block and stays on S3. A distributed cluster, whose shard block selects nothing, is refused with `FilesystemFallback` until you delete the shard block or state the filesystem as described below.
- *A level above starts to say `type: filesystem`.* When `spec.backup`, the database's `backup` or the tablegroup's `backup` says `type: filesystem` in the update and did not before, that states a location: an S3 shard block the same update leaves unchanged is removed, and the shard backs up to the filesystem. This is the exception to the rule above: it also removes a block that set a bucket of its own.
- *`spec.backup` over the stored default.* A stored `spec.backup` always has a type, because defaulting fills an absent one with the filesystem default. A filesystem block sent for `spec.backup` over that default is what any client sends that reads the object and writes it back, so it is not seen as new and removes nothing, also in the update that deletes a database's S3 block. Write `type: filesystem` in the database's or tablegroup's `backup` instead, or delete the shard block. Writing it in `spec.backup` works when `spec.backup` was the S3 block.
- *A kept block is the shard's own from then on.* In a Lite cluster, a copy that stayed because the S3 block above it was deleted is the only statement of the shard's location, and nothing tells it from a block someone wrote. Putting S3 back above it later, or editing the bucket there, does not reach the shard: it keeps the bucket in its own block until you delete that block. Each such update returns an admission warning that names the shard, the bucket and prefix it still backs up to, and where the levels above it now resolve to; `kubectl` prints it. The warning comes with the update that changes the location above the shard, not with later writes, so a shard that is meant to keep its own location is not reported every time the cluster is edited.

With the webhook disabled nothing is removed: a distributed cluster is refused with `OverrideNotApplied` until the shard block is deleted, and a Lite cluster keeps the shard block's location until you edit or delete the block.

The cluster controller applies every rule admission applies, the credential and filesystem-path rules below included, and reports the first violation in `BackupRepositoryValid`. While a MultigresCluster is refused, its cells and global components still reconcile, but its TableGroups are not rewritten, so its Shards keep the spec they had: running pods are left alone, and spec changes below the cluster do not reach them. Two things are kept as they were or still carried down. An existing Cell keeps the global topology server it already uses, because a change to it is registered only once the cluster is accepted. A `pvcDeletionPolicy` of `Retain` set on the refused cluster still reaches its TableGroups and Shards, and a refused Shard still releases its claims for it, so you can protect the volumes of a cluster you cannot repair before deleting it; a change to `Delete` waits until the cluster is accepted. The pods of a refused Shard keep their pgBackRest certificate rotated and their readiness gate current. Its `BackupHealthy` condition follows the backups its pods report; where they cannot be asked (a Lite shard, no reachable topology, pooler or primary) a `True` from before the refusal becomes `Unknown`, and the last backup's time and type are kept. A `Retain` on the cluster reaches every TableGroup's own level even when a shard of the spec cannot be resolved, for example because its ShardTemplate is gone; a stored shard or pool that says `Delete` itself is then not changed, and the reconcile error names those shards, so check it before deleting a refused cluster. Only an edit to the cluster's backup configuration clears the refusal; the operator does not retry in the meantime. The `Degraded` phase is what the `MultigresClusterDegraded` alert watches, so a refused cluster raises it.

**Existing clusters keep their repository paths.** A cluster that already has the operator's cleanup finalizer is not assigned a default, even if its prefix is empty. Before adding the finalizer, the controller checks for prior cluster status, TableGroups and reusable storage. It considers the names and replica ordinals of the PostgreSQL and managed etcd claims in the resolved configuration. A terminating claim, a claim owned by another MultigresCluster UID, or an unrelated claim with the same cluster label does not select a repository.

**Recreating a cluster with retained storage.** The operator records the generated cluster default on PostgreSQL and managed etcd PVCs. A replacement recovers that default from the claims it will mount. Reapply the original backup settings, including any explicit prefixes, bucket and endpoint. Conflicting recorded allocations stop initialization. The cluster’s `BackupPrefixInitializationFailed` event identifies the affected claims. If reusable claims predate the allocation record and the S3 configuration omits its prefix, supply the original `keyPrefix`. Use `/` only when the original repository was at the bucket root. Existing unprefixed clusters sharing a bucket remain exposed to collisions and require a planned repository migration.

Without a prefix, the repository path depends only on the database layout: `/multigres` for the default table group and shard, or a path under `/tablegroups` for another shard. Two independent clusters using the same bucket, endpoint, and layout can therefore share a repository. A fresh pooler may restore the other cluster's backup and then archive WAL into the same stanza; if no complete backup exists, stanza creation can fail instead.

**Do not change a running cluster's repository path without a migration.** The pooler creates the stanza during bootstrap. Changing `keyPrefix` while retaining PostgreSQL data can leave the new location without a stanza, breaking backups and WAL archiving. Explicit backup-location edits remain possible; automatic defaulting does not migrate existing backups.

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

**Compatible image set.** The sidecar needs an image that ships `/multigres/bin/pgbackrestd`. The `multipooler` and `minigres` binaries must accept `--pgbackrest-control-socket` and `--pgbackrest-wal-socket`, and `pgctld` must be a build that archives through the sidecar's WAL socket and runs no pgBackRest server of its own. The operator passes no `--pgbackrest-sidecar` flag to any of them. The sidecar's sandbox mode is `pgbackrestd serve --sandbox=required|auto|off`, which the operator's `--pgbackrest-sandbox` option (or `MULTIGRES_PGBACKREST_SANDBOX`) selects, defaulting to `required`. The sidecar image defaults to the resolved Multipooler image, but that default does not verify binary compatibility. An incompatible image can fail on an unknown or missing flag.

**Coordinated manual migration (no compatibility gate).** Multigres #1614 is additive, but staging its image with the old operator requires validating mixed-version operation and pgBackRest client/server protocol compatibility across the selected image set, including replicas that remain on the previous version during the roll. It is not a universally safe preparatory rollout. #1615 removes the legacy path: the old operator cannot drive those final binaries. Staging #1614 does **not** make the final old/new transition automatic.

1. Inventory every affected cluster's effective images, including explicit pins and pending lazy image updates. Select and record a tested, compatible **full image set** for the new operator (including Postgres/pgctld, multipooler/minigres, pgbackrestd and the other Multigres components), and retain the previous operator and image set for rollback.
2. Pause reconciliation with the reconcile gate, not by stopping the operator. The MultigresCluster webhooks fail closed (`failurePolicy: Fail`) and are served by the operator, so with no operator running the API server rejects every write to a MultigresCluster, the image edits of step 4 included. Label each affected namespace before anything else changes:

   ```bash
   kubectl label namespace <namespace> multigres.com/reconcile=disabled
   kubectl get namespaces -l multigres.com/reconcile=disabled
   ```

   An operator with the gate (see [reconcile gating](development/reconcile-gating.md)) reconciles nothing in a labelled namespace and keeps serving admission for it. The label is not a webhook-covered object, so it can be set whatever operator is running. An operator release from before the gate ignores the label and keeps reconciling with the images it already rendered, which changes nothing until step 4; do not edit images while such an operator is still running.
3. Replace the old operator with the new one, CRDs included, and prevent automation from putting the old one back. The new operator starts with every affected namespace already gated, so it serves admission and renders nothing. Confirm that before going on: the operator's pods are Ready, and the pods, TableGroups and Shards of the gated namespaces are unchanged (same pod UIDs, same `resourceVersion` on each TableGroup).
4. With the new operator serving admission and the namespaces still gated, write the image selection, and in the same edit make the changes the [check before upgrading](#pgbackrest-sidecar) table asks for. In particular, for a cluster with a custom `s3.endpoint`: the previous release did not verify the certificate of a custom endpoint at all, and the sidecar does, so a cluster whose S3 service has a private or self-signed certificate must set `s3.ca` (or `verifyTLS: false`) now. Without it the roll succeeds, the pods are Ready, and every backup and WAL push then fails on certificate verification while WAL accumulates on the data volume. For explicitly pinned clusters, update the pins deliberately. For lazy-update clusters, select the compatible full image set and acknowledge the pending image update as required by the image update policy. Verify the stored selection and acknowledgement on every cluster, and that nothing in the gated namespaces has changed, **before** lifting any gate; a pending update or a default image change alone is not sufficient. A wrong edit can be taken back at this point without any pod having been built from it.
5. Lift the gate on a canary namespace first: `kubectl label namespace <namespace> multigres.com/reconcile-`. The new operator then rolls that namespace's clusters to the selected images. Verify pod startup, backup and restore/WAL operation, credential isolation, and sidecar telemetry before lifting the gate on the next namespaces. Never use the old operator to roll final binaries as an intermediate step.
6. Taking the change back is only covered here for a namespace whose gate has not been lifted since step 2. Its pods, TableGroups and Shards are still the old operator's, so restore the previous image selection on its clusters while the new operator serves admission, verify it, replace the new operator with the previous one, and lift the gate. Restore a tested **paired operator + full image set**; do not roll back only the operator or only pgbackrestd.

   **Rolling back a namespace that has already rolled is an operator downgrade, not a spec revert.** Install the previous operator release, with the image set that release pins, in place of the new one. Reverting the image selection of a cluster while the new operator keeps running is not a rollback of a rolled namespace:
   - Its TableGroups and Shards carry the new image set. The gate pauses every controller of a namespace together and releases them together, so it cannot make the cluster controller rewrite the TableGroups and Shards before a shard controller builds pods from the ones that are there.
   - A cluster on the lazy image update policy records the image set it adopted. Restoring the earlier spec does not restore that record: the cluster stays on the recorded set until the earlier set's revision is acknowledged.
   - The previous operator cannot drive the final binaries (see above), so the downgrade has to bring the pods back to the previous release's images as well.

   The downgrade is not exercised by this repository's tests. Qualify it on a cluster, with the releases and the image policy you run, before relying on it, and until then choose the canary as if lifting its gate could not be taken back. Preserve data and backup PVCs in any case, and validate data/config compatibility separately before reverting binaries.

**Credentials.** Every S3 backup config must use exactly one of these:

| Source | How |
|--------|-----|
| IRSA | `serviceAccountName` on the S3 config, or the cluster's `shardServiceAccount` carrying the role annotation |
| Static keys | `credentialsSecret` (with or without `useEnvCredentials: true`, the documented static-key form) |

Node or instance-profile credentials (an S3 config with neither of the above), `useEnvCredentials: true` without a `credentialsSecret`, and EKS Pod Identity are not supported yet. Admission rejects them. With the webhook disabled the MultigresCluster reports `BackupRepositoryValid=False` (reason `UnsupportedCredentials`) and `Degraded` and builds no Shard from the configuration, and a Shard that carries it anyway reports the same condition, reports `Degraded` and creates or replaces no pod, instead of rendering a sidecar that cannot get credentials. A cluster already stored with such a configuration stays updatable and deletable: updates that leave the violation unchanged are admitted with a warning. Filesystem repositories must use the default path `/backups`, under the same rule (reason `UnsupportedPath`).

A Secret named by a backup block (`s3.credentialsSecret`, `encryption.secretName`, `pgbackrestTLS.secretName`) must not also be the cluster's `postgresPasswordSecretRef`, `postgresInitSecretsRef` or `postgresMountsSecretRef`: the first three are mounted only into the sidecar and the last three only into the postgres containers, and one Secret in both roles would give each side the other's material. Admission rejects it, and admits an unchanged stored cluster with a warning.

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

The block follows the rule of the pool's `postgres.resources` and `multipooler.resources`:

- **Absent, or a bare `pgbackrestResources: {}`:** the pool inherits the block from the ShardTemplate or the level above it, and gets the defaults when nothing sets one. The defaulting webhook writes the result into a template-free cluster's spec, as it does for the other containers.
- **Any member set:** the block replaces what it would inherit as a whole, so state every request and limit you want. A block with only `limits` has no requests of its own (Kubernetes then uses each limit as the request).
- **Members set to empty,** such as `pgbackrestResources: {limits: {}}`, which is what deleting the last entry with `kubectl edit` leaves: the sidecar runs with no requests and no limits. It is not defaulted.

Changing the block, emptying it included, replaces the pool's pods one at a time like any other pod template change. Admission, in the cluster and in a ShardTemplate, rejects a block a pod could not have: a limit below the request of the same resource, a negative quantity, a name that is not `cpu`, `memory`, `ephemeral-storage`, `hugepages-<size>` or a domain-qualified resource, and any resource claim.

**A pod is not replaced by one the API server rejects.** Admission cannot know everything the API server checks, and a block can reach a Shard without it (webhook disabled, or a ShardTemplate changed under a cluster). So before the shard controller deletes a Lite shard's pod, or drains or deletes a pool pod in order to replace it (a spec change, a filesystem resize, a pod that cannot become Ready, a failed pod, a quarantined pod whose data is wiped for a restore from backup), it sends the replacement to the API server as a dry-run create under the pod's own name. A pool pod is asked about when its drain starts and again when the drain is complete, before the pod is deleted, since the spec or an admission policy can change in between. The answer is read as one of four things.

- *Accepted*: no error, or only the name is taken by the pod being replaced. The replacement goes ahead.
- *Rejected*: any other client error, whatever sends it: a validation error, a `LimitRange`, Pod Security, an admission policy, a webhook that denies the pod (with `409` too). The existing pod, and for a quarantined pod its data, is left as it is. The Shard reports `PodTemplateAccepted=False` with the API server's message and a `PodTemplateRejected` event, and the Shard, its TableGroup and the cluster report `Degraded`.
- *Not answered*: a server error, a timeout, throttling, a transport error. Nothing is known about the pod yet, and the create itself would fail the same way. A Lite shard's pod is its only one, so it is kept and the question is asked again, less and less often: after an unanswered dry run the shard waits as long as the question has been unanswered so far, at least 1 second and at most 1000 seconds (about 17 minutes), before it sends the next one, so its asks fall at 1, 2, 4, 8... seconds and then every 1000. That holds whatever reconciles the shard, for as long as the question is the same: when the pod is replaced by another, turns terminal, or the spec asks for a different replacement, the new question is asked at once. A shard whose pod is not primary yet is reconciled every 10 seconds, and those passes do not each send a dry run. A settled shard is reconciled again when its next ask is due and not before. The schedule is per shard: nothing limits how many shards ask at once except the controller's 20 workers and the operator's API client rate (50 requests a second, burst 100), which all of the operator's controllers share. An API server fault that leaves every Lite shard waiting therefore starts with a wave of dry runs per doubling and thins out to one per shard every 1000 seconds; it does not turn into a steady load on the server that is failing. The schedule is kept in the operator's memory, so a restarted operator asks once and starts over. A quarantined pod, whose data cannot be brought back, is kept as well and asked about again whenever the shard is reconciled, which is at least once a minute while a pod is quarantined. Those dry runs are not spaced out the way a Lite shard's are: one is sent per reconcile for as long as the API server gives no answer, which for a shard that is otherwise idle is one a minute. A pool pod is replaced as before, unless a replacement is recorded as rejected for the shard: one refusal is recorded at a time, so the recorded one may stand for another pod built from the same template, and an unanswered question does not undo it. Each unanswered question is reported with a `PodReplacementUnanswered` warning event on the Shard. No condition is set for it and the phase does not change, because the cause is outside the shard and usually short; `UpToDate=False` on a Lite shard, and the event, say that a pod is waiting.
- *Cannot be asked*: an admission webhook that matches pods declares side effects it cannot suppress, so the API server refuses every dry run of a pod (`admission webhook "..." does not support dry run`) while it lets the create through. Waiting cannot change that, so the replacement goes ahead as it would without the question, for Lite, pool and quarantined pods alike. The one exception is a pod whose replacement is recorded as rejected, which stays until the API server can be asked again or the spec is edited. `admissionregistration.k8s.io/v1` does not accept such a webhook, so only configurations stored through `v1beta1` and never rewritten have one.

**A pod over quota is not a rejected pod, except a Lite shard's only pod.** A `ResourceQuota` turns down a pod or claim the namespace has no room for with `exceeded quota: <name>, requested: ..., used: ..., limited: ...`. Quota runs last in admission, so the object has passed every other check. For a pool pod the replacement goes ahead, no refusal is recorded, and the create is sent again until there is room. A Lite shard has one pod, and the dry run is counted next to it, so the answer does not say whether the replacement fits once the pod is gone: the pod and its data are kept, and the answer is recorded like a rejection (`PodTemplateAccepted=False` with the quota's message). A Lite shard therefore needs room for one more pod than it runs in order to replace its pod: in a namespace whose quota fits the shard exactly, raise the quota by one pod's worth for the rollout. While the pod is kept this way the Shard, its TableGroup and the cluster report `Degraded`, like any other refused pod template, so an alert on `Degraded` fires for a Lite cluster whose rollout is waiting for quota headroom although the database is serving. A Lite pod that has failed or completed is replaced whatever the quota says, since it serves nothing. `insufficient quota to consume: ...`, the answer when the cluster requires a quota for a resource and the namespace has none that covers it, is a rejection for pool pods too: removing the old pod cannot supply a quota, so the pod is kept until a covering quota exists or the spec changes. A create that gets this answer (a pod that is missing, not one being replaced) is sent again like one that has no room. A quota that refuses the pod for what it is, such as `must specify limits.memory`, is an ordinary rejection, and `status unknown for quota: ...` counts as not answered. The operator knows a quota only from these answers: it does not read or watch `ResourceQuota` objects and needs no permission on them.

- *Where to look.* The Shard's `Available` condition names the wait after its pod count, for example `2/3 pods ready; pod <name> waiting on ResourceQuota <quota> (pods) since <time>`: one pod and one claim at most, the quota, the resources it has no room for, and since when, all taken from the API server's answer to the create. The count is of the pods the spec asks for; a ready pod beyond a replica count does not stand in for a missing one. Each failed create also ends the reconcile with a `FailedApply` event, but events about one object are rate limited, so read the condition. A Lite pod kept for a full quota shows in `PodTemplateAccepted` instead, since no create was turned down. The text is kept in the operator's memory: after an operator restart it reappears, with a new time, once a create is turned down again.
- *What to do.* Raise the quota, free capacity in the namespace, or lower the requests in the spec. This is the namespace administrator's step: the operator waits safely and does not work around a full quota. It deletes no pod, claim or data because a create was refused, and it does not rearrange a shard to make room.
- *A full quota can stop a rollout or a change of backup location.* A rollout replaces a pool pod by removing it and creating its successor, so in a namespace with no room for the successor it stops one pod short; a Lite shard's rollout does not start. A change of the backup location to `filesystem` needs a new shared backup claim, and every pod mounts that claim, so in a namespace at its claim limit the reconcile ends at the claim: no pod is created, replaced or removed until the claim exists. A maintenance surge that is no longer needed, or a pod beyond a lowered replica count, can be what holds the room, and the operator does not remove either while the shared claim is missing: raise the quota. Each of these shows the same way: the `Available` condition's wait text above, a `FailedApply` event whose message contains `exceeded quota`, the same message as the reconcile error in the operator's log (`Failed to create a pool pod or claim`, or `failed to apply shared backup PVC` for the shared claim), and a rollout whose `RollingUpdate` condition stays at `PodsDrifted` or `RolloutBlocked`.
- *When the create is tried again.* Nothing tells the operator that a quota was raised or that usage fell. A pod or claim that was turned down is not sent again by every reconcile: the next create of that object goes out 5 seconds after the first, and the wait doubles with each one turned down, up to 5 minutes. A reconcile that reaches the object in between ends as if the create had been turned down again (its error starts with `not sent again before <time>`), so the shard stays in the same state without a request to the API server. A reconcile that ends this way is retried with the controller's usual delay for a failing object, which doubles from 5 milliseconds with every failure in a row up to 1000 seconds (about 17 minutes), so after a long wait the next create can be that far off. A spec change that reaches the Shard is sent at once. To retry at once after raising a quota, set or change the `multigres.com/retry-creates` annotation on any pod of the shard, for example `kubectl annotate pod <pod> multigres.com/retry-creates=$(date +%s) --overwrite`: the operator ends that shard's waits and reconciles it. No other pod annotation does this, and the admission webhook rejects direct edits of the Shard itself. A shard with no pod yet (one whose shared backup claim was turned down before any pod existed) has no such handle and is retried on the delays above. A Lite pod kept for a full quota is asked about again every 5 to 6 minutes and when the annotation is set.
- *What goes on meanwhile.* A Lite shard whose pod is missing is down until there is room; one whose pod was kept goes on serving on its current spec. A pool is short of that one pod, and no other pod of the shard is drained for a rollout while a desired pod does not exist. Pods that serve nothing are still replaced: a terminal pod, a pod stranded on an earlier topology, a pod whose sidecar never started. Scale-down is not held while a pod or a data claim waits: lowering `replicasPerCell` of any pool of the shard removes the pods beyond the new count, one at a time and subject to the usual durability check, also when what waits is a maintenance surge. While the shared backup claim waits (a running shard whose shared claim was deleted, or whose backup type changed to filesystem), nothing else of the shard is reconciled.
- *Shared namespaces.* Shards that share a namespace and its quota compete for room on equal terms, so the room a rolling replacement frees by removing its old pod can go to another shard, and the rollout then stops one pod short until there is room again. Give each cluster its own namespace, or size the quota for a rollout's extra pod, if that matters.

One refusal is reported at a time. It is removed when the spec is edited, when its pod is gone, or when its pod's replacement is acceptable, which the controller checks whenever it asks about another pod and on every status update, for a Lite shard and a pool alike, so a cause fixed outside the spec (a `LimitRange` edited, a policy removed) clears the condition and the `Degraded` phase without a spec edit, and another refused pod is shown then. A pod that is already gone, or is being removed for good (scale-down, shard deletion), is not asked about.

**Telemetry.** The sidecar uses the same runtime OTEL configuration as the other components, including operator-environment fallback and `otlpEndpoint: disabled`, with `multigres.component=pgbackrestd`. A referenced sampling ConfigMap is mounted read-only. Collect the sidecar's telemetry for the `pgbackrest.server.*` metrics that moved out of pgctld.

**Probes.** The sidecar has a startup probe and a conservative liveness probe running `pgbackrestd health`, which only asks the sidecar's status endpoint and never waits on a backup or restore, so a long operation cannot get it restarted.

**A sidecar restart takes the pod out of Ready.** Kubelet counts a native sidecar among the pod's containers when it computes readiness. When the sidecar exits, which it does on purpose after a stanza delete it could not prove complete, kubelet restarts that container alone, and the pod is NotReady until the sidecar's startup probe passes again. Postgres and the pooler keep running and existing connections are not closed, but the pod is removed from Service endpoints for that time. A pool has other replicas; a Lite shard's pod is the gateway Service's only endpoint, so new connections to a Lite cluster fail until the sidecar is back. Repeated exits add kubelet's restart backoff to that time.

**What the nodes and the S3 store must provide.** The operator cannot check either of these, and a sidecar that lacks one fails at start, which keeps its pod from becoming Ready:

- Landlock. With the default `--pgbackrest-sandbox=required`, a node whose kernel or seccomp profile does not allow Landlock fails every sidecar scheduled on it. See [pgBackRest sandbox mode](configuration.md#pgbackrest-sandbox-mode) for the requirements and for `auto` and `off`.
- An S3 endpoint certificate the sidecar can verify: one the sidecar image already trusts, or one covered by the bundle `s3.ca` names (see [TLS verification of the S3 endpoint](#1-s3-recommended-for-production)).

**Upgrading an existing cluster.** After the coordinated migration above, every pod of a cluster created before the sidecar rolls once to the new pod template, one at a time through the normal gated roll. The data volume and the shared backup volume keep their claims, and the shard ServiceAccount named by `shardServiceAccount` is adopted if it already exists. A shard spec stored by the previous operator version carries no sidecar image, so the shard controller does not touch its pods until the cluster controller rewrites it, which happens on the first reconcile after the upgrade. Until then the Shard reports `BackupRepositoryValid=False` with reason `SidecarNotConfigured`.

**Check before upgrading.** The upgraded operator refuses the cluster shapes below. Admission still accepts an unchanged stored cluster with a warning, so nothing stops the upgrade from reaching one. Every row is refused by the cluster controller: the MultigresCluster reports `BackupRepositoryValid=False` with the row's reason and `Degraded`, and its TableGroups are not rewritten, so its Shards keep a spec without a sidecar image and the shard controller stops reconciling them (`SidecarNotConfigured`): a pod lost to a node drain or failure is not recreated, pools do not scale, volumes do not expand and multiorch stays on its old image, while cells and global components move to the new images. List every MultigresCluster the operator reconciles, and fix each match before upgrading:

| Shape | Reason | Before the upgrade |
|-------|--------|--------------------|
| Distributed cluster with S3 only in a tablegroup or shard backup block. It backs up to its filesystem volume today. | `FilesystemFallback` | Move the S3 block to `spec.backup` or the database's `backup`. That changes the repository from the volume to S3, so plan it as a repository migration. |
| Any backup block with an `s3` section under `type: filesystem`. | `FilesystemFallback` | Remove the `s3` section, or change the type to `s3` at cluster or database level. |
| Lite cluster with a `filesystem` block below an S3 block. | `FilesystemFallback` | Remove the lower block, or make every level S3. |
| Distributed cluster whose tablegroup or shard block sets `type`, `encryption`, `pgbackrestTLS.secretName`, or an S3 `bucket`, `endpoint`, `region` or `keyPrefix` that differs from the database-level location. | `OverrideNotApplied` | Set the value in `spec.backup` or the database's `backup`, or remove it from the lower block. A lower block that repeats the database-level values needs no change. A shard block that only repeats what the database-level location was before someone edited it has no use, was probably written by defaulting in an earlier operator version, and can be deleted. If the differing value is `encryption`, the existing repository is encrypted with that key: `encryption` is immutable, so this needs a planned migration. |
| S3 location with none of `credentialsSecret`, `serviceAccountName` or the cluster's `shardServiceAccount` (node role, `useEnvCredentials` alone, EKS Pod Identity). | `UnsupportedCredentials` | Configure IRSA or static keys, see Credentials above. |
| Filesystem location with a `path` other than `/backups`. | `UnsupportedPath` | Remove `path` or set it to `/backups`. |
| S3 location whose `endpoint` has a scheme other than `https` (`http://minio:9000`), or is neither `host[:port]` nor a URL. | `UnsupportedEndpoint` | Change it to `host[:port]` or `https://host[:port]`; a service that only speaks plain HTTP has to get TLS first. Make the edit in step 4 of the migration. A bare `host[:port]` endpoint needs no change. |
| S3 location with a custom `endpoint` whose certificate the sidecar image does not trust (private CA, self-signed). Not refused: the previous release did not verify a custom endpoint's certificate, and the sidecar does. | none | Set `s3.ca` to the CA bundle, or `verifyTLS: false`, in step 4 of the migration, before the gate is lifted. |

A pool that has no `fsGroup` gains one (the postgres group, with `fsGroupChangePolicy: OnRootMismatch`), because the 8432 server must read its TLS Secret. The first time kubelet sees that `fsGroup` on an already populated data volume it recursively changes the volume's group ownership before starting the pod. On a large `PGDATA` that walk delays the start of each pod once; later starts skip it. It is safe for postgres, since `pgctld` normalises `PGDATA` to mode 0700 and rewrites the password file as 0600. Set an explicit pool `fsGroup` beforehand to take the walk separately.

**Service account.** `shardServiceAccount` is optional. When it is set, the cluster reports `ShardServiceAccountReady`. An account of that name that another object controls, another MultigresCluster included, is not written to, and the condition is `False` with reason `Conflict`. A conflict is reported once, in the condition and one Warning event that also names the TableGroups it keeps from being created, and is not retried on error backoff: the cluster applies again when the account changes, or, for an account without the operator's `app.kubernetes.io/managed-by` label, which the operator does not watch, about every five minutes. Editing the account's name or annotations in the cluster applies at once. While the account is not applied, for that reason or because the apply failed (`ApplyFailed`), the rest of the cluster still reconciles, but its existing TableGroups keep the account name they have and no new TableGroup is created, so no pod starts under an account this cluster does not manage. They keep their backup configuration with it, and the topology record of each database keeps its backup location: a repository and the account that reaches it are one selection, so an edit that changes both arrives whole once the account is applied, and until then backups go on to the repository the pods already use. Other edits are not held. When unset, shard pods run as the ServiceAccount named by an S3 config's `serviceAccountName` (the IRSA account an existing cluster already uses), or as the namespace default if there is none. Either way the pod does not mount an API token.
