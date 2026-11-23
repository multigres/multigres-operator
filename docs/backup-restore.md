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
| **Static credentials** | `credentialsSecret` (optionally `useEnvCredentials: true`) | Operator mounts the K8s Secret read-only into the pgBackRest sidecar; credentials are not injected into the other containers. |

Node/EC2 instance-profile fallback and EKS Pod Identity are not supported. See [Credentials](#pgbackrest-sidecar) below for admission and migration behavior.

```yaml
spec:
  backup:
    type: s3
    s3:
      bucket: my-database-backups
      region: us-east-1
      endpoint: https://s3.us-east-1.amazonaws.com  # Optional, for S3-compatible stores whose certificate the sidecar image trusts
      keyPrefix: prod/cluster-1

      # Option 1: IRSA (recommended for EKS)
      # User creates the SA externally with eks.amazonaws.com/role-arn annotation
      serviceAccountName: "multigres-backup"

      # Option 2: Static credentials from a Secret
      # (mutually exclusive with serviceAccountName)
      # credentialsSecret: "my-aws-secret"
      # useEnvCredentials: true

```

#### Repository isolation

New clusters in both Lite and distributed mode receive an S3 `keyPrefix` of `clusters/<cluster UID>` when a prefix is omitted, empty, or contains multiple slashes and nothing else. Set `keyPrefix: "/"` explicitly to restore a legacy backup from the bucket root, including when the original cluster and its PVCs are gone. With the generated prefix, the default table group and shard use `/clusters/<cluster UID>/multigres` within the bucket. Kubernetes assigns a different UID to each cluster object, including a cluster recreated with the same name, so independently created clusters do not share the default repository.

The cluster controller records the allocation in the `multigres.com/default-backup-key-prefix` annotation and writes missing prefixes into the configured backup blocks in the same API update that first adds its cleanup finalizer. This happens before creating child resources or writing topology and also works with the webhook disabled. The annotation is operator-managed; omit it when creating a cluster. Admission prevents changing or removing it after initialization. It keeps the same default available on restarts and when S3 settings or shards are added later. With admission enabled, defaulted fields are also materialized on subsequent updates; without admission, the controller applies that saved default when resolving the configuration.

Backup settings inherit from cluster to database, table group, and shard. Explicit prefixes, including inherited ones, are preserved exactly. If you supply prefixes yourself, use distinct values for independent clusters sharing a bucket; the operator does not check S3 for uniqueness. Filesystem backups are unchanged.

The `pgbackrest` sidecar in each shard pod is started with the location topology records for the shard's database, with the same prefix. In a distributed cluster that is the database-level location (`spec.backup` merged with the database's `backup`); tablegroup and shard backup blocks do not select the repository. In a Lite cluster it is the shard's resolved backup, so a block at any level applies. Multipooler reads the location from topology, compares it with the one its sidecar reports at startup, and refuses a sidecar that serves a different location. `keyPrefix: "/"` reaches both as an empty prefix, because pgBackRest adds the leading slash to the repository path itself.

**S3 never falls back to the filesystem.** If any backup block of the cluster configures S3 and a shard's location still resolves to the local filesystem, the operator refuses instead of proceeding: backups on the cluster's own volume would report success and be lost with the cluster. This happens when a distributed cluster configures S3 only in a tablegroup or shard block, when a block carries `s3` settings under a filesystem `type`, and when a block below an S3 one selects `filesystem`. Admission rejects such a cluster. With the webhook disabled, the MultigresCluster reports `BackupRepositoryValid=False` (reason `FilesystemFallback`) with a `BackupRepositoryInvalid` event, reports the `Degraded` phase, and reconciles no topology records or TableGroups until the configuration is fixed. A Shard reports the same condition and creates no pod when its own spec has an S3 backup but a filesystem sidecar location, or an S3 location without a key prefix although its cluster was allocated one (reason `MissingKeyPrefix`).

**A distributed cluster's tablegroup and shard blocks cannot change the repository's encryption, certificate or bucket.** Every sidecar of a database is configured from the database-level location, so a tablegroup or shard backup block is refused, the same way and with reason `OverrideNotApplied`, when it sets any of these to a value the database-level location does not have: `type`, `encryption`, `pgbackrestTLS.secretName` (a `pgbackrestTLS` block without a secret name asks for the generated certificate), or the S3 `bucket`, `endpoint` or `region`. Applying the block's value is not possible, and ignoring it would leave a repository that is unencrypted, or in another bucket, than the block says. A block of another `type` is refused because it replaces everything it would inherit, encryption included, while the sidecar keeps the database-level location. A block that repeats the database-level values is accepted; two spellings of one endpoint count as the same value only where they cannot differ: the letter case of the scheme and of the host name, and the single slash after a host with nothing behind it. A path, a query, a port or credentials in the endpoint are compared as written. The rule does not apply to a Lite cluster, where the shard's resolved backup is the location.

A stored cluster can have shard backup blocks that only repeat what the shard inherits: the defaulting webhook of earlier operator versions copied each shard's merged backup configuration into the shard's block when the cluster used no ShardTemplate. The webhook does not write that copy. A copy already stored keeps the values of the day it was made, so it contradicts a later edit of the location above it. In a distributed cluster that makes the edit an `OverrideNotApplied` refusal. In a Lite cluster, where the shard's block is the location, backups would keep going to the old place although the cluster level says otherwise. So on an update the defaulting webhook removes a shard block when all of this holds: the update does not change the block; merged over what the shard inherited before the update, the block added nothing; and against the edited levels above it, the block now asks for something else (in a distributed cluster, something the database-level location would not apply). The shard then inherits the edited values. A block you add or edit in the same update is never removed, and neither is one that set anything of its own, with the one exception described next: those are judged by validation, and in a Lite cluster a shard block of its own remains the shard's location. The webhook cannot tell a copy it wrote from a block a person wrote to say the same thing, and removes both.

**Taking S3 away above an S3 shard block.** An S3 shard block that the levels above it no longer resolve to is handled by a rule of its own, because removing it moves the shard's backups from S3 to the cluster's own volume.

- *The S3 block above is deleted.* That states no location, so the shard block stays, also when a filesystem block further up shows through, such as the default that defaulting stores in `spec.backup`. A Lite shard keeps its block and stays on S3. A distributed cluster, whose shard block selects nothing, is refused with `FilesystemFallback` until you delete the shard block or state the filesystem as described below.
- *A level above starts to say `type: filesystem`.* When `spec.backup`, the database's `backup` or the tablegroup's `backup` says `type: filesystem` in the update and did not before, that states a location: an S3 shard block the same update leaves unchanged is removed, and the shard backs up to the filesystem. This is the exception to the rule above: it also removes a block that set a bucket of its own.
- *`spec.backup` over the stored default.* A stored `spec.backup` always has a type, because defaulting fills an absent one with the filesystem default. A filesystem block sent for `spec.backup` over that default is what any client sends that reads the object and writes it back, so it is not seen as new and removes nothing, also in the update that deletes a database's S3 block. Write `type: filesystem` in the database's or tablegroup's `backup` instead, or delete the shard block. Writing it in `spec.backup` works when `spec.backup` was the S3 block.
- *A kept block is the shard's own from then on.* In a Lite cluster, a copy that stayed because the S3 block above it was deleted is the only statement of the shard's location, and nothing tells it from a block someone wrote. Putting S3 back above it later, or editing the bucket there, does not reach the shard: it keeps the bucket in its own block until you delete that block. Each such update returns an admission warning that names the shard, the bucket and prefix it still backs up to, and where the levels above it now resolve to; `kubectl` prints it. The warning comes with the update that changes the location above the shard, not with later writes, so a shard that is meant to keep its own location is not reported every time the cluster is edited.

With the webhook disabled nothing is removed: a distributed cluster is refused with `OverrideNotApplied` until the shard block is deleted, and a Lite cluster keeps the shard block's location until you edit or delete the block.

The cluster controller applies every rule admission applies, the credential and filesystem-path rules below included, and reports the first violation in `BackupRepositoryValid`. While a MultigresCluster is refused, its cells and global components still reconcile, but its TableGroups are not rewritten, so its Shards keep the spec they had: running pods are left alone, and spec changes below the cluster do not reach them. Only an edit to the cluster's backup configuration clears the refusal; the operator does not retry in the meantime. The `Degraded` phase is what the `MultigresClusterDegraded` alert watches, so a refused cluster raises it.

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
2. Pause reconciliation before switching incompatible operator/image versions. Stop all operator instances that reconcile the affected clusters, and prevent automation from restarting them during the switch; do not assume a per-cluster pause annotation exists. Keep reconciliation paused while installing the new operator and setting the selected images.
3. For explicitly pinned clusters, update the pins deliberately. For lazy-update clusters, select the compatible full image set and acknowledge the pending image update as required by the image update policy. Verify the effective selection and acknowledgement **before** resuming the new operator; a pending update or a default image change alone is not sufficient.
4. Resume on a canary with the paired new operator and image versions first. Verify pod startup, backup and restore/WAL operation, credential isolation, and sidecar telemetry before expanding the rollout. Never use the old operator to roll final binaries as an intermediate step.
5. If rollback is needed, pause reconciliation again and restore a tested **paired operator + full image set** before resuming. Do not roll back only the operator or only pgbackrestd. Preserve data and backup PVCs; validate data/config compatibility separately before reverting binaries.

**Credentials.** Every S3 backup config must use exactly one of these:

| Source | How |
|--------|-----|
| IRSA | `serviceAccountName` on the S3 config, or the cluster's `shardServiceAccount` carrying the role annotation |
| Static keys | `credentialsSecret` (with or without `useEnvCredentials: true`, the documented static-key form) |

Node or instance-profile credentials (an S3 config with neither of the above), `useEnvCredentials: true` without a `credentialsSecret`, and EKS Pod Identity are not supported yet. Admission rejects them. With the webhook disabled the MultigresCluster reports `BackupRepositoryValid=False` (reason `UnsupportedCredentials`) and `Degraded` and builds no Shard from the configuration, and a Shard that carries it anyway reports the same condition, reports `Degraded` and creates or replaces no pod, instead of rendering a sidecar that cannot get credentials. A cluster already stored with such a configuration stays updatable and deletable: updates that leave the violation unchanged are admitted with a warning. Filesystem repositories must use the default path `/backups`, under the same rule (reason `UnsupportedPath`).

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

An override replaces the defaults as a whole, so state every request and limit you want. Admission, in the cluster and in a ShardTemplate, rejects a block a pod could not have: a limit below the request of the same resource, a negative quantity, a name that is not `cpu`, `memory`, `ephemeral-storage`, `hugepages-<size>` or a domain-qualified resource, and any resource claim.

**A pod is not replaced by one the API server rejects.** Admission cannot know everything the API server checks, and a block can reach a Shard without it (webhook disabled, or a ShardTemplate changed under a cluster). So before the shard controller deletes a Lite shard's pod, or drains or deletes a pool pod in order to replace it (a spec change, a filesystem resize, a pod that cannot become Ready, a failed pod, a quarantined pod whose data is wiped for a restore from backup), it sends the replacement to the API server as a dry-run create under the pod's own name. The answer is read as one of four things.

- *Accepted*: no error, or only the name is taken by the pod being replaced. The replacement goes ahead.
- *Rejected*: any other client error, whatever sends it: a validation error, a `LimitRange`, Pod Security, an admission policy, a webhook that denies the pod (with `409` too). The existing pod, and for a quarantined pod its data, is left as it is. The Shard reports `PodTemplateAccepted=False` with the API server's message and a `PodTemplateRejected` event, and the Shard, its TableGroup and the cluster report `Degraded`.
- *Not answered*: a server error, a timeout, throttling, a transport error. Nothing is known about the pod yet, and the create itself would fail the same way. A Lite shard's pod is its only one, so it is kept and the question is asked again, less and less often: after an unanswered dry run the shard waits as long as the question has been unanswered so far, at least 1 second and at most 1000 seconds (about 17 minutes), before it sends the next one, so its asks fall at 1, 2, 4, 8... seconds and then every 1000. That holds whatever reconciles the shard, for as long as the question is the same: when the pod is replaced by another, turns terminal, or the spec asks for a different replacement, the new question is asked at once. A shard whose pod is not primary yet is reconciled every 10 seconds, and those passes do not each send a dry run. A settled shard is reconciled again when its next ask is due and not before. The schedule is per shard: nothing limits how many shards ask at once except the controller's 20 workers and the operator's API client rate (50 requests a second, burst 100), which all of the operator's controllers share. An API server fault that leaves every Lite shard waiting therefore starts with a wave of dry runs per doubling and thins out to one per shard every 1000 seconds; it does not turn into a steady load on the server that is failing. The schedule is kept in the operator's memory, so a restarted operator asks once and starts over. A quarantined pod, whose data cannot be brought back, is kept as well and asked about again whenever the shard is reconciled, which is at least once a minute while a pod is quarantined. Those dry runs are not spaced out the way a Lite shard's are: one is sent per reconcile for as long as the API server gives no answer, which for a shard that is otherwise idle is one a minute. A pool pod is replaced as before, unless a replacement is recorded as rejected for the shard: one refusal is recorded at a time, so the recorded one may stand for another pod built from the same template, and an unanswered question does not undo it. Each unanswered question is reported with a `PodReplacementUnanswered` warning event on the Shard. No condition is set for it and the phase does not change, because the cause is outside the shard and usually short; `UpToDate=False` on a Lite shard, and the event, say that a pod is waiting.
- *Cannot be asked*: an admission webhook that matches pods declares side effects it cannot suppress, so the API server refuses every dry run of a pod (`admission webhook "..." does not support dry run`) while it lets the create through. Waiting cannot change that, so the replacement goes ahead as it would without the question, for Lite, pool and quarantined pods alike. The one exception is a pod whose replacement is recorded as rejected, which stays until the API server can be asked again or the spec is edited. `admissionregistration.k8s.io/v1` does not accept such a webhook, so only configurations stored through `v1beta1` and never rewritten have one.

**A pod over quota is not a rejected pod.** A `ResourceQuota` turns down a pod the namespace has no room for with `exceeded quota: <name>, requested: ..., used: ..., limited: ...`, and that answer counts as accepted: the replacement goes ahead and no refusal is recorded. Quota runs last in admission, so such a pod has passed validation and every other check. The dry run is also counted next to the pod it replaces, so a namespace sized for exactly its pods turns down every dry run, and holding pods for that would stop every rollout there. When the replacement really does not fit after the old pod is gone, its create fails with the quota's message, the reconcile fails with a `FailedApply` event, and the Shard's `Available` condition names the wait after its pod count, for example `2/3 pods ready; pod <name> waiting on ResourceQuota <quota> (pods) since <time>`. The condition is the place to look: events about one object are rate limited by the client, and a Shard that has already reported many is not shown the `FailedApply` ones. The text names one pod or claim per kind (claims first): the first whose create was turned down since the Shard last started waiting for that kind, also when others are missing too. When that pod or claim turns out to exist, because something else created it, the text names the one turned down last instead, with the same time, and nothing for that kind if that one exists too. When it is not within a replica count of the spec, as after a maintenance surge is withdrawn, the text names the one turned down last if that one is within a replica count; two surge pods that are both being turned down do not replace each other. With the name come the quota and the resources it has no room for, and when that first create was turned down. The quota is the one that refused the named object's own create, or, once the reconciles have ended at another object for longer than twice the wait between creates, the one that refused the Shard's last create of that kind. Where two missing pods are refused for different resources, the resources in the text can therefore change, at most once per two waits. A kind's wait, and with it the time, starts over whenever a create of that kind succeeds or a reconcile ends without one being turned down or held back. The text stays the same while the wait does, with these exceptions. A spec edit keeps the text for the reconcile that sends the edited create and then shows that create's own pod or claim and answer, or nothing if the reconcile did not get to it yet. When two quotas of the namespace are both full, the API server names either, so the quota in the text can change from one create to the next. The text is removed on the reconcile after the create succeeds, and when the create is no longer wanted the operator reconciles the Shard once more to remove it. It is kept in the operator's memory: after an operator restart it is removed and reappears, with a new time, once a create is turned down again. The create is tried again when the namespace has room: the operator watches every `ResourceQuota` and reconciles the shards waiting in a quota's namespace when the quota is deleted, a limit in it is raised or removed, its usage falls, or its usage is computed for the first time. A shard waiting because no quota covers its pod is also reconciled when a quota is created. The pass such an event starts sends the create at once. Without an event the shard sends its next create after as long as it has been turned down so far, at least 1 second and at most 1000 seconds, so at 1, 2, 4, 8... seconds and then every 1000; a create that is still turned down right after an event starts that sequence over at 1 second. A spec edit also lets one create through at once: an answer is kept for the generation of the Shard it was given to, because an edit may ask for less (smaller requests, a smaller claim) and no quota event reports that. If that create is turned down too, the waits go on from where they were. Otherwise no create of that kind is sent for the shard in between, whatever reconciles it: the passes a shard gets every few seconds while a disruption check is waiting, and those any pod event starts, do their other work and leave the create alone. The schedule is kept per kind, pods apart from claims, and covers the shard's data volume claims and the shared backup claim alike. A quota that counts only claims or storage therefore does not hold back a pod create the pass reaches: a pod that is lost while a new claim of its own pool and cell waits on such a quota, and that comes before that claim's ordinal, is recreated at once. A pass still ends at a claim a quota turns down or that is held back, unless the shard has a pod to scale down, so a pod lost in a later cell or in a pool reconciled after it waits for the claim's next due time or for a quota event, as it does without the schedule. The shard is retried when the create its pass ended at is next due, and not for a create of the other kind that the pass does not reach. Within a kind it holds every create, not only the one that was turned down: a second pod that would have fitted waits for the shard's next due time or for a quota event that makes room. The schedule covers creates, the apply that creates the shared backup claim included, and no updates: a change to a claim that already exists is never held back, and when a quota refuses one, such as an expansion the storage quota has no room for, the reconcile fails and is retried on the controller's ordinary backoff. A kind's waits end when a create of that kind succeeds. The schedule is kept in the operator's memory, per shard; nothing limits how many shards send a create at once except the controller's 20 workers and the API client rate. How much a namespace may hold is its administrator's decision, and the operator does not work around it. When several shards share a namespace and its quota, they compete for room on equal terms: a quota event reconciles every waiting shard of the namespace at once, in no particular order, so the room a rolling replacement frees by removing its old pod can go to another shard that has waited longer for a pod of its own, and the rollout then stops one pod short until there is room again. The total stays within the quota either way. Give each cluster its own namespace, or size the quota for a rollout's extra pod, if that matters. The cost is bounded: a Lite shard is down until there is room, as it is on any failed create, and a pool is short of that one pod, because no other pod of the shard is drained for a rollout while a desired pod does not exist. Pods that serve nothing are still replaced meanwhile: a terminal pod, a pod stranded on an earlier topology, a pod whose sidecar never started. Scale-down is not held either: if you lower `replicasPerCell` of any pool of the shard to make room, the pod beyond the new count is drained and removed although desired pods are missing, in that pool or another, and their creates still fail, since it may hold exactly the room a missing pod needs. The same goes for a maintenance surge pod or claim that a quota turns down: the cell that needed the surge gets nothing else until the surge exists, and scale-down goes on in the other cells. Two pools that are each short of a pod and each hold a pod beyond their count are scaled down one pod at a time. The shared backup claim is the exception: it is applied before any pool is reconciled, and while it is missing and a quota turns its create down the pass ends there, before any scale-down, so lowering `replicasPerCell` frees nothing until that claim exists. That only arises when the shared claim of a running shard is deleted or its backup type changes. Whether the shard stays durable without the pod is still checked before each removal. A pod beyond the count that is the primary is chosen only when no other such pod is left, and is then drained the same way a primary is for a rolling update. The same answer is read from `insufficient quota to consume: ...`, which the API server gives when the cluster requires a quota for a resource and the namespace has none that covers it. `status unknown for quota: ...`, the answer for a quota whose usage is not computed yet, counts as not answered. A quota that refuses the pod for what it is, such as `must specify limits.memory`, is an ordinary rejection.

One refusal is reported at a time. It is removed when the spec is edited, when its pod is gone, or when its pod's replacement is acceptable, which the controller checks whenever it asks about another pod and on every status update, for a Lite shard and a pool alike, so a cause fixed outside the spec (a `LimitRange` edited, a policy removed) clears the condition and the `Degraded` phase without a spec edit, and another refused pod is shown then. A pod that is already gone, or is being removed for good (scale-down, shard deletion), is not asked about.

**Telemetry.** The sidecar uses the same runtime OTEL configuration as the other components, including operator-environment fallback and `otlpEndpoint: disabled`, with `multigres.component=pgbackrestd`. A referenced sampling ConfigMap is mounted read-only. Collect the sidecar's telemetry for the `pgbackrest.server.*` metrics that moved out of pgctld.

**Probes.** The sidecar has a startup probe and a conservative liveness probe running `pgbackrestd health`, which only asks the sidecar's status endpoint and never waits on a backup or restore, so a long operation cannot get it restarted.

**A sidecar restart takes the pod out of Ready.** Kubelet counts a native sidecar among the pod's containers when it computes readiness. When the sidecar exits, which it does on purpose after a stanza delete it could not prove complete, kubelet restarts that container alone, and the pod is NotReady until the sidecar's startup probe passes again. Postgres and the pooler keep running and existing connections are not closed, but the pod is removed from Service endpoints for that time. A pool has other replicas; a Lite shard's pod is the gateway Service's only endpoint, so new connections to a Lite cluster fail until the sidecar is back. Repeated exits add kubelet's restart backoff to that time.

**What the nodes and the S3 store must provide.** The operator cannot check either of these, and a sidecar that lacks one fails at start, which keeps its pod from becoming Ready:

- Landlock. With the default `--pgbackrest-sandbox=required`, a node whose kernel or seccomp profile does not allow Landlock fails every sidecar scheduled on it. See [pgBackRest sandbox mode](configuration.md#pgbackrest-sandbox-mode) for the requirements and for `auto` and `off`.
- An S3 endpoint certificate the sidecar image already trusts. The sidecar always verifies the endpoint's TLS certificate, and the operator has no field that gives it a CA. An S3-compatible store behind a private CA cannot be used.

**Upgrading an existing cluster.** After the coordinated migration above, every pod of a cluster created before the sidecar rolls once to the new pod template, one at a time through the normal gated roll. The data volume and the shared backup volume keep their claims, and the shard ServiceAccount named by `shardServiceAccount` is adopted if it already exists. A shard spec stored by the previous operator version carries no sidecar image, so the shard controller does not touch its pods until the cluster controller rewrites it, which happens on the first reconcile after the upgrade. Until then the Shard reports `BackupRepositoryValid=False` with reason `SidecarNotConfigured`.

**Check before upgrading.** The upgraded operator refuses the cluster shapes below. Admission still accepts an unchanged stored cluster with a warning, so nothing stops the upgrade from reaching one. Every row is refused by the cluster controller: the MultigresCluster reports `BackupRepositoryValid=False` with the row's reason and `Degraded`, and its TableGroups are not rewritten, so its Shards keep a spec without a sidecar image and the shard controller stops reconciling them (`SidecarNotConfigured`): a pod lost to a node drain or failure is not recreated, pools do not scale, volumes do not expand and multiorch stays on its old image, while cells and global components move to the new images. List every MultigresCluster the operator reconciles, and fix each match before upgrading:

| Shape | Reason | Before the upgrade |
|-------|--------|--------------------|
| Distributed cluster with S3 only in a tablegroup or shard backup block. It backs up to its filesystem volume today. | `FilesystemFallback` | Move the S3 block to `spec.backup` or the database's `backup`. That changes the repository from the volume to S3, so plan it as a repository migration. |
| Any backup block with an `s3` section under `type: filesystem`. | `FilesystemFallback` | Remove the `s3` section, or change the type to `s3` at cluster or database level. |
| Lite cluster with a `filesystem` block below an S3 block. | `FilesystemFallback` | Remove the lower block, or make every level S3. |
| Distributed cluster whose tablegroup or shard block sets `type`, `encryption`, `pgbackrestTLS.secretName`, or an S3 `bucket`, `endpoint` or `region` that differs from the database-level location. | `OverrideNotApplied` | Set the value in `spec.backup` or the database's `backup`, or remove it from the lower block. A lower block that repeats the database-level values needs no change. A shard block that only repeats what the database-level location was before someone edited it has no use, was probably written by defaulting in an earlier operator version, and can be deleted. If the differing value is `encryption`, the existing repository is encrypted with that key: `encryption` is immutable, so this needs a planned migration. |
| S3 location with none of `credentialsSecret`, `serviceAccountName` or the cluster's `shardServiceAccount` (node role, `useEnvCredentials` alone, EKS Pod Identity). | `UnsupportedCredentials` | Configure IRSA or static keys, see Credentials above. |
| Filesystem location with a `path` other than `/backups`. | `UnsupportedPath` | Remove `path` or set it to `/backups`. |

A pool that has no `fsGroup` gains one (the postgres group, with `fsGroupChangePolicy: OnRootMismatch`), because the 8432 server must read its TLS Secret. The first time kubelet sees that `fsGroup` on an already populated data volume it recursively changes the volume's group ownership before starting the pod. On a large `PGDATA` that walk delays the start of each pod once; later starts skip it. It is safe for postgres, since `pgctld` normalises `PGDATA` to mode 0700 and rewrites the password file as 0600. Set an explicit pool `fsGroup` beforehand to take the walk separately.

**Service account.** `shardServiceAccount` is optional. When unset, shard pods run as the ServiceAccount named by an S3 config's `serviceAccountName` (the IRSA account an existing cluster already uses), or as the namespace default if there is none. Either way the pod does not mount an API token.
