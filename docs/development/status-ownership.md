# Status Ownership

Every status field of the five CRs (`MultigresCluster`, `TableGroup`, `Cell`, `TopoServer`, `Shard`) has exactly one field manager that writes it, and the test suite fails any case where the objects disagree. This page explains the contract, how it is enforced, how objects written by an earlier operator version are migrated, and what to do when you add a field.

## The contract lives in code

[`pkg/ownership`](../../pkg/ownership) holds the table, `Fields`. Each row gives a status field prefix, its owning field manager and its class. Gate and WorkflowMemory rows carry a comment naming who reads the field and what unblocks the reader when the value is degraded, unreadable or stale. `OwnerOf(kind, path)` resolves a path by longest prefix at a path-element boundary, so one row for `.status.conditions[type="X"]` covers every sub-field of that condition. The rows are not copied here; read `fields.go`.

The table also has one Pod row, for the `multigres.com/pooler-data-ready` condition the shard controller writes on pool Pods.

## Managers

Two kinds of write, two naming rules.

| Write | Manager |
|:---|:---|
| Status, one per controller | `multigres-cluster`, `multigres-tablegroup`, `multigres-cell`, `multigres-toposerver`, `multigres-shard` |
| Status facets | `multigres-shard-guard`, `multigres-shard-readiness` (pool Pod readiness), `multigres-toposerver-guard`, `multigres-etcd-maintenance` |
| Child objects | `multigres-operator` (`ownership.ManagerChildWriter`) |

Every status write and every child-object apply names its owner per call, from the constants in `pkg/ownership`; never use a string literal. Status applies go through `ownership.ApplyStatus(ctx, c, obj, manager)`, which applies with `ForceOwnership` and then runs the legacy tidy-up described below. The merge-patch status writes (the Shard's backup and pod-role updates, the etcd maintenance record) pass `client.FieldOwner` themselves.

**Writes with no explicit owner.** Finalizer adds and removals, `Create` calls (the maintenance surge PVC and Pod in `shard/maintenance_surge.go`) and metadata merge patches (the cluster's tracking labels in `multigrescluster_controller.go`) pass no field owner. The apiserver attributes them to the program name in the client's User-Agent, which is `manager` in the shipped image (and the test binary's name under `go test`). They are left that way: none of them writes status, each field they touch has one writer, and renaming them would strand fields under the old name exactly as described for child objects below. The ownership invariant still catches such a write if it ever touches status, because its status rule checks every manager, not only the operator's names.

**Why one status manager per controller.** SSA never reports a conflict between a manager and itself. When a parent and a child controller wrote status under one shared name, each could overwrite the other's fields and nothing in the API would say so.

**Why child objects keep `multigres-operator`.** Children are the Deployments, Services, StatefulSets, ConfigMaps, PVCs, PDBs, NetworkPolicies and cert-manager Certificates a controller renders, plus the spec of a child CR its parent applies (`Cell`, `TableGroup`, `Shard`, `TopoServer`). Each has exactly one writer, so a per-controller name would separate nobody. Renaming is also unsafe for objects that already exist: an apply under a new name leaves the old entry co-owning every field it set, so a field the new apply omits is never pruned (a removed pod annotation stays on the Deployment; disabling `externalGateway` leaves `spec.externalIPs` on the global Service). Worse, a spec change that drops a field the old entry owns can fail validation on every apply: raise a CPU request past the old limit and remove the limit, and the merged object still carries the limit, so the apply is rejected forever and nothing after it in the reconcile runs. `certs.FieldOwner` is the same name for the same reason. `multigres-operator-cert` (the webhook's own Secrets and `caBundle`) is unrelated.

## Classes and rules

- **Observation**: reconstructible from the world; survives a status wipe unchanged.
- **Gate**: the table entry names the reader and what unblocks it when the input is degraded, unreadable or stale. A gate whose input is fed by the waiter is a defect.
- **WorkflowMemory**: exactly one writer; written by an owned SSA field or under optimistic lock; declared, because wiping it changes behaviour.

The Pod readiness condition `multigres.com/pooler-data-ready` is a Gate: it feeds Pod Ready, which the drain and disruption gates read.

### Gates

The TableGroup drain (`handlePendingDeletion`) reads each child Shard's `ReadyForDeletion` condition from the informer cache, which can lag. That is safe only because the Shard writes the condition `True` once and never back to `False`: a stale read can only show it absent or `False`, which delays the drain by one requeue and never deletes a Shard early. A writer that could clear it would make the cached read unsafe, and the read would have to go to the API server.

The Cell's `ReadyForDeletion` is the exception: the Cell controller clears it when MultigresCluster cancels a drain by putting the Cell back into spec. Three rules keep the cluster's cached read safe. The drain writes the condition conditioned on the `resourceVersion` it read, so the condition is only ever stored on a version that still carries the pending-deletion stamp, and every un-annotated version a later reconcile reads already shows it. The Cell controller clears it, and persists the clear, before it recreates anything the drain removed, so a stored `True` always describes a Cell whose drain postcondition still holds, even one left over from a cancelled drain. And `reconcileCells` deletes with a `{UID, ResourceVersion}` precondition taken from the read that showed the stamp and the condition together, so a Cell that has changed since (cancelled, cleared, stamped again) is not deleted on the strength of a stale read; the conflict requeues the cluster.

## Enforcement

Every `test/suite` case checks ownership in cleanup (`newCase` registers `requireOwnership`). The invariant (`test/suite/ownership_invariant.go`) lists the five CR kinds and every Pod in the case namespace and fails on:

- a status field claimed by any manager other than its table owner, or claimed by anyone when the table has no row for it. Legacy entries (see below) are judged by the legacy rule instead;
- a populated status leaf claimed by no manager at all. Each leaf is checked on its own: every non-zero scalar, every member of a list the CRD declares a set, every other list that is not a list of conditions, and every field of every condition, keyed by type. A leaf is owned only when an entry claims it or something below it. A claim on a sibling (`postgresConfig.lastAppliedAt` beside `postgresConfig.error`) or on an ancestor alone (`postgresConfig: {}`) owns nothing. Zero values are exempt, because the typed read cannot tell a zero from an absent field. On a Pod only the fields of conditions whose type has a table row are read, so kubelet's status is never checked;
- the same non-status field claimed by more than one operator manager, the child writer included;
- on a Pod, a claim on a table-declared path by any manager other than the declared one (the implicit `manager` included), or an operator manager claiming anything under `.status` that the table does not declare;
- a legacy entry. A case that does not write legacy entries on purpose allows none. `TestLegacyManagerMigration` allows only the entries `DropLegacy` must keep: a legacy entry that claims at least one field no current entry covers. One whose every claim is covered was removable, so the tidy-up missed it.

Together the first two make the contract exact for every populated status leaf: it has an owner, and outside the migration scenario that owner is the one in the table. A field whose value is zero can be ownerless without failing.

The check is lenient about empty namespaces: a case that creates no CR has nothing to violate and passes. `TestLegacyManagerMigration` requires that it examined at least one CR.

`TestStatusWipe` proves the class rules. `TestFieldsCoverEveryStatusField` (`pkg/ownership`) fails when a status field of any of the five CRs has no row.

## Adding a status field

Add a row to `Fields` and pick the class. For a Gate or WorkflowMemory row, add a comment naming the reader and what unblocks it. Until the row exists, `TestFieldsCoverEveryStatusField` fails, and so does every suite case that writes the field.

A new condition type is different: it falls under the per-kind `conditionsOwner` catch-all row, so nothing forces a decision on it. Add a row when a condition has its own writer.

## Force

Every apply uses `client.ForceOwnership`, which is standard for controllers: a controller must be able to take back a field. It means conflict errors will never flag a clobber. The ownership invariant above is what catches one.

## Legacy status entries

An operator version that wrote status under shared names leaves entries the per-controller managers do not replace: `multigres-operator` and `multigres-resource-handler` on the status subresource, and `multigres-resource-handler-guard`, `multigres-resource-handler-readiness` and `multigres-operator-guard` anywhere. Legacy is a property of an entry, not of a name: `multigres-operator` on a main resource is the live child writer. `ownership.IsLegacyEntry` is the predicate, and it never treats a main-resource `multigres-operator` entry as legacy.

`ownership.DropLegacy` removes legacy entries in one JSON patch on the main resource; Force alone does not retire a legacy co-owner that applied the same value. A field is covered when a current (non-legacy) entry claims it or something below it; the bare `.status` container is ignored. A claim on an ancestor alone never covers: the apiserver records an applied empty object (`postgresConfig: {}`, or a struct such as `status.images.applied` sent empty when every image is pinned) and the remainder of a granular map as a claimed container with nothing below it, which owns none of the fields that may sit there. The rule errs toward keeping: a wrong "not covered" costs a kept entry or one extra apply, a wrong "covered" loses a field's owner. `ownership.Owns` and the suite invariant use the same rule. A legacy entry is:

- **Removed** when every field it claims is covered.
- **Kept** otherwise, without error: removing it would leave a populated field with no owner, invisible to every later apply. A later pass removes it once a current writer claims those fields.

A legacy entry goes on the first pass whose current applies send everything it claims. Every status writer re-applies its whole status each pass, and the Shard's main status apply sends the pod roles and every condition but the shard guard's `StorageClassValid` and `BackupRepositoryValid`, so this covers the Update entries the Shard's merge-patch writers leave as well (see Known limitations for when it does not happen).

A status writer that skips an unchanged value (the storage-class guards, the Lite backup-repository guard, pool Pod readiness) skips only when its own entry already owns it; `ownership.SkipUnchanged` holds that rule and also runs the tidy-up, so a writer whose value never changes (a missing StorageClass stops every pass at the guard) still retries one that failed.

The patch has no resourceVersion precondition, which a busy object would fail every time. It tests what the decision read: each removed index still holds the identical entry, and for every field of a removed entry, one current entry covering it still has the same manager, operation, subresource and fields. A current writer withdrawing a shared field in between fails the patch, and the legacy entry stays the field's owner.

`ApplyStatus` runs the tidy-up (`ownership.TidyLegacy`) after every status apply and `SkipUnchanged` on every skip. It is best-effort: a failure is logged, never returned or requeued, and the next pass retries. The apiserver reports a JSON patch it cannot apply, which here means a test op found a changed list, as a 422 with reason `Invalid` and empty details; that shape is logged at Info and everything else at Error. Schema and CEL failures, ValidatingAdmissionPolicy denials and this repo's webhooks (controller-runtime, 403 or a 422 naming the object) do not have that shape. A foreign admission webhook that answers 422 `Invalid` with no details does, and is logged at Info as a changed list: nothing distinguishes the two.

### Known limitations

**A field the first apply under a new name omits is never pruned.** If a status field a legacy Apply entry owns is absent from the first apply under the current manager, the current manager never claims it. It stays populated, owned only by the kept legacy entry (or by nobody, once that entry is removed by hand), and no apply prunes it until someone removes the entry and the field. It takes a state change that coincides with the upgrade: an external gateway disabled at upgrade time leaves the cluster's `GatewayExternalReady=True` condition; a Shard's `status.postgresConfig.error` survives a recovery completed during the upgrade. The kept entry is kept whole, so this reaches beyond the omitted field: every field it co-owns with the current manager stays co-owned, and when the current manager later omits one of them (a `phase` it stops sending) the value stays, still owned by the legacy entry. Only when the first apply sends everything the legacy entry claims is the entry removed and pruning normal from then on.

**The implicit manager's entry on MultigresClusters.** An operator version that wrote status with `Status().Update` and no field owner leaves a `manager`/`Update`/`status` entry on each MultigresCluster it wrote (`manager` is the program name in the shipped image's User-Agent). It owns the status fields that update changed: `.status.images` and the `ImageRolloutPending` condition. `IsLegacyEntry` does not select it, because `manager` is also the live name of the non-status writes above, so the tidy-up never touches it. `status.images.applied` and `status.images.available` are then never pruned when a cluster moves to fully pinned images, and `applied` is the WorkflowMemory row `recordedImages` falls back to when the applied-images annotation is missing. Clear it by hand with the procedure below, after checking that it claims only those fields.

### Removing an entry by hand

Remove one entry by index with a JSON patch whose `test` op pins the whole entry as you inspected it, as `DropLegacy` does. Save the object, list its entries and pick the index:

```sh
kubectl get multigrescluster <name> -n <ns> --show-managed-fields -o json > obj.json
jq -c '.metadata.managedFields | to_entries[] | [.key, .value.manager, .value.operation, .value.subresource, .value.fieldsV1]' obj.json
```

Build the patch from the saved copy and apply it:

```sh
jq --argjson i <index> '[
  {op: "test", path: "/metadata/managedFields/\($i)", value: .metadata.managedFields[$i]},
  {op: "remove", path: "/metadata/managedFields/\($i)"}
]' obj.json > patch.json
kubectl patch multigrescluster <name> -n <ns> --type=json --patch-file=patch.json
```

If the entry changed in any way since `obj.json` was saved (another write, new claims, a reordered list), the `test` op fails with "the server rejected our request due to an error in our request" and nothing changes; save the object again and start over. On a MultigresCluster the patch goes through the validating webhook, which denies it while the cluster's spec does not validate. The patch changes no value. A field only the removed entry owned stays populated with no owner: no later apply prunes it, and a writer that sends it takes ownership. To clear a stale value, apply it under a throwaway manager on the status subresource, then apply nothing under that manager.

**Never reset the list.** Patching `managedFields` to `[]` changes nothing. Patching it to `[{}]` removes every entry at once: spec and metadata fields are unowned until the next main-resource apply, and the next status apply records a `before-first-apply`/`Update`/`status` entry that claims every status field present (and the labels), so nothing that apply omits is ever pruned. That brings back the stranded-field and validation-deadlock problems described for child objects above. Remove entries by index instead.

**Rolling back.** An operator version that writes status under the shared names leaves the per-controller managers' entries co-owning what they set, so its applies cannot prune those fields. After the rolled-back operator has applied once, remove the per-controller entries (`multigres-cluster`, `multigres-shard` and the others in the table at the top) by index as above.

### When to delete this code

The migration code is `DropLegacy`, `TidyLegacy`, `SkipUnchanged`'s ownership check, `IsLegacyEntry`, the legacy constants and the invariant's `keptLegacy` rule. Query the fleet for the entries `IsLegacyEntry` selects on the five CR kinds and on pool Pods. It can go when none remain other than the documented leftovers above, each cleared by hand or accepted. Judge a legacy entry that persists by its claims, not by its manager, operation or subresource: what it claims that no current entry sends is the field the tidy-up could not cover, and that needs investigating before the code goes.
