# Status Ownership

Every status field of the five CRs (`MultigresCluster`, `TableGroup`, `Cell`, `TopoServer`, `Shard`) has exactly one field manager that writes it, and the test suite fails any case where the objects disagree. This page explains the contract, how it is enforced, and what to do when you add a field.

## The contract lives in code

[`pkg/ownership`](../../pkg/ownership) holds the table, `Fields`. Each row gives a status field prefix, its owning field manager, its class, when it is expected to change, and, for Gate and WorkflowMemory fields, who reads it and what unblocks the reader when the value is degraded, unreadable or stale. `OwnerOf(kind, path)` resolves a path by longest prefix at a path-element boundary, so one row for `.status.conditions[type="X"]` covers every sub-field of that condition. The rows are not copied here; read `fields.go`.

The table also has one Pod row, for the `multigres.com/pooler-data-ready` condition the shard controller writes on pool Pods.

## Managers

One manager per controller, plus facets for writes a controller makes outside its main status apply:

| Kind | Manager |
|:---|:---|
| Controller | `multigres-cluster`, `multigres-tablegroup`, `multigres-cell`, `multigres-toposerver`, `multigres-shard` |
| Facet | `multigres-shard-guard`, `multigres-shard-readiness`, `multigres-toposerver-guard`, `multigres-etcd-maintenance` |

Each controller's client is wrapped with `client.WithFieldOwner` in its `SetupWithManagerReconciler`, so every write through `r.Client` carries the controller's name. Facets pass a per-call `client.FieldOwner` instead. The constants are in `pkg/ownership`; use them, never a string literal.

Why one per controller: SSA never reports a conflict between a manager and itself. When a parent and a child controller shared a name, each could overwrite the other's fields and nothing in the API would say so.

## Classes and rules

- **Observation**: reconstructible from the world; survives a status wipe unchanged.
- **Gate**: the table entry names the reader and what unblocks it when the input is degraded, unreadable or stale. A gate whose input is fed by the waiter is a defect.
- **WorkflowMemory**: exactly one writer; written by an owned SSA field or under optimistic lock; declared, because wiping it changes behaviour.

The Pod readiness condition `multigres.com/pooler-data-ready` is a Gate: it feeds Pod Ready, which the drain and disruption gates read.

### Gates

The TableGroup drain's `ReadyForDeletion` reads a stale cache until PR #709 merges; it is the canonical stale-input Gate.

## Enforcement

Every `test/suite` case checks ownership in cleanup (`newCase` registers `requireOwnership`). The invariant (`test/suite/ownership_invariant.go`) fails on:

- a status field claimed by a manager other than its table owner, or claimed by anyone when the table has no row for it;
- the same non-status field claimed by more than one operator manager (operator-vs-operator overlap);
- an operator manager claiming anything under `.status` of a pool Pod that the table does not declare;
- any legacy manager name on any checked object.

The check lists the five CR kinds and every Pod in the case namespace. For a case that creates no CRs it is lenient: it checks whatever objects exist and passes if there are none. For a namespace passed by hand to `ownershipViolations` it is strict and errors on zero CR objects, so a mistyped namespace cannot pass having compared nothing.

`TestStatusWipe` proves the class rules. `TestFieldsCoverEveryStatusField` (`pkg/ownership`) fails when a status field of any of the five CRs has no row.

## Adding a status field

Add a row to `Fields`, pick the class, and write `Readers` if anything decides on the value. Until you do, `TestFieldsCoverEveryStatusField` fails, and so does every suite case that writes the field.

A new condition type is different: it falls under the per-kind `conditionsOwner` catch-all row, so nothing forces a decision on it. Add a row when a condition has its own writer.

## Force

Every apply uses `client.ForceOwnership`, which is standard for controllers: a controller must be able to take back a field. It means conflict errors will never flag a clobber. The ownership invariant above is what catches one.

## Legacy names and `DropLegacy`

Before the split the controllers wrote as `multigres-operator`, `multigres-resource-handler`, `multigres-resource-handler-guard`, `multigres-resource-handler-readiness` and `multigres-operator-guard`. `ownership.IsLegacy` recognises them, and `ownership.DropLegacy` removes their `managedFields` entries from an object in one guarded JSON patch. Force alone cannot do this: it does not retire a legacy co-owner that applied the same value, so the entry has to be removed. Controllers reach it through `ownership.TidyLegacy` from their status or readiness paths, after the write the tidy-up follows has stood. The tidy-up is best-effort: a failure is logged, never returned and never requeued, and the next reconcile or resync retries it. A 422 usually means the object was stale (the patch tests the resourceVersion, and a caller can pass a cached object), so it is logged at info level; anything else is logged as an error.

Known limitation: a field that only a legacy entry owned is stranded, because nothing will prune it, if the first apply under the new name omits it. Before the rename the same manager would have pruned it on that apply. The window is one reconcile around the upgrade.

This is temporary migration code. Remove `DropLegacy`, `TidyLegacy`, `IsLegacy`, the legacy constants and the invariant's legacy rule once the fleet reports no legacy managers in any environment.

`certs.FieldOwner` (`multigres-operator`, writes Secrets) and `multigres-operator-cert` are unrelated Secret writers and keep their names.
