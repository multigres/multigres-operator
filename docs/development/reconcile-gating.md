# Reconcile Gating and Laptop Mode

Reconcile gating lets a namespace (or a single object) be marked "hands off" so
the deployed operator does nothing to it: no child creation, no updates, no
deletion. It exists so a developer can run a **second** operator from their
laptop against one namespace of a shared (or kind) cluster without the two
processes fighting over the same objects.

Two pieces ship together:

- **The gate**: a label (`multigres.com/reconcile=disabled`) that any operator
  honors. This is the behaviour every release binary has.
- **Laptop mode**: the `--namespace` flag, compiled only under the `kubedial`
  build tag, which *claims* one namespace and refuses to touch anything outside
  it. A release binary cannot enter laptop mode at all.

---

## The label

| | |
|:---|:---|
| Key | `multigres.com/reconcile` |
| Gating value | `disabled` (exact, case-sensitive) |
| Value charset | `[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?`, 63 chars max |
| Where | On a **Namespace**, or on an individual gated object |

`disabled`, on the object or on its namespace, gates the object. Nothing
exempts one object inside a gated namespace. Any other value, including the
empty string or a typo like `Disabled`, is ignored exactly as if the label were
absent: a malformed label must never be the reason an object unexpectedly
freezes.

There is no operator-level override and no force-off. Only a label can gate an
object: no CLI flag, no config. That keeps "why is this paused?" answerable with
`kubectl get` on two objects.

The current label value on an object is what counts. When the label is removed,
the next reconcile sees the object as enabled again.

## What gating does: the operator does not see the object

A gated object produces no reconcile, so nothing is read for it and **nothing is
written to it**: no status, no condition, no event, no finalizer. Deletion is
included. The label is the record of why: it sits on the object or on its
namespace, so `kubectl get` on either answers "why is this paused?". There is no
status condition and no per-object metric.

This is deliberate: any marker an operator wrote on a gated object would be seen
by the other operator, and with a laptop operator claiming the namespace the two
would re-enqueue each other forever. An untouched object is what makes two
operators on one namespace stable.

Two mechanisms enforce it, and both are read-only:

- **An event filter on every controller.** Each of the five controllers
  gets the gate's event filter from `features.Gate.Wire`. The filter applies to
  every source of the controller (`For`, `Owns`, `Watches`), so child events in a
  gated namespace stop waking the parent too. It reads the `reconcile` label from
  the event object and its Namespace, through the manager cache, and drops the
  event when gated. A relabelled namespace takes
  effect on the next event, live, with no restart. Namespace objects themselves
  always pass, or the namespace watch below could never fire.

  A Namespace that is **not yet in the cache counts as gated**. The API server
  refuses objects in a nonexistent namespace, so a miss only means the Namespace
  informer is behind the object informer; failing open would give a freshly
  created gated namespace one unguarded reconcile. The Namespace's own create
  event re-enqueues its objects.
- **A backstop at the top of every `Reconcile`.** An event filter gates events,
  not the workqueue, so a `RequeueAfter` scheduled before the label went on, or a
  request produced by the namespace watch, still arrives. `Gate.Gated`
  evaluates the same rule against the cached object and Namespace and returns
  `ctrl.Result{}` with no requeue and no write. (A Namespace missing from the
  cache does not gate here, unlike in the filter: requests only reach `Reconcile`
  after the Namespace was seen, and the fake-client unit tests seed no Namespace
  at all.) TableGroup runs it as the first step of its pipeline.

Gated objects stay in the informer's memory; that is the price of gating without
a custom cache, and it is the memory the operator holds today.

### Deletion hangs on a gated object

A gated object that carries our finalizer **hangs on delete**. This is the safe
behaviour: the alternative is a paused object tearing down pods, PVCs and etcd
data, the opposite of what someone reaching for a pause switch expects (Cluster
API's paused annotation behaves the same way). **Check the object's labels and
its namespace's labels when a delete hangs.** The label is the record, and a
namespace-level `disabled` is easy to forget.

For **MultigresCluster** and **Shard**, which carry their own finalizer, a gated
delete waits until the gate is cleared. **TableGroup, Cell, and TopoServer have
no finalizer of their own**: a gated delete on one of those proceeds via
owner-reference cascade garbage collection exactly as it would without the gate.

### Alerting on a forgotten pause

The operator exports no paused metric: a gated object is never reconciled, so it
has no point at which to set a per-object series, and one set before gating would
go stale. The label is already queryable. kube-state-metrics exports namespace and
object labels through its label allowlist (`--metric-labels-allowlist`), which is
where an alert on "paused longer than N hours" belongs. Server side,
`kubectl get ns -l multigres.com/reconcile=disabled` is the audit query.

### Which controllers are gated

All five reconcilers, so a gated namespace is wholly quiet:

| Controller | Gated object |
|:---|:---|
| `multigrescluster` | `MultigresCluster` |
| `tablegroup` | `TableGroup` |
| `cell` | `Cell` |
| `toposerver` | `TopoServer` |
| `shard` | `Shard` |

The per-controller cost is one `Gate` field, one `Gate.Wire` call in
`SetupWithManager` and one backstop call.

**Gate the topmost object you intend to edit.** Gating a child (Cell,
TableGroup, TopoServer, Shard) stops only that child's controller. Its events no
longer wake the parent, so a hand edit to the gated child's own spec holds for
now, but the parent still owns that spec and overwrites the edit the next time
it reconciles for any reason: a change to the parent, another child's event, or
the periodic resync, possibly hours later. To hold a change in place, gate the
MultigresCluster (or the whole namespace).

## How a gate is noticed: the namespace watch

A label lives on the object; the gate for a whole namespace lives on the
`Namespace`. Kubernetes does not deliver "my namespace changed" events to a
namespaced resource, so a predicate alone can never wake a gated controller when
its namespace's label changes. `Gate.Wire` therefore also adds, for each
controller, a watch on Namespaces that lists every object of the controller's
kind in the changed namespace and requeues all of them, so clearing the label
immediately resumes convergence without touching the object. The requests bypass
the event filter (it applies to the Namespace event, which it passes); the
backstop decides. In laptop mode a namespace outside the claimed set is skipped
**before** listing, since a List outside the scoped cache errors on every
namespace edit.

The watch's predicate, `features.LabelsChanged()`, compares **only the
`reconcile` label**. Unrelated label or annotation churn on a busy namespace
produces no requeues; without this the namespace watch would be a requeue storm.

Un-gating produces no event for the objects inside the namespace, which is why
the watch exists. The object-level label has the mirror problem: labels do not
bump `metadata.generation`, so removing the label from a single object enqueues
nothing unless its `For()` predicate also passes a `reconcile` label change.

Two rules keep the watch honest:

- **The `For()` predicate must be OR'd with the same predicate**, e.g.
  `predicate.Or(existingPredicate, features.LabelsChanged())`. `LabelsChanged`
  returns false for every update except a `reconcile` label change, so used alone
  it would silently drop every spec update on the object itself.
- **Do not put a `LabelSelector` on the `Namespace` cache entry.** Cache all
  namespaces and let the predicate filter events; a selector on the cache would
  make the watch blind to the label change that clears the gate.

---

## Laptop mode (`--namespace`)

`--namespace=<ns>` runs the operator on a developer's machine against one
namespace of an existing cluster, in place of the deployed operator.

### Build-tag isolation

The flag is registered **only** under the `kubedial` build tag
(`cmd/multigres-operator/laptop_kubedial.go`). In a stock build the same file is
replaced by `laptop_stock.go`, where `laptopNamespace()` returns `""` and the
flag does not exist, so a release binary passed `--namespace` dies with an
unknown-flag error. That is the whole safeguard. Because a laptop process may be
run against a shared cluster by mistake and can now write to it directly, laptop
mode must never be reachable from a released artifact.

`make run` builds with `-tags kubedial` (`go run -tags kubedial
./cmd/multigres-operator`) so a local run has the flag. The Dockerfile does not
set the tag, so released images do not.

### The claim

`--namespace=<dev-ns>` is a claim on that one namespace:

- **In the claimed namespace, the laptop operator reconciles instead of the
  deployed one**, and only while the namespace carries the `disabled` label (see
  Limitations). Object labels are not consulted there.
- **Everywhere else, the operator refuses everything, unconditionally.** The
  gate treats "laptop mode on and not the claimed namespace" as gated, in the
  event filter and the backstop alike, and the cache holds nothing else anyway.

The practical consequence: claiming a namespace makes the laptop the only
operator willing to touch it, provided the deployed operator has been told to
leave it alone via the gate.

### The hard-error guards

All of these are checked at startup, before the manager is constructed and before
any watch starts. A misconfigured run exits `1` with a reason rather than
racing the deployed operator:

0. **An out-of-cluster run without `--namespace` on a cluster where an operator
   has ever been deployed** is refused. Before starting, the process lists the
   deployed operator's leader-election lease (`multigres-operator.multigres.com`)
   across all namespaces, since the install namespace is the installer's choice.
   Only its absence everywhere, as on a bare Kind cluster, lets the run start
   cluster-wide. Existence alone refuses, whoever holds it and however stale,
   because a liveness test would pass during every rollout while the lease sits
   released between leaders. After removing a deployed operator without deleting
   its namespace, delete the lease to run cluster-wide again. A list that fails
   for any reason, RBAC included, also refuses, in this mode and in laptop mode
   alike, so running either needs `list` on the `multigres-operator.multigres.com`
   lease across all namespaces. The manager ClusterRole grants it for this
   reason, scoped by `resourceNames` to that one lease (the search's
   `metadata.name` field selector is what lets Kubernetes authorise a
   cluster-wide list against a named resource; a marker in
   `api/v1alpha1/multigrescluster_types.go`), so a laptop process that
   impersonates the deployed operator's ServiceAccount passes the check with
   the operator's own permissions. This check runs in every build, stock or
   `kubedial`.
1. **`--leader-elect=true` with `--namespace`**: refused. Laptop mode has no
   business holding the leader-election lease; doing so would take the lease
   from the deployed operator and shut it down.
2. **`--webhook-enable=true` with `--namespace`**: refused. **This is
   unconditional: in-cluster or out-of-cluster makes no difference.** The
   internal certificate rotator the webhook server starts patches the shared,
   cluster-wide `MutatingWebhookConfiguration`, so a second process enabling
   webhooks would fight the deployed operator over admission configuration no
   matter where the second process runs. Laptop mode always requires
   `--webhook-enable=false`.
3. **The claimed namespace holds the deployed operator's lease**: refused. That
   is the deployed operator's own namespace, never a cluster's, and it is found
   by the same lease search as guard 0 rather than assumed to be
   `multigres-operator`.
4. **The claimed namespace does not exist or is not labelled
   `multigres.com/reconcile=disabled`**: refused. This is the claim check: the
   laptop operator reads the namespace before starting, which also rejects a
   malformed name. An unlabelled namespace would mean the deployed operator is
   still reconciling it, and the two processes would fight. Label it first.

### Connectivity: the kubedial-backed dialer

Laptop mode is the only configuration that sets the TopoServer reconciler's
`DialContext`, and only the `kubedial` build provides a real one
(`laptop_kubedial.go`). It is `kubedial.New(cfg).DialContext` with **no explicit
mode**, so it resolves `ModeAuto`: direct in-cluster dialing when the process is
in-cluster, and port-forwarding through the API server otherwise. That is what
lets a host process reach etcd member addresses inside a kind cluster with **no
VPN prerequisite**. The deployed operator keeps today's in-cluster DNS-based
dialing unchanged; outside laptop mode `DialContext` is nil and `clientv3` uses
its default.

#### Every operator connection goes through the dialer

In laptop mode the dialer carries every connection the operator opens into the
cluster, not only Kubernetes API traffic:

- the TopoServer controller's etcd member-maintenance client;
- the MultigresCluster controller's read of the etcd identity behind a topology
  reference;
- topology store connections (MultigresCluster and Shard), through multigres'
  `TopoConfig.ContextDialer`;
- multipooler RPCs (posture, backup health, reload), through
  `rpcclient.WithContextDialer`, which addresses each pooler as
  `passthrough:///host:port`. A plain gRPC dial option would not be enough:
  gRPC resolves the pooler hostname through DNS before dialling, which fails on
  a laptop before the dialer is ever called.

Outside laptop mode every one of these dials directly, exactly as before.

### Cache scoping

Laptop mode must not cache the whole cluster, so `--namespace` narrows the
manager cache to the claimed namespace alone. The rules, and
why `DefaultNamespaces` has to move with the per-type map, are in
[Caching Strategy](caching-strategy.md#laptop-mode-cache-scoping).

### Limitations

- **The claim lasts only while the label does.** A claimed namespace is
  reconciled by the laptop process only while it carries
  `multigres.com/reconcile=disabled`. Removing the label hands it back: the
  deployed operator resumes, and the laptop process stops acting on it within
  one reconcile of each object, with no restart and no ordering to remember.
- **Nothing prevents two separate laptop processes from claiming the same
  namespace.** The claim is per-process; two developers who both pass
  `--namespace=dev` will both bypass the gate in `dev` and fight each other.

---

## Laptop mode walkthrough

This is the manual end-to-end check. It
requires a kind cluster and a second operator process running from the host.
`<ns>` below is the namespace you claim; this walkthrough uses `default`.

### Deploy the cluster and gate the namespace

```sh
# Deploy the in-cluster operator (webhook enabled, into multigres-operator).
make kind-deploy
```

Apply the sample (`config/samples/minimal.yaml`) with `replicasPerCell: 3` set.
The sample's own defaults resolve to `replicasPerCell: 2` with
`durabilityPolicy: AT_LEAST_2`, which multiorch refuses to initialize while
every CR still reports `Healthy`, so override it explicitly by adding an
explicit shard/pool to the sample:

```sh
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: multigres-admin-password
  namespace: default
type: Opaque
stringData:
  password: postgres
---
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: minimal
  namespace: default
spec:
  postgresPasswordSecretRef:
    name: multigres-admin-password
    key: password
  pvcDeletionPolicy:
    whenDeleted: Delete
    whenScaled: Delete
  cells:
    - name: zone-a
      zoneId: us-central1-a
  databases:
    - name: postgres
      default: true
      tableGroups:
        - name: default
          default: true
          shards:
            - name: 0-inf
              spec:
                pools:
                  primary:
                    replicasPerCell: 3
                    type: readWrite
                    cells: [zone-a]
EOF
```

Wait for the cluster to converge, then gate the namespace:

```sh
kubectl label namespace default multigres.com/reconcile=disabled
```

Delete a pool pod by hand and assert it is **not** recreated: the in-cluster
operator is now gated and does nothing:

```sh
kubectl -n default delete pod -l multigres.com/pool=primary
kubectl -n default get pods -w    # the deleted pod does not come back
```

Nothing is written to the `MultigresCluster` while it is gated; its
`resourceVersion` stays put.

### Run the laptop operator and confirm it takes over

From the repository root, on the host. `make run` sets the `kubedial` tag but
does not take an `ARGS` passthrough, so invoke `go run` directly:

```sh
go run -tags kubedial ./cmd/multigres-operator \
  --namespace=default \
  --leader-elect=false \
  --webhook-enable=false
```

Assertions:

- The deleted pool pod is recreated.
- The deployed operator makes no write to any object in the claimed namespace
  while the laptop runs (watch `kubectl get -w ... -o yaml` for
  `resourceVersion` churn that is not the laptop's).

- **The maintenance-client dial reaches a real etcd member.** The
  TopoServer controller reserves a member for defragmentation in
  `status.etcdMaintenance`, does a linearizable health read against that member
  through the laptop dialer, and only clears `inProgress` once that read
  succeeds. So a successful dial shows as `inProgress: false` with no
  `EtcdMaintenanceFailed` warning events:

  ```sh
  # No dial/timeout failures were emitted by the maintenance pass.
  kubectl -n default get events \
    --field-selector 'involvedObject.kind=TopoServer,reason=EtcdMaintenanceFailed'
  # The reserved member's health read completed (inProgress false, not stuck true).
  kubectl -n default get toposerver -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.etcdMaintenance}{"\n"}{end}'
  ```

  A stale `inProgress: true` paired with `EtcdMaintenanceFailed` events is
  exactly the failure mode this step is here to catch: the dialer never saw
  real member traffic.

**Negative case**: the claim check must refuse an unlabelled namespace. Remove
the label first, then start the laptop operator:

```sh
kubectl label namespace default multigres.com/reconcile-
go run -tags kubedial ./cmd/multigres-operator \
  --namespace=default --leader-elect=false --webhook-enable=false
# exits 1: namespace "default" is not labelled multigres.com/reconcile=disabled
```

(The `--leader-elect=true` and `--webhook-enable=true` guards fail the same way,
before the manager starts.)

Re-gate the namespace before continuing, and stop the laptop operator:

```sh
kubectl label namespace default multigres.com/reconcile=disabled
```

### Annotation-oscillation check

Build the operator with a deliberately different default image set (a second
`image_defaults` revision), run it as the laptop operator against the same
cluster with the namespace gated, and confirm that
`multigres.com/applied-images` on the `MultigresCluster` does **not** flap:

```sh
kubectl -n default get multigrescluster minimal \
  -o jsonpath='{.metadata.annotations.multigres\.com/applied-images}{"\n"}'
```

Without gating, two operators resolving different default images overwrite the
annotation and roll the pods back and forth. With gating, only the claimed
operator reconciles, so the annotation settles.

### Remove the label and confirm the deployed operator resumes

```sh
kubectl label namespace default multigres.com/reconcile-
```

Stop the laptop operator. The in-cluster operator's namespace watch fires,
requeues the cluster in `default`, and it resumes ownership with **no restart**.
