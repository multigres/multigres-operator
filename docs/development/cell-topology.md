# Cell Topology and Zone ID/Region Allocation

## Local TopoServer (Cell Children) — Not Yet Implemented

### What the Design Specifies

The API design document defines an optional `LocalTopoServer` as a child of each `Cell`. When a Cell specifies a `localTopoServer` (via inline `CellSpec`, `CellTemplate`, or the override chain), the Cell controller should create a dedicated `TopoServer` child CR with its own etcd cluster, providing cell-local topology storage instead of relying on the global topology server.

The relevant design elements:
- The `MultigresCluster` resource tree shows `LocalTopoServer (Child CR, optional)` under each Cell.
- The `CellTemplate` spec includes an optional `localTopoServer` section with `etcd` or `external` options.
- The Cell child CR has three topology options: (1) use global (default, empty `topoServer`), (2) inline external, and (3) managed local etcd.

### What Is Already Scaffolded in the Operator

The following plumbing is in place and functional:

1. **API Types**: `LocalTopoServerSpec` (in `toposerver_types.go`) defines the configuration with `etcd` and `external` options, including CEL validation for mutual exclusion.
2. **CellTemplate**: The `CellTemplateSpec` has a `LocalTopoServer *LocalTopoServerSpec` field.
3. **MultigresCluster inline CellSpec**: The `CellInlineSpec` has a `LocalTopoServer *LocalTopoServerSpec` field.
4. **Cell child CR**: The `CellSpec` has a `TopoServer *LocalTopoServerSpec` field ready to receive the resolved config.
5. **Resolver**: `pkg/resolver/cell.go` fully resolves `LocalTopoServerSpec` through the 4-level override chain (inline → template → cluster default → namespace default).
6. **Webhook defaulter**: `pkg/webhook/handlers/defaulter.go` passes the resolved `localTopoSpec` into the Cell CR builder.
7. **Cluster controller**: `builders_cell.go` accepts `localTopoSpec` as a parameter and sets it on the Cell CR's `spec.topoServer`.
8. **CEL Feature Gate**: Both `CellInlineSpec` and `CellTemplateSpec` have a CEL `XValidation` rule (`!has(self.localTopoServer)`) that rejects any attempt to set `localTopoServer`. This prevents silent misconfiguration until the feature is implemented. The rule must be removed and CRDs regenerated when the feature ships.

### Why It Is Not Implemented

The Cell controller (`pkg/resource-handler/controller/cell/cell_controller.go`) only reconciles the **MultiGateway Deployment** and **MultiGateway Service**. It does **not** inspect `spec.topoServer` or create a child `TopoServer` CR. The reason is that **upstream multigres does not yet support separate per-cell topology servers in practice**.

Specifically:

1. **Upstream architecture supports it conceptually**: The `topoclient.Store` in multigres maintains a two-tier topology model — a global connection and per-cell connections via `ConnForCell()`. The `Cell` protobuf (`clustermetadata.Cell`) has `ServerAddresses` and `Root` fields that can point to a separate etcd instance.

2. **But the `createclustermetadata` CLI always reuses the global server**: When cells are registered by the upstream `multigres topo createclustermetadata` command, each cell's `ServerAddresses` is set to the global topo server address, and the cell's `Root` is derived as `{globalRoot}/{cellName}`. This means all cells share the same etcd cluster, just with different key prefixes.

3. **No upstream testing or validation of separate per-cell etcd**: While `ConnForCell()` would technically connect to a different etcd address if a cell had one, this path has never been exercised in production or in the upstream test suite. There may be undiscovered issues with connection lifecycle, failure handling, or data consistency when cells use independent etcd clusters.

4. **The operator's topology registration**: Cell and database registration in the global topology server is centralized in the MultigresCluster controller (`reconcileTopology`), not in individual Cell or Shard controllers. The Cell CR includes a `topologyReconciliation.registerCell` flag that is set by the MultigresCluster controller when building Cell CRs, but the actual registration is performed centrally. This registration currently always sets the cell's topo address to the global topo server. Supporting local topo servers would require the operator to register cells with different `ServerAddresses` pointing to the local etcd.

### How to Implement When Upstream Adds Support

Once upstream multigres validates and supports per-cell topology servers, the operator implementation would involve:

#### 0. Remove CEL Feature Gate

Delete the `XValidation` markers from `CellInlineSpec` (in `multigrescluster_types.go`) and `CellTemplateSpec` (in `celltemplate_types.go`), then run `make generate manifests` to regenerate CRDs.

#### 1. Cell Controller: Create TopoServer Child CR

Add a `reconcileLocalTopoServer` step in `cell_controller.go` before the MultiGateway reconciliation:

```go
// In Reconcile(), before reconciling MultiGateway:
if cell.Spec.TopoServer != nil && cell.Spec.TopoServer.Etcd != nil {
    if err := r.reconcileLocalTopoServer(ctx, cell); err != nil {
        return ctrl.Result{}, err
    }
}
```

This would create a `TopoServer` child CR (owned by the Cell) using the existing `TopoServer` controller infrastructure already used for the global topo server. The naming would follow the pattern `{cluster}-{cell}-local-topo`.

#### 2. Cell Controller: Update `SetupWithManager`

Add `Owns(&multigresv1alpha1.TopoServer{})` to the controller builder so the Cell controller watches its child TopoServer.

#### 3. Cell Registration: Use Local TopoServer Address

When registering the cell in the global topology (via `createclustermetadata` or an equivalent controller-side call), set the cell's `ServerAddresses` to the local etcd service address instead of the global one:

```
Cell "us-east-1a":
  ServerAddresses: ["{cluster}-{cell}-local-topo-client.{ns}.svc.cluster.local:2379"]
  Root: "/multigres/{cell}"
```

This way, when multigres components call `ConnForCell("us-east-1a")`, the `topoclient.Store` would connect to the local etcd instead of the global one.

#### 4. MultiGateway/MultiOrch: No Changes Needed

The multigateway and multiorch binaries already connect to cell topology via `ConnForCell()`, which reads the cell's `ServerAddresses` from the global topology. If the operator registers the cell with a local address, pool discovery and orchestration will automatically use the local etcd. No changes to the data-plane flags are required.

#### 5. Cell Controller: Status Updates

The Cell status should reflect the local TopoServer's health when present. This means:
- Adding TopoServer readiness to the Cell's conditions.
- Blocking MultiGateway creation until the local TopoServer is available (since the gateway needs a working topo server to discover poolers).

#### 6. External TopoServer Support

For `spec.topoServer.external`, no child CR creation is needed — the cell registration simply uses the provided external endpoints as the cell's `ServerAddresses`. This is simpler to implement and could be done first as a stepping stone.

---

## What Cells Are

A Cell is a **logical failure domain** — a boundary that upstream multigres uses for consensus, sync replication, and pool discovery. When a pooler or gateway registers itself in the topology server, it carries an `ID.Cell` field that identifies which cell it belongs to.

Upstream multigres has **no awareness of physical infrastructure topology**. The `Cell` protobuf (`clustermetadata.Cell`) contains only `Name`, `ServerAddresses`, and `Root` — there are no zone, region, or Kubernetes scheduling fields. The mapping of "cell = availability zone" is a convention enforced entirely by the operator.

## Why Cells Matter for Durability

The durability policy controls how multiorch enforces synchronous replication acknowledgment during failover. It is configurable via `spec.durabilityPolicy` on the `MultigresCluster` (cluster-wide default) and per-database via the `databases[].durabilityPolicy` override. See the [Durability Policy](../durability-policy.md) user guide for configuration details.

The `MULTI_CELL_AT_LEAST_2` policy groups standbys by `Id.Cell` and requires synchronous replication acknowledgment from N distinct cells. `BuildSyncReplicationConfig` in multiorch **excludes standbys in the same cell as the primary** when building the sync standby list for multi-cell policies.

This means cells are the mechanism by which multigres guarantees that a committed write survives the failure of an entire availability zone. If a shard has pools in `zone-1` and `zone-2` and uses `MULTI_CELL_AT_LEAST_2`, a write is only acknowledged after replication to a standby in the opposite zone.

However, this guarantee is only real if the cell's pods actually run in the correct zone. If the Kubernetes scheduler places `zone-1` pool pods on `us-east-1b` nodes, the cross-cell quorum is meaningless — both "cells" are on the same physical infrastructure.

## Zone ID/Region Scheduling Implementation

The operator natively integrates Cell topology with Kubernetes node scheduling via the `zoneId` and `region` fields on the `Cell` and `CellConfig` definitions.

### 1. Optional Topology Constraint

The `zoneId` and `region` fields are optional and mutually exclusive (validated via CEL: `!(has(self.zoneId) && has(self.region))`). At least one of the two must be specified.

- `zoneId` set → injects `nodeSelector: { topology.k8s.aws/zone-id: <zoneId> }`
- `region` set → injects `nodeSelector: { topology.kubernetes.io/region: <region> }`

`zoneId` uses the AWS availability zone ID (e.g. `use1-az1`) exposed via the `topology.k8s.aws/zone-id` node label set by the [AWS Node Labeler](https://github.com/aws/aws-node-labeler). Unlike zone names (e.g. `us-east-1a`), zone IDs are consistent across AWS accounts, making them the correct identifier for cross-account or shared-infrastructure deployments.

### 2. Auto-injection of `nodeSelector`

The injected `nodeSelector` is **strict** and **additive**:
- **Strict:** Pods will remain `Pending` if no matching nodes exist, preserving the failure domain guarantee. Silently scheduling to the wrong zone is strictly avoided.
- **Additive:** `nodeSelector` does not overwrite the user-provided `Affinity` rules (e.g. from `PoolSpec.Affinity` or `CellInlineSpec.MultiGateway.Affinity`). The Kubernetes scheduler applies both constraints, enabling users to layer intra-zone anti-affinity or topology spread constraints.

The components receiving this injection are:
| Component | Injection Logic |
|---|---|
| **MultiGateway Deployment** | Determined directly from the Cell CR `Spec.ZoneID` / `Spec.Region` |
| **Pool Pods** | Looked up via `CellTopologyLabels` in `ShardSpec` |
| **MultiOrch Deployment** | Looked up via `CellTopologyLabels` in `ShardSpec` |

*(Note: PVCs for backups don't need explicit injection, as Kubernetes binds dynamically provisioned PersistentVolumes based on the pod's scheduling decision through topology-aware volume provisioning).*

### 3. Spec Propagation (CellTopologyLabels)

To avoid independent API reads, the cluster controller builds a `CellTopologyLabels map[CellName]map[string]string` from the user's `MultigresCluster` cell configs. This map propagates down through the `TableGroupSpec` and finally the `ShardSpec`. The shard controller uses this map to instantaneously inject the appropriate `nodeSelector` for any given cell without additional API roundtrips.

### 4. Pre-flight Validation Warnings

The operator's validating webhook (`ValidateClusterLogic` in `resolver.go`) checks the live cluster for nodes matching the specified topology labels. Specifically, it lists all `corev1.Node` objects at admission time.
If a cell specifies a `zoneId` or `region` that does not exist on any current nodes, the webhook emits a Kubernetes Admission **Warning**:

`cell 'X': no nodes currently match topology.k8s.aws/zone-id=Y; pods will be Pending until matching nodes are available`

We use warnings rather than rejections because cluster nodes are ephemeral (e.g., Karpenter or Cluster Autoscaler may spin up a new AZ from zero upon spotting Pending pods).

### 5. User-Provided Affinity Remains Available

The existing `Affinity` field on `StatelessSpec` (multigateway) and `PoolSpec` (pools) is preserved and works additively with the auto-injected `nodeSelector`. Users can use it for:

- **Pod anti-affinity** — prevent multiple pool replicas from landing on the same node within a zone.
- **Topology spread constraints** — distribute pods evenly across nodes within a zone.
- **Additional scheduling preferences** — soft preferences for instance types, etc.
