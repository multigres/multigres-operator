# Controller Patterns

## Operator Performance Tuning

### High Concurrency & Throughput
To handle large-scale clusters with thousands of shards, we have tuned the operator for high concurrency.

**1. Increased API Client Limits**
The default client-go rate limits (QPS: 5, Burst: 10) are insufficient for an operator managing thousands of resources. We have increased these limits globally:
- **QPS:** 50
- **Burst:** 100

This prevents the operator from self-throttling when multiple workers are requesting resources simultaneously.

**2. Parallel Reconciliation (20 Workers)**
By default, the controller runtime uses a single worker per controller. This means if one Shard is slow to reconcile (e.g. waiting for a slow API call), all other Shards are blocked.

We have enabled **20 Parallel Workers** (`MaxConcurrentReconciles: 20`) for all major controllers:
- `MultigresCluster`
- `Shard`
- `Cell`
- `TableGroup`
- `TopoServer`

**Advantages:**
- **Throughput:** Can process 20 resources simultaneously.
- **Resilience:** A stalled reconciliation does not block the entire queue.
- **Speed:** Faster convergence during initial startup or massive updates.

**Downsides:**
- **Load:** Increases load on the Kubernetes API Server (mitigated by our [Caching Strategy](caching-strategy.md)).
- **Complexity:** Requires careful handling of shared resources (though our controllers are designed to be stateless/independent).

---

## Event Filtering & Idempotency

### GenerationChangedPredicate

We apply the `GenerationChangedPredicate` to the **Primary Resource** (the `For` object) in all controllers. This ensures the controller does **not** reconcile when only the `Status` subresource changes, preventing infinite loops where a controller updates status, triggers a new reconcile, updates status again, and so on.

**When to use it:**
Use this on the *primary* resource being reconciled to break self-recursion loops.

**Code Example:**
```go
func (r *MultigresClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        // FILTER: Only reconcile if Spec or Metadata changes. Ignore Status changes.
        For(&multigresv1alpha1.MultigresCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
        // ...
        Complete(r)
}
```

**Potential Issues:**
If the controller logic *depended* on reading its own status to make decisions (e.g. "If status is 'Initializing', do X"), using this predicate would break that logic because the controller wouldn't wake up after setting 'Initializing'.

> [!CAUTION]
> If you ever need a controller to make decisions based on its own `.status` fields, you **must remove** this predicate from that controller's `For` clause. Otherwise the controller will never re-reconcile after writing its own status, effectively deadlocking. This trade-off is acceptable only when status is purely observational output.

*   *Current design:* Our controllers are level-triggered based on `Spec` and child resources. We do not use the primary resource's `Status` as a state machine driver; `Status` is purely an output observation.

**Child Resources (Why we don't use it there):**
We do *not* apply this predicate to child resources (`Owns`). We must react to child resource status transitions (e.g., a Deployment becoming Ready, a Pod changing state) to update the parent's status. Each parent only monitors the status of its *immediate* children (e.g. `MultigresCluster` watches `Cell`, and `Shard` watches `Pod` and `Deployment` directly).

### Server-Side Apply (SSA) & Idempotency

We deliberately omit client-side `reflect.DeepEqual` checks before patching status. We rely on the Kubernetes API Server's **Server-Side Apply (SSA)** logic to detect no-ops.

**No-Op Logic:**
If the status patch matches the existing state, the API Server will treat it as a no-op, generating no events and no resource version updates.

**Code Example:**
```go
// SSA Pattern: blindly patch the status.
// The API Server calculates the diff. If no change, it returns 200 OK with no-op.
if err := r.Status().Patch(
    ctx,
    &multigresv1alpha1.MultigresCluster{
        ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
        Status:     newStatus,
    },
    client.Apply,
    client.ForceOwnership,
); err != nil {
    return err
}
```

**Alternative (Client-Side Check):**
If network bandwidth becomes a bottleneck, we *could* implement a client-side check, but it requires deep-copying and careful handling of pointers:
```go
// NOT RECOMMENDED unless profiling shows network IO is a bottleneck.
// Requires deep equality check which is expensive on CPU and hard to maintain.
if !reflect.DeepEqual(cluster.Status, newStatus) {
    r.Status().Patch(...)
}
```
**Decision:** We prefer the cleaner code of SSA and rely on the API Server's optimized diffing engine until proven otherwise.

> [!NOTE]
> The field owner is not passed per call: each controller's client carries its own manager name (`client.WithFieldOwner` in `SetupWithManagerReconciler`). Which manager owns which status field, and how the suite enforces it, is in [Status Ownership](status-ownership.md).
