# PVC Deletion Policy

## Overview

The operator supports fine-grained control over PVC lifecycle through the `PVCDeletionPolicy` type, which is embedded at multiple levels of the resource hierarchy. For pool pods, PVC deletion is handled directly by the operator's reconcile loop. For TopoServer StatefulSets, it maps to Kubernetes's `persistentVolumeClaimRetentionPolicy`.

## API Design

**Type Definition** (`api/v1alpha1/common_types.go`):
```go
type PVCDeletionPolicy struct {
    WhenDeleted PVCRetentionPolicyType `json:"whenDeleted,omitempty"`
    WhenScaled  PVCRetentionPolicyType `json:"whenScaled,omitempty"`
}

type PVCRetentionPolicyType string
const (
    RetainPVCRetentionPolicy PVCRetentionPolicyType = "Retain"
    DeletePVCRetentionPolicy PVCRetentionPolicyType = "Delete"
)
```

**Fields**:
- `WhenDeleted`: Controls PVC deletion when the parent resource (Cluster, TopoServer, Shard) is deleted
- `WhenScaled`: Controls PVC deletion when replicas are reduced (scale-down)

**Default Values**: `WhenDeleted` defaults to `Retain`, `WhenScaled` defaults to `Delete`. This is enforced via:
1. CRD-level defaults: `+kubebuilder:default=Retain` (WhenDeleted), `+kubebuilder:default=Delete` (WhenScaled)
2. Webhook defaulting: Ensures nil policies are populated
3. Operator-level defaults in `pkg/resolver`

## Hierarchical Merging

The policy propagates through the resource hierarchy with child values taking precedence:

**Hierarchy Path**:
```
MultigresCluster.Spec.PVCDeletionPolicy
  ↓ (merged with)
TableGroup.Spec.PVCDeletionPolicy
  ↓ (merged with)
Shard.Spec.PVCDeletionPolicy  (inline spec)
  → Resolved and stored in TableGroup.Spec.Shards[].PVCDeletionPolicy
```

**Template Overrides**:
- `CoreTemplate.Spec.GlobalTopoServer.PVCDeletionPolicy` → applied to cluster's GlobalTopoServer
- `ShardTemplate.Spec.PVCDeletionPolicy` → applied to shards using that template

**Merge Function** (`api/v1alpha1/common_types.go:MergePVCDeletionPolicy`):
- Field-level inheritance: If child specifies only `whenDeleted`, it inherits parent's `whenScaled`
- Nil handling: If both child and parent are nil, returns nil (caller falls back to operator defaults)
- Empty struct handling: If merged result has both fields empty, returns nil

## Implementation Points

### 1. Webhook Defaulter (`pkg/webhook/handlers/defaulter.go`)
- Resolves shard templates and captures the `PVCDeletionPolicy` from resolver
- Previously ignored the third return value from `ResolveShard()`, causing "invisible defaults"
- Fix: Now properly assigns `resolvedPvcPolicy` to `shard.Spec.PVCDeletionPolicy`

### 2. Resolver (`pkg/resolver/shard.go`)
- `ResolveShard()` returns `(multiOrch, pools, pvcPolicy, error)`
- Merges policies in precedence order: inline spec → shard template → cluster defaults
- Returns nil if no policy specified at any level (signals use of operator defaults)

### 3. Resource Handlers

**TopoServer StatefulSets** (`pkg/resource-handler/controller/toposerver/statefulset.go`):
```go
sts.Spec.PersistentVolumeClaimRetentionPolicy = pvc.BuildRetentionPolicy(
    topo.Spec.PVCDeletionPolicy,
)
```

**Shard Pool Pods** (`pkg/resource-handler/controller/shard/reconcile_pool_pods.go`):
Pool pod PVCs are managed directly by the operator. During scale-down, the `cleanupDrainedPod` function checks `shard.Spec.PVCDeletionPolicy.WhenScaled` and, for a scaled-down pod (index >= replicas) under the `Delete` policy (the default), orphans the data PVC for garbage collection; a rolling-update pod (index < replicas) keeps its PVC. During shard/cluster deletion, PVCs are garbage-collected by Kubernetes via conditional owner references: when `WhenDeleted` is `Delete`, PVCs are created with an ownerRef to the Shard CR, enabling cascade deletion. When `WhenDeleted` is `Retain`, PVCs have no ownerRef and persist after deletion. The `reconcilePVCOwnerRefs` function ensures existing PVCs stay in sync with the current policy during mid-lifecycle changes.

**Utility Function** (`pkg/util/pvc/retention.go:BuildRetentionPolicy`):
- Used by TopoServer StatefulSets to convert operator's `PVCDeletionPolicy` to Kubernetes `StatefulSetPersistentVolumeClaimRetentionPolicy`
- For pool pods, the operator checks the policy directly in the reconcile loop
- Handles nil input by returning safe `Retain/Retain` default
- Maps enum values: `Delete` → `Delete`, anything else → `Retain`

## Critical Caveats for Future Developers

### 1. TopoServer StatefulSet Spec Changes Trigger Recreation

**Issue**: Modifying `spec.persistentVolumeClaimRetentionPolicy` on an existing TopoServer StatefulSet requires recreation (not supported by in-place updates). This caveat does not apply to pool pods, which the operator manages directly.

**Decision**: Deferred to future implementation. Current behavior (requiring manual intervention for TopoServer) is acceptable for v1alpha1.

### 2. No Validation of Policy vs. Cluster Intent

**Issue**: The operator does **not** prevent users from setting `whenDeleted: Delete` on production clusters.

**Rationale**: We cannot reliably determine user intent (dev vs. prod). Cluster labels or annotations are not standardized.

**Mitigation**:
- Clear documentation in README with warnings
- Recommend using CI/CD policy enforcement (e.g., OPA, Kyverno) to prevent `Delete` in production namespaces
- Consider adding a validating webhook warning (not error) in future versions

### 3. Immediate Effect on Scale-Down

**Behavior**: When `whenScaled: Delete` is set, scaling down **immediately deletes PVCs** (within seconds of pod termination).

**Risk**: Users may expect a grace period or confirmation step.

**Mitigation**:
- Document this behavior prominently
- Recommend backups before scale-down operations
- Future enhancement: Add a `gracePeriodSeconds` field to delay PVC deletion

### 4. Template Changes Don't Auto-Update Existing Shards

**Issue**: If a user changes `ShardTemplate.Spec.PVCDeletionPolicy` after shards are created, existing shards do **not** automatically update.

**Why**: The resolved policy is "frozen" in `TableGroup.Spec.Shards[].PVCDeletionPolicy` at creation time.

**Workaround**: Users must:
1. Edit the TableGroup spec directly (not recommended - it's a child resource), OR
2. Delete and recreate the cluster

**Future Enhancement**: Implement a "template sync" reconciliation loop that detects template changes and propagates them to resolved specs.

### 5. Nil vs. Empty Struct Semantics

**Critical**: The `MergePVCDeletionPolicy` function returns:
- `nil`: No policy specified at this level or above → use operator defaults
- `&PVCDeletionPolicy{WhenDeleted: "", WhenScaled: ""}`: **Also treated as nil**
- `&PVCDeletionPolicy{WhenDeleted: "Retain", WhenScaled: ""}`: Use `Retain` for deletion, inherit scaled from parent

**Implication**: Controllers must check `if policy == nil` and apply defaults, not just check `if policy.WhenDeleted == ""`.

**Code Pattern**:
```go
finalPolicy := resolvedPolicy
if finalPolicy == nil {
    finalPolicy = &PVCDeletionPolicy{
        WhenDeleted: RetainPVCRetentionPolicy,
        WhenScaled:  DeletePVCRetentionPolicy,
    }
}
```

This is already handled correctly in `pkg/util/pvc/retention.go`.

## Testing Coverage

**Integration Tests**:
- `pkg/cluster-handler/controller/multigrescluster/integration_test.go`: Verifies policy propagation through hierarchy
- `pkg/cluster-handler/controller/multigrescluster/integration_resolution_enforcement_test.go`: Verifies template overrides work correctly
- `pkg/webhook/handlers/defaulter_test.go`: (Should be added) Unit tests for webhook defaulting logic

**Unit Tests**:
- `api/v1alpha1/common_types_test.go`: (Should be added) Tests for `MergePVCDeletionPolicy` function
- `pkg/util/pvc/retention_test.go`: (Should be added) Tests for `BuildRetentionPolicy` conversion

**Notable Test Case**: The "invisible defaults" bug was caught because tests expected `nil` but got `Retain/Retain` after the fix. This validates that the fix is working correctly.
