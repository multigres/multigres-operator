# Durability Policy

The durability policy controls how multigres enforces synchronous replication acknowledgment during failover. It determines the quorum rules that multiorch applies when deciding whether a write is considered durable.

## Supported Policies

| Policy | Description |
|:---|:---|
| `AT_LEAST_2` | Any 2 nodes in the shard must acknowledge writes. This is the default and provides single-cell quorum — suitable for clusters where all pools reside in one availability zone. |
| `MULTI_CELL_AT_LEAST_2` | Any 2 nodes from **different cells** must acknowledge writes. This enforces cross-AZ quorum — a committed write survives the failure of an entire availability zone. Requires pools deployed in at least 2 cells. |

> [!NOTE]
> Upstream multigres plans to support additional user-defined policies in the future. The field accepts any string value to accommodate this.

## Configuration

### Cluster-wide default

Set `spec.durabilityPolicy` on the `MultigresCluster` to apply a default policy to all databases:

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: my-cluster
spec:
  durabilityPolicy: "AT_LEAST_2"
  cells:
    - name: zone-a
      zoneId: use1-az1
    - name: zone-b
      zoneId: use1-az2
  databases:
    - name: postgres
      default: true
```

If omitted, the webhook defaults the field to `"AT_LEAST_2"`.

### Per-database override

Override the cluster default for a specific database via `databases[].durabilityPolicy`:

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: multi-az-cluster
spec:
  durabilityPolicy: "AT_LEAST_2"  # default for all databases
  cells:
    - name: zone-a
      zoneId: use1-az1
    - name: zone-b
      zoneId: use1-az2
  databases:
    - name: postgres
      default: true
      durabilityPolicy: "MULTI_CELL_AT_LEAST_2"  # override for this database
```

In this example, the `postgres` database uses `MULTI_CELL_AT_LEAST_2` (cross-AZ quorum), while any future databases would inherit the cluster default of `AT_LEAST_2`.

## How It Works

1. The webhook resolver materializes the default (`"AT_LEAST_2"`) on `CREATE` if the field is empty.
2. The cluster-handler merges the database-level override with the cluster default (database wins if set).
3. The resolved value propagates through `TableGroup` → `Shard` child resources.
4. The data-handler writes the policy to the database entry in the global topology (etcd).
5. Multiorch reads the policy from the topology during shard bootstrap and configures synchronous replication accordingly.

## Choosing a Policy

| Scenario | Recommended Policy |
|:---|:---|
| Single availability zone | `AT_LEAST_2` |
| Multi-AZ with cross-zone durability requirement | `MULTI_CELL_AT_LEAST_2` |
| Development / testing | `AT_LEAST_2` |

When using `MULTI_CELL_AT_LEAST_2`, ensure your shard's pools span at least 2 cells and that each cell maps to a distinct availability zone via `spec.cells[].zoneId`. See [Cell Topology](development/cell-topology.md) for details on how cells map to Kubernetes scheduling constraints.

## Replica Count Considerations

The `AT_LEAST_2` policy requires at least 3 replicas per pool (1 primary + 2 standbys) for zero-downtime rolling upgrades, since quorum must be maintained while one node is being drained. The CRD enforces a minimum of 1 replica, but pools with fewer than 3 replicas will lose quorum during drain operations.
