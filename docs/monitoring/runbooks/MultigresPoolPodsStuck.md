# MultigresPoolPodsStuck

## Meaning

Pool pods have been not Ready for more than 15 minutes while their cell has at least one Ready, schedulable node that matches the pod's node selector and required node affinity. The `multigres_operator_pool_pods_stuck` gauge is above 0 for the affected pool and cell.

The gauge stays 0 while the zone itself is down (no Ready node can run the pods), so this alert points at a problem that is not a zone outage. Taints are not considered, so a node the pod cannot tolerate still counts as able to run it.

If the operator cannot list nodes, the gauge keeps its last value instead of dropping to 0, and the operator retries about every minute. The series is removed while the shard is being deleted or drained (`PendingDeletion`), so the alert clears for those shards.

## Impact

The pool runs with fewer ready replicas than desired. Failover capacity and read capacity in the cell are reduced.

## Investigation

```bash
# Find the not-Ready pods in the pool and cell
kubectl get pods -n <namespace> -l multigres.com/shard=<shard-name>,multigres.com/pool=<pool-name>,multigres.com/cell=<cell-name> -o wide

# Scheduling events (taints, resources, volume node affinity)
kubectl describe pods -n <namespace> -l multigres.com/shard=<shard-name>,multigres.com/pool=<pool-name>,multigres.com/cell=<cell-name>

# Nodes in the cell's zone and their conditions
kubectl get nodes -l topology.kubernetes.io/zone=<zone> -L workload

# Taints on those nodes
kubectl get nodes -l topology.kubernetes.io/zone=<zone> \
  -o custom-columns=NAME:.metadata.name,TAINTS:.spec.taints
```

## Remediation

1. **Pending with taint or resource events** — Add capacity, remove the taint, or lower resource requests. The gauge ignores taints, so a tainted-only zone alerts here.
2. **Pending on a PVC bound to another node or zone** — Check the PVC's volume node affinity against the available nodes.
3. **Scheduled but not Ready** — Check container logs and readiness probes. See `MultigresShardPoolDegraded`.
4. **Zone recovered recently** — The gauge refreshes about every minute while the shard has not-Ready pods. Wait for the pods to schedule before escalating.
