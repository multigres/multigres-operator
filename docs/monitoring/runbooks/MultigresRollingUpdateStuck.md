# MultigresRollingUpdateStuck

## Meaning

A pool's rolling update has been in progress for more than 30 minutes. The `multigres_operator_rolling_update_in_progress` gauge has been 1 for the affected pool continuously.

The alert's `namespace` label is the operator's namespace; the Shard and its pods live in the project namespace. The `pool` label names the pool being updated, which may not be the pool that is blocking the rollout.

## Impact

The pool is running a mix of old and new pod specs. Depending on the change, this may cause inconsistent behavior across replicas. New pods may be stuck in Pending or CrashLoopBackOff, blocking the rollout from completing.

## Investigation

```bash
# Find the Shard's namespace
kubectl get shard -A | grep <shard-name>

# Check the Shard status and why the operator cannot remove the next pod
kubectl describe shard <shard-name> -n <namespace>
kubectl get events -n <namespace> --field-selector involvedObject.name=<shard-name> | grep DisruptionBlocked

# List pool pods with their spec-hash (annotation) and drain state
kubectl get pods -n <namespace> -l app.kubernetes.io/component=shard-pool \
  -L multigres.com/pool -L multigres.com/cell
kubectl get pods -n <namespace> -l app.kubernetes.io/component=shard-pool \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.multigres\.com/spec-hash}{"\t"}{.metadata.annotations.drain\.multigres\.com/state}{"\n"}{end}'

# Check pod events for scheduling or image pull issues
kubectl describe pods -n <namespace> -l app.kubernetes.io/component=shard-pool,multigres.com/pool=<pool-name>

# If all pods are Ready, check each replica's Postgres log for paused WAL replay
kubectl logs -n <namespace> <pod> -c postgres | grep -E "lower setting than on the primary|recovery has paused"
```

If you have database access, `select pg_get_wal_replay_pause_state(), pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();` on the replica confirms it: state `paused`, receive LSN advancing, replay LSN frozen.

The `RollingUpdate` condition names the pool being updated. If the `DisruptionBlocked` event says the remaining cohort cannot satisfy durability while every pod is Ready, a replica is excluded from the cohort; check it for paused replay (case 5). To find who changed the spec, read `metadata.managedFields` on the MultigresCluster and your API server audit log.

## Remediation

1. **Pod stuck Pending** — Check for insufficient resources or node affinity mismatches. Scale the node pool or adjust resource requests.
2. **Pod stuck CrashLoopBackOff** — Check container logs. The new spec may have a configuration error. Consider reverting the template change.
3. **Drain stuck** — If a pod is stuck in a drain state, check topology server connectivity. See `MultigresDrainTimeout`.
4. **Health gate blocking** — Rolling updates are deferred when the pool is already degraded. Resolve the degraded state first (see `MultigresShardPoolDegraded`).
5. **Replica paused on hot-standby parameter check** — PostgreSQL requires `max_connections`, `max_wal_senders`, `max_worker_processes`, `max_prepared_transactions` and `max_locks_per_transaction` on a standby to be >= the primary's. The operator rolls replicas first, so lowering one of these (including by dropping an inline `postgresConfig` key so the derived default applies) pauses replay on the restarted replicas. The pod stays Ready but is excluded from the durability cohort, so the rollout cannot continue. Set the parameter back to >= the primary's value in the MultigresCluster (add the key to the existing `postgresConfig` map, do not replace the map), wait for the `<shard-name>-postgres-config` ConfigMap to show it, then delete the paused replica pod. Do not delete the pod first; the new pod loads the same config. Do not run `pg_wal_replay_resume()`; the standby shuts down. If a decrease is intended it must be applied primary-first, which the operator cannot do today. If the primary is also lost in this state, see `MultigresClusterDegraded`.
