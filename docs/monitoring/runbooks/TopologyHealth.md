# Topology health and failover protection

These alerts distinguish topology and failover failures from SQL availability.
An existing primary may continue serving queries while topology access or
multiorch is unavailable. `Available=True` alone does not confirm that a new
primary can be elected.

## Conditions and probes

The operator checks each managed etcd member every 30 seconds with a status RPC
and a read-only, linearizable Get. The probes run independently of resource
reconciliation and defragmentation, with a five-second deadline. A successful
linearizable read confirms quorum even when the operator cannot reach every
member. Status RPCs alone do not confirm quorum. A status error from any member
sets `QuorumAvailable=False` with reason `EtcdStatusError`, even when reads succeed.
For example, a NOSPACE alarm allows reads but blocks the topology writes required
during failover. The condition message identifies each member reporting an error.

`TopoServer.status.conditions[QuorumAvailable]` reports the result.
`TopologyUnreachable` means no member answered the operator; it does not prove
that the members have lost quorum among themselves. `QuorumUnavailable` means
members answered status requests but none completed the read. `ClusterMismatch`
means the endpoints reported different etcd cluster IDs. Client setup
failures, including invalid TLS credentials, produce `Unknown` with reason
`ProbeFailed`. `status.healthCheckedAt` records the latest probe completion.
The TopoServer `Ready` condition continues to report StatefulSet readiness.

On a MultigresCluster:

- `TopologyReady` reports the result of topology registration, including failures
  during the startup grace period.
- `TopologyQuorumAvailable` aggregates managed global and cell-local topology
  servers. Missing servers report false; observations older than two minutes or
  from an earlier generation report unknown.
- `FailoverReady` requires successful topology access, current managed quorum
  observations, and `OrchReady` for every desired shard at its current generation.
  A missing or unready shard reports false. Unknown observations prevent a true
  result. A previously initialized cluster with lost failover readiness is degraded.

External topology is not probed directly. Its quorum condition is unknown with
reason `ExternalTopology`; failover readiness uses topology registration and shard
orchestrator readiness. Monitor external etcd through its owner.

## Alert thresholds

| Alert | Threshold | Severity |
| --- | --- | --- |
| `MultigresTopologyQuorumUnavailable` | Failed quorum check, inconsistent cluster IDs, or etcd status errors for one minute | critical |
| `MultigresTopologyMemberUnavailable` | A member cannot complete reads for two minutes | warning |
| `MultigresTopologyHealthUnknown` | Unknown probe or observation over two minutes old, for one minute | warning |
| `MultigresTopologyBackendNearQuota` | Backend over 80% of quota for five minutes | warning |
| `MultigresTopologyMemoryPressure` | Working set over 80% of memory limit for one minute | warning |
| `MultigresTopologyMemberOOMKilled` | An observed OOM termination within the last five minutes | critical |
| `MultigresTopologyMemberRestarting` | At least three restarts in ten minutes | warning |
| `MultigresFailoverUnavailable` | FailoverReady false for one minute | critical |
| `MultigresFailoverHealthUnknown` | Unknown failover readiness or an observation over two minutes old, for two minutes | warning |

Alert delays are in addition to the probe, scrape, and evaluation intervals.
The pressure alerts can warn before resource exhaustion; a rapid memory spike
can reach the limit between scrapes.

## Investigate

Read the conditions and their messages, then inspect the affected members:

```bash
kubectl -n <namespace> get multigrescluster <cluster> -o yaml
kubectl -n <namespace> get toposervers -l multigres.com/cluster=<cluster> -o yaml
kubectl -n <namespace> get shards -l multigres.com/cluster=<cluster> -o yaml
kubectl -n <namespace> describe pod <member>
kubectl -n <namespace> logs <member> -c etcd --previous
```

For quorum or connectivity failures, check member logs, headless Service and DNS,
network reachability from the operator, and TLS Secrets. Check all members before
restarting one: taking down another voter can turn a single-member failure into
quorum loss. When topology is healthy but `FailoverReady` names an orchestrator,
inspect the affected shard's multiorch Deployments and pod readiness.

For backend pressure, compare total backend bytes with bytes in use. Review
compaction and defragmentation settings in [Topology maintenance](../../topology-maintenance.md).
If the condition reports NOSPACE, reclaim backend space before disarming the alarm
with `etcdctl alarm disarm`. Successful reads alone do not confirm recovery.
For memory pressure or OOMs, inspect working-set history and the running pod's
memory limit. Raising the backend quota does not increase available memory.

## Metrics and queries

Topology metrics use `cluster`, `name`, and `target_namespace`. Member metrics also
include `member` (the pod name). These labels avoid collisions with the operator
scrape target's own `namespace` and `pod` labels.

All names below begin with `multigres_operator_`:

| Suffix | Observation |
| --- | --- |
| `toposerver_quorum_available` | 1 true, 0 false, -1 unknown; includes `reason` |
| `toposerver_health_checked_timestamp_seconds` | Last completed probe |
| `toposerver_member_up` | Member completed a linearizable read; may remain 1 during a NOSPACE alarm |
| `toposerver_backend_bytes` | Total backend size |
| `toposerver_backend_in_use_bytes` | Backend bytes in use |
| `toposerver_backend_quota_bytes` | Quota configured on the running pod |
| `toposerver_revision` | Current MVCC revision |
| `toposerver_memory_limit_bytes` | Running pod's memory limit; zero means unlimited |
| `toposerver_member_restarts_total` | Observed container restart count, reset on pod replacement |
| `toposerver_member_oom_timestamp_seconds` | Latest OOM termination still recorded in pod status, or zero |
| `cluster_failover_ready` | 1 true, 0 false, -1 unknown; labels `cluster`, `target_namespace`, `reason` |
| `cluster_failover_checked_timestamp_seconds` | Last failover readiness observation |

Backend and revision series are removed when a member cannot be observed. They
are not carried forward as current measurements. Kubernetes retains only the
current and last container termination, so the OOM timestamp is not a complete
history of OOM events. Restart counts are exported as gauges because they come
from pod status; `increase` handles their resets.

Revision growth per second:

```promql
deriv(multigres_operator_toposerver_revision[5m])
```

Backend quota utilization:

```promql
multigres_operator_toposerver_backend_bytes
/ multigres_operator_toposerver_backend_quota_bytes
```

The memory alert requires kubelet/cAdvisor
`container_memory_working_set_bytes{container="etcd"}` with `namespace` and `pod`
labels. The local `deploy-observability` overlay configures this scrape using the
Prometheus service account. Other installations must enable their kubelet scrape.
A missing scrape or an unlimited container produces no memory-pressure alert;
check scrape health and configure limits when enabling this alert. The local
scrape accepts the kubelet's self-signed serving certificate; production scrape
configuration should use the cluster's trusted kubelet certificates.

To run the alert evaluation fixtures:

```bash
PROMTOOL=/path/to/promtool go test ./pkg/monitoring -run TestTopologyAlertRules
```
