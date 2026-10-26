# Log Tracing and kubectl Cross-Reference

## Cross-Referencing Observer Findings with kubectl

After identifying findings from the observer, verify with direct kubectl:

```bash
# Check pod status
KUBECONFIG=kubeconfig.yaml kubectl get pods -A | grep -v kube-system

# Check shard status
KUBECONFIG=kubeconfig.yaml kubectl get shards -A -o wide

# Check CRD conditions
KUBECONFIG=kubeconfig.yaml kubectl get multigresclusters -A -o yaml | grep -A5 conditions

# Check drain annotations
KUBECONFIG=kubeconfig.yaml kubectl get pods -l app.kubernetes.io/component=shard-pool \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.drain\.multigres\.com/state}{"\n"}{end}'

# Cross-reference CRD podRoles with actual SQL state
# (catches stale podRoles where CRD says REPLICA but pod is actually PRIMARY)
KUBECONFIG=kubeconfig.yaml kubectl get shard -A -o json | jq '[.items[] | {name: .metadata.name, podRoles: .status.podRoles}]'
# psql over TCP needs the password; read it from the file mounted in the pod
for pod in $(KUBECONFIG=kubeconfig.yaml kubectl get pods -l app.kubernetes.io/component=shard-pool -o name); do
  echo -n "$pod: "
  KUBECONFIG=kubeconfig.yaml kubectl exec $pod -c postgres -- sh -c 'PGPASSWORD=$(cat "$POSTGRES_PASSWORD_FILE") psql -h 127.0.0.1 -p 5432 -U postgres -tAc \
    "SELECT CASE WHEN pg_is_in_recovery() THEN '"'"'REPLICA'"'"' ELSE '"'"'PRIMARY'"'"' END"'
done

# Check finding history for patterns
observer /api/history | jq '{persistent: (.persistent // []) | length, flapping: (.flapping // []) | length, transient: (.transient // []) | length}'
```

## Tracing Failures Through Component Logs

The observer detects symptoms — the root cause is in the component logs. Trace the actual failure through the call chain before looking at code.

```bash
# Gateway: trace connection lifecycle, check routing decisions
KUBECONFIG=kubeconfig.yaml kubectl logs -l app.kubernetes.io/component=multigateway --tail=200 \
  | grep -E 'error|failed|timeout|cancel'

# MultiOrch: check if it can reach poolers (gRPC Status RPCs)
KUBECONFIG=kubeconfig.yaml kubectl logs -l app.kubernetes.io/component=multiorch --tail=200 \
  | grep -v 'capacity' | grep -E 'error|warn|failed|poll|dead|quorum' -i

# Pooler: check gRPC server health, backend connections
KUBECONFIG=kubeconfig.yaml kubectl logs <pod-name> -c multipooler --tail=200 \
  | grep -E 'error|failed|grpc|backend' -i

# Postgres: check for replication issues, connection limits
KUBECONFIG=kubeconfig.yaml kubectl logs <pod-name> -c postgres --tail=200 \
  | grep -E 'ERROR|FATAL|LOG.*replication'
```

## Key Log Patterns

| Pattern | Meaning |
|---------|---------|
| Gateway SQL timeout + multiorch "pooler poll failed" | Multiorch can't reach poolers → gateway can't route queries |
| `DeadlineExceeded` on gRPC Status RPCs | Pooler gRPC server is hanging or unreachable (check port 15270, check version mismatches) |
| `PrimaryIsDead` + "insufficient poolers for quorum" | Multiorch thinks primary is dead because it can't poll it, and can't failover without quorum |
| `method not implemented` on gRPC calls | Version mismatch between multiorch and multipooler binaries |
