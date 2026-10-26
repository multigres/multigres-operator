---
name: exercise_cluster
description: Deploy MultigresCluster fixtures, run mutation scenarios, and validate health using the observer. Finds bugs in the operator and upstream multigres by exercising real cluster operations and verifying end-to-end health beyond CRD phase status. Use this skill whenever the user wants to test the operator, exercise the cluster, run exerciser scenarios, validate cluster health after changes, find bugs through mutation testing, or deploy and mutate fixtures.
---

# Exercise Cluster Skill

**Goal:** Find bugs in the multigres operator and upstream multigres by deploying real MultigresCluster configurations, mutating them through operator-driven workflows, and using the observer to verify true end-to-end cluster health.

**Core principles:**
- The **observer is the single source of truth** for cluster health. CRD phase `Healthy` is necessary but NOT sufficient — it misses broken replication, connection failures, and multi-primary states.
- **You drive kubectl directly.** Read the live CR, understand its structure, construct correct patches. Fixtures have different structures (`.spec.pools` vs `.overrides.pools`).
- **Every post-grace-period error is potentially a real bug.** NEVER dismiss errors — operator logs, RBAC warnings, webhook warnings, kubectl output. Investigate everything, report everything, including transient findings that resolved.

## Phase 0: Cluster Setup

Verify the kind cluster and observer are running:
```bash
KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl cluster-info
KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl get pods -l app.kubernetes.io/name=multigres-observer -n multigres-operator
```
If cluster is down: `make kind-deploy`. If only observer is missing: `make kind-deploy-observer`.

Define the observer helper:
```bash
observer() {
  KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl exec -n multigres-operator deploy/multigres-observer -- curl -sf "http://localhost:9090$1"
}
```
Verify: `observer /api/status | jq '.summary'`

## Phase 1: Deploy Fixture & Baseline

Pick a fixture from **Fixture Selection** below. For topology fixtures, read `references/topology-awareness.md` first.

1. Deploy prerequisites if they exist, wait for pods to be Running:
   ```bash
   KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl apply -f fixtures/<fixture>/prerequisites.yaml
   ```
2. Deploy the cluster. **Read the kubectl output** — webhook warnings mean real problems. Stop and fix before proceeding.
   ```bash
   KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl apply -f fixtures/<fixture>/cluster.yaml
   ```
3. Run the **Stability Verification Protocol** (below) with tier `lifecycle`. Baseline must be fully clean — any error is a bug.
4. For template/override fixtures: run `references/template-verification.md`.

## Phase 2: Mutation Testing

Consult `references/scenarios/index.md` for the full scenario catalog. For each scenario:

1. Read the live CR: `KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl get multigrescluster <name> -n <ns> -o yaml`
2. Save pre-mutation state for teardown.
3. Construct and apply the correct patch based on the actual CR.
4. Verify using the appropriate protocol:
   - Fast-path eligible → `references/fast-path-verification.md`
   - Concurrent mutations → `references/concurrent-mutations.md`
   - Negative assertions → `references/negative-assertions.md`
   - All others → **Stability Verification Protocol** below
5. Log results, teardown if applicable, verify stability again. Proceed only after confirmed stable.

## Stability Verification Protocol

Run after EVERY cluster change: deploy, mutation, teardown.

### Tiers

| Tier | When | CRD Timeout | Min Observation |
|------|------|-------------|-----------------|
| `quick` | Config-only (annotations, PVC policy) | 3 min | 60s |
| `standard` | Scale, resources, images | 5 min | 60s |
| `lifecycle` | Deploy, delete-recreate, template switches | 10 min | 90s |

### Step 1 — CRD Phase Gate

Poll `.status.phase` every 5s until `Healthy`:
```bash
KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl get multigrescluster <name> -n <ns> -o jsonpath='{.status.phase}'
```
If `Degraded`/`Failed` persists >2 min or timeout reached → STOP, investigate.

### Step 2 — Grace Period

The observer suppresses pool pod errors for 2 min after creation. Wait until ALL pool pods are at least **150s old**:
```bash
KUBECONFIG=$(pwd)/kubeconfig.yaml kubectl get pods -n <ns> -l app.kubernetes.io/component=shard-pool \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.creationTimestamp}{"\n"}{end}'
```

### Step 3 — Stability Observation

Poll every 10s, tracking: `consecutive_clean` (polls with 0 errors/fatals), `all_findings` (every error/fatal seen), `elapsed`.

```bash
observer /api/status | jq '{
  summary: .summary,
  errors: [(.findings // [])[] | select(.level == "error" or .level == "fatal")],
  warns: [(.findings // [])[] | select(.level == "warn")]
}'
```

**Exit conditions:**
| Condition | Action |
|-----------|--------|
| `consecutive_clean >= 3` AND `elapsed >= min_observation` | **STABLE** |
| Error persists > 3 min | **UNSTABLE** — investigate |
| `elapsed >= 5 min` without stability | **TIMEOUT** — investigate |

A finding "persists" when the same check+component appears in consecutive polls. Use `/api/history` to classify.

### Step 4 — Result Classification

- **STABLE (clean)**: No findings during observation.
- **STABLE (transients observed)**: Errors appeared then resolved. List each with check, component, message, duration. Report these — they may indicate intermittent bugs.
- **UNSTABLE**: Persistent errors. Investigate via `references/deep-investigation.md`.

### Step 5 — Post-Stability Checks

Run all three after stability is confirmed:

**Warn review:**
```bash
observer /api/status | jq '[(.findings // [])[] | select(.level == "warn")]'
```
Note persistent warnings (replication lag, WAL replay paused).

**Primary verification** — catches stale podRoles bugs:
```bash
# Exactly 1 PRIMARY per pool per cell in CRD
kubectl get shard -n <ns> -o json | jq '[.items[].status.podRoles | to_entries[] | select(.value == "PRIMARY")] | length'

# Cross-reference with actual PG state (psql over TCP needs the password; read it from the mounted file)
for pod in $(kubectl get pods -n <ns> -l app.kubernetes.io/component=shard-pool -o name); do
  echo -n "$pod: "
  kubectl exec -n <ns> $pod -c postgres -- sh -c 'PGPASSWORD=$(cat "$POSTGRES_PASSWORD_FILE") psql -h 127.0.0.1 -p 5432 -U postgres -tAc "SELECT CASE WHEN pg_is_in_recovery() THEN '"'"'REPLICA'"'"' ELSE '"'"'PRIMARY'"'"' END"'
done
```
CRD PRIMARY must match SQL `pg_is_in_recovery() = false`. Mismatch → trigger reconcile, report as "error".

**Observer history assertion:**
```bash
observer /api/history | jq '{persistent: .persistent, flapping: .flapping, transientCount: (.transient | length)}'
```
Assert `persistent == []` and `flapping == []`. Investigate before proceeding if not.

## Execution Modes

| Mode | What it does | When to use |
|------|-------------|-------------|
| **smoke** | Deploy → baseline verification. Template fixtures include TVP. | Quick sanity check |
| **core** | smoke + scale-up, scale-down, update-resources, delete-pool-pod | Standard coverage |
| **full** | All applicable scenarios including concurrent, webhooks, negatives | Thorough testing |

Default to **core** when unspecified.

## Fixture Selection

| Fixture | Kind-Ready | TVP | Tests |
|---------|-----------|-----|-------|
| `minimal-retain` | Yes | — | Core logic, PVC retention |
| `minimal-delete` | Yes | — | PVC deletion paths |
| `templated-full` | Yes (prereqs) | Full | Template resolution |
| `overrides-complex` | Yes (prereqs) | Override | Override merging |
| `external-etcd-mixed` | Yes (prereqs) | — | External topology server |
| `s3-backup` | Needs real S3 | — | Backup with S3 |
| `multi-cell-quorum` | Yes (heavy) | — | Multi-cell, quorum |
| `postgres-config-ref` | Yes (prereqs) | — | ConfigMap-based postgresql.conf, rolling update on content change |
| `external-adminweb` | Yes | — | External admin web IPs, annotations, status |
| `multi-cell-topology` | `kind-deploy-topology` | — | Zone-aware scheduling |
| `observability-custom` | Yes (prereqs) | — | Custom observability |

Prerequisites are self-contained (except `s3-backup`). Deploy prereqs first, wait for pods Running.

**Recommended order:** `minimal-retain` → `minimal-delete` → `templated-full` → `overrides-complex` → `external-etcd-mixed` → `multi-cell-topology`

## Reporting

Create report at `agent-docs/exerciser/exercise-run-<YYYY-MM-DD-HHMMSS>.md`:
- Environment (cluster, observer, operator image, multigres images)
- Per-fixture: baseline, each scenario (mutation, stability result, all findings, teardown)
- Summary: fixtures tested, scenarios run, bugs found, transients observed

## Reference Documents

| Reference | When to Read |
|-----------|-------------|
| `references/scenarios/index.md` | Before mutations — master scenario lookup with files, tiers, fixtures |
| `references/scenarios/core.md` | Core mode scenarios (scale, resources, delete-pod) |
| `references/scenarios/*.md` | Load specific scenario files as directed by the index |
| `references/operator-knowledge.md` | When investigating bugs |
| `references/fast-path-verification.md` | For fast-path eligible scenarios |
| `references/template-verification.md` | After deploying template/override fixtures |
| `references/negative-assertions.md` | For deletion, rejection, cleanup scenarios |
| `references/deep-investigation.md` | When UNSTABLE |
| `references/concurrent-mutations.md` | Full mode concurrent testing |
| `references/topology-awareness.md` | Topology fixtures or topology warnings |
| `patches/` | Reusable mutation scripts |
