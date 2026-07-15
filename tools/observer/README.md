# Multigres Observer

A read-only health validation tool for Multigres clusters. It runs inside the cluster alongside the operator and provides continuous monitoring across 11 check categories. The observer's core purpose is catching issues that are **invisible to both `kubectl` and the operator itself** — silent data-plane failures, replication anomalies, and state mismatches that no single component can detect on its own.

## Why This Exists

Multigres has many moving parts (gateway, orchestrator, pooler, postgres, etcd) and getting a full picture of cluster health requires checking each component individually. The observer consolidates all of these checks into a single loop and cross-references the results — for example, detecting when Kubernetes thinks a pod is Ready but the observer's own probes show it is broken, or when the operator reports Healthy but replication is actually degraded.

Neither `kubectl` nor the operator can reliably surface these issues:

- **`kubectl`** only shows pod-level status (Running, Ready, CrashLoopBackOff) — it cannot probe SQL replication, detect async standbys, or verify that gRPC health endpoints are actually serving.
- **The operator** focuses on driving the desired state and reporting its own phase. It trusts component health endpoints at face value. When those endpoints lie (e.g., multipooler returning `/ready=200` while its gRPC server hangs), the operator cannot detect the failure.

The observer independently verifies what each component self-reports. It is the tool of last resort for finding bugs that hide in the gaps between `kubectl`, the operator, and upstream multigres.

## Quick Start

### 1. Deploy the Observer

The observer deploys automatically alongside the operator:

```bash
make kind-deploy
```

This chains `kind-deploy-observer` at the end. To deploy the observer alone:

```bash
make kind-deploy-observer
```

### 2. Watch the Logs

```bash
KUBECONFIG=kubeconfig.yaml kubectl logs -f -l app.kubernetes.io/name=multigres-observer -n multigres-operator
```

Findings are structured JSON — one line per finding with severity, check name, component, and details. At the end of each 10-second cycle, a summary line reports total findings and error/fatal counts.

### 3. Query the Diagnostic API

The observer exposes structured JSON endpoints for diagnostics:

```bash
KUBECONFIG=kubeconfig.yaml kubectl port-forward svc/multigres-observer -n multigres-operator 9090:9090
```

**Latest cycle snapshot:**
```bash
curl -s http://localhost:9090/api/status | jq .
```

**Finding history** (persistent, transient, and flapping classification across cycles):
```bash
curl -s http://localhost:9090/api/history | jq .
```

**On-demand targeted check** (immediate, does not wait for next cycle):
```bash
curl -s 'http://localhost:9090/api/check?categories=pod-health,connectivity' | jq .
```

See [Architecture](docs/architecture.md) for the full JSON schemas and endpoint details.

### 4. Prometheus Metrics

Metrics are exposed at `:9090/metrics` inside the pod, integrating with the existing observability stack:

```bash
curl http://localhost:9090/metrics
```

## Using the Skills

The observer ships with two AI agent skills that turn its raw diagnostics into actionable workflows. These are the recommended way to interact with the observer — they encode hard-won investigation patterns and verification protocols that prevent the most common mistakes (dismissing real errors, skipping cross-checks, missing transient bugs).

### What Are Skills?

Skills are structured prompts that teach an AI agent how to use the observer for a specific task. Each skill defines step-by-step protocols, verification procedures, and investigation rules. When an agent loads a skill, it gains deep domain knowledge about Multigres architecture, operator internals, and the observer's check categories.

The two skills serve complementary purposes:

```
exercise_cluster (Proactive)          diagnose_with_observer (Reactive)
──────────────────────────            ──────────────────────────────────
"Find bugs before they               "A bug was found — what caused
 reach production"                     it and where is the fix?"

Deploy fixtures → Mutate →            Fetch /api/status → Triage
Verify stability → Report             findings → Trace logs →
                                       Investigate code → Report
```

### exercise_cluster — Proactive Bug Hunting

**Location:** [`skills/exercise_cluster/SKILL.md`](skills/exercise_cluster/SKILL.md)

This skill systematically exercises the operator through real cluster operations and uses the observer to catch bugs that only appear under mutation. It deploys MultigresCluster fixtures, runs mutation scenarios (scale up/down, rolling updates, pod deletions, config changes), and verifies end-to-end health after each operation.

**What it does:**

1. **Deploys fixtures** from `fixtures/` — real MultigresCluster configurations ranging from minimal single-shard setups to complex multi-cell, templated, and override configurations.

2. **Runs mutation scenarios** — a catalog of 40+ scenarios covering:
   - Scale operations (pool replicas, gateway replicas)
   - Configuration changes (resource limits, annotations, PVC policy)
   - Lifecycle events (pod deletion, cluster delete-recreate, cell/pool addition)
   - Template and override mutations
   - Rolling updates that trigger drain state transitions
   - Concurrent mutations (race condition testing)
   - Webhook rejection tests (storage shrink, immutable etcd replicas, invalid references)
   - Failure injection (etcd unavailability, image pull backoff)

3. **Verifies stability** using a multi-step protocol after every change:
   - CRD phase gate (waits for `Healthy`)
   - Grace period wait (observer suppresses pool pod noise for 2 minutes)
   - Stability observation (3 consecutive clean polls from the observer)
   - Primary verification (CRD podRoles matches actual SQL state)
   - Observer history assertion (no persistent or flapping findings)

4. **Classifies results** as STABLE (clean), STABLE (transients observed), or UNSTABLE, and produces a detailed exercise report.

**Key principle:** CRD phase `Healthy` is necessary but NOT sufficient. The observer probes replication state, gRPC health, SQL connectivity, and topology registration — issues that the operator's phase calculation cannot see.

**Example workflow:**
```bash
# Agent deploys a fixture and runs core scenarios
# (this happens automatically when you invoke the skill)

# 1. Deploy
kubectl apply -f fixtures/minimal-retain/cluster.yaml

# 2. Wait for healthy + grace period
# 3. Run observer stability check
curl -s http://localhost:9090/api/status | jq '.summary'

# 4. Mutate (e.g., scale up)
kubectl patch multigrescluster my-cluster --type=merge \
  -p '{"spec":{"shards":[{"pools":[{"replicasPerCell":4}]}]}}'

# 5. Re-verify stability
# 6. Report findings
```

**Reference files:**
- [`references/scenarios/index.md`](skills/exercise_cluster/references/scenarios/index.md) — full scenario catalog with mutation commands and success criteria
- [`references/operator-knowledge.md`](skills/exercise_cluster/references/operator-knowledge.md) — operator architecture, code paths for investigation, upstream vs operator decision tree

### diagnose_with_observer — Reactive Investigation

**Location:** [`skills/diagnose_with_observer/SKILL.md`](skills/diagnose_with_observer/SKILL.md)

This skill investigates when something is already broken. It fetches the observer's diagnostic snapshot, triages findings by severity, traces root causes through component logs and operator code, and determines whether the bug is in the operator or upstream multigres.

**What it does:**

1. **Fetches diagnostics** from `/api/status`, `/api/history`, and `/api/check`.

2. **Triages by severity:**
   - **Fatal** (address immediately): split-brain, blocked writes, backward drain transitions, silent data plane failures
   - **Error** (investigate): missing replicas, stuck drains, generation divergence, connectivity failures
   - **Warn** (monitor): replication lag, stale backups, topology unreachable

3. **Traces root causes** through the full component chain:
   - Gateway → Multiorch → Multipooler → PostgreSQL logs
   - CRD status → operator reconciliation → etcd topology
   - Kubernetes events → pod scheduling → resource constraints

4. **Investigates code** — determines whether the bug is in the operator (`pkg/resource-handler/`, `pkg/data-handler/`) or upstream multigres (`go/cmd/multigateway/`, `go/cmd/multipooler/`, etc.), including version checking against the running image SHA.

5. **Produces a bug report** with: finding summary, affected component, root cause with code path, version info, and suggested fix.

**Key principle:** Never blame infrastructure without proof. A 5-second timeout on `SELECT 1` is not "expected in kind" — trace the actual call chain to find where the request gets stuck.

**Quick triage queries:**
```bash
# Summary
curl -s http://localhost:9090/api/status | jq '.summary'

# Fatal and error findings only
curl -s http://localhost:9090/api/status | jq '[.findings[] | select(.level == "fatal" or .level == "error")]'

# Silent data plane failures (highest-signal finding)
curl -s http://localhost:9090/api/status | jq '[.findings[] | select(.message | test("reports Ready but"))]'

# Persistent findings (real issues, not transient noise)
curl -s http://localhost:9090/api/history | jq '.persistent'
```

### Using Skills Together

The two skills are designed to work together. A typical workflow:

1. **exercise_cluster** deploys a fixture and runs mutation scenarios.
2. A scenario produces UNSTABLE results — persistent observer findings after stabilization.
3. **diagnose_with_observer** takes over: fetches the snapshot, triages findings, traces logs, and identifies the root cause.
4. The bug report goes to the developer with the exact code path and suggested fix.

This separation keeps each skill focused: exercise finds the problem, diagnose traces the root cause. You can also use them independently — diagnose works on any running cluster, and exercise can operate without investigation if all scenarios pass cleanly.

### Example Prompts

The exerciser has three execution modes (`smoke`, `core`, `full`) and 8 fixtures with different characteristics. A full run across all fixtures can take over an hour. Use specific prompts to control what runs.

#### Exerciser — Controlling Scope

```
# Quick sanity check (~5 min) — deploy one fixture, verify baseline only
"Run a smoke test on the minimal-retain fixture"

# Standard operator coverage (~15 min) — scale up, scale down, pod deletion, resource updates
"Exercise the minimal-retain fixture in core mode"

# Thorough testing (~30+ min) — all scenarios including concurrent mutations and webhook rejections
"Run a full exercise on minimal-retain"

# Specific scenario — run just one mutation
"Exercise the minimal-retain fixture — only run the scale-down-pool-replicas scenario"

# Template-focused testing — verify template resolution and propagation
"Run smoke on templated-full and verify template propagation"

# Multiple fixtures — cover different code paths
"Exercise minimal-retain and minimal-delete in core mode"

# Webhook validation only — fast, no cluster mutations
"Run the webhook rejection scenarios (storage-shrink, etcd-replicas-immutable, invalid-template-reference)"

# Failure injection — test degraded-state handling
"Run the etcd-unavailability and image-pull-backoff failure injection scenarios on minimal-retain"

# After a code change — targeted verification
"I changed the drain state machine. Exercise minimal-retain with scale-down and rolling-update scenarios"

# Full audit — test everything across multiple fixtures (1+ hour)
"Run a full exercise on minimal-retain, minimal-delete, and templated-full. Test every scenario."

# Pre-release audit — maximum coverage, all fixtures, all scenarios
"I'm preparing a release. Run full exercises on all kind-ready fixtures, including webhook
rejections, failure injection, concurrent mutations, and template verification. Take your time."
```

If you just say "exercise the cluster" without specifying, the agent defaults to **core** mode on whatever fixture is already deployed (or `minimal-retain` if nothing is deployed).

#### Diagnose — Controlling Depth

```
# Quick look — just show what's wrong
"What does the observer say about the cluster?"

# Targeted investigation — focus on one check category
"Diagnose the replication findings from the observer"

# Full investigation with root cause analysis
"The observer shows fatal findings — diagnose and trace the root cause"

# After a mutation — check if things settled
"I just scaled down a pool. Check the observer for any persistent findings"

# History-based analysis — look for patterns over time
"Check the observer history for flapping or persistent findings"
```

#### Choosing a Fixture

| Fixture | Best for | Time |
|---------|----------|------|
| `minimal-retain` | Core operator logic, fastest feedback | ~5 min smoke, ~15 min core |
| `minimal-delete` | PVC deletion paths (historically buggy) | ~5 min smoke, ~15 min core |
| `templated-full` | Template resolution bugs, high bug surface | ~8 min smoke (includes TVP) |
| `overrides-complex` | Override merging, template+override interaction | ~8 min smoke |
| `multi-cell-quorum` | Multi-cell behavior, quorum | ~10 min smoke (resource heavy) |

Start with `minimal-retain` for the fastest feedback loop. Move to `templated-full` when testing template-related changes.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                 multigres-observer                  │
│                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────────────┐ │
│  │ Observer │ → │ Reporter │ → │ JSON stdout      │ │
│  │ (checks) │   │          │   │ Prometheus :9090 │ │
│  └──────────┘   └──────────┘   └──────────────────┘ │
│       │                                             │
│  ┌────┴───────────────────────────────────────────┐ │
│  │ Checks:                                        │ │
│  │  pod-health · resource-validation · crd-status │ │
│  │  drain-state · connectivity · replication      │ │
│  │  operator-logs · dataplane-logs · events       │ │
│  │  topology                                      │ │
│  └────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
         ↕ Kubernetes API              ↕ Direct SQL
   CRDs, Pods, Events, Logs      PostgreSQL on pool pods
```

The observer is **mostly stateless** — no PVC, no persistent storage. Every cycle re-evaluates from scratch. In-memory trackers (restart counts, phase timestamps, drain state, phase transitions, finding history) reset when the pod restarts.

## What It Detects

The observer catches issues across the full stack:

**Control plane:**
- Pod crashes, OOMKills, restart storms
- Orphaned resources, broken ownership chains
- CRD phase regressions, stale ObservedGeneration
- Invalid phase transitions (e.g., Healthy → Initializing)
- Stuck-in-Progressing phases (>10 minutes without advancing)
- Empty status messages on Degraded/Unknown phases
- Stuck drains, invalid drain state transitions
- Operator health and metrics endpoint failures

**Data plane (SQL probes):**
- Replication lag (warn >10s, error >60s)
- Truncated `application_name` breaking sync replication matching
- Async standbys when synchronous replication is configured
- Blocked writes on the primary
- WAL receiver disconnected on replicas
- WAL replay paused on replicas
- Split-brain (multiple pods reporting as primary)

**Infrastructure:**
- TCP/HTTP/gRPC connectivity to all components
- Probe latency exceeding thresholds
- PVC validation (missing PVCs, unbound PVCs, orphaned PVCs)
- Service endpoint validation (missing or empty endpoints)
- etcd topology drift vs CRD state
- Kubernetes Warning events (FailedScheduling, OOM, Unhealthy)

**Backup health:**
- Stale backups (warn >25h, error >49h)
- Backup configured but never completed
- Unknown backup types

**CRD status consistency:**
- Cell gateway replica count mismatches (ready > total)
- Gateway deployment replica cross-checks
- Missing gateway service names on healthy cells

## Startup Grace Period

Newly created pool pods go through a brief startup phase where they are not yet serving (Postgres is starting, WAL receiver hasn't connected, standby is initially async). The observer suppresses transient noise from these pods to avoid cluttering findings with expected startup behavior:

| Pod age | Ready? | Behavior |
|---------|--------|----------|
| < 60s | any | **Suppressed** — all findings for this pod are skipped |
| ≥ 60s | no | **Downgraded** — `error`/`fatal` findings become `warn` |
| ≥ 60s | yes | **Full severity** — this is the high-value signal |

The grace period only applies to pool pods (`component=shard-pool`). Operator, multiorch, multigateway, and toposerver findings are always reported at full severity.

When a pod that Kubernetes marks as Ready is showing errors, that is exactly the kind of issue the observer exists to catch — what `kubectl` and the operator cannot see.

## Documentation

| Document | Description |
|----------|-------------|
| [Observer Reference](docs/observer.md) | Full reference for all 11 check categories with thresholds and SQL queries |
| [Configuration Reference](docs/configuration.md) | Flags, environment variables, thresholds, RBAC |
| [Architecture](docs/architecture.md) | Internal design, report format, JSON schemas, metrics, deployment model |
| [Skills Overview](skills/README.md) | AI agent skills for proactive testing and reactive diagnosis |
| [Exercise Cluster Skill](skills/exercise_cluster/SKILL.md) | Proactive mutation testing with stability verification |
| [Diagnose with Observer Skill](skills/diagnose_with_observer/SKILL.md) | Reactive investigation, triage, and root cause analysis |
| [Scenario Catalog](skills/exercise_cluster/references/scenarios/index.md) | 40+ mutation scenarios with commands and success criteria |
| [Operator Knowledge](skills/exercise_cluster/references/operator-knowledge.md) | Operator architecture and code paths for investigation |

## Project Structure

```
tools/observer/
├── cmd/multigres-observer/
│   └── main.go                    # Entrypoint, flags, metrics server
├── pkg/
│   ├── observer/                  # 11 check categories + API
│   │   ├── observer.go            # Main loop, cycle orchestration, HTTP handlers
│   │   ├── history.go             # Finding history ring buffer + classification
│   │   ├── history_test.go        # Unit tests for history
│   │   ├── probes.go              # Per-cycle probe data collector
│   │   ├── snapshot.go            # Thread-safe latest-cycle snapshot store
│   │   ├── pods.go                # Pod health, restarts, OOM, counts
│   │   ├── resources.go           # Resource ownership, labels, PVC validation, service endpoints
│   │   ├── status.go              # CRD phases, conditions, podRoles, backup staleness, cell fields
│   │   ├── drain.go               # Drain state machine monitoring
│   │   ├── connectivity.go        # TCP/HTTP/gRPC/SQL endpoint probing, operator metrics
│   │   ├── replication.go         # SQL replication health, split-brain
│   │   ├── logs.go                # Log streaming, error patterns
│   │   ├── events.go              # Kubernetes event monitoring
│   │   ├── topology.go            # etcd topology validation
│   │   ├── status_extended_test.go    # Tests for phase, backup, cell status checks
│   │   └── resources_extended_test.go # Tests for PVC and service endpoint checks
│   ├── report/                    # Structured reporting + Prometheus
│   │   ├── reporter.go            # JSON log emission, SummaryWithFindings
│   │   ├── types.go               # Finding, Summary, StatusResponse, CoverageInfo
│   │   └── metrics.go             # Prometheus gauges/counters/histograms
│   └── common/                    # Shared constants, client setup
│       ├── client.go              # Kubernetes client factory
│       └── constants.go           # Labels, ports, thresholds, states
├── skills/                        # AI agent skills
│   ├── README.md                  # Skills overview and API reference
│   ├── exercise_cluster/          # Proactive mutation testing
│   │   ├── SKILL.md               # Skill definition with protocols
│   │   └── references/            # Scenario catalog, operator knowledge
│   └── diagnose_with_observer/    # Reactive diagnosis and triage
│       └── SKILL.md               # Skill definition with investigation rules
├── deploy/
│   └── base/                      # Kustomize manifests (read-only RBAC)
├── Dockerfile
└── go.mod                         # Separate Go module
```

## Roadmap

The observer exists primarily because upstream Multigres components are still maturing — they don't yet expose enough health detail for the operator to act on every failure mode. The operator itself is thorough in its reconciliation, status reporting, and self-diagnosis, but it can only work with what the components tell it. When a multipooler returns `/ready=200` while its gRPC server hangs, or when a multiorch reports healthy while all its pooler connections are dead, the operator has no way to know. The observer fills that gap by independently probing each component. As the system evolves, the observer's role will change too.

**Chaos testing integration.** The exerciser skill currently drives mutations through `kubectl patch` and verifies health via the observer API. A natural next step is integrating with chaos testing frameworks like Chaos Mesh to inject infrastructure-level faults (network partitions, disk I/O delays, node failures, clock skew) that `kubectl patch` cannot simulate. The observer and its skills already have the verification protocols and finding classification needed to evaluate cluster behavior under fault injection — what's missing is the fault injection layer itself. This is not practical yet: Multigres needs to be more resilient to infrastructure faults before chaos testing produces actionable results rather than cascading failures.

**Self-diagnosing Multigres.** As upstream Multigres matures, many of the observer's checks should move into the components themselves. The multiorch could detect split-brain conditions and report them via its own health endpoint rather than requiring external SQL probes. The multipooler could expose gRPC server state in its readiness response so the operator can distinguish "healthy" from "accepting HTTP but gRPC is hanging." The multigateway could report routing failures through structured health data instead of requiring the observer to scrape `/debug/status` HTML. Once the components expose richer health signals, the operator — which already has strong reconciliation and status reporting — can incorporate them directly. The observer's check implementations, thresholds, and SQL queries serve as a concrete reference for what upstream self-diagnosis should cover — they document every failure mode discovered through real-world operation and testing. As these capabilities move into Multigres, the observer can be simplified or retired, having served its purpose as a proving ground for health validation logic.

**Current state.** Today, the observer fills a gap that nothing else covers. It is the only component that independently verifies end-to-end health across the full stack (SQL replication, gRPC connectivity, topology consistency, CRD status accuracy). The skills encode investigation patterns that would otherwise require deep knowledge of both operator and upstream internals. Until upstream Multigres components expose richer health signals that the operator can act on, the observer remains the tool of last resort for catching silent failures.
