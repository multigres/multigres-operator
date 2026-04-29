# Multigres Operator — Observability Demo

This hands-on tutorial walks through every pillar of the operator's observability
stack: **structured logging**, **Kubernetes events**, **Prometheus metrics**,
**distributed tracing**, **Grafana dashboards**, and **alerting**.

The operator and its data plane use **two different metric models**: the operator
exposes metrics via a traditional Prometheus scrape endpoint (pull), while data
plane components (multiorch, multipooler, multigateway) push metrics via
OpenTelemetry. An OTel Collector bridges the gap by routing pushed signals to
the appropriate backends.

By the end you will have a running cluster with full telemetry and understand
how each signal is generated, collected, and visualised.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Environment Setup](#2-environment-setup)
3. [Deploy the Sample Cluster](#3-deploy-the-sample-cluster)
4. [Logging & Events](#4-logging--events)
5. [Metrics](#5-metrics)
6. [Distributed Tracing](#6-distributed-tracing)
7. [Grafana Dashboards](#7-grafana-dashboards)
8. [Alerting](#8-alerting)
9. [Production Configuration](#9-production-configuration)
10. [Cleanup](#10-cleanup)

---

## 1. Prerequisites

| Tool | Version |
|------|---------|
| Docker | 24+ |
| Kind | 0.20+ |
| kubectl | 1.28+ |
| Go | 1.23+ |
| Make | any |

Ensure Docker is running and the Kind binary is in your `$PATH`.

---

## 2. Environment Setup

The operator ships with a single Makefile target that builds the operator image,
creates a Kind cluster, and deploys the full observability stack (Prometheus,
Tempo, Grafana) as a sidecar pod alongside the operator.

```bash
# Build & deploy everything (takes ~2 min on first run)
make kind-deploy-observability
```

Once complete, open the UIs with:

```bash
make kind-portforward
```

This starts `kubectl port-forward` and prints the URLs:

| Service    | URL                        |
|------------|----------------------------|
| Grafana    | http://localhost:3000      |
| Prometheus | http://localhost:9090      |
| Tempo      | http://localhost:3200      |

> [!TIP]
> Grafana is pre-configured with **anonymous admin** access — no login required.
> Datasources for Prometheus and Tempo are auto-provisioned with cross-linking
> (trace → metric correlation) already wired up.

> [!NOTE]
> `kubectl port-forward` connections can drop under load (especially on Kind).
> If one connection drops, the others stay alive — just re-run `make kind-portforward`.

### What gets deployed

The observability overlay (`config/deploy-observability/`) deploys each service
as a separate Deployment (or StatefulSet for Prometheus):

| Deployment         | Image                                          | Purpose |
|--------------------|------------------------------------------------|---------|
| `otel-collector`   | `otel/opentelemetry-collector-contrib:0.120.0` | Receives OTLP from data plane, routes traces → Tempo, metrics → Prometheus |
| `prometheus`       | Managed by Prometheus Operator                 | Scrapes operator `/metrics` via ServiceMonitor and receives pushed metrics via OTLP |
| `tempo`            | `grafana/tempo:2.7.2`                          | Stores distributed traces |
| `grafana`          | `grafana/grafana:11.4.0`                       | Pre-loaded dashboards and datasources |

The **Prometheus Operator** is installed from the upstream bundle. It manages
the Prometheus instance and natively consumes `PrometheusRule` CRDs for alert
rules and `ServiceMonitor` CRDs for scrape target discovery.

The operator deployment is patched with the environment variable
`OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.multigres-operator.svc:4318`
to enable trace and metric export to the collector.

### Why an OTel Collector?

The operator and data plane have fundamentally different metric architectures:

| Component | Metrics Model | Transport |
|-----------|---------------|-----------|
| **Operator** | **Pull** — Prometheus scrapes `/metrics` on `:8443` | controller-runtime uses `prometheus/client_golang` natively |
| **Data plane** (multiorch, multipooler, multigateway) | **Push** — OTLP to collector | Multigres binaries use the OpenTelemetry SDK; all telemetry (traces + metrics) goes to a single endpoint |

Because multigres components push **all** OTLP signals to one endpoint, we need
the collector to split them: traces go to Tempo, metrics go to Prometheus. Without
the collector, data plane metrics would be sent to Tempo and silently dropped
(Tempo only handles traces).

---

## 3. Deploy the Sample Cluster

Apply the minimal sample to create a `MultigresCluster`:

```bash
kubectl apply -f config/samples/minimal.yaml
```

```yaml
# config/samples/minimal.yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: minimal
  namespace: default
spec:
  cells:
    - name: "zone-a"
      zoneId: "use1-az1"
```

The operator fills in all defaults: a 3-node etcd TopoServer, a MultiAdmin,
a default database with a default tablegroup and a single shard.

Watch the resources converge:

```bash
kubectl get multigrescluster,cell,shard,toposerver -A
```

```
NAMESPACE   NAME                                     AVAILABLE   AGE   PHASE
default     multigrescluster.multigres.com/minimal   True        30s   Progressing

NAMESPACE   NAME                                         GATEWAY   READY   PHASE
default     cell.multigres.com/minimal-zone-a-96c88ae0   1         True    Healthy

NAMESPACE   NAME                                                          READY   PHASE
default     shard.multigres.com/minimal-postgres-default-0-inf-1e3c045c   False   Progressing

NAMESPACE   NAME                                           READY   PHASE
default     toposerver.multigres.com/minimal-global-topo           Progressing
```

Give it another 30–60 seconds and all resources will reach `Healthy`.

---

## 4. Logging & Events

### 4.1 Structured Logs

The operator uses `zap` for structured JSON logging. Every log line includes the
controller name, the reconciled object, and — when tracing is enabled — the
`trace_id` and `span_id` fields.

```bash
kubectl logs deploy/multigres-operator-controller-manager \
  -n multigres-operator --tail=5
```

Example output (formatted for readability):

```json
{
  "level": "debug",
  "ts": "2026-02-11T17:50:50Z",
  "msg": "reconcile started",
  "controller": "multigrescluster",
  "MultigresCluster": {"name":"minimal","namespace":"default"},
  "reconcileID": "4a0f9c5e-3e23-4425-b760-56370da6d73e",
  "trace_id": "07a3af4c44a257a71260af8628312b03",
  "span_id": "55b429f1b4de41e5"
}
```

```json
{
  "level": "debug",
  "ts": "2026-02-11T17:50:50Z",
  "msg": "reconcile complete",
  "controller": "toposerver",
  "TopoServer": {"name":"minimal-global-topo","namespace":"default"},
  "trace_id": "e15e6d96455a6e2bfce600487f4b0a0d",
  "span_id": "a4c3fda9b0fdd889",
  "duration": "17.788233ms"
}
```

> [!IMPORTANT]
> **Log-Trace Correlation:** The `trace_id` in logs is the same ID you can
> search for in Grafana/Tempo. This lets you jump from a suspicious log line
> directly to the full distributed trace.

#### How it works

The function `monitoring.EnrichLoggerWithTrace(ctx, logger)` (in
`pkg/monitoring/tracing.go`) extracts the active span from the context and
injects `trace_id` + `span_id` as structured fields into the zap logger.

### 4.2 Kubernetes Events

The operator emits Kubernetes events on every significant action. Each controller
uses `record.EventRecorder` to publish `Normal` and `Warning` events.

```bash
kubectl get events -n default --sort-by='.lastTimestamp' | tail -20
```

Example output:

```
Normal    Synced        multigrescluster/minimal            Successfully reconciled MultigresCluster
Normal    Applied       multigrescluster/minimal            Applied Global TopoServer minimal-global-topo
Normal    Applied       multigrescluster/minimal            Applied Cell minimal-zone-a-96c88ae0
Normal    Applied       multigrescluster/minimal            Applied MultiAdmin Deployment minimal-multiadmin
Normal    Applied       multigrescluster/minimal            Applied TableGroup minimal-postgres-default-f7d4ad4a
Normal    Synced        cell/minimal-zone-a-96c88ae0        Successfully reconciled Cell
Warning   RegistrationFailed  cell/minimal-zone-a-96c88ae0 Failed to register cell in topology: Code: UNAVAILABLE...
Normal    Synced        toposerver/minimal-global-topo      Successfully reconciled TopoServer
Normal    Applied       toposerver/minimal-global-topo      Applied StatefulSet minimal-global-topo
Normal    PhaseChange   toposerver/minimal-global-topo      Transitioned from 'Progressing' to 'Healthy'
Normal    Synced        tablegroup/minimal-postgres-default  Successfully reconciled TableGroup
Normal    Applied       tablegroup/minimal-postgres-default  Applied Shard minimal-postgres-default-0-inf-1e3c045c
```

#### Event categories

**Lifecycle:**

| Reason | Type | Meaning |
|--------|------|---------|
| `Synced` | Normal | Reconcile completed successfully |
| `Applied` | Normal | A child resource was created or updated |
| `Deleted` | Normal | A child resource was deleted |
| `ImplicitDefault` | Normal | Operator injected a default configuration |
| `Generated` | Normal | Certificate or config was generated |
| `Rotated` | Normal | Certificate was rotated |

**Topology & Status:**

| Reason | Type | Meaning |
|--------|------|---------|
| `TopologyWaiting` | Normal | Waiting for topology server connectivity |
| `CleanupComplete` | Normal | Topology deregistration completed |
| `PoolersPruned` | Normal | Stale pooler entries removed from topology |
| `TopoConnectFailed` | Warning | Topology server connection failed (transient) |
| `TopologyError` | Warning | Topology registration or query failed |

**Drain & Deletion:**

| Reason | Type | Meaning |
|--------|------|---------|
| `PendingDeletion` | Normal | Shard entered PendingDeletion flow |
| `DrainStarted` | Normal | Graceful drain initiated for a pod |
| `DrainCompleted` | Normal | Pod drain completed successfully |
| `ReadyForDeletion` | Normal | All pods drained; safe to delete |
| `StuckTerminating` | Warning | Pod stuck in Terminating state; force-deleted |

**Backup & Storage:**

| Reason | Type | Meaning |
|--------|------|---------|
| `BackupHealthy` | Normal | Backup age is within threshold |
| `BackupStale` | Warning | Backup is older than threshold |
| `PVCExpanded` | Normal | PVC volume expansion succeeded |
| `ExpandPVCFailed` | Warning | PVC volume expansion failed |
| `FilesystemResize` | Normal | Filesystem resize triggered |

**Errors:**

| Reason | Type | Meaning |
|--------|------|---------|
| `ConfigError` | Warning | Configuration generation failed (e.g. pg_hba) |
| `FailedApply` | Warning | Failed to create or update a child resource |
| `StatusError` | Warning | Failed to update resource status |
| `TemplateMissing` | Warning | Referenced template not found |
| `PodReplaced` | Warning | A drained pod was replaced |

You can also see events directly on the resource:

```bash
kubectl describe multigrescluster minimal
```

Note the `multigres.com/traceparent` annotation on the object — this is how the
operator propagates trace context from the webhook to the reconciler:

```yaml
Annotations:
  multigres.com/traceparent: 00-5e0d253c3e7b3db8671a8f7ad6c71180-63d1e01929da64a3-01
  multigres.com/traceparent-ts: 1770832214
```

---

## 5. Metrics

Metrics come from **two sources** using different collection models.

### 5.1 Operator Metrics (Pull)

The operator exposes Prometheus metrics on `:8443/metrics` (mTLS-protected in
production, scraped by the in-cluster Prometheus in this demo). These metrics
are registered via controller-runtime's Prometheus integration and include both
custom operator metrics and standard framework metrics.

#### Custom metrics

Defined in `pkg/monitoring/metrics.go` and set by `pkg/monitoring/recorder.go`.

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `multigres_operator_cluster_info` | Gauge | `name`, `namespace`, `phase` | Info-style metric for cluster discovery |
| `multigres_operator_cluster_cells_total` | Gauge | `cluster`, `namespace` | Number of cells in the cluster |
| `multigres_operator_cluster_shards_total` | Gauge | `cluster`, `namespace` | Number of shards in the cluster |
| `multigres_operator_cell_gateway_replicas` | Gauge | `cell`, `namespace`, `state` | Gateway replicas (desired vs ready) |
| `multigres_operator_shard_pool_replicas` | Gauge | `cluster`, `shard`, `pool`, `cell`, `namespace`, `state` | Pool replicas (desired vs ready) |
| `multigres_operator_pool_pods_drifted` | Gauge | `cluster`, `shard`, `pool`, `cell`, `namespace` | Pods with spec-hash mismatch pending rolling update |
| `multigres_operator_toposerver_replicas` | Gauge | `name`, `namespace`, `state` | TopoServer replicas (desired vs ready) |
| `multigres_operator_webhook_request_total` | Counter | `operation`, `resource`, `result` | Webhook admission request count |
| `multigres_operator_webhook_request_duration_seconds` | Histogram | `operation`, `resource` | Webhook latency distribution |
| `multigres_operator_last_backup_age_seconds` | Gauge | `cluster`, `shard`, `namespace` | Age of the most recent completed backup |
| `multigres_operator_drain_operations_total` | Counter | `cluster`, `shard`, `result` | Total graceful pod drain operations |
| `multigres_operator_rolling_update_in_progress` | Gauge | `cluster`, `shard`, `pool`, `cell`, `namespace` | Whether a rolling update is active (0/1) |

#### Framework metrics

The operator also exposes standard controller-runtime and Go metrics:

| Metric | Purpose |
|--------|---------|
| `controller_runtime_reconcile_total` | Total reconcile count per controller |
| `controller_runtime_reconcile_errors_total` | Error count per controller |
| `controller_runtime_reconcile_time_seconds` | Reconcile duration histogram |
| `workqueue_depth` | Current work queue depth |
| `go_goroutines` | Active goroutine count |
| `process_resident_memory_bytes` | Operator RSS |

### 5.2 Data Plane Metrics (Push)

The data plane components (multiorch, multipooler, multigateway) push metrics
via OTLP to the OTel Collector, which forwards them to Prometheus. These
metrics are generated by the multigres binaries using the OpenTelemetry SDK.

Unlike the operator's pull model, data plane pods have **no `/metrics` endpoint**.
All telemetry (traces, metrics, logs) is pushed to the configured
`OTEL_EXPORTER_OTLP_ENDPOINT`, which points to the OTel Collector.

The signal flow looks like this:

```
┌──────────────┐      ┌────────────────┐  traces  ┌────────────┐
│  multiorch   │─OTLP─▶                │─────────▶│   Tempo    │
│  multipooler │─OTLP─▶ OTel Collector │          └────────────┘
│  multigateway│─OTLP─▶                │ metrics  ┌────────────┐
└──────────────┘      └────────────────┘─────────▶│ Prometheus │
                                                  └────────────┘
```

Data plane metrics use the `multiorch_` prefix (from multiorch) and are
visible in Prometheus alongside the operator's `multigres_operator_` metrics.

#### Querying data plane metrics

Open the Prometheus UI at **http://localhost:9090** and try:

```promql
{__name__=~"multiorch_.*"}
```

You should see metrics like:

| Metric | Description |
|--------|-----------|
| `multiorch_recovery_action_duration_milliseconds` | Duration of recovery actions |
| `multiorch_recovery_cluster_metadata_refresh_duration_seconds` | Metadata refresh latency |
| `multiorch_recovery_pooler_poll_duration_seconds` | Pooler polling latency |
| `multiorch_recovery_pooler_store_size` | Number of pooler entries tracked |

> [!NOTE]
> Data plane metrics only appear **after** the cluster has been created and
> the multigres pods are running. They arrive via OTLP push through the
> collector, so there may be a 30-60 second delay after pod startup.

### 5.3 Exploring Prometheus

Open the Prometheus UI at **http://localhost:9090**.

To browse available metrics without knowing their names, click the **⋮** (three
dots) menu next to the query input box and select **Metrics Explorer**. This
shows every metric name Prometheus has collected, with search and filter.

Other useful pages:

| Page | URL | Purpose |
|------|-----|---------|
| Targets | http://localhost:9090/targets | Shows what Prometheus is scraping and whether each target is UP |
| Alerts | http://localhost:9090/alerts | Shows configured alert rules and their current state |
| TSDB Status | http://localhost:9090/tsdb-status | Top metric names by series count |

> [!TIP]
> Switch to the **Table** tab (instead of Graph) when exploring metrics to see
> all label combinations at a glance.

### 5.4 Querying Operator Metrics

Open the Prometheus UI at **http://localhost:9090** and try these queries:

#### Cluster info

```promql
multigres_operator_cluster_info
```

```
multigres_operator_cluster_info{name="minimal", namespace="default", phase="Progressing"} = 1
```

#### Topology counts

```promql
multigres_operator_cluster_cells_total
```

```
multigres_operator_cluster_cells_total{cluster="minimal", namespace="default"} = 1
```

```promql
multigres_operator_cluster_shards_total
```

```
multigres_operator_cluster_shards_total{cluster="minimal", namespace="default"} = 1
```

#### Reconcile throughput

This uses the standard `controller_runtime_reconcile_total` metric from the
controller-runtime framework:

```promql
sum by (controller, result) (controller_runtime_reconcile_total)
```

```
controller=cell            result=success   count=3
controller=multigrescluster result=success  count=12
controller=shard           result=success   count=9
controller=shard           result=error     count=5
controller=tablegroup      result=success   count=8
controller=toposerver      result=success   count=6
```

> [!NOTE]
> Early `shard` controller errors are **expected** — the shard controller
> registers shards in the topology server, which is unavailable while etcd
> is still starting. It reconciles again once connectivity is established.

---

## 6. Distributed Tracing

### 6.1 What Are Traces?

A **distributed trace** is a tree of timed operations (**spans**) that records
the full lifecycle of a request as it flows through a system. Each span has:

- A **name** describing the operation (e.g. `Cell.Reconcile`)
- A **duration** (start time → end time)
- A **parent** linking it to the span that triggered it
- **Attributes** (key-value metadata such as the resource name or namespace)
- A **trace ID** shared by every span in the same trace

In a monolithic application, a stack trace gives you the full call chain. In
a Kubernetes operator, work is split across multiple controllers that react
asynchronously to watch events — there is no single call stack. Distributed
tracing reconstructs that causal chain by linking spans across controllers.

#### Why this matters for operators

When a `MultigresCluster` is created, the operator triggers a cascade of
reconciliations:

```
MultigresCluster.Reconcile
  ├── creates Cell → triggers Cell.Reconcile
  │     └── creates MultiGateway Deployment
  ├── creates TopoServer → triggers TopoServer.Reconcile
  │     └── creates StatefulSet
  ├── creates TableGroup → triggers TableGroup.Reconcile
  │     └── creates Shard → triggers Shard.Reconcile
  │           ├── creates pool Pods (primary, replica)
  │           ├── creates MultiOrch Deployment
  │           ├── creates MultiPooler Deployment
  │           └── registers shard in topology
  └── creates MultiAdmin Deployment
```

Without tracing, debugging a failure in `Shard.Reconcile` means grepping
logs across multiple controllers and mentally reconstructing the timeline.
With tracing, you open a single trace and see the full waterfall — which
controllers ran, in what order, how long each took, and where the failure
occurred.

### 6.2 How Tracing Works in the Operator

The operator uses OpenTelemetry for distributed tracing. When the environment
variable `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the operator:

1. Initialises a `TracerProvider` on startup (`monitoring.InitTracing()`)
2. Creates a root span for every reconcile (`monitoring.StartReconcileSpan()`)
3. Creates child spans for sub-operations (`monitoring.StartChildSpan()`)
4. Enriches logs with trace context (`monitoring.EnrichLoggerWithTrace()`)
5. Propagates trace context across async boundaries using annotations

### 6.3 Trace Propagation (Webhook → Reconcile)

The operator bridges the async gap between the webhook admission request and the
controller reconciliation using annotations:

```
┌─────────────────────┐         ┌────────────────────────┐
│  Webhook Admission  │         │  Controller Reconcile  │
│                     │         │                        │
│  1. Create span     │         │  4. Extract traceparent│
│  2. Inject trace    │────────▶│     from annotation    │
│     context into    │         │  5. Create child span  │
│     annotation      │         │     under parent       │
│  3. Mutate object   │         │  6. Full trace visible │
└─────────────────────┘         └────────────────────────┘
```

The annotation `multigres.com/traceparent` carries the W3C Trace Context:

```
00-5e0d253c3e7b3db8671a8f7ad6c71180-63d1e01929da64a3-01
│   │                                │                  │
│   └── trace-id (128-bit)           └── parent-id      └── flags
└── version
```

**Key functions:**
- `monitoring.InjectTraceContext()` — Webhook writes the traceparent annotation
- `monitoring.ExtractTraceContext()` — Controller reads it and re-parents its span

### 6.4 Viewing Traces in Grafana

1. Open **http://localhost:3000/explore**
2. Select the **Tempo** datasource
3. Choose the **TraceQL** tab
4. Run the query `{}` to see all traces

You should see traces for every controller reconciliation:

| Name | Duration | Description |
|------|----------|-------------|
| `MultigresCluster.Reconcile` | <1ms | Root reconcile of the cluster |
| `Cell.Reconcile` | 74ms | Cell controller reconcile |
| `Shard.Reconcile` | 134ms | Shard controller reconcile (includes topology registration) |
| `TableGroup.Reconcile` | 21–53ms | TableGroup reconcile |
| `TopoServer.Reconcile` | 32ms | TopoServer reconcile |

#### Filtering traces

Find traces for a specific cluster:

```traceql
{span.k8s.resource.name = "minimal" && span.k8s.resource.kind = "MultigresCluster"}
```

Find slow reconciliations (>500ms):

```traceql
{name =~ ".*Reconcile" && duration > 500ms}
```

### 6.5 Log-Trace Correlation

Copy a `trace_id` from the operator logs and paste it into Tempo's
"Import trace" field to jump directly to the full trace:

```bash
# Grab a trace ID from logs
kubectl logs deploy/multigres-operator-controller-manager \
  -n multigres-operator --tail=3 | grep trace_id
```

```
"trace_id": "5e0d253c3e7b3db8671a8f7ad6c71180"
```

Paste `5e0d253c3e7b3db8671a8f7ad6c71180` into Grafana → Explore → Tempo →
Import Trace to see the full waterfall.

### 6.6 Debugging with Traces — A Walkthrough

Scenario: a `MultigresCluster` is stuck in `Progressing` and you want to find
out which sub-controller is failing.

**Step 1 — Find the trace ID from the cluster's logs:**

```bash
kubectl logs deploy/multigres-operator-controller-manager \
  -n multigres-operator | grep '"controller":"multigrescluster"' | tail -1
```

Output:
```json
{"controller":"multigrescluster", "msg":"reconcile complete", "trace_id":"07a3af4c44a257a71260af8628312b03", ...}
```

**Step 2 — Open the trace in Grafana:**

Go to Grafana → Explore → Tempo and search by trace ID
`07a3af4c44a257a71260af8628312b03`. You will see a waterfall like:

```
[MultigresCluster.Reconcile]  ─────────────────────────── 45ms
  [Cell.Reconcile]            ──────────────────────────── 74ms
  [TopoServer.Reconcile]      ────────────────────── 32ms
  [TableGroup.Reconcile]      ──────────────────────── 53ms
    [Shard.Reconcile]         ─── ERROR ── 1.0s ──────────
```

**Step 3 — Identify the problem:**

The `Shard.Reconcile` span shows an error with 1.0s duration — the shard
controller is timing out trying to register the shard in the topology server.
Clicking the span reveals the error attribute:

```
status: ERROR
error.message: "rpc error: code = Unavailable desc = connection refused"
```

This tells you the shard controller cannot reach the TopoServer's etcd
cluster, which is still starting. The operator will automatically retry and
the errors will resolve once etcd is ready.

**Step 4 — Correlate with logs:**

Search the operator logs for the same trace ID:

```bash
kubectl logs deploy/multigres-operator-controller-manager \
  -n multigres-operator | grep '07a3af4c44a257a71260af8628312b03'
```

This returns every log line emitted during that trace, across all controllers,
giving you the complete narrative of what happened.

---

## 7. Grafana Dashboards

Three pre-provisioned dashboards give you at-a-glance monitoring.

### 7.1 Multigres Operator Overview

**URL:** http://localhost:3000/d/multigres-operator-overview

This dashboard shows the health of the **operator itself**:

| Panel | What it Shows |
|-------|---------------|
| Reconcile Rate by Controller | `rate(controller_runtime_reconcile_total[5m])` per controller |
| Reconcile Errors by Controller | Error rates with threshold lines |
| Reconcile Duration (p50/p99) | Latency percentiles |
| Work Queue Depth | Items waiting in each controller's queue |
| Webhook Request Rate | `rate(multigres_operator_webhook_request_total[5m])` |
| Webhook Latency (p50/p99) | Admission webhook response time |
| Goroutines | `go_goroutines` |
| Memory | RSS + heap allocation |
| GC Pause Duration | Garbage collection impact |

### 7.2 Multigres Cluster Health

**URL:** http://localhost:3000/d/multigres-cluster-health

This dashboard shows the health of the **clusters being managed**:

| Panel | What it Shows |
|-------|---------------|
| Cluster Phases | Table of all clusters with current phase (color-coded) |
| Cells per Cluster | `multigres_operator_cluster_cells_total` |
| Shards per Cluster | `multigres_operator_cluster_shards_total` |
| MultiGateway Replicas | Desired vs ready replicas (dashed vs solid lines) |
| Pool Replicas | Shard pool desired vs ready replicas |
| TopoServer Replicas | TopoServer desired vs ready replicas |
| Last Backup Age | Age of most recent backup per shard (thresholds at 24h/48h) |
| Drain Operations | Rate of drain operations by result (success/timeout/failure) |
| Rolling Update In Progress | Which pools are actively rolling (0/1) |
| Spec-Hash Drifted Pods | Count of pods with configuration mismatch pending update |

### 7.3 Multigres Data Plane

**URL:** http://localhost:3000/d/multigres-data-plane

This dashboard shows telemetry from the **data plane components** (multigateway, multipooler, multiorch, topology). It is organized into four sections:

**Multigateway – Client Gateway:**

| Panel | What it Shows |
|-------|---------------|
| HTTP Response Codes | `rate(http_server_duration_milliseconds_count[...])` by status code |
| HTTP Latency (p95/p50) | Client-facing HTTP latency percentiles |
| gRPC Client Response Codes | `rate(rpc_client_duration_milliseconds_count[...])` by status |
| gRPC Client Latency (p95/p50) | gRPC client call latency |

**Multipooler – Connection Pooling:**

| Panel | What it Shows |
|-------|---------------|
| gRPC Server Response Codes | Server-side gRPC status codes for pooler |
| gRPC Server Latency | Server-side gRPC latency percentiles |
| PostgreSQL Connection Pool States | `multipooler_pgpool_conns` by state (idle/used) |
| gRPC Connection Cache Size | `multipooler_grpc_connection_cache_size` |
| gRPC Connection Pool Operations | Reuse vs create rates |
| gRPC Connection Dial Duration | Time to establish new gRPC connections |

**Multiorch – Orchestration & Recovery:**

| Panel | What it Shows |
|-------|---------------|
| gRPC Client Response Codes | RPC calls to multipooler and pgctld |
| gRPC Client Latency | Latency of outgoing RPC calls |
| Recovery Actions | `rate(multiorch_recovery_action_total[...])` by type |
| Recovery Action Duration | Time per recovery action (p95/p50) |
| Detected Problems | `multiorch_recovery_detected_problems` by type |
| Health Check Failures | `multiorch_recovery_health_check_failure_total` by check |
| Pooler Health Check Duration | Time to check pooler health (p95/p50) |
| Pooler Store Size | Number of poolers tracked |
| Cluster Metadata Refresh Duration | Time to refresh metadata from topology |

**Topology & Consensus:**

| Panel | What it Shows |
|-------|---------------|
| Topology Lock Operations | Lock acquire/release rates by resource type |
| Topology Lock Duration | Lock hold time (p95/p50) — high values indicate contention |

> **Note:** Panels with exemplars enabled support clicking data points to jump directly to Tempo traces.

### 7.4 Cross-Linking

The dashboards are pre-wired with **Prometheus → Tempo cross-linking**:

- Prometheus exemplars contain `trace_id` labels
- Clicking an exemplar opens the full trace in the linked Tempo datasource
- Tempo is configured with `tracesToMetrics` to link back to Prometheus

This configuration lives in `config/deploy-observability/grafana.yaml`:

```yaml
datasources:
  - name: Prometheus
    jsonData:
      exemplarTraceIdDestinations:
        - name: trace_id
          datasourceUid: tempo-uid
  - name: Tempo
    jsonData:
      tracesToMetrics:
        datasourceUid: prometheus-uid
```

---

## 8. Alerting

### 8.1 Alert Rules

The operator ships with alert rules as a `PrometheusRule` CRD in
`config/monitoring/prometheus-rules.yaml`. The Prometheus Operator natively
consumes this CRD — no manual rule file configuration needed.

Verify the rules are loaded at http://localhost:9090/alerts.

The rules define ten alerts:

| Alert | Severity | `for` | Condition |
|-------|----------|-------|-----------|
| `MultigresClusterReconcileErrors` | warning | 5m | Reconcile error rate > 0 for any controller |
| `MultigresClusterDegraded` | warning | 10m | Cluster phase ≠ Healthy for > 10 min |
| `MultigresCellGatewayUnavailable` | critical | 5m | Zero ready gateway replicas |
| `MultigresShardPoolDegraded` | warning | 10m | Ready < desired pool replicas |
| `MultigresWebhookErrors` | warning | 5m | Webhook error rate > 0 |
| `MultigresBackupStale` | warning | 30m | Backup age > 24 hours |
| `MultigresRollingUpdateStuck` | warning | 30m | Rolling update in progress > 30 min |
| `MultigresDrainTimeout` | warning | 10m | Drain timeout rate > 0 |
| `MultigresReconcileSlow` | warning | 5m | p99 reconcile duration > 30s |
| `MultigresControllerSaturated` | warning | 10m | Work queue depth > 50 |

#### Example: Alert Expression

```yaml
- alert: MultigresClusterDegraded
  expr: multigres_operator_cluster_info{phase!="Healthy"} == 1
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "MultigresCluster {{ $labels.name }} is degraded"
    description: >-
      Cluster {{ $labels.name }} in namespace {{ $labels.namespace }}
      has been in phase "{{ $labels.phase }}" for more than 10 minutes.
    runbook_url: "https://github.com/multigres/multigres-operator/blob/main/docs/monitoring/runbooks/MultigresClusterDegraded.md"
```

### 8.2 Checking Active Alerts

In Prometheus (http://localhost:9090/alerts):

```bash
# Or via the API
curl -s http://localhost:9090/api/v1/alerts | python3 -m json.tool
```

### 8.3 Runbooks

Every alert includes a `runbook_url` annotation linking to investigation and
remediation procedures in `docs/monitoring/runbooks/`:

| Runbook | File |
|---------|------|
| MultigresClusterReconcileErrors | `docs/monitoring/runbooks/MultigresClusterReconcileErrors.md` |
| MultigresClusterDegraded | `docs/monitoring/runbooks/MultigresClusterDegraded.md` |
| MultigresCellGatewayUnavailable | `docs/monitoring/runbooks/MultigresCellGatewayUnavailable.md` |
| MultigresShardPoolDegraded | `docs/monitoring/runbooks/MultigresShardPoolDegraded.md` |
| MultigresWebhookErrors | `docs/monitoring/runbooks/MultigresWebhookErrors.md` |
| MultigresReconcileSlow | `docs/monitoring/runbooks/MultigresReconcileSlow.md` |
| MultigresControllerSaturated | `docs/monitoring/runbooks/MultigresControllerSaturated.md` |

Example runbook checklist (from `MultigresClusterReconcileErrors.md`):

1. Check operator logs: `kubectl logs deploy/multigres-operator-controller-manager -n multigres-operator`
2. Check cluster status: `kubectl get multigrescluster -A`
3. Check events: `kubectl get events -n <namespace> --sort-by='.lastTimestamp'`
4. Check pod status: `kubectl get pods -n <namespace>`
5. Remediate: fix configuration, clear resource limits, or restart the operator.

---

## 9. Production Configuration

The demo setup uses the Kind observability overlay for convenience. In
production you will deploy the operator and observability stack separately.
This section explains how to configure each.

### 9.1 Operator Tracing

The operator itself is configured through environment variables on the
controller-manager Deployment. Tracing is opt-in: if
`OTEL_EXPORTER_OTLP_ENDPOINT` is not set, tracing is completely disabled
(noop TracerProvider).

| Environment Variable | Default | Description |
|--|--|--|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(unset/disabled)_ | OTLP collector URL (e.g. `http://otel-collector.monitoring:4318`) |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` | Transport protocol (`http/protobuf` or `grpc`) |
| `OTEL_TRACES_EXPORTER` | `otlp` | Exporter backend. Set `none` to disable |
| `OTEL_TRACES_SAMPLER` | `always_on` | Sampling strategy (`always_on`, `parentbased_traceidratio`, etc.) |

Example Deployment patch:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: multigres-operator-controller-manager
  namespace: multigres-operator
spec:
  template:
    spec:
      containers:
        - name: manager
          env:
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: "http://otel-collector.monitoring.svc:4318"
            - name: OTEL_EXPORTER_OTLP_PROTOCOL
              value: "http/protobuf"
```

The operator registers itself with `service.name=multigres-operator` in
its trace resource.

### 9.2 Data-Plane Telemetry (MultigresCluster → Cell → Shard)

The operator configures OpenTelemetry on the data-plane components it deploys
(MultiOrch, MultiPooler, MultiGateway) through the `ObservabilityConfig` in
the CRD hierarchy.

#### Inheritance model

```
MultigresCluster.spec.observability
          │
          ├──▶ Cell.spec.observability     (MultiGateway container)
          │
          └──▶ Shard.spec.observability    (MultiOrch, MultiPooler containers)
                (via TableGroup/ShardSpec)
```

When the operator creates Cell and Shard child resources, it copies
`cluster.Spec.Observability` into each child's spec. The resource-handler
controllers then call `BuildOTELEnvVars()` to inject the corresponding
`OTEL_*` environment variables into every data-plane container.

#### Default behaviour (inherit from operator)

If `observability` is not set on the `MultigresCluster` (i.e. `nil`),
`BuildOTELEnvVars()` falls back to the operator process's own environment
variables. This means data-plane pods automatically inherit the operator's
telemetry endpoint with zero configuration:

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: production
spec:
  # observability: not set → inherits operator's OTEL_EXPORTER_OTLP_ENDPOINT
  cells:
    - name: "zone-a"
      zoneId: "use1-az1"
```

#### Override per cluster

To send a specific cluster's data-plane telemetry to a different collector
(e.g. a team-specific Tempo instance), set the `observability` field:

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: team-analytics
spec:
  observability:
    otlpEndpoint: "http://team-analytics-collector.monitoring:4318"
    tracesExporter: "otlp"
    metricsExporter: "otlp"
    tracesSampler: "parentbased_traceidratio"
  cells:
    - name: "zone-a"
      zoneId: "use1-az1"
```

#### Disable data-plane telemetry

Set `otlpEndpoint` to `"disabled"` to suppress all OTEL environment variables
for that cluster's data-plane pods:

```yaml
spec:
  observability:
    otlpEndpoint: "disabled"
```

#### Full ObservabilityConfig reference

| Field | Maps to Env Var | Values |
|--|--|--|
| `otlpEndpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT` | URL or `"disabled"` |
| `otlpProtocol` | `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf`, `grpc` |
| `tracesExporter` | `OTEL_TRACES_EXPORTER` | `otlp`, `none`, `console` |
| `metricsExporter` | `OTEL_METRICS_EXPORTER` | `otlp`, `none`, `console` |
| `logsExporter` | `OTEL_LOGS_EXPORTER` | `otlp`, `none`, `console` |
| `metricExportInterval` | `OTEL_METRIC_EXPORT_INTERVAL` | Milliseconds (e.g. `"60000"`) |
| `metricsTemporality` | `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` | `cumulative`, `delta` |
| `tracesSampler` | `OTEL_TRACES_SAMPLER` | `always_on`, `always_off`, `parentbased_traceidratio`, `multigres_custom` |
| `samplingConfigRef` | `OTEL_TRACES_SAMPLER_CONFIG` | ConfigMap ref for file-based sampling |

### 9.3 Distinguishing Operator vs Data-Plane Traces

When both the operator and data-plane components send traces to the same
collector, you can distinguish them using the `service.name` resource
attribute:

| Component | `service.name` |
|--|--|
| Operator | `multigres-operator` |
| MultiOrch | Set by the multiorch binary (e.g. `multiorch`) |
| MultiPooler | Set by the multipooler binary (e.g. `multipooler`) |
| MultiGateway | Set by the multigateway binary (e.g. `multigateway`) |

In Tempo, filter by service name to see only one component's traces:

```traceql
{resource.service.name = "multigres-operator"}
```

```traceql
{resource.service.name = "multiorch"}
```

Or see all Multigres-related traces:

```traceql
{resource.service.name =~ "multigres.*|multi.*"}
```

### 9.4 Prometheus & Grafana

For production, deploy Prometheus (or the Prometheus Operator) and Grafana
separately. The operator provides ready-to-use resources under
`config/monitoring/`:

| Resource | File | Purpose |
|--|--|--|
| PrometheusRule | `config/monitoring/prometheus-rules.yaml` | Alert definitions |
| ConfigMap (dashboards) | `config/monitoring/grafana-dashboard-*.json` | Grafana dashboard JSON (assembled via kustomization) |

Apply them with:

```bash
kubectl apply -k config/monitoring/
```

Prometheus scrapes the operator's `/metrics` endpoint. The operator exposes
it on `:8443` with mTLS. The dev stack includes a `ServiceMonitor` in
`config/deploy-observability/prometheus.yaml` as a reference — adapt it to
your cluster's label selectors.

---

## 10. Cleanup

Remove the sample cluster:

```bash
kubectl delete -f config/samples/minimal.yaml
```

Tear down the Kind cluster:

```bash
make kind-down
```

---

## Architecture Reference

### Source Files

| Path | Purpose |
|------|---------|
| `pkg/monitoring/metrics.go` | Custom metric definitions |
| `pkg/monitoring/recorder.go` | Metric setters (cluster info, topology, webhooks) |
| `pkg/monitoring/tracing.go` | OTel init, span helpers, trace propagation, log enrichment |
| `cmd/multigres-operator/main.go` | TracerProvider initialization |
| `config/monitoring/prometheus-rules.yaml` | PrometheusRule alert definitions |
| `config/monitoring/grafana-dashboards.yaml` | Grafana dashboard JSON (ConfigMap) |
| `config/deploy-observability/` | Kustomize overlay for local observability stack |
| `docs/monitoring/runbooks/` | Alert investigation runbooks |

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            KUBERNETES CLUSTER                               │
│                                                                             │
│  ┌─────────────────────────┐                                                │
│  │   multigres-operator    │                                                │
│  │                         │  OTLP (traces)                                 │
│  │  Webhooks ────────────────────┐                                          │
│  │  Controllers ─────────────────┤                                          │
│  └─────────────────────────┘     │                                          │
│              ▲                   │                                          │
│  Prometheus  │                   │                                          │
│  scrapes     │                   ▼                                          │
│  /metrics    │   ┌──────────────────────┐                                   │
│  (via        │   │  OTel Collector      │  traces   ┌──────────────────┐    │
│  Service-    │   │  :4317 (gRPC)        │──────────▶│  Tempo           │    │
│  Monitor)    │   │  :4318 (HTTP)        │           │  :3200           │    │
│              │   │                      │  metrics  └──────────────────┘    │
│              │   │  Receives OTLP       │──────────▶┌──────────────────┐    │
│              │   │  from data plane     │           │  Prometheus      │    │
│              │   │  + operator traces   │           │  :9090           │    │
│              │   └──────────────────────┘           │  (Prom Operator) │    │
│              │                                      └───────┬──────────┘    │
│              │                                              │               │
│  ┌───────────┴─────────┐                  ┌─────────────────▼─────────────┐ │
│  │  multiorch          │                  │  Grafana :3000                │ │
│  │  multipooler        │  OTLP            │  Dashboards + Explore         │ │
│  │  multigateway       │─────────────────▶│  (queries Prometheus + Tempo) │ │
│  │  (data plane)       │  (traces+metrics)└───────────────────────────────┘ │
│  └─────────────────────┘                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```
