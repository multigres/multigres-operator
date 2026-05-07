# Observability Architecture

## Package Layout

All observability code lives in `pkg/monitoring/`:

| File | Purpose |
|:---|:---|
| `metrics.go` | Prometheus metric declarations and registration |
| `recorder.go` | Type-safe recorder functions that controllers call |
| `tracing.go` | OTel tracer init, span helpers, traceparent bridge, log-trace correlation |
| `metrics_test.go` | Tests for metric registration |
| `tracing_test.go` | Tests for all tracing functions |
| `recorder_test.go` | Tests for metric recording |

External artifacts:

| Path | Purpose |
|:---|:---|
| `config/monitoring/prometheus-rules.yaml` | PrometheusRule alerts (10 rules) |
| `config/monitoring/grafana-dashboard-operator.json` | Operator health dashboard |
| `config/monitoring/grafana-dashboard-cluster.json` | Per-cluster topology dashboard |
| `config/monitoring/grafana-dashboard-data-plane.json` | Data-plane operations dashboard |
| `config/monitoring/kustomization.yaml` | Assembles dashboards into ConfigMap for Grafana sidecar |
| `docs/monitoring/runbooks/*.md` | Alert runbooks (10 files) |

---

## Metrics

### Registration Pattern

Metrics are declared as package-level `var` blocks in `metrics.go` and registered in an `init()` function using `sigs.k8s.io/controller-runtime/pkg/metrics.Registry.MustRegister(...)`. This ensures they are available as soon as the monitoring package is imported.

Controllers do **not** interact with Prometheus types directly. Instead, they call type-safe recorder functions in `recorder.go`:

```go
// In a controller:
monitoring.SetClusterInfo(cluster.Name, cluster.Namespace, string(cluster.Status.Phase))
monitoring.SetShardPoolReplicas(shard.Name, pool.Name, shard.Namespace, desired, ready)
monitoring.RecordWebhookRequest("DEFAULT", "MultigresCluster", err, duration)
```

This indirection keeps controller code free of Prometheus imports and makes it easy to add/change metric dimensions without touching every controller.

### Metric Naming Convention

All operator-specific metrics use the `multigres_operator_` prefix. Labels follow Kubernetes conventions (`name`, `namespace`, `cluster`, `cell`, `shard`, `pool`).

The `cluster_info` gauge uses the **info-style pattern**: it is always set to `1` and uses labels (`phase`) to expose the cluster's current state. This allows PromQL joins:

```promql
multigres_operator_cluster_info{phase!="Healthy"} == 1
```

When the phase changes, `SetClusterInfo` calls `DeletePartialMatch` first to clean up the old phase label, preventing stale series with the old phase from lingering.

### Adding New Metrics

1. Declare the metric variable in `metrics.go`
2. Register it in `init()`
3. Add it to the `Collectors()` function (used by tests)
4. Create a recorder function in `recorder.go`
5. Call the recorder from the appropriate controller

---

## Metrics Collection: Pull vs Push

### The Two Models

The operator ecosystem uses **two different metric collection models** simultaneously:

| Component | Model | Transport | Why |
|:---|:---|:---|:---|
| **Operator** | **Pull** (Prometheus scrape) | HTTP `/metrics` on `:8443` | controller-runtime uses `prometheus/client_golang` natively |
| **Data plane runtimes** (multiorch, multipooler, multigateway, pgctld) | **Push** (OTLP) | gRPC/HTTP to OTLP endpoint | Multigres binaries use the OpenTelemetry SDK with `autoexport` |
| **Postgres engine metrics** (`postgres_exporter` sidecar on shard pool pods) | **Pull** (Prometheus scrape) | HTTP `/metrics` on pool headless Service `metrics` port | Scraped by ServiceMonitor targeting `app.kubernetes.io/component=shard-pool` |

### Why the Operator Uses Pull

controller-runtime's metrics infrastructure is built on `prometheus/client_golang`. Every metric registered via `sigs.k8s.io/controller-runtime/pkg/metrics.Registry` is automatically exposed on the HTTP handler. The framework provides no built-in OTLP metrics exporter.

We **could** add one by programmatically creating an `otelsdkmetric.MeterProvider` with an OTLP exporter and bridging the Prometheus registry into it. However, this would:

1. **Fight the framework** — controller-runtime assumes pull-based Prometheus metrics. All built-in metrics (`controller_runtime_reconcile_total`, `workqueue_depth`, etc.) go through the Prometheus registry. Bridging them to OTLP adds complexity for no functional gain.
2. **Duplicate signals** — Prometheus would still scrape `/metrics`, so every metric would exist in two places unless we disabled scraping entirely, which breaks standard monitoring patterns.
3. **Be unnecessary** — the Prometheus pull model works well for a single long-lived operator pod. Push-based metrics exist to solve problems the operator doesn't have (short-lived processes, scale-to-zero, high cardinality per-request metrics).

### Why the Data Plane Uses Push

Multigres binaries (multiorch, multipooler, multigateway, pgctld) are built with the OpenTelemetry SDK and the `autoexport` library, which reads `OTEL_*` environment variables to configure exporters automatically. Metrics are pushed through OTLP using either `OTEL_EXPORTER_OTLP_ENDPOINT` or the metrics-specific `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`.

This design is intentional: multigres components are data-plane workloads that may run at high scale across many pods. Push-based telemetry avoids the complexity of service discovery and scrape configuration for hundreds of pool replicas.

The operator injects `OTEL_RESOURCE_ATTRIBUTES` alongside the exporter
configuration for runtime containers. It follows the pod metadata model used by
log collection:

| Attribute | Source |
|:---|:---|
| `multigres.project` | `multigres.com/project-ref` annotation, falling back to cluster name |
| `multigres.cluster` | cluster label/name |
| `multigres.component` | runtime component (`multigateway`, `multiorch`, `multipooler`, `pgctld`) |

Multigres merges these environment-provided attributes into its OTel resource
while preserving its own service identity attributes.

### The OTel Collector Bridge

When multigres sends all signals to a single OTLP endpoint, the local observability stack uses an **OTel Collector** to split them:

```
multigres pods ──OTLP──▶ OTel Collector ──▶ Tempo      (traces)
                                          ──▶ Prometheus (metrics, via OTLP receiver)

operator pod   ◀── Prometheus scrapes /metrics (pull, unchanged)
```

Without the collector, metrics would be sent to Tempo (which only handles traces) and silently dropped. The collector's pipeline configuration routes each signal type to the appropriate backend.

For production ingestion paths, deployers can instead use
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` to send metrics directly to a
metrics-capable OTLP endpoint while keeping traces on the generic endpoint.

---

## Tracing

### Lifecycle

Tracing is initialised in `main.go` via `monitoring.InitTracing()`:

```go
shutdown, err := monitoring.InitTracing(ctx, "multigres-operator", version)
if err != nil {
    setupLog.Error(err, "failed to initialise tracing")
    os.Exit(1)
}
defer shutdown(ctx)
```

If `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, `InitTracing` returns a noop shutdown — the global `Tracer` stays as the default noop tracer from `otel.Tracer()`, so all `StartReconcileSpan`/`StartChildSpan` calls are zero-cost.

When the env var **is** set, `InitTracing`:
1. Creates an OTLP gRPC exporter (auto-configures from standard OTel env vars)
2. Builds a `Resource` with `service.name` and `service.version` semantic conventions
3. Registers a `TracerProvider` with batched export
4. Sets the W3C `TraceContext` propagator
5. Re-acquires the package-level `Tracer` from the new provider

### Span Hierarchy

```
MultigresCluster.Reconcile (root span — or child if traceparent bridge is active)
├── MultigresCluster.PopulateDefaults
├── MultigresCluster.ReconcileTableGroups
├── MultigresCluster.ReconcileCells
├── MultigresCluster.ReconcileGlobalComponents
└── MultigresCluster.UpdateStatus
```

Each controller's `Reconcile` creates a top-level span via `StartReconcileSpan`, and sub-operations use `StartChildSpan`. The span carries `k8s.resource.name`, `k8s.namespace`, and `k8s.resource.kind` attributes.

### Traceparent Annotation Bridge

The Kubernetes webhook and reconcile loop are asynchronous: the API Server calls the webhook, persists the object, and then the informer wakes the controller at an unpredictable time. To bridge this gap:

1. **Webhook side** (`defaulter.go`): After applying defaults, `InjectTraceContext(ctx, cluster.Annotations)` writes:
   - `multigres.com/traceparent` — W3C traceparent header
   - `multigres.com/traceparent-ts` — Unix timestamp of injection

2. **Controller side** (`multigrescluster_controller.go`): After fetching the cluster, `ExtractTraceContext(annotations)` reads the annotation:
   - **Fresh** (< 10 min): Ends the initial orphan span and restarts it as a child of the webhook trace
   - **Stale** (> 10 min): Creates a new root span with an OTel **Link** to the old trace, preserving causal history without creating misleading parent-child relationships

**Why only MultigresCluster?** Child resources (Cell, Shard, etc.) are created within the MultigresCluster reconcile loop, so they naturally inherit the trace context via the `ctx` parameter. Only the top-level resource needs the annotation bridge.

**The stale threshold (10 minutes)** prevents requeues, periodic reconciles, or operator restarts from creating misleading child spans under a trace from hours ago. A Link preserves the relationship without implying the old webhook is "still running."

### Adding Tracing to New Code

For a new controller:
```go
func (r *MyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    ctx, span := monitoring.StartReconcileSpan(ctx, "MyKind.Reconcile", req.Name, req.Namespace, "MyKind")
    defer span.End()
    ctx = monitoring.EnrichLoggerWithTrace(ctx)
    // ...
}
```

For a new sub-operation:
```go
ctx, span := monitoring.StartChildSpan(ctx, "MyKind.DoSomething")
defer span.End()
```

To record errors:
```go
if err != nil {
    monitoring.RecordSpanError(span, err)
    return ctrl.Result{}, err
}
```

---

## Log-Trace Correlation

`EnrichLoggerWithTrace(ctx)` extracts `trace_id` and `span_id` from the current OTel span and injects them into the `logr` logger in the context. All subsequent `log.FromContext(ctx).Info(...)` calls will include these fields automatically.

This enables "click log → view trace" in Grafana when Loki and Tempo are connected via a derived field on `trace_id`.

**Placement rule:** Call `EnrichLoggerWithTrace(ctx)` immediately after `StartReconcileSpan`, before acquiring the logger. This ensures the enriched logger is used throughout the entire reconcile, including by sub-operations that call `log.FromContext(ctx)`.

---

## Alerts & Runbooks

The 10 PrometheusRule alerts in `config/monitoring/prometheus-rules.yaml` are grouped by signal type:

| Category | Alerts |
|:---|:---|
| **Errors** | `MultigresClusterReconcileErrors`, `MultigresClusterDegraded`, `MultigresCellGatewayUnavailable`, `MultigresShardPoolDegraded`, `MultigresWebhookErrors` |
| **Backup & Drain** | `MultigresBackupStale`, `MultigresRollingUpdateStuck`, `MultigresDrainTimeout` |
| **Latency** | `MultigresReconcileSlow` |
| **Saturation** | `MultigresControllerSaturated` |

Each alert's `annotations.runbook_url` points to a markdown file in `docs/monitoring/runbooks/` with:
- **Meaning** — what the alert indicates
- **Impact** — what happens if ignored
- **Investigation Steps** — PromQL queries and `kubectl` commands to diagnose
- **Remediation** — specific actions to resolve

**Adding a new alert:**
1. Add the `PrometheusRule` entry in `prometheus-rules.yaml`
2. Create a runbook in `docs/monitoring/runbooks/{AlertName}.md`
3. Link the runbook URL in the alert's annotations

---

## Grafana Dashboards

Three JSON dashboards are provisioned via a `ConfigMap` (generated by `config/monitoring/kustomization.yaml`) that uses the standard Grafana sidecar label (`grafana_dashboard: "1"`):

**Operator Dashboard** — focuses on operator health:
- Reconcile rate and error rate per controller
- p50/p99 reconcile latency
- Work queue depth and saturation
- Webhook request rate and latency

**Cluster Dashboard** — focuses on cluster topology:
- Cluster phase status
- Cell and shard counts
- Gateway and pool replica health (desired vs. ready)
- TopoServer replica status

**Data Plane Dashboard** — focuses on data-plane operations:
- Pool pod drift (spec-hash mismatch)
- Rolling update progress
- Drain operations and timeouts
- Backup age tracking

**Editing dashboards:** Export the updated dashboard JSON from Grafana, save it to `config/monitoring/grafana-dashboard-*.json`, and update the ConfigMap checksum annotation if needed.
