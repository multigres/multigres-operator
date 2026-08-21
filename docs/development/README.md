# Developer Documentation

Internal documentation for contributors and operators. For user-facing documentation, see the [README](../../README.md).

## Architecture & Design

| Document | Description |
|:---|:---|
| [API Design (v1alpha1)](api-design/multigres-operator-api-v1alpha1-design.md) | CRD structure, field definitions, templates, overrides, and lifecycle management |
| [Admission Controller & Defaults](admission-controller-and-defaults.md) | Hybrid admission control strategy: CRD schema, CEL validation, webhook mutating/validating |
| [Pod Management Architecture](pod-management-architecture.md) | Direct pod management design, drain state machine, rolling updates |
| [Pod Management Design](pod-management-design.md) | Detailed design for transitioning from StatefulSets to direct pod management |

## Implementation Details

| Document | Description |
|:---|:---|
| [Controller Patterns](controller-patterns.md) | Performance tuning (QPS, workers), event filtering (GenerationChangedPredicate), SSA idempotency |
| [Phase Lifecycle](phase-lifecycle.md) | Phase values, computation per resource, crash-loop detection, periodic re-reconciliation |
| [Caching Strategy](caching-strategy.md) | Informer filtering, secret access patterns (Option A/B/C), IRSA |
| [Certificate Management](certificate-management.md) | `pkg/cert` module, webhook TLS, pgBackRest TLS, consumer extensibility |
| [Naming Strategy](naming-strategy.md) | Hierarchical names, hash collision prevention, character constraints |
| [PVC Lifecycle](pvc-lifecycle.md) | Deletion policy, hierarchical merging, caveats for developers |
| [Backup Architecture](backup-architecture.md) | Shared PVC design rationale, replica selection logic, cell isolation |
| [Template Propagation](template-propagation.md) | Template update behavior, proposed update policy (`Auto`/`Manual`/`Window`) |
| [Cell Topology](cell-topology.md) | Local TopoServer design, failure domains, zone/region scheduling implementation |
| [Shared Topology Roots](shared-topology.md) | Canonical cluster-scoped global and cell topology roots for sharing one physical etcd cluster |
| [PostgreSQL Image Strategy](postgresql-image-strategy.md) | Bundled pgctld image, future custom image support plan |
| [Observability Internals](observability-internals.md) | Metrics registration, tracing lifecycle, traceparent bridge, log-trace correlation, alerts & dashboards |
| [Known Behaviors](known-behaviors.md) | Documented quirks (e.g., "configured" loop with CSA + mutating webhooks) |

## Testing

| Document | Description |
|:---|:---|
| [Upstream Test Patterns](upstream-test-patterns.md) | Research on upstream Multigres test patterns for informing E2E tests |
