# Multigres Operator

The **[Multigres](https://github.com/multigres/multigres) Operator** is the Kubernetes entry point for **[Multigres v0.1.0 Alpha](https://github.com/multigres/multigres/releases/tag/v0.1.0)**. It manages distributed, sharded PostgreSQL clusters across multiple failure domains (zones or regions) and provides a unified API to define the topology of your database system, handling the complex orchestration of `shards`, `cells` (failure domains), and `gateways`.

This alpha release is suitable for experimentation, demos, local testing, and feedback. It is not production-ready.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Compatibility](#compatibility)
- [How it Works](#how-it-works)
- [Configuration & Defaults](#configuration--defaults)
- [Backup & Restore](#backup--restore)
- [Observability](#observability)
- [Webhook & Certificate Management](#webhook--certificate-management)
- [GitOps & Webhook Defaults](#gitops--webhook-defaults)
- [Pool Replication & Quorum](#pool-replication--quorum)
- [Constraints & Limits](#constraints--limits-v1alpha1)
- [Further Reading](#further-reading)

## Features
- **Global Cluster Management**: Single source of truth (`MultigresCluster`) for the entire database topology.
- **Automated Sharding**: Manages `TableGroups` and `Shards` as first-class citizens.
- **Direct Pod Management**: Manages individual Pods and PVCs directly (no StatefulSets), enabling targeted decommissioning, rolling updates with primary awareness, and granular PVC lifecycle control.
- **Failover & High Availability**: Orchestrates Primary/Standby failovers across defined Cells.
- **Template System**: Define configuration once (`CoreTemplate`, `CellTemplate`, `ShardTemplate`) and reuse it across the cluster.
- **Hierarchical Defaults**: Smart override logic allowing for global defaults, namespace defaults, and granular overrides.
- **External Gateway Exposure**: Optional external gateway support via `spec.externalGateway` with configurable `externalIPs`, tracked by a `GatewayExternalReady` condition.
- **External Admin Web Exposure**: Optional external exposure for the multiadmin-web Service via `spec.externalAdminWeb`, mirroring the gateway pattern with an `AdminWebExternalReady` condition.
- **PostgreSQL Configuration**: Reference a user-created ConfigMap with `postgresql.conf` overrides via `postgresConfigRef` on shard templates. ConfigMap content changes trigger automatic rolling updates.
- **Integrated Cert Management**: Built-in self-signed certificate generation and rotation for validating webhooks, with optional support for `cert-manager`.

---

## Installation

### Prerequisites
- Kubernetes v1.25+

### Quick Start

Install the operator with built-in self-signed certificate management:

```bash
kubectl apply --server-side -f \
  https://github.com/multigres/multigres-operator/releases/download/v0.11.0/install.yaml
```

This deploys the operator into the `multigres-operator` namespace with:
- All CRDs (MultigresCluster, Cell, Shard, TableGroup, TopoServer, and templates)
- RBAC roles and bindings
- Mutating and Validating webhooks with self-signed certificates (auto-rotated)
- The operator Deployment
- Metrics endpoint

Once the operator is running, try a sample cluster:

```bash
kubectl apply -f \
  https://raw.githubusercontent.com/multigres/multigres-operator/v0.11.0/config/samples/postgres-password-secret.yaml

kubectl apply -f \
  https://raw.githubusercontent.com/multigres/multigres-operator/v0.11.0/config/samples/minimal.yaml
```

For more sample configurations, see the [samples directory](config/samples/README.md).

### Deployment Options

| Option | Description | Guide |
| :--- | :--- | :--- |
| **Self-signed certs** (default) | Zero-config TLS — operator generates and rotates its own CA. | *(Installed above)* |
| **cert-manager** | External certificate management via cert-manager. | [Cert-Manager Demo](demo/cert-manager/) |
| **Observability stack** | Full metrics, tracing, and dashboards (Prometheus, Tempo, Grafana). | [Observability Demo](demo/observability/) |

---

## Compatibility

Multigres core and Multigres Operator are versioned separately. Documented release pairings are listed in the Multigres core [release documentation](https://github.com/multigres/multigres/blob/main/RELEASE.md#compatibility).

| Release | Operator | Core | Kubernetes | PostgreSQL | Primary install path | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Initial OSS release | `v0.11.x` | `v0.1.x` | Kubernetes v1.25+ | PostgreSQL 17.x | Multigres Operator release manifests | Supported alpha |

The quick start above uses Multigres Operator `v0.11.0`, which defaults runtime images to Multigres core `v0.1.0`.

---

## How it Works

The Multigres Operator follows a **Parent/Child** architecture. You, the user, manage the **Root** resource (`MultigresCluster`) and its shared **Templates**. The operator automatically creates and reconciles all necessary child resources (`Cells`, `TableGroups`, `Shards`, `TopoServers`) to match your desired state.

### Resource Hierarchy

```ascii
[MultigresCluster] 🚀 (Root CR - User Editable)
      │
      ├── 📍 Defines [TemplateDefaults] (Cluster-wide default templates)
      │
      ├── 🌍 [GlobalTopoServer] (Child CR) ← 📄 Uses [CoreTemplate] OR inline [spec]
      │
      ├── 🤖 MultiAdmin Resources ← 📄 Uses [CoreTemplate] OR inline [spec]
      │
      ├── 💠 [Cell] (Child CR) ← 📄 Uses [CellTemplate] OR inline [spec]
      │    │
      │    ├── 🚪 MultiGateway Resources
      │    └── 📡 [LocalTopoServer] (Child CR, optional)
      │
      └── 🗃️ [TableGroup] (Child CR)
           │
           └── 📦 [Shard] (Child CR) ← 📄 Uses [ShardTemplate] OR inline [spec]
                │
                ├── 🧠 MultiOrch Resources (Deployment)
                └── 🏊 Pools (Operator-managed Pods + PVCs)

📄 [CoreTemplate] (User-editable, scoped config)
   ├── globalTopoServer
   └── multiadmin

📄 [CellTemplate] (User-editable, scoped config)
   ├── multigateway
   └── localTopoServer (optional)

📄 [ShardTemplate] (User-editable, scoped config)
   ├── multiorch
   └── pools (postgres + multipooler)
```

**Important**:
*   **Only** `MultigresCluster`, `CoreTemplate`, `CellTemplate`, and `ShardTemplate` are meant to be edited by users.
*   Child resources (`Cell`, `TableGroup`, `Shard`, `TopoServer`) are **Read-Only**. Any manual changes to them will be immediately reverted by the operator to ensure the system stays in sync with the root configuration.

---

## Configuration & Defaults

The operator uses a **4-Level Override Chain** to resolve configuration for every component. This allows you to keep your `MultigresCluster` spec clean while maintaining full control when needed.

### 1. The Default Hierarchy

When determining the configuration for a component (e.g., a Shard), the operator looks for configuration in this order:

1.  **Inline Spec / Explicit Template Ref**: Defined directly on the component in the `MultigresCluster` YAML.
2.  **Cluster-Level Template Default**: Defined in `spec.templateDefaults` of the `MultigresCluster`.
3.  **Namespace-Level Default**: A template of the correct kind (e.g., `ShardTemplate`) named `"default"` in the same namespace.
4.  **Operator Hardcoded Defaults**: Fallback values built into the operator Webhook.

### 2. Templates and Overrides

Templates allow you to define standard configurations (e.g., "Standard High-Availability Cell"). You can then apply specific **overrides** on top of a template.

**Example: Using a Template with Overrides**
```yaml
spec:
  cells:
    - name: "us-east-1a"
      cellTemplate: "standard-ha-cell" # <--- Uses the template
      overrides:                       # <--- Patches specific fields
        multigateway:
          replicas: 5                  # <--- Overrides only the replica count
```

**Note on Overrides**: When using `overrides`, you must provide the complete struct for the section you are overriding if it's a pointer. For specific fields like resources, it's safer to ensure you provide the full context if the merge behavior isn't granular enough for your needs (currently, the resolver performs a deep merge).

### 3. Template Update Behavior

> [!WARNING]
> When a template (`CoreTemplate`, `CellTemplate`, or `ShardTemplate`) is updated, **all clusters using that template are reconciled immediately**. This means changes to a shared template propagate to every referencing cluster at once.

For production environments where you want controlled rollouts, consider **versioning templates by name**:

```yaml
# Instead of editing "standard-shard" in-place...
apiVersion: multigres.com/v1alpha1
kind: ShardTemplate
metadata:
  name: standard-shard-v2   # <--- New version = new resource
spec:
  # ... updated configuration
```

Then update each cluster's `templateRef` individually when ready:
```yaml
spec:
  templateDefaults:
    shardTemplate: "standard-shard-v2"  # <--- Opt-in to the new version
```

> [!NOTE]
> Avoid using `default`-named templates (the namespace-level fallback) in production if you need controlled rollouts. They cannot be versioned since any cluster without an explicit template reference will automatically use whichever template is named `default`.
>
> This mechanism may change in future versions. See [Template Propagation](docs/development/template-propagation.md) for details on planned improvements.

---

## Backup & Restore

The operator integrates **pgBackRest** for automated backups, WAL archiving, and point-in-time recovery (PITR). Two storage backends are supported: **S3** (recommended for production and multi-cell clusters) and **Filesystem** (PVC-based, for development/single-node). Backup configuration is fully declarative and propagates from the cluster level down to individual shards.

Key features:
- **Replica-based backups** — backups run on a replica to avoid impacting the primary
- **S3 credential options** — IRSA, static credentials, or EC2 instance metadata
- **Auto-generated TLS** — pgBackRest inter-node TLS is managed automatically, with optional cert-manager support

> [!WARNING]
> Filesystem backups are **cell-local**. Cross-cell failover cannot restore from another cell's backup. Use S3 for multi-cell clusters.

📖 **Full documentation:** [Backup & Restore Guide](docs/backup-restore.md)

## Observability

The operator ships with built-in support for **metrics**, **alerting**, **distributed tracing**, and **structured logging**.

- **Metrics** — Prometheus endpoint with 8 operator-specific metrics + controller-runtime framework metrics
- **Alerts** — 10 pre-configured PrometheusRule alerts with dedicated runbooks ([view runbooks](docs/monitoring/runbooks/))
- **Grafana Dashboards** — Operator dashboard, per-cluster topology dashboard, and data-plane operations dashboard
- **Distributed Tracing** — OpenTelemetry OTLP support, disabled by default, zero overhead when off
- **Structured Logging** — JSON logging with automatic `trace_id`/`span_id` injection for log-trace correlation

📖 **Full documentation:** [Observability Guide](docs/observability.md) · [Observability Demo](demo/observability/)

---

## Webhook & Certificate Management

The operator includes a Mutating and Validating Webhook to enforce defaults and data integrity.

### Automatic Certificate Management (Default)
By default, the operator manages its own TLS certificates using the generic `pkg/cert` module. This implements a **Split-Secret PKI** architecture:

1.  **Bootstrap**: On startup, the cert rotator generates a self-signed Root CA (ECDSA P-256) and a Server Certificate, storing them in two separate Kubernetes Secrets.
2.  **CA Bundle Injection**: A post-reconcile hook automatically patches the `MutatingWebhookConfiguration` and `ValidatingWebhookConfiguration` with the CA bundle.
3.  **Rotation**: A background loop checks certificates hourly. Certs nearing expiry (or signed by a rotated CA) are automatically renewed without downtime.
4.  **Owner References**: Both secrets are owned by the operator Deployment, so they are garbage-collected on uninstall.

### Using external Cert-Manager
If you prefer to use `cert-manager` or another external tool, deploy using the cert-manager overlay (`install-certmanager.yaml`). This overlay:

1.  Creates a `Certificate` and `ClusterIssuer` resource for cert-manager to manage.
2.  Mounts the cert-manager-provisioned secret to `/var/run/secrets/webhook` so certificates exist on disk at startup.

The operator **automatically detects** the certificate management strategy on startup:
- If certificates already exist on disk and the operator did not previously manage them (no cert-strategy annotation), it assumes an external provider (e.g. cert-manager) and skips internal rotation.
- If no certificates exist on disk, or the operator previously annotated the ValidatingWebhookConfiguration, internal certificate rotation is enabled.

📖 **Cert-Manager walkthrough:** [Cert-Manager Demo](demo/cert-manager/)

---

## GitOps & Webhook Defaults

The operator's Mutating Webhook materialises all defaults (images, replicas, resources, backup config, etc.) directly into the `MultigresCluster` spec stored in etcd. This means `kubectl get multigrescluster -o yaml` always shows the full effective configuration — no hidden in-memory defaults.

Some fields (like cell assignments on shards) are intentionally kept dynamic and resolved at reconcile time. The resolved values are visible on child CRs (`Shard`, `TableGroup`).

If you use GitOps tooling (ArgoCD, Flux), the webhook-materialised fields can cause diffs between your Git manifests and the live state. The documentation covers recommended mitigations.

📖 **Full documentation:** [Webhook Defaults & GitOps Guide](docs/gitops-and-webhook-defaults.md)

---

## Pool Replication & Quorum

Multigres uses a configurable **durability policy** to control synchronous replication quorum. The default policy is `AT_LEAST_2`, which requires every write to be acknowledged by at least 2 nodes (the primary + 1 synchronous standby). For multi-AZ clusters, `MULTI_CELL_AT_LEAST_2` enforces cross-zone quorum. This has implications for how many replicas you should run per cell in `readWrite` pools.

| Replicas per Cell | Configuration | Rolling Upgrade Behavior |
| :--- | :--- | :--- |
| **1** | 1 pod (primary only, no standbys) | **Downtime during upgrades.** No standby to maintain quorum. |
| **2** | 1 primary + 1 standby | **Downtime during upgrades.** Draining the standby leaves zero synchronous standbys, violating `AT_LEAST_2`. Upstream multigres rejects the `UpdateSynchronousStandbyList REMOVE` because it would empty the synchronous standby list. |
| **3** (recommended) | 1 primary + 2 standbys | **Zero-downtime upgrades.** One standby can be drained while the other maintains quorum. |

Pools default to `replicasPerCell: 1`. `AT_LEAST_2` requires 2 total poolers; `MULTI_CELL_AT_LEAST_2` requires poolers in 2 cells. For high availability, use at least 3 total poolers so the policy remains achievable while one pooler is unavailable.

📖 **Full documentation:** [Durability Policy](docs/durability-policy.md)

---

## Constraints & Limits (v1alpha1)

Please be aware of the following constraints in the current version:

*   **Database Limit**: Only **1** database is supported per cluster. It must be named `postgres` and marked `default: true`.
*   **Shard Naming**: Shards currently must be named `0-inf` - this is a limitation of the current implementation of Multigres.
*   **Naming Lengths**:
    *   **TableGroup Names**: If the combined name (`cluster-db-tg`) exceeds **28 characters**, the operator automatically hashes the database and tablegroup names to ensure that the resulting child resource names (Shards, Pods, PVCs) stay within Kubernetes limits (63 chars).
    *   **Cluster Name**: Recommended to be under **20 characters** to ensure that even with hashing, suffixes fit comfortably.
*   **Immutable Fields**: Some fields like `zone` and `region` in Cell definitions are immutable after creation.
*   **Append-Only Pools and Cells**: Pools and cells cannot be renamed or removed from a cluster. This prevents orphaned pods and stale etcd registrations.

---

## Further Reading

| Resource | Description |
| :--- | :--- |
| [Operator Capability Levels](docs/operator-capability-levels.md) | Maturity assessment against the [Operator Framework capability model](https://operatorframework.io/operator-capabilities/) |
| [Webhook Defaults & GitOps](docs/gitops-and-webhook-defaults.md) | How the webhook materialises defaults, dynamic cell resolution, and GitOps compatibility |
| [Durability Policy](docs/durability-policy.md) | Configurable replication quorum: `AT_LEAST_2` (default) and `MULTI_CELL_AT_LEAST_2` for cross-AZ durability |
| [External Admin Web](docs/external-admin-web.md) | External exposure for the multiadmin-web Service |
| [PostgreSQL Configuration](docs/postgresql-configuration.md) | Custom `postgresql.conf` overrides via ConfigMap reference |
| [Storage Management](docs/storage.md) | PVC deletion policies (Retain/Delete) and volume expansion |
| [Configuration Reference](docs/configuration.md) | Operator flags, environment variables, and logging |
| [Demos](demo/) | Guided walkthroughs (webhook, cert-manager, observability) |
| [Developer Documentation](docs/development/) | Internal architecture, controller patterns, caching strategy |
| [Contributing](CONTRIBUTING.md) | Development setup, local Kind deployment, code style |
| [Changelog](CHANGELOG.md) | Release history |
| [Multigres v0.1.0 Alpha](https://github.com/multigres/multigres/releases/tag/v0.1.0) | Core alpha release paired with this operator release |
| [Multigres Discussions](https://github.com/multigres/multigres/discussions) | Feedback, questions, and community discussion |
| [Operator Releases](https://github.com/multigres/multigres-operator/releases) | Published operator versions and install manifests |
