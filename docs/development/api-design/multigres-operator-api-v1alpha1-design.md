# MultigresCluster API v1alpha1

This is the API design for the Multigres Operator, based on discussions with the Multigres team. The design is approved and implemented — see [pod-management-architecture.md](../pod-management-architecture.md) for operational details.

> [!NOTE]
> This document is kept as a living reference. Sections are updated as the implementation evolves as a best effort. The Go types in `api/v1alpha1/` are the authoritative source of truth for field names, types, and validation rules. This document provides architectural context and rationale.

## Summary

This proposal defines the `v1alpha1` API for the Multigres Operator. The design is centered on a root `MultigresCluster` resource that acts as the single source of truth, supported by three specifically scoped template resources:

1.  **`MultigresCluster`**: The root resource defining the desired state (intent) of the entire cluster.
2.  **`CoreTemplate`**: A reusable, namespaced resource for defining standard configurations for core control plane components (`GlobalTopoServer` and `MultiAdmin`).
3.  **`CellTemplate`**: A reusable, namespaced resource for defining standard configurations for Cell-level components (`MultiGateway` and optionally `LocalTopoServer`).
4.  **`ShardTemplate`**: A reusable, namespaced resource for defining standard configurations for Shard-level components (`MultiOrch` and `Pools`).

All other resources (`TopoServer`, `Cell`, `TableGroup`, `Shard`) should be considered read-only child CRs owned by the `MultigresCluster`. These child CRs reflect the *realized state* of the system and are managed by their own dedicated controllers. If the user edits them directly, they will get immediately reverted by the parent CR.

### Temporary Constraints (v1alpha1)

The API design described below is **forward-looking** and supports multiple logical databases and custom TableGroup names.

However, the **current implementation (v1alpha1)** of the Multigres Gateway has specific temporary limitations:
* **Routing Hardcode:** It routes all traffic to a hardcoded TableGroup named `"default"`.
* **Database Ignorance:** It currently ignores the specific database name requested by the client connection string.

**Implication:** To prevent users from creating "zombie" resources that the Gateway cannot route to, the v1alpha1 Operator will enforce a **Single-Database (System Catalog), Single-TableGroup** topology via validation rules. These constraints will be relaxed in future versions.

## Motivation

Managing a distributed, sharded database system across multiple failure domains is inherently complex. Previous iterations explored monolithic CRDs (too complex), purely composable CRDs (too manual), and "managed" flags (unstable state).

The formalized parent/child model addresses these by ensuring:

  * **Separation of Concerns:** Splitting logic into child CRs results in simple, specialized controllers.
  * **Single Source of Truth:** The `MultigresCluster` is the only editable entry point for cluster topology, preventing conflicting states.
  * **Scoped Reusability:** By splitting templates into `CoreTemplate`, `CellTemplate`, and `ShardTemplate`, we provide clear, reusable configurations.
  * **Explicit Topology:** Removing "shard count" partitioning in favor of explicit shard list definitions provides deterministic control over where exactly data lives.
  * **Consistent Override Chain:** All components follow a predictable 4-level override chain, providing maximum flexibility while maintaining a clear and consistent API pattern.

## Proposal: API Architecture and Resource Topology

  * **Core Components:** `MultiAdmin` and `GlobalTopoServer` are defined as top-level fields in the `MultigresCluster`. Each can be configured inline or by referencing a `CoreTemplate`.
  * **Cells:** Explicitly defined in the root CR; can be specified inline or via a `CellTemplate`.
  * **Databases:** Follows a strict physical hierarchy: `Database` -\> `TableGroup` -\> `Shard`.
  * **Shards:** Can be specified inline or via a `ShardTemplate`.
  * **Multiorch** Located at the **Shard** level to provide dedicated orchestration.

```ascii
[MultigresCluster] 🚀 (Root CR - user-editable)
      │
      ├── 📍 Defines [TemplateDefaults] (Cluster-wide default templates)
      │
      ├── 🌍 [GlobalTopoServer] (Child CR) ← 📄 Uses [CoreTemplate] OR inline [spec]
      │
      ├── 🤖 MultiAdmin Resources ← 📄 Uses [CoreTemplate] OR inline [spec]
      │
      ├── 🌐 MultiAdminWeb Resources ← � Uses [CoreTemplate] OR inline [spec]
      │
      ├── �💠 [Cell] (Child CR) ← 📄 Uses [CellTemplate] OR inline [spec]
      │    │
      │    ├── 🚪 MultiGateway Resources (Deployment + Service)
      │    └── 📡 [LocalTopoServer] (Child CR, optional)
      │
      └── 🗃️ [TableGroup] (Child CR)
           │
           └── 📦 [Shard] (Child CR) ← 📄 Uses [ShardTemplate] OR inline [spec]
                │
                ├── 🧠 MultiOrch Resources (Deployment per cell)
                └── 🏊 Pools (per cell):
                     ├── Pod-0  ← operator-managed, owns data PVC-0
                     ├── Pod-1  ← operator-managed, owns data PVC-1
                     ├── PVC-0, PVC-1  ← operator-managed (data)
                     ├── PodDisruptionBudget  ← per pool per cell
                     ├── Headless Service  ← for pod DNS resolution
                     ├── ConfigMap  ← spec-hash for drift detection
                     └── Backup PVC (shared, filesystem only)

📄 [CoreTemplate] (User-editable, scoped config)
   ├── globalTopoServer
   ├── multiadmin
   └── multiadminWeb

📄 [CellTemplate] (User-editable, scoped config)
   ├── multigateway
   └── localTopoServer (optional)

📄 [ShardTemplate] (User-editable, scoped config)
   ├── multiorch
   └── pools (postgres + multipooler)
```

> [!IMPORTANT]
> **Pool pods are managed directly by the operator, not via StatefulSets.** The operator creates Pods and PVCs explicitly, handling sequential provisioning, rolling updates with primary awareness, and a drain state machine for graceful scale-down. See [pod-management-architecture.md](../pod-management-architecture.md) for details.

## Design Details: API Specification

### User Managed CR: MultigresCluster

  * This CR and the three scoped templates (`CoreTemplate`, `CellTemplate`, `ShardTemplate`) are the *only* editable entries for the end-user.
    * All other child CRs will be owned by this top-level CR. Any manual changes to those child CRs will be immediately reverted by the `MultigresCluster` cluster controller.
  * All component configurations (`globalTopoServer`, `multiadmin`, `cells`, `shards`) follow a consistent pattern: they can be defined via an inline `spec` or by referencing a template (`templateRef`). Providing both is a validation error.
    * **4-level Override Chain:** All components use the following 4-level precedence chain for configuration:
      1.  **Component-Level Definition:** An inline `spec` or an explicit `templateRef` on the component itself.
      2.  **Defaults in MultigresCluster spec:** The corresponding template defined in `spec.templateDefaults` (e.g., `templateDefaults.coreTemplate` or `templateDefaults.cellTemplate`).
      3.  **Namespace-Level Default:** A template of the correct kind (e.g., `CoreTemplate`) named `default` in the same namespace.
      4.  **Operator Hardcoded Defaults:** A final fallback applied by the operator's admission webhook.
    * **Atomic Overrides:** To ensure safety, highly interdependent fields are grouped (e.g., `resources`, `storage`). When using `overrides`, you must replace the *entire* group, not just individual sub-fields (e.g., you cannot override just `cpu limit` without also providing `cpu request`).
  * Images are defined globally to avoid the danger of running multiple incongruent versions at once. This implies the operator handles upgrades.
  * The `MultigresCluster` does not create its grandchildren directly; for example, shard configuration is passed to the `TableGroup` CR, which then creates its own children `Shard` CRs.

> Note: This document prioritizes API specification accuracy. The child CR and template examples are for illustrative purposes and may not perfectly reflect the exact output generated by the provided `MultigresCluster` configuration.

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: example-cluster  # max 25 characters (CEL enforced)
  namespace: example
spec:
  # ----------------------------------------------------------------
  # Global Images Cluster Configuration
  # ----------------------------------------------------------------
  # Images are defined globally to ensure version consistency across
  # all cells and shards.
  # NOTE: Perhaps one day we can template these as well.
  images:
    imagePullPolicy: "IfNotPresent"
    imagePullSecrets:
      - name: "my-registry-secret"
    multigateway: "multigres/multigres:latest"
    multiorch: "multigres/multigres:latest"
    multipooler: "multigres/multigres:latest"
    multiadmin: "multigres/multigres:latest"
    multiadminWeb: "multigres/multigres:latest"  # MultiAdmin Web UI
    postgres: "postgres:15.3"

  # ----------------------------------------------------------------
  # Cluster-Level Template Defaults
  # ----------------------------------------------------------------
  # These are the default templates to use for any component that
  # does not have an inline 'spec' or an explicit 'templateRef'.
  # These are optional.
  # If these defaults are not specified, the controller will pick whichever is
  # named 'default' in the namespace, or use the controller defaults.
  templateDefaults:
    coreTemplate: "cluster-wide-core"
    cellTemplate: "cluster-wide-cell"
    shardTemplate: "cluster-wide-shard"

  # ----------------------------------------------------------------
  # Global Components
  # ----------------------------------------------------------------

  # Global TopoServer is a singleton. It follows the 4-level override chain.
  # It supports EITHER a 'etcd' OR 'external' OR 'templateRef'.
  globalTopoServer:
    # --- OPTION 1: Inline Etcd Spec ---
    etcd:
      image: "quay.io/coreos/etcd:v3.5"
      rootPath: "/multigres/global" # Optional: defaults to /multigres/global
      replicas: 3
      storage:
        size: "10Gi"
        class: "standard-gp3"
      resources:
        requests:
          cpu: "500m"
          memory: "1Gi"
        limits:
          cpu: "1"
          memory: "2Gi"

    # --- OPTION 2: Inline External Spec ---
    # external:
    #   endpoints:
    #     - "https://etcd-1.infra.local:2379"
    #     - "https://etcd-2.infra.local:2379"
    #   caSecret: "etcd-ca-secret"
    #   clientCertSecret: "etcd-client-cert-secret"

    # --- OPTION 3: Explicit Template Reference ---
    # templateRef: "my-explicit-core-template"

  # MultiAdmin is a singleton. It follows the 4-level override chain.
  # It supports EITHER a 'spec' OR a 'templateRef'.
  multiadmin:
    # --- OPTION 1: Inline Spec ---
    spec:
      replicas: 2
      resources:
        requests:
          cpu: "200m"
          memory: "256Mi"
        limits:
          cpu: "500m"
          memory: "512Mi"
      # Affinity can be configured too by user if desired
      # affinity:
      #   podAntiAffinity:
      #     preferredDuringSchedulingIgnoredDuringExecution:
      #     - weight: 100
      #       podAffinityTerm:
      #         labelSelector:
      #           matchLabels:
      #             app.kubernetes.io/component: multiadmin
      #         topologyKey: "kubernetes.io/hostname"

    # --- OPTION 2: Explicit Template Reference ---
    # templateRef: "my-explicit-core-template"

  # MultiAdminWeb is a singleton. It follows the same pattern as MultiAdmin.
  # It deploys the MultiAdmin Web UI component.
  # multiadminWeb:
  #   spec:
  #     replicas: 1
  #     resources:
  #       requests:
  #         cpu: "100m"
  #         memory: "128Mi"
  #       limits:
  #         cpu: "200m"
  #         memory: "256Mi"

  # ----------------------------------------------------------------
  # Cells Configuration
  # ----------------------------------------------------------------
  cells:
    # --- CELL 1: Using an Explicit Template ---
    - name: "us-east-1a"
      # Location must be strictly one of: 'zone' OR 'region'
      zone: "us-east-1a"
      # region: "us-east-1" # use instead of zone

      cellTemplate: "standard-cell-ha"

      # Optional overrides applied ON TOP of the template.
      overrides:
        multigateway:
           replicas: 3

    # --- CELL 2: Using Inline Spec (No Template) ---
    - name: "us-east-1b"
      zone: "us-east-1b"
      spec:
         multigateway:
           replicas: 2
           resources:
             requests:
               cpu: "500m"
               memory: "1Gi"
             limits:
               cpu: "1"
               memory: "2Gi"
         # --- Optional Local TopoServer ---
         # If omitted, this cell uses the GlobalTopoServer.
         # localTopoServer:
         #   etcd:
         #      image: "quay.io/coreos/etcd:v3.5"
         #      replicas: 3
         #      storage:
         #        size: "5Gi"
         #        class: "standard-gp3"

    # --- CELL 3: Using Cluster Default Template ---
    - name: "us-east-1c"
      zone: "us-east-1c"
      # 'spec' and 'cellTemplate' are omitted.
      # This will use 'spec.templateDefaults.cellTemplate' ("cluster-wide-cell")
      # (if that is not set, it will look for 'CellTemplate' named 'default',
      # and if that is not found, it will use the webhook default).

  # ----------------------------------------------------------------
  # Database Topology (Database -> TableGroup -> Shard)
  # ----------------------------------------------------------------
  databases:
    # --- EXAMPLE 1: Configuring the System Default Database ---
    # This entry targets the system-level database created during bootstrap.
    # We mark it as 'default: true' to apply this configuration to the
    # bootstrap resources (System Catalog).
    - name: "postgres"
      default: true
      # durabilityPolicy: override the cluster-level durability policy for this database (optional)
      # backup: override the global backup for this database (optional)
      tablegroups:
        - name: "default"
          default: true
          # backup: override the database backup for this tablegroup (optional)
          # pvcDeletionPolicy: override the cluster policy for this tablegroup (optional)
          shards:
            - name: "0-inf"  # v1alpha1: must be exactly "0-inf" (CEL enforced)
              # define resources for the system default shard
              shardTemplate: "standard-shard-ha"

    # --- EXAMPLE 2: A User Database (Future/Logical Representation) ---
    # [NOTE: v1alpha1 Constraint]
    # While the API allows this definition, v1alpha1 enforces a single-database
    # topology ("postgres"). Multiple database definitions are currently prevented
    # by validation rules until Gateway routing support is added.
    - name: "production_db"
      # default: false (Implicit) - This creates a new logical database
      tablegroups:
        # The default TableGroup for this specific database.
        - name: "main_unsharded"
          default: true
          shards:
            - name: "0"
              spec:
                multiorch:
                  resources:
                     requests:
                       cpu: "100m"
                       memory: "128Mi"
                     limits:
                       cpu: "200m"
                       memory: "256Mi"
                pools:
                  main-app:
                    type: "readWrite"
                    cells:
                      - "us-east-1b"
                    replicasPerCell: 2
                    storage:
                       size: "100Gi"
                       class: "standard-gp3"
                    postgres:
                       resources:
                          requests:
                            cpu: "2"
                            memory: "4Gi"
                          limits:
                            cpu: "4"
                            memory: "8Gi"
                    multipooler:
                       resources:
                          requests:
                            cpu: "500m"
                            memory: "1Gi"
                          limits:
                            cpu: "1"
                            memory: "2Gi"

        # A custom sharded group for high-volume data
        - name: "orders_tg"
          # default: false (Implicit)
          shards:
            # --- SHARD 0: Using Inline Spec (No Template) ---
            - name: "0"
              spec:
                multiorch:
                  resources:
                     requests:
                       cpu: "100m"
                       memory: "128Mi"
                     limits:
                       cpu: "200m"
                       memory: "256Mi"
                pools:
                  main-app:
                    type: "readWrite"
                    cells:
                      - "us-east-1b"
                    replicasPerCell: 2
                    storage:
                       size: "100Gi"
                       class: "standard-gp3"
                    postgres:
                       resources:
                          requests:
                            cpu: "2"
                            memory: "4Gi"
                          limits:
                            cpu: "4"
                            memory: "8Gi"
                    multipooler:
                       resources:
                          requests:
                            cpu: "500m"
                            memory: "1Gi"
                          limits:
                            cpu: "1"
                            memory: "2Gi"

            # --- SHARD 1: Using an Explicit Template ---
            - name: "1"
              shardTemplate: "standard-shard-ha"
              # Overrides are crucial here to pin pools to specific cells
              # if the template uses generic cell names.
              overrides:
                 # MAP STRUCTURE: Keyed by pool name for safe targeting.
                 pools:
                   # Overriding the pool named 'main-app' from the template
                   # to ensure it lives in a specific cell for this shard.
                   main-app:
                     cells:
                       - "us-east-1a"

            # --- SHARD 2: Using Cluster Default Template ---
            - name: "2"
              # 'spec' and 'shardTemplate' are omitted.
              # This will use 'spec.templateDefaults.shardTemplate' ("cluster-wide-shard")
              # (or 'default' template, or webhook default).
              # We still must provide overrides for required fields.
              overrides:
                 pools:
                   main-app:
                     cells:
                       - "us-east-1c"

  # ----------------------------------------------------------------
  # PVC Lifecycle Management
  # ----------------------------------------------------------------
  # Controls what happens to PVCs when the cluster is deleted or scaled down.
  # Defaults to Retain/Delete for production safety.
  pvcDeletionPolicy:
    whenDeleted: "Retain"  # or "Delete"
    whenScaled: "Delete"   # or "Retain"

  # ----------------------------------------------------------------
  # Backup Configuration
  # ----------------------------------------------------------------
  # Global backup settings, overridable at database, tablegroup, or shard level.
  backup:
    type: "filesystem"  # or "s3"
    filesystem:
      path: "/backups"
      storage:
        size: "10Gi"
        class: "standard-rwx"
    # s3:
    #   bucket: "my-backups"
    #   region: "us-east-1"
    #   endpoint: "http://minio:9000"  # optional, for S3-compatible stores
    #   keyPrefix: "my-cluster"        # optional
    #   useEnvCredentials: true         # optional, for static creds from credentialsSecret
    #   credentialsSecret: "aws-creds" # optional
    #   serviceAccountName: "multigres-backup" # optional, for IRSA (mutually exclusive with credentialsSecret/useEnvCredentials)
    pgbackrestTLS:
      secretName: ""  # empty = auto-generated by operator; set to use cert-manager

  # ----------------------------------------------------------------
  # Observability (OpenTelemetry)
  # ----------------------------------------------------------------
  # Configures OTEL for data-plane components (multipooler, multiorch, etc.).
  # When nil, components inherit the operator's own OTEL env vars.
  # observability:
  #   otlpEndpoint: "http://otel-collector:4318"  # or "disabled" to turn off
  #   otlpProtocol: "http/protobuf"  # or "grpc"
  #   tracesExporter: "otlp"  # otlp, none, console
  #   metricsExporter: "otlp"
  #   logsExporter: "otlp"
  #   metricExportInterval: "30000"  # milliseconds
  #   metricsTemporality: "cumulative"  # or "delta"
  #   histogramAggregation: "base2_exponential_bucket_histogram"
  #   tracesSampler: "parentbased_traceidratio"
  #   samplingConfigRef:
  #     name: "otel-sampling-config"  # ConfigMap name
  #     key: "sampling-config.yaml"

  # ----------------------------------------------------------------
  # Topology Pruning
  # ----------------------------------------------------------------
  # Controls whether the operator prunes stale topology entries (cells,
  # databases) that no longer match the MultigresCluster spec.
  # Enabled by default (nil or absent means enabled).
  # topologyPruning:
  #   enabled: true

  # ----------------------------------------------------------------
  # Durability Policy
  # ----------------------------------------------------------------
  # Controls how multiorch enforces synchronous replication acknowledgment
  # during failover. Written to the database entry in the topology (etcd).
  # This is a per-database setting; override per-database via databases[].durabilityPolicy.
  # Defaults to "AT_LEAST_2" if not set.
  #
  # Currently supported (upstream multiorch):
  #   "AT_LEAST_2"            - any 2 nodes must acknowledge writes (single-cell quorum)
  #   "MULTI_CELL_AT_LEAST_2" - 2 nodes from different cells must acknowledge (cross-AZ quorum)
  #
  # Additional user-defined policies may be supported in future upstream releases.
  # durabilityPolicy: "AT_LEAST_2"

  # ----------------------------------------------------------------
  # External Gateway
  # ----------------------------------------------------------------
  # Controls external exposure of the global multigateway Service.
  # When nil or enabled: false, the Service remains ClusterIP-only.
  # externalGateway:
  #   enabled: true
  #   externalIPs:
  #     - "203.0.113.10"
  #     - "203.0.113.11"
  #   annotations:
  #     team.example.com/owner: "platform"

status:
  observedGeneration: 1
  phase: "Healthy"  # Initializing, Progressing, Healthy, Degraded, Unknown
  conditions:
    - type: Available
      status: "True"
      lastTransitionTime: "2025-11-07T12:00:00Z"
      message: "All components are available."
  # Aggregated status for high-level visibility
  cells:
    us-east-1a:
      ready: True
      gatewayReplicas: 3
    us-east-1b:
      ready: True
      gatewayReplicas: 2
    us-east-1c:
      ready: True
      gatewayReplicas: 2 # (Assuming default is 2)
  databases:
    production_db:
      readyShards: 3
      totalShards: 3
  # External gateway status (present when spec.externalGateway is set and enabled)
  # gateway:
  #   externalEndpoint: "203.0.113.10"
  # Tracks which templates were resolved (for template-change enqueuing)
  resolvedTemplates:
    coreTemplates: ["cluster-wide-core"]
    cellTemplates: ["cluster-wide-cell", "standard-cell-ha"]
    shardTemplates: ["cluster-wide-shard", "standard-shard-ha"]
```

### User Managed CR: CoreTemplate

  * This CR is NOT a child resource. It is purely a configuration object.
  * It is namespaced to support RBAC scoping (e.g., platform team owns templates, dev team owns clusters).
  * It defines the shape of the cluster's core control plane, which comprises Global Topo Server and MultiAdmin. A `CoreTemplate` can contain definitions for *both* components. When a component (e.g., `globalTopoServer`) references this template, the controller will extract the relevant section.
  * **Note:** To prevent recursive definitions, `CoreTemplate` fields are strictly defined inline and cannot reference other templates. `multiadmin` uses a flattened spec structure here, unlike the `MultigresCluster` parent which nests it under `spec`.

```yaml
apiVersion: multigres.com/v1alpha1
kind: CoreTemplate
metadata:
  name: "standard-core-ha"
  namespace: example
spec:
  # Defines the Global TopoServer component.
  # RESTRICTION: Templates only support Managed Etcd configurations.
  # External topology servers must be defined in the Cluster CR directly.
  globalTopoServer:
    etcd:
      image: "quay.io/coreos/etcd:v3.5"
      rootPath: "/multigres/global"
      replicas: 3
      storage:
        size: "10Gi"
        class: "standard-gp3"
      resources:
        requests:
          cpu: "500m"
          memory: "1Gi"
        limits:
          cpu: "1"
          memory: "2Gi"

  # Defines the MultiAdmin component.
  multiadmin:
    replicas: 2
    resources:
      requests:
        cpu: "200m"
        memory: "256Mi"
      limits:
        cpu: "500m"
        memory: "512Mi"
    # Affinity can be configured too by user if desired
    # affinity:
    #   podAntiAffinity:
    #     preferredDuringSchedulingIgnoredDuringExecution:
    #     - weight: 100
    #       podAffinityTerm:
    #         labelSelector:
    #           matchLabels:
    #             app.kubernetes.io/component: multiadmin
    #         topologyKey: "kubernetes.io/hostname"
```

### User Managed CR: CellTemplate

  * This CR is NOT a child resource. It is purely a configuration object.
  * It is namespaced to support RBAC scoping (e.g., platform team owns templates, dev team owns clusters).
  * When created, templates are not reconciled until referenced by a `MultigresCluster`.

```yaml
apiVersion: multigres.com/v1alpha1
kind: CellTemplate
metadata:
  name: "standard-cell-ha"
  namespace: example
spec:
  # Template strictly defines only Cell-scoped components.
  multigateway:
    replicas: 2
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
    affinity:
      podAntiAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
             labelSelector:
               matchLabels:
                 app.kubernetes.io/component: multigateway
             topologyKey: "kubernetes.io/hostname"

  # --- OPTIONAL: Local TopoServer ---
  # Define if cells using this template should have their own dedicated etcd.
  # If omitted, cells use the GlobalTopoServer by default.
  #
  # localTopoServer:
  #   etcd:
  #     image: "quay.io/coreos/etcd:v3.5"
  #     rootPath: "/multigres/us-east-1a"
  #     replicas: 3
  #     storage:
  #       class: "standard-gp3"
  #       size: "5Gi"
  #     resources:
  #       requests:
  #         cpu: "500m"
  #         memory: "1Gi"
  #       limits:
  #         cpu: "1"
  #         memory: "2Gi"
```

### User Managed CR: ShardTemplate

* This CR is NOT a child resource. It is purely a configuration object.
* It is namespaced to support RBAC scoping (e.g., platform team owns templates, dev team owns clusters).
* When created, templates are not reconciled until referenced by a `MultigresCluster`.
* Similar to `CellTemplate`, this is a pure configuration object. It defines the "shape" of a shard: its orchestration and its data pools.
* **`pools` is a MAP, keyed by the pool name:** Using a map structure ensures that overrides are resilient to changes in the underlying template; unlike list arrays where inserting or reordering items shifts indices—potentially causing an override targeting "index 1" to accidentally apply to the wrong pool if the template order changes—keyed maps guarantee that an override for a specific pool (e.g., `main-app`) always targets that exact logical resource, regardless of how other pools are added or organized in the template.
* **Pools and cells are append-only:** Once a pool or cell is added, it cannot be renamed or removed. This is enforced via CEL rules (`oldSelf.all(k, k in self)`).
* **MultiOrch Placement:** `multiorch` is deployed to the cells listed in `multiorch.cells`. If this list is empty or omitted, it defaults to all cells where pools are defined.
* **Pool Placement:** `pools` uses a `cells` list. This list typically contains only a few cells rather than using all available cells. Multiple cells can be listed to apply the same configuration across multiple zones and regions.
* **PVC management:** Pool pods are managed directly by the operator (no StatefulSets). PVC lifecycle is controlled by `PVCDeletionPolicy` at the pool, shard, tablegroup, or cluster level.
* **PVC volume expansion:** Increasing `storage.size` on a pool triggers in-place PVC expansion. The `StorageClass` must have `allowVolumeExpansion: true`. Shrinks are rejected at admission. See [pod-management-architecture.md](../pod-management-architecture.md#pvc-volume-expansion) for details.
* **InitdbArgs:** An optional `initdbArgs` field passes extra arguments to `initdb` during PostgreSQL data directory initialization (e.g., `--locale-provider=icu --icu-locale=en_US.UTF-8`). This value is resolved through the same 4-level override chain and propagated as the `POSTGRES_INITDB_ARGS` environment variable on the pgctld container. It only takes effect when a pod initializes a new data directory.

```yaml
apiVersion: multigres.com/v1alpha1
kind: ShardTemplate
metadata:
  name: "standard-shard-ha"
  namespace: example
spec:
  # Template strictly defines only Shard-scoped components.

  # InitdbArgs specifies extra arguments passed to initdb during PostgreSQL
  # data directory initialization. Only takes effect when a pod initializes
  # a new data directory. Applied to all pool pods in shards using this template.
  # initdbArgs: "--locale-provider=icu --icu-locale=en_US.UTF-8"

  # MultiOrch is a shard-level component.
  # The Operator will deploy one instance of this Deployment into EVERY Cell
  # listed in 'cells'. If 'cells' is empty, it defaults to all cells
  # where pools are defined.
  multiorch:
    # replicas: 1 # replicas per cell and pool this multiorch is deployed
    cells: [] # Defaults to all populated cells
    resources:
      requests:
        cpu: "100m"
        memory: "128Mi"
      limits:
        cpu: "200m"
        memory: "256Mi"

  # MAP STRUCTURE: Keyed by pool name for safe targeting.
  pools:
    main-app:
      type: "readWrite"
      replicasPerCell: 2
      storage:
        class: "gp3"
        size: "100Gi"
      postgres:
         resources:
            requests:
              cpu: "1"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
      multipooler:
         resources:
            requests:
              cpu: "500m"
              memory: "512Mi"
            limits:
              cpu: "1"
              memory: "1Gi"
      # Affinity is optional.
      # The Operator automatically applies zone-spreading based on the Cell definition.
      # This field allows adding EXTRA constraints (e.g., specific node types).
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: disktype
                operator: In
                values:
                - ssd
      # PVC lifecycle can be controlled per-pool (overrides shard/tablegroup/cluster defaults)
      # pvcDeletionPolicy:
      #   whenDeleted: "Delete"
      #   whenScaled: "Delete"
    dr-replica:
      type: "readWrite"
      # This pool will be deployed to all cells listed here.
      cells:
        - "us-west-2a"
      replicasPerCell: 1
      storage:
         class: "standard-gp3"
         size: "100Gi"
      postgres:
         resources:
            requests:
              cpu: "1"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
      multipooler:
         resources:
            requests:
              cpu: "500m"
              memory: "512Mi"
            limits:
              cpu: "1"
              memory: "1Gi"
```

### The `default` Flag (System Resources)

Both `Database` and `TableGroup` entries support a boolean `default` flag (defaulting to `false`). This flag maps the definition to the **System Default** infrastructure created during the cluster's bootstrap phase.

  * **On a Database (`default: true`):** Indicates this entry defines the configuration for the system-level database (typically named `postgres`) that contains the global catalog. There can be only one default database per cluster.
  * **On a TableGroup (`default: true`):** Indicates this entry defines the configuration for the "Catch-All" or "Unsharded" group of that database. Every database has exactly one Default TableGroup where tables land by default.

Defining these entries allows the user to explicitly configure the resources (replicas, storage, compute) allocated to these system components, rather than relying on hardcoded operator defaults.

### Child Resources (Read-Only)

These resources are created and reconciled by the `MultigresCluster` controller.

> NOTE: At some point we may want to consider adding status fields for the children to say what template the config is coming from, for simplicity not defining that now.

#### Child CR: TopoServer

  * Applies to both Global (owned by `MultigresCluster`) and Local (owned by `Cell`) topology servers.
  * This CR does *not* exist if a separate, `external` etcd definition is used in the parent (e.g. using etcd-operator to provision one).
  * Configurations are wrapped under their implementation key (e.g., `etcd`) to allow future extensibility (e.g., `consul`).

```yaml
apiVersion: multigres.com/v1alpha1
kind: TopoServer
metadata:
  name: "example-cluster-global"
  namespace: example
  ownerReferences:
    - apiVersion: multigres.com/v1alpha1
      kind: MultigresCluster
      name: "example-cluster"
      controller: true
spec:
  # Configuration is wrapped to allow for future polymorphic types (e.g. Consul).
  etcd:
    replicas: 3
    storage:
      size: "10Gi"
      class: "standard-gp3"
    image: "quay.io/coreos/etcd:v3.5"
    rootPath: "/multigres/global"
    resources:
      requests:
        cpu: "500m"
        memory: "1Gi"
      limits:
        cpu: "1"
        memory: "2Gi"
status:
  conditions:
    - type: Available
      status: "True"
      lastTransitionTime: "2025-11-07T12:01:00Z"
  clientService: "example-cluster-global-client"
  peerService: "example-cluster-global-peer"
```

#### Child CR: Cell

  * Owned by `MultigresCluster`.
  * Strictly contains `MultiGateway` and optional `LocalTopoServer`.
  * The `allCells` field is used for discovery by gateways.

```yaml
apiVersion: multigres.com/v1alpha1
kind: Cell
metadata:
  name: "example-cluster-us-east-1a"
  namespace: example
  labels:
    multigres.com/cluster: "example-cluster"
    multigres.com/cell: "us-east-1a"
  ownerReferences:
    - apiVersion: multigres.com/v1alpha1
      kind: MultigresCluster
      name: "example-cluster"
      uid: "a1b2c3d4-1234-5678-90ab-f0e1d2c3b4a5"
      controller: true
spec:
  name: "us-east-1a"
  zone: "us-east-1a" # this would be region if that was chosen instead.

  # Images and pull config passed down from global configuration
  images:
    imagePullPolicy: "IfNotPresent"
    imagePullSecrets:
      - name: "my-registry-secret"
    multigateway: "multigres/multigres:latest"

  # Resolved from CellTemplate + Overrides
  multigateway:
    replicas: 3
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "1"
        memory: "1Gi"
    affinity:
      podAntiAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
             labelSelector:
               matchLabels:
                 app.kubernetes.io/component: multigateway
             topologyKey: "kubernetes.io/hostname"

  # A reference to the GLOBAL TopoServer.
  # Always populated by the parent controller if no local server is used.
  globalTopoServer:
    address: "example-cluster-global-client.example.svc.cluster.local:2379"
    rootPath: "/multigres/global"
    implementation: "etcd"

  # Option 1: Using the Global TopoServer (Default)
  #
  topoServer: {}

  # Option 2: Inline Definition (External)
  #
  # topoServer:
  #   external:
  #     endpoints:
  #       - "https://my-etcd.some-namespace.svc.cluster.local:2379"
  #     rootPath: "/multigres/us-east-1a"
  #     implementation: "etcd"

  # Option 3: Managed Local
  #
  # topoServer:
  #   etcd:
  #     rootPath: "/multigres/us-east-1a"
  #     image: "quay.io/coreos/etcd:v3.5"
  #     replicas: 3
  #     storage:
  #       size: "5Gi"
  #       class: "standard-gp3"

  # List of all cells in the cluster for discovery.
  allCells:
  - "us-east-1a"
  - "us-east-1b"

  # Topology flags for the Cell controller to act on.
  topologyReconciliation:
    registerCell: true
    prunePoolers: true

  # Observability configuration inherited from MultigresCluster
  # observability:
  #   otlpEndpoint: "http://otel-collector:4318"
  #   tracesExporter: "otlp"

status:
  observedGeneration: 1
  phase: "Healthy"
  conditions:
  - type: Available
    status: "True"
    lastTransitionTime: "2025-11-07T12:01:00Z"
  gatewayReplicas: 3
  gatewayReadyReplicas: 3
  gatewayServiceName: "example-cluster-us-east-1a-gateway"
```

#### Child CR: TableGroup

  * Owned by `MultigresCluster`.
  * Acts as the middle-manager for Shards. It MUST contain the fully resolved specification for all shards it manages, enabling it to be the single source of truth for creating its child `Shard` CRs.

```yaml
apiVersion: multigres.com/v1alpha1
kind: TableGroup
metadata:
  name: "example-cluster-production-db-orders-tg"
  namespace: example
  labels:
    multigres.com/cluster: "example-cluster"
    multigres.com/database: "production_db"
    multigres.com/tablegroup: "orders_tg"
  ownerReferences:
    - apiVersion: multigres.com/v1alpha1
      kind: MultigresCluster
      name: "example-cluster"
      controller: true
spec:
  databaseName: "production_db"
  tableGroupName: "orders_tg"
  default: true

  # Images and pull config passed down from global configuration
  images:
    imagePullPolicy: "IfNotPresent"
    imagePullSecrets:
      - name: "my-registry-secret"
    multiorch: "multigres/multigres:latest"
    multipooler: "multigres/multigres:latest"
    postgres: "postgres:15.3"

  # A reference to the GLOBAL TopoServer.
  globalTopoServer:
    address: "example-cluster-global-client.example.svc.cluster.local:2379"
    rootPath: "/multigres/global"
    implementation: "etcd"

  # The list of FULLY RESOLVED shard specifications.
  # This is pushed down from the MultigresCluster controller.
  shards:
    - name: "0"
      multiorch:
        cells:
          - "us-east-1a"
          - "us-east-1b"
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "200m"
            memory: "256Mi"
      pools:
        main-app:
          cells:
            - "us-east-1a"
          type: "readWrite"
          replicasPerCell: 2
          storage:
             size: "100Gi"
             class: "standard-gp3"
          postgres:
              resources:
                requests:
                  cpu: "2"
                  memory: "4Gi"
                limits:
                  cpu: "4"
                  memory: "8Gi"
          multipooler:
              resources:
                requests:
                  cpu: "1"
                  memory: "512Mi"
                limits:
                  cpu: "2"
                  memory: "1Gi"

    - name: "1"
      multiorch:
        cells:
           - "us-east-1b"
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "200m"
            memory: "256Mi"
      pools:
        main-app:
          cells:
            - "us-east-1b"
          type: "readWrite"
          replicasPerCell: 2
          storage:
             size: "100Gi"
             class: "standard-gp3"
          postgres:
              resources:
                requests:
                  cpu: "2"
                  memory: "4Gi"
                limits:
                  cpu: "4"
                  memory: "8Gi"
          multipooler:
              resources:
                requests:
                  cpu: "1"
                  memory: "512Mi"
                limits:
                  cpu: "2"
                  memory: "1Gi"

    # This shard's spec is resolved from a template
    # (e.g., "cluster-wide-shard")
    - name: "2"
      multiorch:
        cells:
          - "us-east-1c"
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
          limits:
            cpu: "200m"
            memory: "256Mi"
      pools:
        main-app:
          cells:
            - "us-east-1c" # Resolved from override
          type: "readWrite"
          replicasPerCell: 2
          storage:
             size: "100Gi"
             class: "standard-gp3"
          postgres:
              resources:
                requests:
                  cpu: "2"
                  memory: "4Gi"
                limits:
                  cpu: "4"
                  memory: "8Gi"
          multipooler:
              resources:
                requests:
                  cpu: "1"
                  memory: "512Mi"
                limits:
                  cpu: "2"
                  memory: "1Gi"

  # PVC lifecycle management, inherited from MultigresCluster
  pvcDeletionPolicy:
    whenDeleted: "Retain"
    whenScaled: "Delete"

  # Observability inherited from MultigresCluster
  # observability:
  #   otlpEndpoint: "http://otel-collector:4318"

  # Backup config inherited from MultigresCluster -> Database -> TableGroup chain
  # backup:
  #   type: "filesystem"
  #   ...

  # Cell topology labels for nodeSelector injection
  cellTopologyLabels:
    us-east-1a:
      topology.kubernetes.io/zone: "us-east-1a"
    us-east-1b:
      topology.kubernetes.io/zone: "us-east-1b"

status:
  observedGeneration: 1
  phase: "Healthy"
  readyShards: 3
  totalShards: 3
```

#### Child CR: Shard

  * Owned by `TableGroup`.
  * Contains `MultiOrch` (consensus management) and `Pools` (actual data nodes).
  * Represents one entry from the `TableGroup` shards list.

```yaml
apiVersion: multigres.com/v1alpha1
kind: Shard
metadata:
  name: "example-cluster-production-db-orders-tg-0"
  namespace: example
  labels:
     multigres.com/cluster: "example-cluster"
     multigres.com/database: "production_db"
     multigres.com/tablegroup: "orders_tg"
     multigres.com/shard: "0"
  ownerReferences:
    - apiVersion: multigres.com/v1alpha1
      kind: TableGroup
      name: "example-cluster-production-db-orders-tg"
      controller: true
spec:
  databaseName: "production_db"
  tableGroupName: "orders_tg"
  shardName: "0"

  # Images and pull config passed down from global configuration
  images:
    imagePullPolicy: "IfNotPresent"
    imagePullSecrets:
      - name: "my-registry-secret"
    multiorch: "multigres/multigres:latest"
    multipooler: "multigres/multigres:latest"
    postgres: "postgres:15.3"

  # A reference to the GLOBAL TopoServer.
  globalTopoServer:
    address: "example-cluster-global-client.example.svc.cluster.local:2379"
    rootPath: "/multigres/global"
    implementation: "etcd"

  # Fully resolved from parent TableGroup spec
  multiorch:
    cells:
      - "us-east-1a"
    resources:
      requests:
        cpu: "100m"
        memory: "128Mi"
      limits:
        cpu: "200m"
        memory: "256Mi"

  # Extra initdb arguments, resolved from inline/template/override chain.
  # Propagated as POSTGRES_INITDB_ARGS env var on pgctld containers.
  # initdbArgs: "--locale-provider=icu --icu-locale=en_US.UTF-8"

  pools:
    main-app:
      cells:
        - "us-east-1a"
      type: "readWrite"
      replicasPerCell: 2
      storage:
         size: "100Gi"
         class: "standard-gp3"
      postgres:
           resources:
              requests:
                cpu: "2"
                memory: "4Gi"
              limits:
                cpu: "4"
                memory: "8Gi"
      multipooler:
           resources:
              requests:
                cpu: "1"
                memory: "512Mi"
              limits:
                cpu: "2"
                memory: "1Gi"

  # PVC lifecycle management, inherited from cluster/tablegroup/shard settings
  pvcDeletionPolicy:
    whenDeleted: "Retain"
    whenScaled: "Delete"

  # Backup configuration, inherited from cluster -> database -> tablegroup chain
  backup:
    type: "filesystem"
    filesystem:
      path: "/backups"
      storage:
        size: "10Gi"
        class: "standard-rwx"
    pgbackrestTLS:
      secretName: ""

  # Observability configuration, inherited from cluster
  observability:
    otlpEndpoint: "http://otel-collector:4318"
    tracesExporter: "otlp"
    metricsExporter: "otlp"

  # Cell topology labels for nodeSelector injection
  cellTopologyLabels:
    us-east-1a:
      topology.kubernetes.io/zone: "us-east-1a"

  # Total replicas across all pools (for HPA/PDB scale subresource)
  replicas: 2

status:
  observedGeneration: 1
  phase: "Healthy"
  conditions:
    - type: Available
      status: "True"
      lastTransitionTime: "2025-11-07T12:01:00Z"
  cells:
    - "us-east-1a"
  orchReady: True
  poolsReady: True
  readyReplicas: 2
  lastBackupTime: "2025-11-07T06:00:00Z"
  lastBackupType: "full"
  podRoles:
    example-cluster-production-db-orders-tg-0-main-app-us-east-1a-0: "PRIMARY"
    example-cluster-production-db-orders-tg-0-main-app-us-east-1a-1: "REPLICA"
```

## Defaults & Webhooks

To simplify user experience and ensure cluster stability, the operator uses a combination of **Mutating Webhooks** (for applying defaults) and **Validating Webhooks** (for synchronous checks), alongside **Controller Logic** (for asynchronous operations).

### 1\. Configuration Defaults (Mutating Webhook)

A mutating admission webhook applies a strict **4-Level Override Chain** to resolve configurations. This logic is applied consistently for Cluster Components (`GlobalTopoServer`, `MultiAdmin`, `Cells`) and Database Shards.

#### Cluster Component Override Chain

*Applies to `GlobalTopoServer`, `MultiAdmin`, and `Cells` defined in `MultigresCluster`.*

1.  **Component-Level Definition (Highest):** An inline `spec` or an explicit `templateRef` / `cellTemplate`, along with optional `overrides` on the component itself.
2.  **Cluster-Level Default Template:** The corresponding template defined in `spec.templateDefaults` (e.g., `templateDefaults.coreTemplate` or `templateDefaults.cellTemplate`).
3.  **Namespace-Level Default Template:** A template of the correct kind (e.g., `CoreTemplate`) named `default` in the same namespace as the cluster.
4.  **Operator Hardcoded Defaults (Lowest):** A final fallback applied by the operator code (e.g., default resources, default replicas).

#### Shard Override Chain

*Applies to every Shard defined in `spec.databases[].tablegroups[].shards[]`.*

1.  **Shard-Level Definition (Highest):** An inline `spec` or an explicit `shardTemplate` defined on the specific shard entry in the `MultigresCluster` YAML.
2.  **Cluster-Level Default Template:** The `spec.templateDefaults.shardTemplate` field in the root `MultigresCluster` CR.
3.  **Namespace-Level Default Template:** A `ShardTemplate` named `default` in the same namespace as the cluster.
4.  **Operator Hardcoded Defaults (Lowest):** A final fallback applied by the operator.
5.  **List Replacement Behavior:** When overriding the `cells` field (in pools or multiorch), the new list specified in the override *completely replaces* the list defined in the template. It is not merged or appended.
-----

#### System Catalog Smart Defaulting

To handle the "System Catalog" requirements automatically, the webhook applies specific defaulting logic:

1.  **System Database Injection:** If the `spec.databases` list is empty, the webhook automatically injects the System Database (`Name: "postgres"`, `Default: true`).
2.  **Default TableGroup Injection:** For every database, if the `tableGroups` list is empty, the webhook automatically injects the Default TableGroup (`Name: "default"`, `Default: true`).

This ensures that a minimal `MultigresCluster` with only `cells` defined is instantly functional.

### 2\. Synchronous Validating Webhooks

Webhooks are used *only* for fast, synchronous, and semantic validation to prevent invalid configurations from being accepted by the API server.

#### `MultigresCluster`

  * **On `CREATE` and `UPDATE`:**
      * **Template Existence:** Validates that all templates referenced in `spec.templateDefaults` or explicitly in components (Core, Cell, or Shard templates) exist *at the time of application*.
      * **Component Spec Mutex:** Enforces that `etcd`, `external`, and `templateRef` are mutually exclusive for components like `GlobalTopoServer` (CEL: exactly one of the three must be set).
      * **Uniqueness:** Validated via `+listType=map` and `+listMapKey=name` markers, which ensure unique names within their respective scopes:
          * Cell names in `spec.cells`.
          * Database names in `spec.databases`.
          * TableGroup names within a Database.
          * Shard names within a TableGroup.
      * **Topology Integrity:** Verifies that if a Shard is pinned to a specific cell (via overrides), that cell exists in the `spec.cells` list.
      * **Name Length Safety:**
          * `MultigresCluster` name must be ≤ 25 characters (CEL enforced at resource level).
          * Individual name types have their own limits: `DatabaseName` ≤ 30, `TableGroupName` ≤ 25, `ShardName` ≤ 25, `PoolName` ≤ 25, `CellName` ≤ 30.
      * **Append-Only Enforcement (CEL Rules):** Several fields use CEL transition rules to prevent potentially destructive removals:
          * **Cells:** `oldSelf.all(c, c.name in self.map(x, x.name))` — cells cannot be removed or renamed.
          * **Pool cells:** `oldSelf.all(c, c in self)` — cells cannot be removed from a pool.
          * **Pool names:** `oldSelf.all(k, k in self)` — pools cannot be removed or renamed.
      * **v1alpha1 Constraints (CEL Rules):** Due to current Multigres Gateway limitations, the following strict validations are applied via CEL:
          * `MaxItems: 1` for `spec.databases`.
          * The single database MUST be named `"postgres"` and marked `default: true`.
          * Any TableGroup marked `default: true` MUST be named `"default"`.
          * **Mandatory Default TableGroup:** Every defined database must contain exactly one TableGroup marked `default: true`.
          * **Shard name:** Must be exactly `"0-inf"` in this version.
          * **Local TopoServer:** CellInlineSpec blocks `localTopoServer` via CEL (`not yet supported`).

-----

### 3. Scheduling and Placement (Affinity)

The Operator enforces High Availability (HA) by default while allowing user customization.

1.  **Operator-Enforced Placement (Mandatory):**
    * The Operator automatically injects `nodeSelector` or `nodeAffinity` rules to ensure Pods scheduled for a specific `Cell` land on nodes belonging to that Cell (e.g., `topology.kubernetes.io/zone: us-east-1a`).
    * The Operator automatically injects `podAntiAffinity` to spread replicas of the same Shard/Pool across different nodes within that Cell to survive node failures.

2.  **User-Defined Constraints (Additive):**
    * Users can define `affinity` and `tolerations` in the `PoolSpec` (either in the Template or via Overrides).
    * **Merge Logic:** User constraints are **appended** to the Operator's mandatory constraints. This allows users to restrict scheduling further (e.g., "Only run on nodes with label `disktype=ssd`") but prevents them from violating the fundamental Cell topology (e.g., you cannot schedule a "us-east-1a" pod onto a "us-west-2b" node).

### 4. Asynchronous Controller Logic

Asynchronous logic is used for operations that depend on external state or require coordinated deletion, handled by controllers and owner references.

#### `MultigresCluster`

* **Deletion Protection (Owner References):**
    1.  All child CRs (`Cell`, `TableGroup`, `TopoServer`) have owner references pointing to the `MultigresCluster`, enabling Kubernetes garbage collection to cascade-delete them when the cluster is removed.
    2.  **Logic:** When the user deletes the cluster, Kubernetes blocks deletion until all owned child CRs have been successfully deleted and confirmed gone by the API server. This ensures no orphaned resources (like expensive cloud load balancers or PVCs) are left behind. PVC lifecycle during cluster deletion is controlled by the `PVCDeletionPolicy`: when `WhenDeleted` is `Delete`, PVCs have conditional owner references to the Shard CR enabling cascade deletion; when `Retain`, PVCs have no ownerRef and persist.

* **Topology Registration & Pruning:**
    1.  The MultigresCluster controller centralizes all topology management via `reconcileTopology()`. On each reconcile, it registers all cells and databases from the spec into the global topology server (etcd).
    2.  **Pruning:** When `spec.topologyPruning.enabled` is `true` (the default), the controller also prunes stale topology entries — cells and databases that exist in etcd but are no longer present in the MultigresCluster spec. This ensures the topology stays in sync with the desired state.
    3.  **Grace period:** During the first 2 minutes after cluster creation, topology server unavailability is silently requeued (the topo server may still be starting). After the grace period, unavailability is reported as a reconcile error.

#### `CoreTemplate`, `CellTemplate`, `ShardTemplate`

* **In-Use Protection (Validating Webhook):**
    1.  **Mechanism:** A Validating Webhook intercepts all `DELETE` operations on Template resources.
    2.  **Logic:**
        * The webhook queries for any `MultigresCluster` resources that reference the template being deleted.
        * To make this query efficient, the `MultigresCluster` controller must apply **Tracking Labels** to the Cluster CR whenever a template is referenced (e.g., `multigres.com/cell-template: prod-small`).
        * If the query returns any results (meaning the template is in use), the webhook **rejects** the deletion request with a `403 Forbidden` error.
    3.  **Benefit:** This prevents the Template from ever entering a "Terminating" state, ensuring it remains fully editable and active even if a user accidentally tries to delete it.
    4.  **Race Condition Handling (Fail Safe):** In the rare race condition where a template is deleted immediately after a cluster is created (before the controller applies tracking labels), the Cluster Controller handles the missing template gracefully by setting the Cluster Status Condition to `Ready=False` (Reason: `TemplateMissing`) and pausing reconciliation. This ensures no data loss or configuration drift occurs; the operator simply waits for the user to restore the template or update the reference.

## End-User Examples

### 1\. The Ultra-Minimalist (Relying on Namespace/Webhook Defaults)

This creates a functioning Multigres cluster by only defining the physical topology (cells). The operator's "Smart Defaulting" logic automatically injects the system database (`postgres`).

The defaults for this ultra-minimalistic example can be fetched in two ways:

1.  All components are defaulted by the operator's webhook.
2.  If a `CoreTemplate`, `CellTemplate`, and `ShardTemplate` named `default` exists within the same namespace it will take these as its default values.

> Notice that the `cells` field is still necessary but we are not naming the cell, this is because we are not sure yet if we should take a default zone or region at random from the cluster to define this, but if we can do this safely this field also won't be needed

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: minimal-cluster
spec:
  cells:
    - zone: "us-east-1a"
```

When the user does a `kubectl get multigrescluster minimal-cluster -o yaml` after apply this they would see all the values materialized, the default will be applied via webhook.

### 2\. The Minimalist (Relying on Cluster Defaults)

This user relies on the `spec.templateDefaults` field to set cluster-wide defaults.

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: minimal-cluster-with-defaults
spec:
  # This cluster will use the "dev-defaults" CoreTemplate for its
  # global components. All cells and shards will use their respective
  # "dev-defaults" templates.
  templateDefaults:
    coreTemplate: "dev-defaults"
    cellTemplate: "dev-defaults-cell"
    shardTemplate: "dev-defaults-shard"

  # CoreComponents (globalTopoServer, multiadmin) are omitted,
  # so they will use "dev-defaults" (from the CoreTemplate)

  cells:
    - name: "us-east-1a"
      zone: "us-east-1a"
      # 'spec' and 'cellTemplate' are omitted, so "dev-defaults-cell" is used.
    - name: "us-west-2a"
      zone: "us-west-2a"
      # 'spec' and 'cellTemplate' are omitted, so "dev-defaults-cell" is used.

  # NOTE: v1alpha1 Constraint
  # This example uses logical names ("db1", "tg1") to demonstrate API flexibility.
  # However, in v1alpha1, strict validation requires the database to be named "postgres"
  # and the tablegroup to be named "default".
  databases:
    - name: "postgres" # Was "db1"
      default: true    # Added for v1alpha1 validity
      tablegroups:
      - name: "default" # Was "tg1"
        default: true   # Added for v1alpha1 validity
        shards:
          - name: "0-inf"
            # 'spec' and 'shardTemplate' are omitted, so "dev-defaults-shard" is used.
            # We still need to override the 'cell' for the main-app pool.
            overrides:
              pools:
                main-app:
                  cells:
                    - "us-east-1a"
```

### 3\. The Power User (Explicit Overrides)

This user explicitly defines everything, mixing inline specs and templates, and bypassing all defaults.

```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: power-cluster
spec:
  # This user sets cluster defaults, but overrides them everywhere.
  templateDefaults:
    coreTemplate: "cluster-default-core"
    cellTemplate: "cluster-default-cell"
    shardTemplate: "cluster-default-shard"

  globalTopoServer:
    external:
      endpoints:
        - "https://my-etcd-1.infra:2379"
        - "https://my-etcd-2.infra:2379"
        - "https://my-etcd-3.infra:2379"
      caSecret: "etcd-ca"
      clientCertSecret: "etcd-client-cert"

  multiadmin:
    spec:
      replicas: 1
      resources:
        requests:
          cpu: "100m"
          memory: "256Mi"
        limits:
          cpu: "200m"
          memory: "512Mi"
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchLabels:
                  app.kubernetes.io/component: multiadmin
              topologyKey: "kubernetes.io/hostname"

  cells:
    - name: "us-east-1a"
      zone: "us-east-1a"
      cellTemplate: "high-throughput-gateway"
    - name: "us-west-2a"
      zone: "us-west-2a"
      cellTemplate: "standard-gateway"
      overrides:
        multigateway:
          # Overriding the entire resources block
          resources:
             requests:
               cpu: "500m"
               memory: "2Gi"
             limits:
               cpu: "1"
               memory: "4Gi"
          # Affinity can be configured too by user if desired
          # affinity:
          #   podAntiAffinity:
          #     preferredDuringSchedulingIgnoredDuringExecution:
          #     - weight: 100
          #       podAffinityTerm:
          #           labelSelector:
          #             matchLabels:
          #               app.kubernetes.io/component: multiadmin
          #           topologyKey: "kubernetes.io/hostname"

  # [NOTE: v1alpha1 Constraint]
  # This example uses a custom logical database ("users_db") and tablegroup ("auth").
  # In v1alpha1, this must be changed to "postgres" and "default" respectively.
  databases:
    - name: "postgres" # Was "users_db"
      default: true    # Added for v1alpha1 validity
      tablegroups:
        - name: "default" # Was "auth"
          default: true   # Added for v1alpha1 validity
          shards:
            - name: "0-inf"
              shardTemplate: "geo-distributed-shard"
              overrides:
                # MAP-BASED OVERRIDE: Safely targeting 'main-app' pool
                pools:
                  main-app:
                    # Partial override of a simple field
                    cells:
                      - "us-east-1a"
                    # OVERRIDE of Postgres compute for this specific shard
                    postgres:
                       requests:
                         cpu: "8"
                         memory: "16Gi"
                       limits:
                         cpu: "8"
                         memory: "16Gi"
```

## Implementation History

  * **2025-10-08:** Initial proposal to create individual, user-managed CRDs for each component (`MultiGateway`, `MultiOrch`, etc.).
  * **2025-10-14:** A second proposal introduced a top-level `MultigresCluster` CR as the primary user-facing API.
  * **2025-10-28:** The current "parent/child" model was formalized, designating `MultigresCluster` as the single source of truth with read-only children.
  * **2025-11-05:** Explored a simplified V1 API limited to a single shard. Rejected to ensure the API is ready for multi-shard from day one.
  * **2025-11-06:** Explored a single "all-in-one" `DeploymentTemplate`. Rejected due to N:1 conflicts when trying to apply one template to both singular Cell components and multiplied Shard components.
  * **2025-11-07:** Finalized the "Scoped Template" model (`CellTemplate` & `ShardTemplate`) and restored full explicit `database` -\> `tablegroup` -\> `shard` hierarchy.
  * **2025-11-10:** Refactored `pools` to use Maps instead of Lists and introduced atomic grouping for `resources` and `storage` to ensure safer template overrides.
  * **2025-11-11:** Introduced a consistent 4-level override chain (inline/explicit-template -\> cluster-default -\> namespace-default -\> webhook) for all components. Added `CoreTemplate` CRD and `spec.templateDefaults` block to support this. Reverted `spec.coreComponents` nesting to top-level `globalTopoServer` and `multiadmin` fields.
  * **2025-11-14:** Explored a multi-CRD "claim" model (`MultigresDatabase` + `MultigresDatabaseResources`) to support DDL-driven workflows and strong RBAC separation.
  * **2025-11-18:** Reverted to the 2025-11-11 monolithic `MultigresCluster` design to align with client requirements for Postgres-native "System Catalog" management. The `MultigresDatabase` CRD was rejected. We will instead support the DDL workflow via a synchronization controller that patches the monolithic `MultigresCluster` CR based on the `SystemCatalog` state.
  * **2025-11-25:** Moved `multiorch` and `pool` placement from single `cell` fields to explicit `cells` lists. This supports multi-cell pool definitions (e.g., for uniform read replicas) and decouples MultiOrch placement from pool presence, while maintaining safety via a "defaults to all" logic.
  * **2026-01-02:** Introduced "System Catalog Smart Defaulting" and strict CEL validation for v1alpha1 to align the API with the current single-database routing limitations of the Multigres Gateway.
  * **2026-01-15:** Added `PVCDeletionPolicy` at cluster, tablegroup, shard, and pool levels with hierarchical merge logic. Added `BackupConfig` type supporting `filesystem` and `s3` backends, with per-database and per-tablegroup overrides. Added `PgBackRestTLSConfig` for pgBackRest inter-node TLS.
  * **2026-01-20:** Added `ObservabilityConfig` to propagate OpenTelemetry settings from `MultigresCluster` to data-plane Pods. Settings are resolved from operator environment variables by default.
  * **2026-01-25:** Added `MultiAdminWebConfig` for the MultiAdmin Web UI component. Added `multiadminWeb` image to `ClusterImages`. Added `podAnnotations` and `podLabels` to `StatelessSpec`.
  * **2026-02-01:** Added CEL rules for append-only cells, pools, and pool-cell lists to prevent accidental destructive operations. Enforced `MultigresCluster` name ≤ 25 chars. Set shard name to `"0-inf"` for v1alpha1.
  * **2026-02-10:** Added `CellTopologyLabels` to `ShardSpec` to propagate zone/region labels without the shard controller needing to look up Cell CRs directly. Added `ResolvedTemplates` to `MultigresClusterStatus` for efficient template-change triggered reconciliation.
  * **2026-02-20:** Pool management migrated from StatefulSets to direct operator-managed Pods and PVCs. Added scale subresource to Shard CRD (`.spec.replicas` / `.status.readyReplicas`). Added `PodRoles`, `LastBackupTime`, `LastBackupType` to `ShardStatus`.
  * **2026-03-10:** Added `DurabilityPolicy` as a configurable string field at cluster level (`spec.durabilityPolicy`) and per-database override (`databases[].durabilityPolicy`). Propagated through TableGroup and Shard to the topology registration. Previously hardcoded to `"AT_LEAST_2"`. Also fixed `UpdateDatabaseFields()` to sync DurabilityPolicy on re-registration.
  * **2026-03-17:** Changed `WhenScaled` PVC deletion default from `Retain` to `Delete` — pgbackrest should be the source of truth for data recovery. Added DRAINED pod handling: operator keeps DRAINED pods alive for investigation and provisions stand-in replicas to maintain availability.
  * **2026-03-20:** Added `PostgresConfigRef` type (ConfigMap reference) at the shard level for custom `postgresql.conf` parameters. Operator mounts the user-provided ConfigMap and sets `POSTGRES_INITDB_EXTRA_CONF` on pgctld so the user-supplied lines are appended to pgctld's auto-tuned config. See design rationale below.
  * **2026-03-20:** Added `ExternalGatewayConfig` (`spec.externalGateway`) for external exposure of the global multigateway Service via `externalIPs` and user-provided annotations. Added `GatewayStatus` (`status.gateway.externalEndpoint`) and `GatewayExternalReady` condition. Added `InitdbArgs` field to `ShardTemplateSpec`, `ShardInlineSpec`, and `ShardOverrides` for passing extra arguments to `initdb` during PostgreSQL data directory initialization. Added `IPAddress` and `InitdbArgs` validated types to `common_types.go`.
  * **2026-03-20:** Added `ExternalAdminWebConfig` (`spec.externalAdminWeb`) for external exposure of the global multiadmin-web Service via `externalIPs` and user-provided annotations. Mirrors the `ExternalGatewayConfig` pattern. Added `AdminWebStatus` (`status.adminWeb.externalEndpoint`) and `AdminWebExternalReady` condition. Readiness is driven by the admin-web Deployment's `ReadyReplicas` (unlike the gateway which aggregates across Cell CRs).

## Design Rationale: PostgreSQL Configuration (`postgresConfigRef`)

### The Problem

Users need to tune PostgreSQL runtime parameters (`shared_buffers`, `max_connections`, `work_mem`, etc.) for their workloads. Upstream pgctld honors the `POSTGRES_INITDB_EXTRA_CONF` env var, which points at a file whose contents are appended to pgctld's auto-tuned `postgresql.conf`. PostgreSQL's last-write-wins rule then lets the appended lines override matching params. The operator needs to expose this capability through the CRD.

### Approaches Considered

#### Option A: Raw Template String (rejected)

Expose the full Go template as a string field:

```yaml
spec:
  postgresConfigTemplate: |
    max_connections = {{.MaxConnections}}
    shared_buffers = {{.SharedBuffers}}
    work_mem = '256MB'
```

**Rejected because:**
- Couples the CRD to pgctld's internal template variable names (`{{.MaxConnections}}`, `{{.SharedBuffers}}`). If upstream renames a field, user manifests break.
- Users must understand Go template syntax and the `PostgresServerConfig` struct fields.
- Template syntax errors cause pod crashes at startup with opaque errors.
- Impossible to merge through the override chain -- you can't partially override a template string.
- Any upstream change to the default template (new parameters, comment changes) requires users to update their custom templates or miss new defaults.

#### Option B: Structured Fields with Typed Parameters (rejected)

Define individual typed fields for each PostgreSQL parameter:

```go
type PostgresConfig struct {
    SharedBuffers    string `json:"sharedBuffers,omitempty"`
    MaxConnections   *int32 `json:"maxConnections,omitempty"`
    WorkMem          string `json:"workMem,omitempty"`
    // ... 50+ fields
}
```

**Rejected because:**
- PostgreSQL has hundreds of parameters. Maintaining a 1:1 mapping is a permanent maintenance burden.
- Every new PostgreSQL version adds parameters -- the CRD would constantly need updating.
- Users needing less common parameters (extension settings like `auto_explain.log_min_duration`) would be blocked until we add the field.
- CRD schema bloat for parameters most users never touch.

#### Option C: ConfigMap Reference (chosen)

The user creates a ConfigMap with their postgresql.conf content and the CRD references it:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-postgres-config
data:
  custom.conf: |
    shared_buffers = '8GB'
    max_connections = 200
    work_mem = '256MB'
---
spec:
  postgresConfigRef:
    name: my-postgres-config
    key: custom.conf
```

**Chosen because:**
- The operator does not need to carry a copy of upstream's default template -- eliminates drift risk entirely.
- Works with any PostgreSQL parameter, including extension settings, without CRD changes.
- Users only need to know plain `postgresql.conf` syntax for the params they want to override; pgctld's auto-tuned defaults stay in effect for everything else thanks to PostgreSQL's last-write-wins behavior.
- The ConfigMap is a standard Kubernetes resource that can be managed separately (GitOps, Helm, Kustomize).
- Simple pointer override semantics through the override chain (last non-nil wins).
- When not set, pgctld uses only its auto-tuned values -- zero operator involvement in the default path.

#### Option D: Key-Value Map with Template Append (rejected)

Expose a `map[string]string` of PostgreSQL parameter name to value. The operator embeds a copy of pgctld's default template and appends user parameters at the end:

```yaml
spec:
  postgresConfig:
    shared_buffers: "8GB"
    max_connections: "200"
```

**Rejected because:**
- Requires the operator to carry a copy of upstream's default Go template that can drift when upstream changes it.
- Additive map merge through the override chain is more complex than atomic pointer replacement.
- The operator must build and manage an operator-owned ConfigMap per shard.

### Implementation

When `postgresConfigRef` is set on a shard:

1. The operator mounts the referenced ConfigMap as a volume using the `Items` field to project the user's key to the `postgresql.conf` filename under `/etc/pgctld/postgres-config/`.
2. The operator sets `POSTGRES_INITDB_EXTRA_CONF=/etc/pgctld/postgres-config/postgresql.conf` on the pgctld container.
3. pgctld reads its auto-tuned defaults and appends the user-supplied lines, so PostgreSQL's last-write-wins rule lets the user's params override matching defaults.

When `postgresConfigRef` is nil, no volume is mounted and no env var is set. pgctld uses only its auto-tuned values.

### Scope

`postgresConfigRef` is shard-level, not per-pool or per-cluster. All pods in a shard replicate from the same primary and must have compatible PostgreSQL settings. The ShardTemplate provides cluster-wide consistency, with per-shard overrides via the standard merge chain (last non-nil wins).

### Upstream Dependency

No upstream change is required -- `POSTGRES_INITDB_EXTRA_CONF` is already honored by pgctld. The operator never carries a copy of upstream's template, eliminating drift risk.

## Drawbacks

The requirement to create resources via DDL (`CREATE DATABASE`) to provide Postgres native interface was a part of the design discussions. This has not been dropped, but postponed. What follows are some caveats when following the current design as it stands right now and also incorporating this Posgres compatibility requirement:

  * **Broken "Delete" UX (Zombie Resources):** To support the "DB is Truth" requirement, the Operator should not delete a database simply because it is removed from the `spec.databases` list as it might still exist in the default database schema ("system catalog"). This breaks the standard Kubernetes expectation that "deleting config = deleting resource." Users must use imperative SQL (`DROP DATABASE`) to delete resources; removing them from Git will only orphan them, leaving them running and accruing costs ("Zombie Databases"). Including a bidirectional update process here may complicate things.
  * **Perpetual GitOps Drift:** Since users can create databases via DDL at any time, the `MultigresCluster` CR in Git will rarely match the actual cluster state. `kubectl diff` will be noisy, and the `status` field will become the only reliable view of the system, degrading the value of the declarative spec. This can be mitigated if we have a component that constantly writes these changes to either git and applies them declaratively, but it is not a common pattern.
  * **Status Object Bloat (Scalability):** Because the Operator must track "Discovered" (DDL-created) databases in the `status` field to make them visible to SREs, a cluster with thousands of databases risks hitting the Kubernetes object size limit (etcd limits). This limits the scalability of the reporting mechanism compared to fanning out to separate CRs.
  * **Resource "Adoption" Friction:** Databases created via DDL are assigned a default `ShardTemplate`. To "upgrade" or resize these databases, an SRE must manually "adopt" them by adding them to the `MultigresCluster` YAML with the correct name and new template. This introduces a manual step and a potential race condition where a new database might be under-provisioned before it can be adopted.
  * **Default DB Schema (System Catalog) Availability Dependency:** The Operator's reconciliation loop now strictly depends on the read availability of the "Default Database" (System Catalog). This acts as a single point of failure for the control plane: if the System Catalog is down, the Operator cannot manage any other part of the cluster, even if those other parts are healthy.
  * **API Hotspot and "Blast Radius" Risk:** By centralizing all database definitions into the monolithic `MultigresCluster` CR, this single object becomes a massive reconciliation hotspot. A single typo in this large resource (e.g., while "adopting" a database) could potentially break reconciliation for the entire cluster's control plane.
  * **Rename/Replace Ambiguity:** Postgres allows `ALTER DATABASE RENAME`, but Kubernetes relies on stable names. If a user renames a database in SQL, the Operator may perceive this as a "Delete" (of the old name) and "Create" (of the new name), potentially attempting to re-provision the old name if it still exists in the YAML.
  * **Lack of Namespace Isolation:** All databases must be defined in (or adopted into) the central `MultigresCluster` resource. This effectively forces all application teams to rely on platform admins to manage resource sizing, removing the ability to use Kubernetes RBAC for self-service resource management in separate namespaces. No clean DBA/Platform Engineer persona separation model.

## Alternatives

Several alternative designs were considered and rejected in favor of the current parent/child model.

### Alternative 1: Component CRDs Only (No Parent)

This model would provide individual, user-managed CRDs for `MultiGateway`, `MultiOrch`, `MultiPooler`, and `Etcd`. Users would be responsible for "composing" a cluster by creating these resources themselves.

  * **Pros:** Maximum flexibility and composability.
  * **Cons:** Extremely verbose and complex for a standard deployment. Users must manually create all components and wire them together correctly.
  * **Rejected Because:** Makes the common case (deploying a full cluster) unnecessarily complex and error-prone.

### Alternative 2: Hybrid Model with `managed: true/false` Flag

This model would feature a top-level `MultigresCluster` CR, but each component section would have a `managed: true/false` flag. If `true`, the operator manages the child resource. If `false`, the operator ignores it.

  * **Pros:** Offers a "best-of-both-worlds" approach.
  * **Cons:** Introduces significant complexity around resource ownership and lifecycle (e.g., handling transitions from managed to unmanaged). Creates a high risk of cluster misconfiguration.
  * **Rejected Because:** The lifecycle and ownership transitions were deemed too complex and risky for a production-grade operator.

### Alternative 3: The "Claim" Model (`MultigresDatabase`)

This design (explored on 2025-11-14) separated the cluster definition from database definitions. Users would create a `MultigresDatabase` CR in their own namespace, which would "claim" resources from the central cluster.

  * **Pros:** Solved the "API Hotspot" problem by fanning out DB definitions. Enabled true Kubernetes-native multi-tenancy via RBAC. provided a clean, declarative target for DDL translation.
  * **Cons:** Introduced additional CRDs.
  * **Rejected Because:** The client preferred a Postgres-native "System Catalog" approach where the database state is the primary source of truth, rejecting the separation of the "DBA persona" and the additional CRDs.

### Alternative 4: Helm Charts Instead of CRDs

This approach would use Helm charts to deploy Multigres components without an operator.

  * **Pros:** Familiar deployment model for many Kubernetes users.
  * **Cons:** No automatic reconciliation, no custom status reporting, and no active lifecycle management (failover, scaling, etc.).
  * **Rejected Because:** The operator pattern provides superior lifecycle management, observability, and automation, which are critical for a stateful database system.
