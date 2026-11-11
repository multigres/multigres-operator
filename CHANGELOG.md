# Changelog

All notable changes to the Multigres Operator are documented in this file.

---

## [Unreleased]

### Breaking

- **Rollback with dynamic tablegroups is destructive.** A `MultigresCluster` annotated `multigres.com/dynamic-tablegroups: "true"` provisions TableGroups from the Multigres sharding catalog. An operator release before this one computes the wanted TableGroups from the spec alone and drains and deletes every dynamic one, with its Shards and PVCs per their deletion policy, while the catalog still lists them; re-upgrading provisions them again from empty. Remove the annotation and empty the catalog before rolling back an opted-in cluster.

---

## [v0.10.1] — 2026-03-27

**Previous release:** v0.10.0 (2026-03-26)

Migrates the repository to the new `multigres` GitHub organization, updates default upstream images to sha-0faa227, bumps Go to 1.25.8, and hardens CI workflows with explicit permissions, govulncheck, and improved coverage reporting.

**15 commits, 197 files changed, ~893 insertions.**

### Refactoring

- **Organization rename numtide → multigres:** Updated Go module path from `github.com/numtide/multigres-operator` to `github.com/multigres/multigres-operator` across all packages, container image registry prefix `ghcr.io/numtide` → `ghcr.io/multigres`, Dockerfile OCI labels, Prometheus runbook URLs, and documentation references.

### Dependencies

- **Multigres image update:** Pinned default container images to `sha-0faa227` (pgctld, multigres), up from `sha-4bca1d5`. Upstream changes include distributed backup lease locking, consensus term file moved out of PGDATA, multigateway plan-level instrumentation, executil grouping, and etcd `--listen-metrics-urls` fix. multiadmin-web unchanged at `sha-d7be6e4`.
- **Go 1.25.8:** Updated Go toolchain version in `go.mod` and observer Dockerfile.
- **CI workflow improvements:** Replaced intermediate Docker image scan with dedicated `govulncheck` workflow; added explicit readonly permissions to reusable build and manifest workflows; refactored test coverage into a separate `coverage-comment.yaml` workflow with fork-safe artifact-based reporting.

### Observer

- **Multi-pool replication check fix:** Replication connection count was computed across all pools in a shard, producing false alerts for multi-pool shards. Now tracks expected replicas per pool so each primary is only compared against its own pool's replica count.
- **Multi-pool primary count fix:** Primary count validation expected 1 primary per pool, but all pools in a shard share a single primary. Multi-pool shards now correctly validate exactly 1 primary across the entire shard.
- **Exerciser postgres-config scenario:** Added `SHOW shared_buffers` verification to the update-postgres-config-content scenario to catch the upstream pgctld template re-rendering gap (multigres/multigres#781).
- **Organization rename:** Updated observer module path and Dockerfile label to `multigres` org; updated `go.mod` to use main module pseudo-version after rename.
- **Pin images skill:** Refined `pin_upstream_images` skill to filter to binary-relevant code only and added a decision gate before code changes.

---

## [v0.10.0] — 2026-03-26

**Previous release:** v0.9.0 (2026-03-23)

Standardizes the multigateway postgres port from 15432 to 5432, pins upstream images to sha-4bca1d5, corrects misleading external gateway documentation, and includes Makefile and CI maintenance.

**21 commits, 42 files changed, ~194 insertions.**

### Breaking Changes

- **MultiGateway port standardized to 5432:** Both the container listen port (`--pg-port`) and Kubernetes Service port changed from 15432 to 5432. The upstream 15432 convention exists for local dev where processes share a host; in Kubernetes, pods are isolated so the standard port is simpler. Existing clients connecting on port 15432 must update their connection strings. The CRD default for `postgresPort` is also updated to 5432.

### Improvements

- **Makefile cleanup:** Shared CRD installation definition, consistent KUBECONFIG references, added error handling, removed unused targets and unnecessary eval, corrected versioning and typos.

### Bug Fixes

- **External gateway annotation docs corrected:** Cloud LB controller annotations (e.g., `aws-load-balancer-scheme`) on a ClusterIP Service are inert — controllers only provision LBs for `type: LoadBalancer`. Docs, tests, and CHANGELOG v0.8.0 entries rewritten to remove misleading LB annotation examples and clarify `externalIPs` as the sole external access mechanism.

### Dependencies

- **Multigres image update:** Pinned default container images to `sha-4bca1d5` (pgctld, multigres), up from `sha-d9b8ff2`. Upstream changes include multigateway failover buffering, recovery control RPCs, extended query protocol fixes, and process execution wrapper. multiadmin-web unchanged at `sha-d7be6e4`.
- **GitHub Action pins updated:** All GitHub Action versions and SHA pins updated across CI workflows.
- **Nix developer environment:** Updated flake inputs to latest versions; moved Nix files into `nix/` directory.

### Observer

- **Port constant updated:** Observer `PortMultiGatewayPG` constant and connectivity check updated from 15432 to 5432 to match operator change.

---

## [v0.9.0] — 2026-03-23

**Previous release:** v0.8.0 (2026-03-20)

Adds PostgreSQL runtime configuration via ConfigMap references, external admin web exposure, and automatic rolling updates on ConfigMap content changes. Also includes a major test coverage expansion bringing operator packages to 95–100% coverage.

**16 commits, 81 files changed, ~7388 insertions.**

### Features

- **PostgreSQL configuration via ConfigMap:** New `postgresConfigRef` field on `ShardTemplateSpec`, `ShardOverrides`, and `ShardInlineSpec` references a user-created ConfigMap containing `postgresql.conf` overrides. The operator mounts the ConfigMap and passes `--postgres-config-template` to pgctld. When unset, pgctld uses its built-in template. Override chain: template → overrides → inline (last non-nil wins).
- **External admin web exposure:** New `spec.externalAdminWeb` field on MultigresCluster exposes the multiadmin-web Service via `externalIPs` with optional annotations, mirroring the existing `externalGateway` pattern. Includes `AdminWebExternalReady` condition driven by Deployment ReadyReplicas and `status.adminWeb.externalEndpoint`.
- **ConfigMap content hash rolling updates:** The operator now watches ConfigMaps referenced by `postgresConfigRef` and computes a SHA-256 hash of the referenced key data. Hash changes propagate through shard annotations → pod annotations → spec hash, triggering automatic rolling updates via the drain state machine without manual restarts.

### Bug Fixes

- **TopoServer status readiness:** TopoServer status computation used `sts.Status.Replicas` for readiness checks, which is unreliable with certain client implementations. Switched to `sts.Spec.Replicas` for phase computation and condition messages.

### Dependencies

- **Multigres image update:** Pinned default container images to `sha-d9b8ff2` (pgctld, multigres) and `sha-d7be6e4` (multiadmin-web), up from `sha-96175f5` and `sha-d1ba30a`.

### Testing

- **Operator test coverage expansion:** Added tests across 20 files covering multigrescluster, tablegroup, backuphealth, drain, topo, resolver, cell, shard, toposerver, and webhook handlers packages. Includes edge-case coverage for validation, error paths, DeletionTimestamp handling, crash-loop detection, and template resolution (~2,650 lines of test code).

### Documentation

- **External admin web guide:** New `docs/external-admin-web.md` covering configuration, status conditions, and usage patterns.
- **PostgreSQL configuration guide:** New `docs/postgresql-configuration.md` covering ConfigMap references, rolling updates, and a warning about Go template syntax in ConfigMap comments.

### Observer

- **Skill optimization:** Split `exercise_cluster` SKILL.md (557→194 lines) and `diagnose_with_observer` SKILL.md (372→189 lines) by extracting protocols and scenarios into reference files. Reduces context token consumption by ~83%.
- **Exerciser fixtures:** Added `external-adminweb` and `postgres-config-ref` fixtures with prerequisite ConfigMaps and patch scripts.
- **Postgres config scenarios:** Added 4 exerciser scenarios for verifying, updating, removing, and validating PostgreSQL configuration settings.
- **Fixture template fix:** Removed invalid Go template directives (`{{...}}`) from `postgres-config-ref/prerequisites.yaml` comments that caused pgctld's template rendering to fail silently.

---

## [v0.8.0] — 2026-03-20

**Previous release:** v0.7.1 (2026-03-19)

Adds external gateway exposure for DNS wiring and PostgreSQL initdb argument customization. The operator can now expose the global multigateway Service via `spec.externalIPs` with a `GatewayExternalReady` status condition, and users can pass custom initdb arguments (e.g., ICU locale settings) to PostgreSQL data directory initialization at the shard level.

**7 commits, 31 files changed, ~1556 insertions.**

### Features

- **External gateway endpoint:** New `spec.externalGateway` field on MultigresCluster exposes the global multigateway Service via `ClusterIP` + `spec.externalIPs` for external access without requiring LoadBalancer Services. Supports optional user-defined annotations as generic Service metadata. Publishes the resolved endpoint to `status.gateway.externalEndpoint` and manages a `GatewayExternalReady` condition with deterministic transitions: `AwaitingEndpoint` → `NoReadyGateways` → `EndpointReady`. Condition is removed when disabled. (Contributed by @Verolop)
- **PostgreSQL initdb arguments:** New `initdbArgs` field on `ShardTemplateSpec`, `ShardOverrides`, and `ShardInlineSpec` passes extra arguments to `initdb` during PostgreSQL data directory initialization (e.g., `--locale-provider=icu --icu-locale=en_US.UTF-8`). Injected as the `POSTGRES_INITDB_ARGS` environment variable on the pgctld container. Defined at the shard level because all pods in a shard must initialize with compatible settings for replication. Override chain: template → overrides → inline.

### Improvements

- **ExternalIP validation:** New `IPAddress` named type in `common_types.go` with kubebuilder `Pattern` validation for IPv4/IPv6 addresses. Applied per-item via schema validation (zero CEL cost). ExternalIPs also validated server-side via `net.ParseIP` in the admission webhook.
- **Annotation key guardrail:** CEL `XValidation` rule on `ExternalGatewayConfig` rejects annotation keys under the `multigres.com/` prefix, preventing conflicts with operator-managed metadata.
- **Webhook warning for missing externalIPs:** The admission webhook emits a warning when `externalGateway.enabled: true` is set without `externalIPs`, informing the user that the external endpoint will not be resolved.
- **Stale cell generation filtering:** Gateway readiness aggregation ignores Cell CRs whose `ObservedGeneration` lags behind `Generation`, preventing false-ready outcomes during rolling updates.

### Bug Fixes

- **Defaulter webhook initdbArgs propagation:** The defaulting webhook discarded the resolved `initdbArgs` value (assigned to `_`), silently stripping the field on admission. Fixed by capturing and including `InitdbArgs` in the rebuilt `ShardInlineSpec`.

### Dependencies

- **Observer gRPC CVE fix:** Bumped `google.golang.org/grpc` in `tools/observer/go.mod` from v1.79.2 to v1.79.3 to resolve GHSA-p77j-4mvh-x3m3 (critical). The root module was already fixed in v0.7.1.

### Documentation

- **External gateway guide:** New `docs/external-gateway.md` covering configuration, status conditions, DNS automation, and cloud deployment patterns.
- **PostgreSQL initialization guide:** New `docs/postgresql-initialization.md` covering initdb arguments, override chain, use cases (ICU locale, encoding, WAL segment size), and caveats.

### Observer

- **DRAINED pod detection in replication checks:** Observer now classifies DRAINED pods separately from replicas in `checkShardReplication` and `checkReplication`, emitting a `SeverityWarn` finding when DRAINED pods are detected. DRAINED pods are excluded from the expected replica count for primary replication probes.
- **New exerciser fixtures:** Added `pvc-default-policy` and `initdb-args` fixtures for testing default PVC policy propagation and initdb argument injection.

---

## [v0.7.1] — 2026-03-19

**Previous release:** v0.7.0 (2026-03-18)

Patch release fixing a multipooler crash introduced by the upstream image pin, adding IPv6/dual-stack support to the toposerver, and resolving a critical gRPC CVE. Also adds a topology-aware sample manifest for multi-cell deployments.

**10 commits, 12 files changed, ~296 insertions.**

### Bug Fixes

- **Multipooler PGDATA env var:** Upstream commit `c7cfd63` made the `PGDATA` environment variable mandatory for multipooler (previously only required by pgctld). The operator only set `PGDATA` on the postgres container, causing multipooler to crash with "PGDATA environment variable is required" on images >= `sha-96175f5`.

### Improvements

- **IPv6/dual-stack toposerver:** Etcd listen URLs changed from `http://0.0.0.0` to `http://[::]` for IPv6 compatibility. Headless and client Services now set `IPFamilyPolicy: PreferDualStack` for dual-stack cluster support. (Contributed by @Verolop)

### Dependencies

- **Upstream multigres images:** multigres `sha-803b6e7` → `sha-96175f5`, pgctld `sha-803b6e7` → `sha-96175f5`, multiadmin-web unchanged at `sha-d1ba30a`. Upstream changes: multigateway explicit transaction handling fix, pgctld `POSTGRES_INITDB_ARGS` support, per-table query metrics, `PGDATA` as data directory source of truth, multipooler `StateManager` for state transitions.
- **gRPC CVE fix:** Bumped `google.golang.org/grpc` from v1.78.0 to v1.79.3 to resolve GHSA-p77j-4mvh-x3m3 (critical). Transitive bump: `cel.dev/expr` v0.24.0 → v0.25.1.

### Documentation

- **Topology sample:** New `config/samples/topology.yaml` demonstrating a multi-cell deployment with region/zone topology, cell-specific overrides, and topology-aware scheduling.

---

## [v0.7.0] — 2026-03-18

**Previous release:** v0.6.0 (2026-03-17)

Introduces DRAINED pod handling — when multiorch marks a pooler as DRAINED (diverged data, pg_rewind failure), the operator now keeps the pod alive for admin investigation instead of auto-draining it, and spins up a stand-in replica to maintain availability. Changes the `WhenScaled` PVC deletion default from `Retain` to `Delete` so pgbackrest is the sole source of truth for recovery.

**5 commits, 27 files changed, ~567 insertions.**

### Breaking Changes

- **WhenScaled PVC default changed to Delete:** The `PVCDeletionPolicy.WhenScaled` field default changes from `Retain` to `Delete`. PVCs from scaled-down pods are now automatically deleted instead of being retained. Users relying on the `Retain` default for manual PVC recovery must explicitly set `whenScaled: Retain` in their `PVCDeletionPolicy` spec before upgrading.

### Features

- **DRAINED pod preservation:** When multiorch marks a pooler as DRAINED (diverged data, pg_rewind failed), the operator keeps the pod alive for admin investigation instead of auto-draining it. A stand-in replica is created at the next index to compensate for availability. Admins discard the DRAINED pod via `kubectl delete pod`, which triggers the drain state machine and always deletes the PVC (data is known-bad).
- **DRAINED label as durable signal:** New `syncDrainedLabels` phase in `reconcilePoolPods` sets `multigres.com/role=DRAINED` as a durable label that survives data-handler clearing `PodRoles`, ensuring PVC cleanup always identifies DRAINED pods.

### Improvements

- **Effective replica accounting:** Pool replica count is computed as `replicas + drainedCount`, threaded through `createMissingResources`, `handleScaleDown`, and `isPoolHealthy` so DRAINED pods don't consume slots intended for healthy replicas.
- **DRAINED pods excluded from health checks:** `isPoolHealthy` excludes pods with the DRAINED label so they don't block scale-down of stand-in replacements.

### Dependencies

- **Upstream multigres images:** multigres `sha-cb398c4` → `sha-803b6e7`, pgctld `sha-cb398c4` → `sha-803b6e7`, multiadmin-web unchanged at `sha-d1ba30a`. Upstream changes: multigateway EOF log level downgrade (ERROR → DEBUG), multipooler active postgres action tracking in status RPC, shard lock TTL fix (prevents stale lock deadlocks), multiorch `Health()` accessor removal.

### Documentation

- **Drained pod handling design:** New `plans/design-drained-pod-handling.md` documenting the design rationale, pod lifecycle, PVC cleanup semantics, and admin investigation workflow for DRAINED pods.
- **Admission controller doc updated:** `WhenScaled` default updated to `Delete` in admission controller documentation.
- **API design doc updated:** DRAINED pod handling semantics and `WhenScaled` default change reflected in the API design document.

### Observer

- **DRAINED pod detection:** The observer replication check now classifies DRAINED pods and reports a `SeverityWarn` finding when detected.
- **New exerciser fixture:** Added `pvc-default-policy` fixture for validating default PVC deletion policy propagation.

---

## [v0.6.0] — 2026-03-17

**Previous release:** v0.5.3 (2026-03-13)

Adds per-component log level configuration, aligns with upstream multigres flag and enum renames, removes the unimplemented readOnly pool type, and expands e2e test coverage from 4 to 16 subtests. Includes a Go style audit with a P0 fix for a latent PVC deletion bug in pod index resolution.

**17 commits, 127 files changed, ~2,957 insertions.**

### Breaking Changes

- **readOnly pool type removed:** The `readOnly` value has been removed from the `PoolType` enum. It had zero implementation in both the operator and upstream multigres — all pools were treated identically regardless of type. Users with `type: readOnly` in their specs must change to `type: readWrite`. Can be re-added when upstream supports differentiated pool types.
- **Durability policy enum renamed:** `ANY_2` → `AT_LEAST_2` and `MULTI_CELL_ANY_2` → `MULTI_CELL_AT_LEAST_2` to match upstream proto renames. Users referencing the old values in `DurabilityPolicy` fields must update to the new names.
- **Removed deprecated container flags:** `--connpool-admin-password`, `--cluster-metadata-refresh-interval`, `--timeout=30`, `--pg-database=postgres`, `--pg-user=postgres`, `--pooler-health-check-interval=500ms`, and `--recovery-cycle-interval=500ms` have been removed from container args. These either matched upstream defaults (no behavioral change) or were dropped upstream.

### Features

- **Configurable log levels:** New `LogLevels` field on `MultigresClusterSpec`, `CellSpec`, `ShardSpec`, and `TableGroupSpec` with per-component settings for pgctld, multipooler, multiorch, multiadmin, and multigateway. Each component's `--log-level` flag is wired through the resource hierarchy with inheritance. Default is `info`, materialized by the webhook.

### Improvements

- **Go style audit quick wins:** Applied 28 findings from a Go style audit with no behavioral changes: renamed stuttering exports (`EvaluateBackupHealth` → `Evaluate`, `ApplyBackupHealth` → `Apply`, `ParseBackupTime` → `ParseTime`, `ConditionBackupHealthy` → `ConditionHealthy`), replaced `sort.Slice` with `slices.Sort` (5 sites), replaced `reflect.DeepEqual` with `cmp.Diff` (3 test sites), lowercased error strings, fixed import ordering, added doc comments to 9 domain types and all CRD root types, and removed redundant comments.
- **Webhook doc comments:** Added documentation to all webhook handler methods and `ResolveCoreTemplate`.

### Bug Fixes

- **resolvePodIndex in-band error:** `resolvePodIndex` returned `-1` as a sentinel value for missing pods, which was then used as a valid PVC index downstream — a latent PVC deletion bug. Changed to `(int, bool)` return signature.
- **t.Fatalf in goroutine:** Fixed `t.Fatalf` called inside a goroutine in `resource_watcher_test.go`, which could cause test panics instead of clean failures.

### Dependencies

- **Upstream multigres images:** multigres `sha-bb174f9` → `sha-cb398c4`, pgctld `sha-9b51b0f` → `sha-cb398c4`, multiadmin-web `sha-b505c90` → `sha-d1ba30a`. Upstream changes include pooled yyParser, etcd watcher replacing polling in multiorch recovery, and `POSTGRES_USER`/`POSTGRES_PASSWORD` env vars replacing deprecated admin/internal user flags.

### Refactoring

- **Upstream flag alignment:** Removed hardcoded flags that match upstream defaults and dropped the `connpoolAdminPasswordEnvVar()` helper and `CONNPOOL_ADMIN_PASSWORD` env var, replaced by upstream's `POSTGRES_USER`/`POSTGRES_PASSWORD` mechanism.

### Testing

- **E2E test expansion:** Added 4 new shared-mode e2e test packages — scaling (4 subtests), templates (3 subtests), verification (3 subtests), and webhook (2 subtests) — with fixtures, framework helpers, and `syscall.Flock` for concurrent cluster setup. Brings automated e2e coverage from 4 to 16 subtests.
- **Exerciser scenario deduplication:** Removed 8 exerciser scenarios fully covered by the new e2e tests. Reduced exerciser catalog from 56 to 48 scenarios with a documented testing strategy split.

### Documentation

- **Durability policy docs updated:** All references to `ANY_2` and `MULTI_CELL_ANY_2` updated across documentation, samples, and design docs.
- **readOnly references removed:** Removed readOnly pool type references from README, samples, and design docs.

### Observer

- **New exerciser fixtures:** Added `log-levels` and `multi-pool` fixtures. Added debug `logLevels` to all 14 existing exerciser fixtures.
- **Fixture scenario updates:** Updated `multi-cell-quorum` fixture durability policy to `AT_LEAST_2`. Removed exerciser scenarios covered by e2e tests.

---

## [v0.5.3] — 2026-03-13

**Previous release:** v0.5.2 (2026-03-12)

Accelerates cluster bootstrap with parallel pod and PVC creation, hardens admission validation for pool names and resource limits, restructures the E2E test suite for parallel execution, and pins multigres images to upstream fixes for startup deadlocks and connection recycling. The observer gains a spec-compliance check for drift detection between CRD intent and running pod state.

**13 commits, 63 files changed, ~2,910 insertions.**

### Bug Fixes

- **Parallel pod and PVC creation:** Sequential pod creation required one reconcile loop per pod, making bootstrap time proportional to replica count. Removed `actionTaken` and unready-blocks-creation gates in `createMissingResources` so all missing pods and PVCs are created in a single pass. Deletion, drain, filesystem-resize, and rolling-update operations remain strictly sequential and health-gated. Reduces N-replica bootstrap from N reconcile loops to 1.
- **Pool name map key validation:** Kubernetes CRD structural schema does not enforce kubebuilder validation markers on map keys, allowing invalid pool names like `INVALID_NAME!` through the API server. These invalid names then caused PVC creation failures. Added `ValidatePoolName()` in `pkg/resolver/validation.go` with DNS label regex and 25-char length check, called after `ResolveShard` in the webhook validator. Extended `ShardTemplate` webhook to validate pool names on create, update, and delete.
- **Resource limit validation:** The webhook accepted resource specs where limits < requests, causing pods to fail at creation and clusters to get stuck in Progressing. Added `validateResourceRequirements` helper checking limits ≥ requests for all component resource specs (etcd, multiadmin, multiadmin-web, multigateway, multiorch, pool postgres, pool multipooler).

### Dependencies

- Upgrade multigres image `sha-f02e476` → `sha-bb174f9` and pgctld image `sha-f02e476` → `sha-9b51b0f`. Upstream fixes: HTTP server startup deadlock (server now starts before run hooks), connection recycle bugfix (`RESET ROLE`/`SESSION AUTHORIZATION` before `RESET ALL`), and `POSTGRES_DB` env var support.

### Refactoring

- **Stale TODO cleanup:** Removed resolved or obsolete TODO comments from RBAC markers in cell/shard/tablegroup/toposerver types, upstream dependency note in `main.go`, empty `main_test.go` placeholder, duplicate-cell TODOs resolved by webhook validation, and stale comments in `resource_watcher_match.go`. Consolidated multipooler/multiorch container TODOs to a single `--log-level` item each. Added explanatory comment on `IsPrimaryNotReady` optimistic default in `drain.go`.

### Testing

- **E2E test suite restructure:** Split monolithic E2E tests into dedicated and shared test directories (`test/e2e/dedicated/`, `test/e2e/shared/`) with per-scenario `main_test.go` files for parallel execution. Extracted shared helpers into `test/e2e/framework/` package (cluster management, helpers, loader). Updated CI workflows and Makefile targets for the new structure.
- **CI cache fix:** Fixed invalid cache name in the reusable test-coverage workflow causing cache misses.

### Observer

- **Spec-compliance check:** New `spec_compliance.go` with 4 sub-checks — pod resources, tolerations, images, and PVC sizes — compared against fully resolved Shard specs. Detects drift between CRD intent and actual pod state after template merging.
- **Multi-cell primary count fix:** New `countPrimariesForPool` helper correctly counts primaries across all cells when `len(poolSpec.Cells) > 1`, eliminating false positive findings for multi-cell clusters.
- **External etcd endpoint discovery:** `findEtcdAddress` now checks `GlobalTopoServer.External.Endpoints` before managed etcd lookup. `probeTopoServerServices` probes external etcd endpoints.
- **New exerciser fixtures:** Added `multiadmin-web`, `tolerations-affinity`, `multiadmin-lifecycle`, and `namespace-default-template` fixtures with 9 new verification scenarios (resource-limits-violated, add-tolerations, add-affinity, drain-primary-scale-down, multiadmin-scale, expand-pvc-storage, multiadminweb, pdb, durability-policy).

---

## [v0.5.2] — 2026-03-12

**Previous release:** v0.5.1 (2026-03-11)

Hardens data-plane reliability with startup probes on all remaining containers to prevent premature liveness kills during initialization, adds RFC 1123 DNS label validation on all user-provided name fields, and fixes cell gateway service name population and stale podRoles after primary election. The observer gains history tracking, on-demand check endpoints, 14 new health check categories, and a comprehensive exerciser skill for cluster smoke-testing.

**13 commits, 63 files changed, ~5,249 insertions.**

### Bug Fixes

- **Startup probes for remaining containers:** Without startup probes, pgctld's liveness probe could kill the container during slow initdb or WAL replay, and multiadmin/multiadmin-web used `InitialDelaySeconds` as a workaround. Added startup probes to pgctld (`/live` :15400), multiadmin (`/ready` :18000), and multiadmin-web (`/` :18100) with a 150s startup window (period=5s, failureThreshold=30). Removed now-redundant `InitialDelaySeconds` from multiadmin and multiadmin-web probes.
- **DNS name validation:** User-provided name fields accepted any characters (e.g., `INVALID_NAME`), which could produce invalid Kubernetes resource names downstream. Added RFC 1123 DNS label pattern validation to `DatabaseName`, `TableGroupName`, `ShardName`, `PoolName`, and `CellName` CRD types via CEL pattern constraints.
- **Cell gateway service name:** Cell status never populated `GatewayServiceName`, leaving clients with an empty string for gateway service discovery. Now set via `BuildMultiGatewayServiceName()` in the cell status reconciler.
- **Stale podRoles requeue:** The shard controller's reconciliation burst completed before multiorch elected a primary, leaving `podRoles` with 0 primaries indefinitely. Added a `hasPrimary` check with 10s requeue in `reconcileDataPlane` when the shard is Healthy but topology has no primary.

### Dependencies

- Upgrade multigres container images from `sha-8278fad` to `sha-f02e476`. Upstream changes: pprof endpoints enabled by default (no-op for operator), PrimaryObservation wired into multipooler health stream for faster gateway primary discovery.

### Testing

- **Startup probe tests:** Updated unit and integration test expected specs for startup probes on pgctld, multiadmin, and multiadmin-web containers.
- **Samples bump:** All sample manifests updated to `replicasPerCell: 4` minimum.

### Observer

- **History and on-demand check endpoints:** New `/api/history` endpoint with ring buffer tracking findings across cycles, classifying them as persistent, transient, or flapping. New `/api/check` endpoint for immediate targeted checks via channel-based dispatch. Added `--history-capacity` flag (default 30 cycles).
- **Coverage gap checks:** 14 new health check categories — PVC validation (missing/unbound/orphaned), service endpoint validation, backup staleness thresholds, status message checks on Degraded/Unknown phases, cell status field cross-checks, stuck-Progressing and invalid phase transition detection, and operator metrics HTTPS probe on port 8443.
- **Stale podRoles detection:** New `detectStalePodRoles` replication check runs `pg_is_in_recovery()` SQL probes when CRD shows 0 primaries and flags the mismatch.
- **Log pattern refinements:** Narrowed panic/runtime error patterns to avoid false positives during rolling restarts. Added filter for benign Go runtime shutdown panics. Fixed metrics probe to treat HTTP 401/403 as reachable. Fixed PVC orphan false positive during pod grace period.
- **Exercise cluster skill:** New comprehensive exerciser skill with 10 fixtures, 5 mutation patches, 11 scenarios, stability verification protocol, and operator-knowledge reference. Includes topology-aware kind cluster config with zone-labeled workers.
- **Documentation:** Rewrote observer README with skills as first-class citizens and example prompts section. Updated architecture, configuration, and diagnostic skill docs. Added `docs/gitops-and-webhook-defaults.md` documenting webhook default materialisation, intentionally dynamic fields, and GitOps compatibility.

---

## [v0.5.1] — 2026-03-11

**Previous release:** v0.5.0 (2026-03-10)

Hardens cluster reliability with etcd health probes, etcd replica immutability validation, full TemplateDefaults override-chain propagation, and OTEL sampling config support across all data-plane components. Also fixes observer false positives from probe-induced log noise and bumps multigres images to sha-8278fad.

**9 commits, 28 files changed, ~649 insertions.**

### Bug Fixes

- **Etcd health probes:** The etcd StatefulSet had no health probes, so Kubernetes could not detect or recover from unhealthy members. Added startup probe on `/readyz` (30 failure threshold for bootstrap/WAL replay), liveness probe on `/livez`, and readiness probe on `/readyz` — all via HTTP GET on the etcd client port (2379).
- **TemplateDefaults propagation:** The 4-level override chain (Component > Cluster Default Template > Namespace Default > Hardcoded) was broken for CoreTemplate and CellTemplate. `ResolveGlobalTopo`, `ResolveMultiAdmin`, and `ResolveMultiAdminWeb` now fall back to `TemplateDefaults.CoreTemplate`. CellTemplate default is propagated before `ResolveCell`. The webhook validator now resolves `TemplateDefaults.ShardTemplate` for accurate orphan pool checks.
- **Etcd replica immutability:** CEL rule rejects even replica counts. Webhook warns on `replicas=1` (single-node etcd is not HA) and rejects replica changes on Update to prevent cluster ID mismatch from static bootstrap re-initialization.
- **OTEL sampling config propagation:** `SamplingConfigRef` was accepted by the API but never wired through. Now sets `OTEL_TRACES_SAMPLER_CONFIG` env var and mounts the ConfigMap volume into pool pods, multigateway, multiadmin, and multiorch containers. Fixes `CrashLoopBackOff` when using `multigres_custom` sampler.
- **Multiorch sampling volume:** The multiorch container had an `otel-sampling-config` VolumeMount but the corresponding Volume was never added to the Deployment pod spec. Volume is now conditionally attached when `SamplingConfigRef` is configured.
- **Observer probe noise:** TCP health probes to the gateway PG port caused EOF log entries at ERROR level, which the observer then reported as findings. New `isProbeNoise()` filter suppresses these false positives. Grace period bumped from 60s to 2min with multiorch and gateway probes skipped during grace.
- **Observer JSON log parsing:** Log severity is now extracted from JSON `level` fields. Severity is elevated for `ShardNeedsBootstrap` and quorum errors.
- **Observer RBAC:** Added `watch` verb for nodes (controller-runtime cached client requires watch, not just list).

### Dependencies

- Upgrade multigres container images from `sha-d6c1048` to `sha-8278fad` (includes fix for admin conn action lock starvation).

### Testing

- **Etcd probe tests:** Unit tests for probe configuration and integration tests verifying probe fields on the generated StatefulSet.
- **Resolver tests:** Test cases for TemplateDefaults propagation across MultiAdmin, MultiAdminWeb, GlobalTopo, and the webhook validator.
- **Etcd validation tests:** Test cases for `getEffectiveEtcdReplicas` helper in resolver validation.

---

## [v0.5.0] — 2026-03-10

**Previous release:** v0.4.2 (2026-03-09)

Adds configurable durability policy, graceful deletion for orphan TableGroups and Cells, and consolidates the repository from 11 Go modules into one. Fixes native sidecar crash detection, multiorch cross-shard watch leakage, and health probe endpoints for multipooler and pgctld.

**17 commits, 90 files changed, ~1,551 insertions.**

### Features

- **Configurable durability policy:** New `DurabilityPolicy` field on `MultigresClusterSpec` (cluster-wide default) and `DatabaseConfig` (per-database override), propagated through TableGroup → Shard → topology registration. Webhook materializes `"ANY_2"` as default. Enables future cross-AZ replication quorum (`MULTI_CELL_ANY_2`) and user-defined policies.
- **Graceful orphan TableGroup/Cell deletion:** Orphan pruning now follows a 3-step `PendingDeletion` flow (annotate → wait for `ReadyForDeletion` → delete) instead of immediate deletion, routing through the drain state machine to prevent data loss during database or cell removal.
- **TopoServer cleanup on external mode switch:** Switching from embedded to external topo mode now deletes the orphaned TopoServer resource in `reconcile_global.go`.

### Bug Fixes

- **Native sidecar crash detection:** `IsCrashLooping` now checks `InitContainerStatuses` in addition to `ContainerStatuses`, correctly surfacing crash-looping native sidecars (e.g., multipooler) as `PhaseDegraded`.
- **Multiorch cross-shard watch leakage:** `--watch-targets` was hardcoded to the database level, causing each multiorch to discover multipoolers across all shards. Now builds a fully qualified target from database/tablegroup/shard fields.
- **Multipooler health probes:** Startup and readiness probes now use `/live` instead of `/ready`, matching upstream endpoint changes (multigres commit `309da86`).
- **pgctld health probes:** Liveness and readiness probes added using the `/live` endpoint on port 15400, with `--http-port=15400` flag wired into the container args.
- **Malformed backup ID ages:** `EvaluateBackups` now guards against zero-time from `ParseBackupTime`, preventing ~56-year ages in Prometheus metrics and misleading `BackupStale` conditions.
- **PVC ownerRef update conflicts:** Replaced `r.Update()` with `client.MergeFrom` patch for PVC ownerRef changes, reducing conflict errors during concurrent reconciliation.
- **Hardcoded label string:** Replaced hardcoded `"multigres.com/cluster"` string with `metadata.LabelMultigresCluster` constant.

### Dependencies

- Upgrade multigres container images from `sha-8bfe693` to `sha-d6c1048` (pgctld, multigres core, multiorch, multipooler, multigateway).
- Intermediate builder image updated in Dockerfile and CI workflow.

### Refactoring

- **Single Go module consolidation:** Merged 11 Go modules (root + 10 sub-modules under `api/` and `pkg/`) into a single `go.mod`, eliminating ~3,500 lines of duplicated dependency management. Removed `go.work` workspace, simplified Makefile and CI workflows. No external consumers of the sub-modules exist.
- **Webhook test fixture isolation:** Replaced shared test fixtures with `DeepCopy` to prevent cross-test data races.
- **Comment and label fixes:** Fixed copy-paste comment typo in `labels.go`, removed stale `hasInline` comments from `defaulter.go`, added CEL exclusion comment to `buildCellNodeSelector`.

### Testing

- **Test coverage increase:** New tests for `topology.go`, `store_test.go`, `conditions_test.go`, `phase_test.go`, `shard_controller_internal_test.go`, `multiorch_test.go`, `database_test.go`, and `builders_test.go`.

### Documentation

- New `docs/durability-policy.md` explaining configurable replication quorum.
- Updated API design doc with `DurabilityPolicy` field specification.
- Updated admission controller doc with durability policy defaulting.
- Updated `cell-topology.md` with durability policy propagation path.

---

## [v0.4.2] — 2026-03-09

**Previous release:** v0.4.1 (2026-03-05)

Introduces the Multigres Observer — a standalone cluster health monitoring tool — along with degraded phase detection across all controllers, drain state machine hardening, topology pruning propagation, and materialized webhook defaults.

**34 commits, 95 files changed, ~8,300 insertions.**

### Features

- **Multigres Observer tool:** New standalone tool (`tools/observer/`) for continuous cluster health monitoring with 10 check categories (pod health, resource validation, CRD status, drain state, connectivity, replication, log monitoring, events, topology validation, and operator logs). Includes Prometheus metrics, structured JSON logging, `/api/status` endpoint, startup grace period, Dockerfile, and Kustomize deploy manifests. New Makefile targets: `kind-build-observer`, `kind-load-observer`, `kind-deploy-observer`, `kind-undeploy-observer`.
- **Degraded phase detection:** All controllers (Cell, TopoServer, Shard) now detect crash-looping pods (`CrashLoopBackOff`, `OOMKilled`, `ImagePullBackOff`, or terminated with ≥3 restarts) and report `PhaseDegraded` instead of `PhaseProgressing`. MultigresCluster aggregates `PhaseDegraded` from child Cells, TableGroups, and TopoServers.
- **Periodic status re-reconciliation:** Cell and TopoServer controllers requeue after 30s when not `Healthy`, enabling detection of pod-level issues (e.g., CrashLoopBackOff) that don't trigger watch events.
- **Materialized cluster defaults:** Webhook defaulter now materializes `TopologyPruning`, `PVCDeletionPolicy`, `Backup`, etcd `RootPath`, and external topo `Implementation` into the persisted spec, solving the "invisible defaults" problem for these fields.
- **Stale drain cancellation:** Drains in `Requested` state are automatically cancelled when the desired state changes (e.g., scale-down reversed), preventing stuck `Progressing` phases.
- **Topology pruning propagation:** `TopologyPruning` config propagated from MultigresCluster → TableGroup → Shard via the builder chain and new `TopologyPruning` fields on `ShardSpec` and `TableGroupSpec`. CRD schemas updated accordingly.
- **TopoServer status aggregation:** MultigresCluster status now includes TopoServer phase in its health aggregation alongside Cells and TableGroups.

### Bug Fixes

- **Drain: check primary pod readiness before RPCs.** New `IsPrimaryNotReady` guard in `DrainStateDraining` and `DrainStateAcknowledged` prevents sending RPCs to a primary whose containers are not ready. Drains are delayed and requeued instead.
- **Drain: reload PostgreSQL config after sync standby removal.** `ReloadConfig: true` added to `UpdateSynchronousStandbyList` requests so PostgreSQL reloads `synchronous_standby_names` immediately.
- **PodRoles keying fix.** `GetPoolerStatus` now keys by actual Kubernetes pod name (via `PodMatchesPooler` FQDN-aware matching) instead of topology hostname, preventing stale entries from orphaned topology data.
- **Multipooler service ID shortening.** Shorten `--service-id` value to prevent `application_name` overflow at the 63-character `NAMEDATALEN` limit, which was causing synchronous replication matching failures.
- **pgctld `POSTGRES_PASSWORD` env var.** Switch from `PGPASSWORD` to `POSTGRES_PASSWORD` for the pgctld container.
- **Topology defaults fix.** `getGlobalTopoRef` properly branches on `External` vs `Etcd` instead of unconditionally defaulting `rootPath` and `implementation`.
- **Draining pods excluded from ready counts.** Pods with drain annotations are no longer counted as ready in shard status, preventing inflated readiness metrics.
- **Configurable topology pruning.** `reconcilePoolerPrune` now respects the `TopologyPruning.Enabled` flag and skips pruning when disabled.

### Dependencies

- Upgrade multigres container images from `sha-4f39f4a` to `sha-8bfe693` (pgctld, multigres core, multiadmin-web, multiorch, multipooler, multigateway).
- Internal Go module dependencies updated across all modules.

### Documentation

- New `docs/development/phase-lifecycle.md` explaining phase computation across all resources.
- Updated `pod-management-architecture.md` drain state machine and status aggregation sections.
- Fixed stale StatefulSet references in `operator-capability-levels.md` and observer docs.
- Updated `CONTRIBUTING.md` with observer Makefile targets and project structure.

---

## [v0.4.1] — 2026-03-05

**Previous release:** v0.4.0 (2026-03-03)

Maintenance release with container image upgrades, observability improvements, bug fixes, and a major documentation reorganization. No breaking changes.

**69 files changed across 10 PRs (#335–#344).**

### Features

- Upgrade multigres container images from `sha-b0a47e1` to `sha-4f39f4a` (pgctld, multigres core, multiorch, multipooler, multigateway).
- New Grafana dashboard (`grafana-dashboard-cluster.json`) with panels for backup age, drain operations, rolling update progress, and spec-hash drifted pods.
- Three new Prometheus alerting rules: `MultigresBackupStale` (backup older than 24h), `MultigresRollingUpdateStuck` (rolling update >30min), `MultigresDrainTimeout` (drain timeouts for 10min).
- Prometheus alert and saturation rules now cover all five controllers (multigrescluster, tablegroup, shard, cell, toposerver) instead of only multigrescluster and tablegroup.
- E2E test timeout increased from 20m to 30m with parallelism set to 2.

### Bug Fixes

- **Multipooler `--hostname` flag removed:** The `--hostname=$(POD_NAME)` flag added in v0.4.0 caused multipooler to register with a short name that collided with FQDN-based pooler topology entries. Removed to restore correct registration behavior.
- **Pod cache filter added:** Pods were not included in the split-brain cache filter, causing the operator to cache all pods in the cluster. Pods are now filtered by `app.kubernetes.io/managed-by` label like other high-volume resources.
- **Deprecated `result.Requeue` removed:** Replaced deprecated `result.Requeue` with `result.RequeueAfter > 0` in the shard controller data-plane reconciliation path, fixing a staticcheck SA1019 warning.

### Testing

- E2E tests adapted to pod-based pool management: replaced `waitForStatefulSetWithContainer` assertions with `waitForPodWithContainer` to match the v0.4.0 architecture change from StatefulSets to direct Pods.
- Removed zone constraints from E2E test cell configs to allow scheduling on Kind nodes without zone topology labels.

### Documentation

- **README slimmed down:** Moved detailed sections (cert-manager integration, observability, webhook demo, local development) to dedicated files under `demo/` and `docs/`.
- **New user-facing docs:** `docs/storage.md` (PVC lifecycle and volume expansion), `docs/configuration.md` (operator flags), `docs/observability.md` (monitoring setup), `docs/backup-restore.md` (backup guide), `docs/operator-capability-levels.md` (capability assessment with diagram).
- **New developer docs:** `docs/development/` now contains 12 focused documents covering caching strategy, cell topology, certificate management, controller patterns, naming strategy, observability internals, pod management architecture, PostgreSQL image strategy, PVC lifecycle, template propagation, backup architecture, and known behaviors.
- **New demo directories:** `demo/cert-manager/README.md`, `demo/observability/README.md`, `demo/webhook/demo.md` with full step-by-step guides.
- **New runbooks:** `docs/monitoring/runbooks/` with runbooks for `MultigresBackupStale`, `MultigresDrainTimeout`, and `MultigresRollingUpdateStuck` alerts.
- **CONTRIBUTING.md** created with local development setup, testing, and PR guidelines.
- Fixed 15 stale references across all documentation.
- Removed obsolete `docs/architecture.md`, `docs/implementation-guide.md`, `docs/interface-design.md`, and `plans/phase-1/implementation-notes.md`.
- Removed stale E2E config files (`pkg/testutil/e2e-config/`) that referenced the old StatefulSet-based architecture.

---

## [v0.4.0] — 2026-03-03

**Previous release:** v0.3.2 (2026-03-01)

This release delivers the **data-handler refactor** — a major architectural change that merges the separate data-handler controllers into the resource-handler controllers, eliminating cross-controller race conditions, removing all finalizers, and centralizing topology management. It also adds PVC volume expansion support, pod tolerations, and fixes 14+ bugs across drain, topology, deletion, and backup health paths.

**97 files changed, ~7,800 insertions, ~9,800 deletions across 5 PRs (#328, #330–#334).**

### Breaking Changes

- **Finalizers removed from all controllers.** MultigresCluster, Shard, and TopoServer controllers no longer use finalizers. Resource cleanup now relies on Kubernetes garbage collection via owner references. Existing resources with stale finalizers will have them removed on the next reconcile.
- **Data-handler controllers deleted.** The separate cell and shard data-handler controllers (`pkg/data-handler/controller/cell/`, `pkg/data-handler/controller/shard/`) have been removed. Their responsibilities are now handled by the cluster-handler and resource-handler controllers respectively.
- **TopologyPruningConfig added to MultigresClusterSpec.** Topology pruning (cells, databases, poolers) is now managed by the MultigresCluster controller with a new `TopologyPruningConfig` field (enabled by default).

### Features

- Operator now supports in-place PVC resizing: update the storage size in the spec and the operator patches data PVCs when `desired > current`.
- Automatic pod drain triggered when pending filesystem resize is detected (`FileSystemResizePending` condition).
- Webhook rejects storage shrink attempts with a validation error.
- Warning event emitted on Shard when PVC expansion fails (e.g., StorageClass doesn't support expansion).
- New `Tolerations` field on `PoolSpec`, wired into `BuildPoolPod`, `ComputeSpecHash`, and `mergePoolSpec`.
- New `AnnotationPendingDeletion` and `ConditionReadyForDeletion` constants for graceful shard deletion.
- TableGroup controller now uses a 3-step deletion: set PendingDeletion annotation → wait for ReadyForDeletion condition → delete Shard CR.
- Tracking labels (`uses-core-template`, `uses-cell-template`, `uses-shard-template`) applied via `MergeFrom` patch. Webhook `TemplateValidator.ValidateDelete` now uses indexed label lookups.

### Bug Fixes

- **Drain fallthrough on topo unavailability:** The drain state machine fell through with `myPooler=nil` when topo was unavailable. Now requeues instead.
- **Drain error swallowed:** `reconcileDrainState` was silently discarding errors. Errors are now returned.
- **Primary drain check blocked replicas:** `IsPrimaryDraining` returned true for `DrainStateReadyForDeletion`. Now excludes completed drains.
- **Drain timestamp parsing:** Malformed annotations now log a warning and fall back to `time.Now()`.
- **Pooler pruning FQDN mismatch:** `PrunePoolers` compared FQDN hostnames against short pod names. Now uses FQDN-aware helper.
- **Pooler status key garbled:** `GetPoolerStatus` hostname fallback now prefers `Id.Name`.
- **Selector cross-shard interference:** Four functions used only cluster+shard labels. Now include database and tablegroup labels.
- **Orphan deletion races:** `IsNotFound` guards added to prevent duplicate deletion events.
- **Stuck terminating pods:** Pods stuck >60s during graceful deletion are now treated as gone.
- **PVC deletion policy enforcement:** PVCs with Delete policy now get controller ownerRefs for GC cascade-deletion.
- **Nil panic:** `EvaluateBackupHealth` could panic on nil gRPC response. Nil guard added.
- **Backup event spam:** `BackupStale` event now gated on healthy-to-unhealthy transition only.
- **S3 backup fields:** `MinLength=1` validation added to `Bucket` and `Region`.
- **Multipooler hostname:** `--hostname=$(POD_NAME)` now passed to multipooler container.

### Refactoring

- Topo, drain, and backup health logic extracted into standalone packages.
- Shard controller split into `reconcile_deletion.go` and `reconcile_data_plane.go`.
- Resolver split: validation functions extracted into `validation.go`.

---

## [v0.3.2] — 2026-03-01

Safety improvements for shard scale-down operations, more robust topology cleanup during deletion, and significant maintenance updates.

### Features

- Health gate for scale-down: blocks drain operations if remaining pool replicas are unhealthy. Includes `ScaleDownBlocked` warning event.

### Bug Fixes

- Smart topology cleanup during deletion: 2-minute retry for transient topology server unavailability. `TopologyRegistered` / `DatabaseRegistered` conditions for accurate tracking.
- Track drain timeout from drain request start via `drain.multigres.com/requested-at` annotation instead of `DeletionTimestamp`.
- Prevent status patch errors from being swallowed in Shard and Cell controllers.

### Refactoring

- Extract shared `SetCondition`/`IsConditionTrue` helpers into `pkg/util/status/conditions.go`.
- Add missing `doc.go` files to several packages.
- Increase code coverage across all modules.

---

## [v0.3.1] — 2026-02-27

### Features

- IRSA (IAM Roles for Service Accounts) support for S3 authentication.
- Enforce minimum `replicasPerCell` and add quorum loss warnings.

### Bug Fixes

- Resolve status ownership conflicts during reconciliation.
- Ensure backup PVCs are reconciled for all pool-active cells.
- Fix deadlock during scale-down when extra pods are externally deleted.
- Skip drain state machine reconciliation for pods marked for deletion.
- Resolve multiple topo-ordering and reconciliation issues found during bug audit.
- Fix infinite loop during PRIMARY pod drain operations.

### Chores

- Pin multigres container images to specific SHA tags for production stability.

---

## [v0.3.0] — 2026-02-26

### Highlights

- Direct pod management replaces StatefulSet-based pool management.
- Full E2E testing framework with Kind clusters and psql connectivity verification.
- Container images pinned to SHA tags for reproducible builds.

### Features

- Replace StatefulSet pool management with direct Pods: pod, PVC, and PDB builders; sequential pooler pod management; rolling updates and drained pod replacement.
- Implement drain state machine for scale-down.
- Add backup health reporting.
- Add cell append-only validation rules.
- Add E2E testing framework with Kind clusters and psql connectivity verification.

### Bug Fixes

- Prevent PVC deletion during rolling updates.
- Reorder PVC deletion and finalizer removal.
- Implement manual PVC GC to respect retention policies.
- Prevent nil pointer panic in drain state machine.
- Include `ValueFrom` and `EnvFrom` in spec-hash calculation.
- Resolve cross-shard state bleed and status memory leak.
- Correct Shard status to use desired replica counts.
- Fix drain state matching and topology store residency.
- Resolve deletion deadlock and PDB scale subresource warning.

### Refactoring

- Split monolithic shard controller into focused files.
- Remove legacy StatefulSet management and cleanup RBAC.
- Consolidate image defaults to central source of truth.

---

## [v0.2.6] — 2026-02-24

### Features

- Strict node scheduling for cell failure domains: injects `nodeSelector` constraints on MultiGateway Deployments, Pool StatefulSets, and MultiOrch Deployments based on cell zone/region topology. Webhook emits warnings when no matching nodes exist.

### Bug Fixes

- Guard against infinite recursion in PKI: `ensureCA` and `ensureServerCert` now have max recursion depth of 3 with clear error messages.

### Documentation

- Add pod management design document covering resource topology changes, pod identity management, drain state machine, rolling updates, and failure recovery.

---

## [v0.2.5] — 2026-02-23

### Features

- Filter template-triggered reconciliations by resolved refs so updating one template no longer enqueues every cluster in the namespace.
- Add `ResolvedTemplates` status field tracking which templates each cluster uses.
- Gate `localTopoServer` field behind CEL validation with "not yet supported" message.

### Bug Fixes

- Add `app.kubernetes.io/managed-by` label to CA and TLS secrets generated by the CertRotator, fixing informer cache misses leading to infinite Create/AlreadyExists recursion.
- Add `watch` verb to StorageClass RBAC role.

### Refactoring

- Remove dead `Enable`, `CertStrategy`, and `CertDir` fields from webhook options.

---

## [v0.2.4] — 2026-02-21

### Bug Fixes

- Aggregate pool metrics across cells instead of reporting only the last cell's count.
- Merge `StorageSpec` field-by-field in `mergePoolSpec` and `mergeEtcdSpec` to preserve unrelated base fields on partial overrides.
- Generate unique ConfigMap and Secret names per Shard to prevent cross-shard ownership conflicts.
- Remove dead pgctld `--backup-*` CLI flags that caused CrashLoopBackOff on all pool pods.

### Documentation

- Expand secret access patterns guide with decision table and three documented options.
- Document silent HA loss with RWO filesystem backups and `WaitForFirstConsumer` node pinning.

---

## [v0.2.3] — 2026-02-20

### Bug Fixes

- Listen on dual-stack for multiadmin-web: set `HOSTNAME=::` so Next.js listens on both IPv4 and IPv6, fixing liveness probe failures on dual-stack EKS clusters.

---

## [v0.2.2] — 2026-02-20

### Bug Fixes

- Survive SSA cert wipe on upgrade: `PatchWebhookCABundle` now uses Server-Side Apply with a dedicated field owner (`multigres-operator-cert`). `multigres.com/cert-strategy` annotation set at runtime for reliable self-signed mode detection.
- Handle `AlreadyExists` race in secret creation when multiple reconcilers create certs concurrently.

---

## [v0.2.1] — 2026-02-20

### Features

- StorageClass validation in the admission webhook: rejects `MultigresCluster` creation when no default StorageClass exists and no explicit `storage.class` is set.

### Bug Fixes

- Skip topo cleanup on deletion when cell/shard controllers cannot reach the topo server, preventing finalizer deadlocks.

---

## [v0.2.0] — 2026-02-20

### Highlights

- Backup & Restore with filesystem and S3 backend support.
- Generic TLS certificate lifecycle module (`pkg/cert`).
- pgBackRest TLS with auto-generated and user-provided certificate modes.

### Features

- Backup configuration API with filesystem and S3 backend support.
- S3 backup support with configurable endpoint, bucket, region, key prefix, and env-based credentials.
- pgBackRest TLS certificate handling with dual-mode support (auto-generated and user-provided/cert-manager compatible).
- Projected volume key renaming for upstream compatibility.
- Backup configuration resolution with filesystem/S3 backend validation.
- Per-shard backup PVC name generation.

### Refactoring

- Migrate webhook certificate handling from `pkg/webhook/cert` to generic `pkg/cert` module.
- Wire backup configuration into pgctld and multipooler container args.
- Pass S3 environment variables to data plane containers.
- Use APIReader for uncached Secret lookups.

---

## [v0.1.0] — 2026-02-17

First release.
