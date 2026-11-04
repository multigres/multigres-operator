# Upstream Multigres Test Patterns for E2E Testing

## Overview

Research into the upstream multigres project (`github.com/multigres/multigres v0.0.0-20260206234310-62e1d947565c`)
to inform e2e testing of the multigres-operator.

The upstream project has extensive e2e test infrastructure in `go/test/endtoend/` with 242 test files
(37.6% of the codebase). Their tests run processes directly on the host (not in Kubernetes), but the
patterns, configuration, and health checking are directly applicable to our operator e2e tests.

---

## 1. Component Architecture & Startup Order

### Dependency Graph

```
Client (psql)
    |
    v
Multigateway (PG protocol proxy, port 5432)
    | gRPC
    v
Multipooler (connection pooling, gRPC port 16100)  <-->  pgctld (PG control daemon, gRPC port 16200)
    |                                                          |
    v                                                          v
  etcd (topology store, port 2379)                      PostgreSQL (port 5432)
    ^
    |
Multiorch (orchestration/failover, gRPC 17100, HTTP 17000)
```

### Required Startup Order

1. **etcd** (TopoServer) - must be ready first
2. **createclustermetadata** job - creates cell and database metadata in etcd
3. **multipooler StatefulSet** (includes pgctld + multipooler + pgbackrest sidecars)
4. **multiorch** - bootstraps the shard (initializes postgres, elects primary, configures replication)
5. **multigateway** - discovers poolers from topology, starts accepting PG connections

The operator must wait for each phase before proceeding. The key insight is that **multiorch
bootstraps the shard automatically** - once multipoolers are registered in etcd, multiorch will
detect uninitialized nodes, init postgres on one (primary), and configure replication to others.

---

## 2. Cluster Metadata Initialization

### createclustermetadata Job

Before any multigres component can work, cluster metadata must be created in etcd.
The upstream uses a CLI command for this:

```yaml
# From demo/k8s/k8s-createclustermetadata-job.yaml
command: ["/multigres/bin/multigres"]
args:
  - createclustermetadata
  - --global-topo-address=etcd:2379
  - --global-topo-root=/multigres/test/global
  - --cells=zone1
  - --durability-policy=AT_LEAST_2
  - --backup-location=/backups
```

This creates:
- **Cell entry** in global topology (maps cell name -> etcd addresses + root path)
- **Database entry** with backup location and durability policy

### In the upstream e2e tests (programmatic equivalent):

```go
// From shardsetup/setup.go:302-349
ts, err := topoclient.OpenServer("etcd", globalRoot, []string{etcdClientAddr}, topoclient.NewDefaultTopoConfig())

// Create cell
err = ts.CreateCell(ctx, cellName, &clustermetadatapb.Cell{
    ServerAddresses: []string{etcdClientAddr},
    Root:            cellRoot,
})

// Create database
err = ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
    Name:             database,
    BackupLocation:   backupLocation,
    DurabilityPolicy: "AT_LEAST_2",
})
```

### For our operator e2e tests:
The operator already generates a multiadmin Deployment that handles metadata initialization.
We need to verify the multiadmin pod completes successfully before checking other components.

---

## 3. Health Check Endpoints

### Multigateway

| Endpoint | Port | Purpose | Ready Condition |
|----------|------|---------|-----------------|
| `/ready` | HTTP (15000) | Readiness probe | `InitError` is empty string |
| `/live` | HTTP (15000) | Liveness probe | Always 200 (registered but implementation is the same handler framework) |
| `/` | HTTP (15000) | Status page with pooler discovery info | N/A |

**Implementation** (`go/services/multigateway/status.go:104-116`):
```go
func (mg *MultiGateway) handleReady(w http.ResponseWriter, r *http.Request) {
    mg.serverStatus.mu.Lock()
    defer mg.serverStatus.mu.Unlock()
    isReady := (len(mg.serverStatus.InitError) == 0)
    if !isReady {
        w.WriteHeader(http.StatusServiceUnavailable) // 503
    }
}
```

The `InitError` is set by the topology registration alarm callback (`toporeg.Register()`).
Gateway is ready when it connects to etcd and registers itself - but this does NOT mean it
can serve queries. Query serving requires pooler discovery.

### Multipooler

| Endpoint | Port | Purpose |
|----------|------|---------|
| `/ready` | HTTP (16000) | Readiness probe |
| `/live` | HTTP (16000) | Liveness probe |
| `/` | HTTP (16000) | Status page |

### Multiorch

| Endpoint | Port | Purpose |
|----------|------|---------|
| `/ready` | HTTP (17000) | Readiness probe |
| `/live` | HTTP (17000) | Liveness probe |
| `/` | HTTP (17000) | Status page |

### Key Insight: Readiness != Query-Serving

The upstream tests have a separate `WaitForMultigatewayQueryServing()` that polls with
`SELECT 1` via the PG protocol, because HTTP `/ready` only means "registered with topology",
not "can route queries to poolers":

```go
// From shardsetup/cluster.go:350-380
func (s *ShardSetup) WaitForMultigatewayQueryServing(t *testing.T) {
    connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
        s.MultigatewayPgPort, TestPostgresPassword)

    // Poll every 200ms, timeout after 5s
    ticker := time.NewTicker(200 * time.Millisecond)
    for {
        db, err := sql.Open("postgres", connStr)
        if err != nil { continue }
        var result int
        err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
        db.Close()
        if err == nil && result == 1 { return }
    }
}
```

### For our operator e2e tests:
1. Use `kubectl wait --for=condition=ready` for basic pod readiness (HTTP probes)
2. Then separately verify query serving with a `SELECT 1` through multigateway's PG port
3. The K8s probes in the upstream demo manifests:
   - Multigateway: `readinessProbe: httpGet /ready port 15000` (initialDelaySeconds=5, periodSeconds=5)
   - Multipooler: `livenessProbe: httpGet /live port 16000` (initialDelaySeconds=10, periodSeconds=10)
   - Multiorch: `livenessProbe: httpGet /live port 17000` (initialDelaySeconds=10, periodSeconds=10)

---

## 4. Component Configuration for Minimal Test Cluster

### Multigateway Args (from demo/k8s/k8s-multigateway.yaml)

```
--http-port=15000
--grpc-port=15100
--pg-port=5432
--hostname=$(POD_IP)
--topo-global-server-addresses=etcd:2379
--topo-global-root=/multigres/test/global
--cell=zone1
--pprof-http=true
```

### Multipooler Args (from demo/k8s/k8s-multipooler-statefulset.yaml)

```
--http-port=16000
--grpc-port=16100
--topo-global-server-addresses=etcd:2379
--topo-global-root=/multigres/test/global
--cell=zone1
--database=postgres
--table-group=default
--shard=0-inf
--service-id=$(POD_INDEX)
--pooler-dir=/data
--pgctld-addr=localhost:16200
--pg-port=5432

--socket-file=/data/pg_sockets/.s.PGSQL.5432
--grpc-socket-file=/data/multipooler.sock
--service-map=grpc-pooler
--pgbackrest-control-socket=/run/pgbackrest-control/control.sock
--pgbackrest-wal-socket=/run/pgbackrest-wal/wal.sock
```

### pgctld Args (sidecar in multipooler pod)

```
server
--pooler-dir=/data
--grpc-port=16200
--pg-port=5432
--grpc-socket-file=/data/pgctld.sock
--pg-hba-template=/etc/pgctld/pg_hba_template.conf
```

### Multiorch Args (from demo/k8s/k8s-multiorch.yaml)

```
--http-port=17000
--grpc-port=17100
--hostname=$(POD_IP)
--topo-global-server-addresses=etcd:2379
--topo-global-root=/multigres/test/global
--cell=zone1
--watch-targets=postgres
--pooler-health-check-interval=500ms
--recovery-cycle-interval=500ms
```

### Minimum Required Parameters

| Component | Required Params |
|-----------|----------------|
| etcd | Standard etcd flags |
| createclustermetadata | `--global-topo-address`, `--global-topo-root`, `--cells`, `--durability-policy`, `--backup-location` |
| multigateway | `--topo-global-server-addresses`, `--topo-global-root`, `--cell`, `--pg-port` |
| multipooler | `--topo-global-server-addresses`, `--topo-global-root`, `--cell`, `--database`, `--table-group`, `--shard`, `--pgctld-addr`, `--pooler-dir`, `--pg-port`, `--socket-file` |
| pgctld | `server`, `--pooler-dir`, `--grpc-port`, `--pg-port` |
| multiorch | `--topo-global-server-addresses`, `--topo-global-root`, `--cell`, `--watch-targets` |

### Authentication

The upstream tests use a hardcoded password:
```go
TestPostgresPassword = "test_password_123"
```

Set via `PGPASSWORD` environment variable before pgctld initializes PostgreSQL.
The demo K8s manifests use `` and trust auth in pg_hba.conf
for all connections (appropriate for testing).

---

## 5. Upstream E2E Test Patterns

### Test Infrastructure Architecture

The upstream uses `shardsetup.ShardSetup` as the central test infrastructure:

```go
// Functional options pattern for configuration
setup := shardsetup.New(t,
    shardsetup.WithMultipoolerCount(2),   // primary + 1 standby
    shardsetup.WithMultiOrchCount(1),     // 1 orchestrator
    shardsetup.WithMultigateway(),        // enable gateway
    shardsetup.WithDatabase("postgres"),
    shardsetup.WithCellName("test-cell"),
    shardsetup.WithDurabilityPolicy("AT_LEAST_2"),
)
```

### Shared Setup with Test Isolation

Tests share infrastructure but have clean state isolation:

```go
// In main_test.go
var setupManager = shardsetup.NewSharedSetupManager(func(t *testing.T) *shardsetup.ShardSetup {
    return shardsetup.New(t,
        shardsetup.WithMultipoolerCount(2),
        shardsetup.WithMultiOrchCount(1),
        shardsetup.WithMultigateway(),
    )
})

func getSharedSetup(t *testing.T) *shardsetup.ShardSetup {
    return setupManager.Get(t)
}

// In each test
func TestSomething(t *testing.T) {
    setup := getSharedSetup(t)
    setup.SetupTest(t)  // Validates clean state + registers cleanup
    // ... test code ...
}
```

### Bootstrap Flow (What Happens During Setup)

1. Start etcd, create topology cell + database
2. For each multipooler node: start pgctld + multipooler (postgres NOT initialized yet)
3. Start multiorch - it discovers uninitialized poolers via topology
4. Multiorch bootstraps: picks one node as primary, runs `pgctld init`, configures replication
5. Wait for all nodes to be initialized (primary has sync replication, replicas have primary_conninfo)
6. Start multigateway (discovers poolers from topology)
7. Verify multigateway can execute `SELECT 1`

**Bootstrap timeout: 60 seconds** (polling every 1 second)

### Query Serving Test Pattern

```go
// From queryserving/multigateway_pg_test.go
connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
    setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
db, err := sql.Open("postgres", connStr)
require.NoError(t, err)
defer db.Close()

// Verify connectivity
err = db.PingContext(ctx)
require.NoError(t, err)

// Execute queries
var result int
err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
require.NoError(t, err)
```

### Isolated Setup (Destructive Tests)

For tests that kill primaries or perform destructive operations:

```go
setup, cleanup := shardsetup.NewIsolated(t,
    shardsetup.WithMultipoolerCount(3),
    shardsetup.WithMultiOrchCount(2),
)
defer cleanup()
// Kill primary, verify failover, etc.
```

---

## 6. CI/CD Test Setup

### GitHub Actions Workflow (test-integration.yml)

```yaml
steps:
  - Checkout code
  - Setup Go
  - Setup test environment (.github/scripts/setup-test-environment.sh)
  - Build: make build
  - Run: go test -json -v -skip TestPostgreSQLRegression ./go/test/endtoend/...
    env:
      TEST_PRINT_LOGS: "1"
```

### Makefile Targets

```makefile
make test          # All tests (proto + build + tests)
make test-short    # Unit tests only (skip PostgreSQL integration)
make test-race     # Race condition detection
make test-coverage # Coverage report
```

### Test Skip Patterns

```go
if testing.Short() {
    t.Skip("skipping in short mode")
}
if utils.ShouldSkipRealPostgres() {
    t.Skip("PostgreSQL binaries not found")
}
```

---

## 7. Kubernetes-Specific Patterns (from demo/k8s/)

### Kind Cluster Setup

```yaml
# kind.yaml - cluster configuration
# Uses hostPath volumes for data sharing between pods
```

### Launch Order in K8s

1. `launch-infra.sh`:
   - `kind create cluster`
   - Load docker images into kind
   - Deploy etcd StatefulSet
   - Deploy observability stack
   - Run createclustermetadata job
   - Install cert-manager
   - Create pgBackRest TLS certificates
   - Deploy multiadmin

2. `launch-multigres-cluster.sh`:
   - Deploy multipooler StatefulSet
   - Deploy multiorch Deployment
   - Deploy multigateway Deployment
   - `kubectl wait --for=condition=ready` on each

### Pod Readiness Waits

```bash
kubectl wait --for=condition=ready pod -l app=multipooler --timeout=180s
kubectl wait --for=condition=ready pod -l app=multiorch --timeout=120s
kubectl wait --for=condition=ready pod -l app=multigateway --timeout=120s
```

Note the generous timeouts: multipooler gets 180s because it includes PostgreSQL initialization.

### Volume Layout

```
/data/<pod-name>/           # Per-pod data directory (subPathExpr)
  pg_sockets/               # PostgreSQL Unix sockets
  pgbackrest/pgbackrest.conf # Generated by multipooler during init
/data/backups/              # Shared backup directory
/data/certs/                # TLS certificates
```

### pg_hba.conf for Testing

The demo uses **trust authentication for everything** (no passwords for internal connections):

```
local   all             all             trust
host    all             all             0.0.0.0/0    trust
host    replication     all             0.0.0.0/0    trust
```

---

## 8. Recommendations for Our E2E Tests

### What to Test

Based on upstream patterns, our operator e2e tests should verify:

1. **Cluster provisioning**: All components come up in the right order
2. **Metadata initialization**: createclustermetadata (or multiadmin) populates etcd correctly
3. **Bootstrap**: multiorch bootstraps the shard (elects primary, configures replication)
4. **Query serving**: Can connect to multigateway and run `SELECT 1`
5. **Pod readiness**: All pods pass their health checks

### Recommended Timeouts

Based on upstream test values:
- etcd ready: 10s
- multipooler startup (pgctld + multipooler processes): 20s + 15s
- multiorch startup: 15s
- Shard bootstrap (multiorch elects primary + configures replication): **60s**
- Multigateway startup: 3s
- Multigateway query serving (after bootstrap): 5s
- Overall cluster ready: **180s** (K8s demo timeout for multipooler pods)

### Connection String for Verification

```
host=<multigateway-service> port=5432 user=postgres password=<configured-password> dbname=postgres sslmode=disable
```

### Key Polling Pattern

```go
// Poll with short interval, generous timeout
require.Eventually(t, func() bool {
    // try operation
    return success
}, timeout, pollInterval, "description of what we're waiting for")
```

### What NOT to Test at E2E Level

The upstream keeps destructive tests (kill primary, failover) in isolated setups.
For the operator, basic provisioning e2e tests should focus on the happy path.
Failover testing can be a separate, more targeted test suite.

---

## 9. Key Constants and Defaults

| Constant | Value | Source |
|----------|-------|--------|
| Default table group | `"default"` | `constants.DefaultTableGroup` |
| Default shard | `"0-inf"` | `constants.DefaultShard` |
| Default durability policy | `"AT_LEAST_2"` | shardsetup defaults |
| Default cell name (tests) | `"test-cell"` | shardsetup defaults |
| Default database | `"postgres"` | shardsetup defaults |
| Topo implementation | `"etcd"` | topoclient default |
| Global topo root (demo) | `/multigres/test/global` | demo manifests |
| Test postgres password | `"test_password_123"` | `TestPostgresPassword` |
| Multigateway PG port | 5432 | operator defaults |
| Multipooler HTTP port | 16000 | demo manifests |
| Multipooler gRPC port | 16100 | demo manifests |
| pgctld gRPC port | 16200 | demo manifests |
| Multiorch HTTP port | 17000 | demo manifests |
| Multiorch gRPC port | 17100 | demo manifests |
| Multigateway HTTP port | 15000 | demo manifests |
| Multigateway gRPC port | 15100 | demo manifests |
| pgBackRest port | 8432 | demo manifests |

---

## 10. Source File Reference

All paths relative to `$GOMODCACHE/github.com/multigres/multigres@v0.0.0-20260206234310-62e1d947565c/`:

| Purpose | Path |
|---------|------|
| E2E test infrastructure | `go/test/endtoend/shardsetup/` |
| E2E test entry point | `go/test/endtoend/main_test.go` |
| Query serving tests | `go/test/endtoend/queryserving/multigateway_pg_test.go` |
| Multiorch bootstrap tests | `go/test/endtoend/multiorch/bootstrap_test.go` |
| Multigateway health endpoints | `go/services/multigateway/status.go` |
| Multigateway init (endpoint registration) | `go/services/multigateway/init.go` |
| Multipooler init | `go/multipooler/init.go` |
| Multiorch init | `go/services/multiorch/init.go` |
| Topology client | `go/common/topoclient/store.go` |
| etcd topology impl | `go/common/topoclient/etcdtopo/` |
| K8s demo manifests | `demo/k8s/k8s-*.yaml` |
| K8s launch scripts | `demo/k8s/launch-*.sh` |
| Config files | `config/*.yaml` |
| PG config template | `config/postgres/template.cnf` |
| pg_hba template | `config/postgres/pg_hba_template.conf` |
| CI workflows | `.github/workflows/test-integration.yml` |
