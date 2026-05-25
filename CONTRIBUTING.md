# Contributing to Multigres Operator

## Development Environment

### Prerequisites

- Go 1.25+
- Docker (for Kind clusters)
- `kubectl`
- `make`

### Setup

```bash
# Clone the repository
git clone https://github.com/multigres/multigres-operator.git
cd multigres-operator

# Run unit tests
make test

# Run integration tests (requires envtest binaries, installed automatically)
make test-integration
```

### Local Development with Kind

```bash
# Create a Kind cluster and deploy the operator
make kind-deploy

# Apply a sample cluster
kubectl apply -f config/samples/minimal.yaml

# Watch resources
kubectl get multigrescluster,cell,shard,tablegroup,toposerver -A -w

# Clean up
make kind-down
```

**All Kind deployment targets:**

| Command                          | Description                                                                                            |
| :------------------------------- | :----------------------------------------------------------------------------------------------------- |
| `make kind-deploy`               | Deploy operator to local Kind cluster using self-signed certs (Default).                               |
| `make kind-deploy-certmanager`   | Deploy operator to Kind, installing `cert-manager` for certificate handling.                           |
| `make kind-deploy-no-webhook`    | Deploy operator to Kind with the webhook fully disabled.                                               |
| `make kind-deploy-observability` | Deploy operator with full observability stack (Prometheus Operator, OTel Collector, Tempo, Grafana).   |
| `make kind-load-observer`        | Build and load observer image into Kind.                                                               |
| `make kind-deploy-observer`      | Deploy observer alongside the operator (builds and loads automatically).                               |
| `make kind-undeploy-observer`    | Remove observer from Kind.                                                                             |
| `make kind-portforward`          | Port-forward Grafana (3000), Prometheus (9090), Tempo (3200) to localhost. Re-run if connection drops. |

See the [demo/](demo/) folder for guided walkthroughs of cert-manager and observability deployments.

### Development with DevSpace

[DevSpace](https://devspace.sh/) is configured for quick iteration against an already-deployed operator in a live Kubernetes cluster. DevSpace replaces the operator pod with a dev container, then file-syncs your locally-built operator binary into it and restarts the process — typically a few seconds per cycle.

**Prerequisites:**

- The [DevSpace CLI](https://www.devspace.sh/docs/getting-started/installation)
- `KUBECONFIG` pointing to your live dev cluster

**Usage:**

```bash
devspace use namespace multigres-operator

# Builds the operator, swaps the operator pod for a dev container,
# syncs the binary, and streams logs
devspace dev

# Delete deployed DevSpace resources and restore the original operator pod
devspace purge
```

## Code Style

This project follows the [Google Go Style Guide](https://google.github.io/styleguide/go/best-practices).

Key conventions:

- **No transient comments.** Comments should be helpful permanently, not just during development.
- **Error handling.** Always wrap errors with context using `fmt.Errorf("operation: %w", err)`.
- **Naming.** Use idiomatic Go names. Controllers live in `pkg/{handler}/controller/{resource}/`.
- **Testing.** Write table-driven tests. Use `testify/assert` and `testify/require` for assertions.

## Project Structure

```
api/v1alpha1/       # CRD type definitions (kubebuilder markers)
cmd/                # Operator entrypoint (main.go)
config/             # Kustomize manifests, CRDs, RBAC, samples, monitoring
docs/               # User-facing docs, runbooks, developer docs
  development/      # Internal architecture and implementation references
  monitoring/       # Alert runbooks
pkg/
  cluster-handler/  # MultigresCluster controller (top-level reconciler)
  resource-handler/ # Cell, Shard, TableGroup, TopoServer controllers
  data-handler/     # Shared libraries: drain state machine, topo operations, backup health
  resolver/         # Configuration resolution (4-level override chain)
  webhook/          # Admission webhook (mutating + validating)
  cert/             # Generic TLS certificate lifecycle manager
  monitoring/       # Metrics, tracing, log-trace correlation
  util/             # Shared utilities (naming, status, PVC helpers)
  testutil/         # Shared test infrastructure (envtest, Kind helpers)
tools/
  observer/         # Standalone cluster health monitoring tool
  skills/           # AI agent skills for development and release workflows
```

## Making Changes

### Modifying CRDs

1. Edit the type definitions in `api/v1alpha1/`
2. Run `make generate manifests` to regenerate CRDs and DeepCopy methods
3. Update the resolver if the new field needs defaulting or validation
4. Update the webhook handlers if admission logic changes
5. Run `make test test-integration` to verify

### Adding a New Controller

1. Create the controller package under `pkg/resource-handler/controller/{resource}/`
2. Implement the `Reconcile` method following existing patterns (see `cell_controller.go` as an example)
3. Register the controller in `cmd/main.go`
4. Add RBAC markers for any new resource types
5. Write unit and integration tests

### Running Tests

| Command                 | Scope                                |
| :---------------------- | :----------------------------------- |
| `make test`             | Unit tests                           |
| `make test-integration` | Integration tests using envtest      |
| `make test-e2e`         | End-to-end tests using Kind clusters |
| `make lint`             | Linting via golangci-lint            |

## Documentation

- **User-facing docs** go in the [README](README.md) or `docs/` (backup-restore, observability, runbooks)
- **Developer docs** go in `docs/development/`
- **Design proposals** go in `plans/` (mark completed proposals with `## Status: Completed`)
- **Operator capability assessment** is tracked in [docs/operator-capability-levels.md](docs/operator-capability-levels.md) — update it when adding features that advance a capability level

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

```
feat(shard): add PVC volume expansion support
fix(drain): prevent fallthrough on topo unavailability
docs(readme): update backup architecture section
chore(deps): update internal Go module dependencies
```

## AI Agent Skills

This project includes structured AI agent skills under `tools/skills/` and `tools/observer/skills/`. These are prompt-based workflows that teach AI coding agents (e.g., Claude Code) how to perform specific tasks consistently.

### Available Skills

**Operator Development** ([tools/skills/](tools/skills/)):

| Skill                     | Trigger                    | Description                                                              |
| :------------------------ | :------------------------- | :----------------------------------------------------------------------- |
| `generate_commit_message` | `/generate_commit_message` | Generate semantic Conventional Commits messages from staged changes.     |
| `pin_upstream_images`     | `/pin_upstream_images`     | Pin multigres container image SHA tags and review upstream code changes. |
| `prepare_release`         | `/prepare_release`         | Full release preparation: changelog, version inference, and doc audit.   |

**Observer** ([tools/observer/skills/](tools/observer/skills/)):

| Skill                    | Trigger                   | Description                                               |
| :----------------------- | :------------------------ | :-------------------------------------------------------- |
| `exercise_cluster`       | `/exercise_cluster`       | Deploy fixtures, run mutation scenarios, validate health. |
| `diagnose_with_observer` | `/diagnose_with_observer` | Triage observer findings and produce bug reports.         |

### Adding a New Skill

1. Create a directory under `tools/skills/<skill_name>/` with a `SKILL.md` file
2. Use the frontmatter format (`name`, `description`) — see existing skills for examples
3. Add reference files under `references/` or scripts under `scripts/` as needed
4. Create a stub in `.agents/skills/<skill_name>/SKILL.md` pointing to the repo location
5. Update the [tools/skills/README.md](tools/skills/README.md) table
