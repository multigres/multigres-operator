# Operator Configuration

## Required Environment Variables

The operator requires two environment variables to be set on the Deployment (these are pre-configured in the install manifests):

| Variable | Description |
| :--- | :--- |
| `POD_NAMESPACE` | Namespace where the operator is deployed. Used for leader election, cert secrets, and cache filtering. |
| `POD_SERVICE_ACCOUNT` | Service account name of the operator pod. Used for webhook configuration. |

## Flags

You can customize the operator's behavior by passing flags to the binary (or editing the Deployment args).

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--webhook-enable` | `true` | Enable the admission webhook server. |
| `--webhook-port` | `9443` | The port that the webhook server serves at. |
| `--webhook-cert-dir` | `/var/run/secrets/webhook` | Directory to read/write webhook certificates. |
| `--webhook-service-name` | `multigres-operator-webhook-service` | Name of the Service pointing to the webhook. |
| `--webhook-service-namespace`| `$POD_NAMESPACE` | Namespace of the webhook service. |
| `--webhook-service-account` | `$POD_SERVICE_ACCOUNT` | Service Account name of the operator. |
| `--metrics-bind-address` | `:8443` | Address for the metrics endpoint. Set to `0` to disable. |
| `--metrics-secure` | `true` | Serve metrics over HTTPS with authentication and authorization. |
| `--enable-http2` | `false` | Enable HTTP/2 for metrics and webhook servers. |
| `--leader-elect` | `false` | Enable leader election (recommended for HA deployments). |
| `--image-update-strategy` | `immediate` | Default for when clusters adopt a changed default image set: `immediate` rolls a cluster as soon as operator configuration changes; `lazy` holds it on its current set until `spec.imageUpdatePolicy.acknowledgedRevision` names the new set's revision. Individual clusters can override this via `spec.imageUpdatePolicy.strategy`. |
| `--pgbackrest-sandbox` | `$MULTIGRES_PGBACKREST_SANDBOX`, else `required` | Whether pgBackRest child processes are confined with Landlock, passed as `--sandbox=<mode>` to every pgbackrestd sidecar. See [pgBackRest sandbox mode](#pgbackrest-sandbox-mode). |

## pgBackRest sandbox mode

Every pool pod and Lite pod runs a pgbackrestd sidecar that confines the pgBackRest processes it starts with Landlock. Whether that is possible is a property of the nodes, so it is chosen per operator deployment with `--pgbackrest-sandbox` (default from the `MULTIGRES_PGBACKREST_SANDBOX` environment variable; the flag wins over the environment; empty means `required`). There is no per-cluster field and nothing in the CRDs. An unrecognised value stops the operator at startup.

| Mode | Behaviour |
| :--- | :--- |
| `required` (default) | The sidecar refuses to start unless Landlock works. Fails closed. |
| `auto` | For fleets with mixed kernels: confine when the node supports Landlock, otherwise run unconfined. The sidecar logs what it found at start; the operator exposes no status, condition or metric for it. |
| `off` | Never use Landlock. For environments without it. |

Changing the mode changes the sidecar's arguments, and therefore the pod spec hash, so **flipping the mode rolls every pool pod and Lite pod** under the usual rollout rules.

Requirements for `required` (and for `auto` to confine): a Linux kernel with the Landlock LSM enabled at ABI 3 or later (Linux 6.2+), and a seccomp profile that allows `landlock_create_ruleset`, `landlock_add_rule` and `landlock_restrict_self`. Docker's default profile allows them on Docker 29.8.2. The `RuntimeDefault` profile of containerd on real nodes has NOT been verified; if it blocks the syscalls, the probe fails with EPERM and the sidecar exits in every mode but `off` (a blocked syscall is never treated as "unsupported", so `auto` does not silently degrade on it).

## Component Images

Component images are operator configuration, resolved at reconcile time: the
operator uses its compiled-in defaults for any component not explicitly set in
`spec.images`, and each default can be overridden on the operator Deployment.
Explicit `spec.images` values always win. The resolved default set and its
revision are logged at startup. Per-cluster `status.images` reports the
complete effective set and whether it comes from explicit pins, defaults, or
a mixture; default revisions are reported only when defaults participate.

| Variable | Overrides the default image for |
| :--- | :--- |
| `MULTIGRES_IMAGE_POSTGRES` | Postgres (pgctld) |
| `MULTIGRES_IMAGE_MULTIADMIN` | Multiadmin |
| `MULTIGRES_IMAGE_MULTIADMIN_WEB` | MultiadminWeb |
| `MULTIGRES_IMAGE_MULTIORCH` | Multiorch |
| `MULTIGRES_IMAGE_MULTIPOOLER` | Multipooler |
| `MULTIGRES_IMAGE_MULTIGATEWAY` | Multigateway |

See [gitops-and-webhook-defaults.md](gitops-and-webhook-defaults.md) for how
image resolution and the lazy update strategy behave per cluster.

## Logging

The operator uses structured JSON logging (`zap` via controller-runtime). When tracing is enabled, every log line within a traced operation automatically includes `trace_id` and `span_id` fields.

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--zap-devel` | `true` | Development mode preset (console encoder, debug level, warn stacktraces). |
| `--zap-log-level` | depends on mode | Log verbosity: `debug`, `info`, `error`, or an integer. |
| `--zap-encoder` | depends on mode | Log format: `console` or `json`. |
| `--zap-stacktrace-level` | depends on mode | Minimum level that triggers stacktraces. |

| Setting | `--zap-devel=true` (default) | `--zap-devel=false` (production) |
| :--- | :--- | :--- |
| Default log level | `debug` | `info` |
| Encoder | `console` (human-readable) | `json` |
| Stacktraces from | `warn` | `error` |

> [!NOTE]
> For production deployments, set `--zap-devel=false` to switch to JSON encoding and `info`-level logging.

## Component Log Levels

The operator exposes per-component log level configuration for multigres data-plane components via the `LogLevels` field on `MultigresClusterSpec`. These control the `--log-level` flag passed to each component's container.

```yaml
spec:
  logLevels:
    pgctld: "info"
    multipooler: "info"
    multiorch: "debug"      # Increase verbosity for multiorch
    multiadmin: "info"
    multigateway: "info"
```

| Component | Field | Default |
| :--- | :--- | :--- |
| pgctld | `spec.logLevels.pgctld` | `info` |
| multipooler | `spec.logLevels.multipooler` | `info` |
| multiorch | `spec.logLevels.multiorch` | `info` |
| multiadmin | `spec.logLevels.multiadmin` | `info` |
| multigateway | `spec.logLevels.multigateway` | `info` |

Log levels are inherited through the resource hierarchy (`MultigresCluster` → `Cell` → `Shard` → `TableGroup`). The webhook materializes all fields to `info` on `CREATE` if not set.
