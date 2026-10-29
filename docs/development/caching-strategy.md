# Controller Caching Strategy

## The Problem: The Informer Memory Bomb

By default, the `controller-runtime` cache (Informer) watches *every* instance of a resource kind across *every* namespace.
In large multi-tenant clusters (e.g. Supabase production), a single cluster may contain:
- 5,000+ Secrets (Helm releases, other operators, user apps)
- 10,000+ Services
- 2,000+ StatefulSets

If the Multigres Operator watches `Secrets` globally without a filter, it will attempt to:
1.  List all 5,000 secrets on startup.
2.  Maintain a local copy of all 5,000 secrets in memory.
3.  Receive watch events for every change to any secret in the cluster.

This causes:
- **OOM (Out of Memory):** The operator pod crashes as it tries to cache GBs of unrelated data.
- **Slow Startup:** The initial List call times out or takes forever.
- **Network Load:** Massive unnecessary traffic on the API Server.

## The Solution: Hybrid Global/Local Caching

We implement a **Split-Brain Caching Strategy** using `cache.ByObject` options. This allows us to apply different rules based on the namespace.

### Global Rule (The "Noise Cancelling")
For high-volume resources in regular user namespaces, we strictly filter the cache to ONLY store objects managed by this operator.

**Filtered Resources:**
- `Secret`
- `Service`
- `StatefulSet` (still used for TopoServer)
- `Pod`

**Mechanism:**
We use `cache.AllNamespaces` with a `LabelSelector`:
```go
app.kubernetes.io/managed-by = multigres-operator
```
This effectively ignores any Secret/Service/StatefulSet/Pod that does not belong to us.

### Local Exception (The "Safe Zone")
Some critical resources *must* be seen even if they don't have our label.
- **Cert-Manager Secrets:** Created by `cert-manager` for webhook TLS.
- **Leader Election Leases:** Created by `client-go`.
- **Own Deployment:** For `ownerRef` resolution.

**Mechanism:**
We configure a specific override for the operator's own namespace (e.g. `multigres-system`) with an **empty selector** (cache everything).
```go
defaultNS: {} // Unfiltered in our own namespace
```

### ConfigMap Policy (The "Flexibility")
**Decision:** ConfigMaps are **NOT** filtered globally.

**Reasoning:**
Users frequently provide their own "unmanaged" ConfigMaps for Postgres configuration (e.g. `postgresql.conf` overrides).
If we filtered ConfigMaps, the operator would be unable to:
1.  Read the user's `my-postgres-config`.
2.  Calculate its hash.
3.  Trigger a rolling update when the user changes it.

Since ConfigMaps are generally lower volume and lower security risk than Secrets, we trade scalability for usability here.

### Summary Table

| Resource | Scope | Filter | Why? |
| :--- | :--- | :--- | :--- |
| **Secret** | All Namespaces | `managed-by=multigres` | **OOM Prevention.** |
| **Secret** | Operator NS | **NONE (All)** | Cert-Manager compatibility. |
| **Service** | All Namespaces | `managed-by=multigres` | Noise reduction. |
| **Service** | Operator NS | **NONE (All)** | Self-discovery. |
| **StatefulSet** | All Namespaces | `managed-by=multigres` | Noise reduction (still used for TopoServer). |
| **StatefulSet** | Operator NS | **NONE (All)** | Self-discovery. |
| **Pod** | All Namespaces | `managed-by=multigres` | OOM prevention; operator only manages its own pool pods. |
| **Pod** | Operator NS | **NONE (All)** | Self-discovery. |
| **ConfigMap** | All Namespaces | **NONE (All)** | User Configs (postgresql.conf). |

### Laptop-Mode Cache Scoping

The cache options builder is `cacheopts.New(operatorNamespace, laptopNamespace string, watchNamespaces ...string)`.
With an empty `laptopNamespace` and no watch allowlist, the configuration above is
returned unchanged. `--watch-namespace` accepts a comma-separated allowlist and
scopes both `DefaultNamespaces` and every `ByObject` override to those namespaces
plus the operator namespace. These explicit namespaces are unfiltered.
Laptop mode takes precedence over that allowlist and never expands its watch scope.
When `laptopNamespace` is set (laptop mode, `--namespace`), the cache holds that one namespace
and nothing else. Laptop mode is only permitted against a namespace already
labelled `multigres.com/reconcile=disabled`, so this matches what the process is
allowed to reconcile:

- The per-type `ByObject` map becomes a single entry for the claimed namespace,
  with the `managed-by` filter. The operator's own namespace is left out: it
  holds the leader-election lease and webhook cert material, and laptop mode
  refuses both leader election and webhooks, so nothing reads it.
- `DefaultNamespaces` is set to the same single namespace. This matters because
  ConfigMaps are deliberately absent from `ByObject`: they fall back to the
  default, so **setting only the per-type map would leave ConfigMaps cached
  cluster-wide**. The two settings have to move together.

See [Reconcile Gating and Laptop Mode](reconcile-gating.md) for the full laptop
mode behaviour.

## Developer Guide: Reading Secrets

Because Secrets are filtered globally, you cannot simply `r.Get()` an arbitrary user secret (e.g. `spec.passwordSecretRef`) unless it is labeled. There are three patterns for accessing Secrets, each suited to a different scenario.

### Decision Table

| Scenario | Pattern | Cache Hit? | API Server Hit? | Example |
| :--- | :--- | :---: | :---: | :--- |
| Operator-created Secret | **Option A** (Label it) | ✅ | No | `postgres-password` Secret |
| External Secret the operator must validate | **Option B** (APIReader) | ❌ | Yes (per reconcile) | pgBackRest TLS from cert-manager |
| External Secret only pods need at runtime | **Option C** (Kubelet delegation) | N/A | No | S3 `credentialsSecret` |

### Option A (Preferred): Require the Managed-By Label

Secrets created by the operator (via SSA `r.Patch()`) automatically receive the `app.kubernetes.io/managed-by: multigres-operator` label through our standard metadata helpers. These Secrets are visible in the informer cache and accessible via the normal `r.Get()`.

If a user-provided Secret must be visible to the operator's cached client, tell the user: *"Please add `app.kubernetes.io/managed-by: multigres-operator` to your secret."*

### Option B (Bypass Cache): Direct API Reader

When the operator must **read and validate** a Secret that it does not own (e.g., a cert-manager-issued TLS Secret), the informer cache will return `NotFound` because the Secret lacks our label. Use `mgr.GetAPIReader()` to make a live, uncached call to the Kubernetes API Server.

**Setup** (in `main.go`):
```go
&shardcontroller.ShardReconciler{
    Client:    mgr.GetClient(),
    APIReader: mgr.GetAPIReader(), // Uncached reader injected here
}
```

**Usage** (in `reconcile_shared_infra.go:reconcilePgBackRestCerts`):
```go
// User-provided Secret: validate via uncached API reader.
// External Secrets (e.g., cert-manager) lack the managed-by label
// and are invisible to the cached r.Get().
if err := r.APIReader.Get(ctx, types.NamespacedName{
    Name:      secretName,
    Namespace: shard.Namespace,
}, secret); err != nil {
    return fmt.Errorf("pgbackrest TLS secret %q not found: %w", secretName, err)
}
// Validate required keys exist
for _, key := range []string{"ca.crt", "tls.crt", "tls.key"} {
    if _, ok := secret.Data[key]; !ok {
        return fmt.Errorf("secret %q missing required key %q", secretName, key)
    }
}
```

**Trade-offs:**
- **Pro:** The operator can validate the Secret exists and has the correct shape before deploying pods.
- **Con:** One live API call per reconcile per shard. With 20 max concurrent workers, the realistic upper bound is 20 simultaneous calls — trivial for the API server.

### Option C (Kubelet Delegation): Don't Read It At All

When the operator does not need to inspect a Secret's contents — it only needs to pass the Secret to a pod — use `secretKeyRef` or `secretRef` in the container's `env` spec. The **kubelet** resolves the Secret at pod creation time, completely bypassing the operator's cache.

**Usage** (in `containers.go:s3EnvVars`):
```go
// The operator never reads this Secret. It just passes the name
// to the pod spec, and the kubelet resolves it at runtime.
if backup.S3.CredentialsSecret != "" {
    envs = append(envs, corev1.EnvVar{
        Name: "AWS_ACCESS_KEY_ID",
        ValueFrom: &corev1.EnvVarSource{
            SecretKeyRef: &corev1.SecretKeySelector{
                LocalObjectReference: corev1.LocalObjectReference{
                    Name: backup.S3.CredentialsSecret,
                },
                Key: "AWS_ACCESS_KEY_ID",
            },
        },
    })
}
```

**Trade-offs:**
- **Pro:** Zero cache concern, zero API server load from the operator, works with any Secret regardless of labels.
- **Con:** The operator cannot validate the Secret exists before pod creation. If the Secret is missing or malformed, the pod will fail to start with a `CreateContainerConfigError`, which is only visible in pod events, not in the operator's reconcile loop.

**When to use Option C:** When the Secret is only consumed by pods at runtime (credentials, API keys, tokens) and the operator does not need to make decisions based on its contents.

### IRSA (IAM Roles for Service Accounts): No Secret At All

For EKS environments, the recommended S3 credential mechanism is **IRSA**. The user creates a ServiceAccount annotated with `eks.amazonaws.com/role-arn` and sets `s3.serviceAccountName` in the backup config. The operator sets `pod.spec.serviceAccountName` on pool pods — the EKS pod identity webhook then injects `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, and a projected OIDC token volume automatically. The upstream multipooler detects these env vars and configures pgBackRest with `repo1-s3-key-type=web-id`. The operator does not create the ServiceAccount, manage IAM roles, or inject any env vars for IRSA.

This is the lightest-touch credential option: zero Secrets, zero operator cache interaction, zero env var injection. The `serviceAccountName` field is mutually exclusive with both `credentialsSecret` and `useEnvCredentials` (enforced via CEL validation).
