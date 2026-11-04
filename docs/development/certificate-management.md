# Self-Signed Certificate Management (`pkg/cert`)

## Overview

The operator includes a generic, consumer-agnostic TLS certificate lifecycle manager in `pkg/cert`. This module handles CA and server certificate generation, rotation, and Kubernetes Secret management without any knowledge of webhooks or other specific consumers.

## Architecture: Split-Secret PKI

```
┌─────────────────────────────────────────────────────┐
│                     pkg/cert                        │
│                                                     │
│    CertRotator (Bootstrap + Background Loop)        │
│    ┌─────────────┐      ┌──────────────────┐        │
│    │ CA Secret   │──signs──▸ Server Secret │        │
│    │ (ca.crt/key)│      │ (tls.crt/key)    │        │
│    └─────────────┘      └──────────────────┘        │
│          │                       │                  │
│          │       PostReconcileHook(caBundle)        │
│          │              │                           │
└──────────┼──────────────┼───────────────────────────┘
           │              │
           ▼              ▼
   Owner Reference    Consumer-specific logic
   (GC on uninstall)  (e.g. patch webhook configs)
```

**Key design decisions:**

- **Two secrets**: The CA private key is stored in a separate secret that is never mounted to any pod, reducing the blast radius if the server cert secret is compromised.
- **ECDSA P-256**: Modern, fast, and produces small certificates. Avoids RSA key generation overhead.
- **Hook-based extensibility**: The `PostReconcileHook` function receives the CA bundle after every reconcile. For webhooks, this patches the `MutatingWebhookConfiguration` and `ValidatingWebhookConfiguration`. Other consumers can implement their own hooks.
- **Owner references**: The `Owner` field (typically the operator's Deployment) ensures secrets are garbage-collected when the operator is uninstalled.
- **Projection wait**: When `WaitForProjection: true`, the rotator blocks until the Kubelet projects updated secrets to disk, preventing the webhook server from serving stale certificates.

## Configuration via `cert.Options`

| Field | Purpose | Default |
|:---|:---|:---|
| `CASecretName` | Name of the CA Secret | *(required)* |
| `ServerSecretName` | Name of the server cert Secret | *(required)* |
| `ServiceName` | Kubernetes Service name (used in SAN DNS names) | *(required)* |
| `Namespace` | Namespace for all secrets | *(required)* |
| `CertDir` | On-disk path for projected cert files | *(required if WaitForProjection)* |
| `Organization` | X.509 Organization field | `"Multigres Operator"` |
| `AdditionalDNSNames` | Extra SAN DNS names appended to auto-generated ones | `nil` |
| `ExtKeyUsages` | X.509 Extended Key Usages | `[ServerAuth]` |
| `ComponentName` | Label for Kubernetes Events (e.g. `"webhook"`) | `"cert"` |
| `RotationInterval` | How often the background loop checks | `1h` |
| `PostReconcileHook` | Callback after each successful reconcile | `nil` |
| `Labels` | Labels applied to generated Secret objects (required outside operator NS) | `nil` |
| `Owner` | `metav1.Object` for owner references | `nil` |
| `WaitForProjection` | Block until cert file matches Secret on disk | `false` |

## How the Webhook Uses It

In `main.go`, the webhook wires into `pkg/cert` via two helpers from `pkg/webhook/pki.go`:

```go
owner, _ := multigreswebhook.FindOperatorDeployment(ctx, cl, ns, labels, name)

mgr := gencert.NewManager(cl, recorder, gencert.Options{
    Owner:             owner,
    PostReconcileHook: multigreswebhook.PatchWebhookCABundle,
    // ... other options
})
```

- `FindOperatorDeployment`: Locates the operator's own Deployment for setting owner references on cert secrets.
- `PatchWebhookCABundle`: Patches both `MutatingWebhookConfiguration` and `ValidatingWebhookConfiguration` with the CA bundle. Tolerates `NotFound` errors gracefully.

## Reusing for Other TLS Use Cases

The `pkg/cert` module is intentionally generic and can be reused for any component that needs self-signed TLS within the operator. Current and potential use cases:

1. **pgBackRest TLS** *(implemented)*: Generates client/server certs for pgBackRest inter-node backup communication. See below.
2. **Etcd mTLS**: Generate client certificates for etcd authentication.
3. **Inter-component mTLS**: Secure traffic between operator-managed pods.

To add a new consumer:

1. Create a new `Options` with a unique `CASecretName`/`ServerSecretName` pair and appropriate `ExtKeyUsages` (e.g. `ClientAuth` for mTLS).
2. Implement a `PostReconcileHook` for any consumer-specific logic (e.g. restarting pods when certs rotate).
3. Call `NewManager()` and add it as a `Runnable` to the controller-runtime manager.

## pgBackRest TLS Implementation

pgBackRest requires TLS for its inter-node communication protocol. The shard controller (`reconcilePgBackRestCerts`) supports two modes:

**Auto-generated (default):** Uses `pkg/cert` to create a CA Secret (`{shard}-pgbackrest-ca`) and a server cert Secret (`{shard}-pgbackrest-tls`), both owned by the Shard resource.

**User-provided:** Accepts a user-specified Secret (e.g., from cert-manager) via `spec.backup.pgbackrestTLS.secretName`. The operator validates the Secret exists and contains the required keys (`ca.crt`, `tls.crt`, `tls.key`).

**Projected volume with key renaming:** Both modes mount certificates via a projected volume that renames `tls.crt` → `pgbackrest.crt` and `tls.key` → `pgbackrest.key` to the file names `pgbackrestd` reads from `--tls-cert-dir`. Only the `pgbackrest` sidecar mounts the volume. This makes user-provided mode directly compatible with cert-manager's standard Secret output without any manual key renaming.

**APIReader for external Secret validation:** User-provided Secrets (e.g., from cert-manager) lack the `app.kubernetes.io/managed-by: multigres-operator` label and are therefore invisible to the informer cache (see [caching-strategy.md](caching-strategy.md)). The shard controller uses `r.APIReader.Get()` (uncached, direct API server call) to validate these Secrets. This is the `Option B` approach documented in the caching strategy document.
