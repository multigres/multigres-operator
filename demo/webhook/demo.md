# Multigres Operator: Hybrid Admission Control Demo

This document demonstrates the **Hybrid Architecture** of the Multigres Operator. It highlights how the operator balances user experience, safety, and operational resiliency through a design that utilizes both a Kubernetes Webhook and an internal logic resolver.

## Overview: Two Modes of Operation

The Multigres Operator implements a **Shared Resolver** pattern. This logic is shared between the API Server (via Webhook) and the Controller (via Reconciliation Loop), providing two distinct modes:

1.  **Robust Mode (Normal Operation)**: A Mutating Webhook intercepts requests to "hydrate" configurations before they are stored in etcd. This ensures users see the full, realized state of their cluster (**"Visible Defaults"**) and protects critical resources via Validating Webhooks. Certificates are auto-generated ("Self-Bootstrap") unless a user provides them.
2.  **Resilient Mode (No Webhook)**: If the webhook service is unavailable or disabled, the Controller falls back to applying defaults in-memory. This ensures the operator remains functional (**"Invisible Defaults"**) even if admission control is bypassed, though "Fast Fail" validations are lost.

## 🛠️ Prerequisites

The Operator's `Makefile` handles most project-specific dependencies (Kustomize, Controller-Gen), but you must have the base runtime installed on your machine.

**System Requirements:**

1.  **Go (1.23+)**: Required to compile the operator.
2.  **Docker (or Podman)**: Required to build container images.
3.  **Kind**: Required to create the local Kubernetes cluster.
4.  **Kubectl**: Required to interact with the cluster.
5.  **Make**: Required to run the build scripts.

*Note: The demo commands below will fail immediately if these tools are not found in your `$PATH`.*

** Setup (Go Workspace)**:
If you have issues building you may need to setup your go workspace:

```bash
go work init
go work use -r .
```

You may also need to need to run this at the root of the project for the `kubectl` commands to work:

```bash
export KUBECONFIG=$KUBECONFIG:$(pwd)/kubeconfig.yaml
```
---

## 🏗️ Scenario 1: Webhook Enabled (Internal Mode)

**Objective:** Demonstrate the "Zero Config" experience where the operator sets up its own secure webhook infrastructure and hydrates configurations.

### 1. Deploy the Operator

We deploy the operator using the standard target. This builds the image, loads it into Kind, and applies the manifests.

```bash
make kind-deploy
kubectl wait --for=condition=Available deployment/multigres-operator-controller-manager -n multigres-operator --timeout=120s
```

### 2. Verify Internal Cert Mode

The operator automatically detects that no certificates were provided and enters **"Self-Bootstrap"** mode.

```bash
kubectl logs -n multigres-operator deployment/multigres-operator-controller-manager | grep "webhook certificates"
```

**Output:**
`webhook certificates not found on disk; enabling internal certificate rotation`

### 3. Inspect the Self-Signed Certificate

Since we are in Internal Mode, the operator generated its own CA and server certificate.

```bash
kubectl get secret multigres-webhook-certs -n multigres-operator -o yaml
```

### 4. Apply a Minimal Configuration

We apply a cluster definition consisting of only a single Cell.

```bash
kubectl apply -f config/samples/minimal.yaml
```

### 4. Inspect the Result (Visible Defaults)

The Mutating Webhook intercepts the request and uses the Shared Resolver to populate the spec *before* it is persisted.

```bash
kubectl get multigrescluster minimal -o yaml
```

**Output:**

```yaml
spec:
  cells:
  - name: zone-a
    spec:
      multigateway:
        replicas: 1
        resources:         # <--- Fully Materialized Resources
          limits:
            memory: 256Mi
          requests:
            cpu: 100m
            memory: 128Mi
    zoneId: use1-az1
  databases:
  - default: true          # <--- System Catalog Created
    name: postgres
    tablegroups:
    - default: true
      name: default
      shards:
      - name: "0"
        spec:
          pools:
            default:
              postgres:
                resources: # <--- Deep Defaults Applied
                  limits:
                    memory: 512Mi
                  requests:
                    cpu: 100m
                    memory: 256Mi
              type: readWrite
  images:                  # <--- Pinned Versions Injected
    postgres: postgres:15-alpine
```

---

## 🚀 Scenario 2: Explicit Templates & Safety Gates

**Objective:** Demonstrate Template Resolution and the **"Fast Fail"** protection provided by the Validating Webhook.

### 1. Install Standard Templates

We install reusable Templates defining a high-availability topology.

```bash
kubectl apply -f config/samples/templates/
```

### 2. Apply a Templated Cluster

We apply a cluster that explicitly references these templates.

```bash
kubectl apply -f config/samples/templated-cluster.yaml
```

### 3. Verify Resolution

Check that the cluster inherited values from the templates.

```bash
kubectl get multigrescluster standard-ha-cluster -o yaml
```

**Output:**

```yaml
spec:
  cells:
  - name: az1
    zoneId: use1-az1
  databases:
  - default: true
    name: postgres
    tablegroups:
    - default: true
      name: default
      shards:
      - name: "0"
        overrides:
          pools:
            main-app:        # <--- Inherited from 'standard-shard' Template
              cells:
              - az1
              - az2
              replicasPerCell: 2 # <--- Overrides default (1)
  templateDefaults:
    cellTemplate: standard-cell
    coreTemplate: standard-core
    shardTemplate: standard-shard
```

> Note: The template details are NOT resolved by the webhook, they are only used as a reference for the cluster's configuration. The reason for this is that the webhook is not aware of the templates and cannot resolve them. If it it would create issues too. For example, if a user were to add their own inline spec configuration to the template, it would be difficult to differentiate what's a template and what's a cluster spec.

### 4. Test: Safety Gate (Referential Integrity)

Now that the cluster exists and relies on the templates, we verify that the webhook prevents accidental deletion.

```bash
kubectl delete celltemplate standard-cell
```

**Output:**
The webhook blocks the deletion.

```text
Error from server (Forbidden): admission webhook "vcelltemplate.kb.io" denied the request:
cannot delete CellTemplate 'standard-cell' because it is in use by MultigresCluster 'standard-ha-cluster'
```

---

## 🪄 Scenario 3: Implicit Defaults

**Objective:** Demonstrate the system's "Magic" default resolution. If a user does NOT specify a template, but a template named `default` exists, the system automatically adopts it.

### 1. Install Default Templates

We create templates specifically named `default`.

```bash
kubectl apply -f config/samples/default-templates
```

### 2. Apply Minimal Cluster (Again)

We apply the `minimal.yaml` again (or a new one). It has **NO** template references.

```bash
kubectl apply -f config/samples/minimal.yaml
```

### 3. Inspect the Magic

```bash
kubectl get multigrescluster minimal -o yaml
```

**Output:**

```yaml
spec:
  templateDefaults:          # <--- Automatically Populated!
    cellTemplate: default
    coreTemplate: default
    shardTemplate: default
```
>NOTE: You will notice that the pools in the cluster did not change. That's because the webhook will not modify any spec that is already defined and populated in the multigrescluster spec. This is for safety and to prevent any unexpected changes to the cluster.

---

## 🔧 Scenario 4: Deep Overrides

**Objective:** Demonstrate how precise overrides can be applied on top of templates.

### 1. Apply Overrides Configuration

We apply a cluster that uses the `standard-ha` templates but overrides specific parameters (e.g., storage size, replicas) for a single shard or pool.

```bash
kubectl apply -f config/samples/overrides.yaml
```

### 2. Inspect the Result

```bash
kubectl get multigrescluster overrides-cluster -o yaml
```

**Output:**

```yaml
spec:
  cells:
  - name: zone-a
    overrides:
      multigateway:
        replicas: 3          # <--- Specific Override
    zoneId: use1-az1
  databases:
  - default: true
    name: postgres
    tablegroups:
    - default: true
      name: default
      shards:
      - name: "0"
        overrides:
          pools:
            analytics:       # <--- Deep Override (Specific Pool)
              replicasPerCell: 1
              storage:
                size: 50Gi   # <--- Override Storage
              type: readWrite
            main-app:
              storage:
                size: 200Gi  # <--- Override Storage
```

---


## 🏭 Scenario 5: Production Mode (Cert-Manager Integration)

**Objective:** Demonstrate how to use **Cert-Manager** for certificate management.

### 1. Architecture

To switch to External Mode using Cert-Manager, we utilize a Kustomize overlay (`config/deploy-certmanager`). The Operator detects the mounted Secret and automatically disables its internal CA.

### 2. Deploy with Cert-Manager

We use a convenience make target that installs Cert-Manager and deploys the operator with the overlay enabled.

```bash
make kind-down
make kind-deploy-certmanager
kubectl wait --for=condition=Available deployment/multigres-operator-controller-manager -n multigres-operator --timeout=180s
```

### 3. Verify External Mode

Check the logs to confirm the operator detected the certificates provided by Cert-Manager.

```bash
kubectl logs -n multigres-operator deployment/multigres-operator-controller-manager | grep "webhook certificates"
```

**Output:**
`webhook certificates found on disk; using external certificate management`

### 4. Inspect the Managed Certificate

Cert-Manager has issued a certificate and stored it in a Secret, which the operator is now using.

```bash
kubectl get secret webhook-server-cert -n multigres-operator -o yaml
```

---

## 🛡️ Scenario 6: Webhook Disabled (Resilient Fallback)

**Objective:** Demonstrate operational continuity when the Webhook is disabled. Unlike "Strict" operators that fail completely, Multigres falls back to "Invisible Defaults," but loses the "Fast Fail" protections.

> NOTE: running the operator without a webhook may be useful for testing but it is not recommended for production. In fact, given that the webhook has the ability to generate its own certificates seamlessly and we run local tests in kind, we may not even need this at all. The use of the resolver makes it possible to have a fallback to invisible defaults rather seamless.

### 1. Deploy without Webhook

We deploy the operator with the webhook explicitly disabled.

```bash
make kind-down
make kind-deploy-no-webhook
```

### 2. Apply the Minimal Cluster

We apply the exact same minimal YAML from Scenario 1.

```bash
kubectl apply -f config/samples/minimal.yaml
```

### 3. Inspect the Object (Invisible Defaults)

Because the webhook is bypassed, the object is stored "as-is" in `etcd`.

```bash
kubectl get multigrescluster minimal -o yaml
```

**Output:**

```yaml
spec:
  cells:
  - name: zone-a
    zoneId: use1-az1
  images: {}                 # <--- Empty! No Defaults Injected
  templateDefaults: {}
```

### 4. The Resiliency Payoff

Despite the sparse spec, the **Controller** successfully creates the child resources using in-memory defaults.

```bash
kubectl get cells,tablegroups -l multigres.com/cluster=minimal
```

**Output:**

```text
NAME                                        GATEWAY   READY
cell.multigres.com/minimal-zone-a   0

NAME                                                        SHARDS
tablegroup.multigres.com/minimal-postgres-default   0
```

### 5. Test: Missing Protection

To verify the lack of safety gates, we first set up a scenario where a template is explicitly in use.

```bash
kubectl apply -f config/samples/templates/
kubectl apply -f config/samples/templated-cluster.yaml
```

Now we attempt to delete the template that the cluster relies on.

```bash
kubectl delete celltemplate standard-cell
```

**Output:** The deletion **Succeeds**. Without the validating webhook, the safety gate is gone. This is the trade-off for running in Resilient Mode.

---

## 🧠 Internal Architecture

How does the operator achieve this hybrid behavior?

### 1. The Shared Resolver Pattern

Logic is centralized in `pkg/resolver`. Both the Webhook (for mutation) and the Controller (for reconciliation) call this same code.

```go
// pkg/resolver/resolver.go
type Resolver struct {
    Client client.Client
    // ...
}

func (r *Resolver) PopulateClusterDefaults(ctx context.Context, cluster *MultigresCluster) error {
    // 1. apply static defaults (images, etc)
}

func (r *Resolver) Resolve(ctx context.Context, cluster *MultigresCluster) (*ResolvedCluster, error) {
    // 1. Load Templates (Core, Cell, Shard)
    // 2. Initialise Defaults
    // 3. Apply Overrides (Deep Merge)
    // 4. Return fully hydrated configuration
}
```

### 2. Webhook: The "Visible" Layer

The Webhook (`pkg/webhook/handlers/defaulter.go`) calls `Resolve` and writes the result back to the object.

1.  **Static Defaulting**: Sets images, system catalog.
2.  **Implicit Promotion**: If `coreTemplate` is empty but `default` exists, it sets `coreTemplate: "default"`.
3.  **Expansion**: Resolves all deep references logic context.

### 3. Controller: The "Resilient" Layer

The Controller (`pkg/cluster-handler/controller`) is the ultimate source of truth.

```go
// pkg/cluster-handler/controller/multigrescluster/multigrescluster_controller.go
func (r *MultigresClusterReconciler) Reconcile(...) {
    // 1. Create Resolver
    res := resolver.NewResolver(...)

    // 2. Populate Defaults (In-Memory)
    // Even if Webhook failed or didn't run, we ensure defaults are set here.
    res.PopulateClusterDefaults(ctx, cluster)

    // 3. Resolve & Reconcile
    // We proceed to create child resources based on the fully resolved state.
    // This allows the system to work even if the Spec stored in etcd is "sparse".
}
```
