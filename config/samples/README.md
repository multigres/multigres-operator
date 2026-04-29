# Muligres Operator Samples

This directory contains various sample configurations to help you understand how to deploy and configure the Multigres Operator.

| File | Description |
| :--- | :--- |
| `minimal.yaml` | The simplest possible cluster. Relies entirely on system defaults. |
| `templated-cluster.yaml` | Demonstrates how to use reusable `Templates` for configuration. |
| `overrides.yaml` | Advanced usage showing how to patch/override specific fields on top of templates. |
| `templates/` | A directory containing recommended `CoreTemplate`, `CellTemplate`, and `ShardTemplate` specs as individual files. |
| `default-templates/` | A directory containing individual default files (`cell.yaml`, `core.yaml`, `shard.yaml`). |
| `external-etcd.yaml` | Demonstrates connecting to an existing external Etcd cluster instead of deploying one. |
| `no-templates.yaml` | A verbose example where all configuration is defined inline (no templates used). |
| `topology.yaml` | Multi-cell deployment with region/zone topology, cell-specific overrides, and topology-aware scheduling. |

---

## 1. Minimal Cluster (`minimal.yaml`)
This sample demonstrates the **Operator Hardcoded Defaults**. You only need to define the physical topology (Cells). The operator fills in the rest.

**Input:**
```yaml
apiVersion: multigres.com/v1alpha1
kind: MultigresCluster
metadata:
  name: minimal-cluster
spec:
  cells:
    - name: "zone-a"
      zoneId: "use1-az1"
```

**What happens:**
The operator's Webhook and Controller inject standard defaults for all missing components:
1.  **GlobalTopoServer**: Created with 3 replicas (etcd).
2.  **MultiAdmin**: Created with 1 replica.
3.  **Database**: A `postgres` database is created.
4.  **TableGroup**: A `default` tablegroup is created.
5.  **Shard**: A default shard `0-inf` is created.
6.  **Pools**: A default read-write pool is created in `zone-a`.

---

## 2. Templated Cluster (`templated-cluster.yaml`)
This sample demonstrates the **Template System**. Instead of defining specs inline, you point to reusable templates.

**Prerequisite:** Apply the templates first: `kubectl apply -f config/samples/templates/`

**Input:**
```yaml
spec:
  templateDefaults:
    coreTemplate: "standard-core"    # Defines GlobalTopoServer & MultiAdmin
    cellTemplate: "standard-cell"    # Defines MultiGateway & LocalTopoServer
    shardTemplate: "standard-shard"  # Defines MultiOrch & Pools
  cells:
    - name: "az1"
      zoneId: "use1-az1" # Inherits 'standard-cell' config
    - name: "az2"
      zoneId: "use1-az2" # Inherits 'standard-cell' config
```

**What happens:**
Every cell and shard created by this cluster will inherit the configuration defined in the referenced templates. This allows you to manage configuration centrally.

---

## 3. Advanced Overrides (`overrides.yaml`)
This sample shows the interactions between **Templates** and **Overrides**. You can use a template as a base and then patch specific fields for a particular Cell or Shard.

**Input Snippet:**
```yaml
spec:
  cells:
    - name: "zone-a"
      overrides:
        multigateway:
           replicas: 3 # <--- Overrides the template's default (e.g. 2)
```

**Resulting Child Resource (Cell):**
The operator creates a `Cell` resource where the specific field is updated, but other fields (like resources, affinities) are kept from the template.

```yaml
apiVersion: multigres.com/v1alpha1
kind: Cell
metadata:
  name: overrides-cluster-zone-a
spec:
  multigateway:
    replicas: 3      # <--- The override is applied
    resources: ...   # <--- Preserved from standard-cell template
```

**Shard Overrides:**
You can also override specific pools within a shard.

```yaml
shards:
  - name: "0"
    overrides:
      pools:
        "main-app":
           storage:
             size: "200Gi" # Override storage for this specific pool
```

---

## 4. Overlay & Defaults Hierarchy

The operator resolves configuration using a 4-level precedence chain:

1.  **Inline Spec / Overrides**: Highest priority. Defined directly on the component.
2.  **Cluster Template Defaults**: Defined in `spec.templateDefaults`.
3.  **Namespace Defaults**: A template named `default` in the same namespace.
4.  **System Defaults**: Hardcoded values (fallback).

This allows you to set "sane defaults" at the Namespace level (Level 3), override them for a specific Cluster (Level 2), and tweak them for a specific Cell/Shard (Level 1).

---

## 5. Pool Replica Requirements

- **Minimum**: `replicasPerCell` must be at least **1** (CRD-enforced). Setting it to 0 is rejected.
- **Recommended**: at least **3** replicas per cell. The `AT_LEAST_2` durability policy requires 1 primary + 2 synchronous standbys so that one standby can be drained during rolling upgrades while the other maintains write quorum. The operator issues an admission warning if a pool has fewer than 3 replicas.

---

## 6. Scenarios & Gotchas

### Scenario A: Namespace Defaults (The "Happy Path")

If you create your **Default Templates** *before* deploying a minimal cluster, the cluster will automatically pick them up.

1.  **Create Templates**: `kubectl apply -f config/samples/default-templates`
2.  **Create Cluster**: `kubectl apply -f config/samples/minimal.yaml`

**Result**:
The Operator validates the cluster. It sees you have no inline spec, but it finds the templates named `default` in the namespace. It links them automatically.

```yaml
spec:
  templateDefaults:
    cellTemplate: default
    coreTemplate: default
    shardTemplate: default
```

The cluster is now "bound" to your templates. If you update the `default` CoreTemplate, the cluster will roll out the changes (unless you have specific inline overrides).

### Scenario B: The "Persistence" Gotcha (Cluster First)

If you create a **Minimal Cluster** *before* the templates exist, the operator is forced to inject **Operator Defaults** directly into the inline `spec`. This creates a permanent divergence.

1.  **Create Cluster**: `kubectl apply -f config/samples/minimal.yaml`
    *   *Result*: The webhook sees no templates. It injects operator defaults (e.g., GlobalTopoServer replicas: 3) directly into `spec.globalTopoServer`.
2.  **Create Templates**: `kubectl apply -f config/samples/default-templates`
    *   (Assume these templates define replicas: 5)
3.  **Re-Apply Cluster**: `kubectl apply -f config/samples/minimal.yaml`

**Result**:
The webhook detects the new templates and updates `spec.templateDefaults` to point to them. **HOWEVER**, the cluster **does not** switch to 5 replicas.

**Why?**
The **Inline Spec** (Level 1) always takes precedence.
*   In Step 1, we saved `replicas: 3` into the cluster's manifest.
*   In Step 3, we linked the template (`replicas: 5`), but the `replicas: 3` is still present in the inline spec.
*   Therefore, `replicas: 3` wins.

**Fix**:
To "re-bind" the cluster to the template, you must manually clear the inline fields that you want to inherit.
