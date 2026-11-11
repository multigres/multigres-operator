# Topology certificate authentication and authorization

Topology TLS authenticates a connection with a certificate. Managed etcd
authorization also maps the certificate's common name (CN) to an etcd user and
checks that user's role against each requested key range. The two are separate
controls. `topoTLS.enabled` alone requires a trusted client certificate but does
not restrict that client to its cluster's keys.

[Shared topology roots](shared-topology.md) describes the logical roots. This
document covers the cluster certificate and the opt-in authorization
controller. The code is in
[`auth.go`](../../pkg/resource-handler/controller/toposerver/auth.go),
[`auth_grants.go`](../../pkg/resource-handler/controller/toposerver/auth_grants.go),
[`certificate.go`](../../pkg/cluster-handler/controller/multigrescluster/certificate.go),
and [`topo_client_tls_helpers.go`](../../api/v1alpha1/topo_client_tls_helpers.go).

## Trust boundary and issuer policy

**Restrict issuance before using this as a tenant security boundary.** etcd
trusts any certificate signed by the client CA and uses its CN as the etcd
username. Cluster CNs are topology roots, not secrets. Anyone who can
get a certificate signed with another cluster's CN can act as that cluster, and
a certificate with CN `multigres-operator` gets the etcd `root` role.

The operator does not restrict who can request a certificate. Configure the
`topoTLS.issuerName` ClusterIssuer, or a CertificateRequest approver, so that
only the operator can request topology identities, including
`multigres-operator`. Restrict access to the CA key and to issued private keys.
Kubernetes RBAC is part of the same boundary. The
controller trusts ownership, labels, project-ref annotations, templates, and
topology references to decide which cluster owns which roots.

With topology TLS enabled, managed etcd sets `ETCD_CLIENT_CERT_AUTH=true` and
`ETCD_PEER_CLIENT_CERT_AUTH=true`, using the serving Secret's CA bundle for
both listeners. Once etcd auth is enabled, a client certificate CN must also
name an authorized etcd user. The controller creates passwordless users
(`NoPassword=true`) because clients authenticate through mTLS.

## Cluster identity

Every topology client in a cluster uses one identity. Multigateway,
multipooler, multiorch, multiadmin, minigres, and the operator's cluster-scoped
topology store all use the same `<cluster>-topo-client-tls` Secret. The
certificate CN is the canonical cluster `ClusterRoot`, without `/global` or a
cell suffix. Workload containers mount it through the read-only
`topo-client-tls` projected volume and the operator reads it directly.

For a cluster named `abcd`, annotated with
`multigres.com/project-ref: abcd`, the canonical cluster root is
`/multigres/abcd`. Its global root is `/multigres/abcd/global`, and a cell named
`zone-a` uses `/multigres/abcd/zone-a`. The cluster credential is:

```text
Certificate / Secret: abcd-topo-client-tls
CN / etcd user / role: /multigres/abcd
```

Without that annotation, the same cluster in namespace `example` uses
`/multigres/example/abcd` and therefore a different CN. Root escaping, fallback
hashing, and the certificate's root length limits still apply. See
[Canonical layout](shared-topology.md#canonical-layout).

## Certificate and Secret inventory

For operator-issued credentials, each Certificate and its output Secret have
the same name. cert-manager writes `tls.crt`, `tls.key`, and `ca.crt`. The
`MultigresCluster` owns the cluster certificate in its namespace. The
`TopoServer` owns the serving and administrator
Certificates in its namespace.

| Certificate and Secret | Identity / purpose | Consumer |
|:---|:---|:---|
| `<cluster>-topo-client-tls` | Canonical cluster-root CN | All topology clients in the cluster |
| `<toposerver>-topo-server-tls` | Service DNS identity with client and server usages | etcd serving and peer connections |
| `<toposerver>-topo-admin-tls` | CN `multigres-operator`, client usage | Operator etcd administration, maintenance, and managed-server identity probes |

The `multigres-operator` identity is the infrastructure administrator with the
etcd `root` role. Its Secret is never mounted into a Multigres workload.

Topology credentials are separate from the internal component gRPC
certificates such as `multipooler.<cluster>.<namespace>.multigres.internal`.
They can use different issuers and do not share a mount directory.

## Permissions and key ranges

The controller creates one etcd user and same-named role per cluster. The role
has read/write access to every exact global or cell root that belongs to that
cluster on this physical server. It has no access to another cluster's roots
and no infrastructure administration privileges.

Every grant uses a delimiter-terminated prefix and etcd's half-open range:

```text
[prefix, clientv3.GetPrefixRangeEnd(prefix))
```

For `/multigres/abcd/global/`, this is
`[/multigres/abcd/global/, /multigres/abcd/global0)`. It includes descendant
keys, but not the bare `/multigres/abcd/global` key, a sibling such as
`/multigres/abcd/global2/`, or `/multigres/abcde/global/`. Configured roots do
not end in `/`, the controller appends that delimiter when it builds a grant.

The controller rejects roots outside the canonical cluster prefix and roots
containing NULs, repeated slashes, dot segments, or a trailing slash. It does
not rewrite explicit roots. It grants only roots it can tie to this TopoServer,
through owner references or a recognized endpoint name.

## Enabling managed authorization

Authorization is opt-in on the managed TopoServer child resource:

```yaml
spec:
  tls:
    enabled: true
    issuerName: topology-ca
  auth:
    enabled: true
```

There is no cluster-wide `MultigresCluster.spec.auth` switch. The TopoServer
must have managed etcd and TLS enabled. The controller rejects authorization
on a plaintext server.

Before activation, establish the issuer policy and verify that the
operator-issued cluster certificate is Ready and mounted by workloads. Before
it activates auth, the controller waits for the serving and administrator
Secrets to contain all three TLS keys. It does not wait for the cluster Secret
or for workloads to mount it.

The controller resolves every grant before it changes etcd. That covers all
referencing clusters, their existing Cells, and Cells their specs have yet to
create, so it never activates with a partial set. One unresolvable Cell, or one
referencing cluster with custom credentials, fails the whole pass. The
controller writes the previously tracked and currently desired identities to
`status.authIdentities`, configures users and roles with the administrator
certificate, and calls `AuthEnable` only after every grant succeeds. First
activation also fails if no cluster identities resolve.

A CRD transition rule rejects changing `spec.auth.enabled` from true to false
or removing it. The controller never calls `AuthDisable`, and etcd persists
auth in its data. Turning auth off requires etcd administration outside this
controller.

`status.authEnabled` becomes true after successful activation and stays true
through later errors. The TopoServer is not `Ready` until the first activation
succeeds. After that, a credential, discovery, or grant failure does not change
`Ready`, which follows the etcd workload. The `AuthorizationReady` condition
reports the current pass
with reason `Reconciled`, `PendingCredentials`, `ReconcileFailed`, or
`ManagementDisabled`.

## Cleanup and retries

`status.authIdentities` lists the etcd users this controller created, not all
etcd users. The controller records an identity there before it changes etcd, so
it can clean up after an attempt that created some users and then stopped. Do
not clear the field to recover from a failed reconcile. That discards the
record of identities the controller may need to remove.

Each pass sets every managed role to exactly its desired permissions and every
managed user to its one role. The controller deletes identities that are in
`status.authIdentities` and no longer desired, for example when a cluster moves
to another server or a cluster is removed. It
deletes nothing else, and it never prunes `root` or `multigres-operator`.

Before pruning a tracked identity, the controller re-runs discovery against the
API server instead of the informer cache, so a stale cache cannot remove a live
cluster's grants. If that read fails, the pass changes nothing.

Terminating clusters and their existing owned or referencing Cells keep their
grants until those Kubernetes objects disappear, so their workloads keep access
during shutdown and cleanup.

Grant changes are a sequence of etcd RPCs, not a transaction. An interrupted
attempt can leave a partial update until a later pass succeeds. While auth is
managed, the controller rechecks grants every 30 seconds.

## External credentials and endpoint matching

An external reference that sets `caSecret` or `clientCertSecret` keeps those
Secrets, and the operator issues nothing for it. See
[Client credentials](shared-topology.md#client-credentials). Custom external
credentials have no assumed cluster CN or role. Configure their issuer,
permissions, and lifecycle with the external etcd administrator.

For grant discovery and for choosing the probe credential, the controller
recognizes these names for TopoServer `topo` in namespace `infra`:

- `topo` and `topo-headless` when the requesting resource is in `infra`.
- `topo.infra`, `topo.infra.svc`, and `topo.infra.svc.cluster.local`, plus the
  corresponding names for `topo-headless`.
- Member names such as `topo-0.topo-headless.infra.svc` and
  `topo-0.topo-headless.infra.svc.cluster.local`, with a numeric ordinal.

Names may have an `http://` or `https://` scheme, an omitted or explicit `2379`
port, and an optional `/` path. Host matching ignores case and a final dot.
The matcher rejects userinfo, other ports, non-root paths, queries, and
fragments. It does not recognize IP addresses, load balancer hostnames, CNAMEs,
or ExternalName Services, so an endpoint written that way counts as external.

## Retirement probes and administrator access

When a cluster moves from managed global topology to an external server, the
operator deletes the old server only after it proves that the external endpoint
reaches a different etcd cluster, so an alias to the same server cannot trigger
retirement. The identity probe makes only the read-only `MemberList` and
`Status` RPCs.

Once auth is on, those RPCs need a root-capable credential. If the external
endpoint matches a TLS-enabled managed TopoServer, the probe drops the supplied
reference and connects to `https://<toposerver>.<namespace>.svc:2379` with
`<toposerver>-topo-admin-tls`, whatever scheme or alias the reference used.
Probes of the retiring server use the same Service and Secret. For any other
endpoint the probe uses the supplied reference unchanged, so its credential
needs enough privilege on the external etcd for those two RPCs.

An unreadable membership blocks retirement and emits `GlobalTopoRetireBlocked`
instead of deleting the old server. See
[`retire_global_topo.go`](../../pkg/cluster-handler/controller/multigrescluster/retire_global_topo.go)
for the full rollout and retirement checks.

## Verification and troubleshooting

Inspect configuration and reconciliation status without reading private keys:

```sh
kubectl -n infra get toposerver topo -o yaml
kubectl -n example get certificates
kubectl -n example get pods -o yaml
```

Check the TopoServer's `spec.tls`, `spec.auth`, `status.authEnabled`,
`status.authIdentities`, and the `AuthorizationReady` condition. Check
Certificate readiness and pod Secret references, mounts, and topology flags
separately. If you suspect a username mismatch, read the CN from the issued
certificate. The cert-manager Certificate sets it through `spec.literalSubject`.

```sh
kubectl -n example get secret abcd-topo-client-tls \
  -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -noout -subject
```

To verify etcd state, copy `ca.crt`, `tls.crt`, and `tls.key` from
`<toposerver>-topo-admin-tls` into a directory, then run from a host that can
reach the TopoServer Service:

```sh
export ETCDCTL_ENDPOINTS=https://topo.infra.svc:2379
export ETCDCTL_CACERT=admin/ca.crt
export ETCDCTL_CERT=admin/tls.crt
export ETCDCTL_KEY=admin/tls.key
etcdctl auth status
etcdctl user get "$CLUSTER_CN"
etcdctl role get "$CLUSTER_CN"
```

`$CLUSTER_CN` is the CN that the `openssl` command above printed.

To verify enforcement, use a cluster certificate because the administrator
certificate bypasses the restrictions under test. Both a read and a write
beneath another cluster's root must fail:

```sh
etcdctl --endpoints=https://topo.infra.svc:2379 --cacert=cluster/ca.crt \
  --cert=cluster/tls.crt --key=cluster/tls.key \
  get /multigres/other/global/probe

etcdctl --endpoints=https://topo.infra.svc:2379 --cacert=cluster/ca.crt \
  --cert=cluster/tls.crt --key=cluster/tls.key \
  put /multigres/other/global/probe x
```

Each command should exit non-zero with `permission denied` and leave topology
data unchanged.

| Symptom | Checks |
|:---|:---|
| TLS succeeds but etcd denies access | Compare the certificate's CN with the etcd user and role, then compare the requested range with the cluster's configured roots. |
| `authEnabled` stays false | Read the `AuthorizationReady` reason. `PendingCredentials` means the serving or administrator Secret is incomplete. For `ReconcileFailed`, the message names the unresolved Cell, custom-credential cluster, invalid root, or etcd error. |
| Workloads fail after activation despite `authEnabled: true` | Check that the cluster Certificate is Ready and every topology client mounts the cluster Secret. |
| Custom credential cluster blocks activation | Use the operator-issued cluster certificate, or manage authorization on the external etcd independently. |
| Removed identities remain in etcd | Leave `status.authIdentities` intact and read the `AuthorizationReady` message for the failed cleanup RPC. Identities the controller did not create need separate administration. |
| Retirement is blocked on membership reads | Check that the endpoint is a recognized name. An unrecognized endpoint needs a credential with privileges for `MemberList` and `Status`. Inspect `GlobalTopoRetireBlocked` events and controller logs. |
