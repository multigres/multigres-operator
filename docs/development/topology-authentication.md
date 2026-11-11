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
`TopoServer` owns its serving Certificate in its namespace. The administrator
Certificate is not owned by a `TopoServer`. See
[Administrator certificate](#administrator-certificate).

| Certificate and Secret | Identity / purpose | Consumer |
|:---|:---|:---|
| `<cluster>-topo-client-tls` | Canonical cluster-root CN | All topology clients in the cluster |
| `<toposerver>-topo-server-tls` | Service DNS identity with client and server usages | etcd serving and peer connections |
| `<cluster>-topo-client-tls` URI SAN | `spiffe://<trust-domain>/project/<ref>` | Matched by an issuer gate. Nothing in the operator reads it |
| `multigres-operator-topo-admin-<hash>` | CN `multigres-operator`, URI SAN `spiffe://<trust-domain>/cp/multigres-operator`, client usage | Operator etcd administration, maintenance, and managed-server identity probes |

The `multigres-operator` identity is the infrastructure administrator with the
etcd `root` role. Its Secret is never mounted into a Multigres workload.

### Administrator certificate

The administrator Certificate is issued only for a TLS-enabled TopoServer on
which authorization is managed. A TopoServer without managed authorization has
no administrator credential, and the operator uses its serving credential for
probes.

There is one administrator Certificate and Secret per distinct topology issuer,
in the operator's namespace. Both are named
`multigres-operator-topo-admin-<hash>`, where `<hash>` is derived from the issuer
kind and name. Every managed TopoServer that uses the issuer shares it. The
operator reads its own namespace from `POD_NAMESPACE`. No administrator key is
written into a project namespace, so reading Secrets in a project namespace
does not give etcd `root`. An approver-policy rule can also allow
`CN=multigres-operator` only from the operator's namespace.

A namespaced TopoServer cannot own an object in another namespace, so this
Certificate has no owner reference. The operator creates it when missing and
never deletes it, because other TopoServers can still use it. Delete it by hand
when no TopoServer uses its issuer.

Earlier versions issued `<toposerver>-topo-admin-tls` into the TopoServer's
namespace. On each reconcile the operator deletes that Certificate and Secret
when it owns them. The cleanup is best effort and a failure is logged and
retried on the next pass.

### SPIFFE URI SANs

The topology client and administrator certificates carry a SPIFFE URI SAN, for
example `spiffe://multigres.local/project/abcd` for the cluster above and
`spiffe://multigres.local/cp/multigres-operator` for the operator. An issuer
gate matches on the SAN. The CN stays as it is because etcd reads usernames
from it. For a cluster, `<ref>` is the cluster root without `/multigres/`, so it
is the project ref when the annotation is set and `<namespace>/<name>` otherwise.

The trust domain comes from the operator flag `--trust-domain`, which defaults
to `multigres.local`. Changing it, or upgrading from a version without the SAN,
makes the operator update each Certificate, and cert-manager reissues it. The
CN and the etcd user do not change.

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

A cluster-owned TopoServer may also be referenced by other clusters. The
controller discovers those references from materialized Cells and grants each
referencing cluster only its own roots. Ownership identifies the server's
lifecycle owner; it does not limit the server to that cluster's identity.

## Enabling managed authorization

Authorization is requested on the owning `MultigresCluster`. The cluster
controller resolves the inline value with any `CoreTemplate` default and
copies the result to the managed `TopoServer` child:

```yaml
spec:
  topoTLS:
    enabled: true
    issuerName: topology-ca
  globalTopoServer:
    etcd: {}
    auth:
      enabled: true
```

`CoreTemplate.spec.globalTopoServer.auth` can supply the default. An inline
`MultigresCluster.spec.globalTopoServer.auth` value takes precedence. The
managed `TopoServer` child is controller-owned and is not an operator-facing
configuration surface. The final TopoServer must have managed etcd and TLS
enabled. Admission rejects inline authorization on a plaintext cluster.
Admission cannot see a value that comes from a `CoreTemplate`, so the cluster
controller checks the resolved value before it applies the child. A resolved
request without `spec.topoTLS` sets `TopologyReady=False` on the
`MultigresCluster` with reason `TopologyAuthRequiresTLS`, and no TopoServer is
created. Authorization cannot be requested for an inline external topology server,
because that server's authorization policy is not managed by this operator.
When `auth` is present, its `enabled` field is required.

For an existing TLS-enabled cluster, first upgrade the operator and wait for
the shared administrator Certificate and Secret in the operator's namespace to
become Ready.
Then set `spec.globalTopoServer.auth.enabled: true` on the
`MultigresCluster`. Existing plaintext clusters cannot enable topology TLS in
place because `spec.topoTLS` is immutable. They need a separate migration to a
TLS-enabled topology server. There is no legacy identity compatibility mode,
so all topology clients must already use the cluster credential before auth is
requested.

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
activation of a server owned by a cluster or Cell also fails if no cluster
identities resolve.

A standalone server has no owner and is built to be shared. With no tenants it
still activates. The controller creates `root`, `multigres-operator`, and the
`root` role, enables auth, and reports `Reconciled`. A cluster that references
the server later is granted access when its first Cell appears.

Each identity's etcd calls run under their own 10 second deadline. A slow
identity cannot use up the time of those after it. Before activation, the first
failing identity fails the whole pass and auth stays off. After activation, the
controller keeps going past a failing identity. It lists up to three failures
in the `AuthorizationReady` message with reason `IdentitiesFailed` and retries
them on the next pass. A failed identity stays in `status.authIdentities`, and a
failed removal keeps the stale identity tracked.

Project-ref roots and fallback roots share one key space. A cluster named
`global` in namespace `acme`, without a project reference, keeps its records
under `/multigres/acme/global/`, which is inside project `acme`'s global grant.
The controller therefore checks every pass that no two identities have
overlapping or nested permission ranges. A conflict names both clusters in the
`AuthorizationReady` message with reason `IdentityConflict`. Clusters that share
one project reference share one identity and do not conflict. Before
activation, a conflict fails the pass and writes nothing. After activation, the
identities in the conflict are left exactly as they are, and a conflicting
identity that does not exist yet is not created. The other identities still
reconcile. Remove the cause, for example by giving one cluster a project
reference, and the next pass clears the condition.

CRD transition rules reject changing
`MultigresCluster.spec.globalTopoServer.auth.enabled` or the managed
`TopoServer.spec.auth.enabled` from true to false or removing it. The parent
may remove its authorization request only while switching to an inline
external topology server. The old managed TopoServer keeps authorization on
until the retirement workflow removes it. A `CoreTemplate` default is reusable
and is not transition-validated. It must likewise remain enabled after any
child has requested authorization. If the resolved value stops being enabled,
through the template or an inline override of it, the cluster controller
leaves the child unchanged and sets `TopologyReady=False` with reason
`TopologyAuthCannotBeDisabled` until the value is enabled again. The
controller never calls `AuthDisable`,
and etcd persists auth in its data.
Turning auth off requires etcd administration outside this controller.

`status.authEnabled` becomes true after successful activation and stays true
through later errors. The TopoServer is not `Ready` until the first activation
succeeds. After that, a credential, discovery, or grant failure does not change
`Ready`, which follows the etcd workload. The `AuthorizationReady` condition
reports the current pass
with reason `Reconciled`, `PendingCredentials`, `ReconcileFailed`,
`IdentitiesFailed`, `IdentityConflict`, or `ManagementDisabled`.

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

Before pruning a tracked identity, the controller reads the clusters that
required it from the API server instead of the informer cache, so a stale cache
cannot remove a live cluster's grants. It finds them in
`status.authIdentityClusters`, which maps each identity to its clusters. It
reads only those clusters and their Cells, and keeps the identity if one of them
still requires it. An identity recorded before that field existed has no entry.
Confirming its removal falls back to listing every cluster and Cell from the API
server. If a read fails, the pass changes nothing.

Terminating clusters and their existing owned or referencing Cells keep their
grants until those Kubernetes objects disappear, so their workloads keep access
during shutdown and cleanup.

Grant changes are a sequence of etcd RPCs, not a transaction. An interrupted
attempt can leave a partial update until a later pass succeeds.

The operator records applied grants in memory and skips unchanged identities.
It keys the record by TopoServer UID and etcd cluster ID. For each identity it
keeps a hash of the desired grant. A pass skips the etcd reads and writes for an
identity when its grant hash equals the recorded one. An identity that fails, is
pruned, or is frozen by a conflict loses its entry. The operator drops every
entry for a server when etcd reports that auth is off or reports a different
cluster ID. The record is lost on restart, so the first pass after a restart
verifies every identity once.

Entries do not expire. A change made directly in etcd to a project role is not
repaired until the operator restarts or that identity's grant changes. The
operator treats a manual change to etcd roles as unlikely, and a restart is the
fix when it happens.

The controller reconciles grants when a Cell that references the server is
created, deleted, or has its spec changed, so a new cluster is granted access
as its first Cell appears. Its first topology registration can still race the
grant and log one or two `permission denied` events before it succeeds.

The controller also watches MultigresCluster deletions. It resolves the servers
the deleted cluster named from its last spec, and reconciles the auth-managed
ones. A cluster delete needs its own watch for two reasons. A cluster can have
no Cells, so no Cell event follows. The Cell deletions that cascade from a
cluster delete can also arrive while the cluster is still in the cache, and then
the identity is still required. If the cluster uses a template that cannot be
read, the controller enqueues nothing extra for it. A standalone server finds
its tenants through an index of Cells by referenced server and an index of
clusters by inline endpoints. Endpoints a CellTemplate supplies appear through
the Cells they produce.

While auth is managed, the controller resyncs grants every hour, the same
interval as etcd maintenance. That is the safety net for a missed event. There
is no shorter recheck.

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
the administrator Secret in the operator's namespace, whatever scheme or alias
the reference used.
Probes of the retiring server use the same Service and Secret. For any other
endpoint the probe uses the supplied reference unchanged, so its credential
needs enough privilege on the external etcd for those two RPCs.

An unreadable membership blocks retirement and emits `GlobalTopoRetireBlocked`
instead of deleting the old server. See
[`retire_global_topo.go`](../../pkg/cluster-handler/controller/multigrescluster/retire_global_topo.go)
for the full rollout and retirement checks.

## Limits

Auth on a server shared by many clusters is sized for tens of clusters, not
thousands. Per-cluster grants, with a grant per MultigresCluster, a finalizer,
and key cleanup, are the planned fix. A pass with nothing to change costs one auth status read plus the reads for the
two administrator users and the `root` role. The first pass after an operator
restart verifies every identity, at two role reads and one user read each, so it
grows with the number of identities. etcd serves each of those reads through
raft. Each Cell spec change on a referencing cluster re-runs the pass through
the Cell watch. The pass recomputes every grant from the API cache, and it
changes etcd only for the identities whose grants differ.

Every certificate name is forgeable by anyone who can get a certificate from
the trusted CA, not only `multigres-operator`. A cluster CN is a topology root,
not a secret, so the same holds for any cluster. An issuer gate is therefore
required for tenant isolation. Use a ValidatingAdmissionPolicy or an
approver-policy rule that checks the requester, the CN, and the URI SAN.

The container that runs Postgres, `pgctld`, never mounts the topology client
Secret in a pool or a Lite pod, because that certificate can rewrite the
project's own primary records and Postgres runs customer code.

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

To verify etcd state, copy `ca.crt`, `tls.crt`, and `tls.key` from the
`multigres-operator-topo-admin-<hash>` Secret in the operator's namespace into a
directory, then run from a host that can reach the TopoServer Service. This
needs cluster-admin on the operator namespace, because that Secret is etcd
`root`. A short-lived break-glass certificate is planned as a follow-up.

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
| `authEnabled` stays false | Read the `AuthorizationReady` reason. `PendingCredentials` means the serving Secret or the shared administrator Secret is incomplete. For `ReconcileFailed`, the message names the unresolved Cell, custom-credential cluster, invalid root, or etcd error. `IdentityConflict` names two clusters whose roots overlap. |
| Workloads fail after activation despite `authEnabled: true` | Check that the cluster Certificate is Ready and every topology client mounts the cluster Secret. |
| Custom credential cluster blocks activation | Use the operator-issued cluster certificate, or manage authorization on the external etcd independently. |
| Removed identities remain in etcd | Leave `status.authIdentities` intact and read the `AuthorizationReady` message for the failed cleanup RPC. Identities the controller did not create need separate administration. |
| Retirement is blocked on membership reads | Check that the endpoint is a recognized name. An unrecognized endpoint needs a credential with privileges for `MemberList` and `Status`. Inspect `GlobalTopoRetireBlocked` events and controller logs. |
