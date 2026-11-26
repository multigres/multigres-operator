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

A fallback root works only on a server without managed authorization. A cluster
that uses an auth-managed server must have the project-ref annotation. See
[Project reference requirement](#project-reference-requirement).

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
| `<cluster>-topo-client-tls` URI SAN | `spiffe://<trust-domain>/project/<ref>` with a project ref, `spiffe://<trust-domain>/encoded-project/<hex>` for a ref that is not a valid path segment, `spiffe://<trust-domain>/cluster/<namespace>/<name>` without a ref | Matched by an issuer gate. Nothing in the operator reads it |
| `multigres-operator-topo-admin-<hash>` | CN `multigres-operator`, URI SAN `spiffe://<trust-domain>/cp/multigres-operator`, client usage | Operator etcd administration and maintenance |

The `multigres-operator` identity is the infrastructure administrator with the
etcd `root` role. Its Secret is never mounted into a Multigres workload.

### Administrator certificate

The administrator Certificate is issued only for a TLS-enabled TopoServer on
which authorization is managed. A TopoServer without managed authorization has
no administrator credential, and the operator uses its serving credential for
maintenance.

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

### SPIFFE URI SANs

The topology client and administrator certificates carry a SPIFFE URI SAN. Each
kind of identity has its own path, so no SAN is a path prefix of another.

| Identity | SPIFFE ID |
|:---|:---|
| Cluster with a `multigres.com/project-ref` annotation whose value matches `[A-Za-z0-9._-]+` and is not `.` or `..` | `spiffe://<trust-domain>/project/<ref>` |
| Cluster with any other `multigres.com/project-ref` value | `spiffe://<trust-domain>/encoded-project/<hex>` |
| Cluster without the annotation | `spiffe://<trust-domain>/cluster/<namespace>/<name>` |
| Operator | `spiffe://<trust-domain>/cp/multigres-operator` |

For example, a cluster with project ref `abcd` gets
`spiffe://multigres.local/project/abcd`. A cluster `db` in namespace `team`
with no ref gets `spiffe://multigres.local/cluster/team/db`. An issuer gate
matches on the SAN. The CN stays as it is because etcd reads usernames from it.

A SPIFFE ID path segment may hold only letters, digits, `.`, `_` and `-`, and
`.` and `..` are not allowed as segments. Percent-encoding is not allowed
either. A ref that does not fit is written as `encoded-project/<hex>`, where
`<hex>` is the lowercase hex of the ref's raw bytes. The ref `proj/123` gets
`spiffe://multigres.local/encoded-project/70726f6a2f313233`. The two forms
cannot collide. Their first segments differ, and hex encoding is one to one.
A ref literally named `encoded-project` stays under `project/`. The CN is not
affected. It keeps the escaped topology root. When a long fallback root is
hashed into the CN, the SAN still carries the real namespace and name.

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

The owning cluster keeps the root its server serves, `spec.etcd.rootPath`, for
as long as that server exists. When the cluster moves its global topology to
another server, its pods keep using the old one until they restart, so the
grant ends when retirement deletes the server, not when the spec moves. This
holds when the new server uses custom credentials too. A tenant moving off a
shared server can keep its grant while its existing Cell still names that
server for global or local topology and uses its operator-issued credential.

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

### Project reference requirement

Every cluster that uses an auth-managed server must carry a non-empty
`multigres.com/project-ref` annotation. The rule applies to a server the
cluster owns and to a shared or standalone server the cluster references.

The reason is how roots are built. A project-ref root is
`/multigres/<ref>`, and the ref is escaped into one path segment, so two such
roots never nest. A fallback root is `/multigres/<namespace>/<name>`. It can
land inside another cluster's prefix, for example the cluster `global` in
namespace `acme` and project `acme`. Requiring a ref wherever auth is managed
removes the overlap by construction. Clusters that share one ref share one
identity. The rule itself does not move any etcd path. Adding a project ref to a
cluster whose root was derived does move its root, and the old keys stay behind
as described below.

A server without managed authorization is not affected. Single-tenant servers
and clusters without auth keep fallback roots as before.

The rule is enforced in two places.

- The cluster controller fails the topology step. For an owned server, a
  resolved `globalTopoServer.auth` that is enabled, possibly through a
  `CoreTemplate`, on a cluster with no ref sets `TopologyReady=False` with
  reason `TopologyAuthRequiresProjectRef`. The message says to set the
  annotation. No TopoServer is created. For a cluster that points at a server
  by endpoint, the controller reads the TopoServer the endpoint names with one
  cached Get. It checks the global external endpoints and each Cell's external
  local endpoints. When that server is auth-managed, the cluster gets the same
  condition and reason. A server the controller cannot read is skipped, and the
  TopoServer controller still refuses the grant.
- The TopoServer controller refuses the grant. This check covers every
  referencing cluster, however it reaches the server. The rest of this section
  describes it.

A cluster that lacks both topology TLS and a ref is reported with
`TopologyAuthRequiresTLS` first.

The TopoServer controller grants no access to a cluster without a ref. Project-ref roots and fallback roots share one key space. A cluster named
`global` in namespace `acme`, without a project reference, keeps its records
under `/multigres/acme/global/`, which is inside project `acme`'s global grant.
It also refuses a cluster with a ref whose root is outside `/multigres/<ref>`.
A referencing cluster counts when
it owns the server, names it in `spec.globalTopoServer` or a Cell's local
topology server, or has a Cell that does.

Before activation, a refused cluster fails the pass. The controller writes
nothing to etcd, records no identity, and leaves auth off. It sets
`AuthorizationReady=False` with reason `ProjectRefRequired`. The message names
each refused cluster and why, up to three of them with a count of the rest. A
cluster that does have a ref is not granted either until the pass can activate,
because activation is all or nothing.

After activation, a refused cluster gets no new grant and the other identities
keep reconciling. The `AuthorizationReady` message names the cluster, the
condition is `False` with reason `ProjectRefRequired`, and the reconcile
returns an error so it retries. A refused cluster is never added to
`status.authIdentities`, `status.authIdentityClusters`, or the verified-grants
record. A cluster that was granted and is refused later, for example because it
lost its annotation, keeps its access. Its identity, etcd user, role, and
status record stay as they are. They are neither extended nor revoked until the
cluster is accepted again or deleted. Deleting the cluster removes them after
the usual confirmation read. Clusters that share the identity get no grant
changes in that time either. When the message
shows another problem as well, such as `IdentitiesFailed`, the refusal is
appended to it on its own line and the reason stays the other problem's.

After activation, a referencing cluster whose own configuration cannot be
authorized is refused the same way. Examples are custom topology client
credentials, a missing CoreTemplate or CellTemplate, and a project reference
that cannot form a root. Its existing access is kept, and the other clusters
keep reconciling. The condition reason is `ClusterConfigInvalid`, and the
message names the cluster and the cause. The reason is `ProjectRefRequired`
only when every refused cluster lacks a ref or has a root outside its prefix.
A failed API read is not a refusal. It fails the pass with `ReconcileFailed`,
which retries. Before activation, these errors fail the pass with
`ReconcileFailed`, as [Activation](#activation) describes.

Adding the annotation to a refused cluster lifts the refusal. It does not
finish the change for a cluster that already has data. The TopoServer controller watches the project-ref annotation on every
MultigresCluster, resolves the servers the cluster names, and reconciles the
auth-managed ones. The next pass grants the cluster its new root, or activates
auth if the cluster was the only blocker. Changing the annotation on a cluster
that already has one changes its root. That is a separate concern from this
requirement, and the old root's identity is pruned as described in
[Cleanup and retries](#cleanup-and-retries).

Adding a project ref to a cluster whose root was derived, rather than set
explicitly, moves the root to `/multigres/<ref>`. The cluster's existing keys
stay under the old fallback root, `/multigres/<namespace>/<name>`. The operator
does not move or delete them. Those stale keys remain in etcd until someone
cleans them up or migrates them, and the cluster does not see them under its new
root. Clean up or migrate the old keys by hand before adding the ref. Admission
saves a derived root in the spec of a cluster created without templates. That
root stays where it is when the ref is added, so an auth-managed server refuses
the cluster until its root is moved under `/multigres/<ref>`.

### Activation

For an existing TLS-enabled cluster, first upgrade the operator. Then establish
the issuer policy, including any approver-policy rule that allows
`CN=multigres-operator` from the operator's namespace. Verify that the
operator-issued cluster certificate is Ready and mounted by every topology
client. There is no legacy identity compatibility mode, so all topology clients
must already use the cluster credential before auth is requested. Existing
plaintext clusters cannot enable topology TLS in place because `spec.topoTLS`
is immutable. They need a separate migration to a TLS-enabled topology server.

Then set `spec.globalTopoServer.auth.enabled: true` on the `MultigresCluster`
and watch the TopoServer's `AuthorizationReady` condition. Requesting auth is
what makes the controller create the shared administrator Certificate in the
operator's namespace, so it does not exist before this step. Before it
activates auth, the controller waits for the serving and administrator Secrets
to contain all three TLS keys and reports `PendingCredentials` meanwhile. It
does not wait for the cluster Secret or for workloads to mount it. Activation
is done when `status.authEnabled` is true and `AuthorizationReady` is `True`
with reason `Reconciled`.

The controller resolves every grant before it changes etcd. That covers all
referencing clusters, their existing Cells, and Cells their specs have yet to
create. Whenever etcd reports that authorization is disabled, activation reads
Cells and MultigresClusters directly from the API server and resolves their
templates. A cluster that reaches the
server only through a CellTemplate, or names an owned server inline, counts
before it has a Cell. A Cell that names no template
resolves through the cluster's `templateDefaults.cellTemplate`, as it does when
the cluster controller creates it. One unresolvable Cell, one
referencing cluster with custom credentials, or one refused cluster fails the
whole pass. The
controller writes the previously tracked and currently desired identities to
`status.authIdentities`, configures users and roles with the administrator
certificate, and calls `AuthEnable` only after every grant succeeds. First
activation of a server owned by a cluster or Cell also fails if no cluster
identities resolve.

If etcd reports authorization disabled after a restore or manual change, the
same activation checks apply even when `status.authEnabled` remains true. A
refused cluster blocks reactivation and leaves etcd authorization disabled.
Check `AuthorizationReady` for the current refusal and correct it before the
controller can re-enable authorization.

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
failed removal keeps the stale identity tracked. Auth calls go through one
client over every etcd member, so a single unreachable member does not fail the
pass.

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
`Ready`, which follows the etcd workload. Nor does it hold back etcd
maintenance: on a `Healthy` server the controller runs maintenance first and
then returns the authorization error. The `AuthorizationReady` condition
reports the current pass
with reason `Reconciled`, `PendingCredentials`, `ReconcileFailed`,
`IdentitiesFailed`, `ProjectRefRequired`, `ClusterConfigInvalid`, or
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

When a tracked identity loses a recorded cluster, the controller reads that
cluster and its Cells directly from the API server. It finds the clusters in
`status.authIdentityClusters`, which maps each identity to its clusters, and
merges any grants the cluster still requires.

Before revoking a range from a role, the controller checks its recorded and
currently discovered clusters and their Cells against the API server. It
changes the role only when those reads confirm the proposed grant. A
previously tracked identity without recorded clusters must first reconcile
successfully without a removal to record its clusters.

The controller does not wait for pods. A cluster's spec moves to another server
before its pods restart. When a cluster moves between shared servers, the next
pass on the old server revokes its access there, and pods that have not
restarted lose it during the rollout. Such moves need manual coordination, as
[Shared topology roots](shared-topology.md) says. The owner of a server keeps
its root there, as described in
[Permissions and key ranges](#permissions-and-key-ranges).

Terminating clusters and their existing owned or referencing Cells keep their
grants until those Kubernetes objects disappear, so their workloads keep access
during shutdown and cleanup.

Grant changes are a sequence of etcd RPCs, not a transaction. An interrupted
attempt can leave a partial update until a later pass succeeds.

The operator records applied grants in memory and skips unchanged identities.
It keys the record by TopoServer UID and etcd cluster ID. For each identity it
keeps a hash of the desired grant and when it was verified. A pass skips the
etcd reads and writes for an identity when its grant hash equals the recorded
one and the entry has not expired. An identity that fails, is
or is pruned loses its entry. The operator drops every
entry for a server when etcd reports that auth is off or reports a different
cluster ID. The record is lost on restart, so the first pass after a restart
verifies every identity once.

Entries expire 30 to 45 minutes after verification. The exact time comes from
the identity name, so identities verified in one pass come due across later
passes. The first pass after an entry expires re-verifies the identity and
repairs a change made directly in etcd to its role. Passes run at least once an
hour, so repair comes within an hour of expiry. Every expiry is shorter than
the hourly resync, so a resync after a quiet hour re-verifies every identity.

The controller reconciles grants when a Cell that references the server is
created, deleted, or has its spec changed, so a new cluster is granted access
as its first Cell appears. Its first topology registration can still race the
grant and log one or two `permission denied` events before it succeeds.

The controller also watches MultigresCluster deletions and changes to the
project-ref annotation. It resolves the servers the cluster named from its last
spec, including those its Cell templates and its default
`templateDefaults.cellTemplate` supply, and reconciles the auth-managed ones. An annotation change moves no Cell
generation, so it needs the cluster watch to grant a cluster that was refused
for lacking a ref. A cluster delete needs its own watch for two reasons. A cluster can have
no Cells, so no Cell event follows. The Cell deletions that cascade from a
cluster delete can also arrive while the cluster is still in the cache, and then
the identity is still required. If the cluster uses a template that cannot be
read, the controller enqueues nothing extra for it. After activation, a
standalone server finds its tenants through an index of Cells by referenced
server and an index of clusters by inline endpoints. Endpoints a CellTemplate
supplies appear through the Cells they produce.

While auth is managed, the controller resyncs grants every hour, the same
interval as etcd maintenance. That is the safety net for a missed event. A pass
that ends with `AuthorizationReady=False` without an error, such as one waiting
on credentials, is checked again after 30 seconds. There is no other shorter
recheck.

## External credentials and endpoint matching

An external reference that sets `caSecret` or `clientCertSecret` keeps those
Secrets, and the operator issues nothing for it. See
[Client credentials](shared-topology.md#client-credentials). Custom external
credentials have no assumed cluster CN or role. Configure their issuer,
permissions, and lifecycle with the external etcd administrator.

For grant discovery and for choosing the probe credential, the controller
recognizes these names for TopoServer `topo` in namespace `infra`:

- `topo` and `topo-headless` when the requesting resource is in `infra`.
  A TopoServer whose own name ends in `-headless` works too. For a TopoServer
  named `shared-headless`, the host `shared-headless` is its client Service.
  The controller tries the host as written and with `-headless` removed. Grant
  discovery and the project reference check accept a candidate only when the
  real TopoServer name matches the host.
- `topo.infra`, `topo.infra.svc`, and `topo.infra.svc.cluster.local`, plus the
  corresponding names for `topo-headless`.
- Member names such as `topo-0.topo-headless.infra.svc` and
  `topo-0.topo-headless.infra.svc.cluster.local`, with a numeric ordinal.

Names may have an `http://` or `https://` scheme, an omitted or explicit `2379`
port, and an optional `/` path. Host matching ignores case and a final dot.
The matcher rejects userinfo, other ports, non-root paths, queries, and
fragments. It does not recognize IP addresses, load balancer hostnames, CNAMEs,
or ExternalName Services, so an endpoint written that way counts as external.

## Retirement of the old managed server

Managed authorization does not change how the operator retires a managed
global TopoServer after its cluster moves to an external server.
[Storage](../storage.md) describes those checks.

Retirement reads the external server's identity through the configured
endpoint with the configured credential, as the workloads reach it. It calls
`MemberList` and `Status`. etcd allows `Status` only to the `root` role once
auth is on, and a cluster client certificate does not have it. So when the
external server has managed authorization, that read fails. Retirement stays
blocked with a `GlobalTopoRetireBlocked` event, and the old managed TopoServer
keeps running. Retiring a server in that case is planned as a follow-up.

Retirement also reads the identity of the server being retired, through its
own Service with its serving credential, to compare cluster IDs. Once
authorization is managed on that server, the read can fail. Retirement then
skips the comparison, as it does for a dead server. The external server's
membership must still list no member of the server being retired.

Retirement waits only on the owner cluster's readiness and its own pods. It
does not check whether other clusters still use the server, so move every
tenant off it first. Deleting the owner cluster still deletes the server and
its volumes.

## Limits

Auth on a server shared by many clusters is sized for tens of clusters, not
thousands. Per-cluster grants, with a grant per MultigresCluster, a finalizer,
and key cleanup, are the planned fix. A pass with nothing to change costs one auth status read plus the reads for the
two administrator users and the `root` role, plus the reads for identities
whose entries have expired. The first pass after an operator restart, and a
resync an hour after the last pass, verify every identity, at two role reads and one user read each, so it grows
with the number of identities. etcd serves each of those reads through
raft. Each Cell spec change on a referencing cluster re-runs the pass through
the Cell watch. The pass recomputes every grant from the API cache, and it
changes etcd only for the identities whose grants differ. Until activation
succeeds, each pass also resolves the templates of every MultigresCluster.

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
| `authEnabled` stays false | Read the `AuthorizationReady` reason. `PendingCredentials` means the serving Secret or the shared administrator Secret is incomplete. For `ReconcileFailed`, the message names the unresolved Cell, custom-credential cluster, or etcd error. `ProjectRefRequired` names each refused cluster and why. Set the `multigres.com/project-ref` annotation on a cluster that has none. Move a root that is outside the project prefix under `/multigres/<ref>`. |
| A cluster has `TopologyReady=False` with reason `TopologyAuthRequiresProjectRef` | The cluster uses an auth-managed server, owned or referenced, and has no `multigres.com/project-ref` annotation. Set the annotation, or use a server without managed authorization. Access the cluster already has on the server is kept, not revoked, until the annotation is restored or the cluster is deleted. |
| Workloads fail after activation despite `authEnabled: true` | Check that the cluster Certificate is Ready and every topology client mounts the cluster Secret. |
| `AuthorizationReady=False` with reason `ClusterConfigInvalid` | Auth is active and a referencing cluster cannot be authorized. The message names the cluster and the cause, such as custom credentials or a missing template. Fix that cluster. Its existing access is kept and the other clusters keep reconciling meanwhile. |
| Custom credential cluster blocks activation | Use the operator-issued cluster certificate, or manage authorization on the external etcd independently. |
| Removed identities remain in etcd | Leave `status.authIdentities` intact and read the `AuthorizationReady` message for the failed cleanup RPC. Identities the controller did not create need separate administration. |
| Retirement is blocked after a move to a server with managed authorization | Expected. The retirement check needs `Status`, which etcd allows only to the `root` role, and the cluster certificate lacks it. The old TopoServer stays. Inspect `GlobalTopoRetireBlocked` events and controller logs. |
