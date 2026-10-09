# Shared topology roots

Multigres clusters can share one physical etcd cluster while retaining
independent logical topology. Isolation comes from the root passed to the
topology client, not from provisioning a separate etcd cluster for every
Multigres cluster or cell.

## Canonical layout

When a topology root is omitted, the operator derives these paths:

```text
/multigres/<cluster-id>/global
/multigres/<cluster-id>/<cell-name>
```

The `multigres.com/project-ref` annotation is the preferred stable
`<cluster-id>`. Without that annotation, the operator uses two path segments,
`<namespace>/<MultigresCluster name>`, so clusters with the same name in
different namespaces cannot collide. Values that are not safe path segments
are percent-encoded.

When the operator issues the cluster's topology client certificate (see
[Client credentials](#client-credentials)), the cluster root is also that
certificate's common name and must fit within 64 bytes. If the namespace/name
fallback exceeds that limit, it becomes `/multigres-fallback/<hash>`, where
`<hash>` is the unpadded base64url SHA-256 digest of the original escaped
cluster root. Shorter roots, plaintext roots, and roots for an external
topology server that brings its own Secrets keep their existing paths. The `topoTLS` setting
is immutable, so this does not move running plaintext clusters to another
keyspace.

If an explicit global or cell root points outside the shortened identity, the
operator reports a certificate failure instead of changing it. Align the
configured root and migrate any existing topology data before retrying.

Explicit project refs are never shortened. When the operator issues the
client certificate, they must fit within 53 bytes after percent-encoding. An oversized ref prevents
certificate creation and sets `TopologyReady=False` with reason
`TopoCertificateFailed`, including the root length and limit. The condition
clears after a successful topology connection.

For example, a cluster annotated with project ref `proj_123` and containing
cells `zone-a` and `zone-b` uses:

```text
/multigres/proj_123/global
/multigres/proj_123/zone-a
/multigres/proj_123/zone-b
```

If a cell does not configure a separate local topology server, its cell record
uses the global/shared etcd address with the cell-specific root. The address is
shared; the global and cell keyspaces are not.

Explicit non-empty roots are preserved for both managed and external topology
configuration. This permits specialized layouts and avoids silently moving an
existing cluster's data.

## Client credentials

With `topoTLS` enabled, the operator issues each cluster a client certificate
from `topoTLS.issuerName`, with the cluster root as its common name, into the
`<cluster>-topo-client-tls` Secret. cert-manager writes `ca.crt` into the same
Secret, so it is both the client keypair and the CA bundle that verifies the
topology server.

This applies to a managed topology server and to an external one that sets
neither `caSecret` nor `clientCertSecret`. The second case is how a cluster
connects to a topology server shared by several clusters: the shared server's
serving certificate and every cluster's client certificate come from the same
issuer. An external server that sets either Secret keeps them, and the operator
issues nothing.

## Rollout and existing clusters

Older operator versions materialized `/multigres/global` in the
`MultigresCluster` spec. Because that value is explicit after admission, an
upgrade deliberately leaves it unchanged. Do not connect two such clusters to
the same etcd instance: they would still share one keyspace.

Before moving an existing cluster to shared etcd, choose one of these paths:

1. For disposable environments, remove/reset the old topology data and remove
   the materialized root so admission can assign the canonical root.
2. For environments whose topology data must survive, copy or migrate the old
   keyspace to the canonical root, update the configured root, and validate the
   cluster before retiring the old keyspace.

The operator does not automatically migrate etcd keys. Address changes and root
changes must be coordinated so components never read a partially migrated
topology.

## Physical etcd ownership

Canonical roots make shared etcd safe, but they do not by themselves change
who provisions the etcd StatefulSet. Cluster-wide shared-etcd lifecycle,
credentials, placement, and ownership are separate infrastructure/operator
work. Until that is deployed, the same root scheme also prevents collisions
when multiple clusters are pointed at an externally managed etcd service.
