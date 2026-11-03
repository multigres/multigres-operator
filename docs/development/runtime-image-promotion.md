# Runtime image promotion

The nightly compatibility workflow promotes runtime defaults only after a
canary on `multigres/multigres-operator`'s `main` branch passes. Proto dependency
sync does not change these defaults. Manual canaries are diagnostic by default;
explicit manual promotion requires a successful upstream build ID and fresh
qualification on main.

## Evidence and image mapping

Resolution selects a completed, successful scheduled upstream nightly build and
freezes the operator commit being tested. Preflight resolves immutable image
digests, checks for Linux amd64 and arm64 manifests, and verifies GitHub build
attestations against the upstream source SHA, `refs/heads/main`, the nightly
workflow identity, and the expected image repository. E2E then tests those exact
digests against the frozen operator commit.

After a successful canary, the promotion job commits
`config/runtime-image-promotion.json` with schema version 1, `upstream_sha`,
`operator_sha`, `nightly_run` and `canary_run` (IDs, attempts, and URLs), and an
`images` map containing the three fully qualified digest references. This file is
created by the first successful promotion; it is not seeded with untested images.

The same Git commit updates these constants in `api/v1alpha1/image_defaults.go`:

| Image | Defaults |
| --- | --- |
| `pgctld` | `DefaultPostgresImage` |
| `multigres` | `DefaultMultiadminImage`, `DefaultMultiorchImage`, `DefaultMultipoolerImage`, `DefaultMultigatewayImage` |
| `multiadmin-web` | `DefaultMultiadminWebImage` |

Etcd and Postgres exporter defaults are preserved. The PR describes the current
main and proposed images and links both build and test evidence.

## Lite / Minigres runtime requirements

Lite clusters run `minigres` from `spec.images.multipooler`. Use a runtime built
from `multigres/multigres` main at commit
[`da2974e09ed14e86b9a254471b0c8d7df25e840a`](https://github.com/multigres/multigres/commit/da2974e09ed14e86b9a254471b0c8d7df25e840a)
or a descendant containing both of these changes:

- [#1559](https://github.com/multigres/multigres/pull/1559) packages
  `/multigres/bin/minigres` in the runtime image (`3d7ea95`).
- [#1554](https://github.com/multigres/multigres/pull/1554) removes remote-pooler
  TLS flags and enables serving and restarting after bootstrap while etcd is
  unavailable (`da2974e`).

The earlier `3d7ea95` packaging baseline alone does not provide outage recovery.
A source revision is a compatibility requirement, not evidence that an image has
passed the operator canary. Select immutable `multigres` and `pgctld` image
references from the same verified upstream build; use `spec.images.multipooler`
and `spec.images.postgres` overrides for qualification until promoted defaults
meet this baseline. Do not promote an untested digest or use a mutable `main`
tag as a production pin. Compiled defaults still move only through the verified
canary and promotion process above.

Minigres's gateway reaches its pooler in memory. Lite must not pass
`--multipooler-grpc-*`: newer runtimes reject those flags. Keep `--grpc-*` TLS
on the external admin server for the operator's Status and ReloadConfig RPCs,
and keep topology-client TLS when configured. Setting `spec.certCommonName`
enables public PostgreSQL TLS on Lite: the operator issues the certificate,
mounts `generated-certs` read-only as `public-tls-certs`, and passes Minigres
the `--pg-tls-cert-file` and `--pg-tls-key-file` flags. The public certificate
volume is separate from internal mTLS.

Lite passes `--postgres-unrecoverable-timeout=0` so PostgreSQL keeps retrying
failed starts after a repair, such as freeing or expanding storage. Lite has no
replacement primary to take over from a quarantined node. This flag intentionally
changes existing Lite pod hashes and causes one restart; HA keeps its default.

Minigres persists `topo-database.pb` in `--pooler-dir`, which Lite mounts from
its data PVC. When topology registration times out or is unavailable, the
operator can recreate a missing or terminal pod using an existing bound,
non-deleting data PVC. It retries registration and defers spec-driven replacement,
filesystem-resize restarts and configuration reload RPCs until topology returns.
The PVC's existence does not prove bootstrap: Minigres must validate its cached
record, and a fresh database still needs etcd for its first bootstrap. An explicit
missing-database response is not an outage fallback. Backups and admin operations
that require topology can still fail during an outage.

Lite requires exactly one cluster cell. Mode is immutable in both directions:
converting between Lite and distributed mode requires a data-preserving migration
that is not implemented. The resolved database/table-group/shard/pool/cell
identity is also fixed. Switching templates is allowed only when that identity
stays the same; resource, image and supported configuration changes remain valid.
Admission checks template consumers, including inline shards that still inherit
from the global or fallback shard template. Creating, updating or deleting such a
template must preserve the single resolved pool and its identity. CRD/controller
guards prevent a changed identity from provisioning a second static primary
alongside existing data.

### Published Lite runtime

The Lite fixture pins published images from upstream revision
[`56439a46025bdf7d7f94f4f96152f5b4f7d57eb6`](https://github.com/multigres/multigres/commit/56439a46025bdf7d7f94f4f96152f5b4f7d57eb6),
promoted by [operator #768](https://github.com/multigres/multigres-operator/pull/768).
This runtime includes static-leader backups
([#1584](https://github.com/multigres/multigres/pull/1584)) and topology-record
recreation ([#1561](https://github.com/multigres/multigres/pull/1561)).

Static leaders publish their leadership mode so Multiadmin can route backup,
list, verify, expire and job-status operations to them without `ForcePrimary`.
Their backups do not take an etcd lease. Multiadmin still needs topology to find
the pooler, so this does not make admin backup operations available during an
etcd outage. Preserve per-instance topology roots and backup-location isolation;
these are assumptions of the upstream implementation.

The fixture uses unmodified published images. The historical local
backup-shard-selection patch is no longer needed: this runtime filters poolers
by the requested shard before selecting a backup target. The E2E test retains
its unknown-shard rejection assertion to check that behavior.

Minigres supports a shared config file with `multigateway.*` and
`multipooler.*` keys, but its CLI flags remain compatible with the operator's
flag-based configuration. Lite qualification continues to use `default/0-inf`;
multipooler's support for other shards does not establish Minigres gateway
routing for them or relax Lite's identity guards.

### Runtime qualification

Before qualifying a newer runtime for Lite, exercise these scenarios against the
exact candidate image digests:

1. Bootstrap a TLS-enabled Lite cluster and verify SQL reads/writes plus the
   external admin Status and ReloadConfig RPCs.
2. Stop access to etcd after bootstrap and verify SQL keeps serving.
3. Restart the Minigres container, then separately delete/evict the Lite pod;
   verify recovery, retained PVC identity and persisted SQL data while etcd is down.
4. Restore etcd and verify topology registration and pending reconciliation resume.
5. Verify a fresh deployment cannot bootstrap without etcd.
6. Connect through the gateway Service with `sslmode=verify-full`, verify SQL
   writes/reads, reject an untrusted CA and an incorrect hostname, and reconnect
   after pod replacement with internal mTLS enabled.
7. Back up without `ForcePrimary`, list, verify and expire backups, recover job
   status after Multiadmin replacement, and restore into the original data PVC.

Controller unit and envtest regressions exercise flags, outage handling and PVC
retention, but do not run Minigres or establish runtime SQL compatibility. Keep
that runtime qualification evidence separate from controller test results.

The dedicated `minigres` e2e package automates these scenarios in its own Kind
cluster with cert-manager, a persistent external etcd deployment, and a Lite
cluster. It checks authenticated Status/ReloadConfig calls and rejection of
plaintext, missing client certificates and incorrect server identities. After
stopping etcd, it requires 45 consecutive SQL write/read iterations without
retries, restarts the Minigres container, replaces the pod, and compares row
checksums and PVC/PV identities. It also checks the cold-bootstrap gate and waits
for both gateway and pooler records to advertise the replacement IP after etcd
returns. That outage scenario uses the gateway Service with `sslmode=disable`.
A separate `TestMinigresPublicTLS` scenario sets `certCommonName`, lets the
operator and cert-manager issue the certificate, and runs an independent psql
client through the gateway Service with `sslmode=verify-full`. It checks the
negotiated TLS connection, writes/reads, wrong-hostname and untrusted-CA
rejection, and verified SQL after pod replacement. Admin mTLS remains enabled
and is checked before and after replacement. RWO storage is explicit because
Kind's local-path provisioner does not implement the default ReadWriteOncePod
access mode.

`TestMinigresRetriesPostgresAfterProlongedFailure` injects a PostgreSQL startup
fault for six minutes, then removes it and verifies automatic SQL recovery with
retained data and no pod or Minigres container replacement. It tests the failed-start
timeout without filling the Kind host's disk; CSI capacity expansion is not covered.

Every code PR runs this package with fixture images pinned to the qualified
upstream revision `56439a46025bdf7d7f94f4f96152f5b4f7d57eb6`. The nightly canary
runs both the shared suite and this dedicated package with its verified candidate
digests through `E2E_*` overrides. Both must pass before promotion or a green
compatibility report. The fixture pins do not change compiled runtime defaults.
Manual canaries against older operator refs must include this test package.

On 2026-10-26 the merged Lite branch was qualified locally against the earlier
`d063d0b437dac1fb935e0476c142f1a42a2af9f1` image pair on linux/arm64, in a
disposable single-node Kind cluster with local ReadWriteOnce storage. The full
package passed in 407.886 seconds (246.89 seconds for the runtime scenario):
admin and topology mTLS, 45 uninterrupted outage
write/read iterations, Minigres container restart, complete pod replacement with
the original PVC/PV, blocked cold bootstrap, and restored topology registration
and Healthy status all passed. All 49 final rows survived, with fingerprint
`49:4969bfd9794e22638e7acdbbb0e00f70`. The cluster was removed afterward. The operator
and GC binaries were cross-built with `CGO_ENABLED=0` and packaged in the standard
distroless runtime. That 2026-10-26 run does not qualify amd64, multi-node CSI,
ReadWriteOncePod fencing, public SQL TLS, or static-leader backups.

On 2026-10-28, the regular Minigres package passed again on local linux/arm64
with the same promoted fixture images in 446.047 seconds. The outage/recovery
scenario passed in 258.21 seconds. `TestMinigresPublicTLS` passed in 69.09 seconds:
an independent psql client negotiated TLS 1.3 through the gateway Service with
`sslmode=verify-full`, wrote and read rows, rejected the wrong hostname and an
untrusted CA, and reconnected after pod replacement with the original PVC and
persisted row. Admin mTLS passed before and after replacement. This qualifies
public SQL TLS for those fixture images on single-node Kind with RWO storage;
it does not extend the platform/storage coverage or qualify public SQL TLS for
the historical locally patched backup runtime. The disposable cluster was
removed after the run.

Run the same qualification locally with Docker, Kind and kubectl available:

```sh
make container pull-e2e-images kustomize
OPERATOR_IMG="$(make -s print-img)" REPO_ROOT="$PWD" \
  E2E_FAILURE_LOG_DIR=/tmp/minigres-e2e-logs \
  go test -tags=e2e ./test/e2e/dedicated/minigres -v -count=1 -timeout=30m
```

Set `E2E_MULTIPOOLER_IMAGE`, `E2E_MULTIADMIN_IMAGE` and `E2E_POSTGRES_IMAGE`
to immutable references from the same upstream build to qualify a newer runtime.
Without overrides, all three use the published fixture pins. The framework
prepares digest images inside Kind before workload startup. The test restores
etcd on failure, collects namespace diagnostics, and destroys its dedicated
cluster by default. Set
`E2E_KEEP_CLUSTERS=on-failure` to retain it for investigation. CI uploads failure
logs through the existing reusable e2e workflow; test output records the tested
image references, recovery identities and final checksum.

### Backup and restore coverage

`TestMinigresBackupAndRestore` runs with the normal `e2e` build tag, alongside
the other Lite tests in PR CI and the nightly compatibility canary. No extra
build tag, local runtime patch or image override is required.

The test covers backups without `ForcePrimary`, listing, verification,
expiration, job lookup after Multiadmin replacement, and recovery after erasing
only the test instance's PGDATA. It compares restored SQL contents and PVC
identities, then checks that new writes succeed. A second Lite instance shares
etcd but has a distinct topology root and backup PVC. A request for an unknown
shard must fail instead of returning another shard's backups.

The destructive restore helper is restricted to `e2e-minigres`, a generated test
namespace and the verified data PVC. It stops reconciliation before removing
PGDATA and never mounts the backup PVC. The test uses topology for Multiadmin
operations; it does not qualify admin operations during an etcd outage.

To run just this scenario after preparing the operator image and tooling:

```sh
OPERATOR_IMG="$(make -s print-img)" REPO_ROOT="$PWD" \
  E2E_FAILURE_LOG_DIR=/tmp/minigres-e2e-logs \
  go test -tags=e2e ./test/e2e/dedicated/minigres \
  -run '^TestMinigresBackupAndRestore$' -v -count=1 -timeout=30m
```

## PR lifecycle and retries

Promotion maintains one open PR from `chore/promote-runtime-images` to `main`.
It uses the existing `MULTIGRES_BOT_APP_ID` and
`MULTIGRES_BOT_APP_PRIVATE_KEY`, scoped to contents and pull requests on the
operator repository. The App token ensures branch pushes and PR creation trigger
CI. Promotion PR CI validates the record and runs e2e against the committed PR
head without runtime image overrides. Local `make pull-e2e-images` uses the same
compiled defaults, including the Postgres exporter, to pull tagged images into
Docker for import. Digest-pinned images are prepared directly inside each Kind
node as described below. When the reusable workflow
checks out an older framework that still loads `testutil.MultigresImages`, it
also pulls that revision's legacy list so empty and partial overrides remain
usable. Current frameworks prepare only the compiled set and requested overrides.

Review the PR and require those checks before merging; promotion never merges
automatically.

The promotion script checks both main's record and any pending branch record for
stale revisions. Before the first record exists, it also compares the nightly
against the source revisions in the existing pinned image tags. It refuses to publish if operator main moved since the canary,
the upstream revision is stale/divergent, or an already recorded source revision
has a different digest set. Newer green sets replace all images and evidence in
one tree/ref update, retaining branch ancestry without a force push. Reprocessing
an identical set leaves its commit and original evidence unchanged.

Compatibility reporting runs before promotion, so a recovered incident is closed
before the revision can be checkpointed. This includes explicit manual promotion
on main; diagnostic runs do not close incidents. Reporting has only `issues: write`;
promotion has only `contents: write` and
`pull-requests: write`. A green canary can close the compatibility incident even
if promotion fails. Promotion errors fail the workflow without opening a
compatibility incident.

Only successful PR publication (or verification that the same set is already
published/merged) writes the `nightly-compatibility-promoted-<sha>` checkpoint.
Resolution reads these checkpoints only from successful scheduled or manually
promoted runs on main;
older green-only artifacts do not count. A failed promotion remains eligible for
the next scheduled run. A failed PR API call may leave the complete candidate
commit on the promotion branch; retry repairs PR publication before writing the
checkpoint. No partially updated image set can become visible on the branch.

## Manual qualification and promotion

Both full-suite qualification jobs have a 60-minute budget, including setup.
Existing per-scenario deadlines are unchanged. A timeout is not a green canary.

Run `Nightly upstream compatibility` from operator **main**, selecting a successful
upstream `Nightly Build` run ID and explicitly enabling `promote`. For example,
the build containing #1574 can be qualified and proposed with:

```sh
gh workflow run nightly-compatibility.yaml \
  --repo multigres/multigres-operator --ref main \
  -f nightly-run-id=39846754232 \
  -f upstream-sha=1e0ec0d823f1c9da2a5f6514ef915695639aa944 \
  -f operator-ref=main -F promote=true
```

The build may have been scheduled or manually dispatched, but must be a completed,
successful upstream Nightly Build from main. An optional `upstream-sha` must match
that build. The workflow reruns provenance, architecture and full e2e checks;
it never treats an earlier diagnostic run as qualification. All normal promotion
safeguards and the requirement for maintainer review remain in place.

Leave `promote=false` to diagnose without updating the promotion PR. Explicit
build IDs still receive build validation; SHA-only diagnostics remain supported.
Diagnostic runs can test an operator branch and never write a promotion checkpoint.

Rerunning the promotion PR's checks tests its existing pinned images, not newer
upstream images. Advancing those pins requires the scheduled canary or explicit
manual promotion. If operator main advances during qualification, rerun on current
main. Delayed GitHub PR-head metadata is retried briefly; a concurrent branch
update or persistent mismatch still fails without checkpointing.

## E2E image preparation

Before deploying the operator or creating test workloads, the framework prepares
the operator and configured runtime images on every Kind node. Tagged images,
including the locally built operator, are imported from Docker. Digest-pinned
runtime images are pulled through the node's CRI using the exact reference; the
runtime selects the correct platform and validates the digest. No retagging or
image-reference rewriting is needed.

The existing loader processes images and nodes sequentially. For a digest
reference, `crictl inspecti` checks the node's cache; if missing, `crictl pull
--pull-timeout=10m` downloads it. There is no additional retry loop or preparation
configuration. The runtime image list already removes duplicate references.

Preparation failures identify the image and node in the setup log and stop the
suite before workloads are created. The eight-minute workload-readiness deadline
is unchanged. A fresh Kind node may still need to download new image layers, but
that download time no longer consumes the workload's readiness deadline. Logs
identify the image and node being prepared.

The intended pattern is separate setup and workload validation, also used by
[Kubernetes E2E image prepulling](https://github.com/kubernetes/kubernetes/blob/master/test/e2e/e2e.go)
and [cert-manager's Kind setup](https://github.com/cert-manager/cert-manager/blob/master/make/e2e-setup.mk).

## Local validation

```sh
node --test scripts/promote-runtime-images.test.js scripts/nightly-compatibility.test.js scripts/e2e-images.test.js
go test -tags=e2e ./test/e2e/framework -count=1
actionlint .github/workflows/nightly-compatibility.yaml .github/workflows/pull-request.yaml .github/workflows/_reusable-e2e.yaml
```

On a generated promotion branch, also run
`node scripts/promote-runtime-images.js --check` to check the record against all
six committed constants. Full canary and promotion e2e execution takes place in
GitHub Actions with registry access and a disposable Kind cluster.
