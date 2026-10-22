# Runtime image promotion

The nightly compatibility workflow promotes runtime defaults only after a
scheduled canary on `multigres/multigres-operator`'s `main` branch passes. Proto
dependency sync does not change these defaults. Manual canaries are diagnostic;
they cannot publish a promotion PR or mark an upstream revision handled.

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
tag as a production pin. Compiled defaults still move only through the scheduled
canary and promotion process above.

Minigres's gateway reaches its pooler in memory. Lite must not pass
`--multipooler-grpc-*`: newer runtimes reject those flags. Keep `--grpc-*` TLS
on the external admin server for the operator's Status and ReloadConfig RPCs,
and keep topology-client TLS when configured. Public PostgreSQL TLS remains
unsupported by the operator's Lite pod configuration.

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

Before qualifying a newer runtime for Lite, exercise these scenarios against the
exact candidate image digests:

1. Bootstrap a TLS-enabled Lite cluster and verify SQL reads/writes plus the
   external admin Status and ReloadConfig RPCs.
2. Stop access to etcd after bootstrap and verify SQL keeps serving.
3. Restart the Minigres container, then separately delete/evict the Lite pod;
   verify recovery, retained PVC identity and persisted SQL data while etcd is down.
4. Restore etcd and verify topology registration and pending reconciliation resume.
5. Verify a fresh deployment cannot bootstrap without etcd.

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
returns. SQL uses the gateway Service with `sslmode=disable`; public SQL TLS is
not supported in Lite. RWO storage is explicit because Kind's local-path
provisioner does not implement the default ReadWriteOncePod access mode.

Every code PR runs this package with fixture images pinned to the qualified
upstream revision `8f5c8896bb89f92c533633a0973aa92e3672060a`. The nightly canary
runs both the shared suite and this dedicated package with its verified candidate
digests through `E2E_*` overrides. Both must pass before promotion or a green
compatibility report. The fixture pins do not change compiled runtime defaults.
Manual canaries against older operator refs must include this test package.

Run the same qualification locally with Docker, Kind and kubectl available:

```sh
make container pull-e2e-images kustomize
OPERATOR_IMG="$(make -s print-img)" REPO_ROOT="$PWD" \
  E2E_FAILURE_LOG_DIR=/tmp/minigres-e2e-logs \
  go test -tags=e2e ./test/e2e/dedicated/minigres -v -count=1 -timeout=30m
```

Set `E2E_MULTIPOOLER_IMAGE` and `E2E_POSTGRES_IMAGE` to immutable candidate
references to override the fixture pair. The framework prepares digest images
inside Kind before workload startup. The test restores etcd on failure, collects
namespace diagnostics, and destroys its dedicated cluster by default. Set
`E2E_KEEP_CLUSTERS=on-failure` to retain it for investigation. CI uploads failure
logs through the existing reusable e2e workflow; test output records the tested
image references, recovery identities and final checksum.

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

Compatibility reporting and promotion are sibling terminal jobs. Reporting has
only `issues: write`; promotion has only `contents: write` and
`pull-requests: write`. A green canary can close the compatibility incident even
if promotion fails. Promotion errors fail the workflow without opening a
compatibility incident.

Only successful PR publication (or verification that the same set is already
published/merged) writes the `nightly-compatibility-promoted-<sha>` checkpoint.
Resolution reads these checkpoints only from successful scheduled runs on main;
older green-only artifacts do not count. A failed promotion remains eligible for
the next scheduled run. A failed PR API call may leave the complete candidate
commit on the promotion branch; retry repairs PR publication before writing the
checkpoint. No partially updated image set can become visible on the branch.

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
