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

### Upstream compatibility review — 2026-10-26

Operator main now promotes runtime revision
[`d063d0b437dac1fb935e0476c142f1a42a2af9f1`](https://github.com/multigres/multigres/commit/d063d0b437dac1fb935e0476c142f1a42a2af9f1).
Changes since the earlier Lite qualification at `8f5c8896` include:

- Sagar's [#1573](https://github.com/multigres/multigres/pull/1573) adds a shared
  Minigres config file, with `multigateway.*` and `multipooler.*` keys. Existing
  CLI flags and `MT_*` names are unchanged, so the operator's flag-based pod
  configuration remains compatible. A mounted Minigres config file would be a
  separate operator feature; the PostgreSQL ConfigMap is not this config file.
- Haritabh's [#1549](https://github.com/multigres/multigres/pull/1549) makes query
  stream reuse unconditional and removes `--query-stream-reuse`. The operator
  does not emit this flag. Keep gateway and pooler runtimes from the same build;
  Lite embeds both in one Minigres process.
- Manan's [#1575](https://github.com/multigres/multigres/pull/1575) allows
  non-default tablegroups and canonical keyrange shard names in multipooler.
  This does not establish Minigres gateway routing for arbitrary shards. Lite
  qualification continues to use the catalog pair `default/0-inf`; the change
  does not justify relaxing Lite's one-database/tablegroup/shard identity guards.
- [#1568](https://github.com/multigres/multigres/pull/1568) terminates sessions
  when pinned state is lost, [#1583](https://github.com/multigres/multigres/pull/1583)
  stops statement replay after a connection error, and
  [#1588](https://github.com/multigres/multigres/pull/1588) fixes IPv6 dialing.
  These arrive through the promoted runtime; they need no new operator flags or
  protobuf calls. The operator's Go dependency can remain at `6fd434f73b20`.

Sagar's [#1584](https://github.com/multigres/multigres/pull/1584) is still open as
of this review. It publishes static leadership in topology, routes normal
multiadmin backup operations to that primary, and removes backup lease dependence
on etcd for static leaders. The promoted runtime does **not** include it. A backup
volume/configuration and passing SQL outage tests do not qualify normal Lite
backup/list/verify/expire or backup recovery. The local candidate procedure below
includes that work before merge and tests a matching Minigres/pgctld/multiadmin image set. Promotion still requires the
verified upstream build and canary process above. Preserve per-instance topology
roots and backup-location isolation; that is an assumption of the upstream change.
Topology-record recreation [#1561](https://github.com/multigres/multigres/pull/1561)
also remains open and is not included in the qualified runtime.

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
upstream revision `d063d0b437dac1fb935e0476c142f1a42a2af9f1`. The nightly canary
runs both the shared suite and this dedicated package with its verified candidate
digests through `E2E_*` overrides. Both must pass before promotion or a green
compatibility report. The fixture pins do not change compiled runtime defaults.
Manual canaries against older operator refs must include this test package.

On 2026-10-26 the merged Lite branch was qualified locally against this promoted
image pair on linux/arm64, in a disposable single-node Kind cluster with local
ReadWriteOnce storage. The full package passed in 407.886 seconds (246.89 seconds
for the runtime scenario): admin and topology mTLS, 45 uninterrupted outage
write/read iterations, Minigres container restart, complete pod replacement with
the original PVC/PV, blocked cold bootstrap, and restored topology registration
and Healthy status all passed. All 49 final rows survived, with fingerprint
`49:4969bfd9794e22638e7acdbbb0e00f70`. The cluster was removed afterward. The operator
and GC binaries were cross-built with `CGO_ENABLED=0` and packaged in the standard
distroless runtime. This run does not qualify amd64, multi-node CSI,
ReadWriteOncePod fencing, public SQL TLS, or the pending upstream backup feature.
Compiled defaults and promotion evidence are inherited unchanged from main.

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

## Local candidate including Sagar's backup work

The `e2e,minigres_candidate` build tags add `TestMinigresBackupAndRestore`.
It requires explicit matching Minigres/multiadmin and pgctld overrides. The
normal `e2e` build continues to qualify the promoted runtime. The candidate
covers normal backups without `ForcePrimary`, listing, verification, expiration,
job lookup after multiadmin replacement, and recovery after erasing only the
test instance's PGDATA. It compares restored SQL contents and PVC identities,
then checks that new writes succeed. A second Lite instance shares etcd but has
a distinct topology root and backup PVC.

The source candidate is main `d063d0b437dac1fb935e0476c142f1a42a2af9f1` plus
Sagar's #1584 at `bc0a87704cd944ec515a065d19d56c84c3e8de9a`. Main already
contains his config-file change (#1573). One additional local patch,
`test/e2e/dedicated/minigres/testdata/backup-shard-selection.patch`, restricts
multiadmin backup selection to the requested shard. Without it, a request for
an absent shard can operate on another shard's repository. The patch includes
regressions for static, forced-static, consensus-primary and replica selection;
the operator test also rejects listing an unknown shard.

Build this source in a separate checkout, leaving the applied changes uncommitted:

```sh
operator_root="$PWD"
candidate_dir="$(mktemp -d /tmp/minigres-backup-candidate.XXXXXX)"
git clone https://github.com/multigres/multigres.git "$candidate_dir/source"
cd "$candidate_dir/source"
base=d063d0b437dac1fb935e0476c142f1a42a2af9f1
backup_pr=bc0a87704cd944ec515a065d19d56c84c3e8de9a
git fetch origin "$base" "$backup_pr"
git checkout --detach "$base"
git diff e0706cfb517dc91c299aec2fa44b943ff74c770e "$backup_pr" > "$candidate_dir/sagar.patch"
git apply "$candidate_dir/sagar.patch"
git apply "$operator_root/test/e2e/dedicated/minigres/testdata/backup-shard-selection.patch"
go test -short ./go/services/multiadmin ./go/cmd/minigres ./go/tools/viperutil/... \
  ./go/services/multipooler/internal/manager ./go/services/multipooler/internal/manager/backup -count=1
mkdir "$candidate_dir/images"
# Match the architecture of the local Kind nodes; use amd64 on x86 hosts.
for binary in minigres multiadmin pgctld; do
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath \
    -o "$candidate_dir/images/$binary" "./go/cmd/$binary"
done
cat > "$candidate_dir/images/Dockerfile" <<'DOCKERFILE'
FROM ghcr.io/multigres/multigres@sha256:dfb55d04c2837fb926cedd496f51bb6b79408ad077e05de6c3c1591ab7c819dc
COPY minigres multiadmin /multigres/bin/
DOCKERFILE
cat > "$candidate_dir/images/Dockerfile.pgctld" <<'DOCKERFILE'
FROM ghcr.io/multigres/pgctld@sha256:0d575779d24c1f840123658c8c78bdd1e33e511b1e4fedd4fd4ba76fe2f4e5fc
COPY pgctld /usr/local/bin/pgctld
DOCKERFILE
docker build -t local/multigres:pr723-sagar "$candidate_dir/images"
docker build -t local/pgctld:pr723-sagar \
  -f "$candidate_dir/images/Dockerfile.pgctld" "$candidate_dir/images"
cd "$operator_root"
make container pull-e2e-images kustomize
OPERATOR_IMG="$(make -s print-img)" REPO_ROOT="$PWD" \
  E2E_MULTIPOOLER_IMAGE=local/multigres:pr723-sagar \
  E2E_MULTIADMIN_IMAGE=local/multigres:pr723-sagar \
  E2E_POSTGRES_IMAGE=local/pgctld:pr723-sagar \
  E2E_FAILURE_LOG_DIR=/tmp/minigres-backup-candidate-logs \
  go test -tags=e2e,minigres_candidate ./test/e2e/dedicated/minigres -v -count=1 -timeout=30m
```

These local images replace only the three binaries exercised by this Lite
qualification and preserve the baseline's Postgres/pgBackRest dependencies.
They are not a release image set for distributed clusters. The destructive
restore helper is restricted to `e2e-minigres`, a generated test namespace and
the verified data PVC; it stops reconciliation and never mounts the backup PVC.
The candidate test uses topology while running multiadmin operations; it does
not claim that multiadmin works during an etcd outage.

Local linux/arm64 qualification on 2026-10-26 passed the complete candidate
package in 474.264 seconds: backup/restore in 173.39 seconds and TLS/outage
recovery in 226.85 seconds. Restore recovered all three expected rows with
fingerprint `3:9f3ef9dea5d7279dc77d54a6e7dffff1` and accepted a new write. The
outage scenario retained all 49 rows and the original PVC/PV. The dedicated Kind
cluster was removed afterward. Targeted upstream multiadmin, Minigres/config,
manager and backup-engine unit tests passed; operator candidate-test lint
reported zero issues. The tested local image indexes were:

- Minigres/multiadmin: `sha256:cf9c83d8efa91fdf5fb304efb4a795c137c2c430e872dcae2dfb2bc2b2e14967`.
- pgctld: `sha256:bc87a507085770715bcbd77123296cd4e418514ffca2ebcb39c3100b8a44689b`.

This evidence covers the local arm64 candidate, not amd64 or a published runtime
promotion. The compiled defaults and promotion record still describe main's
runtime without #1584.

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
