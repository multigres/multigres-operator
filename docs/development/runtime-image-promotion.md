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
