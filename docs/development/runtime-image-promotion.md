# Runtime image promotion

The nightly compatibility workflow promotes runtime defaults only after a
scheduled canary on `multigres/multigres-operator`'s `main` branch passes both
the vanilla and Supabase lanes. Proto dependency sync does not change these
defaults. Manual canaries are diagnostic; they cannot publish a promotion PR or
mark an upstream revision handled.

## Evidence and image mapping

Resolution selects a completed, successful scheduled upstream nightly build and
freezes the operator commit being tested. Preflight resolves immutable image
digests, checks for Linux amd64 and arm64 manifests, and verifies GitHub build
attestations against the upstream source SHA, `refs/heads/main`, the nightly
workflow identity, and the expected image repository. E2E then tests those exact
digests against the frozen operator commit in both lanes. The Supabase lane
replaces only Postgres/pgctld with the Supabase Postgres image; the operator and
other nightly runtime images are identical across lanes. Each lane uses the
existing reusable e2e workflow and has separate failure-log artifacts.

Scheduled runs test the Supabase image named by the `SUPABASE_POSTGRES_IMAGE`
repository variable. Set it to a publicly published Supabase Postgres image, as
a fully qualified reference pinned by digest, because the reference appears in
public issues and promotion PRs. The lane fails before e2e when the variable is
unset or not pinned by digest. The e2e
result decides compatibility; the image's bundled pgctld does not need to come
from the same Multigres commit as the operator.

Before e2e, the Supabase lane runs `pgbackrest version` in the Supabase image and
in the nightly `multigres` image and fails if the output differs. pgctld and
multipooler both run pgbackrest against the same backup repository, so the
versions must match exactly.

An on-demand run can supply `supabase-image` as a fully qualified reference
pinned by digest, and `operator-ref` as a full operator commit. Manual runs
remain diagnostic and cannot promote.

After a successful canary, the promotion job commits
`config/runtime-image-promotion.json` with schema version 2, `upstream_sha`,
`operator_sha`, `nightly_run` and `canary_run` (IDs, attempts, and URLs), and an
`images` map containing the three fully qualified digest references. This file is
created from successful validation; it is not seeded with untested images.
The record also contains `lanes` with both results and `supabase.image`, the
Supabase image digest the lane tested. Version 1 records remain readable as
history, but cannot authorize a new promotion.

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
an identical set leaves its commit and original evidence unchanged. A changed
Supabase digest updates the evidence even when the three upstream image digests
are unchanged; a pending PR's evidence is also refreshed when the tested
operator revision changes. When main already carries the tested set, any open
promotion PR is closed, because it is older or records other evidence, such as
a different Supabase image.

Compatibility reporting and promotion are sibling terminal jobs. Reporting has
only `contents: read` and `issues: write`; promotion has only `contents: write`
and `pull-requests: write`. A green canary can close the compatibility issues
even if promotion fails. Promotion errors fail the workflow without opening a
compatibility issue.

Reporting keeps one open issue per kind of failure:

- `upstream-compatibility` tracks failures in resolution, preflight, or the
  vanilla lane. When the vanilla lane fails, only this issue is updated, and it
  records the Supabase lane result.
- `supabase-compatibility` tracks runs where the vanilla lane passes and the
  Supabase lane fails. It lists the Supabase image and what changed since the
  last promotion record (the Multigres range, the operator range, and whether
  the Supabase image changed), so triage can see which side moved.

Each issue is updated on consecutive failures and closed by the next scheduled
run on main where its lane passes.

Only successful PR publication (or verification that the same set is already
published/merged) writes the `nightly-compatibility-promoted-<sha>` checkpoint.
Resolution reads these checkpoints only from successful scheduled runs on main;
older green-only artifacts do not count. Only upstream revisions older than the
checkpoint are skipped. An identical upstream SHA is tested again because the
Supabase image or operator revision can change independently. A failed promotion
remains eligible for the next scheduled run. A failed PR API call may leave the complete candidate
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
