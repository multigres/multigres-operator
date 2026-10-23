# Runtime image promotion

The scheduled compatibility workflow selects a successful scheduled upstream
Nightly Build, verifies signed image provenance and both supported architectures,
and runs the full operator e2e suite. Only a successful canary may update the
runtime image promotion PR. Its committed-defaults check and maintainer review
are still required before merge.

Both full-suite qualification jobs have a 60-minute budget, including setup.
Existing per-scenario deadlines are unchanged. A timeout is a failed qualification;
never promote its results as a green canary.

## Promote a specific verified build manually

Run `Nightly upstream compatibility` from operator **main** with:

- `nightly-run-id`: the successful upstream `Nightly Build` run ID.
- `operator-ref`: `main`.
- `promote`: `true`.
- `upstream-sha`: optionally the full expected commit; it must match the build.

For example, the build containing the #1574 fix can be qualified and proposed with:

```sh
gh workflow run nightly-compatibility.yaml \
  --repo multigres/multigres-operator --ref main \
  -f nightly-run-id=39846754232 \
  -f upstream-sha=1e0ec0d823f1c9da2a5f6514ef915695639aa944 \
  -f operator-ref=main -F promote=true
```

This accepts either a scheduled or manually dispatched successful upstream
Nightly Build from main. It does not reuse a previous canary: provenance,
architectures and the full e2e suite are checked again. The source must belong to
upstream main. The promotion records the build ID and attempt, exact source and
operator commits, image digests, and this canary's ID and attempt.

Promotion refuses stale/divergent source revisions, changed images for an already
recorded revision, and operator main advancing during qualification. It updates
all six defaults and the evidence record together in the existing promotion PR;
it does not merge or deploy. If main advances, rerun on current main.

## Diagnose without promoting

Leave `promote=false` (the default). An explicit build ID still gets the same
build validation. SHA-only diagnostics remain supported without a build ID.
Diagnostic runs may test an operator branch but never update the promotion PR
or write a successful-promotion checkpoint.

Rerunning the promotion PR's checks tests its existing pinned images; it does
not select a newer upstream build. To advance those pins, use the scheduled
canary or the explicit manual promotion above.
