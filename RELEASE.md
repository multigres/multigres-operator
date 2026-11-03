# Release Process

The operator uses a dual-lane release pipeline. Both lanes build fresh container images from the exact triggering commit.

This release model meets internal and OSS needs while Multigres and Multigres Operator are under active development. It may evolve as maintainers and users needs change. Users are also welcome to build their own images from `main` at any point.

## Release lanes

### Current lane (automatic)

Every merge to `main` automatically:

1. Builds a multi-arch OCI archive without publication credentials or shared build caches
2. Inventories and scans both architectures, and generates digest-pinned install manifests
3. Publishes a candidate, signs its digest, and verifies its provenance and platform SBOM attestations
4. Signs all five install manifests, platform SBOMs, `image.txt`, and `checksums.txt` with Sigstore bundles
5. Promotes the verified digest to the full commit SHA, `current`, and `latest` tags, then updates the rolling [`current` pre-release](https://github.com/multigres/multigres-operator/releases/tag/current)

Download the rolling installer, then follow the verification steps below before applying it:

```bash
curl -LO https://github.com/multigres/multigres-operator/releases/download/current/install.yaml
```

### Stable lane (manual)

Semver tag pushes (`vX.Y.Z`) trigger the stable lane, which:

1. Verifies the tag points to a commit on `main`
2. Builds, scans, signs, and verifies artifacts through the same pipeline as the current lane
3. Promotes the verified image digest to the semver tag; manifests retain the digest reference
4. Prepares the grouped changelog with GoReleaser in a read-only job
5. Creates a GitHub release in a separate upload-only job, without replacing existing releases or assets

## How to cut a stable release

Make sure you push the tag to the upstream `multigres/multigres-operator` repository, not to your personal fork.

```bash
git checkout main && git pull
git tag vX.Y.Z
git push <upstream-remote> vX.Y.Z
```

That's it. The `release.yaml` workflow handles the rest.

After pushing, watch the Actions run:

```bash
gh run list --repo multigres/multigres-operator --limit 1 --json databaseId,name,status
gh run watch <run-id> --repo multigres/multigres-operator
```

### Verify the release

Install cosign v3, GitHub CLI with attestation support, Docker Buildx, and jq.
For a stable release, set the exact tag and source commit you expect. For the
rolling release use `VERSION=current` and `REF=refs/heads/main`, with the source
commit from the release you intend to install. A valid old signature alone does
not prove that a rolling artifact is current.

```bash
set -euo pipefail
VERSION=vX.Y.Z
REF=refs/tags/$VERSION
SOURCE_SHA='REPLACE_WITH_EXPECTED_FULL_SOURCE_COMMIT'
REPO=multigres/multigres-operator
IDENTITY="https://github.com/$REPO/.github/workflows/build-and-release.yaml@$REF"
gh release download "$VERSION" --repo "$REPO" --dir verified-release
cd verified-release

# Authenticate the checksum list before trusting its entries.
cosign verify-blob --bundle checksums.txt.sigstore.json \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com checksums.txt
sha256sum -c checksums.txt

IMAGE=$(cat image.txt)
cosign verify --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com "$IMAGE"
gh attestation verify "oci://$IMAGE" --repo "$REPO" --bundle-from-oci \
  --signer-workflow "$REPO/.github/workflows/build-and-release.yaml" \
  --cert-identity "$IDENTITY" --signer-digest "$SOURCE_SHA" \
  --source-ref "$REF" --source-digest "$SOURCE_SHA" --deny-self-hosted-runners

# Verify the amd64 SBOM against its platform digest (repeat with arm64).
PLATFORM=$(docker buildx imagetools inspect "$IMAGE" --raw | jq -er \
  '.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64") | .digest')
gh attestation verify "oci://${IMAGE%@*}@$PLATFORM" --repo "$REPO" --bundle-from-oci \
  --signer-workflow "$REPO/.github/workflows/build-and-release.yaml" \
  --cert-identity "$IDENTITY" --signer-digest "$SOURCE_SHA" \
  --source-ref "$REF" --source-digest "$SOURCE_SHA" --deny-self-hosted-runners \
  --predicate-type https://spdx.dev/Document/v2.3

# Only apply after all verification succeeds.
kubectl apply -f install.yaml
```

Each release file has a `.sigstore.json` bundle for individual verification.
Bundles include signing-time evidence, allowing verification after the signing
certificate expires. Keep certificate and transparency-log checks enabled.

The image signature covers the multiarch index. Each SBOM attestation covers a
platform digest; publication checks that the SBOM matches its image config.
Provenance identifies the source and workflow. These checks do not establish
runtime compatibility, hermeticity, or SLSA Build L3.

### Dry run and retry behavior

Dispatch **Release dry run** on `main` to test the build, scan, signing, and
verification steps. It publishes `candidate-<run-id>-<attempt>` images to the
repository's GHCR namespace and uploads a `verified-release` Actions artifact.
It requires package, OIDC, and attestation permissions, including on forks.
It creates no GitHub release and changes no full-SHA, semver, `current`, or
`latest` tags. Remove candidate images when no longer needed.

Stable releases and full-SHA tags are write-once: retries fail if the existing
digest differs. A full rerun can produce different provenance bytes even when
the executable images are reproducible. Recover by rerunning failed jobs while
their artifacts remain available, or cut a new version; do not delete an existing
stable release to force replacement. A stale current run cannot advance rolling
aliases or replace the rolling GitHub release. An interrupted publication may
leave a verified image without a GitHub release; these two services cannot be
updated atomically.

### Pre-releases

Tags with a semver pre-release suffix (e.g., `v1.0.0-rc.1`, `v1.0.0-beta.2`, `v1.0.0-alpha.3`) trigger the same stable-lane pipeline and are automatically marked as pre-releases on GitHub. The `latest` and `current` container tags are not moved by pre-release tag pushes, those are only updated on `main` pushes by the current lane.

## Container image tags on GHCR

| Tag | Lane | Mutability |
|-----|------|------------|
| `<full-sha>` | Current | Immutable |
| `current` | Current | Updated on each `main` push |
| `latest` | Current | Updated on each `main` push |
| `vX.Y.Z` | Stable | Immutable |

## ⚠️ Important

- Never retag a published semver version. The Go module proxy caches tag-to-commit mappings permanently.
- Tags must point to commits on `main`. The pipeline rejects tags on other branches.
- All GitHub Actions are pinned to commit SHAs per supply-chain security policy.
