# Release Process

Main builds publish one image per commit. Stable releases reuse that verified
image and add a version alias; they do not rebuild it.

This release model meets internal and OSS needs while Multigres and Multigres Operator are under active development. It may evolve as maintainers and users needs change. Users are also welcome to build their own images from `main` at any point.

## Release lanes

### Current lane (automatic)

Every merge to `main` automatically:

1. Builds a multi-arch OCI archive without publication credentials or shared build caches
2. Inventories and scans both architectures, and generates install manifests with the full-SHA tag and image digest
3. Publishes a candidate, signs its digest, and verifies its provenance and platform SBOM attestations
4. Signs all five install manifests, platform SBOMs, `image.txt`, and `checksums.txt` with Sigstore bundles
5. Promotes the verified digest to the full commit SHA, `current`, and `latest` tags, then updates the rolling [`current` pre-release](https://github.com/multigres/multigres-operator/releases/tag/current)
6. Pushes the verified release files to GHCR as a signed OCI artifact, tagged with the full commit SHA, and moves its `current` and `latest` tags (see [Install manifests on GHCR](#install-manifests-on-ghcr))

Download the rolling installer, then follow the verification steps below before applying it:

```bash
curl -LO https://github.com/multigres/multigres-operator/releases/download/current/install.yaml
```

### Stable lane (manual)

Semver tag pushes (`vX.Y.Z`) trigger the stable lane, which:

1. Verifies the tag points to a commit on `main`
2. Verifies the main workflow's signature and provenance for the commit-SHA image, then scans it again
3. Adds the semver image tag without changing the commit-SHA tag; installers use the semver tag and image digest
4. Prepares the grouped changelog with GoReleaser in a read-only job
5. Creates a GitHub release in a separate upload-only job, without replacing existing releases or assets
6. Pushes the verified release files to GHCR as a signed OCI artifact with the semver tag

## How to cut a stable release

Before enabling or advertising dynamic tablegroups, require a passing
[dynamic tablegroup lifecycle canary](docs/development/runtime-image-promotion.md#dynamic-tablegroup-qualification)
for the exact operator revision and runtime image digests being shipped.
Ordinary releases and runtime image promotion can proceed with the feature disabled.

Make sure you push the tag to the upstream `multigres/multigres-operator` repository, not to your personal fork.

Wait for that commit's main publication to succeed before pushing a version tag.
A missing or unsigned commit-SHA image fails the stable release; it is not
replaced by a new build. Older commits published before signing was enabled
cannot use this release path without separately reviewed migration work.

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
test "$(docker buildx imagetools inspect "ghcr.io/$REPO:$SOURCE_SHA" --format '{{json .Manifest}}' | jq -er .digest)" = "${IMAGE##*@}"
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
`latest` tags. It does not push the install manifests OCI artifact. Remove candidate images when no longer needed.
Dry-run installers include the source SHA and candidate digest, but the dry run
does not publish that SHA tag. They are verification artifacts, not approved
release assets for downstream promotion.

Stable releases and full-SHA tags are write-once: retries fail if the existing
digest differs. Main rebuilds can differ if build inputs have changed. Stable
releases reuse the verified main image to avoid this collision. Recover by
rerunning failed jobs while their artifacts remain available, or cut a new
version; do not delete an existing stable release to force replacement.
A stale current run cannot advance rolling
aliases or replace the rolling GitHub release. Its failure also skips
`notify-builder`, so that run does not request a downstream image copy.
An interrupted publication may
leave a verified image without a GitHub release; these two services cannot be
updated atomically.

### Pre-releases

Tags with a semver pre-release suffix (e.g., `v1.0.0-rc.1`, `v1.0.0-beta.2`, `v1.0.0-alpha.3`) trigger the same stable-lane pipeline and are automatically marked as pre-releases on GitHub. The `latest` and `current` container tags are not moved by pre-release tag pushes, those are only updated on `main` pushes by the current lane.

## Container image tags on GHCR

| Tag | Lane | Mutability |
|-----|------|------------|
| `<full-sha>` | Current and stable | Immutable |
| `current` | Current | Updated on each `main` push |
| `latest` | Current | Updated on each `main` push |
| `vX.Y.Z` | Stable | Immutable |

## Install manifests on GHCR

Each `main` build and each stable release also publishes the verified release
files as one OCI artifact in a separate repository,
`ghcr.io/multigres/multigres-operator-manifests`. The artifact holds the five
install manifests, the platform SBOMs, `image.txt`, `checksums.txt`, and the
Sigstore bundle for each file. Its artifact type is
`application/vnd.multigres.operator.manifests.v1`.

The repository is separate from the image repository. The full-SHA tag of the
image repository is already used by the image.

| Tag | Lane | Mutability |
|-----|------|------------|
| `<full-sha>` | Current | Immutable |
| `current` | Current | Updated on each `main` push |
| `latest` | Current | Updated on each `main` push |
| `vX.Y.Z` | Stable | Immutable |

A stable release has no full-SHA tag in this repository. Its `install.yaml`
contains the semver image tag, so its content is different from the `main`
build of the same commit. The two builds use the same image digest.

Install ORAS and pull an artifact:

```bash
oras pull ghcr.io/multigres/multigres-operator-manifests:<full-sha-or-version> --output manifests
```

Verify the artifact signature before you use the files. Use the ref of the
workflow run that published it: `refs/heads/main` for a full-SHA tag, and
`refs/tags/vX.Y.Z` for a version tag.

```bash
cosign verify \
  --certificate-identity "https://github.com/multigres/multigres-operator/.github/workflows/build-and-release.yaml@$REF" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/multigres/multigres-operator-manifests:<full-sha-or-version>
```

Then verify `checksums.txt` and the files as described in
[Verify the release](#verify-the-release). If a tag exists already, a retry of
the same run reuses the published artifact. It does not replace it.

## Downstream promotion contract

`current` installers use
`ghcr.io/multigres/multigres-operator:<full-40-character-sha>@sha256:<digest>`.
Versioned installers use the release tag instead of the SHA, for example
`ghcr.io/multigres/multigres-operator:v0.12.0@sha256:<digest>`.
Both the controller and GC use the same reference, with no `latest` exception.
The registry host, repository path, and lane-specific tag remain available to
the consumer's pin check; its regex stops at `@`. Direct installs use the digest.

The downstream chart constructs its own Deployment image from the registry and
SHA pin. It does not rewrite or deploy the image reference from `install.yaml`,
so the installer's digest does not pin that downstream Deployment. Version tags
remain valid for the consumer's asset pin check, but tag-pinned downstream
deployment is not supported; keeping that tag in stable installers does not add
deployment support.

| Change | Contract affected | Consumer action |
|--------|-------------------|-----------------|
| Sign images, provenance, and platform SBOMs | A registry copy can lose signatures or change digests | Existing SHA-tag copying can continue, but does not carry a verification guarantee. To retain that guarantee, use the copy procedure below. |
| Sign installers and publish checksums | Consumers currently accept unsigned YAML | Existing filenames remain unchanged. To adopt verification, download the additional bundles and checksums and verify the original files before using their contents. |
| Use one tag plus digest for controller and GC | The consumer extracts one distinct non-`latest` tag | No parser change required: main retains the SHA and stable retains the version tag. Direct installs are digest-pinned; downstream Deployments remain SHA-tagged. |
| Reuse the main image for stable releases | Releases must have a full-SHA image tag | No consumer change. Release maintainers must wait for successful signed main publication before creating the version tag. |
| Gate publication on tests, scans, and verification | Downstream copying starts after `current` is updated | `notify-builder` still runs after the release update and sends the full source SHA. Superseded runs fail the tip check and do not dispatch a copy; not every main commit is copied downstream. |
| Keep rolling `current` publication | Assets are mutable and briefly unavailable during replacement | Retry 404s and reject a SHA mismatch. Fetch the entire asset set again if verification fails during replacement. |
| Publish install manifests as an OCI artifact on GHCR | Per-commit manifests exist only in the rolling `current` release, which is recreated on each `main` push | No change is required now. A consumer can later pull `multigres-operator-manifests:<full-sha>` or `:vX.Y.Z` instead of the GitHub release assets, and verify the artifact signature first. |
| Pin build inputs and tighten CI permissions | No change to image references or asset names | None. |

`install-crds.yaml`, `install-rbac.yaml`, and `install.yaml` remain separate
assets on both release lanes. Component-image overrides such as
`MULTIGRES_IMAGE_POSTGRES` and `MULTIGRES_IMAGE_MULTIADMIN` are unchanged.

The GitHub release does not hold per-commit assets, and the delete/recreate
behavior of `current` is unchanged. The per-commit install manifests are in the
OCI artifact on GHCR. Editing a release in place would not make replacement of
multiple assets atomic. Old pins can use the OCI artifact for their commit, an existing versioned
release, or previously retained assets once `current` has advanced. GitHub immutable releases
cannot be enabled for the rolling release under this model.

### Copying verified images to another registry

The copier must preserve the complete multiarch index and its platform manifests,
plus referrers on the index and on each platform. A single-platform Docker pull
and push, or reassembling a manifest list, is not sufficient. BuildKit's inline
provenance remains disabled; signed provenance and SBOMs are separate referrers.

After verifying the source as above, use a referrer-aware copier such as ORAS.
The following adds the full-SHA tag at the destination without rewriting the
index. Copy platform referrers explicitly as well as index referrers:

```bash
SOURCE=ghcr.io/multigres/multigres-operator
DESTINATION=registry.example.com/multigres/multigres-operator
DIGEST=${IMAGE##*@}
test "$(docker buildx imagetools inspect "$SOURCE:$SOURCE_SHA" --format '{{json .Manifest}}' | jq -er .digest)" = "$DIGEST"
oras cp --recursive "$SOURCE@$DIGEST" "$DESTINATION:$SOURCE_SHA"
for arch in amd64 arm64; do
  platform=$(docker buildx imagetools inspect "$SOURCE@$DIGEST" --raw | jq -er \
    --arg arch "$arch" '.manifests[] | select(.platform.os == "linux" and .platform.architecture == $arch) | .digest')
  oras cp --recursive "$SOURCE@$platform" "$DESTINATION:sbom-$arch-$SOURCE_SHA"
done
test "$(docker buildx imagetools inspect "$DESTINATION:$SOURCE_SHA" --format '{{json .Manifest}}' | jq -er .digest)" = "$DIGEST"
```

Before deploying, repeat image signature, provenance, and both platform SBOM
checks against the destination, keeping the original source repository, workflow,
ref, and commit as verification policy. Fail promotion if any evidence is missing
or invalid. ORAS recursive copying does not copy unrelated legacy cosign `.sig`
or `.att` tags; legacy images need those tags copied separately. The new workflow
uses cosign v3 OCI referrers.

The copier and destination registry must be tested together before enforcing
destination verification. Neither their referrer support nor digest preservation
is established by the operator's CI. Installer signatures cover the original
downloaded bytes, not YAML modified for the destination registry; retain the
verified originals and the source-to-destination digest mapping.

## ⚠️ Important

- Never retag a published semver version. The Go module proxy caches tag-to-commit mappings permanently.
- Tags must point to commits on `main`. The pipeline rejects tags on other branches.
- All GitHub Actions are pinned to commit SHAs per supply-chain security policy.
