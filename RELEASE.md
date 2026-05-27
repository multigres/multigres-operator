# Release Process

The operator uses a dual-lane release pipeline. Both lanes build fresh container images from the exact triggering commit.

This release model meets internal and OSS needs while Multigres and Multigres Operator are under active development. It may evolve as maintainers and users needs change. Users are also welcome to build their own images from `main` at any point.

## Release lanes

### Current lane (automatic)

Every merge to `main` automatically:

1. Builds a multi-arch container image and pushes it to GHCR with the full commit SHA, `current`, and `latest` tags
2. Generates install manifests (`install.yaml`, `install-certmanager.yaml`, `install-observability.yaml`)
3. Updates the rolling [`current` pre-release](https://github.com/multigres/multigres-operator/releases/tag/current) with the new manifests

No manual steps required. Internal consumers can always pull the latest from the stable URL:

```bash
curl -LO https://github.com/multigres/multigres-operator/releases/download/current/install.yaml
kubectl apply -f install.yaml
```

### Stable lane (manual)

Semver tag pushes (`vX.Y.Z`) trigger the stable lane, which:

1. Verifies the tag points to a commit on `main`
2. Builds a multi-arch container image tagged with the semver version
3. Generates install manifests with the semver-tagged image reference
4. Creates a GitHub release with grouped changelog and install manifest assets via GoReleaser

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

```bash
# Check the GitHub release has manifests
gh release view vX.Y.Z --repo multigres/multigres-operator

# Check the container image exists on GHCR
docker buildx imagetools inspect ghcr.io/multigres/multigres-operator:vX.Y.Z

# Check the manifest references the correct image
curl -sL https://github.com/multigres/multigres-operator/releases/download/vX.Y.Z/install.yaml | grep 'image:'
```

### Pre-releases

Tags with a semver pre-release suffix (e.g., `v1.0.0-rc.1`, `v1.0.0-beta.2`, `v1.0.0-alpha.3`) trigger the same stable-lane pipeline and are automatically marked as pre-releases on GitHub by GoReleaser. The `latest` and `current` container tags are not moved by pre-release tag pushes, those are only updated on `main` pushes by the current lane.

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
