# Reproducible operator images

The operator and observer Dockerfiles pin their builder and runtime image
indexes by digest. Dependabot checks for Docker updates weekly.

The operator's reproducibility check builds each architecture twice with separate
BuildKit containers and no restored caches. It pins BuildKit, uses the commit
date for embedded metadata and `SOURCE_DATE_EPOCH`, rewrites layer timestamps,
and compares image manifest digests. Run it from the repository root:

```sh
bash scripts/check-reproducible-image.sh amd64
bash scripts/check-reproducible-image.sh arm64
```

The check runs for build-input PRs and weekly. It requires Docker, Buildx, jq,
network access, and space for two builds. It publishes nothing.

The comparison excludes provenance and signatures, which contain run-specific
data. Builds download dependencies and do not meet hermetic-build or SLSA Build
L3 requirements.

`make container` and `make docker-buildx` also use commit-time metadata. Plain
Docker builds default to `dev`/`unknown` unless given metadata build arguments.
The observer is excluded from the check: its `apk add curl` still uses the
mutable Alpine package repository.
