# Reproducible operator images

The operator and observer Dockerfiles pin their builder and runtime image
indexes by digest. Dependabot proposes reviewed Docker updates weekly. A tag
beside a digest is a human-readable version, not permission to follow a moved tag.

The operator's reproducibility check builds each architecture twice with separate
BuildKit containers and no restored caches. It pins BuildKit, uses the commit
date for embedded metadata and `SOURCE_DATE_EPOCH`, rewrites layer timestamps,
and compares image manifest digests. Run it from the repository root:

```sh
bash scripts/check-reproducible-image.sh amd64
bash scripts/check-reproducible-image.sh arm64
```

The check runs for build-input PRs and weekly for source changes. It requires
Docker, Buildx, jq, network access to download the pinned inputs, and sufficient
space for two independent builds. It publishes nothing. A failure blocks that
check; it must not be suppressed by replacing the expected digest.

Provenance and signing bundles intentionally include run-specific information,
so the comparison excludes those envelopes. It does not establish hermetic
builds or SLSA Build L3. Go module checksums authenticate dependency contents;
network access still occurs during dependency acquisition. An isolated builder
that generates provenance outside the source build's control is a separate
requirement for stronger provenance guarantees.

`make container` and `make docker-buildx` also use commit-time metadata. Plain
Docker builds retain their documented `dev`/`unknown` metadata defaults unless
the caller supplies build arguments. The observer's base images are pinned, but
its `apk add curl` still depends on the Alpine package repository; this check
does not claim observer image reproducibility.
