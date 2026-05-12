# PostgreSQL Image Strategy

## Current Approach (v1alpha1): Bundled pgctld Image

The operator currently uses a single all-in-one image (`ghcr.io/multigres/pgctld:main`) for PostgreSQL containers. This image bundles:

- PostgreSQL 17
- pgctld binary (manages the PostgreSQL lifecycle)
- pgbackrest (backup/restore)

This approach was chosen for simplicity and compatibility during the initial release. The `buildPgctldSidecar()` function in `containers.go` is the sole code path for building the postgres container spec.

## Future Plan: Custom PostgreSQL Image Support

In a future version, the operator will support users bringing their own PostgreSQL image (e.g., `postgres:17`, a custom-built image with extensions, or a hardened enterprise image). The architecture for this would be:

1. **Init container** copies pgctld and pgbackrest binaries from the pgctld image into a shared emptyDir volume.
2. **User's PostgreSQL container** mounts the shared volume and runs pgctld from the mounted path (e.g., `/usr/local/bin/multigres/pgctld`).

This decouples the PostgreSQL version/configuration from the pgctld control plane, enabling:
- Custom PostgreSQL versions and configurations
- Enterprise or hardened PostgreSQL images with specific compliance requirements
- PostgreSQL images with custom extensions pre-installed

## Removed Dead Code

The following functions and constants from `containers.go` were scaffolded for the custom image approach but were never activated. They were removed to reduce code surface and avoid confusion:

- `buildPostgresContainer()` — built a container spec for a stock postgres image
- `buildPgctldInitContainer()` — copied pgctld/pgbackrest binaries via init container
- `buildPgctldVolume()` — created the shared emptyDir volume for binary transfer
- `PgctldVolumeName`, `PgctldBinDir`, `PgctldMountPath` — related constants

When custom image support is implemented, these can be reintroduced with proper user image handling and the correct separation between the pgctld source image and the user's PostgreSQL image.
