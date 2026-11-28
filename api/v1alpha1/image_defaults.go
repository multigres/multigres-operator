package v1alpha1

// NOTE: The Makefile extracts image references from this file (lines matching
// = "...") to pre-load them into the kind cluster.
const (
	// DefaultPostgresImage is the default container image for PostgreSQL instances.
	// Uses the pgctld image which bundles PostgreSQL, pgctld, and pgbackrest.
	DefaultPostgresImage = "ghcr.io/multigres/pgctld@sha256:936c63e2967e07ab2b2ff8218ea8a848db21d4dd3b0cc2b60ddb6dcd7bf119a8"

	// DefaultEtcdImage is the default container image for the managed Etcd cluster.
	DefaultEtcdImage = "gcr.io/etcd-development/etcd:v3.6.7"

	// DefaultMultiadminImage is the default container image for the Multiadmin component.
	DefaultMultiadminImage = "ghcr.io/multigres/multigres@sha256:5c50189cd2f813822a0142e2064ffabe93c5987b7a4da390c9dea2f3fdfbc4d5"

	// DefaultMultiadminWebImage is the default container image for the MultiadminWeb component.
	DefaultMultiadminWebImage = "ghcr.io/multigres/multiadmin-web@sha256:c3f6f2fc5395d4bba73bc19c3013b5699568c070a2bb6cc35deaf17a04346e39"

	// DefaultMultiorchImage is the default container image for the Multiorch component.
	DefaultMultiorchImage = "ghcr.io/multigres/multigres@sha256:5c50189cd2f813822a0142e2064ffabe93c5987b7a4da390c9dea2f3fdfbc4d5"

	// DefaultMultipoolerImage is the default container image for the Multipooler component.
	DefaultMultipoolerImage = "ghcr.io/multigres/multigres@sha256:5c50189cd2f813822a0142e2064ffabe93c5987b7a4da390c9dea2f3fdfbc4d5"

	// DefaultMultigatewayImage is the default container image for the Multigateway component.
	DefaultMultigatewayImage = "ghcr.io/multigres/multigres@sha256:5c50189cd2f813822a0142e2064ffabe93c5987b7a4da390c9dea2f3fdfbc4d5"

	// DefaultPostgresExporterImage is the default container image for postgres_exporter sidecars.
	DefaultPostgresExporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.20.1"
)
