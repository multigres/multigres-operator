package v1alpha1

// NOTE: The Makefile extracts image references from this file (lines matching
// = "...") to pre-load them into the kind cluster.
const (
	// DefaultPostgresImage is the default container image for PostgreSQL instances.
	// Uses the pgctld image which bundles PostgreSQL, pgctld, and pgbackrest.
	DefaultPostgresImage = "ghcr.io/multigres/pgctld@sha256:27a5f13d2cbca78f40cff802c42562808faaa19688029017fcbd7349a6d7705e"

	// DefaultEtcdImage is the default container image for the managed Etcd cluster.
	DefaultEtcdImage = "gcr.io/etcd-development/etcd:v3.6.7"

	// DefaultMultiadminImage is the default container image for the Multiadmin component.
	DefaultMultiadminImage = "ghcr.io/multigres/multigres@sha256:e1fa313f48f5183b82388197d5fb408604a92d3019f67edb651c62079e413e7a"

	// DefaultMultiadminWebImage is the default container image for the MultiadminWeb component.
	DefaultMultiadminWebImage = "ghcr.io/multigres/multiadmin-web@sha256:cafbf05764da42d7e80a31f32e5f413038ae4bd4daa4fad9e35bc74139326b78"

	// DefaultMultiorchImage is the default container image for the Multiorch component.
	DefaultMultiorchImage = "ghcr.io/multigres/multigres@sha256:e1fa313f48f5183b82388197d5fb408604a92d3019f67edb651c62079e413e7a"

	// DefaultMultipoolerImage is the default container image for the Multipooler component.
	DefaultMultipoolerImage = "ghcr.io/multigres/multigres@sha256:e1fa313f48f5183b82388197d5fb408604a92d3019f67edb651c62079e413e7a"

	// DefaultMultigatewayImage is the default container image for the Multigateway component.
	DefaultMultigatewayImage = "ghcr.io/multigres/multigres@sha256:e1fa313f48f5183b82388197d5fb408604a92d3019f67edb651c62079e413e7a"

	// DefaultPostgresExporterImage is the default container image for postgres_exporter sidecars.
	DefaultPostgresExporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.20.1"
)
