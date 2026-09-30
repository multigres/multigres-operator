package v1alpha1

// NOTE: The Makefile extracts image references from this file (lines matching
// = "...") to pre-load them into the kind cluster.
const (
	// DefaultPostgresImage is the default container image for PostgreSQL instances.
	// Uses the pgctld image which bundles PostgreSQL, pgctld, and pgbackrest.
	DefaultPostgresImage = "ghcr.io/multigres/pgctld@sha256:2258e556a9281d059135be18121bfe34a76eb4a4800c51411a0ceb8456450abe"

	// DefaultEtcdImage is the default container image for the managed Etcd cluster.
	DefaultEtcdImage = "gcr.io/etcd-development/etcd:v3.6.7"

	// DefaultMultiadminImage is the default container image for the Multiadmin component.
	DefaultMultiadminImage = "ghcr.io/multigres/multigres@sha256:9e6d2bf8aeab1e04d3e86fa38c82d5b91c10847c249e436094345b15771cee4e"

	// DefaultMultiadminWebImage is the default container image for the MultiadminWeb component.
	DefaultMultiadminWebImage = "ghcr.io/multigres/multiadmin-web@sha256:84db37793db4d8e8b63ce8705aa0aac76888699cbbce173123ef875655b81cdf"

	// DefaultMultiorchImage is the default container image for the Multiorch component.
	DefaultMultiorchImage = "ghcr.io/multigres/multigres@sha256:9e6d2bf8aeab1e04d3e86fa38c82d5b91c10847c249e436094345b15771cee4e"

	// DefaultMultipoolerImage is the default container image for the Multipooler component.
	DefaultMultipoolerImage = "ghcr.io/multigres/multigres@sha256:9e6d2bf8aeab1e04d3e86fa38c82d5b91c10847c249e436094345b15771cee4e"

	// DefaultMultigatewayImage is the default container image for the Multigateway component.
	DefaultMultigatewayImage = "ghcr.io/multigres/multigres@sha256:9e6d2bf8aeab1e04d3e86fa38c82d5b91c10847c249e436094345b15771cee4e"

	// DefaultPostgresExporterImage is the default container image for postgres_exporter sidecars.
	DefaultPostgresExporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.20.1"
)
