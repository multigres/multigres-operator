package v1alpha1

// NOTE: The Makefile extracts image references from this file (lines matching
// = "...") to pre-load them into the kind cluster.
const (
	// DefaultPostgresImage is the default container image for PostgreSQL instances.
	// Uses the pgctld image which bundles PostgreSQL, pgctld, and pgbackrest.
	DefaultPostgresImage = "ghcr.io/multigres/pgctld:sha-ba3ed8e"

	// DefaultEtcdImage is the default container image for the managed Etcd cluster.
	DefaultEtcdImage = "gcr.io/etcd-development/etcd:v3.6.7"

	// DefaultMultiAdminImage is the default container image for the MultiAdmin component.
	DefaultMultiAdminImage = "ghcr.io/multigres/multigres:sha-ba3ed8e"

	// DefaultMultiAdminWebImage is the default container image for the MultiAdminWeb component.
	DefaultMultiAdminWebImage = "ghcr.io/multigres/multiadmin-web:sha-c2db14c"

	// DefaultMultiOrchImage is the default container image for the MultiOrch component.
	DefaultMultiOrchImage = "ghcr.io/multigres/multigres:sha-ba3ed8e"

	// DefaultMultiPoolerImage is the default container image for the MultiPooler component.
	DefaultMultiPoolerImage = "ghcr.io/multigres/multigres:sha-ba3ed8e"

	// DefaultMultiGatewayImage is the default container image for the MultiGateway component.
	DefaultMultiGatewayImage = "ghcr.io/multigres/multigres:sha-ba3ed8e"

	// DefaultPostgresExporterImage is the default container image for postgres_exporter sidecars.
	DefaultPostgresExporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.18.1"
)
