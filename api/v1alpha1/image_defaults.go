package v1alpha1

// NOTE: The Makefile extracts image references from this file (lines matching
// = "...") to pre-load them into the kind cluster.
const (
	// DefaultPostgresImage is the default container image for PostgreSQL instances.
	// Uses the pgctld image which bundles PostgreSQL, pgctld, and pgbackrest.
	DefaultPostgresImage = "ghcr.io/multigres/pgctld@sha256:ae2298042784363fd4843436e784f0f00ef9a6224f7f50ad6ba63d6829a6f9a4"

	// DefaultEtcdImage is the default container image for the managed Etcd cluster.
	DefaultEtcdImage = "gcr.io/etcd-development/etcd:v3.6.7"

	// DefaultMultiadminImage is the default container image for the Multiadmin component.
	DefaultMultiadminImage = "ghcr.io/multigres/multigres@sha256:ef0fdcb6b6ed641e50ad06190f0f6c553062b8916249edeeaa2f7b5c4df76aa0"

	// DefaultMultiadminWebImage is the default container image for the MultiadminWeb component.
	DefaultMultiadminWebImage = "ghcr.io/multigres/multiadmin-web@sha256:b4ab64735aedcedec8c2426dd72b1751e0a8ac03c73ae9944a0fcc7e9ec0d45c"

	// DefaultMultiorchImage is the default container image for the Multiorch component.
	DefaultMultiorchImage = "ghcr.io/multigres/multigres@sha256:ef0fdcb6b6ed641e50ad06190f0f6c553062b8916249edeeaa2f7b5c4df76aa0"

	// DefaultMultipoolerImage is the default container image for the Multipooler component.
	DefaultMultipoolerImage = "ghcr.io/multigres/multigres@sha256:ef0fdcb6b6ed641e50ad06190f0f6c553062b8916249edeeaa2f7b5c4df76aa0"

	// DefaultMultigatewayImage is the default container image for the Multigateway component.
	DefaultMultigatewayImage = "ghcr.io/multigres/multigres@sha256:ef0fdcb6b6ed641e50ad06190f0f6c553062b8916249edeeaa2f7b5c4df76aa0"

	// DefaultPostgresExporterImage is the default container image for postgres_exporter sidecars.
	DefaultPostgresExporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.20.1"
)
