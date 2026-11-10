package v1alpha1

// NOTE: The Makefile extracts image references from this file (lines matching
// = "...") to pre-load them into the kind cluster.
const (
	// DefaultPostgresImage is the default container image for PostgreSQL instances.
	// Uses the pgctld image which bundles PostgreSQL, pgctld, and pgbackrest.
	DefaultPostgresImage = "ghcr.io/multigres/pgctld@sha256:f03c810d4f6e40d578f2c98673c8f35189b79bc49ab6d95a0c256c34a9fffff1"

	// DefaultEtcdImage is the default container image for the managed Etcd cluster.
	DefaultEtcdImage = "gcr.io/etcd-development/etcd:v3.6.7"

	// DefaultMultiadminImage is the default container image for the Multiadmin component.
	DefaultMultiadminImage = "ghcr.io/multigres/multigres@sha256:723b30aeb7045d779935f110ffe3e88c8f26ad4c33d62515f0fd905b432b6abf"

	// DefaultMultiadminWebImage is the default container image for the MultiadminWeb component.
	DefaultMultiadminWebImage = "ghcr.io/multigres/multiadmin-web@sha256:571729b2891b47e91bca8d6b8f4b22de1fc1a06cb1605d0bbb50b7a232ce50d2"

	// DefaultMultiorchImage is the default container image for the Multiorch component.
	DefaultMultiorchImage = "ghcr.io/multigres/multigres@sha256:723b30aeb7045d779935f110ffe3e88c8f26ad4c33d62515f0fd905b432b6abf"

	// DefaultMultipoolerImage is the default container image for the Multipooler component.
	DefaultMultipoolerImage = "ghcr.io/multigres/multigres@sha256:723b30aeb7045d779935f110ffe3e88c8f26ad4c33d62515f0fd905b432b6abf"

	// DefaultMultigatewayImage is the default container image for the Multigateway component.
	DefaultMultigatewayImage = "ghcr.io/multigres/multigres@sha256:723b30aeb7045d779935f110ffe3e88c8f26ad4c33d62515f0fd905b432b6abf"

	// DefaultPostgresExporterImage is the default container image for postgres_exporter sidecars.
	DefaultPostgresExporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.20.1"
)
