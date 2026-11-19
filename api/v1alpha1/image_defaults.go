package v1alpha1

// NOTE: The Makefile extracts image references from this file (lines matching
// = "...") to pre-load them into the kind cluster.
const (
	// DefaultPostgresImage is the default container image for PostgreSQL instances.
	// Uses the pgctld image which bundles PostgreSQL, pgctld, and pgbackrest.
	DefaultPostgresImage = "ghcr.io/multigres/pgctld@sha256:1618e70d0d68bc1714b413e862d1272dfce6da77ed08bfd236dbf75206a59699"

	// DefaultEtcdImage is the default container image for the managed Etcd cluster.
	DefaultEtcdImage = "gcr.io/etcd-development/etcd:v3.6.7"

	// DefaultMultiadminImage is the default container image for the Multiadmin component.
	DefaultMultiadminImage = "ghcr.io/multigres/multigres@sha256:c01b0e5276b54b604718a566718282abde400868b8382aec56b33c9265d0dea1"

	// DefaultMultiadminWebImage is the default container image for the MultiadminWeb component.
	DefaultMultiadminWebImage = "ghcr.io/multigres/multiadmin-web@sha256:4e5d4918183e1f8121553180b83206939b0f4f2bfa832172dea6d629ee845543"

	// DefaultMultiorchImage is the default container image for the Multiorch component.
	DefaultMultiorchImage = "ghcr.io/multigres/multigres@sha256:c01b0e5276b54b604718a566718282abde400868b8382aec56b33c9265d0dea1"

	// DefaultMultipoolerImage is the default container image for the Multipooler component.
	DefaultMultipoolerImage = "ghcr.io/multigres/multigres@sha256:c01b0e5276b54b604718a566718282abde400868b8382aec56b33c9265d0dea1"

	// DefaultMultigatewayImage is the default container image for the Multigateway component.
	DefaultMultigatewayImage = "ghcr.io/multigres/multigres@sha256:c01b0e5276b54b604718a566718282abde400868b8382aec56b33c9265d0dea1"

	// DefaultPostgresExporterImage is the default container image for postgres_exporter sidecars.
	DefaultPostgresExporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.20.1"
)
