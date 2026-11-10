package v1alpha1

// NOTE: The Makefile extracts image references from this file (lines matching
// = "...") to pre-load them into the kind cluster.
const (
	// DefaultPostgresImage is the default container image for PostgreSQL instances.
	// Uses the pgctld image which bundles PostgreSQL, pgctld, and pgbackrest.
	DefaultPostgresImage = "945488516053.dkr.ecr.us-east-1.amazonaws.com/pgctld-pr@sha256:c156150b7b7f6b4c74d8470197692c735636b4b163e0e1a6fdbae577436d364c"

	// DefaultEtcdImage is the default container image for the managed Etcd cluster.
	DefaultEtcdImage = "gcr.io/etcd-development/etcd:v3.6.7"

	// DefaultMultiadminImage is the default container image for the Multiadmin component.
	DefaultMultiadminImage = "945488516053.dkr.ecr.us-east-1.amazonaws.com/multigres-pr@sha256:494ec969ab92ed9ca2062c31393e4afc021d9bcbb35396bd53d43992b2a60e42"

	// DefaultMultiadminWebImage is the default container image for the MultiadminWeb component.
	DefaultMultiadminWebImage = "ghcr.io/multigres/multiadmin-web@sha256:d1aae39f4d78054245da49a84c5682ab7b45dd01987ca6d0ddf5aafa081c1342"

	// DefaultMultiorchImage is the default container image for the Multiorch component.
	DefaultMultiorchImage = "945488516053.dkr.ecr.us-east-1.amazonaws.com/multigres-pr@sha256:494ec969ab92ed9ca2062c31393e4afc021d9bcbb35396bd53d43992b2a60e42"

	// DefaultMultipoolerImage is the default container image for the Multipooler component.
	DefaultMultipoolerImage = "945488516053.dkr.ecr.us-east-1.amazonaws.com/multigres-pr@sha256:494ec969ab92ed9ca2062c31393e4afc021d9bcbb35396bd53d43992b2a60e42"

	// DefaultMultigatewayImage is the default container image for the Multigateway component.
	DefaultMultigatewayImage = "945488516053.dkr.ecr.us-east-1.amazonaws.com/multigres-pr@sha256:494ec969ab92ed9ca2062c31393e4afc021d9bcbb35396bd53d43992b2a60e42"

	// DefaultPostgresExporterImage is the default container image for postgres_exporter sidecars.
	DefaultPostgresExporterImage = "quay.io/prometheuscommunity/postgres-exporter:v0.20.1"
)
