package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// A bound tablegroup is a dynamic tablegroup whose one shard the sharding
// catalog binds to an external PostgreSQL (CREATE TABLEGROUP ... CONNECTION). It is provisioned
// like any dynamic tablegroup, except that its one Shard runs a single pooler
// pod in front of that database and nothing else: no PostgreSQL, volumes,
// backups or orchestrator.
const (
	// AnnotationBackingConnection is set on the TableGroup and Shard of a bound
	// tablegroup and holds the name of the shard's connection in the sharding
	// catalog.
	AnnotationBackingConnection = "multigres.com/backing-connection"

	// BoundShardName is the name of the only shard of a bound tablegroup, which
	// the sharding catalog enforces: the external database is not split.
	BoundShardName = "0-inf"
)

// BackingConnectionOf returns the connection a dynamic TableGroup or Shard is
// bound to, or "" for one that is not bound.
func BackingConnectionOf(obj metav1.Object) string {
	return obj.GetAnnotations()[AnnotationBackingConnection]
}
