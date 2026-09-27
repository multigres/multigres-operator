package suite

import multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

// The API types this package builds most, without the package qualifier.
//
// multigresv1alpha1.MultigresCluster is thirty-three characters and says
// "multigres" twice, and a test body that constructs a dozen API objects
// spends more width on the qualifier than on what it is asserting.
//
// Two rules keep this from becoming a dot-import in disguise, which would
// trade that width for provenance nobody can recover:
//
//   - Only types, never values or functions. multigresv1alpha1.PhaseHealthy
//     and multigresv1alpha1.DeletePVCRetentionPolicy stay qualified, because
//     a bare PhaseHealthy in an assertion genuinely does read as though it
//     could be this package's own. A composite literal names its type on the
//     line above, so &Shard{} does not have the same problem.
//   - Only types used three or more times here. Aliasing a type used once
//     saves eighteen characters and costs the next reader a lookup.
//
// Deliberately the same names as upstream, so there is nothing to learn and
// nothing to bikeshed: this drops the qualifier and changes nothing else.
// MultigresCluster in particular keeps its full name; a bare Cluster is far
// too overloaded in a workspace where that word also means a Kubernetes
// cluster and an EKS cluster.
//
// Orthogonal to any future rename of the multigresv1alpha1 alias itself,
// which is a repo-wide question across 217 files. These read the same either
// way.
type (
	MultigresCluster          = multigresv1alpha1.MultigresCluster
	Shard                     = multigresv1alpha1.Shard
	PoolSpec                  = multigresv1alpha1.PoolSpec
	ShardList                 = multigresv1alpha1.ShardList
	PVCDeletionPolicy         = multigresv1alpha1.PVCDeletionPolicy
	CellConfig                = multigresv1alpha1.CellConfig
	MultigresClusterSpec      = multigresv1alpha1.MultigresClusterSpec
	PoolName                  = multigresv1alpha1.PoolName
	PostgresPasswordSecretRef = multigresv1alpha1.PostgresPasswordSecretRef
	TableGroupList            = multigresv1alpha1.TableGroupList
	CellName                  = multigresv1alpha1.CellName
	DatabaseConfig            = multigresv1alpha1.DatabaseConfig
	TableGroupConfig          = multigresv1alpha1.TableGroupConfig
	ShardConfig               = multigresv1alpha1.ShardConfig
	ShardInlineSpec           = multigresv1alpha1.ShardInlineSpec
	TopoServerList            = multigresv1alpha1.TopoServerList
)
