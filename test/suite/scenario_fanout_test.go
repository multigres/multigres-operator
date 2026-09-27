package suite

import (
	"testing"
	"time"

	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/name"
	"github.com/multigres/testkit/ctrltest"
)

// fanoutCluster builds a MultigresCluster with one cell and one database,
// table group and shard: enough for the cluster controller to fan out to
// every child kind it owns (TopoServer, Cell, TableGroup) and for the
// TableGroup controller it creates to fan out to a Shard of its own.
func (c *C) fanoutCluster(clusterName string) *MultigresCluster {
	c.Helper()
	return c.newCluster(clusterName, func(s *MultigresClusterSpec) {
		s.Databases = []DatabaseConfig{
			{
				Name:    "postgres",
				Default: true,
				TableGroups: []TableGroupConfig{
					{
						Name:    "default",
						Default: true,
						Shards: []ShardConfig{{
							Name: "0-inf",
							Spec: &ShardInlineSpec{
								Multiorch: multigresv1alpha1.MultiorchSpec{
									StatelessSpec: multigresv1alpha1.StatelessSpec{
										Replicas: ptr.To(int32(1)),
									},
								},
								Pools: map[PoolName]PoolSpec{
									"primary": {
										ReplicasPerCell: ptr.To(int32(1)),
										Type:            "readWrite",
										Cells: []CellName{
											defaultSimCell,
										},
									},
								},
							},
						}},
					},
				},
			},
		}
	})
}

// findOp returns the index of the first op in ops matching kind and name.
// batch has already been validated as an exact multiset by WaitForAll, so a
// miss here would mean this helper's own matching is wrong, not that the op
// is absent.
func (c *C) findOp(ops []ctrltest.Op, kind, objName string) int {
	c.Helper()
	for i, op := range ops {
		if ctrltest.KindSuffix(op.Kind) == kind && op.Key.Name == objName {
			return i
		}
	}
	c.Fatalf("no %s %q op among %v", kind, objName, ops)
	return -1
}

// TestClusterFanOutSequence asserts that creating a MultigresCluster fans out
// to TopoServer, Cell and TableGroup attributed to the multigrescluster
// controller, in that relative order, and that the second-order fan-out to
// Shard is attributed to the tablegroup controller rather than to
// multigrescluster.
//
// Ordering choice: multigrescluster_controller.go calls
// reconcileGlobalComponents, then reconcileCells, then (after
// reconcileTopology, which makes no Kubernetes writes here) reconcileDatabases,
// as sequential statements inside one Reconcile call. That is a genuine,
// code-level guarantee, so TopoServer < Cell < TableGroup is asserted below.
// Nothing is asserted about the relative order of the Multiadmin/MultiadminWeb
// writes reconcileGlobalComponents also makes: they are unconditional (there
// is no way to disable them from the spec) and sit between the TopoServer and
// Cell writes, but their order relative to each other, or to TopoServer and
// Cell, is not what this test is about and is left unspecified.
//
// Matcher choice: WaitForNext cannot express this, because those
// Multiadmin/MultiadminWeb writes are real, deterministic, and land between
// TopoServer and Cell, so a strict next-op chain from TopoServer would hit a
// Deployment patch instead of Cell. WaitForAll is the right tool instead: one
// call consumes the whole first pass as a multiset, which (a) still proves
// each of TopoServer/Cell/TableGroup was written by multigrescluster and
// nothing else was (in particular, that multigrescluster never itself writes
// a Shard), and (b) hands back the ops in recorded order, which is what the
// ordering check below reads. WaitForMatching was avoided entirely: it would
// let the assertion silently skip past a misordered write instead of failing
// on it, which is exactly what an ordering test must not do.
func TestClusterFanOutSequence(t *testing.T) {
	c := newCase(t)
	const clusterName = "fanout"

	// Cursors are opened before the cluster exists, not after: five
	// reconcilers run concurrently (one goroutine each, per suite.go), so a
	// cursor opened even one line late can start its scan after another
	// controller's reaction to the same write has already landed, and then
	// miss the very op it was meant to catch.
	clusterCur := c.Cursor("multigrescluster")
	tgCur := c.Cursor("tablegroup")

	c.fanoutCluster(clusterName)

	topoServerName := clusterName + "-global-topo"
	cellName := name.JoinWithConstraints(name.DefaultConstraints, clusterName, defaultSimCell)
	tableGroupName := name.JoinWithConstraints(
		name.DefaultConstraints, clusterName, "postgres", "default",
	)
	shardName := name.JoinWithConstraints(
		name.DefaultConstraints, clusterName, "postgres", "default", "0-inf",
	)

	batch := clusterCur.WaitForAll(t, []ctrltest.Expect{
		// ensureClusterFinalizer, then resolveImages recording the default
		// image set: both patches of the cluster object itself, before any
		// child is touched.
		ctrltest.ExpectPatch("MultigresCluster", clusterName),
		ctrltest.ExpectPatch("MultigresCluster", clusterName),
		ctrltest.ExpectPatch("TopoServer", topoServerName),
		ctrltest.ExpectPatch("Deployment", clusterName+"-multiadmin"),
		ctrltest.ExpectPatch("Service", clusterName+"-multiadmin"),
		ctrltest.ExpectPatch("Deployment", clusterName+"-multiadmin-web"),
		ctrltest.ExpectPatch("Service", clusterName+"-multiadmin-web"),
		ctrltest.ExpectPatch("Service", clusterName+"-multigateway"),
		ctrltest.ExpectPatch("Service", clusterName+"-multigateway-replica"),
		ctrltest.ExpectPatch("Cell", cellName),
		ctrltest.ExpectPatch("TableGroup", tableGroupName),
		ctrltest.ExpectStatusPatch("MultigresCluster", clusterName),
	}, 30*time.Second)

	topoIdx := c.findOp(batch, "TopoServer", topoServerName)
	cellIdx := c.findOp(batch, "Cell", cellName)
	tgIdx := c.findOp(batch, "TableGroup", tableGroupName)
	c.True(topoIdx < cellIdx,
		"expected TopoServer before Cell in the multigrescluster controller's "+
			"writes, got positions %d, %d in %v", topoIdx, cellIdx, batch)
	c.True(cellIdx < tgIdx,
		"expected Cell before TableGroup in the multigrescluster controller's "+
			"writes, got positions %d, %d in %v", cellIdx, tgIdx, batch)

	// Second-order fan-out: the TableGroup controller, not the cluster
	// controller, creates the Shard. Applying the desired Shard is the first
	// write TableGroupReconciler makes (stepListChildShards only reads), so
	// WaitForNext is sound here without any of the batching above. The
	// attribution claim itself comes from the cursor's scope, not from a
	// separate check: tgCur can only ever return an op whose Controller is
	// "tablegroup" (see Recorder.firstMatchFrom), and the WaitForAll batch
	// above already proved multigrescluster wrote no Shard of its own, since
	// one would have shown up there as an unexpected op.
	tgCur.WaitForNext(t, "patch", "Shard", shardName, 30*time.Second)
}
