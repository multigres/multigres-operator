package suite

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ctrltest"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// fanoutCluster builds a MultigresCluster with one cell and one database,
// table group and shard: enough for the cluster controller to fan out to
// every child kind it owns (TopoServer, Cell, TableGroup) and for the
// TableGroup controller it creates to fan out to a Shard of its own.
func fanoutCluster(t *testing.T, ns, clusterName string) *multigresv1alpha1.MultigresCluster {
	t.Helper()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: adminSecretName, Namespace: ns},
		StringData: map[string]string{"password": "postgres"},
	}
	if err := Suite.Client.Create(t.Context(), secret); err != nil {
		t.Fatalf("create password secret: %v", err)
	}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: adminSecretName,
				Key:  "password",
			},
			PVCDeletionPolicy: &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
				WhenScaled:  multigresv1alpha1.DeletePVCRetentionPolicy,
			},
			Cells: []multigresv1alpha1.CellConfig{
				{Name: defaultSimCell, ZoneID: "us-central1-a"},
			},
			Databases: []multigresv1alpha1.DatabaseConfig{
				{
					Name:    "postgres",
					Default: true,
					TableGroups: []multigresv1alpha1.TableGroupConfig{
						{
							Name:    "default",
							Default: true,
							Shards: []multigresv1alpha1.ShardConfig{{
								Name: "0-inf",
								Spec: &multigresv1alpha1.ShardInlineSpec{
									Multiorch: multigresv1alpha1.MultiorchSpec{
										StatelessSpec: multigresv1alpha1.StatelessSpec{
											Replicas: ptr.To(int32(1)),
										},
									},
									Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
										"primary": {
											ReplicasPerCell: ptr.To(int32(1)),
											Type:            "readWrite",
											Cells: []multigresv1alpha1.CellName{
												defaultSimCell,
											},
										},
									},
								},
							}},
						},
					},
				},
			},
		},
	}
	if err := Suite.Client.Create(t.Context(), cluster); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	return cluster
}

// findOp returns the index of the first op in ops matching kind and name.
// batch has already been validated as an exact multiset by WaitForAll, so a
// miss here would mean this helper's own matching is wrong, not that the op
// is absent.
func findOp(t *testing.T, ops []ctrltest.Op, kind, objName string) int {
	t.Helper()
	for i, op := range ops {
		if ctrltest.KindSuffix(op.Kind) == kind && op.Key.Name == objName {
			return i
		}
	}
	t.Fatalf("no %s %q op among %v", kind, objName, ops)
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
	ns := Suite.Namespace(t)
	const clusterName = "fanout"

	// Cursors are opened before the cluster exists, not after: five
	// reconcilers run concurrently (one goroutine each, per suite.go), so a
	// cursor opened even one line late can start its scan after another
	// controller's reaction to the same write has already landed, and then
	// miss the very op it was meant to catch.
	clusterCur := Suite.Ops.CursorForT(t, ns, "multigrescluster")
	tgCur := Suite.Ops.CursorForT(t, ns, "tablegroup")

	fanoutCluster(t, ns, clusterName)

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

	topoIdx := findOp(t, batch, "TopoServer", topoServerName)
	cellIdx := findOp(t, batch, "Cell", cellName)
	tgIdx := findOp(t, batch, "TableGroup", tableGroupName)
	if topoIdx >= cellIdx || cellIdx >= tgIdx {
		t.Fatalf(
			"expected TopoServer < Cell < TableGroup in the multigrescluster "+
				"controller's writes, got positions %d, %d, %d in %v",
			topoIdx, cellIdx, tgIdx, batch,
		)
	}

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
