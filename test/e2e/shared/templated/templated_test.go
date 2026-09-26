//go:build e2e

package templated_test

import (
	"context"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"

	"github.com/multigres/testkit/assert"
)

// TestTemplatedCluster applies the template CRs from config/samples/templates/
// and the templated cluster CR from config/samples/templated-cluster.yaml, then
// verifies the full resource tree, pod health, and psql connectivity.
func TestTemplatedCluster(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")
	ctx := context.Background()

	// Create templates first — the cluster CR references them.
	coreTmpl := framework.MustLoadCoreTemplate("config/samples/templates/core.yaml", ns)
	ck.NoError(c.Create(ctx, coreTmpl), "create CoreTemplate")
	cellTmpl := framework.MustLoadCellTemplate("config/samples/templates/cell.yaml", ns)
	ck.NoError(c.Create(ctx, cellTmpl), "create CellTemplate")
	shardTmpl := framework.MustLoadShardTemplate("config/samples/templates/shard.yaml", ns)
	ck.NoError(c.Create(ctx, shardTmpl), "create ShardTemplate")

	// Create the cluster referencing the templates.
	cr := framework.MustLoadCluster("config/samples/templated-cluster.yaml", ns)
	ck.NoError(c.Create(ctx, cr), "create MultigresCluster")

	// Verify child CRDs.
	framework.WaitForCRDCount(t, c, ns,
		&multigresv1alpha1.CellList{},
		func(l *multigresv1alpha1.CellList) int { return len(l.Items) },
		2, "2 Cells",
	)
	framework.WaitForCRDCount(t, c, ns,
		&multigresv1alpha1.TopoServerList{},
		func(l *multigresv1alpha1.TopoServerList) int { return len(l.Items) },
		1, "TopoServer",
	)
	framework.WaitForCRDCount(t, c, ns,
		&multigresv1alpha1.ShardList{},
		func(l *multigresv1alpha1.ShardList) int { return len(l.Items) },
		1, "Shard",
	)

	// Verify leaf resources.
	framework.WaitForStatefulSet(t, c, ns, "etcd")
	framework.WaitForDeployment(t, c, ns, "multiadmin")
	framework.WaitForDeployment(t, c, ns, "multiorch")
	framework.WaitForPod(t, c, ns, "postgres")

	// All pods ready.
	cluster.WaitForAllPodsReady(t, ns)

	// SELECT 1 through multigateway.
	gwSvc := framework.FindGatewayService(t, cluster, ns)
	framework.WaitForQueryServing(t, cluster, ns, gwSvc)
}
