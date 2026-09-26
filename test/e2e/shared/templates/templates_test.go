//go:build e2e

package templates_test

import (
	"context"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/testkit/assert"
)

// TestTemplatePropagation verifies that template values propagate correctly
// to child resources and that inline overrides take precedence.
func TestTemplatePropagation(t *testing.T) {
	t.Run("VerifyPropagation", testVerifyPropagation)
	t.Run("PartialOverride", testPartialOverride)
	t.Run("PVCDeletionPolicyInheritance", testPVCDeletionPolicyInheritance)
}

func testVerifyPropagation(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.Require().NoError(err, "create CR client")
	ctx := context.Background()

	// Create templates first.
	coreTmpl := framework.MustLoadCoreTemplate("test/e2e/fixtures/templates/core.yaml", ns)
	ck.Require().NoError(c.Create(ctx, coreTmpl), "create CoreTemplate")
	cellTmpl := framework.MustLoadCellTemplate("test/e2e/fixtures/templates/cell.yaml", ns)
	ck.Require().NoError(c.Create(ctx, cellTmpl), "create CellTemplate")
	shardTmpl := framework.MustLoadShardTemplate("test/e2e/fixtures/templates/shard.yaml", ns)
	ck.Require().NoError(c.Create(ctx, shardTmpl), "create ShardTemplate")

	// Create cluster referencing the templates.
	cr := framework.MustLoadCluster("test/e2e/fixtures/templated.yaml", ns)
	ck.Require().NoError(c.Create(ctx, cr), "create MultigresCluster")

	// Wait for Shard CRD to be created.
	framework.WaitForCRDCount(t, c, ns,
		&multigresv1alpha1.ShardList{},
		func(l *multigresv1alpha1.ShardList) int { return len(l.Items) },
		1, "Shard",
	)

	// Verify Shard inherited values from templates.
	shards := &multigresv1alpha1.ShardList{}
	ck.Require().NoError(c.List(ctx, shards, client.InNamespace(ns)), "list Shards")
	shard := shards.Items[0]
	ck.Require().Len(shard.Spec.Pools, len(shardTmpl.Spec.Pools))

	// Pool storage from ShardTemplate should be 1Gi.
	for poolName, pool := range shard.Spec.Pools {
		ck.Require().
			EqDeep(shardTmpl.Spec.Pools[poolName].ReplicasPerCell, pool.ReplicasPerCell, "pool %s must inherit the template's failure-safe replica count", poolName)
		ck.Eq(
			"1Gi",
			pool.Storage.Size,
			"pool %s storage = %s, want 1Gi (from ShardTemplate)",
			poolName,
			pool.Storage.Size,
		)
	}

	// Wait for all pods to come up.
	cluster.WaitForAllPodsReady(t, ns)

	// Check actual resolution, not values that could also come from defaults.
	live := framework.GetCluster(t, c, ns, cr.Name)
	ck.Require().NotNil(live.Status.ResolvedTemplates)
	ck.Require().
		ElementsMatch([]multigresv1alpha1.TemplateRef{"e2e-core"}, live.Status.ResolvedTemplates.CoreTemplates)
	ck.Require().
		ElementsMatch([]multigresv1alpha1.TemplateRef{"e2e-cell"}, live.Status.ResolvedTemplates.CellTemplates)
	ck.Require().
		ElementsMatch([]multigresv1alpha1.TemplateRef{"e2e-shard"}, live.Status.ResolvedTemplates.ShardTemplates)
}

func testPartialOverride(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")
	ctx := context.Background()

	// Create templates.
	coreTmpl := framework.MustLoadCoreTemplate("test/e2e/fixtures/templates/core.yaml", ns)
	ck.NoError(c.Create(ctx, coreTmpl), "create CoreTemplate")
	cellTmpl := framework.MustLoadCellTemplate("test/e2e/fixtures/templates/cell.yaml", ns)
	ck.NoError(c.Create(ctx, cellTmpl), "create CellTemplate")
	shardTmpl := framework.MustLoadShardTemplate("test/e2e/fixtures/templates/shard.yaml", ns)
	ck.NoError(c.Create(ctx, shardTmpl), "create ShardTemplate")

	// Create cluster with an inline override on multiadmin replicas.
	cr := framework.MustLoadCluster("test/e2e/fixtures/templated.yaml", ns)
	cr.Spec.Multiadmin = &multigresv1alpha1.MultiadminConfig{
		Spec: &multigresv1alpha1.StatelessSpec{
			Replicas: int32Ptr(2),
		},
	}
	framework.WithCIResources(&cr.Spec)
	ck.NoError(c.Create(ctx, cr), "create MultigresCluster")

	// Wait for multiadmin to have 2 replicas (override wins over template's 1).
	framework.WaitForDeploymentReplicas(t, c, ns, "multiadmin", 2)
	cluster.WaitForAllPodsReady(t, ns)
}

func testPVCDeletionPolicyInheritance(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.Require().NoError(err, "create CR client")
	ctx := context.Background()

	// Create templates — ShardTemplate has pvcDeletionPolicy: Delete/Delete.
	coreTmpl := framework.MustLoadCoreTemplate("test/e2e/fixtures/templates/core.yaml", ns)
	ck.Require().NoError(c.Create(ctx, coreTmpl), "create CoreTemplate")
	cellTmpl := framework.MustLoadCellTemplate("test/e2e/fixtures/templates/cell.yaml", ns)
	ck.Require().NoError(c.Create(ctx, cellTmpl), "create CellTemplate")
	shardTmpl := framework.MustLoadShardTemplate("test/e2e/fixtures/templates/shard.yaml", ns)
	ck.Require().NoError(c.Create(ctx, shardTmpl), "create ShardTemplate")

	// Create cluster referencing templates.
	cr := framework.MustLoadCluster("test/e2e/fixtures/templated.yaml", ns)
	ck.Require().NoError(c.Create(ctx, cr), "create MultigresCluster")

	// Wait for Shard to be created.
	framework.WaitForCRDCount(t, c, ns,
		&multigresv1alpha1.ShardList{},
		func(l *multigresv1alpha1.ShardList) int { return len(l.Items) },
		1, "Shard",
	)

	// Verify Shard inherited the PVC deletion policy from ShardTemplate.
	shards := &multigresv1alpha1.ShardList{}
	ck.Require().NoError(c.List(ctx, shards, client.InNamespace(ns)), "list Shards")
	shard := shards.Items[0]
	ck.Require().
		NotNil(shard.Spec.PVCDeletionPolicy, "Shard PVCDeletionPolicy is nil, expected inheritance from ShardTemplate")
	ck.Eq("Delete", shard.Spec.PVCDeletionPolicy.WhenDeleted, "PVCDeletionPolicy.WhenDeleted")
	ck.Eq("Delete", shard.Spec.PVCDeletionPolicy.WhenScaled, "PVCDeletionPolicy.WhenScaled")
}

func int32Ptr(i int32) *int32 { return &i }
