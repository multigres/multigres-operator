//go:build e2e

package templates_test

import (
	"context"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}
	ctx := context.Background()

	// Create templates first.
	coreTmpl := framework.MustLoadCoreTemplate("test/e2e/fixtures/templates/core.yaml", ns)
	if err := c.Create(ctx, coreTmpl); err != nil {
		t.Fatalf("create CoreTemplate: %v", err)
	}
	cellTmpl := framework.MustLoadCellTemplate("test/e2e/fixtures/templates/cell.yaml", ns)
	if err := c.Create(ctx, cellTmpl); err != nil {
		t.Fatalf("create CellTemplate: %v", err)
	}
	shardTmpl := framework.MustLoadShardTemplate("test/e2e/fixtures/templates/shard.yaml", ns)
	if err := c.Create(ctx, shardTmpl); err != nil {
		t.Fatalf("create ShardTemplate: %v", err)
	}

	// Create cluster referencing the templates.
	cr := framework.MustLoadCluster("test/e2e/fixtures/templated.yaml", ns)
	if err := c.Create(ctx, cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}

	// Wait for Shard CRD to be created.
	framework.WaitForCRDCount(t, c, ns,
		&multigresv1alpha1.ShardList{},
		func(l *multigresv1alpha1.ShardList) int { return len(l.Items) },
		1, "Shard",
	)

	// Verify Shard inherited values from templates.
	shards := &multigresv1alpha1.ShardList{}
	if err := c.List(ctx, shards, client.InNamespace(ns)); err != nil {
		t.Fatalf("list Shards: %v", err)
	}
	shard := shards.Items[0]

	// Pool storage from ShardTemplate should be 1Gi.
	for poolName, pool := range shard.Spec.Pools {
		if pool.Storage.Size != "1Gi" {
			t.Errorf("pool %s storage = %s, want 1Gi (from ShardTemplate)", poolName, pool.Storage.Size)
		}
	}

	// Wait for all pods to come up.
	cluster.WaitForAllPodsReady(t, ns)
}

func testPartialOverride(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}
	ctx := context.Background()

	// Create templates.
	coreTmpl := framework.MustLoadCoreTemplate("test/e2e/fixtures/templates/core.yaml", ns)
	if err := c.Create(ctx, coreTmpl); err != nil {
		t.Fatalf("create CoreTemplate: %v", err)
	}
	cellTmpl := framework.MustLoadCellTemplate("test/e2e/fixtures/templates/cell.yaml", ns)
	if err := c.Create(ctx, cellTmpl); err != nil {
		t.Fatalf("create CellTemplate: %v", err)
	}
	shardTmpl := framework.MustLoadShardTemplate("test/e2e/fixtures/templates/shard.yaml", ns)
	if err := c.Create(ctx, shardTmpl); err != nil {
		t.Fatalf("create ShardTemplate: %v", err)
	}

	// Create cluster with an inline override on multiadmin replicas.
	cr := framework.MustLoadCluster("test/e2e/fixtures/templated.yaml", ns)
	cr.Spec.Multiadmin = &multigresv1alpha1.MultiadminConfig{
		Spec: &multigresv1alpha1.StatelessSpec{
			Replicas: int32Ptr(2),
		},
	}
	framework.WithCIResources(&cr.Spec)
	if err := c.Create(ctx, cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}

	// Wait for multiadmin to have 2 replicas (override wins over template's 1).
	framework.WaitForDeploymentReplicas(t, c, ns, "multiadmin", 2)
	cluster.WaitForAllPodsReady(t, ns)
}

func testPVCDeletionPolicyInheritance(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}
	ctx := context.Background()

	// Create templates — ShardTemplate has pvcDeletionPolicy: Delete/Delete.
	coreTmpl := framework.MustLoadCoreTemplate("test/e2e/fixtures/templates/core.yaml", ns)
	if err := c.Create(ctx, coreTmpl); err != nil {
		t.Fatalf("create CoreTemplate: %v", err)
	}
	cellTmpl := framework.MustLoadCellTemplate("test/e2e/fixtures/templates/cell.yaml", ns)
	if err := c.Create(ctx, cellTmpl); err != nil {
		t.Fatalf("create CellTemplate: %v", err)
	}
	shardTmpl := framework.MustLoadShardTemplate("test/e2e/fixtures/templates/shard.yaml", ns)
	if err := c.Create(ctx, shardTmpl); err != nil {
		t.Fatalf("create ShardTemplate: %v", err)
	}

	// Create cluster referencing templates.
	cr := framework.MustLoadCluster("test/e2e/fixtures/templated.yaml", ns)
	if err := c.Create(ctx, cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}

	// Wait for Shard to be created.
	framework.WaitForCRDCount(t, c, ns,
		&multigresv1alpha1.ShardList{},
		func(l *multigresv1alpha1.ShardList) int { return len(l.Items) },
		1, "Shard",
	)

	// Verify Shard inherited the PVC deletion policy from ShardTemplate.
	shards := &multigresv1alpha1.ShardList{}
	if err := c.List(ctx, shards, client.InNamespace(ns)); err != nil {
		t.Fatalf("list Shards: %v", err)
	}
	shard := shards.Items[0]
	if shard.Spec.PVCDeletionPolicy == nil {
		t.Fatal("Shard PVCDeletionPolicy is nil, expected inheritance from ShardTemplate")
	}
	if shard.Spec.PVCDeletionPolicy.WhenDeleted != "Delete" {
		t.Errorf("PVCDeletionPolicy.WhenDeleted = %s, want Delete", shard.Spec.PVCDeletionPolicy.WhenDeleted)
	}
	if shard.Spec.PVCDeletionPolicy.WhenScaled != "Delete" {
		t.Errorf("PVCDeletionPolicy.WhenScaled = %s, want Delete", shard.Spec.PVCDeletionPolicy.WhenScaled)
	}
}

func int32Ptr(i int32) *int32 { return &i }
