//go:build e2e

package webhook_test

import (
	"context"
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

// TestWebhookRejections verifies that the admission rules correctly reject
// invalid mutations.
//
// E2E tests run with --webhook-enable=false (no programmatic webhook).
// Only CEL XValidation rules embedded in the CRDs are enforced by the
// kube-apiserver. Tests requiring the programmatic Go webhook (storage
// shrink, template existence, pool name regex) belong in integration tests.
func TestWebhookRejections(t *testing.T) {
	t.Run("RemoveCell", testRemoveCell)
	t.Run("RemovePool", testRemovePool)
}

func testRemoveCell(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	// Create cluster with 2 cells so we can try removing one.
	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	cr.Spec.Cells = append(cr.Spec.Cells, multigresv1alpha1.CellConfig{
		Name:   "zone-b",
		Region: "us-central1",
	})
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Get live CR and remove the second cell.
	live := framework.GetCluster(t, c, ns, cr.Name)
	live.Spec.Cells = live.Spec.Cells[:1]

	framework.AssertWebhookRejects(t, c, live, "cannot be removed")
}

func testRemovePool(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	// Create cluster with 2 pools.
	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	shard := &cr.Spec.Databases[0].TableGroups[0].Shards[0]
	shard.Spec.Pools["extra"] = multigresv1alpha1.PoolSpec{
		Type:            "readWrite",
		Cells:           []multigresv1alpha1.CellName{"zone-a"},
		ReplicasPerCell: int32Ptr(1),
		Storage: multigresv1alpha1.StorageSpec{
			Size: "1Gi",
		},
	}
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Get live CR and remove the extra pool.
	live := framework.GetCluster(t, c, ns, cr.Name)
	delete(live.Spec.Databases[0].TableGroups[0].Shards[0].Spec.Pools, "extra")

	framework.AssertWebhookRejects(t, c, live, "cannot be removed")
}

func int32Ptr(i int32) *int32 { return &i }
