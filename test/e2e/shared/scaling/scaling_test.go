//go:build e2e

package scaling_test

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/test/e2e/framework"
)

// TestStatelessScaling verifies that scaling stateless components (multiadmin,
// multigateway, etcd) works correctly.
func TestStatelessScaling(t *testing.T) {
	t.Run("ScaleMultiadmin", testScaleMultiadmin)
	t.Run("ScaleMultigateway", testScaleMultigateway)
	t.Run("LargeScaleMultiadmin", testLargeScaleMultiadmin)
	t.Run("LargeScaleMultigateway", testLargeScaleMultigateway)
}

func testScaleMultiadmin(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Verify initial: 1 multiadmin replica.
	framework.WaitForDeploymentReplicas(t, c, ns, "multiadmin", 1)

	// Scale up to 2.
	framework.PatchCluster(t, c, cr, []byte(`{
		"spec": {
			"multiadmin": {
				"spec": {
					"replicas": 2
				}
			}
		}
	}`))
	framework.WaitForDeploymentReplicas(t, c, ns, "multiadmin", 2)
	cluster.WaitForAllPodsReady(t, ns)
}

func testScaleMultigateway(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Verify initial: 1 multigateway replica.
	framework.WaitForDeploymentReplicas(t, c, ns, "multigateway", 1)

	// Scale up to 2.
	framework.PatchCluster(t, c, cr, []byte(`{
		"spec": {
			"cells": [{
				"name": "zone-a",
				"spec": {
					"multigateway": {
						"replicas": 2
					}
				}
			}]
		}
	}`))
	framework.WaitForDeploymentReplicas(t, c, ns, "multigateway", 2)
	cluster.WaitForAllPodsReady(t, ns)

	// Suppress unused import warning.
	_ = client.MatchingLabels{}
}

func testLargeScaleMultiadmin(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Scale 1 → 5.
	framework.PatchCluster(t, c, cr, []byte(`{
		"spec": {"multiadmin": {"spec": {"replicas": 5}}}
	}`))
	framework.WaitForDeploymentReplicas(t, c, ns, "multiadmin", 5)
	cluster.WaitForAllPodsReady(t, ns)

	// Scale 5 → 1.
	framework.PatchCluster(t, c, cr, []byte(`{
		"spec": {"multiadmin": {"spec": {"replicas": 1}}}
	}`))
	framework.WaitForDeploymentReplicas(t, c, ns, "multiadmin", 1)
	cluster.WaitForAllPodsReady(t, ns)
}

func testLargeScaleMultigateway(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/base.yaml", ns)
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Scale 1 → 5.
	framework.PatchCluster(t, c, cr, []byte(`{
		"spec": {"cells": [{"name": "zone-a", "spec": {"multigateway": {"replicas": 5}}}]}
	}`))
	framework.WaitForDeploymentReplicas(t, c, ns, "multigateway", 5)
	cluster.WaitForAllPodsReady(t, ns)

	// Scale 5 → 1.
	framework.PatchCluster(t, c, cr, []byte(`{
		"spec": {"cells": [{"name": "zone-a", "spec": {"multigateway": {"replicas": 1}}}]}
	}`))
	framework.WaitForDeploymentReplicas(t, c, ns, "multigateway", 1)
	cluster.WaitForAllPodsReady(t, ns)
}
