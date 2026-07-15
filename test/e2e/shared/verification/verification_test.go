//go:build e2e

package verification_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

// TestResourceVerification verifies that the operator creates the expected
// Kubernetes resources (PDBs, deployments, services) with correct configuration.
func TestResourceVerification(t *testing.T) {
	t.Run("PDB", testPDB)
	t.Run("MultiadminWeb", testMultiadminWeb)
	t.Run("LogLevels", testLogLevels)
}

func testPDB(t *testing.T) {
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

	// Verify at least 1 PDB exists.
	pdbs := framework.ListPDBs(t, c, ns)
	if len(pdbs) == 0 {
		t.Fatal("expected at least 1 PDB, got 0")
	}

	// Verify PDB has a selector and maxUnavailable.
	for _, pdb := range pdbs {
		if pdb.Spec.Selector == nil || len(pdb.Spec.Selector.MatchLabels) == 0 {
			t.Errorf("PDB %s has no selector", pdb.Name)
		}
		if pdb.Spec.MaxUnavailable == nil {
			t.Errorf("PDB %s has no maxUnavailable", pdb.Name)
		}
	}
}

func testMultiadminWeb(t *testing.T) {
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

	// Verify multiadminweb deployment exists (container name has a hyphen).
	dep := framework.WaitForDeployment(t, c, ns, "multiadmin-web")
	if dep.Status.ReadyReplicas < 1 {
		t.Errorf("multiadmin-web has %d ready replicas, want >= 1", dep.Status.ReadyReplicas)
	}

	// Verify multiadminweb service exists.
	framework.WaitForService(t, c, ns, "http", 18100)
}

func testLogLevels(t *testing.T) {
	t.Parallel()
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}

	cr := framework.MustLoadCluster("test/e2e/fixtures/log-levels.yaml", ns)
	if err := c.Create(context.Background(), cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}
	cluster.WaitForAllPodsReady(t, ns)

	// Check that pods have the expected --log-level settings.
	ctx := context.Background()
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(ns)); err != nil {
		t.Fatalf("list pods: %v", err)
	}

	expectedLevels := map[string]string{
		"multipooler":  "warn",
		"multiorch":    "debug",
		"multiadmin":   "warn",
		"multigateway": "debug",
	}

	checkArgs := func(containerName string, args []string, podName string) {
		expectedLevel, ok := expectedLevels[containerName]
		if !ok {
			return
		}
		// Check both formats: "--log-level=value" (single arg) and
		// "--log-level" "value" (two separate args).
		for i, arg := range args {
			if arg == "--log-level="+expectedLevel {
				return
			}
			if arg == "--log-level" && i+1 < len(args) && args[i+1] == expectedLevel {
				return
			}
		}
		t.Errorf("container %s in pod %s: expected --log-level %s in args %v",
			containerName, podName, expectedLevel, args)
	}

	for _, pod := range pods.Items {
		for _, cont := range pod.Spec.Containers {
			checkArgs(cont.Name, cont.Args, pod.Name)
		}
		for _, cont := range pod.Spec.InitContainers {
			checkArgs(cont.Name, cont.Args, pod.Name)
		}
	}

	// Suppress unused variable warning.
	_ = multigresv1alpha1.MultigresCluster{}
}
