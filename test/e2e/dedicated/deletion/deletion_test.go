//go:build e2e

package deletion_test

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"

	"github.com/multigres/testkit/assert"
)

// TestClusterDeletion verifies that deleting a MultigresCluster triggers
// cascading deletion of all child resources (CRDs and Kubernetes resources).
func TestClusterDeletion(t *testing.T) {
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")
	ctx := context.Background()

	// Load and create the minimal sample.
	cr := framework.MustLoadCluster("config/samples/minimal.yaml", ns)
	cr.Name = "delete-me" // distinct name for clarity in logs

	cr.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	ck.NoError(c.Create(ctx, cr), "create MultigresCluster")

	// Wait for full provisioning.
	cluster.WaitForAllPodsReady(t, ns)
	t.Log("cluster fully provisioned, initiating deletion...")

	// Delete the cluster.
	clusterKey := client.ObjectKeyFromObject(cr)
	ck.NoError(c.Delete(ctx, cr), "delete MultigresCluster")

	// Wait for the MultigresCluster object to disappear.
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	err = wait.PollUntilContextCancel(
		pollCtx,
		3*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			err := c.Get(ctx, clusterKey, &multigresv1alpha1.MultigresCluster{})
			return apierrors.IsNotFound(err), nil
		},
	)
	ck.NoError(err, "MultigresCluster not deleted")

	// Verify child CRDs are cleaned up.
	framework.WaitForEmpty(t, c, ns,
		&multigresv1alpha1.CellList{},
		func(l *multigresv1alpha1.CellList) int { return len(l.Items) },
		"Cells", 3*time.Minute,
	)
	framework.WaitForEmpty(t, c, ns,
		&multigresv1alpha1.TopoServerList{},
		func(l *multigresv1alpha1.TopoServerList) int { return len(l.Items) },
		"TopoServers", 3*time.Minute,
	)

	// Verify Kubernetes resources are cleaned up.
	framework.WaitForEmpty(t, c, ns,
		&appsv1.DeploymentList{},
		func(l *appsv1.DeploymentList) int { return len(l.Items) },
		"Deployments", 3*time.Minute,
	)
	framework.WaitForEmpty(t, c, ns,
		&appsv1.StatefulSetList{},
		func(l *appsv1.StatefulSetList) int { return len(l.Items) },
		"StatefulSets", 3*time.Minute,
	)
	framework.WaitForEmpty(t, c, ns,
		&corev1.ServiceList{},
		func(l *corev1.ServiceList) int { return len(l.Items) },
		"Services", 3*time.Minute,
	)

	// Verify topo (etcd) PVCs are cleaned up. The MultigresCluster cleanup
	// finalizer deletes topo PVCs regardless of the configured PVCDeletionPolicy
	// (which this test forces to Retain above), so
	// PVCs from the global topo StatefulSet should not survive the cluster.
	framework.WaitForEmpty(t, c, ns,
		&corev1.PersistentVolumeClaimList{},
		func(l *corev1.PersistentVolumeClaimList) int {
			count := 0
			for _, pvc := range l.Items {
				if strings.HasPrefix(pvc.Name, "data-"+cr.Name+"-") &&
					strings.Contains(pvc.Name, "-topo-") {
					count++
				}
			}
			return count
		},
		"topo PVCs", 3*time.Minute,
	)

	// Verify data (non-topo) PVCs are retained under the Retain policy forced
	// above, only topo PVCs are force-deleted by cluster cleanup.
	pvcList := &corev1.PersistentVolumeClaimList{}
	ck.NoError(c.List(ctx, pvcList, client.InNamespace(ns)), "list PVCs")
	nonTopoCount := 0
	for _, pvc := range pvcList.Items {
		if !strings.Contains(pvc.Name, "-topo-") {
			nonTopoCount++
		}
	}
	ck.NotEq(
		0,
		nonTopoCount,
		"expected at least one non-topo (data) PVC to survive cluster deletion under Retain, found none",
	)
}
