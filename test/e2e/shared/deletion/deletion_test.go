//go:build e2e

package deletion_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/test/e2e/framework"

	"github.com/multigres/testkit/assert"
)

// TestClusterDeletion verifies that deleting a MultigresCluster triggers
// cascading deletion of all child resources (CRDs and Kubernetes resources).
func TestClusterDeletion(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")
	ctx := context.Background()

	// Load and create the minimal sample.
	cr := framework.MustLoadCluster("config/samples/minimal.yaml", ns)
	cr.Name = "delete-me" // distinct name for clarity in logs
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
}

// TestClusterDeletionAfterSwitchingToExternalTopo verifies that retained PVCs
// from a previously managed topology server are still cleaned up when the
// cluster is later deleted. This covers the case where the TopoServer and its
// StatefulSet no longer exist by the time cluster deletion starts.
func TestClusterDeletionAfterSwitchingToExternalTopo(t *testing.T) {
	t.Parallel()
	ck := assert.NewCollecting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.Require().NoError(err, "create CR client")
	ctx := context.Background()

	cr := framework.MustLoadCluster("config/samples/minimal.yaml", ns)
	cr.Name = "delete-external-topo"
	cr.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
		WhenScaled:  multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	ck.Require().NoError(c.Create(ctx, cr), "create MultigresCluster")

	cluster.WaitForAllPodsReady(t, ns)
	topoStatefulSet := framework.WaitForStatefulSet(t, c, ns, "etcd")

	topoPVCNames := waitForBoundTopoPVCs(t, c, ns, cr.Name)
	nonTopoPVCNames := listNonTopoPVCNames(t, c, ns, topoPVCNames)
	ck.Require().
		NotEmpty(nonTopoPVCNames, "expected at least one non-topo PVC to verify the Retain policy")

	// Patch directly instead of using framework.PatchCluster (the external
	// endpoint is intentionally unreachable).
	ck.Require().NoError(c.Patch(ctx, cr, client.RawPatch(types.MergePatchType, []byte(`{
		"spec": {
			"globalTopoServer": {
				"etcd": null,
				"templateRef": null,
				"external": {
					"endpoints": ["http://external-etcd.invalid:2379"]
				}
			}
		}
	}`))), "switch cluster to external topology")

	waitForObjectsDeleted(t, c, 3*time.Minute,
		objectToDelete{
			key:         client.ObjectKey{Namespace: ns, Name: topoStatefulSet.Name},
			object:      &appsv1.StatefulSet{},
			description: "managed topology StatefulSet",
		},
		objectToDelete{
			key:         client.ObjectKey{Namespace: ns, Name: topoStatefulSet.Name},
			object:      &multigresv1alpha1.TopoServer{},
			description: "managed TopoServer",
		},
	)

	// Retain must leave the old managed-topology PVCs behind during the switch.
	for _, name := range topoPVCNames {
		pvc := &corev1.PersistentVolumeClaim{}
		ck.Require().
			NoError(c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, pvc), "expected retained topo PVC %q after switching to external topo", name)
		ck.Require().
			True(pvc.DeletionTimestamp.IsZero(), "topo PVC %q is unexpectedly terminating after switching to external topo", name)
	}

	liveCluster := &multigresv1alpha1.MultigresCluster{}
	clusterKey := client.ObjectKeyFromObject(cr)
	ck.Require().
		NoError(c.Get(ctx, clusterKey, liveCluster), "get MultigresCluster before deletion")
	ck.Require().NoError(c.Delete(ctx, liveCluster), "delete MultigresCluster")

	objects := []objectToDelete{{
		key:         clusterKey,
		object:      &multigresv1alpha1.MultigresCluster{},
		description: "MultigresCluster",
	}}
	for _, name := range topoPVCNames {
		objects = append(objects, objectToDelete{
			key:         client.ObjectKey{Namespace: ns, Name: name},
			object:      &corev1.PersistentVolumeClaim{},
			description: fmt.Sprintf("stale topo PVC %q", name),
		})
	}
	waitForObjectsDeleted(t, c, 5*time.Minute, objects...)
	framework.WaitForEmpty(t, c, ns,
		&appsv1.StatefulSetList{},
		func(l *appsv1.StatefulSetList) int { return len(l.Items) },
		"StatefulSets", 3*time.Minute,
	)

	// Cluster cleanup must remain scoped to topology PVCs.
	for _, name := range nonTopoPVCNames {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(
			ctx,
			client.ObjectKey{Namespace: ns, Name: name},
			pvc,
		); err != nil {
			t.Errorf("expected retained non-topo PVC %q after cluster deletion: %v", name, err)
			continue
		}
		ck.True(
			pvc.DeletionTimestamp.IsZero(),
			"retained non-topo PVC %q is unexpectedly terminating",
			name,
		)
	}
}

func waitForBoundTopoPVCs(
	t testing.TB,
	c client.Client,
	ns string,
	clusterName string,
) []string {
	t.Helper()
	var names []string
	pollCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	err := wait.PollUntilContextCancel(
		pollCtx,
		2*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			list := &corev1.PersistentVolumeClaimList{}
			if err := c.List(
				ctx,
				list,
				client.InNamespace(ns),
				client.MatchingLabels{
					metadata.LabelMultigresCluster: clusterName,
					metadata.LabelAppComponent:     metadata.ComponentTopoServer,
				},
			); err != nil {
				return false, nil
			}
			if len(list.Items) == 0 {
				return false, nil
			}
			currentNames := make([]string, 0, len(list.Items))
			for _, pvc := range list.Items {
				if pvc.Status.Phase != corev1.ClaimBound {
					return false, nil
				}
				currentNames = append(currentNames, pvc.Name)
			}
			names = currentNames
			return true, nil
		},
	)
	assert.NewAborting(t).NoError(err, "timed out waiting for bound managed-topology PVCs")
	return names
}

func listNonTopoPVCNames(
	t testing.TB,
	c client.Client,
	ns string,
	topoPVCNames []string,
) []string {
	t.Helper()
	topoPVCs := make(map[string]struct{}, len(topoPVCNames))
	for _, name := range topoPVCNames {
		topoPVCs[name] = struct{}{}
	}

	list := &corev1.PersistentVolumeClaimList{}
	assert.NewAborting(t).
		NoError(c.List(context.Background(), list, client.InNamespace(ns)), "list PVCs")
	var names []string
	for _, pvc := range list.Items {
		if _, isTopo := topoPVCs[pvc.Name]; !isTopo {
			names = append(names, pvc.Name)
		}
	}
	return names
}

type objectToDelete struct {
	key         client.ObjectKey
	object      client.Object
	description string
}

func waitForObjectsDeleted(
	t testing.TB,
	c client.Client,
	timeout time.Duration,
	objects ...objectToDelete,
) {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := wait.PollUntilContextCancel(
		pollCtx,
		2*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			for _, object := range objects {
				if err := c.Get(
					ctx,
					object.key,
					object.object.DeepCopyObject().(client.Object),
				); !apierrors.IsNotFound(
					err,
				) {
					return false, nil
				}
			}
			return true, nil
		},
	)
	if err != nil {
		pending := make([]string, 0, len(objects))
		for _, object := range objects {
			if err := c.Get(
				context.Background(),
				object.key,
				object.object.DeepCopyObject().(client.Object),
			); !apierrors.IsNotFound(err) {
				pending = append(pending, object.description)
			}
		}
		t.Fatalf("timed out waiting for deletion of %v: %v", pending, err)
	}
}
