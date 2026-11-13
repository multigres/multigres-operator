package multigrescluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// initializeBackupKeyPrefix runs only before the cleanup finalizer is first
// added. That finalizer predates automatic prefixes and is persisted before any
// child resources or topology records, so its presence means we must preserve
// the cluster's existing backup paths. The prefix and finalizer are committed
// together by ensureClusterFinalizer, before provisioning can start.
func (r *MultigresClusterReconciler) initializeBackupKeyPrefix(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	// Be conservative with clusters restored from older Kubernetes objects.
	if cluster.Status.ObservedGeneration != 0 || cluster.Status.Phase != "" {
		return nil
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	opts := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{metadata.LabelMultigresCluster: cluster.Name},
	}
	tableGroups := &multigresv1alpha1.TableGroupList{}
	if err := reader.List(ctx, tableGroups, opts...); err != nil {
		return fmt.Errorf(
			"failed to list existing tablegroups before allocating backup prefix: %w",
			err,
		)
	}
	if len(tableGroups.Items) != 0 {
		return nil
	}
	// A new CR may reuse data/topology PVCs from a deleted cluster. Their
	// repository cannot be inferred from the PVC, so preserve the supplied
	// configuration exactly, just as for an already provisioned cluster.
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(ctx, pvcs, opts...); err != nil {
		return fmt.Errorf("checking retained PVCs before allocating backup prefix: %w", err)
	}
	if len(pvcs.Items) != 0 {
		return nil
	}
	if cluster.UID == "" {
		return fmt.Errorf("cannot allocate backup prefix before the cluster has a UID")
	}
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix] = "clusters/" + string(
		cluster.UID,
	)
	multigresv1alpha1.DefaultBackupKeyPrefixes(cluster)
	return nil
}
