package multigrescluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
	logger := log.FromContext(ctx)
	// Be conservative with clusters restored from older Kubernetes objects.
	if cluster.Status.ObservedGeneration != 0 || cluster.Status.Phase != "" {
		logger.Info("Preserving backup paths because the cluster has prior status",
			"observedGeneration", cluster.Status.ObservedGeneration, "phase", cluster.Status.Phase)
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
	for i := range tableGroups.Items {
		if tg := &tableGroups.Items[i]; mayBeReusedByCluster(tg, cluster) {
			logger.Info(
				"Preserving backup paths because a TableGroup exists",
				"tableGroup",
				client.ObjectKeyFromObject(tg),
			)
			return nil
		}
	}
	// A new CR may reuse data/topology PVCs from a deleted cluster. Their
	// repository cannot be inferred from the PVC, so preserve the supplied
	// configuration exactly, just as for an already provisioned cluster.
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(ctx, pvcs, opts...); err != nil {
		return fmt.Errorf("checking retained PVCs before allocating backup prefix: %w", err)
	}
	for i := range pvcs.Items {
		if pvc := &pvcs.Items[i]; mayBeReusedByCluster(pvc, cluster) {
			logger.Info(
				"Preserving backup paths because a PVC exists",
				"pvc",
				client.ObjectKeyFromObject(pvc),
			)
			return nil
		}
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

// A same-name replacement can see resources awaiting garbage collection.
// A Shard or StatefulSet UID does not identify the owning MultigresCluster,
// and a retained PVC can have no owner at all.
func mayBeReusedByCluster(
	obj metav1.Object,
	cluster *multigresv1alpha1.MultigresCluster,
) bool {
	if !obj.GetDeletionTimestamp().IsZero() {
		return false
	}
	clusterKind := multigresv1alpha1.GroupVersion.WithKind("MultigresCluster").GroupKind()
	hasClusterOwner := false
	for _, ref := range obj.GetOwnerReferences() {
		if schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).GroupKind() != clusterKind {
			continue
		}
		if ref.UID == "" || ref.UID == cluster.UID {
			return true
		}
		hasClusterOwner = true
	}
	return !hasClusterOwner
}
