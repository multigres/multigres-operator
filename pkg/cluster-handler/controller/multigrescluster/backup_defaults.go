package multigrescluster

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/backup"
	"github.com/multigres/multigres-operator/pkg/resolver"
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
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	key := multigresv1alpha1.AnnotationDefaultBackupKeyPrefix
	claim, prefix, err := backup.RetainedDefault(
		ctx,
		reader,
		resolver.NewResolver(r.Client, cluster.Namespace),
		cluster,
	)
	if err != nil {
		return err
	}
	if prefix != "" {
		metav1.SetMetaDataAnnotation(&cluster.ObjectMeta, key, prefix)
		multigresv1alpha1.DefaultBackupKeyPrefixes(cluster)
		logger.Info("Recovered backup prefix from retained PVC", "pvc", claim, "prefix", prefix)
		return nil
	}
	// Be conservative with clusters restored from older Kubernetes objects.
	if cluster.Status.ObservedGeneration != 0 || cluster.Status.Phase != "" {
		logger.Info("Preserving backup paths because the cluster has prior status",
			"observedGeneration", cluster.Status.ObservedGeneration, "phase", cluster.Status.Phase)
		return nil
	}
	if claim != "" {
		// A volume predating allocation records may belong to either a legacy
		// root repository or a generated one. Require the caller to disambiguate.
		probe := cluster.DeepCopy()
		metav1.SetMetaDataAnnotation(&probe.ObjectMeta, key, "clusters/"+string(cluster.UID))
		multigresv1alpha1.DefaultBackupKeyPrefixes(probe)
		if !equality.Semantic.DeepEqual(probe.Spec.Backup, cluster.Spec.Backup) ||
			!equality.Semantic.DeepEqual(probe.Spec.Databases, cluster.Spec.Databases) {
			return fmt.Errorf(
				"retained PVC %s has no recorded backup prefix: set the original S3 keyPrefix (use / for the legacy bucket root) before restoring",
				claim,
			)
		}
		logger.Info("Preserving supplied backup settings for retained PVC", "pvc", claim)
		return nil
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
		if tg := &tableGroups.Items[i]; backup.MayBeReusedByCluster(tg, cluster) {
			logger.Info(
				"Preserving backup paths because a TableGroup exists",
				"tableGroup",
				client.ObjectKeyFromObject(tg),
			)
			return nil
		}
	}
	if cluster.UID == "" {
		return fmt.Errorf("cannot allocate backup prefix before the cluster has a UID")
	}
	metav1.SetMetaDataAnnotation(&cluster.ObjectMeta, key, "clusters/"+string(cluster.UID))
	multigresv1alpha1.DefaultBackupKeyPrefixes(cluster)
	return nil
}
