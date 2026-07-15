package multigrescluster

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

func (r *MultigresClusterReconciler) reconcileDatabases(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) (bool, error) {
	existingTGs := &multigresv1alpha1.TableGroupList{}
	if err := r.List(
		ctx,
		existingTGs,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"multigres.com/cluster": cluster.Name},
	); err != nil {
		return false, fmt.Errorf("failed to list existing tablegroups: %w", err)
	}

	globalTopoRef, err := r.globalTopoRef(ctx, cluster, res)
	if err != nil {
		return false, fmt.Errorf("failed to get global topo ref: %w", err)
	}

	activeTGNames := make(map[string]bool)

	for _, db := range cluster.Spec.Databases {
		dbBackup := multigresv1alpha1.MergeBackupConfig(db.Backup, cluster.Spec.Backup)
		for _, tg := range db.TableGroups {
			tgBackup := multigresv1alpha1.MergeBackupConfig(tg.Backup, dbBackup)
			tgNameFull := fmt.Sprintf("%s-%s-%s", cluster.Name, string(db.Name), string(tg.Name))
			// ACTIVE MAP REGISTRATION MOVED DOWN: We must use the desired.Name (which might be hashed)
			// instead of the logical tgNameFull.

			resolvedShards := []multigresv1alpha1.ShardResolvedSpec{}

			// Extract all valid cell names for this cluster (Contextual Awareness)
			var allCellNames []multigresv1alpha1.CellName
			for _, c := range cluster.Spec.Cells {
				allCellNames = append(allCellNames, c.Name)
			}

			for _, shard := range tg.Shards {
				// Create a copy to avoid mutating the original spec
				shardCfg := shard.DeepCopy()

				// Apply Global Template Default if Shard Template is not explicitly set
				if shardCfg.ShardTemplate == "" &&
					cluster.Spec.TemplateDefaults.ShardTemplate != "" {
					shardCfg.ShardTemplate = cluster.Spec.TemplateDefaults.ShardTemplate
				}

				// Pass allCellNames to the resolver so it can perform "Empty means Everybody" defaulting.
				// tgBackup carries the merged chain: TableGroup -> Database -> Cluster.
				orch, pools, pvcPolicy, finalShardBackup, initdbArgs, postgresConfigRef, err := res.ResolveShard(
					ctx,
					shardCfg,
					resolver.ResolveShardOptions{
						AllCellNames:            allCellNames,
						InheritedBackup:         tgBackup,
						MaterializeCellDefaults: true,
					},
				)
				if err != nil {
					r.Recorder.Eventf(
						cluster,
						"Warning",
						"ConfigError",
						"Failed to resolve shard %s: %v",
						shard.Name,
						err,
					)
					return false, fmt.Errorf(
						"failed to resolve shard '%s': %w",
						shard.Name,
						err,
					)
				}

				// The Resolver now handles the "Empty Cells = All Cells" logic authoritatively.
				// We no longer need to manually infer or sort here, just trust the resolver.
				resolvedShards = append(resolvedShards, multigresv1alpha1.ShardResolvedSpec{
					Name:              string(shard.Name),
					Multiorch:         *orch,
					InitdbArgs:        initdbArgs,
					PostgresConfigRef: postgresConfigRef,
					Pools:             pools,
					PVCDeletionPolicy: pvcPolicy,
					Backup:            finalShardBackup,
				})
			}

			desired, err := BuildTableGroup(
				cluster,
				db,
				&tg,
				resolvedShards,
				globalTopoRef,
				r.Scheme,
			)
			if err != nil {
				return false, fmt.Errorf("failed to build tablegroup '%s': %w", tgNameFull, err)
			}
			activeTGNames[desired.Name] = true

			// Server Side Apply
			desired.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("TableGroup"))
			if err := r.Patch(
				ctx,
				desired,
				client.Apply,
				client.ForceOwnership,
				client.FieldOwner("multigres-operator"),
			); err != nil {
				return false, fmt.Errorf("failed to apply tablegroup '%s': %w", tgNameFull, err)
			}
			r.Recorder.Eventf(cluster, "Normal", "Applied", "Applied TableGroup %s", desired.Name)
		}
	}

	var pendingDeletion bool

	for i := range existingTGs.Items {
		item := &existingTGs.Items[i]
		if activeTGNames[item.Name] {
			continue
		}

		// Step 1: Set PendingDeletion annotation if not already set.
		if item.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			patch := client.MergeFrom(item.DeepCopy())
			if item.Annotations == nil {
				item.Annotations = make(map[string]string)
			}
			item.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = metav1.Now().
				UTC().Format(time.RFC3339)
			if err := r.Patch(ctx, item, patch); err != nil {
				return false, fmt.Errorf("failed to set PendingDeletion on tablegroup '%s': %w",
					item.Name, err)
			}
			r.Recorder.Eventf(cluster, "Normal", "PendingDeletion",
				"Marked TableGroup %s for graceful deletion", item.Name)
			pendingDeletion = true
			continue
		}

		// Step 2: Wait for ReadyForDeletion condition.
		if !meta.IsStatusConditionTrue(
			item.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion,
		) {
			pendingDeletion = true
			continue
		}

		// Step 3: Drain complete — safe to delete.
		if err := r.Delete(ctx, item); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf(
				"failed to delete orphaned tablegroup '%s': %w",
				item.Name,
				err,
			)
		} else if err == nil {
			r.Recorder.Eventf(cluster, "Normal", "Deleted",
				"Deleted orphaned TableGroup %s", item.Name)
		}
	}

	return pendingDeletion, nil
}
