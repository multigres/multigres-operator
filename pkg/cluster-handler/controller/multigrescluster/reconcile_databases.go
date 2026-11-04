package multigrescluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

// reconcileDatabases applies every TableGroup the spec names and drains and
// deletes every other one. The second half never waits on the first: a
// TableGroup that fails to resolve, build or apply is still counted as wanted,
// so it is never pruned, but its failure does not stop another TableGroup's
// drain from advancing. Otherwise one bad template would hold every removed
// TableGroup, drained or not, in place until the template was fixed.
// Failures are joined into the returned error.
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

	// A TableGroup already draining under a name the spec still uses is not
	// reused: its drain deletes pods and is not undone, and nothing removes
	// PendingDeletion, so applying over it would leave a TableGroup the spec
	// wants serving, drained and never reconciled again. Leaving it out of
	// activeTGNames lets the pruning below finish its handshake and delete
	// it; the next reconcile then creates a fresh one. Shards re-added
	// mid-drain are handled the same way by the TableGroup controller
	// (observedCleanupInProgress).
	//
	// Waiting on that drain counts as a pending deletion, including on the
	// pass that deletes the old object, when the pruning loop alone would
	// report nothing pending: the TableGroup the spec names does not exist
	// yet, so the cluster must not count itself settled (retiring the managed
	// global topology waits on that) until the fresh one is created.
	var pendingDeletion bool
	activeTGNames := make(map[string]bool)
	for _, db := range cluster.Spec.Databases {
		for _, tg := range db.TableGroups {
			tgName := TableGroupName(cluster.Name, db.Name, tg.Name)
			if !tgCleanupInProgress(existingTGs, tgName) {
				activeTGNames[tgName] = true
				continue
			}
			pendingDeletion = true
			r.Recorder.Eventf(cluster, "Normal", "AwaitingDrain",
				"TableGroup %s is still draining; it is recreated once the drain completes",
				tgName)
		}
	}

	var errs []error
	if err := r.applyTableGroups(ctx, cluster, res, activeTGNames); err != nil {
		errs = append(errs, err)
	}

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
				errs = append(errs, fmt.Errorf(
					"failed to set PendingDeletion on tablegroup '%s': %w", item.Name, err))
				continue
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
		if err := r.Delete(ctx, item); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf(
				"failed to delete orphaned tablegroup '%s': %w",
				item.Name,
				err,
			))
		} else if err == nil {
			r.Recorder.Eventf(cluster, "Normal", "Deleted",
				"Deleted orphaned TableGroup %s", item.Name)
		}
	}

	return pendingDeletion, errors.Join(errs...)
}

// applyTableGroups applies each TableGroup in the spec whose name is in
// active, continuing past a failing one so the rest stay current.
func (r *MultigresClusterReconciler) applyTableGroups(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
	active map[string]bool,
) error {
	globalTopoRef, err := r.globalTopoRef(ctx, cluster, res)
	if err != nil {
		return fmt.Errorf("failed to get global topo ref: %w", err)
	}

	var gatewayClusterViews *multigresv1alpha1.GatewayClusterViewsConfig
	if cluster.Spec.Mode == multigresv1alpha1.ModeLite {
		if len(cluster.Spec.Cells) != 1 {
			return fmt.Errorf("lite mode requires exactly one cell")
		}
		cellCfg := cluster.Spec.Cells[0].DeepCopy()
		cellCfg.CellTemplate = cluster.Spec.EffectiveCellTemplate(cellCfg.CellTemplate)
		gateway, _, _, err := res.ResolveCell(ctx, cluster, cellCfg)
		if err != nil {
			return fmt.Errorf("failed to resolve Lite gateway configuration: %w", err)
		}
		gatewayClusterViews = gateway.ClusterViews
	}

	var errs []error
	for _, db := range cluster.Spec.Databases {
		for i := range db.TableGroups {
			tg := &db.TableGroups[i]
			if !active[TableGroupName(cluster.Name, db.Name, tg.Name)] {
				continue
			}
			if err := r.applyTableGroup(
				ctx, cluster, res, db, tg, globalTopoRef, gatewayClusterViews,
			); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// applyTableGroup resolves one TableGroup's Shards and server-side applies it.
func (r *MultigresClusterReconciler) applyTableGroup(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
	db multigresv1alpha1.DatabaseConfig,
	tg *multigresv1alpha1.TableGroupConfig,
	globalTopoRef multigresv1alpha1.GlobalTopoServerRef,
	gatewayClusterViews *multigresv1alpha1.GatewayClusterViewsConfig,
) error {
	dbBackup := multigresv1alpha1.MergeBackupConfig(db.Backup, cluster.Spec.Backup)
	tgBackup := multigresv1alpha1.MergeBackupConfig(tg.Backup, dbBackup)
	tgNameFull := fmt.Sprintf("%s-%s-%s", cluster.Name, string(db.Name), string(tg.Name))

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
		resolved, err := res.ResolveShard(
			ctx,
			shardCfg,
			resolver.ResolveShardOptions{
				AllCellNames:            allCellNames,
				InheritedBackup:         tgBackup,
				MaterializeCellDefaults: true,
				Mode:                    cluster.Spec.Mode,
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
			return fmt.Errorf(
				"failed to resolve shard '%s': %w",
				shard.Name,
				err,
			)
		}

		// The Resolver now handles the "Empty Cells = All Cells" logic authoritatively.
		// We no longer need to manually infer or sort here, just trust the resolver.
		resolvedShards = append(resolvedShards, multigresv1alpha1.ShardResolvedSpec{
			Name:                string(shard.Name),
			Multiorch:           resolved.Multiorch,
			GatewayClusterViews: gatewayClusterViews.DeepCopy(),
			InitdbArgs:          resolved.InitdbArgs,
			PostgresConfigRef:   resolved.PostgresConfigRef,
			PostgresConfig:      resolved.PostgresConfig,
			Pools:               resolved.Pools,
			PVCDeletionPolicy:   resolved.PVCDeletionPolicy,
			Backup:              resolved.Backup,
		})
	}

	desired, err := BuildTableGroup(
		cluster,
		db,
		tg,
		resolvedShards,
		globalTopoRef,
		r.Scheme,
	)
	if err != nil {
		return fmt.Errorf("failed to build tablegroup '%s': %w", tgNameFull, err)
	}

	desired.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("TableGroup"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner(ownership.ManagerChildWriter),
	); err != nil {
		return fmt.Errorf("failed to apply tablegroup '%s': %w", tgNameFull, err)
	}
	r.Recorder.Eventf(cluster, "Normal", "Applied", "Applied TableGroup %s", desired.Name)
	return nil
}

// tgCleanupInProgress reports whether the observed TableGroup named name is
// draining or terminating.
func tgCleanupInProgress(existing *multigresv1alpha1.TableGroupList, name string) bool {
	for i := range existing.Items {
		if existing.Items[i].Name == name {
			return tgDraining(&existing.Items[i])
		}
	}
	return false
}

// tgDraining reports whether tg has started its drain handshake or is being
// deleted.
func tgDraining(tg *multigresv1alpha1.TableGroup) bool {
	return tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" ||
		tg.DeletionTimestamp != nil
}
