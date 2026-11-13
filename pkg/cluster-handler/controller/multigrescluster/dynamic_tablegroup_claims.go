package multigrescluster

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// Provision claims (TableGroupProvisionClaim in multigres, at
// databases/<db>/TableGroupClaims/<id> in the global topology) are the
// operator's word to the catalog that it holds something for a tablegroup id.
// The catalog primary completes a drop only once the id has neither a claim
// nor a pooler record, so:
//
//   - Before provisioning an id, the operator claims it, then reads the list
//     again, and applies the TableGroup only if the id is still creating or
//     active. An id dropped between the first read and the claim was, being
//     unclaimed, free to be deleted at once; the second read is what sees
//     that. States only move forward (creating, dropping, deleted), so an id
//     the second read lists as creating or active cannot complete a drop
//     until the claim is released.
//   - For an id the catalog lists as dropping, the TableGroup is left out of
//     the active set and pruned. Each Shard's deletion removes the id's pooler
//     records before its finalizer goes (cleanupDynamicShardTopology). The
//     claim is released last, once no TableGroup and no Shard carry the id.
//     The catalog primary deletes the shards' initialization claims itself
//     once the drop completes.
//
// A claim is never released for an id merely absent from a list, except right
// after claiming it, when nothing has been provisioned for it yet.

// claimStore opens the global topology once per reconcile, on first use.
type claimStore struct {
	r       *MultigresClusterReconciler
	cluster *multigresv1alpha1.MultigresCluster
	ref     multigresv1alpha1.GlobalTopoServerRef
	store   topoclient.Store
}

func (c *claimStore) get(ctx context.Context) (topoclient.Store, error) {
	if c.store != nil {
		return c.store, nil
	}
	store, err := c.r.openTopoStore(ctx, c.cluster.Namespace, c.ref)
	if err != nil {
		return nil, fmt.Errorf("failed to open topology store: %w", err)
	}
	c.store = store
	return store, nil
}

func (c *claimStore) close() {
	if c.store != nil {
		_ = c.store.Close()
	}
}

// provisionerID names this cluster as the holder of its claims. A cluster
// recreated under the same name takes over the claims of the one before.
func provisionerID(cluster *multigresv1alpha1.MultigresCluster) string {
	return fmt.Sprintf("multigres-operator/%s/%s", cluster.Namespace, cluster.Name)
}

// claimAndRecheck claims every entry of pending, reads the database's list
// again, and returns the claimed entries the second read still lists as
// creating or active, which may now be provisioned. The claims of the rest are
// released: a pending entry passed priorIncarnationPresent, so no TableGroup
// or Shard of its name exists and nothing is held for it. A failed second read
// returns its error and provisions nothing; the claims stay until a later
// read decides them.
func (r *MultigresClusterReconciler) claimAndRecheck(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	claims *claimStore,
	db multigresv1alpha1.DatabaseName,
	pending []multiadmin.TableGroup,
) ([]multiadmin.TableGroup, []string, error) {
	ctx, cancel := context.WithTimeout(ctx, topoOperationTimeout)
	defer cancel()

	var problems []string
	store, err := claims.get(ctx)
	if err != nil {
		return nil, []string{fmt.Sprintf("%s: %v", db, err)}, nil
	}
	var claimed []multiadmin.TableGroup
	for _, want := range pending {
		if err := store.ClaimTableGroupProvisioning(ctx, string(db),
			&clustermetadatapb.TableGroupProvisionClaim{
				Provisioner:    provisionerID(cluster),
				TablegroupId:   want.ID,
				TablegroupName: want.Name,
			}); err != nil {
			problems = append(problems, fmt.Sprintf("%s/%s (id %d): failed to claim: %v",
				db, want.Name, want.ID, err))
			continue
		}
		claimed = append(claimed, want)
	}
	if len(claimed) == 0 {
		return nil, problems, nil
	}

	again, err := r.listDesiredTableGroups(ctx, cluster, string(db))
	if err != nil {
		return nil, append(problems, fmt.Sprintf("%s: %v", db, err)),
			fmt.Errorf(
				"failed to recheck the tablegroups of database %s after claiming: %w",
				db,
				err,
			)
	}
	still := map[int64]bool{}
	for _, tg := range again {
		if tg.Provisionable() {
			still[tg.ID] = true
		}
	}
	var ready []multiadmin.TableGroup
	for _, want := range claimed {
		if still[want.ID] {
			ready = append(ready, want)
			continue
		}
		if err := store.ReleaseTableGroupProvisioning(ctx, string(db), want.ID); err != nil {
			problems = append(problems, fmt.Sprintf("%s/%s (id %d): failed to release claim: %v",
				db, want.Name, want.ID, err))
		}
	}
	return ready, problems, nil
}

// releaseDroppedClaims releases this cluster's claim on every dropping entry
// that has nothing left: no TableGroup and no Shard carrying its id. A claim
// another provisioner holds is left to it.
func (r *MultigresClusterReconciler) releaseDroppedClaims(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	claims *claimStore,
	db multigresv1alpha1.DatabaseName,
	dropping []multiadmin.TableGroup,
	existing map[string]*multigresv1alpha1.TableGroup,
) []string {
	if len(dropping) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, topoOperationTimeout)
	defer cancel()

	gone, err := r.droppedIDsWithNothingLeft(ctx, cluster, db, dropping, existing)
	if err != nil {
		return []string{fmt.Sprintf("%s: %v", db, err)}
	}
	if len(gone) == 0 {
		return nil
	}
	store, err := claims.get(ctx)
	if err != nil {
		return []string{fmt.Sprintf("%s: %v", db, err)}
	}
	held, err := store.ListTableGroupProvisionClaims(ctx, string(db))
	if err != nil {
		return []string{fmt.Sprintf("%s: %v", db, err)}
	}
	ours := map[int64]bool{}
	for _, c := range held {
		if c.GetProvisioner() == provisionerID(cluster) {
			ours[c.GetTablegroupId()] = true
		}
	}
	var problems []string
	for _, tg := range gone {
		if !ours[tg.ID] {
			continue
		}
		if err := store.ReleaseTableGroupProvisioning(ctx, string(db), tg.ID); err != nil {
			problems = append(problems, fmt.Sprintf("%s/%s (id %d): failed to release claim: %v",
				db, tg.Name, tg.ID, err))
		}
	}
	return problems
}

// droppedIDsWithNothingLeft returns the entries of dropping for which no
// TableGroup of existing and no Shard carry the id. Shards are listed from the
// API server when an uncached reader is set: one created just before its
// TableGroup was deleted may not be in the cache yet.
func (r *MultigresClusterReconciler) droppedIDsWithNothingLeft(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	db multigresv1alpha1.DatabaseName,
	dropping []multiadmin.TableGroup,
	existing map[string]*multigresv1alpha1.TableGroup,
) ([]multiadmin.TableGroup, error) {
	lister := client.Reader(r.Client)
	if r.APIReader != nil {
		lister = r.APIReader
	}
	shards := &multigresv1alpha1.ShardList{}
	if err := lister.List(ctx, shards, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		metadata.LabelMultigresCluster:  cluster.Name,
		metadata.LabelMultigresDatabase: string(db),
	}); err != nil {
		return nil, fmt.Errorf("failed to list shards: %w", err)
	}
	held := map[int64]bool{}
	for i := range shards.Items {
		held[multigresv1alpha1.TableGroupIDOf(&shards.Items[i])] = true
	}
	for _, tg := range existing {
		if isDynamic(tg) && tg.Spec.DatabaseName == db {
			held[multigresv1alpha1.TableGroupIDOf(tg)] = true
		}
	}
	var gone []multiadmin.TableGroup
	for _, tg := range dropping {
		if !held[tg.ID] {
			gone = append(gone, tg)
		}
	}
	return gone, nil
}
