package multigrescluster

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// syncBackupRepositoryCondition sets ConditionBackupRepositoryValid from the
// defaulted spec and returns the refusal, if any. It applies every rule
// admission applies, so the refusal holds with the webhook disabled and for a
// stored cluster that already violates one. The event is emitted when the
// refusal starts or its reason changes.
//
// The caller does not register databases in topology or apply TableGroups for
// a refused cluster: either would start, or keep, backups going somewhere
// other than the repository its configuration asks for, or build Shards whose
// sidecars cannot be started.
func (r *MultigresClusterReconciler) syncBackupRepositoryCondition(
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionBackupRepositoryValid,
		Status:             metav1.ConditionTrue,
		Message:            "Backups go to the configured repository",
		ObservedGeneration: cluster.Generation,
	}
	reason, err := resolver.SidecarConfigProblem(cluster)
	cond.Reason = reason
	if err != nil {
		cond.Status = metav1.ConditionFalse
		cond.Message = err.Error()
		prev := meta.FindStatusCondition(
			cluster.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid,
		)
		if r.Recorder != nil &&
			(prev == nil || prev.Status != metav1.ConditionFalse || prev.Reason != reason) {
			r.Recorder.Event(cluster, "Warning", "BackupRepositoryInvalid", cond.Message)
		}
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, cond)
	return err
}

// syncS3TLSCondition sets ConditionS3TLSVerified to False while a backup
// location of the spec turns TLS verification of its S3 endpoint off, and
// removes the condition otherwise. The condition is derived from the spec the
// sidecars are started from: the sidecar reports the same in its own status
// and as a metric, on a socket the operator has no client for.
func syncS3TLSCondition(cluster *multigresv1alpha1.MultigresCluster) {
	unverified := resolver.UnverifiedS3Locations(cluster)
	if len(unverified) == 0 {
		meta.RemoveStatusCondition(
			&cluster.Status.Conditions, multigresv1alpha1.ConditionS3TLSVerified)
		return
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:   multigresv1alpha1.ConditionS3TLSVerified,
		Status: metav1.ConditionFalse,
		Reason: multigresv1alpha1.ReasonS3TLSVerificationDisabled,
		Message: "TLS verification of the S3 endpoint is disabled (verifyTLS: false) for " +
			strings.Join(unverified, ", "),
		ObservedGeneration: cluster.Generation,
	})
}

// retainPVCsWhileRefused carries one thing from a refused cluster's spec to
// its existing TableGroups: a PVC deletion policy that asks for claims to be
// kept where the TableGroup still says to delete them.
//
// Nothing else of a refused spec is applied, so without this an edit to
// Retain is accepted by admission and never arrives. Deleting the cluster
// then deletes every claim its Shards still own, and which claims those are
// follows the policy the Shards last received. The edit is the one an owner
// makes before deleting a cluster that cannot be repaired.
//
// Only Retain is carried, for whenDeleted and whenScaled each. A refused spec
// that asks for Delete where the TableGroup keeps claims changes nothing:
// the spec is not one the operator acts on, and that direction destroys data.
// The policy is compared as the Shard controller reads it, per pool with the
// shard's and the TableGroup's behind it, so a Retain at any level of the
// spec arrives.
//
// A TableGroup the catalog created is named nowhere in the spec. It takes its
// policy from the database's default tablegroup, as buildDynamicTableGroup
// gives it: the default tablegroup's own level, and the first resolved shard
// of it for every shard. Nothing is provisioned or pruned here.
func (r *MultigresClusterReconciler) retainPVCsWhileRefused(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) error {
	existing := &multigresv1alpha1.TableGroupList{}
	if err := r.List(ctx, existing, client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"multigres.com/cluster": cluster.Name}); err != nil {
		return fmt.Errorf("failed to list tablegroups of a refused cluster: %w", err)
	}
	byName := make(map[string]*multigresv1alpha1.TableGroup, len(existing.Items))
	for i := range existing.Items {
		byName[existing.Items[i].Name] = &existing.Items[i]
	}
	var cells []multigresv1alpha1.CellName
	for _, c := range cluster.Spec.Cells {
		cells = append(cells, c.Name)
	}

	// wanted is the policy a tablegroup of the spec asks for: its own level,
	// and each of its shards resolved.
	type wanted struct {
		tableGroup *multigresv1alpha1.PVCDeletionPolicy
		shards     map[string]multigresv1alpha1.ShardResolvedSpec
	}
	// dynamic is, per database, what a TableGroup the catalog created takes
	// from the database's default tablegroup.
	// shard is nil when the default tablegroup's first shard could not be
	// resolved. The tablegroup's own level does not depend on that
	// resolution and is carried either way.
	type dynamicWanted struct {
		tableGroup *multigresv1alpha1.PVCDeletionPolicy
		shard      *multigresv1alpha1.ShardResolvedSpec
	}
	dynamic := map[multigresv1alpha1.DatabaseName]dynamicWanted{}

	var errs []error
	for _, db := range cluster.Spec.Databases {
		for i := range db.TableGroups {
			cfg := &db.TableGroups[i]
			want := wanted{
				tableGroup: multigresv1alpha1.MergePVCDeletionPolicy(
					cfg.PVCDeletionPolicy, cluster.Spec.PVCDeletionPolicy),
				shards: map[string]multigresv1alpha1.ShardResolvedSpec{},
			}
			unresolved := map[string]bool{}
			if cfg.Default {
				dynamic[db.Name] = dynamicWanted{tableGroup: want.tableGroup}
			}
			for n, shard := range cfg.Shards {
				shardCfg := shard.DeepCopy()
				if shardCfg.ShardTemplate == "" {
					shardCfg.ShardTemplate = cluster.Spec.TemplateDefaults.ShardTemplate
				}
				resolved, err := res.ResolveShard(ctx, shardCfg, resolver.ResolveShardOptions{
					AllCellNames:            cells,
					MaterializeCellDefaults: true,
					Mode:                    cluster.Spec.Mode,
				})
				if err != nil {
					errs = append(errs, fmt.Errorf(
						"failed to resolve shard '%s' for its PVC deletion policy: %w",
						shard.Name, err))
					unresolved[string(shard.Name)] = true
					continue
				}
				spec := multigresv1alpha1.ShardResolvedSpec{
					Pools: resolved.Pools, PVCDeletionPolicy: resolved.PVCDeletionPolicy,
				}
				want.shards[string(shard.Name)] = spec
				if cfg.Default && n == 0 {
					dynamic[db.Name] = dynamicWanted{tableGroup: want.tableGroup, shard: &spec}
				}
			}
			tg := byName[TableGroupName(cluster.Name, db.Name, cfg.Name)]
			if tg == nil || isDynamic(tg) || tg.DeletionTimestamp != nil {
				continue
			}
			if err := r.retainInTableGroup(ctx, cluster, tg, want.tableGroup,
				func(shard string) (*multigresv1alpha1.ShardResolvedSpec, bool) {
					if spec, ok := want.shards[shard]; ok {
						return &spec, true
					}
					return nil, unresolved[shard]
				}); err != nil {
				errs = append(errs, err)
			}
		}
	}
	// The list comes back in name order, so the TableGroups are visited, and
	// their errors reported, in the same order every pass.
	for i := range existing.Items {
		tg := &existing.Items[i]
		if !isDynamic(tg) || tg.DeletionTimestamp != nil {
			continue
		}
		want, ok := dynamic[tg.Spec.DatabaseName]
		if !ok {
			continue
		}
		if err := r.retainInTableGroup(ctx, cluster, tg, want.tableGroup,
			func(string) (*multigresv1alpha1.ShardResolvedSpec, bool) {
				return want.shard, true
			}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// retainInTableGroup raises tg's PVC deletion policy to Retain wherever
// wantTG and the shard specs wantShard returns ask for it, and writes tg when
// that changed it. wantShard reports false for a shard the spec does not
// have, which is left as it is.
//
// wantShard returns nil and true for a shard the spec has and that could not
// be resolved, so what the spec asks for at the shard's and its pools' own
// levels is not known. The tablegroup's level is still written, and reaches
// such a shard wherever the stored shard sets nothing of its own. Where the
// stored shard or one of its pools says Delete against a tablegroup level of
// Retain, the claims are still deleted with the cluster, and nothing here
// can tell whether the spec wants that. That is returned as an error naming
// the shards, after the write, so that it is in the event and the log of a
// cluster whose owner may be about to delete it.
func (r *MultigresClusterReconciler) retainInTableGroup(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	tg *multigresv1alpha1.TableGroup,
	wantTG *multigresv1alpha1.PVCDeletionPolicy,
	wantShard func(shard string) (*multigresv1alpha1.ShardResolvedSpec, bool),
) error {
	stored := tg.DeepCopy()
	changed := retainWhere(&tg.Spec.PVCDeletionPolicy, nil, wantTG)
	var unprotected []string
	for j := range tg.Spec.Shards {
		shard := &tg.Spec.Shards[j]
		want, wanted := wantShard(shard.Name)
		if !wanted {
			continue
		}
		if want == nil {
			if !retainedThroughout(shard, tg.Spec.PVCDeletionPolicy, wantTG) {
				unprotected = append(unprotected, shard.Name)
			}
			continue
		}
		wantShardPolicy := multigresv1alpha1.MergePVCDeletionPolicy(
			want.PVCDeletionPolicy, wantTG)
		if retainWhere(&shard.PVCDeletionPolicy, tg.Spec.PVCDeletionPolicy, wantShardPolicy) {
			changed = true
		}
		have := multigresv1alpha1.MergePVCDeletionPolicy(
			shard.PVCDeletionPolicy, tg.Spec.PVCDeletionPolicy)
		for name, pool := range shard.Pools {
			wantPool, wanted := want.Pools[name]
			if !wanted {
				continue
			}
			if retainWhere(&pool.PVCDeletionPolicy, have,
				multigresv1alpha1.MergePVCDeletionPolicy(
					wantPool.PVCDeletionPolicy, wantShardPolicy)) {
				shard.Pools[name] = pool
				changed = true
			}
		}
	}
	var unknown error
	if len(unprotected) > 0 {
		unknown = fmt.Errorf(
			"tablegroup '%s' shard %s: the spec's PVC deletion policy for the shard could not "+
				"be resolved, and the stored shard or one of its pools deletes its PVCs where "+
				"the tablegroup's level retains them; those PVCs are not kept",
			tg.Name, strings.Join(unprotected, ", "))
	}
	if !changed {
		return unknown
	}
	if err := r.writeRetained(ctx, tg, stored); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted since it was listed: it has no claims to keep.
			return nil
		}
		return errors.Join(
			fmt.Errorf("failed to keep the PVCs of tablegroup '%s': %w", tg.Name, err), unknown)
	}
	r.Recorder.Eventf(cluster, "Normal", "PVCRetentionApplied",
		"TableGroup %s keeps its PVCs as the spec asks, although the spec is refused",
		tg.Name)
	return unknown
}

// retainedThroughout reports whether shard, as stored, keeps every claim
// that want keeps: its own level, which the shared backup claim follows, and
// each of its pools, read with the tablegroup's level behind them.
func retainedThroughout(
	shard *multigresv1alpha1.ShardResolvedSpec,
	tableGroup, want *multigresv1alpha1.PVCDeletionPolicy,
) bool {
	const retain = multigresv1alpha1.RetainPVCRetentionPolicy
	if want == nil {
		return true
	}
	covers := func(have *multigresv1alpha1.PVCDeletionPolicy) bool {
		if have == nil {
			have = &multigresv1alpha1.PVCDeletionPolicy{}
		}
		return (want.WhenDeleted != retain || have.WhenDeleted == retain) &&
			(want.WhenScaled != retain || have.WhenScaled == retain)
	}
	own := multigresv1alpha1.MergePVCDeletionPolicy(shard.PVCDeletionPolicy, tableGroup)
	if !covers(own) {
		return false
	}
	for _, name := range slices.Sorted(maps.Keys(shard.Pools)) {
		if !covers(multigresv1alpha1.MergePVCDeletionPolicy(
			shard.Pools[name].PVCDeletionPolicy, own)) {
			return false
		}
	}
	return true
}

// writeRetained stores tg, whose spec differs from stored only in the PVC
// deletion policies raised to Retain.
//
// It is written as a server-side apply under the manager that applies
// TableGroups, so that the policy stays in what that manager owns. Written by
// another manager, a Retain at a level the spec stops setting once the
// cluster is accepted would outlive the refusal: the accepted apply leaves
// that field out, and leaving out a field another manager owns removes
// nothing.
//
// An apply removes what its manager owns and the object sent leaves out, and
// takes a share of what it sends. So the apply carries the stored spec, the
// owner references, and of the labels and annotations exactly the keys the
// manager owns: a key another writer set, such as the pending-deletion
// annotation, is neither removed nor taken. The hash of the last applied spec
// is the one owned key left out. It no longer describes the object, and with
// it in place the apply that follows an accepted spec equal to that one would
// be skipped.
//
// A TableGroup the manager has no apply entry on was not written by an
// apply, so there is no owned set to keep, and it is patched instead.
//
// Either write is conditional on the TableGroup being the one that was read,
// by UID and resourceVersion. stored comes from the informer. An apply
// creates what it does not find, so one sent for a TableGroup deleted since
// would bring it back, with its Shards and pods behind it, and one sent over
// a TableGroup recreated or rewritten since would put the spec that was read
// over the newer one. The API server refuses both instead, the next pass
// reads again, and a TableGroup that is gone has no claims to keep.
func (r *MultigresClusterReconciler) writeRetained(
	ctx context.Context,
	tg, stored *multigresv1alpha1.TableGroup,
) error {
	labels, applied := appliedMetadataKeys(stored, ownership.ManagerChildWriter, "labels")
	annotations, _ := appliedMetadataKeys(stored, ownership.ManagerChildWriter, "annotations")
	if !applied {
		delete(tg.Annotations, metadata.AnnotationSpecHash)
		return r.Patch(ctx, tg, client.MergeFromWithOptions(
			stored, client.MergeFromWithOptimisticLock{}))
	}
	desired := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:            tg.Name,
			Namespace:       tg.Namespace,
			UID:             stored.UID,
			ResourceVersion: stored.ResourceVersion,
			OwnerReferences: stored.OwnerReferences,
		},
		Spec: tg.Spec.ForApply(),
	}
	for key := range labels {
		if value, ok := stored.Labels[key]; ok {
			metav1.SetMetaDataLabel(&desired.ObjectMeta, key, value)
		}
	}
	for key := range annotations {
		if value, ok := stored.Annotations[key]; ok && key != metadata.AnnotationSpecHash {
			metav1.SetMetaDataAnnotation(&desired.ObjectMeta, key, value)
		}
	}
	desired.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("TableGroup"))
	return r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner(ownership.ManagerChildWriter),
	)
}

// retainWhere sets Retain in *have for whenDeleted and whenScaled each where
// want says Retain and *have, read with inherited behind it, does not. It
// reports whether it changed *have.
func retainWhere(
	have **multigresv1alpha1.PVCDeletionPolicy,
	inherited, want *multigresv1alpha1.PVCDeletionPolicy,
) bool {
	const retain = multigresv1alpha1.RetainPVCRetentionPolicy
	if want == nil {
		return false
	}
	effective := multigresv1alpha1.MergePVCDeletionPolicy(*have, inherited)
	if effective == nil {
		effective = &multigresv1alpha1.PVCDeletionPolicy{}
	}
	changed := false
	raise := func(wanted, kept multigresv1alpha1.PVCRetentionPolicyType,
		field func(*multigresv1alpha1.PVCDeletionPolicy) *multigresv1alpha1.PVCRetentionPolicyType,
	) {
		if wanted != retain || kept == retain {
			return
		}
		if *have == nil {
			*have = &multigresv1alpha1.PVCDeletionPolicy{}
		}
		*field(*have) = retain
		changed = true
	}
	raise(want.WhenDeleted, effective.WhenDeleted,
		func(p *multigresv1alpha1.PVCDeletionPolicy) *multigresv1alpha1.PVCRetentionPolicyType {
			return &p.WhenDeleted
		})
	raise(want.WhenScaled, effective.WhenScaled,
		func(p *multigresv1alpha1.PVCDeletionPolicy) *multigresv1alpha1.PVCRetentionPolicyType {
			return &p.WhenScaled
		})
	return changed
}
