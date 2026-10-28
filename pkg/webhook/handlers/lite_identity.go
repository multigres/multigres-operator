package handlers

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

// Compare the resolved identity, not the template reference: a different
// template with the same pool is safe, while an override or fallback template
// can change the pool without changing an explicit reference.
func validateLiteIdentityUpdate(ctx context.Context, oldResolver, newResolver *resolver.Resolver,
	oldCluster, newCluster *multigresv1alpha1.MultigresCluster,
) error {
	oldID, err := resolvedLiteIdentity(ctx, oldResolver, oldCluster)
	if err != nil {
		return err
	}
	newID, err := resolvedLiteIdentity(ctx, newResolver, newCluster)
	if err != nil {
		return err
	}
	if oldID != newID {
		return fmt.Errorf(
			"lite pool identity is immutable (%s -> %s); data-preserving migration is not supported",
			oldID,
			newID,
		)
	}
	return nil
}

func resolvedLiteIdentity(
	ctx context.Context,
	res *resolver.Resolver,
	cluster *multigresv1alpha1.MultigresCluster,
) (string, error) {
	if len(cluster.Spec.Databases) != 1 || len(cluster.Spec.Databases[0].TableGroups) != 1 ||
		len(cluster.Spec.Databases[0].TableGroups[0].Shards) != 1 || len(cluster.Spec.Cells) != 1 {
		return "", fmt.Errorf(
			"lite mode requires exactly one database, table group, shard and cluster cell",
		)
	}
	db := cluster.Spec.Databases[0]
	tg := db.TableGroups[0]
	shard := tg.Shards[0].DeepCopy()
	if shard.ShardTemplate == "" {
		shard.ShardTemplate = cluster.Spec.TemplateDefaults.ShardTemplate
	}
	resolved, err := res.ResolveShard(ctx, shard, resolver.ResolveShardOptions{
		AllCellNames:            []multigresv1alpha1.CellName{cluster.Spec.Cells[0].Name},
		MaterializeCellDefaults: true, Mode: multigresv1alpha1.ModeLite,
	})
	if err != nil {
		return "", err
	}
	if len(resolved.Pools) != 1 {
		return "", fmt.Errorf("lite mode requires exactly one resolved pool")
	}
	for name, pool := range resolved.Pools {
		if len(pool.Cells) != 1 {
			return "", fmt.Errorf("lite mode requires exactly one resolved pool cell")
		}
		return fmt.Sprintf("%s/%s/%s/%s/%s", db.Name, tg.Name, shard.Name, name, pool.Cells[0]), nil
	}
	return "", fmt.Errorf("lite mode requires exactly one resolved pool")
}

// Template writes must preserve every Lite consumer's resolved identity. This
// includes creating the implicit default template after a cluster was created.
func (v *TemplateValidator) validateLiteShardTemplateConsumers(
	ctx context.Context,
	obj runtime.Object,
) error {
	tpl, ok := obj.(*multigresv1alpha1.ShardTemplate)
	if v.Kind != "ShardTemplate" || !ok {
		return nil
	}
	clusters := &multigresv1alpha1.MultigresClusterList{}
	if err := v.Client.List(ctx, clusters, client.InNamespace(tpl.Namespace)); err != nil {
		return err
	}
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if cluster.Spec.Mode != multigresv1alpha1.ModeLite || cluster.DeletionTimestamp != nil {
			continue
		}
		if !usesResolvedShardTemplate(cluster, tpl.Name) {
			continue
		}
		before := resolver.NewResolver(v.Client, tpl.Namespace)
		after := resolver.NewResolver(v.Client, tpl.Namespace)
		after.ShardTemplateCache[tpl.Name] = tpl
		if err := validateLiteIdentityUpdate(ctx, before, after, cluster, cluster); err != nil {
			return fmt.Errorf(
				"ShardTemplate %q would change Lite cluster %q: %w",
				tpl.Name,
				cluster.Name,
				err,
			)
		}
		if _, err := after.ValidateClusterLogic(ctx, cluster); err != nil {
			return fmt.Errorf("lite cluster %q: %w", cluster.Name, err)
		}
	}
	return nil
}

func usesResolvedShardTemplate(cluster *multigresv1alpha1.MultigresCluster, name string) bool {
	for _, db := range cluster.Spec.Databases {
		for _, tg := range db.TableGroups {
			for _, shard := range tg.Shards {
				// Inline specs overlay the template, so they consume it too.
				ref := shard.ShardTemplate
				if ref == "" {
					ref = cluster.Spec.TemplateDefaults.ShardTemplate
				}
				if ref == "" {
					ref = resolver.FallbackShardTemplate
				}
				if string(ref) == name {
					return true
				}
			}
		}
	}
	return false
}
