// Package backup identifies retained storage used to recover a cluster's S3 default.
package backup

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

// RetainedDefault returns a reusable claim name and the generated default recorded
// on such claims. Older volumes may have no record. All recorded allocations must agree.
func RetainedDefault(
	ctx context.Context,
	reader client.Reader,
	res *resolver.Resolver,
	cluster *api.MultigresCluster,
) (claim, prefix string, err error) {
	// Pods mount PVCs by name. A claim restored from a snapshot can lack the
	// original labels and still supply the data volume.
	var pvcs corev1.PersistentVolumeClaimList
	if err := reader.List(
		ctx,
		&pvcs,
		client.InNamespace(cluster.Namespace),
	); err != nil {
		return "", "", fmt.Errorf("checking retained PVCs before allocating backup prefix: %w", err)
	}
	var candidates []*corev1.PersistentVolumeClaim
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if strings.HasPrefix(pvc.Name, "data-") && MayBeReusedByCluster(pvc, cluster) {
			candidates = append(candidates, pvc)
		}
	}
	if len(candidates) == 0 {
		return "", "", nil
	}
	replicas, err := desiredDataClaims(ctx, res, cluster)
	if err != nil {
		return "", "", err
	}
	for _, pvc := range candidates {
		name := pvc.Name
		if !replicas.includes(name) {
			continue
		}
		if claim == "" {
			claim = name
		}
		saved, recorded := pvc.Annotations[api.AnnotationDefaultBackupKeyPrefix]
		if !recorded {
			continue
		}
		uid, ok := strings.CutPrefix(saved, "clusters/")
		if !ok || len(validation.IsDNS1123Label(uid)) != 0 {
			return "", "", fmt.Errorf(
				"retained PVC %s has invalid backup allocation %q; verify its original repository before restoring",
				name,
				saved,
			)
		}
		if prefix != "" && saved != prefix {
			return "", "", fmt.Errorf(
				"retained PVCs %s and %s record conflicting backup allocations %q and %q; verify which volumes belong to this cluster",
				claim,
				name,
				prefix,
				saved,
			)
		}
		claim, prefix = name, saved
	}
	return claim, prefix, nil
}

// claimReplicas maps the claim name at ordinal zero to the number of mounted
// replicas. Match the ordinal on an actual PVC without enumerating all replicas.
type claimReplicas map[string]int32

func (replicas claimReplicas) includes(claim string) bool {
	first, ordinal, ok := pvcutil.Ordinal(claim)
	return ok && ordinal < int(replicas[first])
}

// Resolve the same templates, pool layout and volume names used during provisioning.
// Work on a copy because the cleanup finalizer patch must not persist unrelated defaults.
func desiredDataClaims(
	ctx context.Context,
	res *resolver.Resolver,
	original *api.MultigresCluster,
) (claimReplicas, error) {
	cluster := original.DeepCopy()
	if _, err := res.PopulateClusterDefaults(ctx, cluster); err != nil {
		return nil, err
	}
	names := make(claimReplicas)
	addTopo := func(server string, etcd *api.EtcdSpec) {
		if etcd != nil {
			names["data-"+server+"-0"] = ptr.Deref(etcd.Replicas, resolver.DefaultEtcdReplicas)
		}
	}
	global, err := res.ResolveGlobalTopo(ctx, cluster)
	if err != nil {
		return nil, err
	}
	addTopo(cluster.Name+"-global-topo", global.Etcd)
	var cells []api.CellName
	for _, cell := range cluster.Spec.Cells {
		cells = append(cells, cell.Name)
		cell.CellTemplate = cluster.Spec.EffectiveCellTemplate(cell.CellTemplate)
		_, _, local, err := res.ResolveCell(ctx, cluster, &cell)
		if err != nil {
			return nil, err
		}
		if local != nil {
			cellName := name.JoinWithConstraints(
				name.DefaultConstraints,
				cluster.Name,
				string(cell.Name),
			)
			addTopo(topo.ManagedLocalTopoServerName(cellName), local.Etcd)
		}
	}
	for _, db := range cluster.Spec.Databases {
		for _, tg := range db.TableGroups {
			for _, cfg := range tg.Shards {
				if cfg.ShardTemplate == "" {
					cfg.ShardTemplate = cluster.Spec.TemplateDefaults.ShardTemplate
				}
				resolved, err := res.ResolveShard(ctx, &cfg, resolver.ResolveShardOptions{
					AllCellNames: cells, MaterializeCellDefaults: true, Mode: cluster.Spec.Mode,
				})
				if err != nil {
					return nil, err
				}
				identity := &api.Shard{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{metadata.LabelMultigresCluster: cluster.Name},
					},
					Spec: api.ShardSpec{
						DatabaseName:   db.Name,
						TableGroupName: tg.Name,
						ShardName:      cfg.Name,
					},
				}
				for poolName, pool := range resolved.Pools {
					for _, cell := range pool.Cells {
						first := pvcutil.PoolDataName(
							identity,
							string(poolName),
							string(cell),
							0,
						)
						names[first] = ptr.Deref(pool.ReplicasPerCell, int32(1))
					}
				}
			}
		}
	}
	return names, nil
}

// A same-name replacement can see resources awaiting garbage collection.
// A Shard or StatefulSet UID does not identify the owning MultigresCluster,
// and a retained PVC can have no owner at all.
func MayBeReusedByCluster(
	obj metav1.Object,
	cluster *api.MultigresCluster,
) bool {
	if !obj.GetDeletionTimestamp().IsZero() {
		return false
	}
	clusterKind := api.GroupVersion.WithKind("MultigresCluster").GroupKind()
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
