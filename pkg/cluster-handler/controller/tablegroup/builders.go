package tablegroup

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// BuildShard constructs the desired Shard resource.
func BuildShard(
	tg *multigresv1alpha1.TableGroup,
	shardSpec *multigresv1alpha1.ShardResolvedSpec,
	scheme *runtime.Scheme,
) (*multigresv1alpha1.Shard, error) {
	// Build shard name from logical parts (cluster, database, tablegroup, shard)
	// NOT using tg.Name which includes the parent's hash
	clusterName := tg.Labels[metadata.LabelMultigresCluster]

	shardNameFull := name.JoinWithConstraints(
		name.DefaultConstraints,
		clusterName,
		string(tg.Spec.DatabaseName),
		string(tg.Spec.TableGroupName),
		shardSpec.Name,
	)

	labels := metadata.BuildStandardLabels(clusterName, metadata.ComponentShard)
	metadata.AddClusterLabel(labels, clusterName)
	metadata.AddDatabaseLabel(labels, tg.Spec.DatabaseName)
	metadata.AddTableGroupLabel(labels, tg.Spec.TableGroupName)
	metadata.AddShardLabel(labels, multigresv1alpha1.ShardName(shardSpec.Name))
	var annotations map[string]string
	if projectRef := tg.Annotations[metadata.AnnotationProjectRef]; projectRef != "" {
		annotations = map[string]string{
			metadata.AnnotationProjectRef: projectRef,
		}
	}

	shardCR := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:        shardNameFull,
			Namespace:   tg.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:      tg.Spec.DatabaseName,
			TableGroupName:    tg.Spec.TableGroupName,
			ShardName:         multigresv1alpha1.ShardName(shardSpec.Name),
			Images:            tg.Spec.Images,
			GlobalTopoServer:  tg.Spec.GlobalTopoServer,
			Multiorch:         shardSpec.Multiorch,
			InitdbArgs:        shardSpec.InitdbArgs,
			PostgresConfigRef: shardSpec.PostgresConfigRef,
			Pools:             shardSpec.Pools,
			Replicas:          calculateTotalReplicas(shardSpec.Pools),
			// Merge hierarchy: Shard → TableGroup
			PVCDeletionPolicy: multigresv1alpha1.MergePVCDeletionPolicy(
				shardSpec.PVCDeletionPolicy,
				tg.Spec.PVCDeletionPolicy,
			),
			Observability:             tg.Spec.Observability,
			LogLevels:                 tg.Spec.LogLevels,
			Backup:                    shardSpec.Backup,
			TopologyPruning:           tg.Spec.TopologyPruning,
			DurabilityPolicy:          tg.Spec.DurabilityPolicy,
			PostgresSuperuser:         tg.Spec.PostgresSuperuser,
			PostgresPasswordSecretRef: tg.Spec.PostgresPasswordSecretRef,
			CellTopologyLabels:        tg.Spec.CellTopologyLabels,
		},
	}
	if err := controllerutil.SetControllerReference(tg, shardCR, scheme); err != nil {
		return nil, err
	}

	return shardCR, nil
}

func calculateTotalReplicas(
	pools map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec,
) *int32 {
	var total int32
	for _, pool := range pools {
		// Default matches pkg/resource-handler/controller/shard/pool_pod.go
		replicas := int32(1)
		if pool.ReplicasPerCell != nil {
			replicas = *pool.ReplicasPerCell
		}
		total += replicas * int32(len(pool.Cells)) // #nosec G115
	}
	return &total
}
