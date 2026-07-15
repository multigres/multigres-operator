package multigrescluster

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// BuildTableGroup constructs the desired TableGroup resource.
func BuildTableGroup(
	cluster *multigresv1alpha1.MultigresCluster,
	dbCfg multigresv1alpha1.DatabaseConfig,
	tgCfg *multigresv1alpha1.TableGroupConfig,
	resolvedShards []multigresv1alpha1.ShardResolvedSpec,
	globalTopoRef multigresv1alpha1.GlobalTopoServerRef,
	scheme *runtime.Scheme,
) (*multigresv1alpha1.TableGroup, error) {
	tgNameHash := name.JoinWithConstraints(
		name.DefaultConstraints,
		cluster.Name,
		string(dbCfg.Name),
		string(tgCfg.Name),
	)

	labels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentTableGroup)
	metadata.AddClusterLabel(labels, cluster.Name)
	metadata.AddDatabaseLabel(labels, dbCfg.Name)
	metadata.AddTableGroupLabel(labels, tgCfg.Name)
	var annotations map[string]string
	if projectRef := cluster.Annotations[metadata.AnnotationProjectRef]; projectRef != "" {
		annotations = map[string]string{
			metadata.AnnotationProjectRef: projectRef,
		}
	}

	tgCR := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        tgNameHash,
			Namespace:   cluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   dbCfg.Name,
			TableGroupName: tgCfg.Name,
			IsDefault:      tgCfg.Default,
			Images: multigresv1alpha1.ShardImages{
				Multiorch:        cluster.Spec.Images.Multiorch,
				Multipooler:      cluster.Spec.Images.Multipooler,
				Postgres:         cluster.Spec.Images.Postgres,
				ImagePullPolicy:  cluster.Spec.Images.ImagePullPolicy,
				ImagePullSecrets: cluster.Spec.Images.ImagePullSecrets,
			},
			GlobalTopoServer: globalTopoRef,
			Shards:           resolvedShards,
			// Merge hierarchy: TableGroup → MultigresCluster
			PVCDeletionPolicy: multigresv1alpha1.MergePVCDeletionPolicy(
				tgCfg.PVCDeletionPolicy,
				cluster.Spec.PVCDeletionPolicy,
			),
			// Merge hierarchy: TableGroup -> Database -> MultigresCluster
			Backup: multigresv1alpha1.MergeBackupConfig(
				tgCfg.Backup,
				multigresv1alpha1.MergeBackupConfig(dbCfg.Backup, cluster.Spec.Backup),
			),
			Observability:      cluster.Spec.Observability,
			LogLevels:          cluster.Spec.LogLevels,
			CellTopologyLabels: buildCellTopologyLabels(cluster),
			TopologyPruning:    cluster.Spec.TopologyPruning,
			DurabilityPolicy: multigresv1alpha1.MergeDurabilityPolicy(
				dbCfg.DurabilityPolicy,
				cluster.Spec.DurabilityPolicy,
			),
			PostgresSuperuser:         cluster.Spec.PostgresSuperuser,
			PostgresPasswordSecretRef: cluster.Spec.PostgresPasswordSecretRef,
		},
	}

	if err := controllerutil.SetControllerReference(cluster, tgCR, scheme); err != nil {
		return nil, err
	}

	return tgCR, nil
}

// buildCellTopologyLabels builds a map of cell name → nodeSelector labels from the cluster's cells.
// ZoneID cells get {metadata.NodeLabelZoneID: value}, region cells get
// {"topology.kubernetes.io/region": value}. Cells without zoneId or region are omitted.
func buildCellTopologyLabels(
	cluster *multigresv1alpha1.MultigresCluster,
) map[multigresv1alpha1.CellName]map[string]string {
	m := make(map[multigresv1alpha1.CellName]map[string]string)
	for _, cell := range cluster.Spec.Cells {
		switch {
		case cell.ZoneID != "":
			m[cell.Name] = map[string]string{
				metadata.NodeLabelZoneID: string(cell.ZoneID),
			}
		case cell.Region != "":
			m[cell.Name] = map[string]string{
				"topology.kubernetes.io/region": string(cell.Region),
			}
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
