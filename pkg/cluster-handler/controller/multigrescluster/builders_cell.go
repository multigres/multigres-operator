package multigrescluster

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// BuildCell constructs the desired Cell resource.
func BuildCell(
	cluster *multigresv1alpha1.MultigresCluster,
	cellCfg *multigresv1alpha1.CellConfig,
	gatewaySpec *multigresv1alpha1.StatelessSpec,
	gatewayPlacement *multigresv1alpha1.PodPlacementSpec,
	localTopoSpec *multigresv1alpha1.LocalTopoServerSpec,
	globalTopoRef multigresv1alpha1.GlobalTopoServerRef,
	allCellNames []multigresv1alpha1.CellName,
	scheme *runtime.Scheme,
) (*multigresv1alpha1.Cell, error) {
	labels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentCell)
	metadata.AddClusterLabel(labels, cluster.Name)
	metadata.AddCellLabel(labels, cellCfg.Name)
	var annotations map[string]string
	if projectRef := cluster.Annotations[metadata.AnnotationProjectRef]; projectRef != "" {
		annotations = map[string]string{
			metadata.AnnotationProjectRef: projectRef,
		}
	}

	cellCR := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name: name.JoinWithConstraints(
				name.DefaultConstraints, cluster.Name, string(cellCfg.Name)),
			Namespace:   cluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: multigresv1alpha1.CellSpec{
			Name:   cellCfg.Name,
			ZoneID: cellCfg.ZoneID,
			Region: cellCfg.Region,
			Images: multigresv1alpha1.CellImages{
				Multigateway:     cluster.Spec.Images.Multigateway,
				ImagePullPolicy:  cluster.Spec.Images.ImagePullPolicy,
				ImagePullSecrets: cluster.Spec.Images.ImagePullSecrets,
			},
			Multigateway:          *gatewaySpec,
			MultigatewayPlacement: gatewayPlacement.DeepCopy(),
			AllCells:              allCellNames,
			GlobalTopoServer:      globalTopoRef,
			TopoServer:            localTopoSpec.DeepCopy(),
			TopologyReconciliation: multigresv1alpha1.TopologyReconciliation{
				RegisterCell: true,
				PrunePoolers: isPruningEnabled(cluster),
			},
			Observability:  cluster.Spec.Observability,
			LogLevels:      cluster.Spec.LogLevels,
			CertCommonName: cluster.Spec.CertCommonName,
		},
	}

	if err := controllerutil.SetControllerReference(cluster, cellCR, scheme); err != nil {
		return nil, err
	}

	return cellCR, nil
}
