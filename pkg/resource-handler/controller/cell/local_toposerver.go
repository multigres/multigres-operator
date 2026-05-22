package cell

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// BuildLocalTopoServerName returns the managed TopoServer name for a Cell.
func BuildLocalTopoServerName(cell *multigresv1alpha1.Cell) string {
	return topo.ManagedLocalTopoServerName(cell.Name)
}

func hasManagedLocalTopoServer(cell *multigresv1alpha1.Cell) bool {
	return cell.Spec.TopoServer != nil && cell.Spec.TopoServer.Etcd != nil
}

func isControlledByCell(obj metav1.Object, cell *multigresv1alpha1.Cell) bool {
	controller := metav1.GetControllerOf(obj)
	if controller == nil {
		return false
	}
	if cell.UID != "" {
		return controller.UID == cell.UID
	}
	return controller.APIVersion == multigresv1alpha1.GroupVersion.String() &&
		controller.Kind == "Cell" &&
		controller.Name == cell.Name
}

// BuildLocalTopoServer constructs the desired managed local TopoServer for a Cell.
// External local topology is user-managed and does not need a child resource.
func BuildLocalTopoServer(
	cell *multigresv1alpha1.Cell,
	scheme *runtime.Scheme,
) (*multigresv1alpha1.TopoServer, error) {
	if !hasManagedLocalTopoServer(cell) {
		return nil, nil
	}

	etcd := cell.Spec.TopoServer.Etcd.DeepCopy()
	clusterName := cell.Labels[metadata.LabelMultigresCluster]
	labels := metadata.BuildStandardLabels(clusterName, "local-topo")
	metadata.AddClusterLabel(labels, clusterName)
	metadata.AddCellLabel(labels, cell.Spec.Name)

	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BuildLocalTopoServerName(cell),
			Namespace: cell.Namespace,
			Labels:    labels,
		},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd:              etcd,
			PVCDeletionPolicy: etcd.PVCDeletionPolicy,
		},
	}

	if err := ctrl.SetControllerReference(cell, ts, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return ts, nil
}
