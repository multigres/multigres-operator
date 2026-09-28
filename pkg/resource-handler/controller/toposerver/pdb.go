package toposerver

import (
	"fmt"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// BuildPodDisruptionBudget limits voluntary disruptions to one etcd member at a time.
func BuildPodDisruptionBudget(
	toposerver *multigresv1alpha1.TopoServer,
	scheme *runtime.Scheme,
) (*policyv1.PodDisruptionBudget, error) {
	clusterName := toposerver.Labels[metadata.LabelMultigresCluster]
	labels := metadata.BuildStandardLabels(clusterName, ComponentName)
	labels = metadata.MergeLabels(labels, toposerver.GetLabels())
	maxUnavailable := intstr.FromInt32(1)
	unhealthyPodEvictionPolicy := policyv1.AlwaysAllow

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toposerver.Name,
			Namespace: toposerver.Namespace,
			Labels:    labels,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: metadata.GetSelectorLabels(labels),
			},
			UnhealthyPodEvictionPolicy: &unhealthyPodEvictionPolicy,
		},
	}

	if err := ctrl.SetControllerReference(toposerver, pdb, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return pdb, nil
}
