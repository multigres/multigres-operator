package shard

import (
	"fmt"
	"maps"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

const (
	// MultiorchComponentName is the component label value for Multiorch resources
	MultiorchComponentName = "multiorch"
)

// BuildMultiorchDeployment creates a Deployment for the Multiorch component in a specific cell.
// For shards spanning multiple cells, this function should be called once per cell.
// Multiorch handles orchestration for the shard.
func BuildMultiorchDeployment(
	shard *multigresv1alpha1.Shard,
	cellName string,
	scheme *runtime.Scheme,
) (*appsv1.Deployment, error) {
	// Default to 1 replica per cell if not specified
	replicas := int32(1)
	if shard.Spec.Multiorch.Replicas != nil {
		replicas = *shard.Spec.Multiorch.Replicas
	}

	// Use DefaultConstraints (253 chars) for Deployments => Long, Readable Names
	name := buildMultiorchNameWithCell(shard, cellName, nameutil.DefaultConstraints)
	clusterName := shard.Labels["multigres.com/cluster"]
	labels := buildMultiorchLabelsWithCell(shard, cellName)
	annotations := maps.Clone(shard.Spec.Multiorch.PodAnnotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	delete(annotations, metadata.AnnotationPrometheusScrape)
	delete(annotations, metadata.AnnotationPrometheusPort)
	delete(annotations, metadata.AnnotationPrometheusPath)
	annotations[metadata.AnnotationProjectRef] = metadata.ResolveProjectRef(
		shard.Annotations,
		clusterName,
	)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: shard.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: metadata.GetSelectorLabels(labels),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      metadata.MergeLabels(labels, shard.Spec.Multiorch.PodLabels),
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						buildMultiorchContainer(shard, cellName),
					},
					NodeSelector: shard.Spec.CellTopologyLabels[multigresv1alpha1.CellName(cellName)],
					Affinity:     shard.Spec.Multiorch.Affinity,
					Tolerations:  tolerationsFromPlacement(shard.Spec.Multiorch.Placement),
				},
			},
		},
	}

	if otelVol, _ := multigresv1alpha1.BuildOTELSamplingVolume(
		shard.Spec.Observability,
	); otelVol != nil {
		deployment.Spec.Template.Spec.Volumes = append(
			deployment.Spec.Template.Spec.Volumes, *otelVol)
	}

	if err := ctrl.SetControllerReference(shard, deployment, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return deployment, nil
}

// BuildMultiorchService creates a Service for the Multiorch component in a specific cell.
func BuildMultiorchService(
	shard *multigresv1alpha1.Shard,
	cellName string,
	scheme *runtime.Scheme,
) (*corev1.Service, error) {
	// Use ServiceConstraints (63 chars) for Services => Truncated, Safe Names
	name := buildMultiorchNameWithCell(shard, cellName, nameutil.ServiceConstraints)
	labels := buildMultiorchLabelsWithCell(shard, cellName)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: shard.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: metadata.GetSelectorLabels(labels),
			Ports:    buildMultiorchServicePorts(),
		},
	}

	if err := ctrl.SetControllerReference(shard, svc, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return svc, nil
}

// buildMultiorchNameWithCell generates the name for Multiorch resources in a specific cell.
// Format: {cluster}-{db}-{tg}-{shard}-multiorch-{cellName}
func buildMultiorchNameWithCell(
	shard *multigresv1alpha1.Shard,
	cellName string,
	constraints nameutil.Constraints,
) string {
	// Logic: Use LOGICAL parts from Spec/Labels to avoid double hashing.
	// shard.Name is already hashed (cluster-db-tg-shard-HASH).
	clusterName := shard.Labels["multigres.com/cluster"]
	return nameutil.JoinWithConstraints(
		constraints,
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		"multiorch",
		cellName,
	)
}

// buildMultiorchLabelsWithCell creates labels for Multiorch resources in a specific cell.
func buildMultiorchLabelsWithCell(
	shard *multigresv1alpha1.Shard,
	cellName string,
) map[string]string {
	clusterName := shard.Labels["multigres.com/cluster"]
	labels := metadata.BuildStandardLabels(clusterName, MultiorchComponentName)
	metadata.AddCellLabel(labels, multigresv1alpha1.CellName(cellName))
	metadata.AddDatabaseLabel(labels, shard.Spec.DatabaseName)
	metadata.AddTableGroupLabel(labels, shard.Spec.TableGroupName)
	labels = metadata.MergeLabels(labels, shard.GetObjectMeta().GetLabels())
	return labels
}
