package toposerver

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resource-handler/controller/storage"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/pvc"
)

const (
	// ComponentName is the component label value for toposerver resources
	ComponentName = metadata.ComponentTopoServer

	// DefaultReplicas is the default number of etcd replicas
	DefaultReplicas int32 = 3

	// DefaultStorageSize is the default storage size for etcd data
	DefaultStorageSize = "10Gi"

	// DataVolumeName is the name of the data volume
	DataVolumeName = "data"

	// DataMountPath is the mount path for etcd data
	DataMountPath = "/var/lib/etcd"
)

// BuildStatefulSet creates a StatefulSet for the TopoServer cluster.
// Returns a deterministic StatefulSet based on the TopoServer spec.
func BuildStatefulSet(
	toposerver *multigresv1alpha1.TopoServer,
	scheme *runtime.Scheme,
) (*appsv1.StatefulSet, error) {
	// Topo PVCs are only ever removed via the MultigresCluster deletion
	// finalizer (handleDeletion), never via the StatefulSet's native
	// retention policy, deleting or recreating just this StatefulSet must
	// never drop etcd data. WhenScaled still follows the resolved policy.
	stsRetentionPolicy := &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.RetainPVCRetentionPolicy,
	}
	if toposerver.Spec.PVCDeletionPolicy != nil {
		stsRetentionPolicy.WhenScaled = toposerver.Spec.PVCDeletionPolicy.WhenScaled
	}
	replicas := DefaultReplicas
	if toposerver.Spec.Etcd != nil && toposerver.Spec.Etcd.Replicas != nil {
		replicas = *toposerver.Spec.Etcd.Replicas
	}

	image := multigresv1alpha1.DefaultEtcdImage
	if toposerver.Spec.Etcd != nil && toposerver.Spec.Etcd.Image != "" {
		image = string(toposerver.Spec.Etcd.Image)
	}

	headlessServiceName := toposerver.Name + "-headless"
	// TODO: Support cell-local TopoServers by adding CellName field to TopoServerSpec
	// For now, TopoServer is always global topology
	clusterName := toposerver.Labels["multigres.com/cluster"]
	labels := metadata.BuildStandardLabels(clusterName, ComponentName)
	labels = metadata.MergeLabels(labels, toposerver.GetObjectMeta().GetLabels())

	resources := corev1.ResourceRequirements{}
	if toposerver.Spec.Etcd != nil {
		resources = toposerver.Spec.Etcd.Resources
	}

	// Defensive copy so the built StatefulSet does not alias the TopoServer spec.
	var placement *multigresv1alpha1.TopoServerPlacementSpec
	if toposerver.Spec.Placement != nil {
		placement = toposerver.Spec.Placement.DeepCopy()
	} else {
		placement = &multigresv1alpha1.TopoServerPlacementSpec{}
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toposerver.Name,
			Namespace: toposerver.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: headlessServiceName,
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: metadata.GetSelectorLabels(labels),
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:      "etcd",
							Image:     image,
							Resources: resources,
							Env: buildContainerEnv(
								toposerver.Name,
								toposerver.Namespace,
								replicas,
								headlessServiceName,
							),
							Ports: buildContainerPorts(toposerver),
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      DataVolumeName,
									MountPath: DataMountPath,
								},
							},
							StartupProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/readyz",
										Port: intstr.FromInt32(MetricsPort),
									},
								},
								PeriodSeconds:    5,
								FailureThreshold: 30,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/livez",
										Port: intstr.FromInt32(MetricsPort),
									},
								},
								PeriodSeconds: 10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/readyz",
										Port: intstr.FromInt32(MetricsPort),
									},
								},
								PeriodSeconds: 5,
							},
						},
					},
					NodeSelector:              placement.NodeSelector,
					Affinity:                  placement.Affinity,
					Tolerations:               placement.Tolerations,
					TopologySpreadConstraints: placement.TopologySpreadConstraints,
				},
			},
			VolumeClaimTemplates:                 buildVolumeClaimTemplates(toposerver),
			PersistentVolumeClaimRetentionPolicy: pvc.BuildRetentionPolicy(stsRetentionPolicy),
		},
	}

	if err := ctrl.SetControllerReference(toposerver, sts, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return sts, nil
}

// buildVolumeClaimTemplates creates the PVC templates for etcd data storage.
func buildVolumeClaimTemplates(
	toposerver *multigresv1alpha1.TopoServer,
) []corev1.PersistentVolumeClaim {
	var storageClass *string
	storageSize := DefaultStorageSize
	var accessModes []corev1.PersistentVolumeAccessMode

	if toposerver.Spec.Etcd != nil {
		if toposerver.Spec.Etcd.Storage.Class != "" {
			storageClass = &toposerver.Spec.Etcd.Storage.Class
		}
		if toposerver.Spec.Etcd.Storage.Size != "" {
			storageSize = toposerver.Spec.Etcd.Storage.Size
		}
		accessModes = toposerver.Spec.Etcd.Storage.AccessModes
	}

	return []corev1.PersistentVolumeClaim{
		storage.BuildPVCTemplate(DataVolumeName, storageClass, storageSize, accessModes),
	}
}
