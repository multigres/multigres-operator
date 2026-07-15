package cell

import (
	"fmt"
	"maps"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

const (
	// MultigatewayComponentName is the component label value for Multigateway resources
	MultigatewayComponentName = metadata.ComponentMultigateway

	// DefaultMultigatewayReplicas is the default number of Multigateway replicas
	DefaultMultigatewayReplicas int32 = 1

	// MultigatewayHTTPPort is the default port for HTTP connections
	MultigatewayHTTPPort int32 = 15100

	// MultigatewayGRPCPort is the default port for GRPC connections
	MultigatewayGRPCPort int32 = 15170

	// MultigatewayPostgresPort is the port for database connections,
	// used by both the container and the Kubernetes Service.
	MultigatewayPostgresPort int32 = 5432

	// MultigatewayPostgresReplicaPort is the default port for replica-read database connections.
	MultigatewayPostgresReplicaPort int32 = 5433

	// TLS volume/mount constants for the multigateway cert-manager certificate.
	tlsVolumeName = "tls-certs"
	tlsMountPath  = "/etc/multigateway/tls"
	tlsCertFile   = tlsMountPath + "/tls.crt"
	tlsKeyFile    = tlsMountPath + "/tls.key"
)

// BuildMultigatewayDeploymentName generates the Deployment name.
// It uses DefaultConstraints (253 chars) to use readable long names.
func BuildMultigatewayDeploymentName(cell *multigresv1alpha1.Cell) string {
	clusterName := cell.Labels["multigres.com/cluster"]
	return name.JoinWithConstraints(
		name.DefaultConstraints,
		clusterName,
		string(cell.Spec.Name),
		"multigateway",
	)
}

// BuildMultigatewayServiceName generates the Service name.
// It uses ServiceConstraints (63 chars) for DNS safety.
func BuildMultigatewayServiceName(cell *multigresv1alpha1.Cell) string {
	clusterName := cell.Labels["multigres.com/cluster"]
	return name.JoinWithConstraints(
		name.ServiceConstraints,
		clusterName,
		string(cell.Spec.Name),
		"multigateway",
	)
}

// BuildMultigatewayDeployment creates a Deployment for the Multigateway component.
func BuildMultigatewayDeployment(
	cell *multigresv1alpha1.Cell,
	scheme *runtime.Scheme,
) (*appsv1.Deployment, error) {
	replicas := DefaultMultigatewayReplicas
	if cell.Spec.Multigateway.Replicas != nil {
		replicas = *cell.Spec.Multigateway.Replicas
	}

	image := multigresv1alpha1.DefaultMultigatewayImage
	if cell.Spec.Images.Multigateway != "" {
		image = string(cell.Spec.Images.Multigateway)
	}

	name := BuildMultigatewayDeploymentName(cell)
	clusterName := cell.Labels["multigres.com/cluster"]
	labels := metadata.BuildStandardLabels(clusterName, MultigatewayComponentName)
	annotations := maps.Clone(cell.Spec.Multigateway.PodAnnotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	delete(annotations, metadata.AnnotationPrometheusScrape)
	delete(annotations, metadata.AnnotationPrometheusPort)
	delete(annotations, metadata.AnnotationPrometheusPath)
	annotations[metadata.AnnotationProjectRef] = metadata.ResolveProjectRef(
		cell.Annotations,
		clusterName,
	)
	metadata.AddCellLabel(labels, cell.Spec.Name)
	if cell.Spec.ZoneID != "" {
		metadata.AddZoneIDLabel(labels, cell.Spec.ZoneID)
	}
	if cell.Spec.Region != "" {
		metadata.AddRegionLabel(labels, cell.Spec.Region)
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cell.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: metadata.GetSelectorLabels(labels),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      metadata.MergeLabels(labels, cell.Spec.Multigateway.PodLabels),
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "multigateway",
							Image: image,
							Args: []string{
								"multigateway",
								"--http-port",
								fmt.Sprintf("%d", MultigatewayHTTPPort),
								"--grpc-port",
								fmt.Sprintf("%d", MultigatewayGRPCPort),
								"--pg-port",
								fmt.Sprintf("%d", MultigatewayPostgresPort),
								"--pg-replica-port",
								fmt.Sprintf("%d", MultigatewayPostgresReplicaPort),
								"--topo-global-server-addresses",
								cell.Spec.GlobalTopoServer.Address,
								"--topo-global-root",
								cell.Spec.GlobalTopoServer.RootPath,
								"--cell",
								string(cell.Spec.Name),
								"--log-level",
								string(cell.Spec.LogLevels.Multigateway),
							},
							Resources: cell.Spec.Multigateway.Resources,
							Env: multigresv1alpha1.BuildOTELEnvVarsWithResourceAttributes(
								cell.Spec.Observability,
								map[string]string{
									"multigres.project": metadata.ResolveProjectRef(
										cell.Annotations,
										clusterName,
									),
									"multigres.cluster":   clusterName,
									"multigres.component": MultigatewayComponentName,
								},
							),
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: MultigatewayHTTPPort,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "grpc",
									ContainerPort: MultigatewayGRPCPort,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "postgres",
									ContainerPort: MultigatewayPostgresPort,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "pg-replica",
									ContainerPort: MultigatewayPostgresReplicaPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							StartupProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/ready",
										Port: intstr.FromInt32(MultigatewayHTTPPort),
									},
								},
								PeriodSeconds:    5,
								FailureThreshold: 30,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/live",
										Port: intstr.FromInt32(MultigatewayHTTPPort),
									},
								},
								PeriodSeconds: 10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/ready",
										Port: intstr.FromInt32(MultigatewayHTTPPort),
									},
								},
								PeriodSeconds: 5,
							},
						},
					},
					Affinity:     cell.Spec.Multigateway.Affinity,
					Tolerations:  tolerationsFromPlacement(cell.Spec.MultigatewayPlacement),
					NodeSelector: buildCellNodeSelector(cell),
				},
			},
		},
	}

	if otelVol, otelMount := multigresv1alpha1.BuildOTELSamplingVolume(
		cell.Spec.Observability,
	); otelVol != nil {
		podSpec := &deployment.Spec.Template.Spec
		podSpec.Volumes = append(podSpec.Volumes, *otelVol)
		podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, *otelMount)
	}

	// Mount TLS certificate and add flags when CertCommonName is configured.
	if cell.Spec.CertCommonName != "" {
		podSpec := &deployment.Spec.Template.Spec
		defaultMode := int32(0o440)
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: tlsVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  multigresv1alpha1.CertSecretName,
					DefaultMode: &defaultMode,
				},
			},
		})
		podSpec.Containers[0].VolumeMounts = append(
			podSpec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      tlsVolumeName,
				MountPath: tlsMountPath,
				ReadOnly:  true,
			},
		)
		podSpec.Containers[0].Args = append(podSpec.Containers[0].Args,
			"--pg-tls-cert-file", tlsCertFile,
			"--pg-tls-key-file", tlsKeyFile,
		)
	}

	if err := ctrl.SetControllerReference(cell, deployment, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return deployment, nil
}

// BuildMultigatewayService creates a Service for the Multigateway component.
func BuildMultigatewayService(
	cell *multigresv1alpha1.Cell,
	scheme *runtime.Scheme,
) (*corev1.Service, error) {
	name := BuildMultigatewayServiceName(cell)
	clusterName := cell.Labels["multigres.com/cluster"]
	labels := metadata.BuildStandardLabels(clusterName, MultigatewayComponentName)
	metadata.AddCellLabel(labels, cell.Spec.Name)
	if cell.Spec.ZoneID != "" {
		metadata.AddZoneIDLabel(labels, cell.Spec.ZoneID)
	}
	if cell.Spec.Region != "" {
		metadata.AddRegionLabel(labels, cell.Spec.Region)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cell.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: metadata.GetSelectorLabels(labels),
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       MultigatewayHTTPPort,
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "grpc",
					Port:       MultigatewayGRPCPort,
					TargetPort: intstr.FromString("grpc"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "postgres",
					Port:       MultigatewayPostgresPort,
					TargetPort: intstr.FromString("postgres"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(cell, svc, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return svc, nil
}

// buildCellNodeSelector returns a nodeSelector map for the cell's topology.
// buildCellNodeSelector returns a nodeSelector map for the cell's topology.
// Returns nil if no topology is set.
func buildCellNodeSelector(cell *multigresv1alpha1.Cell) map[string]string {
	switch {
	case cell.Spec.ZoneID != "":
		return map[string]string{
			metadata.NodeLabelZoneID: string(cell.Spec.ZoneID),
		}
	case cell.Spec.Region != "":
		return map[string]string{
			"topology.kubernetes.io/region": string(cell.Spec.Region),
		}
	default:
		return nil
	}
}
