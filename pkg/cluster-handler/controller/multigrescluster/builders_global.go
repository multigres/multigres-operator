package multigrescluster

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	globalMultigatewayReplicaPort       int32 = 5433
	globalMultigatewayReplicaTargetPort       = "pg-replica"
)

// BuildGlobalTopoServer constructs the desired TopoServer for the global topology.
// Note: We do NOT use safe hashing here because GlobalTopo is a singleton resource
// per cluster with a predictable name pattern that is unlikely to exceed length limits.
// It returns nil, nil if the spec does not require an Etcd server (e.g. external).
func BuildGlobalTopoServer(
	cluster *multigresv1alpha1.MultigresCluster,
	spec *multigresv1alpha1.GlobalTopoServerSpec,
	scheme *runtime.Scheme,
) (*multigresv1alpha1.TopoServer, error) {
	if spec == nil {
		return nil, nil
	}

	// Only build TopoServer for etcd-based backends. External topo servers are managed externally.
	if spec.Etcd == nil {
		return nil, nil
	}

	labels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentGlobalTopo)
	metadata.AddClusterLabel(labels, cluster.Name)

	// Merge hierarchy: Etcd → GlobalTopoServer → MultigresCluster

	// 1. Merge Cluster and GlobalTopo
	mergedGlobal := multigresv1alpha1.MergePVCDeletionPolicy(
		spec.PVCDeletionPolicy,
		cluster.Spec.PVCDeletionPolicy,
	)

	// 2. Merge Etcd and Result 1
	var etcdPolicy *multigresv1alpha1.PVCDeletionPolicy
	if spec.Etcd != nil {
		etcdPolicy = spec.Etcd.PVCDeletionPolicy
	}

	finalPolicy := multigresv1alpha1.MergePVCDeletionPolicy(etcdPolicy, mergedGlobal)

	ts := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-global-topo",
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{
				Image:     spec.Etcd.Image,
				Replicas:  spec.Etcd.Replicas,
				Storage:   spec.Etcd.Storage,
				Resources: spec.Etcd.Resources,
				RootPath:  spec.Etcd.RootPath,
			},
			PVCDeletionPolicy: finalPolicy,
		},
	}

	if err := controllerutil.SetControllerReference(cluster, ts, scheme); err != nil {
		return nil, err
	}

	return ts, nil
}

// BuildMultiadminDeployment constructs the desired Multiadmin Deployment.
// Note: We do NOT use safe hashing here because Multiadmin is a singleton resource
// per cluster with a predictable name pattern that is unlikely to exceed length limits.
func BuildMultiadminDeployment(
	cluster *multigresv1alpha1.MultigresCluster,
	spec *multigresv1alpha1.StatelessSpec,
	scheme *runtime.Scheme,
) (*appsv1.Deployment, error) {
	standardLabels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentMultiadmin)
	metadata.AddClusterLabel(standardLabels, cluster.Name)

	// Merge with user provided pod labels, but standard labels take precedence
	podLabels := metadata.MergeLabels(standardLabels, spec.PodLabels)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-multiadmin", cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    standardLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: spec.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: metadata.GetSelectorLabels(standardLabels),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: spec.PodAnnotations,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: cluster.Spec.Images.ImagePullSecrets,
					Containers: []corev1.Container{
						{
							Name:  "multiadmin",
							Image: string(cluster.Spec.Images.Multiadmin),
							Command: []string{
								"/multigres/bin/multiadmin",
							},
							Args: []string{
								"--http-port=18000",
								"--grpc-port=18070",
								fmt.Sprintf(
									"--topo-global-server-addresses=%s-global-topo.%s.svc:2379",
									cluster.Name,
									cluster.Namespace,
								),
								"--topo-global-root=/multigres/global",
								"--service-map=grpc-multiadmin",
								"--pprof-http=true",
								"--log-level=" + string(cluster.Spec.LogLevels.Multiadmin),
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: 18000,
									Protocol:      corev1.ProtocolTCP,
								},
								{
									Name:          "grpc",
									ContainerPort: 18070,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Resources: spec.Resources,
							Env: multigresv1alpha1.BuildOTELEnvVars(
								cluster.Spec.Observability,
							),
							StartupProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/ready",
										Port: intstr.FromInt(18000),
									},
								},
								PeriodSeconds:    5,
								FailureThreshold: 30,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/live",
										Port: intstr.FromInt(18000),
									},
								},
								PeriodSeconds: 10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/ready",
										Port: intstr.FromInt(18000),
									},
								},
								PeriodSeconds: 5,
							},
						},
					},
					Affinity: spec.Affinity,
				},
			},
		},
	}

	if otelVol, otelMount := multigresv1alpha1.BuildOTELSamplingVolume(
		cluster.Spec.Observability,
	); otelVol != nil {
		podSpec := &deploy.Spec.Template.Spec
		podSpec.Volumes = append(podSpec.Volumes, *otelVol)
		podSpec.Containers[0].VolumeMounts = append(podSpec.Containers[0].VolumeMounts, *otelMount)
	}

	if err := controllerutil.SetControllerReference(cluster, deploy, scheme); err != nil {
		return nil, err
	}

	return deploy, nil
}

// BuildMultiadminService constructs the desired Service for Multiadmin.
func BuildMultiadminService(
	cluster *multigresv1alpha1.MultigresCluster,
	scheme *runtime.Scheme,
) (*corev1.Service, error) {
	standardLabels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentMultiadmin)
	metadata.AddClusterLabel(standardLabels, cluster.Name)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-multiadmin", cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    standardLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector: metadata.GetSelectorLabels(standardLabels),
			Type:     corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       18000,
					TargetPort: intstr.FromInt(18000),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "grpc",
					Port:       18070,
					TargetPort: intstr.FromInt(18070),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, svc, scheme); err != nil {
		return nil, err
	}

	return svc, nil
}

// BuildMultiadminWebDeployment constructs the desired MultiadminWeb Deployment.
func BuildMultiadminWebDeployment(
	cluster *multigresv1alpha1.MultigresCluster,
	spec *multigresv1alpha1.StatelessSpec,
	scheme *runtime.Scheme,
) (*appsv1.Deployment, error) {
	standardLabels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentMultiadminWeb)
	metadata.AddClusterLabel(standardLabels, cluster.Name)

	// Merge with user provided pod labels, but standard labels take precedence
	podLabels := metadata.MergeLabels(standardLabels, spec.PodLabels)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-multiadmin-web", cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    standardLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: spec.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: metadata.GetSelectorLabels(standardLabels),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: spec.PodAnnotations,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: cluster.Spec.Images.ImagePullSecrets,
					Containers: []corev1.Container{
						{
							Name:  "multiadmin-web",
							Image: string(cluster.Spec.Images.MultiadminWeb),
							Env: []corev1.EnvVar{
								{
									Name:  "HOSTNAME",
									Value: "::",
								},
								{
									Name:  "MULTIADMIN_API_URL",
									Value: fmt.Sprintf("http://%s-multiadmin:18000", cluster.Name),
								},
								{
									Name:  "POSTGRES_HOST",
									Value: fmt.Sprintf("%s-multigateway", cluster.Name),
								},
								{
									Name:  "POSTGRES_PORT",
									Value: "5432",
								},
								{
									Name:  "POSTGRES_DATABASE",
									Value: "postgres",
								},
								{
									Name: "POSTGRES_USER",
									Value: postgresSuperuserOrDefault(
										cluster.Spec.PostgresSuperuser,
									),
								},
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: 18100,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Resources: spec.Resources,
							StartupProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/",
										Port: intstr.FromInt(18100),
									},
								},
								PeriodSeconds:    5,
								FailureThreshold: 30,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/",
										Port: intstr.FromInt(18100),
									},
								},
								PeriodSeconds: 10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{
										Path: "/",
										Port: intstr.FromInt(18100),
									},
								},
								PeriodSeconds: 5,
							},
						},
					},
					Affinity: spec.Affinity,
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, deploy, scheme); err != nil {
		return nil, err
	}

	return deploy, nil
}

// BuildMultiadminWebService constructs the desired Service for MultiadminWeb.
func BuildMultiadminWebService(
	cluster *multigresv1alpha1.MultigresCluster,
	extAW *multigresv1alpha1.ExternalAdminWebConfig,
	scheme *runtime.Scheme,
) (*corev1.Service, error) {
	standardLabels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentMultiadminWeb)
	metadata.AddClusterLabel(standardLabels, cluster.Name)

	var annotations map[string]string
	var externalIPs []string
	if extAW != nil && extAW.Enabled {
		if len(extAW.ExternalIPs) > 0 {
			externalIPs = make([]string, len(extAW.ExternalIPs))
			for i, ip := range extAW.ExternalIPs {
				externalIPs[i] = string(ip)
			}
		}
		if len(extAW.Annotations) > 0 {
			annotations = make(map[string]string, len(extAW.Annotations))
			for k, v := range extAW.Annotations {
				annotations[k] = v
			}
		}
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-multiadmin-web", cluster.Name),
			Namespace:   cluster.Namespace,
			Labels:      standardLabels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Selector:    metadata.GetSelectorLabels(standardLabels),
			Type:        corev1.ServiceTypeClusterIP,
			ExternalIPs: externalIPs,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       18100,
					TargetPort: intstr.FromInt(18100),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, svc, scheme); err != nil {
		return nil, err
	}

	return svc, nil
}

// BuildMultigatewayGlobalService constructs a cluster-level Service that selects
// all multigateway pods across all cells. This provides a stable DNS name for
// multiadmin-web to connect to PostgreSQL via any available multigateway,
// regardless of which cells exist.
func BuildMultigatewayGlobalService(
	cluster *multigresv1alpha1.MultigresCluster,
	extGw *multigresv1alpha1.ExternalGatewayConfig,
	scheme *runtime.Scheme,
) (*corev1.Service, error) {
	labels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentMultigateway)
	metadata.AddClusterLabel(labels, cluster.Name)

	// The selector intentionally omits the cell label so it matches
	// multigateway pods from all cells in this cluster.
	selector := map[string]string{
		metadata.LabelAppComponent: metadata.ComponentMultigateway,
		metadata.LabelAppInstance:  cluster.Name,
	}

	svcType := corev1.ServiceTypeClusterIP
	var annotations map[string]string
	var externalIPs []string
	if extGw != nil && extGw.Enabled {
		if len(extGw.ExternalIPs) > 0 {
			externalIPs = make([]string, len(extGw.ExternalIPs))
			for i, ip := range extGw.ExternalIPs {
				externalIPs[i] = string(ip)
			}
		}
		if len(extGw.Annotations) > 0 {
			annotations = make(map[string]string, len(extGw.Annotations))
			for k, v := range extGw.Annotations {
				annotations[k] = v
			}
		}
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-multigateway", cluster.Name),
			Namespace:   cluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Selector:    selector,
			Type:        svcType,
			ExternalIPs: externalIPs,
			Ports: []corev1.ServicePort{
				{
					Name:       "postgres",
					Port:       5432,
					TargetPort: intstr.FromString("postgres"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, svc, scheme); err != nil {
		return nil, err
	}

	return svc, nil
}

// BuildMultigatewayGlobalReplicaService constructs a cluster-level Service that
// selects all multigateway pods across all cells and exposes the replica-read endpoint.
func BuildMultigatewayGlobalReplicaService(
	cluster *multigresv1alpha1.MultigresCluster,
	scheme *runtime.Scheme,
) (*corev1.Service, error) {
	labels := metadata.BuildStandardLabels(cluster.Name, metadata.ComponentMultigateway)
	metadata.AddClusterLabel(labels, cluster.Name)

	selector := map[string]string{
		metadata.LabelAppComponent: metadata.ComponentMultigateway,
		metadata.LabelAppInstance:  cluster.Name,
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-multigateway-replica", cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: selector,
			Type:     corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       "pg-replica",
					Port:       globalMultigatewayReplicaPort,
					TargetPort: intstr.FromString(globalMultigatewayReplicaTargetPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, svc, scheme); err != nil {
		return nil, err
	}

	return svc, nil
}

// postgresSuperuserOrDefault returns the configured superuser name, or the
// upstream default "postgres" when unset.
func postgresSuperuserOrDefault(name string) string {
	if name == "" {
		return "postgres"
	}
	return name
}
