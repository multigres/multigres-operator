package cell

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

func TestBuildMultiGatewayDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tests := map[string]struct {
		cell    *multigresv1alpha1.Cell
		scheme  *runtime.Scheme
		want    *appsv1.Deployment
		wantErr bool
	}{
		"minimal spec - all defaults": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cell-multigateway",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "test-cell",
							UID:                "test-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(DefaultMultiGatewayReplicas),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: multigresv1alpha1.DefaultMultiGatewayImage,
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "zone1",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
						},
					},
				},
			},
		},
		"custom replicas": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-custom-replicas",
					Namespace: "test-ns",
					UID:       "custom-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone2",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
					MultiGateway: multigresv1alpha1.StatelessSpec{
						Replicas: ptr.To(int32(5)),
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-custom-replicas-multigateway",
					Namespace: "test-ns",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-custom-replicas",
							UID:                "custom-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(int32(5)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: multigresv1alpha1.DefaultMultiGatewayImage,
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "zone2",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
						},
					},
				},
			},
		},
		"custom image": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-custom-image",
					Namespace: "default",
					UID:       "image-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone3",
					Images: multigresv1alpha1.CellImages{
						MultiGateway: "custom/multigateway:v1.2.3",
					},
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-custom-image-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-custom-image",
							UID:                "image-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(DefaultMultiGatewayReplicas),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: "custom/multigateway:v1.2.3",
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "zone3",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
						},
					},
				},
			},
		},
		"with affinity": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-affinity",
					Namespace: "default",
					UID:       "affinity-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone4",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
					MultiGateway: multigresv1alpha1.StatelessSpec{
						Affinity: &corev1.Affinity{
							NodeAffinity: &corev1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
									NodeSelectorTerms: []corev1.NodeSelectorTerm{
										{
											MatchExpressions: []corev1.NodeSelectorRequirement{
												{
													Key:      "node-type",
													Operator: corev1.NodeSelectorOpIn,
													Values:   []string{"gateway"},
												},
											},
										},
									},
								},
							},
						},
					},
					MultiGatewayPlacement: &multigresv1alpha1.PodPlacementSpec{
						Tolerations: []corev1.Toleration{
							{
								Key:      "workload",
								Operator: corev1.TolerationOpEqual,
								Value:    "customer-pg",
								Effect:   corev1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-affinity-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-affinity",
							UID:                "affinity-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(DefaultMultiGatewayReplicas),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: multigresv1alpha1.DefaultMultiGatewayImage,
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "zone4",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
							Affinity: &corev1.Affinity{
								NodeAffinity: &corev1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
										NodeSelectorTerms: []corev1.NodeSelectorTerm{
											{
												MatchExpressions: []corev1.NodeSelectorRequirement{
													{
														Key:      "node-type",
														Operator: corev1.NodeSelectorOpIn,
														Values:   []string{"gateway"},
													},
												},
											},
										},
									},
								},
							},
							Tolerations: []corev1.Toleration{
								{
									Key:      "workload",
									Operator: corev1.TolerationOpEqual,
									Value:    "customer-pg",
									Effect:   corev1.TaintEffectNoSchedule,
								},
							},
						},
					},
				},
			},
		},
		"with resource requirements": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-resources",
					Namespace: "default",
					UID:       "resources-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone5",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
					MultiGateway: multigresv1alpha1.StatelessSpec{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-resources-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-resources",
							UID:                "resources-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(DefaultMultiGatewayReplicas),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: multigresv1alpha1.DefaultMultiGatewayImage,
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "zone5",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("500m"),
											corev1.ResourceMemory: resource.MustParse("512Mi"),
										},
									},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
						},
					},
				},
			},
		},
		"with pod labels and annotations": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-pod-labels",
					Namespace: "default",
					UID:       "pod-labels-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone-labels",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
					MultiGateway: multigresv1alpha1.StatelessSpec{
						PodLabels: map[string]string{
							"custom-label":   "custom-value",
							"team":           "platform",
							"sidecar-inject": "true",
						},
						PodAnnotations: map[string]string{
							"custom-annotation": "keep-me",
						},
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-pod-labels-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-pod-labels",
							UID:                "pod-labels-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(DefaultMultiGatewayReplicas),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"custom-label":                 "custom-value",
								"team":                         "platform",
								"sidecar-inject":               "true",
							},
							Annotations: map[string]string{
								"custom-annotation": "keep-me",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: multigresv1alpha1.DefaultMultiGatewayImage,
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "zone-labels",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
						},
					},
				},
			},
		},
		"pod labels cannot overwrite operator selector labels": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-override-labels",
					Namespace: "default",
					UID:       "override-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone-override",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
					MultiGateway: multigresv1alpha1.StatelessSpec{
						PodLabels: map[string]string{
							"app.kubernetes.io/component": "hacked",
							"multigres.com/cell":          "wrong-cell",
							"safe-label":                  "safe-value",
						},
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-override-labels-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-override-labels",
							UID:                "override-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(DefaultMultiGatewayReplicas),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"safe-label":                   "safe-value",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: multigresv1alpha1.DefaultMultiGatewayImage,
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "zone-override",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
						},
					},
				},
			},
		},
		"with topology labels": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-topology",
					Namespace: "default",
					UID:       "topo-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name:   "zone-topo",
					ZoneID: "use1-az1",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-topology-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/zone-id":        "use1-az1",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-topology",
							UID:                "topo-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(DefaultMultiGatewayReplicas),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/zone-id":        "use1-az1",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: multigresv1alpha1.DefaultMultiGatewayImage,
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "zone-topo",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
							NodeSelector: map[string]string{
								"topology.k8s.aws/zone-id": "use1-az1",
							},
						},
					},
				},
			},
		},
		"with region labels": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-region",
					Namespace: "default",
					UID:       "region-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name:   "region-cell",
					Region: "us-west-2",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			},
			scheme: scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-region-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/region":         "us-west-2",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-region",
							UID:                "region-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(DefaultMultiGatewayReplicas),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "multigateway",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "multigateway",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/region":         "us-west-2",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "multigateway",
									Image: multigresv1alpha1.DefaultMultiGatewayImage,
									Args: []string{
										"multigateway",
										"--http-port", "15100",
										"--grpc-port", "15170",
										"--pg-port", "5432",
										"--pg-replica-port", "5433",
										"--topo-global-server-addresses", "global-topo:2379",
										"--topo-global-root", "/multigres/global",
										"--cell", "region-cell",
										"--log-level", "info",
									},
									Resources: corev1.ResourceRequirements{},
									Ports: []corev1.ContainerPort{
										{
											Name:          "http",
											ContainerPort: MultiGatewayHTTPPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "grpc",
											ContainerPort: MultiGatewayGRPCPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "postgres",
											ContainerPort: MultiGatewayPostgresPort,
											Protocol:      corev1.ProtocolTCP,
										},
										{
											Name:          "pg-replica",
											ContainerPort: MultiGatewayPostgresReplicaPort,
											Protocol:      corev1.ProtocolTCP,
										},
									},
									StartupProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds:    5,
										FailureThreshold: 30,
									},
									LivenessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/live",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 10,
									},
									ReadinessProbe: &corev1.Probe{
										ProbeHandler: corev1.ProbeHandler{
											HTTPGet: &corev1.HTTPGetAction{
												Path: "/ready",
												Port: intstr.FromInt32(MultiGatewayHTTPPort),
											},
										},
										PeriodSeconds: 5,
									},
								},
							},
							NodeSelector: map[string]string{
								"topology.kubernetes.io/region": "us-west-2",
							},
						},
					},
				},
			},
		},
		"invalid scheme - should error": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			scheme:  runtime.NewScheme(), // empty scheme
			wantErr: true,
		},
	}

	// Calculate expected names dynamically to handle hashing
	buildName := func(cell *multigresv1alpha1.Cell) string {
		clusterName := cell.Labels["multigres.com/cluster"]
		// Deployment uses DefaultConstraints
		return name.JoinWithConstraints(
			name.DefaultConstraints,
			clusterName,
			string(cell.Spec.Name),
			"multigateway",
		)
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Update expected name in the want object
			if tc.want != nil {
				expectedName := buildName(tc.cell)
				tc.want.Name = expectedName
				if tc.want.Labels != nil {
					tc.want.Labels["app.kubernetes.io/instance"] = tc.cell.Labels["multigres.com/cluster"]
					tc.want.Labels["multigres.com/cell"] = string(tc.cell.Spec.Name)
				}
				if tc.want.Spec.Selector != nil {
					tc.want.Spec.Selector.MatchLabels["app.kubernetes.io/instance"] = tc.cell.Labels["multigres.com/cluster"]
					tc.want.Spec.Selector.MatchLabels["multigres.com/cell"] = string(
						tc.cell.Spec.Name,
					)
				}
				if tc.want.Spec.Template.Labels != nil {
					tc.want.Spec.Template.Labels["app.kubernetes.io/instance"] = tc.cell.Labels["multigres.com/cluster"]
					tc.want.Spec.Template.Labels["multigres.com/cell"] = string(tc.cell.Spec.Name)
				}
				if tc.want.Spec.Template.Annotations == nil {
					tc.want.Spec.Template.Annotations = map[string]string{}
				}
				tc.want.Spec.Template.Annotations[metadata.AnnotationProjectRef] = metadata.ResolveProjectRef(
					tc.cell.Annotations,
					tc.cell.Labels[metadata.LabelMultigresCluster],
				)
			}

			got, err := BuildMultiGatewayDeployment(tc.cell, tc.scheme)

			if (err != nil) != tc.wantErr {
				t.Errorf("BuildMultiGatewayDeployment() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildMultiGatewayDeployment() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildCellNodeSelector(t *testing.T) {
	tests := map[string]struct {
		spec multigresv1alpha1.CellSpec
		want map[string]string
	}{
		"zoneId only": {
			spec: multigresv1alpha1.CellSpec{ZoneID: "use1-az1"},
			want: map[string]string{"topology.k8s.aws/zone-id": "use1-az1"},
		},
		"region": {
			spec: multigresv1alpha1.CellSpec{Region: "us-west-2"},
			want: map[string]string{"topology.kubernetes.io/region": "us-west-2"},
		},
		"none": {
			spec: multigresv1alpha1.CellSpec{},
			want: nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cell := &multigresv1alpha1.Cell{Spec: tc.spec}
			if diff := cmp.Diff(tc.want, buildCellNodeSelector(cell)); diff != "" {
				t.Errorf("buildCellNodeSelector() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildMultiGatewayDeployment_ProjectRefAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tests := map[string]struct {
		annotations map[string]string
		want        string
	}{
		"falls back to cluster name": {
			want: "test-cluster",
		},
		"uses explicit project ref": {
			annotations: map[string]string{
				metadata.AnnotationProjectRef: "proj_123",
			},
			want: "proj_123",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cell := &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-cell",
					Namespace:   "default",
					UID:         "test-uid",
					Labels:      map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
					Annotations: tc.annotations,
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			}

			deploy, err := BuildMultiGatewayDeployment(cell, scheme)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got := deploy.Spec.Template.Annotations[metadata.AnnotationProjectRef]; got != tc.want {
				t.Fatalf("annotation %q = %q, want %q", metadata.AnnotationProjectRef, got, tc.want)
			}

			assertedLabels := map[string]string{
				metadata.LabelAppInstance:  "test-cluster",
				metadata.LabelAppComponent: MultiGatewayComponentName,
				metadata.LabelAppManagedBy: metadata.ManagedByMultigres,
			}
			for key, want := range assertedLabels {
				if got := deploy.Spec.Template.Labels[key]; got != want {
					t.Fatalf("label %q = %q, want %q", key, got, want)
				}
			}
		})
	}
}

func TestBuildMultiGatewayDeployment_OmitsPrometheusScrapeAnnotations(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cell",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone1",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},
			MultiGateway: multigresv1alpha1.StatelessSpec{
				PodAnnotations: map[string]string{
					metadata.AnnotationPrometheusScrape: "true",
					metadata.AnnotationPrometheusPort:   "15100",
					metadata.AnnotationPrometheusPath:   "/metrics",
					"custom-annotation":                 "keep-me",
				},
			},
		},
	}

	deploy, err := BuildMultiGatewayDeployment(cell, scheme)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := deploy.Spec.Template.Annotations[metadata.AnnotationPrometheusScrape]; ok {
		t.Fatalf("annotation %q should be omitted", metadata.AnnotationPrometheusScrape)
	}
	if _, ok := deploy.Spec.Template.Annotations[metadata.AnnotationPrometheusPort]; ok {
		t.Fatalf("annotation %q should be omitted", metadata.AnnotationPrometheusPort)
	}
	if _, ok := deploy.Spec.Template.Annotations[metadata.AnnotationPrometheusPath]; ok {
		t.Fatalf("annotation %q should be omitted", metadata.AnnotationPrometheusPath)
	}
	if got := deploy.Spec.Template.Annotations["custom-annotation"]; got != "keep-me" {
		t.Fatalf("custom annotation = %q, want %q", got, "keep-me")
	}
}

func TestBuildMultiGatewayService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tests := map[string]struct {
		cell    *multigresv1alpha1.Cell
		scheme  *runtime.Scheme
		want    *corev1.Service
		wantErr bool
	}{
		"minimal spec": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			scheme: scheme,
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "test-cell",
							UID:                "test-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeClusterIP,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "test-cluster",
						"app.kubernetes.io/component": "multigateway",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       MultiGatewayHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       MultiGatewayGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "postgres",
							Port:       MultiGatewayPostgresPort,
							TargetPort: intstr.FromString("postgres"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
		},
		"with different cell name": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "production-cell",
					Namespace: "prod-ns",
					UID:       "prod-uid",
					Labels:    map[string]string{"multigres.com/cluster": "prod-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "us-west",
				},
			},
			scheme: scheme,
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "production-cell-multigateway",
					Namespace: "prod-ns",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "prod-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "production-cell",
							UID:                "prod-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeClusterIP,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "prod-cluster",
						"app.kubernetes.io/component": "multigateway",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       MultiGatewayHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       MultiGatewayGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "postgres",
							Port:       MultiGatewayPostgresPort,
							TargetPort: intstr.FromString("postgres"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
		},
		"with topology labels": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-topology",
					Namespace: "default",
					UID:       "topo-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name:   "zone-topo",
					ZoneID: "use1-az1",
				},
			},
			scheme: scheme,
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-topology-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/zone-id":        "use1-az1",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-topology",
							UID:                "topo-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeClusterIP,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "test-cluster",
						"app.kubernetes.io/component": "multigateway",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       MultiGatewayHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       MultiGatewayGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "postgres",
							Port:       MultiGatewayPostgresPort,
							TargetPort: intstr.FromString("postgres"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
		},
		"with region labels": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-region",
					Namespace: "default",
					UID:       "region-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{
					Name:   "region-cell",
					Region: "us-west-2",
				},
			},
			scheme: scheme,
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cell-region-multigateway",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "multigateway",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/region":         "us-west-2",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Cell",
							Name:               "cell-region",
							UID:                "region-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeClusterIP,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "test-cluster",
						"app.kubernetes.io/component": "multigateway",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       MultiGatewayHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       MultiGatewayGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "postgres",
							Port:       MultiGatewayPostgresPort,
							TargetPort: intstr.FromString("postgres"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
		},
		"invalid scheme - should error": {
			cell: &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cell",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.CellSpec{
					Name: "zone1",
				},
			},
			scheme:  runtime.NewScheme(), // empty scheme
			wantErr: true,
		},
	}

	// Calculate expected names dynamically to handle hashing
	buildName := func(cell *multigresv1alpha1.Cell) string {
		clusterName := cell.Labels["multigres.com/cluster"]
		return name.JoinWithConstraints(
			name.ServiceConstraints,
			clusterName,
			string(cell.Spec.Name),
			"multigateway",
		)
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Update expected name in the want object
			if tc.want != nil {
				expectedName := buildName(tc.cell)
				tc.want.Name = expectedName
				if tc.want.Labels != nil {
					tc.want.Labels["app.kubernetes.io/instance"] = tc.cell.Labels["multigres.com/cluster"]
					tc.want.Labels["multigres.com/cell"] = string(tc.cell.Spec.Name)
				}
				if tc.want.Spec.Selector != nil {
					tc.want.Spec.Selector["app.kubernetes.io/instance"] = tc.cell.Labels["multigres.com/cluster"]
					tc.want.Spec.Selector["multigres.com/cell"] = string(tc.cell.Spec.Name)
				}
			}

			got, err := BuildMultiGatewayService(tc.cell, tc.scheme)

			if (err != nil) != tc.wantErr {
				t.Errorf("BuildMultiGatewayService() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildMultiGatewayService() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildMultiGatewayDeployment_Observability(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cellObj := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-otel",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			Annotations: map[string]string{
				metadata.AnnotationProjectRef: "project-ref-123",
			},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "zone-otel",
			Observability: &multigresv1alpha1.ObservabilityConfig{
				OTLPMetricsEndpoint: "http://vmagent:4318/v1/metrics",
				MetricsExporter:     "otlp",
				SamplingConfigRef: &multigresv1alpha1.SamplingConfigRef{
					Name: "otel-sampler-config",
				},
			},
		},
	}
	deploy, err := BuildMultiGatewayDeployment(cellObj, scheme)
	if err != nil {
		t.Fatalf("BuildMultiGatewayDeployment failed: %v", err)
	}
	if len(deploy.Spec.Template.Spec.Volumes) == 0 {
		t.Errorf("expected volumes to contain otel config")
	}
	env := deploy.Spec.Template.Spec.Containers[0].Env
	assertMultiGatewayEnvVar(
		t,
		env,
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"http://vmagent:4318/v1/metrics",
	)
	assertMultiGatewayEnvVar(t, env, "OTEL_METRICS_EXPORTER", "otlp")
	assertMultiGatewayResourceAttribute(t, env, "multigres.project=project-ref-123")
	assertMultiGatewayResourceAttribute(t, env, "multigres.cluster=test-cluster")
	assertMultiGatewayResourceAttribute(t, env, "multigres.component=multigateway")
}

func assertMultiGatewayEnvVar(t *testing.T, envVars []corev1.EnvVar, name, want string) {
	t.Helper()
	for _, envVar := range envVars {
		if envVar.Name == name {
			if envVar.Value != want {
				t.Fatalf("env var %q = %q, want %q", name, envVar.Value, want)
			}
			return
		}
	}
	t.Fatalf("expected env var %q, got none", name)
}

func assertMultiGatewayResourceAttribute(t *testing.T, envVars []corev1.EnvVar, want string) {
	t.Helper()
	for _, envVar := range envVars {
		if envVar.Name == "OTEL_RESOURCE_ATTRIBUTES" {
			if !strings.Contains(envVar.Value, want) {
				t.Fatalf("OTEL_RESOURCE_ATTRIBUTES = %q, want it to contain %q", envVar.Value, want)
			}
			return
		}
	}
	t.Fatalf("expected OTEL_RESOURCE_ATTRIBUTES to contain %q, got none", want)
}

func TestBuildMultiGatewayDeployment_TLS(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	t.Run("TLS enabled with certCommonName", func(t *testing.T) {
		cellObj := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-tls",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: multigresv1alpha1.CellSpec{
				Name:           "zone-tls",
				CertCommonName: "db.abc123.supabase.red",
				GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
					Address:  "global-topo:2379",
					RootPath: "/multigres/global",
				},
				LogLevels: multigresv1alpha1.ComponentLogLevels{
					Multigateway: "info",
				},
			},
		}
		deploy, err := BuildMultiGatewayDeployment(cellObj, scheme)
		if err != nil {
			t.Fatalf("BuildMultiGatewayDeployment failed: %v", err)
		}

		// Verify TLS volume exists
		var foundVol bool
		for _, v := range deploy.Spec.Template.Spec.Volumes {
			if v.Name == tlsVolumeName {
				foundVol = true
				if v.Secret == nil {
					t.Error("TLS volume should use a Secret source")
				} else if v.Secret.SecretName != multigresv1alpha1.CertSecretName {
					t.Errorf(
						"TLS volume secretName = %q, want %q",
						v.Secret.SecretName, multigresv1alpha1.CertSecretName,
					)
				}
			}
		}
		if !foundVol {
			t.Errorf("expected TLS volume %q in pod spec", tlsVolumeName)
		}

		// Verify TLS volumeMount exists
		container := deploy.Spec.Template.Spec.Containers[0]
		var foundMount bool
		for _, m := range container.VolumeMounts {
			if m.Name == tlsVolumeName {
				foundMount = true
				if m.MountPath != tlsMountPath {
					t.Errorf("TLS mount path = %q, want %q", m.MountPath, tlsMountPath)
				}
				if !m.ReadOnly {
					t.Error("TLS mount should be readOnly")
				}
			}
		}
		if !foundMount {
			t.Errorf("expected TLS volumeMount %q in container", tlsVolumeName)
		}

		// Verify TLS args are appended
		args := container.Args
		wantArgs := []string{
			"--pg-tls-cert-file", tlsCertFile,
			"--pg-tls-key-file", tlsKeyFile,
		}
		// The TLS args should be the last 4 args
		if len(args) < 4 {
			t.Fatalf("expected at least 4 args, got %d", len(args))
		}
		tailArgs := args[len(args)-4:]
		if diff := cmp.Diff(wantArgs, tailArgs); diff != "" {
			t.Errorf("TLS args mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("TLS disabled without certCommonName", func(t *testing.T) {
		cellObj := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-no-tls",
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: multigresv1alpha1.CellSpec{
				Name: "zone-no-tls",
				GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
					Address:  "global-topo:2379",
					RootPath: "/multigres/global",
				},
				LogLevels: multigresv1alpha1.ComponentLogLevels{
					Multigateway: "info",
				},
			},
		}
		deploy, err := BuildMultiGatewayDeployment(cellObj, scheme)
		if err != nil {
			t.Fatalf("BuildMultiGatewayDeployment failed: %v", err)
		}

		// No TLS volume should exist
		for _, v := range deploy.Spec.Template.Spec.Volumes {
			if v.Name == tlsVolumeName {
				t.Error("TLS volume should not be present when CertCommonName is empty")
			}
		}

		// No TLS args should be present
		container := deploy.Spec.Template.Spec.Containers[0]
		for _, arg := range container.Args {
			if arg == "--pg-tls-cert-file" || arg == "--pg-tls-key-file" {
				t.Errorf("TLS arg %q should not be present when CertCommonName is empty", arg)
			}
		}
	})
}
