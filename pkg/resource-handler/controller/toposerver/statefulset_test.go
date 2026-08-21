package toposerver

import (
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
)

func TestBuildStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tests := map[string]struct {
		toposerver *multigresv1alpha1.TopoServer
		scheme     *runtime.Scheme
		want       *appsv1.StatefulSet
		wantErr    bool
	}{
		"minimal spec - all defaults": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			scheme: scheme,
			want: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "toposerver",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "test-cluster",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "TopoServer",
							Name:               "test-toposerver",
							UID:                "test-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					ServiceName: "test-toposerver-headless",
					Replicas:    ptr.To(int32(3)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "toposerver",
							"multigres.com/cluster":       "test-cluster",
						},
					},
					PodManagementPolicy: appsv1.ParallelPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
					},
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "toposerver",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "test-cluster",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:      "etcd",
									Image:     multigresv1alpha1.DefaultEtcdImage,
									Resources: corev1.ResourceRequirements{},
									Env: buildContainerEnv(
										"test-toposerver",
										"default",
										3,
										"test-toposerver-headless",
									),
									Ports: buildContainerPorts(nil), // Default
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
						},
					},
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: DataVolumeName,
							},
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteOnce,
								},
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: resource.MustParse(
											DefaultStorageSize,
										),
									},
								},
							},
						},
					},
				},
			},
		},
		"with placement": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Placement: &multigresv1alpha1.TopoServerPlacementSpec{
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
			want: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "toposerver",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "test-cluster",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "TopoServer",
							Name:               "test-toposerver",
							UID:                "test-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					ServiceName: "test-toposerver-headless",
					Replicas:    ptr.To(int32(3)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "toposerver",
							"multigres.com/cluster":       "test-cluster",
						},
					},
					PodManagementPolicy: appsv1.ParallelPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
					},
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "toposerver",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "test-cluster",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:      "etcd",
									Image:     multigresv1alpha1.DefaultEtcdImage,
									Resources: corev1.ResourceRequirements{},
									Env: buildContainerEnv(
										"test-toposerver",
										"default",
										3,
										"test-toposerver-headless",
									),
									Ports: buildContainerPorts(nil), // Default
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
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: DataVolumeName,
							},
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteOnce,
								},
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: resource.MustParse(
											DefaultStorageSize,
										),
									},
								},
							},
						},
					},
				},
			},
		},
		"custom replicas and image": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "toposerver-custom",
					Namespace: "test",
					UID:       "custom-uid",
					Labels:    map[string]string{"multigres.com/cluster": "custom-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Replicas: ptr.To(int32(5)),
						Image:    "quay.io/coreos/etcd:v3.5.15",
					},
				},
			},
			scheme: scheme,
			want: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "toposerver-custom",
					Namespace: "test",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "custom-cluster",
						"app.kubernetes.io/component":  "toposerver",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "custom-cluster",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "TopoServer",
							Name:               "toposerver-custom",
							UID:                "custom-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					ServiceName: "toposerver-custom-headless",
					Replicas:    ptr.To(int32(5)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "custom-cluster",
							"app.kubernetes.io/component": "toposerver",
							"multigres.com/cluster":       "custom-cluster",
						},
					},
					PodManagementPolicy: appsv1.ParallelPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
					},
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "custom-cluster",
								"app.kubernetes.io/component":  "toposerver",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "custom-cluster",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:      "etcd",
									Image:     "quay.io/coreos/etcd:v3.5.15",
									Resources: corev1.ResourceRequirements{},
									Env: buildContainerEnv(
										"toposerver-custom",
										"test",
										5,
										"toposerver-custom-headless",
									),
									Ports: buildContainerPorts(nil),
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
						},
					},
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: DataVolumeName,
							},
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteOnce,
								},
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: resource.MustParse(
											DefaultStorageSize,
										),
									},
								},
							},
						},
					},
				},
			},
		},
		"custom VolumeClaimTemplate": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{
						Storage: multigresv1alpha1.StorageSpec{
							Size:  "50Gi",
							Class: "fast-ssd",
						},
					},
				},
			},
			scheme: scheme,
			want: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  "toposerver",
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "test-cluster",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "TopoServer",
							Name:               "test-toposerver",
							UID:                "test-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.StatefulSetSpec{
					ServiceName: "test-toposerver-headless",
					Replicas:    ptr.To(int32(3)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": "toposerver",
							"multigres.com/cluster":       "test-cluster",
						},
					},
					PodManagementPolicy: appsv1.ParallelPodManagement,
					UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
						Type: appsv1.RollingUpdateStatefulSetStrategyType,
					},
					PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
						WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
						WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  "toposerver",
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "test-cluster",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:      "etcd",
									Image:     multigresv1alpha1.DefaultEtcdImage,
									Resources: corev1.ResourceRequirements{},
									Env: buildContainerEnv(
										"test-toposerver",
										"default",
										3,
										"test-toposerver-headless",
									),
									Ports: buildContainerPorts(nil),
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
						},
					},
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: DataVolumeName,
							},
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{
									corev1.ReadWriteOnce,
								},
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: resource.MustParse("50Gi"),
									},
								},
								StorageClassName: ptr.To("fast-ssd"),
							},
						},
					},
				},
			},
		},
		"scheme with incorrect type - should error": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			scheme:  runtime.NewScheme(), // empty scheme with incorrect type
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := BuildStatefulSet(tc.toposerver, tc.scheme)

			if (err != nil) != tc.wantErr {
				t.Errorf("BuildStatefulSet() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildStatefulSet() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildStatefulSetPlacementControls(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "etcd"}}
	placement := &multigresv1alpha1.TopoServerPlacementSpec{
		NodeSelector: map[string]string{"node-pool": "topology"},
		Affinity: &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: selector,
						TopologyKey:   corev1.LabelHostname,
					},
				},
			},
		},
		Tolerations: []corev1.Toleration{{Key: "workload", Value: "topology"}},
		TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
			{
				MaxSkew:           1,
				TopologyKey:       corev1.LabelTopologyZone,
				WhenUnsatisfiable: corev1.DoNotSchedule,
				LabelSelector:     selector,
			},
		},
	}
	toposerver := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-toposerver",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.TopoServerSpec{Placement: placement},
	}

	got, err := BuildStatefulSet(toposerver, scheme)
	if err != nil {
		t.Fatalf("BuildStatefulSet() error = %v", err)
	}

	podSpec := got.Spec.Template.Spec
	if diff := cmp.Diff(placement.NodeSelector, podSpec.NodeSelector); diff != "" {
		t.Errorf("NodeSelector mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(placement.Affinity, podSpec.Affinity); diff != "" {
		t.Errorf("Affinity mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(placement.Tolerations, podSpec.Tolerations); diff != "" {
		t.Errorf("Tolerations mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(
		placement.TopologySpreadConstraints,
		podSpec.TopologySpreadConstraints,
	); diff != "" {
		t.Errorf("TopologySpreadConstraints mismatch (-want +got):\n%s", diff)
	}

	// The built object must not alias the source custom resource.
	podSpec.NodeSelector["node-pool"] = "other"
	podSpec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey = "other"
	podSpec.TopologySpreadConstraints[0].TopologyKey = "other"
	if placement.NodeSelector["node-pool"] != "topology" ||
		placement.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey != corev1.LabelHostname ||
		placement.TopologySpreadConstraints[0].TopologyKey != corev1.LabelTopologyZone {
		t.Fatal("BuildStatefulSet() aliased placement fields from the TopoServer")
	}
}
