package shard

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

func TestBuildMultiOrchDeployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tests := map[string]struct {
		shard    *multigresv1alpha1.Shard
		cellName string
		scheme   *runtime.Scheme
		want     *appsv1.Deployment
		wantErr  bool
	}{
		"minimal spec - all defaults": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
				},
			},
			cellName: "zone-a",
			scheme:   scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard-multiorch-zone-a",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  MultiOrchComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "test-cluster",
						"multigres.com/cell":           "zone-a",
						"multigres.com/database":       "testdb",
						"multigres.com/tablegroup":     "default",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "test-shard",
							UID:                "test-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(int32(1)), // Replicas = len(cells)
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "test-cluster",
							"app.kubernetes.io/component": MultiOrchComponentName,
							"multigres.com/cluster":       "test-cluster",
							"multigres.com/cell":          "zone-a",
							"multigres.com/database":      "testdb",
							"multigres.com/tablegroup":    "default",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "test-cluster",
								"app.kubernetes.io/component":  MultiOrchComponentName,
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "test-cluster",
								"multigres.com/cell":           "zone-a",
								"multigres.com/database":       "testdb",
								"multigres.com/tablegroup":     "default",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								buildMultiOrchContainer(&multigresv1alpha1.Shard{
									Spec: multigresv1alpha1.ShardSpec{
										DatabaseName:   "testdb",
										TableGroupName: "default",
										ShardName:      "0",
										GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
											Address:        "global-topo:2379",
											RootPath:       "/multigres/global",
											Implementation: "etcd",
										},
									},
								}, "zone-a"),
							},
						},
					},
				},
			},
		},
		"with different shard name": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "production-shard",
					Namespace: "prod-ns",
					UID:       "prod-uid",
					Labels:    map[string]string{"multigres.com/cluster": "prod-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "proddb",
					TableGroupName: "prod-tg",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						Cells: []multigresv1alpha1.CellName{
							"zone1",
							"zone2",
						},
					},
				},
			},
			cellName: "zone1",
			scheme:   scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "production-shard-multiorch-zone1",
					Namespace: "prod-ns",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "prod-cluster",
						"app.kubernetes.io/component":  MultiOrchComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "prod-cluster",
						"multigres.com/cell":           "zone1",
						"multigres.com/database":       "proddb",
						"multigres.com/tablegroup":     "prod-tg",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "production-shard",
							UID:                "prod-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(int32(1)), // 1 replica per cell
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "prod-cluster",
							"app.kubernetes.io/component": MultiOrchComponentName,
							"multigres.com/cluster":       "prod-cluster",
							"multigres.com/cell":          "zone1",
							"multigres.com/database":      "proddb",
							"multigres.com/tablegroup":    "prod-tg",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "prod-cluster",
								"app.kubernetes.io/component":  MultiOrchComponentName,
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "prod-cluster",
								"multigres.com/cell":           "zone1",
								"multigres.com/database":       "proddb",
								"multigres.com/tablegroup":     "prod-tg",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								buildMultiOrchContainer(&multigresv1alpha1.Shard{
									Spec: multigresv1alpha1.ShardSpec{
										DatabaseName:   "proddb",
										TableGroupName: "prod-tg",
										ShardName:      "0",
										GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
											Address:        "global-topo:2379",
											RootPath:       "/multigres/global",
											Implementation: "etcd",
										},
									},
								}, "zone1"),
							},
						},
					},
				},
			},
		},
		"with custom replicas per cell": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "replicas-shard",
					Namespace: "default",
					UID:       "replicas-uid",
					Labels:    map[string]string{"multigres.com/cluster": "repl-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{
							Replicas: ptr.To(int32(3)),
						},
						Cells: []multigresv1alpha1.CellName{"zone1"},
					},
				},
			},
			cellName: "zone1",
			scheme:   scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "replicas-shard-multiorch-zone1",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "repl-cluster",
						"app.kubernetes.io/component":  MultiOrchComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "repl-cluster",
						"multigres.com/cell":           "zone1",
						"multigres.com/database":       "testdb",
						"multigres.com/tablegroup":     "default",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "replicas-shard",
							UID:                "replicas-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(int32(3)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "repl-cluster",
							"app.kubernetes.io/component": MultiOrchComponentName,
							"multigres.com/cluster":       "repl-cluster",
							"multigres.com/cell":          "zone1",
							"multigres.com/database":      "testdb",
							"multigres.com/tablegroup":    "default",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "repl-cluster",
								"app.kubernetes.io/component":  MultiOrchComponentName,
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "repl-cluster",
								"multigres.com/cell":           "zone1",
								"multigres.com/database":       "testdb",
								"multigres.com/tablegroup":     "default",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								buildMultiOrchContainer(&multigresv1alpha1.Shard{
									Spec: multigresv1alpha1.ShardSpec{
										DatabaseName:   "testdb",
										TableGroupName: "default",
										ShardName:      "0",
										GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
											Address:        "global-topo:2379",
											RootPath:       "/multigres/global",
											Implementation: "etcd",
										},
									},
								}, "zone1"),
							},
						},
					},
				},
			},
		},
		"with pod labels, annotations, and affinity": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "labels-shard",
					Namespace: "default",
					UID:       "labels-uid",
					Labels:    map[string]string{"multigres.com/cluster": "labels-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{
							PodLabels: map[string]string{
								"custom-label":   "custom-value",
								"team":           "platform",
								"sidecar-inject": "true",
							},
							PodAnnotations: map[string]string{
								"custom-annotation": "keep-me",
							},
							Affinity: &corev1.Affinity{
								PodAntiAffinity: &corev1.PodAntiAffinity{
									PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
										{
											Weight: 100,
											PodAffinityTerm: corev1.PodAffinityTerm{
												TopologyKey: "kubernetes.io/hostname",
											},
										},
									},
								},
							},
						},
						Placement: &multigresv1alpha1.PodPlacementSpec{
							Tolerations: []corev1.Toleration{
								{
									Key:      "workload",
									Operator: corev1.TolerationOpEqual,
									Value:    "customer-pg",
									Effect:   corev1.TaintEffectNoSchedule,
								},
							},
						},
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
				},
			},
			cellName: "zone-a",
			scheme:   scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "labels-shard-multiorch-zone-a",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "labels-cluster",
						"app.kubernetes.io/component":  MultiOrchComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "labels-cluster",
						"multigres.com/cell":           "zone-a",
						"multigres.com/database":       "testdb",
						"multigres.com/tablegroup":     "default",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "labels-shard",
							UID:                "labels-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(int32(1)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "labels-cluster",
							"app.kubernetes.io/component": MultiOrchComponentName,
							"multigres.com/cluster":       "labels-cluster",
							"multigres.com/cell":          "zone-a",
							"multigres.com/database":      "testdb",
							"multigres.com/tablegroup":    "default",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "labels-cluster",
								"app.kubernetes.io/component":  MultiOrchComponentName,
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "labels-cluster",
								"multigres.com/cell":           "zone-a",
								"multigres.com/database":       "testdb",
								"multigres.com/tablegroup":     "default",
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
								buildMultiOrchContainer(&multigresv1alpha1.Shard{
									Spec: multigresv1alpha1.ShardSpec{
										DatabaseName:   "testdb",
										TableGroupName: "default",
										ShardName:      "0",
										GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
											Address:        "global-topo:2379",
											RootPath:       "/multigres/global",
											Implementation: "etcd",
										},
									},
								}, "zone-a"),
							},
							Affinity: &corev1.Affinity{
								PodAntiAffinity: &corev1.PodAntiAffinity{
									PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
										{
											Weight: 100,
											PodAffinityTerm: corev1.PodAffinityTerm{
												TopologyKey: "kubernetes.io/hostname",
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
		"pod labels cannot overwrite operator selector labels": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "override-shard",
					Namespace: "default",
					UID:       "override-uid",
					Labels:    map[string]string{"multigres.com/cluster": "override-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{
							PodLabels: map[string]string{
								"app.kubernetes.io/component": "hacked",
								"multigres.com/cell":          "wrong-cell",
								"safe-label":                  "safe-value",
							},
						},
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
				},
			},
			cellName: "zone-a",
			scheme:   scheme,
			want: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "override-shard-multiorch-zone-a",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "override-cluster",
						"app.kubernetes.io/component":  MultiOrchComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "override-cluster",
						"multigres.com/cell":           "zone-a",
						"multigres.com/database":       "testdb",
						"multigres.com/tablegroup":     "default",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "override-shard",
							UID:                "override-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(int32(1)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"app.kubernetes.io/instance":  "override-cluster",
							"app.kubernetes.io/component": MultiOrchComponentName,
							"multigres.com/cluster":       "override-cluster",
							"multigres.com/cell":          "zone-a",
							"multigres.com/database":      "testdb",
							"multigres.com/tablegroup":    "default",
						},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"app.kubernetes.io/name":       "multigres",
								"app.kubernetes.io/instance":   "override-cluster",
								"app.kubernetes.io/component":  MultiOrchComponentName,
								"app.kubernetes.io/part-of":    "multigres",
								"app.kubernetes.io/managed-by": "multigres-operator",
								"multigres.com/cluster":        "override-cluster",
								"multigres.com/cell":           "zone-a",
								"multigres.com/database":       "testdb",
								"multigres.com/tablegroup":     "default",
								"safe-label":                   "safe-value",
							},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								buildMultiOrchContainer(&multigresv1alpha1.Shard{
									Spec: multigresv1alpha1.ShardSpec{
										DatabaseName:   "testdb",
										TableGroupName: "default",
										ShardName:      "0",
										GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
											Address:        "global-topo:2379",
											RootPath:       "/multigres/global",
											Implementation: "etcd",
										},
									},
								}, "zone-a"),
							},
						},
					},
				},
			},
		},
		"invalid scheme - should error": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
				},
			},
			cellName: "zone-a",
			scheme:   runtime.NewScheme(), // empty scheme
			wantErr:  true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.want != nil {
				hashedName := buildMultiOrchNameWithCell(
					tc.shard,
					tc.cellName,
					nameutil.DefaultConstraints,
				)
				tc.want.Name = hashedName
				if tc.want.Spec.Template.Annotations == nil {
					tc.want.Spec.Template.Annotations = map[string]string{}
				}
				tc.want.Spec.Template.Annotations[metadata.AnnotationProjectRef] = metadata.ResolveProjectRef(
					tc.shard.Annotations,
					tc.shard.Labels[metadata.LabelMultigresCluster],
				)
			}

			got, err := BuildMultiOrchDeployment(tc.shard, tc.cellName, tc.scheme)

			if (err != nil) != tc.wantErr {
				t.Errorf("BuildMultiOrchDeployment() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildMultiOrchDeployment() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildMultiOrchDeployment_ProjectRefAnnotation(t *testing.T) {
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
			shard := &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-shard",
					Namespace:   "default",
					UID:         "test-uid",
					Labels:      map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
					Annotations: tc.annotations,
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					MultiOrch: multigresv1alpha1.MultiOrchSpec{
						Cells: []multigresv1alpha1.CellName{"zone-a"},
					},
				},
			}

			deploy, err := BuildMultiOrchDeployment(shard, "zone-a", scheme)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got := deploy.Spec.Template.Annotations[metadata.AnnotationProjectRef]; got != tc.want {
				t.Fatalf("annotation %q = %q, want %q", metadata.AnnotationProjectRef, got, tc.want)
			}

			assertedLabels := map[string]string{
				metadata.LabelAppInstance:  "test-cluster",
				metadata.LabelAppComponent: MultiOrchComponentName,
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

func TestBuildMultiOrchDeployment_OmitsPrometheusScrapeAnnotations(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "labels-shard",
			Namespace: "default",
			UID:       "labels-uid",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "labels-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			MultiOrch: multigresv1alpha1.MultiOrchSpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{
					PodAnnotations: map[string]string{
						metadata.AnnotationPrometheusScrape: "true",
						metadata.AnnotationPrometheusPort:   "8080",
						metadata.AnnotationPrometheusPath:   "/metrics",
						"custom-annotation":                 "keep-me",
					},
				},
				Cells: []multigresv1alpha1.CellName{"zone-a"},
			},
		},
	}

	deploy, err := BuildMultiOrchDeployment(shard, "zone-a", scheme)
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

func TestBuildMultiOrchService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tests := map[string]struct {
		shard    *multigresv1alpha1.Shard
		cellName string
		scheme   *runtime.Scheme
		want     *corev1.Service
		wantErr  bool
	}{
		"minimal spec": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
				},
			},
			cellName: "zone-a",
			scheme:   scheme,
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard-multiorch-zone-a",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  MultiOrchComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "test-cluster",
						"multigres.com/cell":           "zone-a",
						"multigres.com/database":       "testdb",
						"multigres.com/tablegroup":     "default",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "test-shard",
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
						"app.kubernetes.io/component": MultiOrchComponentName,
						"multigres.com/cluster":       "test-cluster",
						"multigres.com/cell":          "zone-a",
						"multigres.com/database":      "testdb",
						"multigres.com/tablegroup":    "default",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       DefaultMultiOrchHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       DefaultMultiOrchGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
		},
		"with different namespace": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "production-shard",
					Namespace: "prod-ns",
					UID:       "prod-uid",
					Labels:    map[string]string{"multigres.com/cluster": "prod-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "proddb",
					TableGroupName: "prod-tg",
					ShardName:      "0",
				},
			},
			cellName: "zone2",
			scheme:   scheme,
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "production-shard-multiorch-zone2",
					Namespace: "prod-ns",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "prod-cluster",
						"app.kubernetes.io/component":  MultiOrchComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cluster":        "prod-cluster",
						"multigres.com/cell":           "zone2",
						"multigres.com/database":       "proddb",
						"multigres.com/tablegroup":     "prod-tg",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "production-shard",
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
						"app.kubernetes.io/component": MultiOrchComponentName,
						"multigres.com/cluster":       "prod-cluster",
						"multigres.com/cell":          "zone2",
						"multigres.com/database":      "proddb",
						"multigres.com/tablegroup":    "prod-tg",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       DefaultMultiOrchHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       DefaultMultiOrchGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
		},
		"invalid scheme - should error": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
				},
			},
			cellName: "zone-a",
			scheme:   runtime.NewScheme(), // empty scheme
			wantErr:  true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.want != nil {
				hashedName := buildMultiOrchNameWithCell(
					tc.shard,
					tc.cellName,
					nameutil.ServiceConstraints,
				)
				tc.want.Name = hashedName
			}

			got, err := BuildMultiOrchService(tc.shard, tc.cellName, tc.scheme)

			if (err != nil) != tc.wantErr {
				t.Errorf("BuildMultiOrchService() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildMultiOrchService() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
