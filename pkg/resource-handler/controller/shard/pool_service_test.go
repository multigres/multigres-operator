package shard

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestBuildPoolHeadlessService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	tests := map[string]struct {
		shard    *multigresv1alpha1.Shard
		poolName string
		cellName string
		poolSpec multigresv1alpha1.PoolSpec
		scheme   *runtime.Scheme
		want     *corev1.Service
		wantErr  bool
	}{
		"replica pool headless service": {
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
				},
			},
			poolName: "primary",
			cellName: "zone1",
			poolSpec: multigresv1alpha1.PoolSpec{
				Type: "replica",
			},
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard-pool-primary-zone1-headless",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  PoolComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cell":           "zone1",
						"multigres.com/database":       "testdb",
						"multigres.com/tablegroup":     "default",
						"multigres.com/shard":          "",
						"multigres.com/cluster":        "test-cluster",
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
					ClusterIP: corev1.ClusterIPNone,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "test-cluster",
						"app.kubernetes.io/component": PoolComponentName,
						"multigres.com/cell":          "zone1",
						"multigres.com/database":      "testdb",
						"multigres.com/tablegroup":    "default",
						"multigres.com/shard":         "",
						"multigres.com/cluster":       "test-cluster",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       DefaultMultiPoolerHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       DefaultMultiPoolerGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "postgres",
							Port:       DefaultPostgresPort,
							TargetPort: intstr.FromString("postgres"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "metrics",
							Port:       DefaultPostgresExporterPort,
							TargetPort: intstr.FromString("metrics"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
					PublishNotReadyAddresses: true,
				},
			},
		},
		"readWrite pool with custom cell": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "shard-001",
					Namespace: "prod",
					UID:       "prod-uid",
					Labels:    map[string]string{"multigres.com/cluster": "prod-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
				},
			},
			poolName: "ro",
			cellName: "zone-east",
			poolSpec: multigresv1alpha1.PoolSpec{
				Type:  "readWrite",
				Cells: []multigresv1alpha1.CellName{"zone-east"},
			},
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "shard-001-pool-ro-zone-east-headless",
					Namespace: "prod",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "prod-cluster",
						"app.kubernetes.io/component":  PoolComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cell":           "zone-east",
						"multigres.com/database":       "testdb",
						"multigres.com/tablegroup":     "default",
						"multigres.com/shard":          "",
						"multigres.com/cluster":        "prod-cluster",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "shard-001",
							UID:                "prod-uid",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: corev1.ClusterIPNone,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "prod-cluster",
						"app.kubernetes.io/component": PoolComponentName,
						"multigres.com/cell":          "zone-east",
						"multigres.com/database":      "testdb",
						"multigres.com/tablegroup":    "default",
						"multigres.com/shard":         "",
						"multigres.com/cluster":       "prod-cluster",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       DefaultMultiPoolerHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       DefaultMultiPoolerGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "postgres",
							Port:       DefaultPostgresPort,
							TargetPort: intstr.FromString("postgres"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "metrics",
							Port:       DefaultPostgresExporterPort,
							TargetPort: intstr.FromString("metrics"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
					PublishNotReadyAddresses: true,
				},
			},
		},
		"pool without type uses index in name": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "shard-002",
					Namespace: "default",
					UID:       "uid-002",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
				},
			},
			poolName: "primary",
			cellName: "zone1",
			poolSpec: multigresv1alpha1.PoolSpec{},
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "shard-002-pool-primary-zone1-headless",
					Namespace: "default",
					Labels: map[string]string{
						"app.kubernetes.io/name":       "multigres",
						"app.kubernetes.io/instance":   "test-cluster",
						"app.kubernetes.io/component":  PoolComponentName,
						"app.kubernetes.io/part-of":    "multigres",
						"app.kubernetes.io/managed-by": "multigres-operator",
						"multigres.com/cell":           "zone1",
						"multigres.com/database":       "testdb",
						"multigres.com/tablegroup":     "default",
						"multigres.com/shard":          "",
						"multigres.com/cluster":        "test-cluster",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "multigres.com/v1alpha1",
							Kind:               "Shard",
							Name:               "shard-002",
							UID:                "uid-002",
							Controller:         ptr.To(true),
							BlockOwnerDeletion: ptr.To(true),
						},
					},
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: corev1.ClusterIPNone,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "test-cluster",
						"app.kubernetes.io/component": PoolComponentName,
						"multigres.com/cell":          "zone1",
						"multigres.com/database":      "testdb",
						"multigres.com/tablegroup":    "default",
						"multigres.com/shard":         "",
						"multigres.com/cluster":       "test-cluster",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "http",
							Port:       DefaultMultiPoolerHTTPPort,
							TargetPort: intstr.FromString("http"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "grpc",
							Port:       DefaultMultiPoolerGRPCPort,
							TargetPort: intstr.FromString("grpc"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "postgres",
							Port:       DefaultPostgresPort,
							TargetPort: intstr.FromString("postgres"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "metrics",
							Port:       DefaultPostgresExporterPort,
							TargetPort: intstr.FromString("metrics"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
					PublishNotReadyAddresses: true,
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
				},
			},
			poolSpec: multigresv1alpha1.PoolSpec{
				Type: "replica",
			},
			poolName: "primary",
			cellName: "zone1",
			scheme:   runtime.NewScheme(), // empty scheme
			wantErr:  true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.want != nil {
				hashedSvcName := buildPoolHeadlessServiceName(tc.shard, tc.poolName, tc.cellName)
				tc.want.Name = hashedSvcName
				if tc.want.Labels != nil {
					tc.want.Labels["multigres.com/pool"] = tc.poolName
				}
				if tc.want.Spec.Selector != nil {
					tc.want.Spec.Selector["multigres.com/pool"] = tc.poolName
				}
			}

			testScheme := scheme
			if tc.scheme != nil {
				testScheme = tc.scheme
			}
			got, err := BuildPoolHeadlessService(
				tc.shard,
				tc.poolName,
				tc.cellName,
				tc.poolSpec,
				testScheme,
			)

			if (err != nil) != tc.wantErr {
				t.Errorf("BuildPoolHeadlessService() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("BuildPoolHeadlessService() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
