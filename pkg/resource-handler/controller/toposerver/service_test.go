package toposerver

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestBuildHeadlessService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	preferDualStack := corev1.IPFamilyPolicyPreferDualStack

	tests := map[string]struct {
		toposerver *multigresv1alpha1.TopoServer
		scheme     *runtime.Scheme
		want       *corev1.Service
		wantErr    bool
	}{
		"minimal spec": {
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
			want: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver-headless",
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
				Spec: corev1.ServiceSpec{
					IPFamilyPolicy: &preferDualStack,
					ClusterIP:      corev1.ClusterIPNone,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "test-cluster",
						"app.kubernetes.io/component": "toposerver",
						"multigres.com/cluster":       "test-cluster",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "client",
							Port:       2379,
							TargetPort: intstr.FromString("client"),
							Protocol:   corev1.ProtocolTCP,
						},
						{
							Name:       "peer",
							Port:       2380,
							TargetPort: intstr.FromString("peer"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
					PublishNotReadyAddresses: true,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := BuildHeadlessService(tc.toposerver, tc.scheme)

			if (err != nil) != tc.wantErr {
				t.Errorf("BuildHeadlessService() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			assert.NewCollecting(t).EqDiff(tc.want, got, "BuildHeadlessService() mismatch")
		})
	}
}

func TestBuildClientService(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	preferDualStack := corev1.IPFamilyPolicyPreferDualStack

	tests := map[string]struct {
		toposerver *multigresv1alpha1.TopoServer
		scheme     *runtime.Scheme
		want       *corev1.Service
		wantErr    bool
	}{
		"minimal spec": {
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
			want: &corev1.Service{
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
				Spec: corev1.ServiceSpec{
					IPFamilyPolicy: &preferDualStack,
					Type:           corev1.ServiceTypeClusterIP,
					Selector: map[string]string{
						"app.kubernetes.io/instance":  "test-cluster",
						"app.kubernetes.io/component": "toposerver",
						"multigres.com/cluster":       "test-cluster",
					},
					Ports: []corev1.ServicePort{
						{
							Name:       "client",
							Port:       2379,
							TargetPort: intstr.FromString("client"),
							Protocol:   corev1.ProtocolTCP,
						},
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := BuildClientService(tc.toposerver, tc.scheme)

			if (err != nil) != tc.wantErr {
				t.Errorf("BuildClientService() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if tc.wantErr {
				return
			}

			assert.NewCollecting(t).EqDiff(tc.want, got, "BuildClientService() mismatch")
		})
	}
}
