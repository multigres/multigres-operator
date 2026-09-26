package toposerver

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestBuildContainerPorts(t *testing.T) {
	tests := map[string]struct {
		toposerver *multigresv1alpha1.TopoServer
		want       []corev1.ContainerPort
	}{
		"default ports": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			want: []corev1.ContainerPort{
				{
					Name:          "client",
					ContainerPort: 2379,
					Protocol:      corev1.ProtocolTCP,
				},
				{
					Name:          "peer",
					ContainerPort: 2380,
					Protocol:      corev1.ProtocolTCP,
				},
				{
					Name:          "metrics",
					ContainerPort: 2381,
					Protocol:      corev1.ProtocolTCP,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildContainerPorts(tc.toposerver)
			assert.NewCollecting(t).EqDiff(tc.want, got, "buildContainerPorts() mismatch")
		})
	}
}

func TestBuildHeadlessServicePorts(t *testing.T) {
	tests := map[string]struct {
		toposerver *multigresv1alpha1.TopoServer
		want       []corev1.ServicePort
	}{
		"default ports": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			want: []corev1.ServicePort{
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
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildHeadlessServicePorts(tc.toposerver)
			assert.NewCollecting(t).EqDiff(tc.want, got, "buildHeadlessServicePorts() mismatch")
		})
	}
}

func TestBuildClientServicePorts(t *testing.T) {
	tests := map[string]struct {
		toposerver *multigresv1alpha1.TopoServer
		want       []corev1.ServicePort
	}{
		"default port": {
			toposerver: &multigresv1alpha1.TopoServer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-toposerver",
					Namespace: "default",
				},
				Spec: multigresv1alpha1.TopoServerSpec{},
			},
			want: []corev1.ServicePort{
				{
					Name:       "client",
					Port:       2379,
					TargetPort: intstr.FromString("client"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildClientServicePorts(tc.toposerver)
			assert.NewCollecting(t).EqDiff(tc.want, got, "buildClientServicePorts() mismatch")
		})
	}
}
