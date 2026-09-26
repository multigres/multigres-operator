package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/multigres/testkit/assert"
)

func TestBuildMultipoolerContainerPorts(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ContainerPort
	}{
		{
			name: "returns correct ports",
			want: []corev1.ContainerPort{
				{
					Name:          "http",
					ContainerPort: DefaultMultipoolerHTTPPort,
					Protocol:      corev1.ProtocolTCP,
				},
				{
					Name:          "grpc",
					ContainerPort: DefaultMultipoolerGRPCPort,
					Protocol:      corev1.ProtocolTCP,
				},
				{
					Name:          "postgres",
					ContainerPort: DefaultPostgresPort,
					Protocol:      corev1.ProtocolTCP,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got := buildMultipoolerContainerPorts()

			if len(got) != len(tt.want) {
				t.Errorf(
					"buildMultipoolerContainerPorts() length = %d, want %d",
					len(got),
					len(tt.want),
				)
				return
			}

			for i, port := range got {
				c.Eq(tt.want[i].Name, port.Name, "port[%d].Name = %s, want", i, port.Name)
				c.Eq(
					tt.want[i].ContainerPort,
					port.ContainerPort,
					"port[%d].ContainerPort = %d, want",
					i,
					port.ContainerPort,
				)
				c.Eq(
					tt.want[i].Protocol,
					port.Protocol,
					"port[%d].Protocol = %s, want",
					i,
					port.Protocol,
				)
			}
		})
	}
}

func TestBuildPoolHeadlessServicePorts(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ServicePort
	}{
		{
			name: "returns correct service ports",
			want: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       DefaultMultipoolerHTTPPort,
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "grpc",
					Port:       DefaultMultipoolerGRPCPort,
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got := buildPoolHeadlessServicePorts()

			if len(got) != len(tt.want) {
				t.Errorf(
					"buildPoolHeadlessServicePorts() length = %d, want %d",
					len(got),
					len(tt.want),
				)
				return
			}

			for i, port := range got {
				c.Eq(tt.want[i].Name, port.Name, "port[%d].Name = %s, want", i, port.Name)
				c.Eq(tt.want[i].Port, port.Port, "port[%d].Port = %d, want", i, port.Port)
				c.Eq(
					tt.want[i].TargetPort,
					port.TargetPort,
					"port[%d].TargetPort = %v, want",
					i,
					port.TargetPort,
				)
				c.Eq(
					tt.want[i].Protocol,
					port.Protocol,
					"port[%d].Protocol = %s, want",
					i,
					port.Protocol,
				)
			}
		})
	}
}

func TestBuildMultiorchContainerPorts(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ContainerPort
	}{
		{
			name: "returns correct ports",
			want: []corev1.ContainerPort{
				{
					Name:          "http",
					ContainerPort: DefaultMultiorchHTTPPort,
					Protocol:      corev1.ProtocolTCP,
				},
				{
					Name:          "grpc",
					ContainerPort: DefaultMultiorchGRPCPort,
					Protocol:      corev1.ProtocolTCP,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got := buildMultiorchContainerPorts()

			if len(got) != len(tt.want) {
				t.Errorf(
					"buildMultiorchContainerPorts() length = %d, want %d",
					len(got),
					len(tt.want),
				)
				return
			}

			for i, port := range got {
				c.Eq(tt.want[i].Name, port.Name, "port[%d].Name = %s, want", i, port.Name)
				c.Eq(
					tt.want[i].ContainerPort,
					port.ContainerPort,
					"port[%d].ContainerPort = %d, want",
					i,
					port.ContainerPort,
				)
				c.Eq(
					tt.want[i].Protocol,
					port.Protocol,
					"port[%d].Protocol = %s, want",
					i,
					port.Protocol,
				)
			}
		})
	}
}

func TestBuildMultiorchServicePorts(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ServicePort
	}{
		{
			name: "returns correct service ports",
			want: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       DefaultMultiorchHTTPPort,
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "grpc",
					Port:       DefaultMultiorchGRPCPort,
					TargetPort: intstr.FromString("grpc"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got := buildMultiorchServicePorts()

			if len(got) != len(tt.want) {
				t.Errorf(
					"buildMultiorchServicePorts() length = %d, want %d",
					len(got),
					len(tt.want),
				)
				return
			}

			for i, port := range got {
				c.Eq(tt.want[i].Name, port.Name, "port[%d].Name = %s, want", i, port.Name)
				c.Eq(tt.want[i].Port, port.Port, "port[%d].Port = %d, want", i, port.Port)
				c.Eq(
					tt.want[i].TargetPort,
					port.TargetPort,
					"port[%d].TargetPort = %v, want",
					i,
					port.TargetPort,
				)
				c.Eq(
					tt.want[i].Protocol,
					port.Protocol,
					"port[%d].Protocol = %s, want",
					i,
					port.Protocol,
				)
			}
		})
	}
}

func TestBuildPostgresExporterContainerPorts(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ContainerPort
	}{
		{
			name: "returns exporter metrics port",
			want: []corev1.ContainerPort{
				{
					Name:          "metrics",
					ContainerPort: DefaultPostgresExporterPort,
					Protocol:      corev1.ProtocolTCP,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got := buildPostgresExporterContainerPorts()

			if len(got) != len(tt.want) {
				t.Errorf(
					"buildPostgresExporterContainerPorts() length = %d, want %d",
					len(got),
					len(tt.want),
				)
				return
			}

			for i, port := range got {
				c.Eq(tt.want[i].Name, port.Name, "port[%d].Name = %s, want", i, port.Name)
				c.Eq(
					tt.want[i].ContainerPort,
					port.ContainerPort,
					"port[%d].ContainerPort = %d, want",
					i,
					port.ContainerPort,
				)
				c.Eq(
					tt.want[i].Protocol,
					port.Protocol,
					"port[%d].Protocol = %s, want",
					i,
					port.Protocol,
				)
			}
		})
	}
}
