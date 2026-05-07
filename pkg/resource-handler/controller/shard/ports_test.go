package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestBuildMultiPoolerContainerPorts(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ContainerPort
	}{
		{
			name: "returns correct ports",
			want: []corev1.ContainerPort{
				{
					Name:          "http",
					ContainerPort: DefaultMultiPoolerHTTPPort,
					Protocol:      corev1.ProtocolTCP,
				},
				{
					Name:          "grpc",
					ContainerPort: DefaultMultiPoolerGRPCPort,
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
			got := buildMultiPoolerContainerPorts()

			if len(got) != len(tt.want) {
				t.Errorf(
					"buildMultiPoolerContainerPorts() length = %d, want %d",
					len(got),
					len(tt.want),
				)
				return
			}

			for i, port := range got {
				if port.Name != tt.want[i].Name {
					t.Errorf("port[%d].Name = %s, want %s", i, port.Name, tt.want[i].Name)
				}
				if port.ContainerPort != tt.want[i].ContainerPort {
					t.Errorf(
						"port[%d].ContainerPort = %d, want %d",
						i,
						port.ContainerPort,
						tt.want[i].ContainerPort,
					)
				}
				if port.Protocol != tt.want[i].Protocol {
					t.Errorf(
						"port[%d].Protocol = %s, want %s",
						i,
						port.Protocol,
						tt.want[i].Protocol,
					)
				}
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				if port.Name != tt.want[i].Name {
					t.Errorf("port[%d].Name = %s, want %s", i, port.Name, tt.want[i].Name)
				}
				if port.Port != tt.want[i].Port {
					t.Errorf("port[%d].Port = %d, want %d", i, port.Port, tt.want[i].Port)
				}
				if port.TargetPort != tt.want[i].TargetPort {
					t.Errorf(
						"port[%d].TargetPort = %v, want %v",
						i,
						port.TargetPort,
						tt.want[i].TargetPort,
					)
				}
				if port.Protocol != tt.want[i].Protocol {
					t.Errorf(
						"port[%d].Protocol = %s, want %s",
						i,
						port.Protocol,
						tt.want[i].Protocol,
					)
				}
			}
		})
	}
}

func TestBuildMultiOrchContainerPorts(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ContainerPort
	}{
		{
			name: "returns correct ports",
			want: []corev1.ContainerPort{
				{
					Name:          "http",
					ContainerPort: DefaultMultiOrchHTTPPort,
					Protocol:      corev1.ProtocolTCP,
				},
				{
					Name:          "grpc",
					ContainerPort: DefaultMultiOrchGRPCPort,
					Protocol:      corev1.ProtocolTCP,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildMultiOrchContainerPorts()

			if len(got) != len(tt.want) {
				t.Errorf(
					"buildMultiOrchContainerPorts() length = %d, want %d",
					len(got),
					len(tt.want),
				)
				return
			}

			for i, port := range got {
				if port.Name != tt.want[i].Name {
					t.Errorf("port[%d].Name = %s, want %s", i, port.Name, tt.want[i].Name)
				}
				if port.ContainerPort != tt.want[i].ContainerPort {
					t.Errorf(
						"port[%d].ContainerPort = %d, want %d",
						i,
						port.ContainerPort,
						tt.want[i].ContainerPort,
					)
				}
				if port.Protocol != tt.want[i].Protocol {
					t.Errorf(
						"port[%d].Protocol = %s, want %s",
						i,
						port.Protocol,
						tt.want[i].Protocol,
					)
				}
			}
		})
	}
}

func TestBuildMultiOrchServicePorts(t *testing.T) {
	tests := []struct {
		name string
		want []corev1.ServicePort
	}{
		{
			name: "returns correct service ports",
			want: []corev1.ServicePort{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildMultiOrchServicePorts()

			if len(got) != len(tt.want) {
				t.Errorf(
					"buildMultiOrchServicePorts() length = %d, want %d",
					len(got),
					len(tt.want),
				)
				return
			}

			for i, port := range got {
				if port.Name != tt.want[i].Name {
					t.Errorf("port[%d].Name = %s, want %s", i, port.Name, tt.want[i].Name)
				}
				if port.Port != tt.want[i].Port {
					t.Errorf("port[%d].Port = %d, want %d", i, port.Port, tt.want[i].Port)
				}
				if port.TargetPort != tt.want[i].TargetPort {
					t.Errorf(
						"port[%d].TargetPort = %v, want %v",
						i,
						port.TargetPort,
						tt.want[i].TargetPort,
					)
				}
				if port.Protocol != tt.want[i].Protocol {
					t.Errorf(
						"port[%d].Protocol = %s, want %s",
						i,
						port.Protocol,
						tt.want[i].Protocol,
					)
				}
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
				if port.Name != tt.want[i].Name {
					t.Errorf("port[%d].Name = %s, want %s", i, port.Name, tt.want[i].Name)
				}
				if port.ContainerPort != tt.want[i].ContainerPort {
					t.Errorf(
						"port[%d].ContainerPort = %d, want %d",
						i,
						port.ContainerPort,
						tt.want[i].ContainerPort,
					)
				}
				if port.Protocol != tt.want[i].Protocol {
					t.Errorf(
						"port[%d].Protocol = %s, want %s",
						i,
						port.Protocol,
						tt.want[i].Protocol,
					)
				}
			}
		})
	}
}
