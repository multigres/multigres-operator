package shard

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// DefaultMultiPoolerHTTPPort is the default port for MultiPooler HTTP traffic.
	DefaultMultiPoolerHTTPPort int32 = 15200

	// DefaultMultiPoolerGRPCPort is the default port for MultiPooler gRPC traffic.
	DefaultMultiPoolerGRPCPort int32 = 15270

	// DefaultPostgresPort is the default port for PostgreSQL protocol traffic.
	DefaultPostgresPort int32 = 5432

	// DefaultPgctldHTTPPort is the default port for pgctld HTTP traffic.
	DefaultPgctldHTTPPort int32 = 15400

	// DefaultPostgresExporterPort is the default port for postgres_exporter metrics.
	DefaultPostgresExporterPort int32 = 9187

	// DefaultMultiOrchHTTPPort is the default port for MultiOrch HTTP traffic.
	DefaultMultiOrchHTTPPort int32 = 15300

	// DefaultMultiOrchGRPCPort is the default port for MultiOrch gRPC traffic.
	DefaultMultiOrchGRPCPort int32 = 15370
)

// buildMultiPoolerContainerPorts creates the port definitions for the multipooler sidecar container.
// Returns ports for HTTP, gRPC, and PostgreSQL traffic.
func buildMultiPoolerContainerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
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
	}
}

// buildPoolHeadlessServicePorts creates service ports for the pool headless service.
// Includes HTTP, gRPC, and PostgreSQL ports for pool pod discovery.
func buildPoolHeadlessServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
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
	}
}

// buildMultiOrchContainerPorts creates the port definitions for the MultiOrch container.
// Returns ports for HTTP and gRPC traffic.
func buildMultiOrchContainerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
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
	}
}

// buildPostgresExporterContainerPorts creates the port definition for postgres_exporter metrics.
func buildPostgresExporterContainerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
		{
			Name:          "metrics",
			ContainerPort: DefaultPostgresExporterPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
}

// buildMultiOrchServicePorts creates service ports for the MultiOrch service.
// Includes HTTP and gRPC ports.
func buildMultiOrchServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
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
	}
}
