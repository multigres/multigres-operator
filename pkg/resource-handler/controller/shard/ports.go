package shard

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// DefaultMultipoolerHTTPPort is the default port for Multipooler HTTP traffic.
	DefaultMultipoolerHTTPPort int32 = 15200

	// DefaultMultipoolerGRPCPort is the default port for Multipooler gRPC traffic.
	DefaultMultipoolerGRPCPort int32 = 15270

	// DefaultPostgresPort is the default port for PostgreSQL protocol traffic.
	DefaultPostgresPort int32 = 5432

	// DefaultPgctldHTTPPort is the default port for pgctld HTTP traffic.
	DefaultPgctldHTTPPort int32 = 15400

	// DefaultPostgresExporterPort is the default port for postgres_exporter metrics.
	DefaultPostgresExporterPort int32 = 9187

	// DefaultMultiorchHTTPPort is the default port for Multiorch HTTP traffic.
	DefaultMultiorchHTTPPort int32 = 15300

	// DefaultMultiorchGRPCPort is the default port for Multiorch gRPC traffic.
	DefaultMultiorchGRPCPort int32 = 15370
)

// buildMultipoolerContainerPorts creates the port definitions for the multipooler sidecar container.
// Returns ports for HTTP, gRPC, and PostgreSQL traffic.
func buildMultipoolerContainerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
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
	}
}

// buildPoolHeadlessServicePorts creates service ports for the pool headless service.
// Includes HTTP, gRPC, and PostgreSQL ports for pool pod discovery.
func buildPoolHeadlessServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
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
	}
}

// buildMultiorchContainerPorts creates the port definitions for the Multiorch container.
// Returns ports for HTTP and gRPC traffic.
func buildMultiorchContainerPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
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

// buildMultiorchServicePorts creates service ports for the Multiorch service.
// Includes HTTP and gRPC ports.
func buildMultiorchServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
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
	}
}
