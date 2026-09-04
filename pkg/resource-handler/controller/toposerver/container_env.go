package toposerver

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// buildContainerEnv constructs all environment variables for etcd clustering in
// StatefulSets. This combines pod identity, etcd config, and cluster peer
// discovery details. When tlsEnabled is set, etcd serves its client and peer
// listeners over TLS and requires a client certificate.
func buildContainerEnv(
	toposerverName, namespace string,
	replicas int32,
	serviceName string,
	tlsEnabled bool,
) []corev1.EnvVar {
	envVars := make([]corev1.EnvVar, 0)

	// Add pod identity variables from downward API
	envVars = append(envVars, buildPodIdentityEnv()...)

	// Add etcd configuration variables
	envVars = append(
		envVars,
		buildEtcdConfigEnv(toposerverName, serviceName, namespace, tlsEnabled)...,
	)

	// Add the initial cluster peer list
	clusterPeerList := buildEtcdClusterPeerList(
		toposerverName, serviceName, namespace, replicas, peerScheme(tlsEnabled),
	)
	envVars = append(envVars, corev1.EnvVar{
		Name:  "ETCD_INITIAL_CLUSTER",
		Value: clusterPeerList,
	})

	return envVars
}

// buildPodIdentityEnv creates environment variables for pod name and namespace.
// These are required for etcd to construct its advertise URLs in StatefulSets,
// and this association of both Pod name and namespace are common.
//
// Ref: https://etcd.io/docs/latest/op-guide/clustering/
func buildPodIdentityEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
	}
}

// clientScheme returns the URL scheme etcd uses for client traffic.
func clientScheme(tlsEnabled bool) string {
	if tlsEnabled {
		return "https"
	}
	return "http"
}

// peerScheme returns the URL scheme etcd uses for peer traffic. Peer TLS is
// switched on together with client TLS: the serving certificate is issued for
// both the client Service and the headless peer Service, so member-to-member
// traffic uses the same credential rather than staying in plaintext.
func peerScheme(tlsEnabled bool) string {
	if tlsEnabled {
		return "https"
	}
	return "http"
}

// buildEtcdConfigEnv creates etcd configuration environment variables.
// These configure etcd's network endpoints and cluster formation.
//
// The metrics listener stays on http regardless of TLS: the StatefulSet's
// probes target it and carry no client credentials, so moving it to TLS would
// break them.
//
// Ref: https://etcd.io/docs/latest/op-guide/configuration/
func buildEtcdConfigEnv(
	toposerverName, serviceName, namespace string,
	tlsEnabled bool,
) []corev1.EnvVar {
	cScheme := clientScheme(tlsEnabled)
	pScheme := peerScheme(tlsEnabled)

	env := []corev1.EnvVar{
		{
			Name:  "ETCD_NAME",
			Value: "$(POD_NAME)",
		},
		{
			Name:  "ETCD_DATA_DIR",
			Value: "/var/lib/etcd",
		},
		{
			Name:  "ETCD_LISTEN_CLIENT_URLS",
			Value: cScheme + "://[::]:2379",
		},
		{
			Name:  "ETCD_LISTEN_PEER_URLS",
			Value: pScheme + "://[::]:2380",
		},
		{
			Name:  "ETCD_LISTEN_METRICS_URLS",
			Value: "http://[::]:2381",
		},
		{
			Name: "ETCD_ADVERTISE_CLIENT_URLS",
			Value: fmt.Sprintf(
				"%s://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379",
				cScheme,
				serviceName,
			),
		},
		{
			Name: "ETCD_INITIAL_ADVERTISE_PEER_URLS",
			Value: fmt.Sprintf(
				"%s://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380",
				pScheme,
				serviceName,
			),
		},
		{
			Name:  "ETCD_INITIAL_CLUSTER_STATE",
			Value: "new",
		},
		{
			Name:  "ETCD_INITIAL_CLUSTER_TOKEN",
			Value: toposerverName,
		},
	}

	if tlsEnabled {
		env = append(env, buildEtcdTLSEnv()...)
	}

	return env
}

// buildEtcdTLSEnv returns the etcd variables that point both listeners at the
// mounted serving certificate and require a client certificate on each. Client
// and peer connections are verified against the same CA the certificate chains
// to, so only a holder of a certificate from that CA can reach the topology.
func buildEtcdTLSEnv() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "ETCD_CERT_FILE",
			Value: TopoServerTLSCertFile,
		},
		{
			Name:  "ETCD_KEY_FILE",
			Value: TopoServerTLSKeyFile,
		},
		{
			Name:  "ETCD_TRUSTED_CA_FILE",
			Value: TopoServerTLSCAFile,
		},
		{
			Name:  "ETCD_CLIENT_CERT_AUTH",
			Value: "true",
		},
		{
			Name:  "ETCD_PEER_CERT_FILE",
			Value: TopoServerTLSCertFile,
		},
		{
			Name:  "ETCD_PEER_KEY_FILE",
			Value: TopoServerTLSKeyFile,
		},
		{
			Name:  "ETCD_PEER_TRUSTED_CA_FILE",
			Value: TopoServerTLSCAFile,
		},
		{
			Name:  "ETCD_PEER_CLIENT_CERT_AUTH",
			Value: "true",
		},
	}
}

// buildEtcdClusterPeerList generates the initial cluster member list for
// bootstrap. This tells each etcd member about all other members during cluster
// formation.
//
// Format: member-0=<scheme>://member-0.service.ns.svc.cluster.local:2380,...
//
// Ref: https://etcd.io/docs/latest/op-guide/clustering/#static
func buildEtcdClusterPeerList(
	toposerverName, serviceName, namespace string,
	replicas int32,
	scheme string,
) string {
	if replicas < 0 {
		return ""
	}

	peers := make([]string, 0, replicas)
	for i := range replicas {
		podName := fmt.Sprintf("%s-%d", toposerverName, i)
		peerURL := fmt.Sprintf("%s=%s://%s.%s.%s.svc.cluster.local:2380",
			podName, scheme, podName, serviceName, namespace)
		peers = append(peers, peerURL)
	}

	return strings.Join(peers, ",")
}
