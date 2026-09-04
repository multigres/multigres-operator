/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import corev1 "k8s.io/api/core/v1"

const (
	// TopoClientTLSVolumeName is the volume that carries the certificate a
	// component presents to the topology server, along with the CA bundle
	// that verifies the server.
	TopoClientTLSVolumeName = "topo-client-tls"

	// TopoClientTLSMountPath is where that volume is mounted. It is separate
	// from the internal component mTLS mount so a component that speaks both
	// never has one credential shadow the other.
	TopoClientTLSMountPath = "/etc/multigres/topo-client-tls"

	// TopoClientTLSCertFile is the client certificate presented to the
	// topology server.
	TopoClientTLSCertFile = TopoClientTLSMountPath + "/tls.crt"

	// TopoClientTLSKeyFile is the private key for the client certificate.
	TopoClientTLSKeyFile = TopoClientTLSMountPath + "/tls.key"

	// TopoClientTLSCAFile is the CA bundle that verifies the topology
	// server's certificate.
	TopoClientTLSCAFile = TopoClientTLSMountPath + "/ca.crt"
)

// TopoClientTLSConfigured reports whether the reference carries both the client
// credential and the CA bundle needed to open a TLS connection to the topology
// server. The two are populated together, so either one alone is treated as no
// TLS material.
func TopoClientTLSConfigured(ref GlobalTopoServerRef) bool {
	return ref.ClientCertSecret != "" && ref.CASecret != ""
}

// BuildTopoClientTLSVolume projects the client keypair and the CA bundle into a
// single volume. The keypair comes from ClientCertSecret and the CA from
// CASecret; for a managed topology server both name the same cert-manager
// Secret, while an external topology server may split them across two. Both
// Secrets follow the cert-manager key layout of tls.crt, tls.key and ca.crt.
func BuildTopoClientTLSVolume(ref GlobalTopoServerRef) corev1.Volume {
	defaultMode := int32(0o444)
	return corev1.Volume{
		Name: TopoClientTLSVolumeName,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				DefaultMode: &defaultMode,
				Sources: []corev1.VolumeProjection{
					{
						Secret: &corev1.SecretProjection{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: ref.ClientCertSecret,
							},
							Items: []corev1.KeyToPath{
								{Key: "tls.crt", Path: "tls.crt"},
								{Key: "tls.key", Path: "tls.key"},
							},
						},
					},
					{
						Secret: &corev1.SecretProjection{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: ref.CASecret,
							},
							Items: []corev1.KeyToPath{
								{Key: "ca.crt", Path: "ca.crt"},
							},
						},
					},
				},
			},
		},
	}
}

// TopoClientTLSVolumeMount mounts the topo client credential read-only. It is
// mounted only into the containers that open a topology connection, never into
// a container that shares the postgres data volume or socket directory.
func TopoClientTLSVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      TopoClientTLSVolumeName,
		MountPath: TopoClientTLSMountPath,
		ReadOnly:  true,
	}
}

// TopoClientTLSArgs returns the flags that point a component's etcd topology
// client at the mounted credential. Presenting the certificate is harmless
// against a server that does not require it and mandatory against one that
// does.
func TopoClientTLSArgs() []string {
	return []string{
		"--topo-etcd-tls-cert", TopoClientTLSCertFile,
		"--topo-etcd-tls-key", TopoClientTLSKeyFile,
		"--topo-etcd-tls-ca", TopoClientTLSCAFile,
	}
}
