package shard

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	// Public PostgreSQL TLS uses a separate volume from shard-side mTLS.
	litePublicTLSVolumeName = "public-tls-certs"
	litePublicTLSMountPath  = "/etc/multigateway/tls"
	litePublicTLSCertFile   = litePublicTLSMountPath + "/tls.crt"
	litePublicTLSKeyFile    = litePublicTLSMountPath + "/tls.key"
)

// BuildLitePod creates the only pod of a Lite shard. It carries the same
// pgBackRest and pgctld sidecars, exporter, volumes and metadata as the first
// pod BuildPoolPod builds for the pool, with three differences: the main container runs
// minigres instead of multipooler, so the pod also serves the cell's gateway
// and carries the gateway-role label the cell's gateway Service selects on;
// there is no readiness gate, because the data-plane phase that maintains it
// does not run in Lite mode; and there is no headless Service, so the pod
// advertises its IP instead of a hostname under a subdomain.
func BuildLitePod(
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	scheme *runtime.Scheme,
) (*corev1.Pod, error) {
	if err := validatePoolRuntimeIdentity(poolSpec); err != nil {
		return nil, err
	}
	if err := validateSidecarLocation(shard); err != nil {
		return nil, err
	}

	podName := BuildPoolPodName(shard, poolName, cellName, 0)
	serviceID := BuildPoolServiceID(podName)
	clusterName := shard.Labels["multigres.com/cluster"]
	labels := buildPoolLabelsWithCell(shard, poolName, cellName)
	labels[metadata.LabelMultigresGatewayRole] = metadata.GatewayRoleLite

	dataPVCName := BuildPoolDataPVCName(shard, poolName, cellName, 0)
	volumes := buildPoolVolumes(shard, cellName)
	if shard.Spec.CertCommonName != "" {
		volumes = append(volumes, corev1.Volume{
			Name: litePublicTLSVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  multigresv1alpha1.CertSecretName,
					DefaultMode: ptr.To(int32(0o444)),
				},
			},
		})
	}
	volumes = append([]corev1.Volume{{
		Name: DataVolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: dataPVCName,
			},
		},
	}}, volumes...)

	annotations := map[string]string{
		metadata.AnnotationSpecHash:                      "", // placeholder, computed below
		"karpenter.sh/do-not-disrupt":                    "true",
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
		metadata.AnnotationProjectRef: metadata.ResolveProjectRef(
			shard.Annotations,
			clusterName,
		),
		metadata.AnnotationPrometheusScrape: "true",
		metadata.AnnotationPrometheusPort:   "9187",
		metadata.AnnotationPrometheusPath:   "/metrics",
	}
	if h := shard.Annotations[metadata.AnnotationPostgresConfigHash]; h != "" {
		annotations[metadata.AnnotationPostgresConfigHash] = h
	}
	if h := shard.Annotations[metadata.AnnotationPostgresReloadHash]; h != "" {
		annotations[metadata.AnnotationPostgresReloadHash] = h
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   shard.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			SecurityContext:               buildPoolPodSecurityContext(poolSpec),
			TerminationGracePeriodSeconds: ptr.To(defaultTerminationGracePeriod),
			// pgctld is the native sidecar so it outlives minigres on pod
			// termination, as it outlives multipooler in a pool pod. The
			// pgBackRest sidecar starts first, as in a pool pod.
			InitContainers: buildPoolInitContainers(shard, poolSpec),
			Containers: []corev1.Container{
				buildLiteContainer(shard, poolSpec, poolName, cellName, serviceID),
				buildPostgresExporterContainer(shard),
			},
			Volumes:      volumes,
			Affinity:     poolSpec.Affinity,
			Tolerations:  poolSpec.Tolerations,
			NodeSelector: shard.Spec.CellTopologyLabels[multigresv1alpha1.CellName(cellName)],
		},
	}

	applyPgBackRestPodSpec(shard, pod)

	pod.Annotations[metadata.AnnotationSpecHash] = ComputeSpecHash(pod)

	if err := ctrl.SetControllerReference(shard, pod, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return pod, nil
}

// buildLitePod applies operator placement policy to the shared desired pod used
// by reconciliation and status, so both compare the same spec hash.
func (r *ShardReconciler) buildLitePod(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) (*corev1.Pod, error) {
	pod, err := BuildLitePod(shard, poolName, cellName, poolSpec, r.Scheme)
	if err != nil {
		return nil, err
	}
	pod.Spec.PriorityClassName = r.LitePodPriorityClassName
	pod.Annotations[metadata.AnnotationSpecHash] = ComputeSpecHash(pod)
	return pod, nil
}
