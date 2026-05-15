package shard

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

const (
	// defaultTerminationGracePeriod gives multipooler time to gracefully close
	// connections and set NOT_SERVING in etcd before SIGKILL.
	defaultTerminationGracePeriod int64 = 30

	// Default container UIDs used when fsGroup is not provided.
	defaultPostgresRunAsUserUID         int64 = 999
	defaultPostgresExporterRunAsUserUID int64 = 65534
	defaultMultiPoolerRunAsUserUID      int64 = 999

	// DefaultPoolReplicas is the default number of replicas for a pool cell if not specified.
	DefaultPoolReplicas int32 = 1
)

// BuildPoolPodName constructs the deterministic name for a pool pod at the
// given index. The base name is generated using PodConstraints (60 chars) and
// the index is appended as a suffix.
func BuildPoolPodName(shard *multigresv1alpha1.Shard, poolName, cellName string, index int) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	baseName := nameutil.JoinWithConstraints(
		nameutil.PodConstraints,
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		"pool",
		poolName,
		cellName,
	)
	return fmt.Sprintf("%s-%d", baseName, index)
}

// BuildPoolPod creates a Pod for a shard pool in a specific cell at the given
// replica index. It reuses the existing container and volume builders from
// containers.go and sets operator-specific metadata (finalizer, spec-hash).
func BuildPoolPod(
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	index int,
	scheme *runtime.Scheme,
) (*corev1.Pod, error) {
	podName := BuildPoolPodName(shard, poolName, cellName, index)
	clusterName := shard.Labels["multigres.com/cluster"]
	labels := buildPoolLabelsWithCell(shard, poolName, cellName)

	// Construct volumes: reuse shared volumes and prepend the per-pod data PVC.
	dataPVCName := BuildPoolDataPVCName(shard, poolName, cellName, index)
	volumes := buildPoolVolumes(shard, cellName)
	volumes = append([]corev1.Volume{{
		Name: DataVolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: dataPVCName,
			},
		},
	}}, volumes...)

	serviceID := BuildPoolServiceID(podName)

	annotations := map[string]string{
		metadata.AnnotationSpecHash: "", // placeholder, computed below
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
			InitContainers: []corev1.Container{
				buildMultiPoolerSidecar(shard, poolSpec, poolName, cellName, serviceID),
			},
			Containers: []corev1.Container{
				buildPgctldContainer(shard, poolSpec),
				buildPostgresExporterContainer(shard, poolSpec),
			},
			Volumes:      volumes,
			Affinity:     poolSpec.Affinity,
			Tolerations:  poolSpec.Tolerations,
			NodeSelector: shard.Spec.CellTopologyLabels[multigresv1alpha1.CellName(cellName)],
			// Hostname is set to the pod name for DNS resolution via headless service.
			Hostname:  podName,
			Subdomain: buildHeadlessServiceName(shard, poolName, cellName),
		},
	}

	if shard.Spec.Backup != nil &&
		shard.Spec.Backup.S3 != nil &&
		shard.Spec.Backup.S3.ServiceAccountName != "" {
		pod.Spec.ServiceAccountName = shard.Spec.Backup.S3.ServiceAccountName
	}

	pod.Annotations[metadata.AnnotationSpecHash] = ComputeSpecHash(pod)

	if err := ctrl.SetControllerReference(shard, pod, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}

	return pod, nil
}

func buildPoolPodSecurityContext(poolSpec multigresv1alpha1.PoolSpec) *corev1.PodSecurityContext {
	if poolSpec.FSGroup == nil {
		return nil
	}

	fsGroup := *poolSpec.FSGroup
	return &corev1.PodSecurityContext{
		FSGroup: &fsGroup,
	}
}

// buildContainerSecurityContext returns a non-root SecurityContext.
//
// If fsGroup is set, RunAsUser and RunAsGroup are pinned to that value so all
// containers in the pod share the same filesystem identity on shared volumes.
// Otherwise, a container-specific fallback UID is used to ensure kubelet can
// verify runAsNonRoot against a numeric user.
func buildContainerSecurityContext(fsGroup *int64, fallbackUID int64) *corev1.SecurityContext {
	sc := &corev1.SecurityContext{
		RunAsNonRoot: ptr.To(true),
	}
	if fsGroup != nil {
		sc.RunAsUser = fsGroup
		sc.RunAsGroup = fsGroup
		return sc
	}

	sc.RunAsUser = ptr.To(fallbackUID)
	sc.RunAsGroup = ptr.To(fallbackUID)
	return sc
}

// buildHeadlessServiceName constructs the headless service name for DNS
// resolution. Matches the naming used by pool_service.go.
func buildHeadlessServiceName(shard *multigresv1alpha1.Shard, poolName, cellName string) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	return nameutil.JoinWithConstraints(
		nameutil.ServiceConstraints,
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		"pool",
		poolName,
		cellName,
		"headless",
	)
}

// ComputeSpecHash produces a deterministic FNV-1a hex string over the operator-
// managed pod spec fields that should trigger a rolling update when changed.
//
// Fields included: images, commands, args, env vars, resources, volume mounts,
// container security contexts, pod affinity, and node selector.
//
// Hash write errors are discarded throughout because hash.Hash.Write never returns an error
// per the hash.Hash interface contract (it panics on failure instead).
func ComputeSpecHash(pod *corev1.Pod) string {
	h := fnv.New32a()
	spec := &pod.Spec

	hashContainers(h, spec.InitContainers)
	hashContainers(h, spec.Containers)

	for _, v := range spec.Volumes {
		if b, err := json.Marshal(v); err == nil {
			_, _ = h.Write(b)
		}
	}

	if spec.Affinity != nil {
		if b, err := json.Marshal(spec.Affinity); err == nil {
			_, _ = h.Write(b)
		}
	}

	if len(spec.Tolerations) > 0 {
		if b, err := json.Marshal(spec.Tolerations); err == nil {
			_, _ = h.Write(b)
		}
	}

	if spec.NodeSelector != nil {
		keys := sortedKeys(spec.NodeSelector)
		for _, k := range keys {
			_, _ = fmt.Fprintf(h, "%s=%s", k, spec.NodeSelector[k])
		}
	}

	if spec.TerminationGracePeriodSeconds != nil {
		_, _ = fmt.Fprintf(h, "tgp=%d", *spec.TerminationGracePeriodSeconds)
	}

	if spec.ServiceAccountName != "" {
		_, _ = fmt.Fprintf(h, "sa=%s", spec.ServiceAccountName)
	}

	if spec.SecurityContext != nil {
		if b, err := json.Marshal(spec.SecurityContext); err == nil {
			_, _ = fmt.Fprintf(h, "podsc=%s", b)
		}
	}

	if v := pod.Annotations[metadata.AnnotationPostgresConfigHash]; v != "" {
		_, _ = fmt.Fprintf(h, "pgcfg=%s", v)
	}

	return hex.EncodeToString(h.Sum(nil))
}

func hashContainers(h hash.Hash32, containers []corev1.Container) {
	for _, c := range containers {
		_, _ = fmt.Fprintf(h, "name=%s", c.Name)
		_, _ = fmt.Fprintf(h, "image=%s", c.Image)
		for _, cmd := range c.Command {
			_, _ = fmt.Fprintf(h, "cmd=%s", cmd)
		}
		for _, arg := range c.Args {
			_, _ = fmt.Fprintf(h, "arg=%s", arg)
		}
		for _, e := range c.Env {
			_, _ = fmt.Fprintf(h, "env=%s=%s", e.Name, e.Value)
			if e.ValueFrom != nil {
				if b, err := json.Marshal(e.ValueFrom); err == nil {
					_, _ = fmt.Fprintf(h, "envValFrom=%s=%s", e.Name, b)
				}
			}
		}
		for _, ef := range c.EnvFrom {
			if b, err := json.Marshal(ef); err == nil {
				_, _ = fmt.Fprintf(h, "envFromSrc=%s", b)
			}
		}
		if b, err := json.Marshal(c.Resources); err == nil {
			_, _ = fmt.Fprintf(h, "res=%s", b)
		}
		for _, vm := range c.VolumeMounts {
			_, _ = fmt.Fprintf(h, "vm=%s:%s", vm.Name, vm.MountPath)
		}
		if c.SecurityContext != nil {
			if b, err := json.Marshal(c.SecurityContext); err == nil {
				_, _ = fmt.Fprintf(h, "sc=%s", b)
			}
		}
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
