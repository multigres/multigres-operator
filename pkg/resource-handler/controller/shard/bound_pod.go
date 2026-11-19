package shard

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

// A bound shard is the one shard of a tablegroup that the sharding catalog
// binds to an external PostgreSQL. Its only workload is one pooler pod, which
// serves that database and nothing else. There is no PostgreSQL or pgctld, no
// volume, no backup, no exporter and no multiorch: the operator does not own
// the database, and the pooler learns how to reach it from the catalog.
const (
	// BoundPoolerComponentName is the component label of a bound shard's pod.
	// It is not PoolComponentName: the pool machinery (drain, quarantine,
	// readiness gates, disruption) selects pods by that component and must
	// never act on this one.
	BoundPoolerComponentName = "bound-pooler"

	// boundStartupFailureThreshold bounds how long the pod may take to start,
	// at the startup probe's five-second period: ten minutes. Unlike a pooler
	// that runs its own PostgreSQL, a bound pooler first waits for the default
	// primary to say which database it fronts, and the default primary may
	// itself be starting. A restart in that time only repeats the wait.
	boundStartupFailureThreshold int32 = 120

	// boundPoolerServiceMap is the pooler's gRPC service set. The management
	// and consensus services answer every call with an explicit refusal, which
	// is what a coordinator that finds the pooler should be told.
	boundPoolerServiceMap = "grpc-pooler,grpc-poolermanager,grpc-consensus"
)

// isBoundShard reports whether shard fronts an external database.
func isBoundShard(shard *multigresv1alpha1.Shard) bool {
	return multigresv1alpha1.BackingConnectionOf(shard) != ""
}

// boundPool returns the one pool and cell of a bound shard. The TableGroup
// builds exactly one of each.
func boundPool(
	shard *multigresv1alpha1.Shard,
) (poolName, cellName string, poolSpec multigresv1alpha1.PoolSpec, err error) {
	if len(shard.Spec.Pools) != 1 {
		return "", "", multigresv1alpha1.PoolSpec{}, fmt.Errorf(
			"bound shard %s has %d pools, want 1", shard.Name, len(shard.Spec.Pools))
	}
	for name, pool := range shard.Spec.Pools {
		poolName, poolSpec = string(name), pool
	}
	if len(poolSpec.Cells) != 1 {
		return "", "", multigresv1alpha1.PoolSpec{}, fmt.Errorf(
			"bound shard %s: pool %s has %d cells, want 1",
			shard.Name,
			poolName,
			len(poolSpec.Cells),
		)
	}
	return poolName, string(poolSpec.Cells[0]), poolSpec, nil
}

// BuildBoundPoolerPodName is the name of a bound shard's pod. It carries the
// tablegroup's catalog id, like the volumes of a managed one, so a tablegroup
// created again under a dropped one's name starts as a new pooler, with a
// topology identity of its own.
func BuildBoundPoolerPodName(shard *multigresv1alpha1.Shard, cellName string) string {
	return nameutil.JoinWithConstraints(
		nameutil.PodConstraints,
		append(pvcutil.ShardStorageIdentity(shard), "pooler", cellName)...,
	)
}

func boundPoolerSelector(shard *multigresv1alpha1.Shard) map[string]string {
	return map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
		metadata.LabelAppComponent:        BoundPoolerComponentName,
		metadata.LabelAppManagedBy:        metadata.ManagedByMultigres,
	}
}

func buildBoundPoolerLabels(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
) map[string]string {
	labels := metadata.BuildStandardLabels(
		shard.Labels[metadata.LabelMultigresCluster],
		BoundPoolerComponentName,
	)
	metadata.AddShardLabel(labels, shard.Spec.ShardName)
	metadata.AddCellLabel(labels, multigresv1alpha1.CellName(cellName))
	metadata.AddPoolLabel(labels, multigresv1alpha1.PoolName(poolName))
	metadata.AddDatabaseLabel(labels, shard.Spec.DatabaseName)
	metadata.AddTableGroupLabel(labels, shard.Spec.TableGroupName)
	return metadata.MergeLabels(labels, shard.Labels)
}

// BuildBoundPoolerPod creates the pod of a bound shard: the multipooler
// container alone. It never reads or writes the external database's data
// directory, and is not given a flag that describes a PostgreSQL of its own:
// the pooler refuses those when it is unmanaged.
//
// Neither probe depends on the external database. A pooler whose database is
// away must stay reachable and keep its identity; it reports that itself,
// through its health stream and its topology record, by not serving.
func BuildBoundPoolerPod(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	scheme *runtime.Scheme,
) (*corev1.Pod, error) {
	id := multigresv1alpha1.TableGroupIDOf(shard)
	if id == 0 {
		return nil, fmt.Errorf("bound shard %s has no tablegroup id", shard.Name)
	}

	podName := BuildBoundPoolerPodName(shard, cellName)
	clusterName := shard.Labels[metadata.LabelMultigresCluster]

	volumes, mounts := boundPoolerVolumes(shard)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: shard.Namespace,
			Labels:    buildBoundPoolerLabels(shard, poolName, cellName),
			Annotations: map[string]string{
				metadata.AnnotationSpecHash: "", // placeholder, computed below
				metadata.AnnotationProjectRef: metadata.ResolveProjectRef(
					shard.Annotations,
					clusterName,
				),
			},
		},
		Spec: corev1.PodSpec{
			SecurityContext:               buildPoolPodSecurityContext(poolSpec),
			TerminationGracePeriodSeconds: ptr.To(defaultTerminationGracePeriod),
			Containers: []corev1.Container{
				buildBoundPoolerContainer(
					shard, poolSpec, cellName, BuildPoolServiceID(podName), id, mounts,
				),
			},
			Volumes:      volumes,
			Affinity:     poolSpec.Affinity,
			Tolerations:  poolSpec.Tolerations,
			NodeSelector: shard.Spec.CellTopologyLabels[multigresv1alpha1.CellName(cellName)],
		},
	}
	pod.Annotations[metadata.AnnotationSpecHash] = ComputeSpecHash(pod)

	if err := ctrl.SetControllerReference(shard, pod, scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}
	return pod, nil
}

// boundPoolerVolumes returns the volumes of a bound pod and where the pooler
// mounts them: the TLS material it serves and dials with, and nothing it
// writes to.
func boundPoolerVolumes(shard *multigresv1alpha1.Shard) ([]corev1.Volume, []corev1.VolumeMount) {
	var (
		volumes []corev1.Volume
		mounts  []corev1.VolumeMount
	)
	if shardTLSConfigured(shard) {
		volumes = append(volumes, buildShardTLSVolume(
			shard, multigresv1alpha1.ComponentMultiPoolerTLS))
		mounts = append(mounts, shardTLSVolumeMount())
	}
	if multigresv1alpha1.TopoClientTLSConfigured(shard.Spec.GlobalTopoServer) {
		volumes = append(volumes,
			multigresv1alpha1.BuildTopoClientTLSVolume(shard.Spec.GlobalTopoServer))
		mounts = append(mounts, multigresv1alpha1.TopoClientTLSVolumeMount())
	}
	if vol, mount := multigresv1alpha1.BuildOTELSamplingVolume(
		shard.Spec.Observability,
	); vol != nil {
		volumes = append(volumes, *vol)
		mounts = append(mounts, *mount)
	}
	return volumes, mounts
}

// buildBoundPoolerContainer builds the pooler's container. Its flags are those
// the pooler needs to say what it serves and where it registers, plus TLS. It
// is deliberately not given --pg-port, --pooler-dir, --socket-file,
// --pgctld-addr, any pgBackRest flag, or the PGDATA environment: the pooler
// refuses them when unmanaged, and its endpoint and credentials come from the
// tablegroup's connection in the catalog, which it reads through the default
// primary. It is given no connection pool flags either, as a managed pooler
// is not: with --connpool-global-capacity unset it derives its capacity from
// the database it opens its admin pool against.
func buildBoundPoolerContainer(
	shard *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
	cellName, serviceID string,
	tablegroupID int64,
	mounts []corev1.VolumeMount,
) corev1.Container {
	image := multigresv1alpha1.DefaultMultipoolerImage
	if shard.Spec.Images.Multipooler != "" {
		image = string(shard.Spec.Images.Multipooler)
	}

	args := []string{
		"multipooler",
		fmt.Sprintf("--http-port=%d", DefaultMultipoolerHTTPPort),
		fmt.Sprintf("--grpc-port=%d", DefaultMultipoolerGRPCPort),
		"--service-map=" + boundPoolerServiceMap,
		topoGlobalAddressFlag + shard.Spec.GlobalTopoServer.Address,
		topoGlobalRootFlag + shard.Spec.GlobalTopoServer.RootPath,
		"--cell=" + cellName,
		"--database=" + string(shard.Spec.DatabaseName),
		"--table-group=" + string(shard.Spec.TableGroupName),
		"--shard=" + string(shard.Spec.ShardName),
		"--tablegroup-id=" + strconv.FormatInt(tablegroupID, 10),
		"--service-id=" + serviceID,
		"--hostname=$(POD_IP)",
		"--log-level=" + string(shard.Spec.LogLevels.Multipooler),
		// Without this flag, multipooler's default gRPC keepalive enforcement
		// policy rejects pings sent with no active RPC stream.
		"--grpc-server-keepalive-enforcement-policy-permit-without-stream=true",
	}
	if shardTLSConfigured(shard) {
		// The pooler serves TLS, and dials the default primary to learn its
		// connection with the same material, as the multigateway and
		// multiorch dial poolers.
		args = append(args,
			"--grpc-cert", ShardTLSCertFile,
			"--grpc-key", ShardTLSKeyFile,
			"--grpc-ca", ShardTLSCAFile,
			"--grpc-server-ca", ShardTLSCAFile,
			"--multipooler-grpc-cert", ShardTLSCertFile,
			"--multipooler-grpc-key", ShardTLSKeyFile,
			"--multipooler-grpc-ca", ShardTLSCAFile,
			"--multipooler-grpc-server-name",
			multigresv1alpha1.ComponentCertCommonName(
				multigresv1alpha1.ComponentMultiPoolerTLS,
				shard.Labels[metadata.LabelMultigresCluster],
				shard.Namespace,
			),
			"--multipooler-grpc-require-tls",
		)
	}
	if multigresv1alpha1.TopoClientTLSConfigured(shard.Spec.GlobalTopoServer) {
		args = append(args, multigresv1alpha1.TopoClientTLSArgs()...)
	}

	env := []corev1.EnvVar{{
		Name: "POD_IP",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
		},
	}}
	env = append(env, buildRuntimeOTELEnvVars(shard, "multipooler")...)

	probe := func(path string, period int32) *corev1.Probe {
		return &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: path,
					Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
				},
			},
			PeriodSeconds: period,
		}
	}
	startup := probe("/live", 5)
	startup.FailureThreshold = boundStartupFailureThreshold

	return corev1.Container{
		Name:  multipoolerContainerName,
		Image: image,
		Args:  args,
		Ports: []corev1.ContainerPort{
			{Name: "http", ContainerPort: DefaultMultipoolerHTTPPort, Protocol: corev1.ProtocolTCP},
			{Name: "grpc", ContainerPort: DefaultMultipoolerGRPCPort, Protocol: corev1.ProtocolTCP},
		},
		Env:             env,
		Resources:       pool.Multipooler.Resources,
		SecurityContext: buildMultipoolerSecurityContext(pool),
		VolumeMounts:    mounts,
		StartupProbe:    startup,
		LivenessProbe:   probe("/live", 10),
		ReadinessProbe:  probe("/ready", 5),
	}
}
