package shard

import (
	"errors"
	"fmt"
	"hash"
	"path"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"github.com/multigres/multigres/go/common/constants"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

const (
	// PgBackRestSidecarContainerName is the native sidecar that alone holds
	// backup credentials, certificates and the cipher key.
	PgBackRestSidecarContainerName = "pgbackrest"

	// PgBackRestSidecarBinary is where the multigres image installs pgbackrestd.
	PgBackRestSidecarBinary = "/multigres/bin/pgbackrestd"

	// PgBackRestWALVolumeName carries the WAL socket between postgres and the
	// sidecar. Multipooler has no mount for it.
	PgBackRestWALVolumeName = "pgbackrest-wal"

	// PgBackRestWALMountPath is where the WAL socket directory is mounted.
	PgBackRestWALMountPath = "/run/pgbackrest-wal"

	// PgBackRestWALSocketPath is the WAL API socket.
	PgBackRestWALSocketPath = PgBackRestWALMountPath + "/wal.sock"

	// PgBackRestControlVolumeName carries the control socket between
	// multipooler and the sidecar. Postgres has no mount for it.
	PgBackRestControlVolumeName = "pgbackrest-control"

	// PgBackRestControlMountPath is where the control socket directory is mounted.
	PgBackRestControlMountPath = "/run/pgbackrest-control"

	// PgBackRestControlSocketPath is the control API socket.
	PgBackRestControlSocketPath = PgBackRestControlMountPath + "/control.sock"

	// PgBackRestStateVolumeName is the sidecar's private scratch space.
	PgBackRestStateVolumeName = "pgbackrest-state"

	// PgBackRestStateMountPath is the sidecar's private state directory.
	PgBackRestStateMountPath = "/var/lib/pgbackrestd"

	// PgBackRestS3VolumeName is the Secret volume holding static S3 credentials.
	PgBackRestS3VolumeName = "pgbackrest-s3"

	// PgBackRestS3MountPath is where the static S3 credentials Secret is mounted.
	//nolint:gosec // Mount path only; the credentials come from a Kubernetes Secret.
	PgBackRestS3MountPath = "/secrets/pgbackrest-s3"

	// SkipContainersAnnotation names the containers the EKS pod identity
	// webhook must not inject AWS credentials into.
	SkipContainersAnnotation = "eks.amazonaws.com/skip-containers"

	defaultWALSegmentBytes int64 = 16 << 20
	stateFloorBytes        int64 = 256 << 20
	walSegsizeFlag               = "--wal-segsize"
)

// sidecarLocation is the location the sidecar renders. A nil location is
// filesystem /backups, which is what topology records for the same case, so
// the fingerprints agree on default clusters.
func sidecarLocation(shard *multigresv1alpha1.Shard) *multigresv1alpha1.BackupConfig {
	if loc := shard.Spec.SidecarBackupLocation; loc != nil {
		return loc
	}
	return &multigresv1alpha1.BackupConfig{
		Type:       multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: BackupMountPath},
	}
}

func sidecarS3(loc *multigresv1alpha1.BackupConfig) *multigresv1alpha1.S3BackupConfig {
	if loc.Type != multigresv1alpha1.BackupTypeS3 {
		return nil
	}
	return loc.S3
}

// validateSidecarLocation backs up the admission rule for a cluster running
// without the webhook: the sidecar mounts the backup volume only at /backups,
// and rendering any other path would put the repository on the container's
// ephemeral filesystem while topology names a different one. It fails instead
// of rendering something else.
//
// A shard spec stored by an operator that predates the sidecar carries no
// sidecar image, and its backup location is not the one the sidecar must
// render, so it is refused until the cluster controller rewrites it.
func validateSidecarLocation(shard *multigresv1alpha1.Shard) error {
	if shard.Spec.Images.PgBackRestd == "" {
		return errors.New(
			"shard spec has no pgbackrestd image: it predates the pgBackRest sidecar, " +
				"waiting for the cluster controller to update it",
		)
	}
	loc := shard.Spec.SidecarBackupLocation
	if s3 := sidecarS3(sidecarLocation(shard)); s3 != nil && s3.CredentialsSecret == "" &&
		s3.ServiceAccountName == "" && shard.Spec.ShardServiceAccountName == "" {
		return errors.New(
			"s3 backup has no credential source the sidecar supports: " +
				"the sidecar supports IRSA (serviceAccountName or shardServiceAccount) and " +
				"static keys (credentialsSecret) only; node or instance-profile credentials " +
				"(including useEnvCredentials without a credentialsSecret) and EKS Pod Identity " +
				"are not supported",
		)
	}
	if loc == nil || loc.Type != multigresv1alpha1.BackupTypeFilesystem || loc.Filesystem == nil {
		return nil
	}
	if p := loc.Filesystem.Path; p != "" && p != BackupMountPath {
		return fmt.Errorf(
			"filesystem backup path %q is not supported by the pgBackRest sidecar: "+
				"the sidecar only mounts the backup volume at %q, so the path must be %q or unset",
			p, BackupMountPath, BackupMountPath,
		)
	}
	return nil
}

func sidecarFilesystemPath(loc *multigresv1alpha1.BackupConfig) string {
	if loc.Filesystem != nil && loc.Filesystem.Path != "" {
		return loc.Filesystem.Path
	}
	return BackupMountPath
}

// pgbackrestStateSizeLimit sizes the sidecar's emptyDir. It must hold the
// archive-get prefetch queue (at least 256MiB or two segments), two segments
// in flight for archive-push, and fixed headroom for config, locks and logs.
func pgbackrestStateSizeLimit(shard *multigresv1alpha1.Shard) resource.Quantity {
	seg := walSegmentBytes(string(shard.Spec.InitdbArgs))
	queue := max(stateFloorBytes, 2*seg)
	return *resource.NewQuantity(queue+2*seg+stateFloorBytes, resource.BinarySI)
}

// walSegmentBytes reads --wal-segsize=<MB> (or "--wal-segsize <MB>") from an
// initdb argument string, falling back to postgres's 16MiB default for an
// absent or unparseable value.
func walSegmentBytes(initdbArgs string) int64 {
	fields := strings.Fields(initdbArgs)
	for i, f := range fields {
		var val string
		switch {
		case strings.HasPrefix(f, walSegsizeFlag+"="):
			val = strings.TrimPrefix(f, walSegsizeFlag+"=")
		case f == walSegsizeFlag && i+1 < len(fields):
			val = fields[i+1]
		default:
			continue
		}
		if mb, err := strconv.ParseInt(val, 10, 64); err == nil && mb > 0 {
			return mb << 20
		}
	}
	return defaultWALSegmentBytes
}

func buildPgBackRestSidecar(
	shard *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
) corev1.Container {
	loc := sidecarLocation(shard)

	args := []string{
		"--data-volume=" + DataMountPath,
		"--pgdata=" + PgDataPath,
		"--pg-socket-dir=" + PostgresSocketDir,
		"--pg-port=5432",
		"--pg-user=" + postgresSuperuserOrDefault(shard.Spec.PostgresSuperuser),
		"--pg-password-file=" + PostgresPasswordFilePath,
		"--location-type=" + string(loc.Type),
	}

	volumeMounts := []corev1.VolumeMount{
		{Name: DataVolumeName, MountPath: DataMountPath},
		postgresPasswordVolumeMount(),
		{Name: PgBackRestWALVolumeName, MountPath: PgBackRestWALMountPath},
		{Name: PgBackRestControlVolumeName, MountPath: PgBackRestControlMountPath},
		{Name: PgBackRestStateVolumeName, MountPath: PgBackRestStateMountPath},
		{Name: BackupVolumeName, MountPath: BackupMountPath},
	}

	if s3 := sidecarS3(loc); s3 != nil {
		args = append(args, "--s3-bucket="+s3.Bucket)
		if s3.Region != "" {
			args = append(args, "--s3-region="+s3.Region)
		}
		if s3.Endpoint != "" {
			args = append(args, "--s3-endpoint="+s3.Endpoint)
		}
		if s3.KeyPrefix != "" {
			args = append(args, "--s3-key-prefix="+s3.KeyPrefix)
		}
		if s3.CredentialsSecret != "" {
			args = append(args,
				"--s3-cred-source=static",
				"--s3-static-key-dir="+PgBackRestS3MountPath,
			)
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      PgBackRestS3VolumeName,
				MountPath: PgBackRestS3MountPath,
				ReadOnly:  true,
			})
		} else {
			args = append(args, "--s3-cred-source=web-identity")
		}
	} else {
		args = append(args, "--fs-repo-path="+sidecarFilesystemPath(loc))
	}

	// Cert and cipher follow the sidecar location, the same database-level
	// merge topology records, never the shard-level backup config. A nil
	// location renders as filesystem with neither. The sidecar is their only
	// consumer.
	args = append(args, "--tls-cert-dir="+PgBackRestCertMountPath)
	if shard.Spec.SidecarBackupLocation != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      PgBackRestCertVolumeName,
			MountPath: PgBackRestCertMountPath,
			ReadOnly:  true,
		})
	}
	if loc.Encryption != nil {
		args = append(args, "--cipher-key-file="+PgBackRestCipherKeyFilePath)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      PgBackRestCipherKeyVolumeName,
			MountPath: PgBackRestCipherKeyMountPath,
			ReadOnly:  true,
		})
	}

	args = append(args,
		"--restore-pidfile="+path.Join(PoolerDirMountPath, constants.RestoreCommandPIDFile),
	)

	sc := buildPgBackRestSecurityContext(pool)

	return corev1.Container{
		Name:  PgBackRestSidecarContainerName,
		Image: string(shard.Spec.Images.PgBackRestd),
		Command: []string{
			"tini", "--", PgBackRestSidecarBinary, "serve",
		},
		Args:          args,
		RestartPolicy: &sidecarRestartPolicy,
		Ports: []corev1.ContainerPort{
			{Name: "pgbackrest", ContainerPort: PgBackRestPort, Protocol: corev1.ProtocolTCP},
		},
		SecurityContext: sc,
		VolumeMounts:    volumeMounts,
		Resources:       pgBackRestSidecarResources(pool),
		StartupProbe: &corev1.Probe{
			ProbeHandler:     healthProbeHandler(),
			PeriodSeconds:    5,
			TimeoutSeconds:   5,
			FailureThreshold: 60,
		},
		// Restarts a sidecar that is alive but wedged. Kubelet runs no liveness
		// probe until the startup probe has passed, so no initial delay is
		// needed. `health` only asks Status, which never waits on slow work, so
		// a long restore or backup cannot starve it and get the sidecar killed
		// mid-operation; the generous threshold covers a briefly stalled node.
		LivenessProbe: &corev1.Probe{
			ProbeHandler:     healthProbeHandler(),
			PeriodSeconds:    30,
			TimeoutSeconds:   5,
			FailureThreshold: 6,
		},
	}
}

func healthProbeHandler() corev1.ProbeHandler {
	return corev1.ProbeHandler{
		Exec: &corev1.ExecAction{Command: []string{PgBackRestSidecarBinary, "health"}},
	}
}

// pgBackRestSidecarResources returns the pool's override, or the defaults
// when it is empty. Requests are sized so the scheduler accounts for the
// sidecar. The memory limit contains a runaway backup or restore so it cannot
// starve a neighbouring project on the node. There is deliberately no CPU
// limit: pgBackRest compression bursts, and CPU throttling would stretch
// backup and WAL archive latency without protecting anyone, since CPU requests
// already guarantee a fair share.
func pgBackRestSidecarResources(pool multigresv1alpha1.PoolSpec) corev1.ResourceRequirements {
	if len(pool.PgBackRestResources.Requests) > 0 || len(pool.PgBackRestResources.Limits) > 0 ||
		len(pool.PgBackRestResources.Claims) > 0 {
		return *pool.PgBackRestResources.DeepCopy()
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}
}

// buildPgBackRestSecurityContext gives the sidecar postgres's identity, since
// both touch the data volume, then drops everything it does not need. There is
// no pod-level runAsUser to inherit.
func buildPgBackRestSecurityContext(pool multigresv1alpha1.PoolSpec) *corev1.SecurityContext {
	runAsUser, runAsGroup := effectivePgctldIdentity(pool.Postgres)
	sc := buildContainerSecurityContext(runAsUser, runAsGroup)
	sc.AllowPrivilegeEscalation = ptr.To(false)
	sc.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}
	// RuntimeDefault is what lets the sidecar's sandbox use Landlock; every
	// write the sidecar makes lands in an emptyDir or the data volume.
	sc.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	sc.ReadOnlyRootFilesystem = ptr.To(true)
	return sc
}

// effectiveFSGroup is the pod fsGroup. An explicit pool value always wins. An
// unset value defaults to the sidecar's group, because the
// pgBackRest cert Secret is mounted 0440 root-group and the 8432 server runs as
// that group; without it the server cannot read its TLS files and restart-loops.
// An explicit fsGroup different from the sidecar's group is the user's call:
// the Secret is then unreadable to the sidecar unless that group matches.
func effectiveFSGroup(pool multigresv1alpha1.PoolSpec) int64 {
	if pool.FSGroup != nil {
		return *pool.FSGroup
	}
	_, runAsGroup := effectivePgctldIdentity(pool.Postgres)
	return *runAsGroup
}

// buildPgBackRestVolumes returns the volumes the pgBackRest sidecar needs.
func buildPgBackRestVolumes(shard *multigresv1alpha1.Shard) []corev1.Volume {
	stateLimit := pgbackrestStateSizeLimit(shard)
	volumes := []corev1.Volume{
		{
			Name:         PgBackRestWALVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name:         PgBackRestControlVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name: PgBackRestStateVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &stateLimit},
			},
		},
	}
	if certVol := buildPgBackRestCertVolume(shard); certVol != nil {
		volumes = append(volumes, *certVol)
	}
	if cipherVol := buildPgBackRestCipherKeyVolume(shard); cipherVol != nil {
		volumes = append(volumes, *cipherVol)
	}
	loc := sidecarLocation(shard)
	if s3 := sidecarS3(loc); s3 != nil && s3.CredentialsSecret != "" {
		mode := int32(0o444)
		volumes = append(volumes, corev1.Volume{
			Name: PgBackRestS3VolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  s3.CredentialsSecret,
					DefaultMode: &mode,
				},
			},
		})
	}
	return volumes
}

// applyPgBackRestPodSpec sets the pod-level fields the sidecar needs. It runs
// after the containers are assembled because the skip list is derived from them.
func applyPgBackRestPodSpec(shard *multigresv1alpha1.Shard, pod *corev1.Pod) {
	pod.Spec.AutomountServiceAccountToken = ptr.To(false)
	pod.Spec.ServiceAccountName = sidecarServiceAccountName(shard)
	pod.Annotations[SkipContainersAnnotation] = skipContainersValue(pod)
}

// sidecarServiceAccountName prefers the operator-managed shard account. A
// cluster that has not named one keeps the IRSA account from the location, so
// upgrading in place does not drop its web identity; with neither, the pod runs
// as the namespace default.
func sidecarServiceAccountName(shard *multigresv1alpha1.Shard) string {
	if name := shard.Spec.ShardServiceAccountName; name != "" {
		return name
	}
	if s3 := sidecarS3(sidecarLocation(shard)); s3 != nil {
		return s3.ServiceAccountName
	}
	return ""
}

// skipContainersValue lists every container but the sidecar, sorted so the
// annotation is stable across reconciles.
func skipContainersValue(pod *corev1.Pod) string {
	var names []string
	for _, c := range pod.Spec.InitContainers {
		if c.Name != PgBackRestSidecarContainerName {
			names = append(names, c.Name)
		}
	}
	for _, c := range pod.Spec.Containers {
		if c.Name != PgBackRestSidecarContainerName {
			names = append(names, c.Name)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// pgbackrestWALSocketMount is shared by postgres and the sidecar.
func pgbackrestWALSocketMount() corev1.VolumeMount {
	return corev1.VolumeMount{Name: PgBackRestWALVolumeName, MountPath: PgBackRestWALMountPath}
}

// pgbackrestControlSocketMount is shared by multipooler and the sidecar.
func pgbackrestControlSocketMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      PgBackRestControlVolumeName,
		MountPath: PgBackRestControlMountPath,
	}
}

// pgbackrestSocketArgs are the flags that point multipooler (and the minigres
// that embeds it) at the sidecar. Multipooler never opens the WAL socket (it
// has no mount for it); it only forwards the path to pgctld, which renders
// archive_command and restore_command from it.
func pgbackrestSocketArgs() []string {
	return []string{
		"--pgbackrest-control-socket=" + PgBackRestControlSocketPath,
		"--pgbackrest-wal-socket=" + PgBackRestWALSocketPath,
	}
}

// buildPoolInitContainers returns the pod's native sidecars. The
// pgBackRest sidecar starts first so its sockets exist before postgres runs
// archive_command or restore_command.
func buildPoolInitContainers(
	shard *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
) []corev1.Container {
	return []corev1.Container{
		buildPgBackRestSidecar(shard, pool),
		buildPgctldSidecar(shard, pool),
	}
}

// hashPgBackRestPodFields covers the fields the sidecar adds to the rollout
// contract: init-container restart policy, the token automount switch and the
// credential-injection skip list.
func hashPgBackRestPodFields(h hash.Hash32, pod *corev1.Pod) {
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil {
			_, _ = fmt.Fprintf(h, "initRestart=%s=%s", c.Name, *c.RestartPolicy)
		}
	}
	if v := pod.Spec.AutomountServiceAccountToken; v != nil {
		_, _ = fmt.Fprintf(h, "automount=%t", *v)
	}
	if v := pod.Annotations[SkipContainersAnnotation]; v != "" {
		_, _ = fmt.Fprintf(h, "skipContainers=%s", v)
	}
}
