package shard

import (
	"errors"
	"fmt"
	"hash"
	"path"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	// PgBackRestS3CAVolumeName is the volume holding the CA bundle that
	// verifies the S3 endpoint's TLS certificate.
	PgBackRestS3CAVolumeName = "pgbackrest-s3-ca"

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

// validateSidecarLocation refuses a shard whose pgBackRest sidecar cannot be
// built to serve the repository the shard's configuration asks for. Both pod
// builders call it, so a refused shard gets no new pod.
func validateSidecarLocation(shard *multigresv1alpha1.Shard) error {
	_, err := sidecarLocationProblem(shard)
	return err
}

// sidecarLocationProblem is validateSidecarLocation with the reason for the
// BackupRepositoryValid condition. It repeats the admission rules for a
// cluster running without the webhook and for a Shard that was not built from
// a valid cluster. Each case would otherwise render a sidecar that works but
// serves the wrong repository:
//
//   - A spec with no sidecar image was written by an operator that does not
//     set the sidecar's location either. It is refused until the cluster
//     controller writes both.
//   - An S3 backup in the shard's resolved config while the sidecar's
//     location is the local filesystem: backups written to the cluster's own
//     volume while S3 was asked for report success and are lost with the
//     cluster.
//   - An S3 location with no key prefix on a shard whose cluster was
//     allocated one: the repository would be at the bucket root, where other
//     clusters' repositories can be.
//   - A type, encryption, a pgBackRest TLS secret or an S3 bucket, endpoint
//     or region in the shard's resolved config that the sidecar's location
//     does not carry: the repository would be unencrypted, or somewhere else
//     than the config says.
//   - An S3 location with no credential source the sidecar can use.
//   - An S3 endpoint with a scheme other than https, or that is neither a
//     host[:port] nor a URL: pgBackRest would send signed requests without
//     TLS, or could not reach the repository.
//   - A filesystem path other than the one the sidecar mounts the backup
//     volume at: the repository would be on the container's ephemeral
//     filesystem while topology names another directory.
func sidecarLocationProblem(shard *multigresv1alpha1.Shard) (reason string, err error) {
	if shard.Spec.Images.PgBackRestd == "" {
		return multigresv1alpha1.ReasonBackupRepositorySidecarNotConfigured, errors.New(
			"shard spec has no pgbackrestd image, so no pgBackRest sidecar can be built: " +
				"waiting for the cluster controller to write it. If this does not clear, " +
				"the MultigresCluster's BackupRepositoryValid condition says why the " +
				"cluster controller is not updating its shards",
		)
	}
	loc := sidecarLocation(shard)
	if shard.Spec.Backup.ConfiguresS3() && !loc.ResolvesToS3() {
		return multigresv1alpha1.ReasonBackupRepositoryFilesystemFallback, errors.New(
			"an S3 backup is configured for this shard, but its pgBackRest sidecar's " +
				"location is the local filesystem; backups there are lost with the " +
				"cluster's volumes. In a distributed cluster the repository is set by " +
				"spec.backup or the database's backup, not by a tablegroup or shard block",
		)
	}
	if multigresv1alpha1.AllocationWithoutKeyPrefix(shard, loc) {
		return multigresv1alpha1.ReasonBackupRepositoryMissingKeyPrefix, fmt.Errorf(
			"the S3 backup location has no key prefix although the cluster was allocated %q: "+
				"the repository would be at the bucket root. The shard was not built from "+
				"the cluster's backup configuration",
			shard.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix],
		)
	}
	if o := multigresv1alpha1.UnappliedBackupOverride(shard.Spec.Backup, loc); o != nil {
		return multigresv1alpha1.ReasonBackupRepositoryOverrideNotApplied, fmt.Errorf(
			"the shard's backup config sets %s to %q, but its pgBackRest sidecar's location "+
				"has %q, so the value would not be used. In a distributed cluster the sidecar "+
				"is configured by spec.backup and the database's backup, not by a tablegroup "+
				"or shard block",
			o.Field, o.Value, o.Applied,
		)
	}
	if s3 := sidecarS3(loc); s3 != nil && s3.CredentialsSecret == "" &&
		s3.ServiceAccountName == "" && shard.Spec.ShardServiceAccountName == "" {
		return multigresv1alpha1.ReasonBackupRepositoryUnsupportedCredentials, errors.New(
			"s3 backup has no credential source the sidecar supports: " +
				"the sidecar supports IRSA (serviceAccountName or shardServiceAccount) and " +
				"static keys (credentialsSecret) only; node or instance-profile credentials " +
				"(including useEnvCredentials without a credentialsSecret) and EKS Pod Identity " +
				"are not supported",
		)
	}
	if s3 := sidecarS3(loc); s3 != nil {
		if problem := multigresv1alpha1.S3EndpointProblem(s3.Endpoint); problem != "" {
			return multigresv1alpha1.ReasonBackupRepositoryUnsupportedEndpoint, errors.New(problem)
		}
	}
	if loc.Type == multigresv1alpha1.BackupTypeFilesystem && loc.Filesystem != nil {
		if p := loc.Filesystem.Path; p != "" && p != BackupMountPath {
			return multigresv1alpha1.ReasonBackupRepositoryUnsupportedPath, fmt.Errorf(
				"filesystem backup path %q is not supported by the pgBackRest sidecar: "+
					"the sidecar only mounts the backup volume at %q, so the path must be %q or unset",
				p, BackupMountPath, BackupMountPath,
			)
		}
	}
	return multigresv1alpha1.ReasonBackupRepositoryResolved, nil
}

// setBackupRepositoryCondition sets ConditionBackupRepositoryValid from the
// spec alone, so it is current on every status write, including the passes
// that stop at the refusal before any pod is built. The event is emitted when
// the refusal starts or its reason changes, not on every pass.
func (r *ShardReconciler) setBackupRepositoryCondition(shard *multigresv1alpha1.Shard) {
	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionBackupRepositoryValid,
		Status:             metav1.ConditionTrue,
		Message:            "Backups go to the configured repository",
		ObservedGeneration: shard.Generation,
	}
	reason, err := sidecarLocationProblem(shard)
	cond.Reason = reason
	if err != nil {
		cond.Status = metav1.ConditionFalse
		cond.Message = err.Error()
		prev := meta.FindStatusCondition(
			shard.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid,
		)
		if prev == nil || prev.Status != metav1.ConditionFalse || prev.Reason != reason {
			r.Recorder.Event(shard, "Warning", "BackupRepositoryInvalid", cond.Message)
		}
	}
	meta.SetStatusCondition(&shard.Status.Conditions, cond)
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
	cfg podConfig,
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
		// The mode is part of the container args and so of the pod spec hash:
		// changing it rolls every pool pod.
		"--sandbox=" + cfg.sandbox(),
	}
	args = append(args, shardScopeArgs(shard)...)

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
		// The same value topology records for this location: multipooler
		// compares the two and refuses a sidecar that serves another prefix.
		if prefix := s3.RepositoryKeyPrefix(); prefix != "" {
			args = append(args, "--s3-key-prefix="+prefix)
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
		// How the endpoint's certificate is verified is part of what
		// multipooler compares with the topology record, like the prefix.
		// pgbackrestd refuses both together, and so does the CRD.
		switch {
		case s3.SkipsTLSVerification():
			args = append(args, "--s3-skip-tls-verify")
		case s3.CA != nil:
			args = append(args, "--s3-ca-file="+multigresv1alpha1.S3CAFilePath)
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      PgBackRestS3CAVolumeName,
				MountPath: path.Dir(multigresv1alpha1.S3CAFilePath),
				ReadOnly:  true,
			})
		}
	} else {
		args = append(args, "--fs-repo-path="+sidecarFilesystemPath(loc))
	}

	// Cert and cipher follow the sidecar location, the one topology records
	// for this shard. A nil location renders as filesystem with neither. The
	// sidecar is their only consumer.
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

	if _, otelMount := multigresv1alpha1.BuildOTELSamplingVolume(
		shard.Spec.Observability,
	); otelMount != nil {
		volumeMounts = append(volumeMounts, *otelMount)
	}

	sc := buildPgBackRestSecurityContext(pool)

	return corev1.Container{
		Name:  PgBackRestSidecarContainerName,
		Image: string(shard.Spec.Images.PgBackRestd),
		// Run directly as PID 1: stale-worker recovery relies on teardown of
		// this container's dedicated PID namespace and its private state volume.
		Command:       []string{PgBackRestSidecarBinary, "serve"},
		Args:          args,
		Env:           buildRuntimeOTELEnvVars(shard, "pgbackrestd"),
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

// pgBackRestSidecarResources returns the resources the pool's spec gives the
// sidecar, as they are. The defaults are filled in when the MultigresCluster
// is resolved (resolver.DefaultResourcesPgBackRest), so a Shard without the
// block is one whose cluster set it to empty, and its sidecar runs with no
// requests or limits, as its postgres and multipooler containers do.
func pgBackRestSidecarResources(pool multigresv1alpha1.PoolSpec) corev1.ResourceRequirements {
	return *pool.PgBackRestResources.DeepCopy()
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
		volumes = append(volumes, buildPgBackRestS3Volume(s3.CredentialsSecret))
	}
	if s3 := sidecarS3(loc); s3 != nil && s3.CA != nil && !s3.SkipsTLSVerification() {
		volumes = append(volumes, buildPgBackRestS3CAVolume(s3.CA))
	}
	return volumes
}

// buildPgBackRestS3CAVolume projects the one key of the Secret or ConfigMap
// that holds the S3 endpoint's CA bundle, under the file name of
// S3CAFilePath. Only the sidecar mounts it.
//
// The volume is mounted whole, not by subPath, so the kubelet replaces the
// file when the Secret or ConfigMap changes, and pgBackRest reads the file
// for every command it runs: a rotated bundle reaches the sidecar without a
// pod being replaced. Naming another object or key changes the pod template
// and replaces the pods. A missing object or key keeps the pod from
// starting, with the kubelet's event naming it.
func buildPgBackRestS3CAVolume(ca *multigresv1alpha1.S3CAReference) corev1.Volume {
	mode := int32(0o444)
	file := path.Base(multigresv1alpha1.S3CAFilePath)
	var source corev1.VolumeProjection
	switch {
	case ca.Secret != nil:
		source.Secret = &corev1.SecretProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: ca.Secret.Name},
			Items:                []corev1.KeyToPath{{Key: ca.Secret.KeyOrDefault(), Path: file}},
		}
	case ca.ConfigMap != nil:
		source.ConfigMap = &corev1.ConfigMapProjection{
			LocalObjectReference: corev1.LocalObjectReference{Name: ca.ConfigMap.Name},
			Items: []corev1.KeyToPath{
				{Key: ca.ConfigMap.KeyOrDefault(), Path: file},
			},
		}
	}
	return corev1.Volume{
		Name: PgBackRestS3CAVolumeName,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				DefaultMode: &mode,
				Sources:     []corev1.VolumeProjection{source},
			},
		},
	}
}

// S3 static credential keys of the credentials Secret, which are also the
// file names pgbackrestd reads in --s3-static-key-dir.
const (
	s3AccessKeyIDKey     = "AWS_ACCESS_KEY_ID"
	s3SecretAccessKeyKey = "AWS_SECRET_ACCESS_KEY" //nolint:gosec // key name, not a credential
	s3SessionTokenKey    = "AWS_SESSION_TOKEN"     //nolint:gosec // key name, not a credential
)

// buildPgBackRestS3Volume projects the static S3 credential keys of the
// Secret and nothing else in it. The Secret is the cluster owner's and may
// hold other keys, which the sidecar has no use for and must not be given.
// The key id and the secret key are required, so a Secret without them
// keeps the pod from starting with the kubelet's event naming the key; the
// session token is projected when the Secret has one.
func buildPgBackRestS3Volume(secretName string) corev1.Volume {
	mode := int32(0o444)
	item := func(key string) corev1.KeyToPath { return corev1.KeyToPath{Key: key, Path: key} }
	ref := corev1.LocalObjectReference{Name: secretName}
	return corev1.Volume{
		Name: PgBackRestS3VolumeName,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				DefaultMode: &mode,
				Sources: []corev1.VolumeProjection{
					{Secret: &corev1.SecretProjection{
						LocalObjectReference: ref,
						Items: []corev1.KeyToPath{
							item(s3AccessKeyIDKey), item(s3SecretAccessKeyKey),
						},
					}},
					{Secret: &corev1.SecretProjection{
						LocalObjectReference: ref,
						Items:                []corev1.KeyToPath{item(s3SessionTokenKey)},
						Optional:             ptr.To(true),
					}},
				},
			},
		},
	}
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
	slices.Sort(names)
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
	cfg podConfig,
) []corev1.Container {
	return []corev1.Container{
		buildPgBackRestSidecar(shard, pool, cfg),
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

// The values of pgbackrestd's --sandbox flag. Whether pgBackRest children are
// confined with Landlock is a property of the deployment: it needs a kernel
// with Landlock enabled and a seccomp profile that allows landlock_*.
const (
	// SandboxRequired fails closed when Landlock is unavailable. The default.
	SandboxRequired = "required"
	// SandboxAuto confines when the node supports Landlock and runs
	// unconfined otherwise.
	SandboxAuto = "auto"
	// SandboxOff never uses Landlock.
	SandboxOff = "off"
)

// PgBackRestSandboxEnvVar supplies the default of the operator's
// --pgbackrest-sandbox flag.
const PgBackRestSandboxEnvVar = "MULTIGRES_PGBACKREST_SANDBOX"

// ParseSandboxMode validates a sandbox mode. The empty string means required.
func ParseSandboxMode(mode string) (string, error) {
	switch mode {
	case "":
		return SandboxRequired, nil
	case SandboxRequired, SandboxAuto, SandboxOff:
		return mode, nil
	}
	return "", fmt.Errorf("invalid pgBackRest sandbox mode %q: accepted values are %q, %q and %q",
		mode, SandboxRequired, SandboxAuto, SandboxOff)
}

// PodOption carries operator-level settings into the pod builders. Callers
// that pass none get the operator defaults.
type PodOption func(*podConfig)

type podConfig struct {
	sandboxMode string
}

func newPodConfig(opts []PodOption) podConfig {
	var c podConfig
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithSandboxMode sets pgbackrestd's --sandbox value (required, auto or off).
func WithSandboxMode(mode string) PodOption {
	return func(c *podConfig) { c.sandboxMode = mode }
}

// sandbox is the effective mode: empty means the fail-closed default.
func (c podConfig) sandbox() string {
	if c.sandboxMode == "" {
		return SandboxRequired
	}
	return c.sandboxMode
}

// podOptions is how the reconciler's operator-level configuration reaches the
// pod builders. Every desired-pod build the reconciler does, including the
// ones that only compute the spec hash for drift, must go through it, or a
// mode change would not be seen as drift.
func (r *ShardReconciler) podOptions() []PodOption {
	return []PodOption{WithSandboxMode(r.PgBackRestSandbox)}
}
