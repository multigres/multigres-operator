package shard

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

const (
	// DataVolumeName is the name of the data volume for PostgreSQL
	DataVolumeName = "pgdata"

	// DataMountPath is where the PVC is mounted
	// Mounted at parent directory because mounting directly at pg_data/ prevents
	// initdb from setting directory permissions (non-root can't chmod mount points).
	// pgctld creates pg_data/ subdirectory with proper 0700/0750 permissions.
	DataMountPath = "/var/lib/pooler"

	// PgDataPath is the actual postgres data directory (PGDATA env var value)
	// pgctld expects postgres data at <pooler-dir>/pg_data
	PgDataPath = "/var/lib/pooler/pg_data"

	// PoolerDirMountPath must equal DataMountPath because both containers share the PVC
	// and pgctld derives postgres data directory as <pooler-dir>/pg_data
	PoolerDirMountPath = "/var/lib/pooler"

	// SocketDirVolumeName is the name of the shared volume for unix sockets
	SocketDirVolumeName = "socket-dir"

	// SocketDirMountPath is the mount path for unix sockets (postgres and pgctld communicate here)
	// We use /var/run/postgresql because that is the default socket directory for the official postgres image.
	SocketDirMountPath = "/var/run/postgresql"

	// BackupVolumeName is the name of the backup volume for pgbackrest
	BackupVolumeName = "backup-data"

	// BackupMountPath is where the backup volume is mounted
	// pgbackrest stores backups here via --repo1-path
	BackupMountPath = "/backups"

	// PgHbaVolumeName is the name of the volume for pg_hba template
	PgHbaVolumeName = "pg-hba-template"

	// PgHbaMountPath is where the pg_hba template is mounted
	PgHbaMountPath = "/etc/pgctld"

	// PgHbaTemplatePath is the full path to the pg_hba template file
	PgHbaTemplatePath = PgHbaMountPath + "/pg_hba_template.conf"

	// PostgresConfigVolumeName is the name of the volume for the postgresql.conf template
	PostgresConfigVolumeName = "postgres-config-template"

	// PostgresConfigMountPath is where the postgresql.conf template is mounted
	PostgresConfigMountPath = "/etc/pgctld/postgres"

	// PostgresConfigFilePath is the full path to the postgresql.conf template file
	PostgresConfigFilePath = PostgresConfigMountPath + "/postgresql.conf.tmpl"

	// PostgresPasswordSecretKey is the key within the Secret that holds the password
	PostgresPasswordSecretKey = "password"

	// PgBackRestCertVolumeName is the name of the volume for pgBackRest TLS certificates
	PgBackRestCertVolumeName = "pgbackrest-certs"

	// PgBackRestCertMountPath is where pgBackRest TLS certificates are mounted
	PgBackRestCertMountPath = "/certs/pgbackrest"

	// PgBackRestPort is the port for the pgBackRest TLS server
	PgBackRestPort = 8432
)

// PgHbaConfigMapName returns the per-shard ConfigMap name for the pg_hba template.
func PgHbaConfigMapName(shardName string) string {
	return shardName + "-pg-hba"
}

// PostgresPasswordSecretName returns the per-shard Secret name for the postgres password.
func PostgresPasswordSecretName(shardName string) string {
	return shardName + "-postgres-password"
}

// buildSocketDirVolume creates the shared emptyDir volume for unix sockets.
func buildSocketDirVolume() corev1.Volume {
	return corev1.Volume{
		Name: SocketDirVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
}

// buildPgHbaVolume creates the volume for pg_hba template from ConfigMap.
func buildPgHbaVolume(shardName string) corev1.Volume {
	return corev1.Volume{
		Name: PgHbaVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: PgHbaConfigMapName(shardName),
				},
			},
		},
	}
}

// sidecarRestartPolicy is the restart policy for native sidecar containers
var sidecarRestartPolicy = corev1.ContainerRestartPolicyAlways

// buildPgctldContainer creates the postgres container spec using the pgctld image.
// Uses DefaultPostgresImage (ghcr.io/multigres/pgctld:main) which includes:
//   - PostgreSQL 17
//   - pgctld binary at /usr/local/bin/pgctld
//   - pgbackrest for backup/restore operations
func buildPgctldContainer(
	shard *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
) corev1.Container {
	image := multigresv1alpha1.DefaultPostgresImage
	if shard.Spec.Images.Postgres != "" {
		image = string(shard.Spec.Images.Postgres)
	}

	args := []string{
		"server",
		"--pooler-dir=" + PoolerDirMountPath,
		"--grpc-port=15470",
		"--pg-port=5432",
		"--pg-listen-addresses=*",
		"--log-level=" + string(shard.Spec.LogLevels.Pgctld),
		"--grpc-socket-file=" + PoolerDirMountPath + "/pgctld.sock",
		"--pg-hba-template=" + PgHbaTemplatePath,
		"--http-port=15400",
	}

	if shard.Spec.PostgresConfigRef != nil {
		args = append(args, "--postgres-config-template="+PostgresConfigFilePath)
	}

	if shard.Spec.Backup != nil {
		// pgBackRest TLS cert dir and port (enables the pgBackRest TLS server).
		// Backup type/path/S3 config is resolved by multipooler from the etcd
		// topology Database record, not via pgctld CLI flags.
		args = append(args,
			fmt.Sprintf("--pgbackrest-cert-dir=%s", PgBackRestCertMountPath),
			fmt.Sprintf("--pgbackrest-port=%d", PgBackRestPort),
		)
	}

	env := []corev1.EnvVar{
		{
			Name:  "PGDATA",
			Value: PgDataPath,
		},
		pgUserEnvVar(shard.Spec.PostgresSuperuser),
		pgPasswordEnvVar(shard.Name),
	}
	if shard.Spec.InitdbArgs != "" {
		env = append(env, corev1.EnvVar{
			Name:  "POSTGRES_INITDB_ARGS",
			Value: string(shard.Spec.InitdbArgs),
		})
	}
	env = append(env, s3EnvVars(shard.Spec.Backup)...)
	if otelVars := multigresv1alpha1.BuildOTELEnvVars(shard.Spec.Observability); len(otelVars) > 0 {
		env = append(env, otelVars...)
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      DataVolumeName,
			MountPath: DataMountPath,
		},
		{
			Name:      BackupVolumeName,
			MountPath: BackupMountPath,
		},
		{
			Name:      SocketDirVolumeName,
			MountPath: SocketDirMountPath,
		},
		{
			Name:      PgHbaVolumeName,
			MountPath: PgHbaMountPath,
			ReadOnly:  true,
		},
	}
	if shard.Spec.PostgresConfigRef != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      PostgresConfigVolumeName,
			MountPath: PostgresConfigMountPath,
			ReadOnly:  true,
		})
	}
	if shard.Spec.Backup != nil {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      PgBackRestCertVolumeName,
			MountPath: PgBackRestCertMountPath,
			ReadOnly:  true,
		})
	}
	if _, otelMount := multigresv1alpha1.BuildOTELSamplingVolume(
		shard.Spec.Observability,
	); otelMount != nil {
		volumeMounts = append(volumeMounts, *otelMount)
	}

	return corev1.Container{
		Name:      "postgres",
		Image:     image,
		Command:   []string{"/usr/local/bin/pgctld"},
		Args:      args,
		Resources: pool.Postgres.Resources,
		Env:       env,
		SecurityContext: buildContainerSecurityContext(
			pool.FSGroup,
			defaultPostgresRunAsUserUID,
		),
		VolumeMounts: volumeMounts,
		StartupProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/live",
					Port: intstr.FromInt32(DefaultPgctldHTTPPort),
				},
			},
			PeriodSeconds:    5,
			FailureThreshold: 30,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/live",
					Port: intstr.FromInt32(DefaultPgctldHTTPPort),
				},
			},
			PeriodSeconds: 10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/live",
					Port: intstr.FromInt32(DefaultPgctldHTTPPort),
				},
			},
			PeriodSeconds: 5,
		},
	}
}

// buildPostgresExporterContainer creates the postgres_exporter sidecar for scraping local postgres metrics.
func buildPostgresExporterContainer(
	shard *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
) corev1.Container {
	return corev1.Container{
		Name:  "postgres-exporter",
		Image: multigresv1alpha1.DefaultPostgresExporterImage,
		Args: []string{
			"--web.listen-address=:9187",
		},
		Ports: buildPostgresExporterContainerPorts(),
		Env: []corev1.EnvVar{
			{
				Name:  "DATA_SOURCE_URI",
				Value: "localhost:5432/postgres?sslmode=disable",
			},
			{
				Name:  "DATA_SOURCE_USER",
				Value: postgresSuperuserOrDefault(shard.Spec.PostgresSuperuser),
			},
			{
				Name: "DATA_SOURCE_PASS",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: PostgresPasswordSecretName(shard.Name),
						},
						Key: PostgresPasswordSecretKey,
					},
				},
			},
		},
		SecurityContext: buildContainerSecurityContext(
			pool.FSGroup,
			defaultPostgresExporterRunAsUserUID,
		),
	}
}

// BuildPoolServiceID generates a short, deterministic service ID for a
// multipooler from its pod name. The pod name is already guaranteed unique
// (via JoinWithConstraints), so hashing it produces a collision-free short ID.
//
// Format: p-{fnv32a_hex} (e.g. "p-a1b2c3d4", always 10 chars).
// The "p" prefix follows the multigres component naming convention (p=pooler).
func BuildPoolServiceID(podName string) string {
	return "p-" + nameutil.Hash([]string{podName})
}

// buildMultiPoolerSidecar creates the multipooler sidecar container spec.
// Implemented as native sidecar (init container with restartPolicy: Always) because
// multipooler must restart with postgres to maintain connection pool consistency.
func buildMultiPoolerSidecar(
	shard *multigresv1alpha1.Shard,
	pool multigresv1alpha1.PoolSpec,
	poolName string,
	cellName string,
	serviceID string,
) corev1.Container {
	image := multigresv1alpha1.DefaultMultiPoolerImage
	if shard.Spec.Images.MultiPooler != "" {
		image = string(shard.Spec.Images.MultiPooler)
	}

	args := []string{
		"multipooler", // Subcommand
		"--http-port=15200",
		"--grpc-port=15270",
		"--pooler-dir=" + PoolerDirMountPath,
		"--socket-file=" + PoolerDirMountPath + "/pg_sockets/.s.PGSQL.5432", // Unix socket uses trust auth (no password)
		"--service-map=grpc-pooler",                                         // Only enable grpc-pooler service (disables auto-restore service)
		"--topo-global-server-addresses=" + shard.Spec.GlobalTopoServer.Address,
		"--topo-global-root=" + shard.Spec.GlobalTopoServer.RootPath,
		"--cell=" + cellName,
		"--database=" + string(shard.Spec.DatabaseName),
		"--table-group=" + string(shard.Spec.TableGroupName),
		"--shard=" + string(shard.Spec.ShardName),
		"--service-id=" + serviceID,
		"--pgctld-addr=localhost:15470",
		"--pg-port=5432",
		"--log-level=" + string(shard.Spec.LogLevels.Multipooler),
	}

	if shard.Spec.Backup != nil {
		args = append(args,
			"--pgbackrest-cert-file="+PgBackRestCertMountPath+"/pgbackrest.crt",
			"--pgbackrest-key-file="+PgBackRestCertMountPath+"/pgbackrest.key",
			"--pgbackrest-ca-file="+PgBackRestCertMountPath+"/ca.crt",
		)
	}

	c := corev1.Container{
		Name:          "multipooler",
		Image:         image,
		Args:          args,
		Ports:         buildMultiPoolerContainerPorts(),
		Resources:     pool.Multipooler.Resources,
		RestartPolicy: &sidecarRestartPolicy,
		SecurityContext: buildContainerSecurityContext(
			pool.FSGroup,
			defaultMultiPoolerRunAsUserUID,
		),
		StartupProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/live",
					Port: intstr.FromInt32(DefaultMultiPoolerHTTPPort),
				},
			},
			PeriodSeconds:    5,
			FailureThreshold: 30,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/live",
					Port: intstr.FromInt32(DefaultMultiPoolerHTTPPort),
				},
			},
			PeriodSeconds: 10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/live",
					Port: intstr.FromInt32(DefaultMultiPoolerHTTPPort),
				},
			},
			PeriodSeconds: 5,
		},
	}

	env := []corev1.EnvVar{
		{
			Name:  "PGDATA",
			Value: PgDataPath,
		},
		pgUserEnvVar(shard.Spec.PostgresSuperuser),
		pgPasswordEnvVar(shard.Name),
	}
	env = append(env, s3EnvVars(shard.Spec.Backup)...)
	if otelVars := multigresv1alpha1.BuildOTELEnvVars(shard.Spec.Observability); len(otelVars) > 0 {
		env = append(env, otelVars...)
	}
	c.Env = env

	c.VolumeMounts = []corev1.VolumeMount{
		{
			Name:      DataVolumeName, // Shares PVC with postgres for pgbackrest configs and sockets
			MountPath: PoolerDirMountPath,
		},
		{
			Name:      BackupVolumeName,
			MountPath: BackupMountPath,
		},
		{
			Name:      SocketDirVolumeName,
			MountPath: SocketDirMountPath,
		},
	}
	if shard.Spec.Backup != nil {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name:      PgBackRestCertVolumeName,
			MountPath: PgBackRestCertMountPath,
			ReadOnly:  true,
		})
	}
	if _, otelMount := multigresv1alpha1.BuildOTELSamplingVolume(
		shard.Spec.Observability,
	); otelMount != nil {
		c.VolumeMounts = append(c.VolumeMounts, *otelMount)
	}
	return c
}

// buildMultiOrchContainer creates the MultiOrch container spec for a specific cell.
func buildMultiOrchContainer(shard *multigresv1alpha1.Shard, cellName string) corev1.Container {
	image := multigresv1alpha1.DefaultMultiOrchImage
	if shard.Spec.Images.MultiOrch != "" {
		image = string(shard.Spec.Images.MultiOrch)
	}

	watchTarget := fmt.Sprintf("%s/%s/%s",
		shard.Spec.DatabaseName, shard.Spec.TableGroupName, shard.Spec.ShardName)

	args := []string{
		"multiorch", // Subcommand
		"--http-port=15300",
		"--grpc-port=15370",
		"--topo-global-server-addresses=" + shard.Spec.GlobalTopoServer.Address,
		"--topo-global-root=" + shard.Spec.GlobalTopoServer.RootPath,
		"--cell=" + cellName,
		"--watch-targets=" + watchTarget,
		"--log-level=" + string(shard.Spec.LogLevels.Multiorch),
	}

	c := corev1.Container{
		Name:      "multiorch",
		Image:     image,
		Args:      args,
		Ports:     buildMultiOrchContainerPorts(),
		Resources: shard.Spec.MultiOrch.Resources,
		StartupProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/ready",
					Port: intstr.FromInt32(DefaultMultiOrchHTTPPort),
				},
			},
			PeriodSeconds:    5,
			FailureThreshold: 30,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/live",
					Port: intstr.FromInt32(DefaultMultiOrchHTTPPort),
				},
			},
			PeriodSeconds: 10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/ready",
					Port: intstr.FromInt32(DefaultMultiOrchHTTPPort),
				},
			},
			PeriodSeconds: 5,
		},
	}
	if envVars := multigresv1alpha1.BuildOTELEnvVars(shard.Spec.Observability); len(envVars) > 0 {
		c.Env = append(c.Env, envVars...)
	}
	if _, otelMount := multigresv1alpha1.BuildOTELSamplingVolume(
		shard.Spec.Observability,
	); otelMount != nil {
		c.VolumeMounts = append(c.VolumeMounts, *otelMount)
	}
	return c
}

// buildPoolVolumes assembles the complete list of volumes for a pool pod.
// Conditionally includes the pgBackRest cert volume when backup is configured.
// buildPostgresConfigVolume creates a volume that projects a specific key from
// the user-provided ConfigMap to the expected postgresql.conf.tmpl filename.
func buildPostgresConfigVolume(ref *multigresv1alpha1.PostgresConfigRef) corev1.Volume {
	return corev1.Volume{
		Name: PostgresConfigVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: ref.Name,
				},
				Items: []corev1.KeyToPath{
					{Key: ref.Key, Path: "postgresql.conf.tmpl"},
				},
			},
		},
	}
}

func buildPoolVolumes(shard *multigresv1alpha1.Shard, cellName string) []corev1.Volume {
	volumes := []corev1.Volume{
		buildSharedBackupVolume(shard, cellName),
		buildSocketDirVolume(),
		buildPgHbaVolume(shard.Name),
	}
	if shard.Spec.PostgresConfigRef != nil {
		volumes = append(volumes, buildPostgresConfigVolume(shard.Spec.PostgresConfigRef))
	}
	if certVol := buildPgBackRestCertVolume(shard); certVol != nil {
		volumes = append(volumes, *certVol)
	}
	if otelVol, _ := multigresv1alpha1.BuildOTELSamplingVolume(
		shard.Spec.Observability,
	); otelVol != nil {
		volumes = append(volumes, *otelVol)
	}
	return volumes
}

// buildSharedBackupVolume creates the backup volume for pgbackrest.
// If type is Filesystem, references the shared PVC (per-cell).
// If type is S3 (or nil), uses an emptyDir for local scratch/spool.
func buildSharedBackupVolume(shard *multigresv1alpha1.Shard, cellName string) corev1.Volume {
	// Default to EmptyDir (for S3 or no backup config)
	source := corev1.VolumeSource{
		EmptyDir: &corev1.EmptyDirVolumeSource{},
	}

	if shard.Spec.Backup != nil &&
		shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeFilesystem {
		clusterName := shard.Labels["multigres.com/cluster"]
		claimName := nameutil.JoinWithConstraints(
			nameutil.ServiceConstraints,
			"backup-data",
			clusterName,
			string(shard.Spec.DatabaseName),
			string(shard.Spec.TableGroupName),
			string(shard.Spec.ShardName),
			cellName,
		)
		source = corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: claimName,
			},
		}
	}

	return corev1.Volume{
		Name:         BackupVolumeName,
		VolumeSource: source,
	}
}

// buildPgBackRestCertVolume creates the volume for pgBackRest TLS certificates.
// Returns nil if backup is not configured.
//
// Both modes use a projected volume to rename tls.crt → pgbackrest.crt and
// tls.key → pgbackrest.key, matching upstream pgctld/multipooler expectations.
// This makes the user-provided mode directly compatible with cert-manager's
// standard Secret output (ca.crt, tls.crt, tls.key).
//
//   - User-provided: projects from the single Secret specified in PgBackRestTLS.SecretName.
//   - Auto-generated: projects from two operator-managed Secrets
//     ({shard}-pgbackrest-ca and {shard}-pgbackrest-tls).
func buildPgBackRestCertVolume(shard *multigresv1alpha1.Shard) *corev1.Volume {
	if shard.Spec.Backup == nil {
		return nil
	}

	defaultMode := int32(0o440)

	// User-provided Secret: project with key renaming for cert-manager compatibility.
	// Cert-manager outputs ca.crt, tls.crt, tls.key — we rename to match upstream.
	if shard.Spec.Backup.PgBackRestTLS != nil &&
		shard.Spec.Backup.PgBackRestTLS.SecretName != "" {
		secretName := shard.Spec.Backup.PgBackRestTLS.SecretName
		return &corev1.Volume{
			Name: PgBackRestCertVolumeName,
			VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{
					DefaultMode: &defaultMode,
					Sources: []corev1.VolumeProjection{
						{
							Secret: &corev1.SecretProjection{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: secretName,
								},
								Items: []corev1.KeyToPath{
									{Key: "ca.crt", Path: "ca.crt"},
									{Key: "tls.crt", Path: "pgbackrest.crt"},
									{Key: "tls.key", Path: "pgbackrest.key"},
								},
							},
						},
					},
				},
			},
		}
	}

	// Auto-generated: projected volume combining CA and server cert Secrets.
	// Renames tls.crt → pgbackrest.crt and tls.key → pgbackrest.key to match upstream.
	return &corev1.Volume{
		Name: PgBackRestCertVolumeName,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				DefaultMode: &defaultMode,
				Sources: []corev1.VolumeProjection{
					{
						Secret: &corev1.SecretProjection{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: shard.Name + "-pgbackrest-ca",
							},
							Items: []corev1.KeyToPath{
								{Key: "ca.crt", Path: "ca.crt"},
							},
						},
					},
					{
						Secret: &corev1.SecretProjection{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: shard.Name + "-pgbackrest-tls",
							},
							Items: []corev1.KeyToPath{
								{Key: "tls.crt", Path: "pgbackrest.crt"},
								{Key: "tls.key", Path: "pgbackrest.key"},
							},
						},
					},
				},
			},
		},
	}
}

// pgPasswordEnvVar returns a POSTGRES_PASSWORD env var sourced from the per-shard postgres password Secret.
// pgctld reads this during initdb to set the superuser password in pg_authid.
func pgPasswordEnvVar(shardName string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: "POSTGRES_PASSWORD",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: PostgresPasswordSecretName(shardName),
				},
				Key: PostgresPasswordSecretKey,
			},
		},
	}
}

// pgUserEnvVar returns a POSTGRES_USER env var. pgctld uses it during initdb
// to set the superuser name; multipooler uses it to authenticate as admin.
func pgUserEnvVar(user string) corev1.EnvVar {
	return corev1.EnvVar{Name: "POSTGRES_USER", Value: postgresSuperuserOrDefault(user)}
}

// postgresSuperuserOrDefault returns the configured superuser name, or the
// upstream default "postgres" when unset.
func postgresSuperuserOrDefault(user string) string {
	if user == "" {
		return "postgres"
	}
	return user
}

// s3EnvVars returns the AWS environment variables needed for S3 backup.
// Returns nil if backup is not configured for S3.
func s3EnvVars(backup *multigresv1alpha1.BackupConfig) []corev1.EnvVar {
	if backup == nil ||
		backup.Type != multigresv1alpha1.BackupTypeS3 ||
		backup.S3 == nil {
		return nil
	}

	var envs []corev1.EnvVar

	if backup.S3.Region != "" {
		envs = append(envs, corev1.EnvVar{
			Name:  "AWS_REGION",
			Value: backup.S3.Region,
		})
	}

	if backup.S3.CredentialsSecret != "" {
		envs = append(envs, corev1.EnvVar{
			Name: "AWS_ACCESS_KEY_ID",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: backup.S3.CredentialsSecret,
					},
					Key: "AWS_ACCESS_KEY_ID",
				},
			},
		})
		envs = append(envs, corev1.EnvVar{
			Name: "AWS_SECRET_ACCESS_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: backup.S3.CredentialsSecret,
					},
					Key: "AWS_SECRET_ACCESS_KEY",
				},
			},
		})
	}

	return envs
}
