package shard

import (
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestBuildMultipoolerContainer(t *testing.T) {
	tests := map[string]struct {
		shard     *multigresv1alpha1.Shard
		poolSpec  multigresv1alpha1.PoolSpec
		cellName  string
		serviceID string
		want      corev1.Container
	}{
		"default multipooler image with no resources": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "test-shard"},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			},
			poolSpec: multigresv1alpha1.PoolSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			cellName:  "zone1",
			serviceID: "p-test-id",
			want: corev1.Container{
				Name:  "multipooler",
				Image: multigresv1alpha1.DefaultMultipoolerImage,
				Args: []string{
					"multipooler",
					"--http-port=15200",
					"--grpc-port=15270",
					"--pooler-dir=" + PoolerDirMountPath,
					"--socket-file=/var/lib/pooler/pg_sockets/.s.PGSQL.5432",
					"--service-map=grpc-pooler",
					"--topo-global-server-addresses=global-topo:2379",
					"--topo-global-root=/multigres/global",
					"--cell=zone1",
					"--database=testdb",
					"--table-group=default",
					"--shard=0",
					"--service-id=p-test-id",
					"--pgctld-addr=localhost:15470",
					"--pg-port=5432",
					"--connpool-global-capacity=40",
					"--connpool-admin-capacity=5",
					"--log-level=info",
				},
				Ports:     buildMultipoolerContainerPorts(),
				Resources: corev1.ResourceRequirements{},
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: ptr.To(true),
					RunAsUser:    ptr.To(DefaultPostgresUID),
					RunAsGroup:   ptr.To(DefaultPostgresUID),
				},
				StartupProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds:    5,
					FailureThreshold: 30,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds: 10,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds: 5,
				},
				Env: []corev1.EnvVar{
					{Name: "PGDATA", Value: PgDataPath},
					{Name: "POSTGRES_USER", Value: "postgres"},
					pgPasswordFileEnvVar(),
				},
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      DataVolumeName,
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
					postgresPasswordVolumeMount(),
				},
			},
		},
		"custom multipooler image": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "custom-shard"},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "proddb",
					TableGroupName: "orders",
					ShardName:      "1",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					Images: multigresv1alpha1.ShardImages{
						Multipooler: "custom/multipooler:v1.0.0",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			},
			poolSpec: multigresv1alpha1.PoolSpec{
				Cells: []multigresv1alpha1.CellName{"zone2"},
			},
			cellName:  "zone2",
			serviceID: "p-custom-id",
			want: corev1.Container{
				Name:  "multipooler",
				Image: "custom/multipooler:v1.0.0",
				Args: []string{
					"multipooler",
					"--http-port=15200",
					"--grpc-port=15270",
					"--pooler-dir=" + PoolerDirMountPath,
					"--socket-file=/var/lib/pooler/pg_sockets/.s.PGSQL.5432",
					"--service-map=grpc-pooler",
					"--topo-global-server-addresses=global-topo:2379",
					"--topo-global-root=/multigres/global",
					"--cell=zone2",
					"--database=proddb",
					"--table-group=orders",
					"--shard=1",
					"--service-id=p-custom-id",
					"--pgctld-addr=localhost:15470",
					"--pg-port=5432",
					"--connpool-global-capacity=40",
					"--connpool-admin-capacity=5",
					"--log-level=info",
				},
				Ports:     buildMultipoolerContainerPorts(),
				Resources: corev1.ResourceRequirements{},
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: ptr.To(true),
					RunAsUser:    ptr.To(DefaultPostgresUID),
					RunAsGroup:   ptr.To(DefaultPostgresUID),
				},
				StartupProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds:    5,
					FailureThreshold: 30,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds: 10,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds: 5,
				},
				Env: []corev1.EnvVar{
					{Name: "PGDATA", Value: PgDataPath},
					{Name: "POSTGRES_USER", Value: "postgres"},
					pgPasswordFileEnvVar(),
				},
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      DataVolumeName,
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
					postgresPasswordVolumeMount(),
				},
			},
		},
		"with resource requirements": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "resource-shard"},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "mydb",
					TableGroupName: "default",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			},
			poolSpec: multigresv1alpha1.PoolSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
				Multipooler: multigresv1alpha1.ContainerConfig{
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("256Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
				},
			},
			cellName:  "zone1",
			serviceID: "p-resource-id",
			want: corev1.Container{
				Name:  "multipooler",
				Image: multigresv1alpha1.DefaultMultipoolerImage,
				Args: []string{
					"multipooler",
					"--http-port=15200",
					"--grpc-port=15270",
					"--pooler-dir=" + PoolerDirMountPath,
					"--socket-file=/var/lib/pooler/pg_sockets/.s.PGSQL.5432",
					"--service-map=grpc-pooler",
					"--topo-global-server-addresses=global-topo:2379",
					"--topo-global-root=/multigres/global",
					"--cell=zone1",
					"--database=mydb",
					"--table-group=default",
					"--shard=0",
					"--service-id=p-resource-id",
					"--pgctld-addr=localhost:15470",
					"--pg-port=5432",
					"--connpool-global-capacity=40",
					"--connpool-admin-capacity=5",
					"--log-level=info",
				},
				Ports: buildMultipoolerContainerPorts(),
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: ptr.To(true),
					RunAsUser:    ptr.To(DefaultPostgresUID),
					RunAsGroup:   ptr.To(DefaultPostgresUID),
				},
				StartupProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds:    5,
					FailureThreshold: 30,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds: 10,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultipoolerHTTPPort),
						},
					},
					PeriodSeconds: 5,
				},
				Env: []corev1.EnvVar{
					{Name: "PGDATA", Value: PgDataPath},
					{Name: "POSTGRES_USER", Value: "postgres"},
					pgPasswordFileEnvVar(),
				},
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      DataVolumeName,
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
					postgresPasswordVolumeMount(),
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildMultipoolerContainer(
				tc.shard,
				tc.poolSpec,
				"primary",
				tc.cellName,
				tc.serviceID,
			)

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("buildMultipoolerContainer() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildPostgresExporterContainer(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard"},
	}
	pool := multigresv1alpha1.PoolSpec{}

	got := buildPostgresExporterContainer(shard, pool)

	want := corev1.Container{
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
				Value: "postgres",
			},
			{
				Name:  "DATA_SOURCE_PASS_FILE",
				Value: PostgresPasswordFilePath,
			},
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(DefaultPostgresUID),
			RunAsGroup:   ptr.To(DefaultPostgresUID),
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      PostgresPasswordVolumeName,
				MountPath: PostgresPasswordMountPath,
				ReadOnly:  true,
			},
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("buildPostgresExporterContainer() mismatch (-want +got):\n%s", diff)
	}
}

func TestPoolContainers_CustomPostgresSuperuser(t *testing.T) {
	const customSuperuser = "admin"

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard"},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},
			PostgresSuperuser: customSuperuser,
		},
	}
	pool := multigresv1alpha1.PoolSpec{
		Cells: []multigresv1alpha1.CellName{"zone1"},
	}

	t.Run("pgctld", func(t *testing.T) {
		c := buildPgctldSidecar(shard, pool)
		assertEnvVarValue(t, c.Env, "POSTGRES_USER", customSuperuser)
	})

	t.Run("multipooler", func(t *testing.T) {
		c := buildMultipoolerContainer(shard, pool, "primary", "zone1", "p-test-id")
		assertEnvVarValue(t, c.Env, "POSTGRES_USER", customSuperuser)
	})

	t.Run("postgres-exporter", func(t *testing.T) {
		c := buildPostgresExporterContainer(shard, pool)
		assertEnvVarValue(t, c.Env, "DATA_SOURCE_USER", customSuperuser)
	})
}

func TestPoolContainers_PostgresPasswordFile(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard"},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:  "global-topo:2379",
				RootPath: "/multigres/global",
			},
		},
	}
	pool := multigresv1alpha1.PoolSpec{
		Cells: []multigresv1alpha1.CellName{"zone1"},
	}

	for name, c := range map[string]corev1.Container{
		"pgctld":            buildPgctldSidecar(shard, pool),
		"multipooler":       buildMultipoolerContainer(shard, pool, "primary", "zone1", "p-test-id"),
		"postgres-exporter": buildPostgresExporterContainer(shard, pool),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "postgres-exporter" {
				assertEnvVarValue(t, c.Env, "DATA_SOURCE_PASS_FILE", PostgresPasswordFilePath)
				assertNotContainsEnvVar(t, c.Env, "DATA_SOURCE_PASS")
			} else {
				assertEnvVarValue(t, c.Env, "POSTGRES_PASSWORD_FILE", PostgresPasswordFilePath)
			}
			assertNotContainsEnvVar(t, c.Env, "POSTGRES_PASSWORD")
			assertReadOnlyVolumeMount(
				t,
				c.VolumeMounts,
				PostgresPasswordVolumeName,
				PostgresPasswordMountPath,
			)
		})
	}
}

func TestPoolContainers_PostgresPasswordSecretRef(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "test-shard"},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0",
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: "multigres-admin-password",
				Key:  PostgresPasswordSecretKey,
			},
		},
	}

	volumes := buildPoolVolumes(shard, "zone1")
	for _, v := range volumes {
		if v.Name != PostgresPasswordVolumeName {
			continue
		}
		if v.Secret == nil {
			t.Fatal("postgres password volume should use Secret source")
		}
		if v.Secret.SecretName != "multigres-admin-password" {
			t.Errorf(
				"postgres password SecretName = %q, want multigres-admin-password",
				v.Secret.SecretName,
			)
		}
		if len(v.Secret.Items) != 1 ||
			v.Secret.Items[0].Key != PostgresPasswordSecretKey ||
			v.Secret.Items[0].Path != PostgresPasswordSecretKey {
			t.Errorf(
				"postgres password Secret items = %+v, want default key projected to password",
				v.Secret.Items,
			)
		}
		return
	}
	t.Fatalf("expected postgres password Secret volume in pool volumes")
}

func TestBuildMultiorchContainer(t *testing.T) {
	tests := map[string]struct {
		shard    *multigresv1alpha1.Shard
		cellName string
		want     corev1.Container
	}{
		"default multiorch container": {
			shard: &multigresv1alpha1.Shard{
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "testdb",
					TableGroupName: "default",
					ShardName:      "0",
					GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
						Address:        "global-topo:2379",
						RootPath:       "/multigres/global",
						Implementation: "etcd",
					},
					LogLevels: multigresv1alpha1.ComponentLogLevels{
						Pgctld:       "info",
						Multipooler:  "info",
						Multiorch:    "info",
						Multiadmin:   "info",
						Multigateway: "info",
					},
				},
			},
			cellName: "zone1",
			want: corev1.Container{
				Name:  "multiorch",
				Image: multigresv1alpha1.DefaultMultiorchImage,
				Args: []string{
					"multiorch",
					"--http-port=15300",
					"--grpc-port=15370",
					"--topo-global-server-addresses=global-topo:2379",
					"--topo-global-root=/multigres/global",
					"--cell=zone1",
					"--watch-targets=testdb/default/0",
					"--log-level=info",
				},
				Ports:     buildMultiorchContainerPorts(),
				Resources: corev1.ResourceRequirements{},
				StartupProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/ready",
							Port: intstr.FromInt32(DefaultMultiorchHTTPPort),
						},
					},
					PeriodSeconds:    5,
					FailureThreshold: 30,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/live",
							Port: intstr.FromInt32(DefaultMultiorchHTTPPort),
						},
					},
					PeriodSeconds: 10,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/ready",
							Port: intstr.FromInt32(DefaultMultiorchHTTPPort),
						},
					},
					PeriodSeconds: 5,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := buildMultiorchContainer(tc.shard, tc.cellName)

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("buildMultiorchContainer() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// otelShard returns a Shard with observability configured for testing the
// OTEL env var injection branch in each container builder.
func otelShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "otel-shard",
			Labels: map[string]string{
				"multigres.com/cluster": "test-cluster",
			},
			Annotations: map[string]string{
				"multigres.com/project-ref": "project-ref-123",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "global-topo:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			Observability: &multigresv1alpha1.ObservabilityConfig{
				OTLPEndpoint:         "http://tempo:4318",
				OTLPMetricsEndpoint:  "http://vmagent:4318/v1/metrics",
				MetricsExporter:      "otlp",
				MetricExportInterval: "30000",
			},
		},
	}
}

func TestBuildPgctldSidecar(t *testing.T) {
	t.Run("default image", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{}}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		if c.Image != multigresv1alpha1.DefaultPostgresImage {
			t.Errorf("Image = %q, want %q", c.Image, multigresv1alpha1.DefaultPostgresImage)
		}
		if c.Command[0] != "/usr/local/bin/pgctld" {
			t.Errorf("Command = %v, want /usr/local/bin/pgctld", c.Command)
		}
		assertContainsFlag(t, c.Args, "--http-port=15400")
		if c.StartupProbe == nil || c.StartupProbe.HTTPGet.Path != "/live" {
			t.Errorf("expected StartupProbe to hit /live, got %v", c.StartupProbe)
		}
		if c.LivenessProbe == nil || c.LivenessProbe.HTTPGet.Path != "/live" {
			t.Errorf("expected LivenessProbe to hit /live, got %v", c.LivenessProbe)
		}
		if c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet.Path != "/live" {
			t.Errorf("expected ReadinessProbe to hit /live, got %v", c.ReadinessProbe)
		}
	})

	t.Run("custom image", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Images: multigresv1alpha1.ShardImages{Postgres: "custom/pgctld:v1"},
			},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		if c.Image != "custom/pgctld:v1" {
			t.Errorf("Image = %q, want %q", c.Image, "custom/pgctld:v1")
		}
	})

	t.Run("with observability", func(t *testing.T) {
		c := buildPgctldSidecar(otelShard(), multigresv1alpha1.PoolSpec{})
		assertContainsOTELEnvVar(t, c.Env, "buildPgctldSidecar")
		assertEnvVarValue(
			t,
			c.Env,
			"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
			"http://vmagent:4318/v1/metrics",
		)
		assertOTELResourceAttribute(t, c.Env, "multigres.project=project-ref-123")
		assertOTELResourceAttribute(t, c.Env, "multigres.cluster=test-cluster")
		assertOTELResourceAttribute(t, c.Env, "multigres.component=pgctld")
	})

	t.Run("with backup filesystem", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
						Path: "/custom-backups",
					},
				},
			},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertContainsFlag(t, c.Args, "--pgbackrest-cert-dir="+PgBackRestCertMountPath)
		assertContainsFlag(t, c.Args, "--pgbackrest-port=8432")
		assertNotContainsFlag(t, c.Args, "--backup-type")
		assertNotContainsFlag(t, c.Args, "--backup-path")
	})

	t.Run("with backup s3", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeS3,
					S3: &multigresv1alpha1.S3BackupConfig{
						Bucket: "my-bucket",
						Region: "us-west-2",
					},
				},
			},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertContainsFlag(t, c.Args, "--pgbackrest-cert-dir="+PgBackRestCertMountPath)
		assertContainsFlag(t, c.Args, "--pgbackrest-port=8432")
		assertNotContainsFlag(t, c.Args, "--backup-type")
		assertNotContainsFlag(t, c.Args, "--backup-bucket")
		assertNotContainsFlag(t, c.Args, "--backup-region")
	})

	t.Run("no backup flags without backup config", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertNotContainsFlag(t, c.Args, "--pgbackrest-cert-dir")
		assertNotContainsFlag(t, c.Args, "--pgbackrest-port")
		assertNotContainsFlag(t, c.Args, "--backup-type")
	})

	t.Run("s3 credentials secret injects AWS env vars", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeS3,
					S3: &multigresv1alpha1.S3BackupConfig{
						Bucket:            "my-bucket",
						Region:            "us-west-2",
						CredentialsSecret: "aws-creds",
					},
				},
			},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertContainsEnvVar(t, c.Env, "AWS_REGION")
		assertContainsEnvVar(t, c.Env, "AWS_ACCESS_KEY_ID")
		assertContainsEnvVar(t, c.Env, "AWS_SECRET_ACCESS_KEY")
	})

	t.Run("filesystem backup does not inject AWS env vars", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
				},
			},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertNotContainsEnvVar(t, c.Env, "AWS_REGION")
		assertNotContainsEnvVar(t, c.Env, "AWS_ACCESS_KEY_ID")
	})

	t.Run("with initdbArgs", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				InitdbArgs: "--locale-provider=icu --icu-locale=en_US.UTF-8",
			},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertContainsEnvVar(t, c.Env, "POSTGRES_INITDB_ARGS")
		for _, e := range c.Env {
			if e.Name == "POSTGRES_INITDB_ARGS" {
				if e.Value != "--locale-provider=icu --icu-locale=en_US.UTF-8" {
					t.Errorf("POSTGRES_INITDB_ARGS = %q, want %q",
						e.Value, "--locale-provider=icu --icu-locale=en_US.UTF-8")
				}
				return
			}
		}
	})

	t.Run("without initdbArgs", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertNotContainsEnvVar(t, c.Env, "POSTGRES_INITDB_ARGS")
	})

	t.Run("has POSTGRES_INITDB_EXTRA_CONF env when postgresConfigRef set", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{
			PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
				Name: "my-pg-config",
				Key:  "custom.conf",
			},
		}}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertContainsEnvVar(t, c.Env, "POSTGRES_INITDB_EXTRA_CONF")
		for _, e := range c.Env {
			if e.Name == "POSTGRES_INITDB_EXTRA_CONF" {
				if e.Value != PostgresConfigFilePath {
					t.Errorf(
						"POSTGRES_INITDB_EXTRA_CONF = %q, want %q",
						e.Value,
						PostgresConfigFilePath,
					)
				}
			}
		}
	})

	t.Run("no POSTGRES_INITDB_EXTRA_CONF env when postgresConfigRef nil", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{}}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertNotContainsEnvVar(t, c.Env, "POSTGRES_INITDB_EXTRA_CONF")
	})

	t.Run("has postgres config volume mount when postgresConfigRef set", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{
			PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
				Name: "my-pg-config",
				Key:  "custom.conf",
			},
		}}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertContainsVolumeMount(t, c.VolumeMounts, PostgresConfigVolumeName)
		for _, m := range c.VolumeMounts {
			if m.Name == PostgresConfigVolumeName {
				if m.MountPath != PostgresConfigMountPath {
					t.Errorf(
						"postgres config mount path = %q, want %q",
						m.MountPath,
						PostgresConfigMountPath,
					)
				}
				if !m.ReadOnly {
					t.Error("postgres config volume mount should be read-only")
				}
			}
		}
	})

	t.Run("no postgres config volume mount when postgresConfigRef nil", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{}}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		for _, m := range c.VolumeMounts {
			if m.Name == PostgresConfigVolumeName {
				t.Error(
					"should not have postgres config volume mount when postgresConfigRef is nil",
				)
			}
		}
	})
}

func TestS3EnvVars(t *testing.T) {
	t.Run("nil backup returns nil", func(t *testing.T) {
		got := s3EnvVars(nil)
		if got != nil {
			t.Errorf("s3EnvVars(nil) = %v, want nil", got)
		}
	})

	t.Run("filesystem backup returns nil", func(t *testing.T) {
		got := s3EnvVars(&multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeFilesystem,
		})
		if got != nil {
			t.Errorf("s3EnvVars(filesystem) = %v, want nil", got)
		}
	})

	t.Run("s3 with region only", func(t *testing.T) {
		got := s3EnvVars(&multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket: "b",
				Region: "eu-west-1",
			},
		})
		if len(got) != 1 || got[0].Name != "AWS_REGION" || got[0].Value != "eu-west-1" {
			t.Errorf("s3EnvVars(region-only) = %v, want [{AWS_REGION eu-west-1}]", got)
		}
	})

	t.Run("s3 with credentials secret", func(t *testing.T) {
		got := s3EnvVars(&multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket:            "b",
				Region:            "us-east-1",
				CredentialsSecret: "my-secret",
			},
		})
		if len(got) != 3 {
			t.Fatalf("s3EnvVars(full) returned %d vars, want 3", len(got))
		}
		assertContainsEnvVar(t, got, "AWS_REGION")
		assertContainsEnvVar(t, got, "AWS_ACCESS_KEY_ID")
		assertContainsEnvVar(t, got, "AWS_SECRET_ACCESS_KEY")

		// Verify it references the correct secret
		for _, e := range got {
			if e.Name == "AWS_ACCESS_KEY_ID" {
				if e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
					t.Fatal("AWS_ACCESS_KEY_ID missing SecretKeyRef")
				}
				if e.ValueFrom.SecretKeyRef.Name != "my-secret" {
					t.Errorf("AWS_ACCESS_KEY_ID secret = %q, want %q",
						e.ValueFrom.SecretKeyRef.Name, "my-secret")
				}
			}
		}
	})

	t.Run(
		"s3 with serviceAccountName only does not inject AWS credential env vars",
		func(t *testing.T) {
			got := s3EnvVars(&multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3: &multigresv1alpha1.S3BackupConfig{
					Bucket:             "b",
					Region:             "us-east-1",
					ServiceAccountName: "multigres-backup",
				},
			})
			// Should only have AWS_REGION, no credential env vars
			if len(got) != 1 {
				t.Fatalf(
					"s3EnvVars(serviceAccountName-only) returned %d vars, want 1 (AWS_REGION only)",
					len(got),
				)
			}
			assertContainsEnvVar(t, got, "AWS_REGION")
			assertNotContainsEnvVar(t, got, "AWS_ACCESS_KEY_ID")
			assertNotContainsEnvVar(t, got, "AWS_SECRET_ACCESS_KEY")
		},
	)

	t.Run("s3 with no region", func(t *testing.T) {
		got := s3EnvVars(&multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3: &multigresv1alpha1.S3BackupConfig{
				Bucket:            "b",
				CredentialsSecret: "my-secret",
			},
		})
		// Should have 2 vars: KEY_ID and SECRET_KEY, but no REGION
		if len(got) != 2 {
			t.Fatalf("s3EnvVars(no-region) returned %d vars, want 2", len(got))
		}
		assertNotContainsEnvVar(t, got, "AWS_REGION")
		assertContainsEnvVar(t, got, "AWS_ACCESS_KEY_ID")
	})
}

func TestBuildSharedBackupVolume_S3(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeS3,
				S3: &multigresv1alpha1.S3BackupConfig{
					Bucket: "my-bucket",
					Region: "us-east-1",
				},
			},
		},
	}
	vol := buildSharedBackupVolume(shard, "zone-1")

	if vol.Name != BackupVolumeName {
		t.Errorf("volume name = %q, want %q", vol.Name, BackupVolumeName)
	}
	if vol.EmptyDir == nil {
		t.Error("S3 backup volume should use EmptyDir, got PVC or other source")
	}
	if vol.PersistentVolumeClaim != nil {
		t.Error("S3 backup volume should NOT use PersistentVolumeClaim")
	}
}

func assertContainsFlag(t *testing.T, args []string, want string) {
	t.Helper()
	for _, arg := range args {
		if arg == want {
			return
		}
	}
	t.Errorf("args %v does not contain flag %q", args, want)
}

func TestBuildMultipoolerContainer_WithObservability(t *testing.T) {
	c := buildMultipoolerContainer(
		otelShard(),
		multigresv1alpha1.PoolSpec{},
		"primary",
		"zone1",
		"p-otel1234",
	)
	assertContainsOTELEnvVar(t, c.Env, "buildMultipoolerContainer")
	assertEnvVarValue(
		t,
		c.Env,
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"http://vmagent:4318/v1/metrics",
	)
	assertOTELResourceAttribute(t, c.Env, "multigres.project=project-ref-123")
	assertOTELResourceAttribute(t, c.Env, "multigres.cluster=test-cluster")
	assertOTELResourceAttribute(t, c.Env, "multigres.component=multipooler")
}

func TestBuildMultiorchContainer_WithObservability(t *testing.T) {
	c := buildMultiorchContainer(otelShard(), "zone1")
	assertContainsOTELEnvVar(t, c.Env, "buildMultiorchContainer")
	assertEnvVarValue(
		t,
		c.Env,
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"http://vmagent:4318/v1/metrics",
	)
	assertOTELResourceAttribute(t, c.Env, "multigres.project=project-ref-123")
	assertOTELResourceAttribute(t, c.Env, "multigres.cluster=test-cluster")
	assertOTELResourceAttribute(t, c.Env, "multigres.component=multiorch")
}

func assertContainsOTELEnvVar(t *testing.T, envVars []corev1.EnvVar, fnName string) {
	t.Helper()
	for _, e := range envVars {
		if e.Name == "OTEL_EXPORTER_OTLP_ENDPOINT" {
			return
		}
	}
	t.Errorf("%s: expected OTEL_EXPORTER_OTLP_ENDPOINT env var, got none", fnName)
}

func assertContainsEnvVar(t *testing.T, envVars []corev1.EnvVar, name string) {
	t.Helper()
	for _, e := range envVars {
		if e.Name == name {
			return
		}
	}
	t.Errorf("expected env var %q, got none", name)
}

func assertEnvVarValue(t *testing.T, envVars []corev1.EnvVar, name, want string) {
	t.Helper()
	for _, e := range envVars {
		if e.Name == name {
			if e.Value != want {
				t.Errorf("env var %q = %q, want %q", name, e.Value, want)
			}
			return
		}
	}
	t.Errorf("expected env var %q, got none", name)
}

func assertOTELResourceAttribute(t *testing.T, envVars []corev1.EnvVar, want string) {
	t.Helper()
	for _, e := range envVars {
		if e.Name == "OTEL_RESOURCE_ATTRIBUTES" {
			if !strings.Contains(e.Value, want) {
				t.Errorf("OTEL_RESOURCE_ATTRIBUTES = %q, want it to contain %q", e.Value, want)
			}
			return
		}
	}
	t.Errorf("expected OTEL_RESOURCE_ATTRIBUTES to contain %q, got none", want)
}

func assertNotContainsEnvVar(t *testing.T, envVars []corev1.EnvVar, name string) {
	t.Helper()
	for _, e := range envVars {
		if e.Name == name {
			t.Errorf("unexpected env var %q found", name)
			return
		}
	}
}

func assertNotContainsFlag(t *testing.T, args []string, prefix string) {
	t.Helper()
	for _, arg := range args {
		if len(arg) >= len(prefix) && arg[:len(prefix)] == prefix {
			t.Errorf("args should not contain flag starting with %q, but found %q", prefix, arg)
			return
		}
	}
}

func assertContainsVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name string) {
	t.Helper()
	for _, m := range mounts {
		if m.Name == name {
			return
		}
	}
	t.Errorf("volume mounts %v does not contain mount %q", mounts, name)
}

func assertReadOnlyVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name, mountPath string) {
	t.Helper()
	for _, m := range mounts {
		if m.Name != name {
			continue
		}
		if m.MountPath != mountPath {
			t.Errorf("mount %q path = %q, want %q", name, m.MountPath, mountPath)
		}
		if !m.ReadOnly {
			t.Errorf("mount %q should be read-only", name)
		}
		return
	}
	t.Errorf("volume mounts %v does not contain mount %q", mounts, name)
}

func assertNotContainsVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name string) {
	t.Helper()
	for _, m := range mounts {
		if m.Name == name {
			t.Errorf("volume mounts should not contain mount %q", name)
			return
		}
	}
}

func TestBuildPgBackRestCertVolume(t *testing.T) {
	t.Run("nil when no backup", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{}}
		vol := buildPgBackRestCertVolume(shard)
		if vol != nil {
			t.Fatalf("expected nil volume when no backup, got %+v", vol)
		}
	})

	t.Run("auto-generated projected volume", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard"},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
				},
			},
		}
		vol := buildPgBackRestCertVolume(shard)
		if vol == nil {
			t.Fatal("expected non-nil volume for auto-generated certs")
		}
		if vol.Name != PgBackRestCertVolumeName {
			t.Errorf("volume name = %q, want %q", vol.Name, PgBackRestCertVolumeName)
		}
		if vol.Projected == nil {
			t.Fatal("expected projected volume source for auto-generated certs")
		}
		if len(vol.Projected.Sources) != 2 {
			t.Fatalf("expected 2 projection sources, got %d", len(vol.Projected.Sources))
		}

		// Verify CA source
		caSource := vol.Projected.Sources[0]
		if caSource.Secret.Name != "test-shard-pgbackrest-ca" {
			t.Errorf(
				"CA secret name = %q, want %q",
				caSource.Secret.Name,
				"test-shard-pgbackrest-ca",
			)
		}
		if len(caSource.Secret.Items) != 1 || caSource.Secret.Items[0].Key != "ca.crt" {
			t.Errorf("CA items = %+v, want [{Key:ca.crt Path:ca.crt}]", caSource.Secret.Items)
		}

		// Verify TLS source (key renaming)
		tlsSource := vol.Projected.Sources[1]
		if tlsSource.Secret.Name != "test-shard-pgbackrest-tls" {
			t.Errorf(
				"TLS secret name = %q, want %q",
				tlsSource.Secret.Name,
				"test-shard-pgbackrest-tls",
			)
		}
		if len(tlsSource.Secret.Items) != 2 {
			t.Fatalf("expected 2 TLS items, got %d", len(tlsSource.Secret.Items))
		}
		if tlsSource.Secret.Items[0].Key != "tls.crt" ||
			tlsSource.Secret.Items[0].Path != "pgbackrest.crt" {
			t.Errorf(
				"TLS item[0] = %+v, want {Key:tls.crt Path:pgbackrest.crt}",
				tlsSource.Secret.Items[0],
			)
		}
		if tlsSource.Secret.Items[1].Key != "tls.key" ||
			tlsSource.Secret.Items[1].Path != "pgbackrest.key" {
			t.Errorf(
				"TLS item[1] = %+v, want {Key:tls.key Path:pgbackrest.key}",
				tlsSource.Secret.Items[1],
			)
		}
	})

	t.Run("user-provided Secret volume", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeS3,
					S3: &multigresv1alpha1.S3BackupConfig{
						Bucket: "b", Region: "r",
					},
					PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{
						SecretName: "my-custom-certs",
					},
				},
			},
		}
		vol := buildPgBackRestCertVolume(shard)
		if vol == nil {
			t.Fatal("expected non-nil volume for user-provided certs")
		}
		if vol.Projected == nil {
			t.Fatal(
				"expected projected volume for user-provided certs (key renaming for cert-manager compat)",
			)
		}
		if len(vol.Projected.Sources) != 1 {
			t.Fatalf(
				"expected 1 projection source for user-provided, got %d",
				len(vol.Projected.Sources),
			)
		}
		src := vol.Projected.Sources[0]
		if src.Secret.Name != "my-custom-certs" {
			t.Errorf("secret name = %q, want %q", src.Secret.Name, "my-custom-certs")
		}
		if len(src.Secret.Items) != 3 {
			t.Fatalf(
				"expected 3 items (ca.crt, tls.crt→pgbackrest.crt, tls.key→pgbackrest.key), got %d",
				len(src.Secret.Items),
			)
		}
	})

	t.Run("auto-generated when PgBackRestTLS is nil", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "shard1"},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type:          multigresv1alpha1.BackupTypeFilesystem,
					PgBackRestTLS: nil,
				},
			},
		}
		vol := buildPgBackRestCertVolume(shard)
		if vol == nil {
			t.Fatal("expected non-nil volume when PgBackRestTLS is nil (auto-generated)")
		}
		if vol.Projected == nil {
			t.Error("expected projected volume for auto-generated fallback")
		}
	})

	t.Run("auto-generated when SecretName is empty", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "shard1"},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					PgBackRestTLS: &multigresv1alpha1.PgBackRestTLSConfig{
						SecretName: "",
					},
				},
			},
		}
		vol := buildPgBackRestCertVolume(shard)
		if vol == nil {
			t.Fatal("expected non-nil volume when SecretName is empty (auto-generated)")
		}
		if vol.Projected == nil {
			t.Error("expected projected volume for auto-generated fallback")
		}
	})
}

func TestBuildPgBackRestCipherKeyVolume(t *testing.T) {
	t.Run("nil when no backup", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{}}
		vol := buildPgBackRestCipherKeyVolume(shard)
		if vol != nil {
			t.Fatalf("expected nil volume when no backup, got %+v", vol)
		}
	})

	t.Run("nil when backup configured but encryption disabled", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
				},
			},
		}
		vol := buildPgBackRestCipherKeyVolume(shard)
		if vol != nil {
			t.Fatalf("expected nil volume when encryption disabled, got %+v", vol)
		}
	})

	t.Run("user-provided secret volume", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{Name: "test-shard"},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Encryption: &multigresv1alpha1.BackupEncryptionConfig{
						SecretName: "my-cipher-secret",
					},
				},
			},
		}
		vol := buildPgBackRestCipherKeyVolume(shard)
		if vol == nil {
			t.Fatal("expected non-nil volume for user-provided cipher key")
		}
		if vol.Name != PgBackRestCipherKeyVolumeName {
			t.Errorf("volume name = %q, want %q", vol.Name, PgBackRestCipherKeyVolumeName)
		}
		if vol.Secret == nil {
			t.Fatal("expected Secret volume source")
		}
		if vol.Secret.SecretName != "my-cipher-secret" {
			t.Errorf("secret name = %q, want %q", vol.Secret.SecretName, "my-cipher-secret")
		}
		if vol.Secret.DefaultMode == nil || *vol.Secret.DefaultMode != 0o444 {
			t.Errorf("defaultMode = %v, want 0444", vol.Secret.DefaultMode)
		}
	})
}

func TestPgctldContainer_PgBackRestCertArgs(t *testing.T) {
	t.Run("cert args present when backup configured", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
				},
			},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertContainsFlag(t, c.Args, "--pgbackrest-cert-dir=/certs/pgbackrest")
		assertContainsFlag(t, c.Args, "--pgbackrest-port=8432")
		assertContainsVolumeMount(t, c.VolumeMounts, PgBackRestCertVolumeName)

		// Verify volume mount is read-only
		for _, m := range c.VolumeMounts {
			if m.Name == PgBackRestCertVolumeName && !m.ReadOnly {
				t.Error("pgbackrest cert volume mount should be read-only")
			}
		}
	})

	t.Run("no cert args when no backup", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{}}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertNotContainsFlag(t, c.Args, "--pgbackrest-cert-dir")
		assertNotContainsFlag(t, c.Args, "--pgbackrest-port")
		assertNotContainsVolumeMount(t, c.VolumeMounts, PgBackRestCertVolumeName)
	})

	t.Run("no cipher key flag or mount even when encryption enabled", func(t *testing.T) {
		// Upstream only wired --pgbackrest-cipher-key-file into multipooler,
		// not pgctld; pgctld must never get the cipher volume/flag.
		shard := &multigresv1alpha1.Shard{
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Encryption: &multigresv1alpha1.BackupEncryptionConfig{
						SecretName: "test-cipher-secret", // #nosec G101 -- Secret object name, not a credential
					},
				},
			},
		}
		c := buildPgctldSidecar(shard, multigresv1alpha1.PoolSpec{})
		assertNotContainsFlag(t, c.Args, "--pgbackrest-cipher-key-file")
		assertNotContainsVolumeMount(t, c.VolumeMounts, PgBackRestCipherKeyVolumeName)
	})
}

func TestMultipoolerSidecar_PgBackRestCertArgs(t *testing.T) {
	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"multigres.com/cluster": "test"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:  "topo:2379",
				RootPath: "/multigres/global",
			},
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "s1",
		},
	}

	t.Run("cert args present when backup configured", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "b", Region: "r"},
		}
		c := buildMultipoolerContainer(
			shard,
			multigresv1alpha1.PoolSpec{},
			"primary",
			"zone1",
			"p-backup123",
		)
		assertContainsFlag(t, c.Args, "--pgbackrest-cert-file=/certs/pgbackrest/pgbackrest.crt")
		assertContainsFlag(t, c.Args, "--pgbackrest-key-file=/certs/pgbackrest/pgbackrest.key")
		assertContainsFlag(t, c.Args, "--pgbackrest-ca-file=/certs/pgbackrest/ca.crt")
		assertContainsVolumeMount(t, c.VolumeMounts, PgBackRestCertVolumeName)
	})

	t.Run("no cert args when no backup", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		c := buildMultipoolerContainer(
			shard,
			multigresv1alpha1.PoolSpec{},
			"primary",
			"zone1",
			"p-noback123",
		)
		assertNotContainsFlag(t, c.Args, "--pgbackrest-cert-file")
		assertNotContainsFlag(t, c.Args, "--pgbackrest-key-file")
		assertNotContainsFlag(t, c.Args, "--pgbackrest-ca-file")
		assertNotContainsVolumeMount(t, c.VolumeMounts, PgBackRestCertVolumeName)
	})

	t.Run("cipher key flag and mount present when encryption enabled", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "b", Region: "r"},
			Encryption: &multigresv1alpha1.BackupEncryptionConfig{
				SecretName: "test-cipher-secret", // #nosec G101 -- Secret object name, not a credential
			},
		}
		c := buildMultipoolerContainer(
			shard,
			multigresv1alpha1.PoolSpec{},
			"primary",
			"zone1",
			"p-cipher123",
		)
		assertContainsFlag(
			t,
			c.Args,
			"--pgbackrest-cipher-key-file=/secrets/pgbackrest-cipher/keys.json",
		)
		assertContainsVolumeMount(t, c.VolumeMounts, PgBackRestCipherKeyVolumeName)
		for _, m := range c.VolumeMounts {
			if m.Name == PgBackRestCipherKeyVolumeName && !m.ReadOnly {
				t.Error("pgbackrest cipher key volume mount should be read-only")
			}
		}
	})

	t.Run("no cipher key flag or mount when encryption disabled", func(t *testing.T) {
		shard := baseShard.DeepCopy()
		shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
			Type: multigresv1alpha1.BackupTypeS3,
			S3:   &multigresv1alpha1.S3BackupConfig{Bucket: "b", Region: "r"},
		}
		c := buildMultipoolerContainer(
			shard,
			multigresv1alpha1.PoolSpec{},
			"primary",
			"zone1",
			"p-nocipher123",
		)
		assertNotContainsFlag(t, c.Args, "--pgbackrest-cipher-key-file")
		assertNotContainsVolumeMount(t, c.VolumeMounts, PgBackRestCipherKeyVolumeName)
	})
}

func TestBuildPoolVolumes_CertVolumePresence(t *testing.T) {
	t.Run("postgres password secret volume present", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-shard",
				Labels: map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
					Name: "multigres-admin-password",
					Key:  PostgresPasswordSecretKey,
				},
			},
		}
		volumes := buildPoolVolumes(shard, "zone1")
		for _, v := range volumes {
			if v.Name != PostgresPasswordVolumeName {
				continue
			}
			if v.Secret == nil {
				t.Fatal("postgres password volume should use Secret source")
			}
			if v.Secret.SecretName != "multigres-admin-password" {
				t.Errorf(
					"postgres password SecretName = %q, want %q",
					v.Secret.SecretName,
					"multigres-admin-password",
				)
			}
			if v.Secret.DefaultMode == nil || *v.Secret.DefaultMode != 0o444 {
				t.Errorf("postgres password defaultMode = %v, want 0444", v.Secret.DefaultMode)
			}
			if len(v.Secret.Items) != 1 ||
				v.Secret.Items[0].Key != PostgresPasswordSecretKey ||
				v.Secret.Items[0].Path != PostgresPasswordSecretKey {
				t.Errorf(
					"postgres password Secret items = %+v, want password key projected to password",
					v.Secret.Items,
				)
			}
			return
		}
		t.Fatalf("expected postgres password Secret volume in pool volumes")
	})

	t.Run("cert volume present when backup configured", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-shard",
				Labels: map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
				},
			},
		}
		volumes := buildPoolVolumes(shard, "zone1")
		found := false
		for _, v := range volumes {
			if v.Name == PgBackRestCertVolumeName {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected pgbackrest-certs volume when backup configured")
		}
	})

	t.Run("no cert volume when no backup", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.ShardSpec{},
		}
		volumes := buildPoolVolumes(shard, "zone1")
		for _, v := range volumes {
			if v.Name == PgBackRestCertVolumeName {
				t.Error("cert volume should not be present when no backup configured")
			}
		}
	})

	t.Run("cipher key volume present when encryption enabled", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-shard",
				Labels: map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
					Encryption: &multigresv1alpha1.BackupEncryptionConfig{
						SecretName: "test-cipher-secret", // #nosec G101 -- Secret object name, not a credential
					},
				},
			},
		}
		volumes := buildPoolVolumes(shard, "zone1")
		found := false
		for _, v := range volumes {
			if v.Name == PgBackRestCipherKeyVolumeName {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected pgbackrest-cipher volume when encryption configured")
		}
	})

	t.Run("no cipher key volume when encryption disabled", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-shard",
				Labels: map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				Backup: &multigresv1alpha1.BackupConfig{
					Type: multigresv1alpha1.BackupTypeFilesystem,
				},
			},
		}
		volumes := buildPoolVolumes(shard, "zone1")
		for _, v := range volumes {
			if v.Name == PgBackRestCipherKeyVolumeName {
				t.Error("cipher key volume should not be present when encryption disabled")
			}
		}
	})

	t.Run("postgres config volume present when postgresConfigRef set", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-shard",
				Labels: map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				PostgresConfigRef: &multigresv1alpha1.PostgresConfigRef{
					Name: "my-pg-config",
					Key:  "custom.conf",
				},
			},
		}
		volumes := buildPoolVolumes(shard, "zone1")
		found := false
		for _, v := range volumes {
			if v.Name == PostgresConfigVolumeName {
				found = true
				if v.ConfigMap == nil {
					t.Error("postgres config volume should use ConfigMap source")
				} else {
					if v.ConfigMap.Name != "my-pg-config" {
						t.Errorf("postgres config ConfigMap name = %q, want %q",
							v.ConfigMap.Name, "my-pg-config")
					}
					if len(v.ConfigMap.Items) != 1 || v.ConfigMap.Items[0].Key != "custom.conf" ||
						v.ConfigMap.Items[0].Path != "postgresql.conf" {
						t.Errorf(
							"postgres config ConfigMap items = %+v, want [{Key:custom.conf Path:postgresql.conf}]",
							v.ConfigMap.Items,
						)
					}
				}
				break
			}
		}
		if !found {
			t.Error("expected postgres-config volume in pool volumes")
		}
	})

	t.Run("no postgres config volume when postgresConfigRef nil", func(t *testing.T) {
		shard := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-shard",
				Labels: map[string]string{"multigres.com/cluster": "test"},
			},
			Spec: multigresv1alpha1.ShardSpec{},
		}
		volumes := buildPoolVolumes(shard, "zone1")
		for _, v := range volumes {
			if v.Name == PostgresConfigVolumeName {
				t.Error("should not have postgres config volume when postgresConfigRef is nil")
			}
		}
	})
}

func TestBuildPoolServiceID(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		podName := "minimal-postgres-default-0-inf-pool-default-zone-a-a3a0d77b-1"
		id1 := BuildPoolServiceID(podName)
		id2 := BuildPoolServiceID(podName)
		if id1 != id2 {
			t.Errorf("non-deterministic: %q != %q", id1, id2)
		}
	})

	t.Run("format", func(t *testing.T) {
		id := BuildPoolServiceID("some-pod-name")
		pattern := regexp.MustCompile(`^p-[0-9a-f]{8}$`)
		if !pattern.MatchString(id) {
			t.Errorf("BuildPoolServiceID(%q) = %q, want format p-[0-9a-f]{8}", "some-pod-name", id)
		}
	})

	t.Run("different inputs produce different outputs", func(t *testing.T) {
		id1 := BuildPoolServiceID("pod-a")
		id2 := BuildPoolServiceID("pod-b")
		if id1 == id2 {
			t.Errorf("collision: BuildPoolServiceID(%q) == BuildPoolServiceID(%q) == %q",
				"pod-a", "pod-b", id1)
		}
	})

	t.Run("length is always 10", func(t *testing.T) {
		for _, name := range []string{"a", "short", "a-very-long-pod-name-that-goes-on-and-on"} {
			id := BuildPoolServiceID(name)
			if len(id) != 10 {
				t.Errorf("BuildPoolServiceID(%q) = %q (len %d), want len 10", name, id, len(id))
			}
		}
	})
}
