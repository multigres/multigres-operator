package shard

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestPgBackRestSidecarOTEL(t *testing.T) {
	for _, mode := range []string{"pool", "lite"} {
		t.Run(mode, func(t *testing.T) {
			for _, setting := range []string{"configured", "metrics-only", "inherited", "disabled", "unset"} {
				t.Run(setting, func(t *testing.T) {
					for _, key := range []string{
						"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
						"OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER",
						"OTEL_LOGS_EXPORTER", "OTEL_METRIC_EXPORT_INTERVAL",
						"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
						"OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION", "OTEL_TRACES_SAMPLER",
						"OTEL_RESOURCE_ATTRIBUTES",
					} {
						t.Setenv(key, "")
					}
					s := sidecarTestShard()
					s.Labels = otelShard().Labels
					s.Annotations = otelShard().Annotations
					endpoint := "http://inherited:4318"
					metricsEndpoint := "http://inherited:4318/v1/metrics"
					if setting != "unset" {
						t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
						t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", metricsEndpoint)
						t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
						t.Setenv(
							"OTEL_RESOURCE_ATTRIBUTES",
							"deployment.environment=test,multigres.component=operator",
						)
					}
					switch setting {
					case "configured", "metrics-only":
						endpoint = "http://configured:4318"
						metricsEndpoint = "http://configured:4318/v1/metrics"
						s.Spec.Observability = &multigresv1alpha1.ObservabilityConfig{
							OTLPEndpoint:        endpoint,
							OTLPMetricsEndpoint: metricsEndpoint,
							MetricsExporter:     "otlp",
							TracesSampler:       "multigres_custom",
							SamplingConfigRef: &multigresv1alpha1.SamplingConfigRef{
								Name: "sampling",
								Key:  "custom.yaml",
							},
						}
						if setting == "metrics-only" {
							endpoint = ""
							s.Spec.Observability.OTLPEndpoint = ""
							t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
						}
					case "disabled":
						s.Spec.Observability = &multigresv1alpha1.ObservabilityConfig{
							OTLPEndpoint: "disabled",
						}
					}
					var pod *corev1.Pod
					runtimeName := "multipooler"
					if mode == "lite" {
						s.Spec.Mode = multigresv1alpha1.ModeLite
						pod = buildLiteTestPod(t, s)
						runtimeName = "minigres"
					} else {
						pod = buildSidecarPod(t, s)
					}
					c := sidecarTestContainer(t, pod, PgBackRestSidecarContainerName)
					assert.Equal(t, buildRuntimeOTELEnvVars(s, "pgbackrestd"), c.Env)
					env := map[string]string{}
					for _, e := range c.Env {
						_, duplicate := env[e.Name]
						assert.False(t, duplicate, "duplicate env %s", e.Name)
						env[e.Name] = e.Value
					}
					if setting == "disabled" || setting == "unset" {
						assert.Empty(t, c.Env)
					} else {
						assert.Equal(t, endpoint, env["OTEL_EXPORTER_OTLP_ENDPOINT"])
						assert.Equal(t, metricsEndpoint, env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"])
						assert.Equal(t, "otlp", env["OTEL_METRICS_EXPORTER"])
						assert.Equal(
							t,
							"deployment.environment=test,multigres.project=project-ref-123,multigres.cluster=test-cluster,multigres.component=pgbackrestd",
							env["OTEL_RESOURCE_ATTRIBUTES"],
						)
					}
					if setting == "configured" || setting == "metrics-only" {
						assert.Equal(t, "multigres_custom", env["OTEL_TRACES_SAMPLER"])
						assert.Equal(t, "/etc/otel/custom.yaml", env["OTEL_TRACES_SAMPLER_CONFIG"])
						vol := findVolume(
							pod.Spec.Volumes,
							multigresv1alpha1.OTELSamplingVolumeName,
						)
						require.NotNil(t, vol)
						require.NotNil(t, vol.ConfigMap)
						assert.Equal(t, "sampling", vol.ConfigMap.Name)
						assert.Contains(t, c.VolumeMounts, corev1.VolumeMount{
							Name:      multigresv1alpha1.OTELSamplingVolumeName,
							MountPath: "/etc/otel",
							ReadOnly:  true,
						})
					} else {
						assert.False(t, hasMountName(c, multigresv1alpha1.OTELSamplingVolumeName))
					}
					assert.Empty(t, sidecarSecretLeaks(t, pod))
					assert.Equal(
						t,
						runtimeName+",postgres,postgres-exporter",
						pod.Annotations[SkipContainersAnnotation],
					)
					assert.Equal(t, c.Env, sidecarTestContainer(t, func() *corev1.Pod {
						if mode == "lite" {
							return buildLiteTestPod(t, s)
						}
						return buildSidecarPod(t, s)
					}(), PgBackRestSidecarContainerName).Env, "stable env ordering")
				})
			}
		})
	}
}
