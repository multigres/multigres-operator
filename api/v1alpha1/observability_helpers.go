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

import (
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	// OTELSamplingVolumeName is the volume name for the sampling config ConfigMap.
	OTELSamplingVolumeName = "otel-sampling-config"

	// OTELSamplingMountPath is where the sampling config ConfigMap is mounted.
	OTELSamplingMountPath = "/etc/otel"
)

// BuildOTELEnvVars returns OTEL env vars resolved from cfg or the operator env.
// Set otlpEndpoint to "disabled" to suppress all OTEL vars.
func BuildOTELEnvVars(cfg *ObservabilityConfig) []corev1.EnvVar {
	return BuildOTELEnvVarsWithResourceAttributes(cfg, nil)
}

// BuildOTELEnvVarsWithResourceAttributes appends OTEL_RESOURCE_ATTRIBUTES for
// runtime containers.
func BuildOTELEnvVarsWithResourceAttributes(
	cfg *ObservabilityConfig,
	resourceAttributes map[string]string,
) []corev1.EnvVar {
	endpoint := envOrCRD(
		cfg,
		func(c *ObservabilityConfig) string { return c.OTLPEndpoint },
		"OTEL_EXPORTER_OTLP_ENDPOINT",
	)
	if endpoint == "disabled" {
		return nil
	}

	metricsEndpoint := envOrCRD(
		cfg,
		func(c *ObservabilityConfig) string { return c.OTLPMetricsEndpoint },
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	)
	if endpoint == "" && metricsEndpoint == "" {
		return nil
	}

	var vars []corev1.EnvVar
	if endpoint != "" {
		vars = append(vars, corev1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: endpoint})
	}
	if metricsEndpoint != "" {
		vars = append(vars, corev1.EnvVar{
			Name:  "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
			Value: metricsEndpoint,
		})
	}

	appendIfSet := func(envName string, crdValue, fallbackEnv string) {
		v := crdValue
		if v == "" {
			v = os.Getenv(fallbackEnv)
		}
		if v != "" {
			vars = append(vars, corev1.EnvVar{Name: envName, Value: v})
		}
	}

	var proto, traces, metrics, logs, interval, temporality, histogramAgg, sampler string
	if cfg != nil {
		proto = cfg.OTLPProtocol
		traces = cfg.TracesExporter
		metrics = cfg.MetricsExporter
		logs = cfg.LogsExporter
		interval = cfg.MetricExportInterval
		temporality = cfg.MetricsTemporality
		histogramAgg = cfg.HistogramAggregation
		sampler = cfg.TracesSampler
	}

	appendIfSet("OTEL_EXPORTER_OTLP_PROTOCOL", proto, "OTEL_EXPORTER_OTLP_PROTOCOL")
	appendIfSet("OTEL_TRACES_EXPORTER", traces, "OTEL_TRACES_EXPORTER")
	appendIfSet("OTEL_METRICS_EXPORTER", metrics, "OTEL_METRICS_EXPORTER")
	appendIfSet("OTEL_LOGS_EXPORTER", logs, "OTEL_LOGS_EXPORTER")
	appendIfSet("OTEL_METRIC_EXPORT_INTERVAL", interval, "OTEL_METRIC_EXPORT_INTERVAL")
	appendIfSet(
		"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
		temporality,
		"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
	)
	appendIfSet(
		"OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
		histogramAgg,
		"OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
	)
	appendIfSet("OTEL_TRACES_SAMPLER", sampler, "OTEL_TRACES_SAMPLER")
	if attrs := buildOTELResourceAttributes(resourceAttributes); attrs != "" {
		vars = append(vars, corev1.EnvVar{Name: "OTEL_RESOURCE_ATTRIBUTES", Value: attrs})
	}

	if cfg != nil && cfg.SamplingConfigRef != nil {
		key := cfg.SamplingConfigRef.Key
		if key == "" {
			key = "sampling-config.yaml"
		}
		vars = append(vars, corev1.EnvVar{
			Name:  "OTEL_TRACES_SAMPLER_CONFIG",
			Value: filepath.Join(OTELSamplingMountPath, key),
		})
	}

	return vars
}

func buildOTELResourceAttributes(attrs map[string]string) string {
	if len(attrs) == 0 {
		return ""
	}

	orderedKeys := []string{
		"multigres.project",
		"multigres.cluster",
		"multigres.component",
	}
	generated := make(map[string]string, len(attrs))
	for _, key := range orderedKeys {
		if value := attrs[key]; value != "" {
			generated[key] = value
		}
	}
	if len(generated) == 0 {
		return ""
	}

	parts := filterOTELResourceAttributes(os.Getenv("OTEL_RESOURCE_ATTRIBUTES"), generated)
	for _, key := range orderedKeys {
		if value := generated[key]; value != "" {
			parts = append(parts, key+"="+value)
		}
	}
	return strings.Join(parts, ",")
}

func filterOTELResourceAttributes(raw string, generated map[string]string) []string {
	if raw == "" {
		return nil
	}
	parts := splitOTELResourceAttributes(raw)
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, _, ok := strings.Cut(part, "=")
		if ok {
			if _, replace := generated[strings.TrimSpace(key)]; replace {
				continue
			}
		}
		filtered = append(filtered, part)
	}
	return filtered
}

func splitOTELResourceAttributes(raw string) []string {
	var parts []string
	start := 0
	escaped := false
	for i, r := range raw {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if r == ',' {
			parts = append(parts, raw[start:i])
			start = i + 1
		}
	}
	return append(parts, raw[start:])
}

// BuildOTELSamplingVolume returns a Volume and VolumeMount for the sampling
// config ConfigMap. Returns nils if cfg is nil or SamplingConfigRef is not set.
func BuildOTELSamplingVolume(cfg *ObservabilityConfig) (*corev1.Volume, *corev1.VolumeMount) {
	if cfg == nil || cfg.SamplingConfigRef == nil {
		return nil, nil
	}

	vol := &corev1.Volume{
		Name: OTELSamplingVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: cfg.SamplingConfigRef.Name,
				},
			},
		},
	}
	mount := &corev1.VolumeMount{
		Name:      OTELSamplingVolumeName,
		MountPath: OTELSamplingMountPath,
		ReadOnly:  true,
	}
	return vol, mount
}

// envOrCRD returns the CRD field value if non-empty, otherwise falls back
// to the named environment variable.
func envOrCRD(
	cfg *ObservabilityConfig,
	getter func(*ObservabilityConfig) string,
	envName string,
) string {
	if cfg != nil {
		if v := getter(cfg); v != "" {
			return v
		}
	}
	return os.Getenv(envName)
}
