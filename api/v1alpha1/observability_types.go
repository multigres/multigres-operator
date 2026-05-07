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

// ObservabilityConfig defines OpenTelemetry settings for data-plane components.
// All fields are optional. When a field is empty, the operator's own
// corresponding OTEL_* environment variable is used as the default at
// reconcile time. Set otlpEndpoint to "disabled" to explicitly disable
// telemetry regardless of operator settings.
type ObservabilityConfig struct {
	// OTLPEndpoint is the OTLP collector endpoint URL.
	// Maps to OTEL_EXPORTER_OTLP_ENDPOINT.
	// Example: "http://otel-collector:4318"
	// Set to "disabled" to explicitly disable telemetry.
	// +optional
	OTLPEndpoint string `json:"otlpEndpoint,omitempty"`

	// OTLPMetricsEndpoint is the OTLP collector endpoint URL for metrics.
	// Maps to OTEL_EXPORTER_OTLP_METRICS_ENDPOINT.
	// Example: "http://vmagent:4318/v1/metrics"
	// +optional
	OTLPMetricsEndpoint string `json:"otlpMetricsEndpoint,omitempty"`

	// OTLPProtocol is the OTLP transport protocol.
	// Maps to OTEL_EXPORTER_OTLP_PROTOCOL.
	// +optional
	// +kubebuilder:validation:Enum="http/protobuf";"grpc"
	OTLPProtocol string `json:"otlpProtocol,omitempty"`

	// TracesExporter selects the trace exporter.
	// Maps to OTEL_TRACES_EXPORTER.
	// Upstream defaults to "none" — set to "otlp" to enable tracing.
	// +optional
	// +kubebuilder:validation:Enum=otlp;none;console
	TracesExporter string `json:"tracesExporter,omitempty"`

	// MetricsExporter selects the metrics exporter.
	// Maps to OTEL_METRICS_EXPORTER.
	// Upstream defaults to "none" — set to "otlp" to enable metrics export.
	// +optional
	// +kubebuilder:validation:Enum=otlp;none;console
	MetricsExporter string `json:"metricsExporter,omitempty"`

	// LogsExporter selects the logs exporter.
	// Maps to OTEL_LOGS_EXPORTER.
	// Upstream defaults to "none" — set to "otlp" to enable log export.
	// +optional
	// +kubebuilder:validation:Enum=otlp;none;console
	LogsExporter string `json:"logsExporter,omitempty"`

	// MetricExportInterval in milliseconds.
	// Maps to OTEL_METRIC_EXPORT_INTERVAL.
	// +optional
	MetricExportInterval string `json:"metricExportInterval,omitempty"`

	// MetricsTemporality selects the temporality preference.
	// Maps to OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE.
	// +optional
	// +kubebuilder:validation:Enum=cumulative;delta
	MetricsTemporality string `json:"metricsTemporality,omitempty"`

	// HistogramAggregation selects the default histogram aggregation.
	// Maps to OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION.
	// Use "base2_exponential_bucket_histogram" for native/exponential histograms
	// which are more compact and accurate than explicit bucket histograms.
	// +optional
	// +kubebuilder:validation:Enum=base2_exponential_bucket_histogram;explicit_bucket_histogram
	HistogramAggregation string `json:"histogramAggregation,omitempty"`

	// TracesSampler selects the sampler.
	// Maps to OTEL_TRACES_SAMPLER.
	// Standard values: always_on, always_off, parentbased_traceidratio.
	// Use "multigres_custom" for file-based sampling (requires SamplingConfigRef).
	// +optional
	TracesSampler string `json:"tracesSampler,omitempty"`

	// SamplingConfigRef references a ConfigMap containing the sampling config file.
	// Only used when tracesSampler is "multigres_custom".
	// The ConfigMap is mounted at /etc/otel/ and the file path is passed
	// via OTEL_TRACES_SAMPLER_CONFIG.
	// +optional
	SamplingConfigRef *SamplingConfigRef `json:"samplingConfigRef,omitempty"`
}

// SamplingConfigRef references a ConfigMap with sampling configuration.
type SamplingConfigRef struct {
	// Name is the ConfigMap name (must be in the same namespace as the CR).
	Name string `json:"name"`

	// Key is the data key within the ConfigMap.
	// +kubebuilder:default="sampling-config.yaml"
	// +optional
	Key string `json:"key,omitempty"`
}
