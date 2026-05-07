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
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestBuildOTELEnvVars(t *testing.T) {
	// Clear all OTEL env vars so host environment doesn't interfere.
	otelEnvVars := []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_TRACES_EXPORTER",
		"OTEL_METRICS_EXPORTER",
		"OTEL_LOGS_EXPORTER",
		"OTEL_METRIC_EXPORT_INTERVAL",
		"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
		"OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
		"OTEL_TRACES_SAMPLER",
		"OTEL_RESOURCE_ATTRIBUTES",
	}
	for _, env := range otelEnvVars {
		t.Setenv(env, "")
	}

	tests := map[string]struct {
		cfg  *ObservabilityConfig
		want []corev1.EnvVar
	}{
		"nil config returns nil": {
			cfg:  nil,
			want: nil,
		},
		"empty config returns nil": {
			cfg:  &ObservabilityConfig{},
			want: nil,
		},
		"disabled endpoint returns nil": {
			cfg:  &ObservabilityConfig{OTLPEndpoint: "disabled"},
			want: nil,
		},
		"endpoint only": {
			cfg: &ObservabilityConfig{
				OTLPEndpoint: "http://tempo:4318",
			},
			want: []corev1.EnvVar{
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://tempo:4318"},
			},
		},
		"metrics endpoint only": {
			cfg: &ObservabilityConfig{
				OTLPMetricsEndpoint: "http://vmagent:4318/v1/metrics",
				MetricsExporter:     "otlp",
			},
			want: []corev1.EnvVar{
				{
					Name:  "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
					Value: "http://vmagent:4318/v1/metrics",
				},
				{Name: "OTEL_METRICS_EXPORTER", Value: "otlp"},
			},
		},
		"all fields populated": {
			cfg: &ObservabilityConfig{
				OTLPEndpoint:         "http://tempo:4318",
				OTLPMetricsEndpoint:  "http://vmagent:4318/v1/metrics",
				OTLPProtocol:         "grpc",
				TracesExporter:       "otlp",
				MetricsExporter:      "otlp",
				LogsExporter:         "otlp",
				MetricExportInterval: "30s",
				MetricsTemporality:   "cumulative",
				HistogramAggregation: "base2_exponential_bucket_histogram",
				TracesSampler:        "always_on",
			},
			want: []corev1.EnvVar{
				{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://tempo:4318"},
				{
					Name:  "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
					Value: "http://vmagent:4318/v1/metrics",
				},
				{Name: "OTEL_EXPORTER_OTLP_PROTOCOL", Value: "grpc"},
				{Name: "OTEL_TRACES_EXPORTER", Value: "otlp"},
				{Name: "OTEL_METRICS_EXPORTER", Value: "otlp"},
				{Name: "OTEL_LOGS_EXPORTER", Value: "otlp"},
				{Name: "OTEL_METRIC_EXPORT_INTERVAL", Value: "30s"},
				{Name: "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", Value: "cumulative"},
				{
					Name:  "OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
					Value: "base2_exponential_bucket_histogram",
				},
				{Name: "OTEL_TRACES_SAMPLER", Value: "always_on"},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := BuildOTELEnvVars(tc.cfg)
			if len(got) != len(tc.want) {
				t.Fatalf(
					"len(BuildOTELEnvVars()) = %d, want %d\n  got:  %v\n  want: %v",
					len(got),
					len(tc.want),
					got,
					tc.want,
				)
			}
			for i := range got {
				if got[i].Name != tc.want[i].Name || got[i].Value != tc.want[i].Value {
					t.Errorf("env[%d] = {%q, %q}, want {%q, %q}",
						i, got[i].Name, got[i].Value, tc.want[i].Name, tc.want[i].Value)
				}
			}
		})
	}
}

func TestBuildOTELEnvVarsWithResourceAttributes(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	t.Setenv(
		"OTEL_RESOURCE_ATTRIBUTES",
		"deployment.environment=dev,deployment.region=us\\,east,multigres.project=old",
	)

	cfg := &ObservabilityConfig{
		OTLPMetricsEndpoint: "http://vmagent:4318/v1/metrics",
		MetricsExporter:     "otlp",
	}
	got := BuildOTELEnvVarsWithResourceAttributes(cfg, map[string]string{
		"multigres.project":   "project-ref-123",
		"multigres.cluster":   "cluster-a",
		"multigres.component": "multipooler",
	})

	want := []corev1.EnvVar{
		{Name: "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", Value: "http://vmagent:4318/v1/metrics"},
		{Name: "OTEL_METRICS_EXPORTER", Value: "otlp"},
		{
			Name: "OTEL_RESOURCE_ATTRIBUTES",
			Value: "deployment.environment=dev," +
				"deployment.region=us\\,east," +
				"multigres.project=project-ref-123," +
				"multigres.cluster=cluster-a," +
				"multigres.component=multipooler",
		},
	}

	if len(got) != len(want) {
		t.Fatalf(
			"len(BuildOTELEnvVarsWithResourceAttributes()) = %d, want %d\n  got:  %v\n  want: %v",
			len(got),
			len(want),
			got,
			want,
		)
	}
	for i := range got {
		if got[i].Name != want[i].Name || got[i].Value != want[i].Value {
			t.Errorf("env[%d] = {%q, %q}, want {%q, %q}",
				i, got[i].Name, got[i].Value, want[i].Name, want[i].Value)
		}
	}
}

func TestBuildOTELEnvVars_FallbackToEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://from-env:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics-from-env:4318/v1/metrics")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")

	// nil config should fall back to env vars.
	got := BuildOTELEnvVars(nil)
	if len(got) < 3 {
		t.Fatalf("expected at least 3 env vars from fallback, got %d: %v", len(got), got)
	}
	if got[0].Value != "http://from-env:4318" {
		t.Errorf("endpoint = %q, want %q", got[0].Value, "http://from-env:4318")
	}
	if got[1].Value != "http://metrics-from-env:4318/v1/metrics" {
		t.Errorf(
			"metrics endpoint = %q, want %q",
			got[1].Value,
			"http://metrics-from-env:4318/v1/metrics",
		)
	}
	if got[2].Value != "http/protobuf" {
		t.Errorf("protocol = %q, want %q", got[2].Value, "http/protobuf")
	}
}

func TestBuildOTELEnvVars_CRDOverridesEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://from-env:4318")

	cfg := &ObservabilityConfig{
		OTLPEndpoint: "http://from-crd:4318",
	}
	got := BuildOTELEnvVars(cfg)
	if len(got) == 0 {
		t.Fatal("expected at least 1 env var")
	}
	if got[0].Value != "http://from-crd:4318" {
		t.Errorf("endpoint = %q, want CRD value %q", got[0].Value, "http://from-crd:4318")
	}
}

func TestEnvOrCRD(t *testing.T) {
	tests := map[string]struct {
		cfg    *ObservabilityConfig
		envVal string
		crdVal string
		want   string
	}{
		"nil config with env": {
			cfg:    nil,
			envVal: "from-env",
			want:   "from-env",
		},
		"nil config without env": {
			cfg:  nil,
			want: "",
		},
		"config with value": {
			cfg:    &ObservabilityConfig{OTLPEndpoint: "from-crd"},
			crdVal: "from-crd",
			want:   "from-crd",
		},
		"config with empty value falls back to env": {
			cfg:    &ObservabilityConfig{},
			envVal: "from-env",
			want:   "from-env",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TEST_ENVORCRD", tc.envVal)
			got := envOrCRD(
				tc.cfg,
				func(c *ObservabilityConfig) string { return c.OTLPEndpoint },
				"TEST_ENVORCRD",
			)
			if got != tc.want {
				t.Errorf("envOrCRD() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMergeBackupConfig(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		child  *BackupConfig
		parent *BackupConfig
		want   *BackupConfig
	}{
		"both nil": {
			child:  nil,
			parent: nil,
			want:   nil,
		},
		"child nil inherits parent": {
			child: nil,
			parent: &BackupConfig{
				Type:       BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{Path: "/parent"},
			},
			want: &BackupConfig{
				Type:       BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{Path: "/parent"},
			},
		},
		"parent nil uses child": {
			child: &BackupConfig{
				Type: BackupTypeS3,
				S3:   &S3BackupConfig{Bucket: "child-bucket", Region: "us-east-1"},
			},
			parent: nil,
			want: &BackupConfig{
				Type: BackupTypeS3,
				S3:   &S3BackupConfig{Bucket: "child-bucket", Region: "us-east-1"},
			},
		},
		"child overrides parent path for filesystem": {
			child: &BackupConfig{
				Type:       BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{Path: "/child-path"},
			},
			parent: &BackupConfig{
				Type: BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{
					Path:    "/parent-path",
					Storage: StorageSpec{Size: "10Gi"},
				},
			},
			want: &BackupConfig{
				Type: BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{
					Path:    "/child-path",
					Storage: StorageSpec{Size: "10Gi"},
				},
			},
		},
		"child type change fully replaces parent": {
			child: &BackupConfig{
				Type: BackupTypeS3,
				S3:   &S3BackupConfig{Bucket: "my-bucket", Region: "eu-west-1"},
			},
			parent: &BackupConfig{
				Type:       BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{Path: "/old-backups"},
			},
			want: &BackupConfig{
				Type: BackupTypeS3,
				S3:   &S3BackupConfig{Bucket: "my-bucket", Region: "eu-west-1"},
			},
		},
		"child merges S3 fields": {
			child: &BackupConfig{
				Type: BackupTypeS3,
				S3:   &S3BackupConfig{Endpoint: "https://custom.s3.local"},
			},
			parent: &BackupConfig{
				Type: BackupTypeS3,
				S3:   &S3BackupConfig{Bucket: "parent-bucket", Region: "us-west-2"},
			},
			want: &BackupConfig{
				Type: BackupTypeS3,
				S3: &S3BackupConfig{
					Bucket:   "parent-bucket",
					Region:   "us-west-2",
					Endpoint: "https://custom.s3.local",
				},
			},
		},
		"child overrides storage size": {
			child: &BackupConfig{
				Type:       BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{Storage: StorageSpec{Size: "50Gi"}},
			},
			parent: &BackupConfig{
				Type: BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{
					Path:    "/backups",
					Storage: StorageSpec{Size: "10Gi"},
				},
			},
			want: &BackupConfig{
				Type: BackupTypeFilesystem,
				Filesystem: &FilesystemBackupConfig{
					Path:    "/backups",
					Storage: StorageSpec{Size: "50Gi"},
				},
			},
		},
		"s3 child overrides credentialsSecret": {
			child: &BackupConfig{
				Type: BackupTypeS3,
				S3:   &S3BackupConfig{CredentialsSecret: "child-secret"},
			},
			parent: &BackupConfig{
				Type: BackupTypeS3,
				S3: &S3BackupConfig{
					Bucket:            "parent-bucket",
					Region:            "us-west-2",
					CredentialsSecret: "parent-secret",
				},
			},
			want: &BackupConfig{
				Type: BackupTypeS3,
				S3: &S3BackupConfig{
					Bucket:            "parent-bucket",
					Region:            "us-west-2",
					CredentialsSecret: "child-secret",
				},
			},
		},
		"child overrides pgBackRestTLS": {
			child: &BackupConfig{
				Type:          BackupTypeS3,
				S3:            &S3BackupConfig{Bucket: "child-bucket"},
				PgBackRestTLS: &PgBackRestTLSConfig{SecretName: "child-tls-secret"},
			},
			parent: &BackupConfig{
				Type: BackupTypeS3,
				S3:   &S3BackupConfig{Bucket: "parent-bucket", Region: "eu-west-1"},
			},
			want: &BackupConfig{
				Type:          BackupTypeS3,
				S3:            &S3BackupConfig{Bucket: "child-bucket", Region: "eu-west-1"},
				PgBackRestTLS: &PgBackRestTLSConfig{SecretName: "child-tls-secret"},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := MergeBackupConfig(tc.child, tc.parent)
			if tc.want == nil {
				if got != nil {
					t.Errorf("MergeBackupConfig() = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("MergeBackupConfig() = nil, want %+v", tc.want)
			}
			if got.Type != tc.want.Type {
				t.Errorf("Type = %q, want %q", got.Type, tc.want.Type)
			}
			if tc.want.Filesystem != nil {
				if got.Filesystem == nil {
					t.Fatal("Filesystem = nil, want non-nil")
				}
				if got.Filesystem.Path != tc.want.Filesystem.Path {
					t.Errorf(
						"Filesystem.Path = %q, want %q",
						got.Filesystem.Path,
						tc.want.Filesystem.Path,
					)
				}
				if got.Filesystem.Storage.Size != tc.want.Filesystem.Storage.Size {
					t.Errorf(
						"Filesystem.Storage.Size = %q, want %q",
						got.Filesystem.Storage.Size,
						tc.want.Filesystem.Storage.Size,
					)
				}
			}
			if tc.want.S3 != nil {
				if got.S3 == nil {
					t.Fatal("S3 = nil, want non-nil")
				}
				if got.S3.Bucket != tc.want.S3.Bucket {
					t.Errorf("S3.Bucket = %q, want %q", got.S3.Bucket, tc.want.S3.Bucket)
				}
				if got.S3.Region != tc.want.S3.Region {
					t.Errorf("S3.Region = %q, want %q", got.S3.Region, tc.want.S3.Region)
				}
				if got.S3.Endpoint != tc.want.S3.Endpoint {
					t.Errorf("S3.Endpoint = %q, want %q", got.S3.Endpoint, tc.want.S3.Endpoint)
				}
				if got.S3.CredentialsSecret != tc.want.S3.CredentialsSecret {
					t.Errorf(
						"S3.CredentialsSecret = %q, want %q",
						got.S3.CredentialsSecret,
						tc.want.S3.CredentialsSecret,
					)
				}
			}
			if tc.want.PgBackRestTLS != nil {
				if got.PgBackRestTLS == nil {
					t.Fatal("PgBackRestTLS = nil, want non-nil")
				}
				if got.PgBackRestTLS.SecretName != tc.want.PgBackRestTLS.SecretName {
					t.Errorf(
						"PgBackRestTLS.SecretName = %q, want %q",
						got.PgBackRestTLS.SecretName,
						tc.want.PgBackRestTLS.SecretName,
					)
				}
			}
		})
	}
}
