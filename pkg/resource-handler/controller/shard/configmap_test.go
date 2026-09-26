package shard

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestBuildPgHbaConfigMap(t *testing.T) {
	defaultScheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(defaultScheme)

	tests := map[string]struct {
		shard   *multigresv1alpha1.Shard
		scheme  *runtime.Scheme
		wantErr bool
	}{
		"creates configmap with default template": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-shard",
					Namespace: "default",
					UID:       "test-uid",
					Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
				},
			},
			wantErr: false,
		},
		"creates configmap with correct embedded template": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "production-shard",
					Namespace: "prod",
					UID:       "prod-uid",
					Labels:    map[string]string{"multigres.com/cluster": "prod-cluster"},
				},
			},
			wantErr: false,
		},
		"returns error when scheme is invalid (missing Shard kind)": {
			shard: &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "error-shard",
					Namespace: "default",
					UID:       "error-uid",
				},
			},
			scheme:  runtime.NewScheme(), // Empty scheme
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			scheme := tc.scheme
			if scheme == nil {
				scheme = defaultScheme
			}

			cm, err := BuildPgHbaConfigMap(tc.shard, scheme)
			c.Require().ErrorWhen(tc.wantErr, err, "BuildPgHbaConfigMap() error")

			if tc.wantErr {
				return
			}

			c.Eq(PgHbaConfigMapName(tc.shard.Name), cm.Name, "ConfigMap name")
			c.Eq(tc.shard.Namespace, cm.Namespace, "ConfigMap namespace")

			// Verify owner reference
			c.Require().
				Len(cm.OwnerReferences, 1, "Expected 1 owner reference, got %d", len(cm.OwnerReferences))
			ownerRef := cm.OwnerReferences[0]
			if ownerRef.Name != tc.shard.Name || ownerRef.Kind != "Shard" {
				t.Errorf("Owner reference = %+v, want Shard/%s", ownerRef, tc.shard.Name)
			}
			c.True(
				ptr.Deref(ownerRef.Controller, false),
				"Expected owner reference to be controller",
			)

			// Verify labels
			expectedLabels := map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   tc.shard.Labels["multigres.com/cluster"],
				"app.kubernetes.io/component":  "pg-hba-config",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
				"multigres.com/cluster":        tc.shard.Labels["multigres.com/cluster"],
			}
			c.EqDiff(expectedLabels, cm.Labels, "Labels mismatch")

			// Verify template content exists
			template, ok := cm.Data["pg_hba_template.conf"]
			c.Require().True(ok, "ConfigMap missing pg_hba_template.conf key")

			// Verify the template matches what's embedded (source of truth)
			c.Eq(
				DefaultPgHbaTemplate,
				template,
				"Template content doesn't match DefaultPgHbaTemplate",
			)
		})
	}
}

func TestBuildPostgresConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
	}

	t.Run("stores rendered content under the config key with an owner ref", func(t *testing.T) {
		c := assert.NewCollecting(t)
		rendered := "# rendered\nmax_connections = 200\n"
		cm, err := BuildPostgresConfigMap(shard, rendered, scheme)
		c.Require().NoError(err, "BuildPostgresConfigMap() error =")
		c.Eq(PostgresConfigMapName(shard.Name), cm.Name, "name")
		c.Eq(shard.Namespace, cm.Namespace, "namespace")
		got := cm.Data[PostgresConfigMapKey]
		c.Eq(rendered, got, "Data[%q] = %q, want", PostgresConfigMapKey, got)
		if len(cm.OwnerReferences) != 1 ||
			cm.OwnerReferences[0].Name != shard.Name ||
			cm.OwnerReferences[0].Kind != "Shard" {
			t.Errorf("owner reference = %+v, want Shard/%s", cm.OwnerReferences, shard.Name)
		}
		c.True(
			ptr.Deref(cm.OwnerReferences[0].Controller, false),
			"expected owner reference to be controller",
		)
	})

	t.Run("returns error on invalid scheme", func(t *testing.T) {
		_, err := BuildPostgresConfigMap(shard, "x", runtime.NewScheme())
		assert.NewCollecting(t).Error(err, "expected error with empty scheme")
	})
}

func TestBuildPostgresExporterQueriesConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
	}

	t.Run("stores embedded queries under the queries key with an owner ref", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cm, err := BuildPostgresExporterQueriesConfigMap(shard, scheme)
		c.Require().NoError(err, "BuildPostgresExporterQueriesConfigMap() error =")
		c.Eq(PostgresExporterQueriesConfigMapName(shard.Name), cm.Name, "name")
		c.Eq(shard.Namespace, cm.Namespace, "namespace")
		c.Eq(
			DefaultPostgresExporterQueries,
			cm.Data[PostgresExporterQueriesConfigMapKey],
			"Data[%q] doesn't match DefaultPostgresExporterQueries",
			PostgresExporterQueriesConfigMapKey,
		)
		if len(cm.OwnerReferences) != 1 ||
			cm.OwnerReferences[0].Name != shard.Name ||
			cm.OwnerReferences[0].Kind != "Shard" {
			t.Errorf("owner reference = %+v, want Shard/%s", cm.OwnerReferences, shard.Name)
		}
		c.True(
			ptr.Deref(cm.OwnerReferences[0].Controller, false),
			"expected owner reference to be controller",
		)
	})

	t.Run("returns error on invalid scheme", func(t *testing.T) {
		_, err := BuildPostgresExporterQueriesConfigMap(shard, runtime.NewScheme())
		assert.NewCollecting(t).Error(err, "expected error with empty scheme")
	})
}

func TestDefaultPostgresExporterQueriesEmbedded(t *testing.T) {
	c := assert.NewCollecting(t)
	c.Require().
		NotEq("", DefaultPostgresExporterQueries, "DefaultPostgresExporterQueries is empty - go:embed may have failed")

	// Each top-level key becomes the exporter's metric-name prefix.
	for _, queryName := range []string{
		"pg_database:",
		"pg_wal:",
		"pg_stat_database:",
		"physical_replication_lag:",
		"connection_stats:",
		"max_connections:",
	} {
		c.StrContains(
			DefaultPostgresExporterQueries,
			"\n"+queryName,
			"DefaultPostgresExporterQueries missing query block %q",
			queryName,
		)
	}
}

func TestDefaultPgHbaTemplateEmbedded(t *testing.T) {
	c := assert.NewCollecting(t)
	// Verify the embedded template is not empty
	c.NotEq("", DefaultPgHbaTemplate, "DefaultPgHbaTemplate is empty - go:embed may have failed")

	// Verify critical configuration lines exist
	// We check for the presence of rules, ignoring multiple spaces
	checks := []struct {
		desc        string
		mustContain []string
	}{
		{
			desc:        "header",
			mustContain: []string{"# PostgreSQL Client Authentication"},
		},
		{
			desc:        "local scram-sha-256 rule",
			mustContain: []string{"local", "all", "all", "scram-sha-256"},
		},
		{
			desc:        "replication scram-sha-256 rule",
			mustContain: []string{"host", "replication", "all", "0.0.0.0/0", "scram-sha-256"},
		},
	}

	for _, check := range checks {
		found := false
		lines := strings.Split(DefaultPgHbaTemplate, "\n")
		for _, line := range lines {
			allMatch := true
			for _, part := range check.mustContain {
				if !strings.Contains(line, part) {
					allMatch = false
					break
				}
			}
			if allMatch {
				found = true
				break
			}
		}
		c.True(
			found,
			"DefaultPgHbaTemplate missing %s (expected line containing all of: %v)",
			check.desc,
			check.mustContain,
		)
	}
}
