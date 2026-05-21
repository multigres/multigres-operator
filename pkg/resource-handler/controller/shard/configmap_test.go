package shard

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
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
			scheme := tc.scheme
			if scheme == nil {
				scheme = defaultScheme
			}

			cm, err := BuildPgHbaConfigMap(tc.shard, scheme)
			if (err != nil) != tc.wantErr {
				t.Fatalf("BuildPgHbaConfigMap() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			if cm.Name != PgHbaConfigMapName(tc.shard.Name) {
				t.Errorf("ConfigMap name = %v, want %v", cm.Name, PgHbaConfigMapName(tc.shard.Name))
			}
			if cm.Namespace != tc.shard.Namespace {
				t.Errorf("ConfigMap namespace = %v, want %v", cm.Namespace, tc.shard.Namespace)
			}

			// Verify owner reference
			if len(cm.OwnerReferences) != 1 {
				t.Fatalf("Expected 1 owner reference, got %d", len(cm.OwnerReferences))
			}
			ownerRef := cm.OwnerReferences[0]
			if ownerRef.Name != tc.shard.Name || ownerRef.Kind != "Shard" {
				t.Errorf("Owner reference = %+v, want Shard/%s", ownerRef, tc.shard.Name)
			}
			if !ptr.Deref(ownerRef.Controller, false) {
				t.Error("Expected owner reference to be controller")
			}

			// Verify labels
			expectedLabels := map[string]string{
				"app.kubernetes.io/name":       "multigres",
				"app.kubernetes.io/instance":   tc.shard.Labels["multigres.com/cluster"],
				"app.kubernetes.io/component":  "pg-hba-config",
				"app.kubernetes.io/part-of":    "multigres",
				"app.kubernetes.io/managed-by": "multigres-operator",
				"multigres.com/cluster":        tc.shard.Labels["multigres.com/cluster"],
			}
			if diff := cmp.Diff(expectedLabels, cm.Labels); diff != "" {
				t.Errorf("Labels mismatch (-want +got):\n%s", diff)
			}

			// Verify template content exists
			template, ok := cm.Data["pg_hba_template.conf"]
			if !ok {
				t.Fatal("ConfigMap missing pg_hba_template.conf key")
			}

			// Verify the template matches what's embedded (source of truth)
			if template != DefaultPgHbaTemplate {
				t.Error("Template content doesn't match DefaultPgHbaTemplate")
			}
		})
	}
}

func TestDefaultPgHbaTemplateEmbedded(t *testing.T) {
	// Verify the embedded template is not empty
	if DefaultPgHbaTemplate == "" {
		t.Error("DefaultPgHbaTemplate is empty - go:embed may have failed")
	}

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
		if !found {
			t.Errorf(
				"DefaultPgHbaTemplate missing %s (expected line containing all of: %v)",
				check.desc,
				check.mustContain,
			)
		}
	}
}
