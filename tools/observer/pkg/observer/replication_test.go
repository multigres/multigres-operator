package observer

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
)

// TestCheckShardReplication_QuarantinedClassification verifies that a
// QUARANTINED pod is surfaced with its own finding and is not mistaken for a
// replica. Before this, QUARANTINED fell through the role switch into the
// replica bucket, which both suppressed the quarantine finding and inflated the
// expected replica count used by the primary replication probe.
func TestCheckShardReplication_QuarantinedClassification(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "test-ns"},
		Status: multigresv1alpha1.ShardStatus{
			PodRoles: map[string]string{
				"s1-primary-zone1-0": "PRIMARY",
				"s1-primary-zone1-1": "REPLICA",
				"s1-primary-zone1-2": "QUARANTINED",
			},
		},
	}

	// No pod objects exist, so podIPs stays empty and the SQL probes are skipped;
	// only the role-classification finding is exercised.
	o := newTestObserver(shard)
	o.checkShardReplication(t.Context(), shard)

	findings := collectFindings(o)
	if len(findings) != 1 {
		t.Fatalf("expected exactly 1 finding (the quarantine warning), got %d: %+v", len(findings), findings)
	}

	f := findings[0]
	if f.Severity != report.SeverityWarn {
		t.Errorf("expected Warn severity, got %s", f.Severity)
	}
	if !strings.Contains(f.Message, "QUARANTINED") {
		t.Errorf("expected message to mention QUARANTINED, got %q", f.Message)
	}
	if strings.Contains(f.Message, "DRAINED") {
		t.Errorf("message should no longer reference DRAINED, got %q", f.Message)
	}
	if !strings.Contains(f.Message, "s1-primary-zone1-2") {
		t.Errorf("expected message to name the quarantined pod, got %q", f.Message)
	}
	qp, ok := f.Details["quarantinedPods"].([]string)
	if !ok {
		t.Fatalf("expected Details[quarantinedPods] to be []string, got %T", f.Details["quarantinedPods"])
	}
	if len(qp) != 1 || qp[0] != "s1-primary-zone1-2" {
		t.Errorf("expected quarantinedPods=[s1-primary-zone1-2], got %v", qp)
	}
}

// TestCheckReplication_QuarantinedNotCountedAsReplica verifies the aggregate
// counting path: a QUARANTINED pod is tallied as quarantined, not as a replica.
func TestCheckReplication_QuarantinedNotCountedAsReplica(t *testing.T) {
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "test-ns"},
		Status: multigresv1alpha1.ShardStatus{
			PodRoles: map[string]string{
				"s1-primary-zone1-0": "PRIMARY",
				"s1-primary-zone1-1": "REPLICA",
				"s1-primary-zone1-2": "QUARANTINED",
			},
		},
	}

	o := newTestObserver(shard)
	o.enableSQLProbe = true
	o.probes = newProbeCollector()

	o.checkReplication(t.Context())

	repl, ok := o.probes.Data()["replication"].(map[string]any)
	if !ok {
		t.Fatalf("expected replication probe data, got %T", o.probes.Data()["replication"])
	}
	shardsData, ok := repl["shards"].([]map[string]any)
	if !ok || len(shardsData) != 1 {
		t.Fatalf("expected 1 shard entry, got %#v", repl["shards"])
	}
	entry := shardsData[0]
	if got := entry["quarantinedCount"]; got != 1 {
		t.Errorf("expected quarantinedCount=1, got %v", got)
	}
	if got := entry["replicaCount"]; got != 1 {
		t.Errorf("expected replicaCount=1 (quarantined pod excluded), got %v", got)
	}
	if got := entry["primaryCount"]; got != 1 {
		t.Errorf("expected primaryCount=1, got %v", got)
	}
}
