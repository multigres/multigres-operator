package monitoring

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/multigres/testkit/assert"
)

func TestCollectorsRegistered(t *testing.T) {
	collectors := Collectors()
	assert.NewAborting(t).NotEmpty(collectors, "expected at least one collector, got 0")
}

func TestMetricNamingConvention(t *testing.T) {
	for _, c := range Collectors() {
		ch := make(chan *prometheus.Desc, 10)
		c.Describe(ch)
		close(ch)

		for desc := range ch {
			name := extractMetricName(desc)
			if !strings.HasPrefix(name, "multigres_operator_") {
				t.Errorf("metric %q does not start with multigres_operator_ prefix", name)
			}
		}
	}
}

func TestMetricHelpNonEmpty(t *testing.T) {
	for _, c := range Collectors() {
		ch := make(chan *prometheus.Desc, 10)
		c.Describe(ch)
		close(ch)

		for desc := range ch {
			help := extractHelp(desc)
			assert.NewCollecting(t).
				NotEq("", help, "metric %q has empty help string", desc.String())
		}
	}
}

func TestGaugeLabels(t *testing.T) {
	tests := []struct {
		name       string
		collector  *prometheus.GaugeVec
		wantLabels []string
	}{
		{
			name:       "clusterInfo",
			collector:  clusterInfo,
			wantLabels: []string{"name", "namespace", "phase"},
		},
		{
			name:       "clusterCellsTotal",
			collector:  clusterCellsTotal,
			wantLabels: []string{"cluster", "namespace"},
		},
		{
			name:       "cellGatewayReplicas",
			collector:  cellGatewayReplicas,
			wantLabels: []string{"cell", "namespace", "state"},
		},
		{
			name:       "shardPoolReplicas",
			collector:  shardPoolReplicas,
			wantLabels: []string{"shard", "pool", "namespace", "state"},
		},
		{
			name:       "toposerverReplicas",
			collector:  toposerverReplicas,
			wantLabels: []string{"name", "namespace", "state"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan *prometheus.Desc, 1)
			tt.collector.Describe(ch)
			desc := <-ch
			close(ch)

			descStr := desc.String()
			for _, label := range tt.wantLabels {
				assert.NewCollecting(t).
					StrContains(descStr, label, "metric %s missing label %q in descriptor", tt.name, label)
			}
		})
	}
}

// extractMetricName pulls fqName from the Desc string representation.
// Format: Desc{fqName: "multigres_...", help: "...", ...}
func extractMetricName(desc *prometheus.Desc) string {
	s := desc.String()
	prefix := "fqName: \""
	start := strings.Index(s, prefix)
	if start < 0 {
		return ""
	}
	start += len(prefix)
	end := strings.Index(s[start:], "\"")
	if end < 0 {
		return ""
	}
	return s[start : start+end]
}

// extractHelp pulls the help text from the Desc proto representation.
func extractHelp(desc *prometheus.Desc) string {
	d := &dto.Metric{}
	// We can't get help directly from Desc, so parse from string.
	s := desc.String()
	prefix := "help: \""
	start := strings.Index(s, prefix)
	if start < 0 {
		_ = d // avoid unused
		return ""
	}
	start += len(prefix)
	end := strings.Index(s[start:], "\"")
	if end < 0 {
		return ""
	}
	return s[start : start+end]
}
