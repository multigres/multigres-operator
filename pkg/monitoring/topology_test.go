package monitoring

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestTopologyMetricsReplaceFailedObservationsAndDelete(t *testing.T) {
	name, ns := "metrics-topo", "metrics-test"
	t.Cleanup(func() { DeleteTopologyHealth(name, ns) })
	condition := metav1.Condition{Status: metav1.ConditionTrue, Reason: "QuorumAvailable"}
	members := []TopologyMemberMetrics{
		{
			Name:         "metrics-topo-0",
			Up:           true,
			BackendBytes: ptr.To(int64(100)),
			Revision:     ptr.To(int64(42)),
		},
	}
	SetTopologyHealth("example", name, ns, condition, time.Now(), members)
	require.Equal(
		t,
		float64(100),
		gaugeValue(t, topoBackend, "example", name, ns, "metrics-topo-0"),
	)
	condition.Status, condition.Reason = metav1.ConditionFalse, "TopologyUnreachable"
	members[0].Up, members[0].BackendBytes, members[0].Revision = false, nil, nil
	SetTopologyHealth("example", name, ns, condition, time.Now(), members)
	registry := prometheus.NewRegistry()
	registry.MustRegister(topoBackend, topoRevision)
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				require.False(
					t,
					label.GetName() == "name" && label.GetValue() == name,
					"failed probes must not retain backend/revision observations",
				)
			}
		}
	}
	require.Equal(
		t,
		float64(0),
		gaugeValue(t, topoMemberUp, "example", name, ns, "metrics-topo-0"),
	)
	DeleteTopologyHealth(name, ns)
	require.False(t, topoQuorum.DeleteLabelValues("example", name, ns, "TopologyUnreachable"))
}
