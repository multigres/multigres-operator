package monitoring

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/multigres/testkit/assert"
)

func TestSetClusterInfo(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() { clusterInfo.Reset() })

	SetClusterInfo("test-cluster", "default", "Healthy", true)

	val := gaugeValue(t, clusterInfo, "test-cluster", "default", "Healthy", "true")
	c.Eq(1, val, "expected clusterInfo gauge to be 1, got")

	// Phase change should clean up old label set, initialized stays true
	SetClusterInfo("test-cluster", "default", "Degraded", true)

	val = gaugeValue(t, clusterInfo, "test-cluster", "default", "Degraded", "true")
	c.Eq(1, val, "expected clusterInfo gauge for Degraded to be 1, got")

	// Old phase must have been cleaned up (value 0)
	oldVal := gaugeValue(t, clusterInfo, "test-cluster", "default", "Healthy", "true")
	c.Eq(0, oldVal, "old phase label set should have been cleaned up")
}

func TestSetClusterTopology(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() {
		clusterCellsTotal.Reset()
		clusterShardsTotal.Reset()
	})

	SetClusterTopology("test-cluster", "default", 3, 6)

	cells := gaugeValue(t, clusterCellsTotal, "test-cluster", "default")
	c.Eq(3, cells, "expected cells=3, got")
	shards := gaugeValue(t, clusterShardsTotal, "test-cluster", "default")
	c.Eq(6, shards, "expected shards=6, got")
}

func TestSetCellGatewayReplicas(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() { cellGatewayReplicas.Reset() })

	SetCellGatewayReplicas("cell-1", "default", 3, 2)

	desired := gaugeValue(t, cellGatewayReplicas, "cell-1", "default", "desired")
	c.Eq(3, desired, "expected desired=3, got")
	ready := gaugeValue(t, cellGatewayReplicas, "cell-1", "default", "ready")
	c.Eq(2, ready, "expected ready=2, got")
}

func TestSetShardPoolReplicas(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() { shardPoolReplicas.Reset() })

	SetShardPoolReplicas("test-cluster", "shard-1", "primary", "z1", "default", 3, 3)

	desired := gaugeValue(
		t,
		shardPoolReplicas,
		"test-cluster",
		"shard-1",
		"primary",
		"z1",
		"default",
		"desired",
	)
	c.Eq(3, desired, "expected desired=3, got")
	ready := gaugeValue(
		t,
		shardPoolReplicas,
		"test-cluster",
		"shard-1",
		"primary",
		"z1",
		"default",
		"ready",
	)
	c.Eq(3, ready, "expected ready=3, got")
}

func TestSetTopoServerReplicas(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() { toposerverReplicas.Reset() })

	SetTopoServerReplicas("topo-1", "default", 3, 1)

	desired := gaugeValue(t, toposerverReplicas, "topo-1", "default", "desired")
	c.Eq(3, desired, "expected desired=3, got")
	ready := gaugeValue(t, toposerverReplicas, "topo-1", "default", "ready")
	c.Eq(1, ready, "expected ready=1, got")
}

func TestRecordWebhookRequest(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() {
		webhookRequestTotal.Reset()
		webhookRequestDuration.Reset()
	})

	RecordWebhookRequest("CREATE", "MultigresCluster", nil, 50*time.Millisecond)
	RecordWebhookRequest(
		"UPDATE",
		"MultigresCluster",
		errors.New("validation failed"),
		100*time.Millisecond,
	)

	successVal := counterValue(t, webhookRequestTotal, "CREATE", "MultigresCluster", "success")
	c.Eq(1, successVal, "expected success counter=1, got")

	errorVal := counterValue(t, webhookRequestTotal, "UPDATE", "MultigresCluster", "error")
	c.Eq(1, errorVal, "expected error counter=1, got")
}

func TestSetPoolPodsDrifted(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() { poolPodsDrifted.Reset() })

	SetPoolPodsDrifted("cluster-1", "shard-1", "primary", "zone-a", "default", 3)

	val := gaugeValue(t, poolPodsDrifted, "cluster-1", "shard-1", "primary", "zone-a", "default")
	c.Eq(3, val, "expected poolPodsDrifted gauge to be 3, got")

	SetPoolPodsDrifted("cluster-1", "shard-1", "primary", "zone-a", "default", 0)
	val = gaugeValue(t, poolPodsDrifted, "cluster-1", "shard-1", "primary", "zone-a", "default")
	c.Eq(0, val, "expected poolPodsDrifted gauge to be 0, got")
}

func TestSetLastBackupAge(t *testing.T) {
	t.Cleanup(func() { lastBackupAgeSeconds.Reset() })

	age := 30 * time.Minute
	SetLastBackupAge("cluster-1", "shard-1", "default", age)

	val := gaugeValue(t, lastBackupAgeSeconds, "cluster-1", "shard-1", "default")
	assert.NewCollecting(t).Eq(age.Seconds(), val, "expected lastBackupAgeSeconds gauge to be")
}

func TestIncrementDrainOperations(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() { drainOperationsTotal.Reset() })

	IncrementDrainOperations("cluster-1", "shard-1", "success")
	IncrementDrainOperations("cluster-1", "shard-1", "success")
	IncrementDrainOperations("cluster-1", "shard-1", "error")

	successVal := counterValue(t, drainOperationsTotal, "cluster-1", "shard-1", "success")
	c.Eq(2, successVal, "expected drain success counter=2, got")
	errorVal := counterValue(t, drainOperationsTotal, "cluster-1", "shard-1", "error")
	c.Eq(1, errorVal, "expected drain error counter=1, got")
}

func TestSetRollingUpdateInProgress(t *testing.T) {
	c := assert.NewCollecting(t)
	t.Cleanup(func() { rollingUpdateInProgress.Reset() })

	SetRollingUpdateInProgress("cluster-1", "shard-1", "primary", "zone-a", "default", true)
	val := gaugeValue(
		t,
		rollingUpdateInProgress,
		"cluster-1",
		"shard-1",
		"primary",
		"zone-a",
		"default",
	)
	c.Eq(1, val, "expected rollingUpdateInProgress=1 when true, got")

	SetRollingUpdateInProgress("cluster-1", "shard-1", "primary", "zone-a", "default", false)
	val = gaugeValue(
		t,
		rollingUpdateInProgress,
		"cluster-1",
		"shard-1",
		"primary",
		"zone-a",
		"default",
	)
	c.Eq(0, val, "expected rollingUpdateInProgress=0 when false, got")
}

// --- helpers ---

func gaugeValue(t *testing.T, vec *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	c := assert.NewAborting(t)
	g, err := vec.GetMetricWithLabelValues(labels...)
	c.NoError(err, "GetMetricWithLabelValues(%v)", labels)
	m := &dto.Metric{}
	c.NoError(g.Write(m), "Write")
	return m.GetGauge().GetValue()
}

func counterValue(t *testing.T, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	ck := assert.NewAborting(t)
	c, err := vec.GetMetricWithLabelValues(labels...)
	ck.NoError(err, "GetMetricWithLabelValues(%v)", labels)
	m := &dto.Metric{}
	ck.NoError(c.Write(m), "Write")
	return m.GetCounter().GetValue()
}
