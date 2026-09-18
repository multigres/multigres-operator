package monitoring

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestSetClusterInfo(t *testing.T) {
	t.Cleanup(func() { clusterInfo.Reset() })

	SetClusterInfo("test-cluster", "default", "Healthy", true)

	val := gaugeValue(t, clusterInfo, "test-cluster", "default", "Healthy", "true")
	if val != 1 {
		t.Errorf("expected clusterInfo gauge to be 1, got %f", val)
	}

	// Phase change should clean up old label set, initialized stays true
	SetClusterInfo("test-cluster", "default", "Degraded", true)

	val = gaugeValue(t, clusterInfo, "test-cluster", "default", "Degraded", "true")
	if val != 1 {
		t.Errorf("expected clusterInfo gauge for Degraded to be 1, got %f", val)
	}

	// Old phase must have been cleaned up (value 0)
	oldVal := gaugeValue(t, clusterInfo, "test-cluster", "default", "Healthy", "true")
	if oldVal != 0 {
		t.Error("old phase label set should have been cleaned up")
	}
}

func TestSetClusterTopology(t *testing.T) {
	t.Cleanup(func() {
		clusterCellsTotal.Reset()
		clusterShardsTotal.Reset()
	})

	SetClusterTopology("test-cluster", "default", 3, 6)

	cells := gaugeValue(t, clusterCellsTotal, "test-cluster", "default")
	if cells != 3 {
		t.Errorf("expected cells=3, got %f", cells)
	}
	shards := gaugeValue(t, clusterShardsTotal, "test-cluster", "default")
	if shards != 6 {
		t.Errorf("expected shards=6, got %f", shards)
	}
}

func TestSetCellGatewayReplicas(t *testing.T) {
	t.Cleanup(func() { cellGatewayReplicas.Reset() })

	SetCellGatewayReplicas("cell-1", "default", 3, 2)

	desired := gaugeValue(t, cellGatewayReplicas, "cell-1", "default", "desired")
	if desired != 3 {
		t.Errorf("expected desired=3, got %f", desired)
	}
	ready := gaugeValue(t, cellGatewayReplicas, "cell-1", "default", "ready")
	if ready != 2 {
		t.Errorf("expected ready=2, got %f", ready)
	}
}

func TestSetShardPoolReplicas(t *testing.T) {
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
	if desired != 3 {
		t.Errorf("expected desired=3, got %f", desired)
	}
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
	if ready != 3 {
		t.Errorf("expected ready=3, got %f", ready)
	}
}

func TestSetTopoServerReplicas(t *testing.T) {
	t.Cleanup(func() { toposerverReplicas.Reset() })

	SetTopoServerReplicas("topo-1", "default", 3, 1)

	desired := gaugeValue(t, toposerverReplicas, "topo-1", "default", "desired")
	if desired != 3 {
		t.Errorf("expected desired=3, got %f", desired)
	}
	ready := gaugeValue(t, toposerverReplicas, "topo-1", "default", "ready")
	if ready != 1 {
		t.Errorf("expected ready=1, got %f", ready)
	}
}

func TestRecordWebhookRequest(t *testing.T) {
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
	if successVal != 1 {
		t.Errorf("expected success counter=1, got %f", successVal)
	}

	errorVal := counterValue(t, webhookRequestTotal, "UPDATE", "MultigresCluster", "error")
	if errorVal != 1 {
		t.Errorf("expected error counter=1, got %f", errorVal)
	}
}

func TestSetPoolPodsDrifted(t *testing.T) {
	t.Cleanup(func() { poolPodsDrifted.Reset() })

	SetPoolPodsDrifted("cluster-1", "shard-1", "primary", "zone-a", "default", 3)

	val := gaugeValue(t, poolPodsDrifted, "cluster-1", "shard-1", "primary", "zone-a", "default")
	if val != 3 {
		t.Errorf("expected poolPodsDrifted gauge to be 3, got %f", val)
	}

	SetPoolPodsDrifted("cluster-1", "shard-1", "primary", "zone-a", "default", 0)
	val = gaugeValue(t, poolPodsDrifted, "cluster-1", "shard-1", "primary", "zone-a", "default")
	if val != 0 {
		t.Errorf("expected poolPodsDrifted gauge to be 0, got %f", val)
	}
}

func TestSetLastBackupAge(t *testing.T) {
	t.Cleanup(func() { lastBackupAgeSeconds.Reset() })

	age := 30 * time.Minute
	SetLastBackupAge("cluster-1", "shard-1", "default", age)

	val := gaugeValue(t, lastBackupAgeSeconds, "cluster-1", "shard-1", "default")
	if val != age.Seconds() {
		t.Errorf("expected lastBackupAgeSeconds gauge to be %f, got %f", age.Seconds(), val)
	}
}

func TestIncrementDrainOperations(t *testing.T) {
	t.Cleanup(func() { drainOperationsTotal.Reset() })

	IncrementDrainOperations("cluster-1", "shard-1", "success")
	IncrementDrainOperations("cluster-1", "shard-1", "success")
	IncrementDrainOperations("cluster-1", "shard-1", "error")

	successVal := counterValue(t, drainOperationsTotal, "cluster-1", "shard-1", "success")
	if successVal != 2 {
		t.Errorf("expected drain success counter=2, got %f", successVal)
	}
	errorVal := counterValue(t, drainOperationsTotal, "cluster-1", "shard-1", "error")
	if errorVal != 1 {
		t.Errorf("expected drain error counter=1, got %f", errorVal)
	}
}

func TestSetRollingUpdateInProgress(t *testing.T) {
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
	if val != 1 {
		t.Errorf("expected rollingUpdateInProgress=1 when true, got %f", val)
	}

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
	if val != 0 {
		t.Errorf("expected rollingUpdateInProgress=0 when false, got %f", val)
	}
}

// --- helpers ---

func gaugeValue(t *testing.T, vec *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	g, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues(%v): %v", labels, err)
	}
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return m.GetGauge().GetValue()
}

func counterValue(t *testing.T, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	c, err := vec.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues(%v): %v", labels, err)
	}
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return m.GetCounter().GetValue()
}
