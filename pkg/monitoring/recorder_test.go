package monitoring

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestForgetCluster(t *testing.T) {
	t.Cleanup(func() {
		clusterInfo.Reset()
		clusterCellsTotal.Reset()
		clusterShardsTotal.Reset()
	})

	SetClusterInfo("gone", "default", "Progressing", true)
	SetClusterTopology("gone", "default", 3, 6)
	SetClusterInfo("kept", "default", "Healthy", true)
	SetClusterTopology("kept", "default", 1, 2)

	ForgetCluster("gone", "default")

	for _, vec := range []*prometheus.GaugeVec{clusterInfo, clusterCellsTotal, clusterShardsTotal} {
		ch := make(chan prometheus.Metric, 10)
		vec.Collect(ch)
		close(ch)
		if n := len(ch); n != 1 {
			t.Errorf("expected only the kept cluster's series to remain, got %d series", n)
		}
	}
	if val := gaugeValue(t, clusterInfo, "kept", "default", "Healthy", "true"); val != 1 {
		t.Errorf("expected kept cluster's clusterInfo to be 1, got %f", val)
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

func TestReplaceShardPoolPodsStuck(t *testing.T) {
	t.Cleanup(func() { poolPodsStuck.Reset() })

	ReplaceShardPoolPodsStuck("c", "shard-1", "default", []PoolPodsStuck{
		{Pool: "primary", Cell: "zone-a", Count: 2},
		{Pool: "primary", Cell: "zone-b", Count: 1},
	})
	ReplaceShardPoolPodsStuck("c", "shard-2", "default", []PoolPodsStuck{
		{Pool: "primary", Cell: "zone-a", Count: 5},
	})

	if v := gaugeValue(t, poolPodsStuck, "c", "shard-1", "primary", "zone-a", "default"); v != 2 {
		t.Errorf("shard-1 zone-a = %v, want 2", v)
	}

	ReplaceShardPoolPodsStuck("c", "shard-1", "default", []PoolPodsStuck{
		{Pool: "primary", Cell: "zone-a", Count: 0},
	})

	if v := gaugeValue(t, poolPodsStuck, "c", "shard-1", "primary", "zone-a", "default"); v != 0 {
		t.Errorf("shard-1 zone-a = %v, want 0", v)
	}
	if n := testutil.CollectAndCount(poolPodsStuck); n != 2 {
		t.Errorf("series count = %d, want 2 (zone-b series of shard-1 must be dropped)", n)
	}
	if v := gaugeValue(t, poolPodsStuck, "c", "shard-2", "primary", "zone-a", "default"); v != 5 {
		t.Errorf("shard-2 must be untouched, got %v", v)
	}

	ForgetShardPoolPodsStuck("shard-2", "default")
	if n := testutil.CollectAndCount(poolPodsStuck); n != 1 {
		t.Errorf("series count after forget = %d, want 1", n)
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

func TestForgetShard(t *testing.T) {
	perShard := []*prometheus.GaugeVec{
		shardPoolReplicas, poolPodsDrifted, lastBackupAgeSeconds,
		rollingUpdateInProgress, shardPostureInconsistent, shardHasPrimary,
	}
	other := []*prometheus.GaugeVec{
		clusterInfo, clusterCellsTotal, clusterShardsTotal, cellGatewayReplicas, toposerverReplicas,
	}
	reset := func() {
		for _, v := range append(append([]*prometheus.GaugeVec{}, perShard...), other...) {
			v.Reset()
		}
	}
	t.Cleanup(reset)

	populate := func(cluster, shard, namespace string) {
		SetShardPoolReplicas(cluster, shard, "primary", "zone-a", namespace, 3, 3)
		SetPoolPodsDrifted(cluster, shard, "primary", "zone-a", namespace, 1)
		SetLastBackupAge(cluster, shard, namespace, time.Minute)
		SetRollingUpdateInProgress(cluster, shard, "primary", "zone-a", namespace, true)
		SetShardPostureInconsistent(cluster, shard, namespace, true)
		SetShardHasPrimary(cluster, shard, namespace, true)
	}
	countSeries := func(shard, namespace string) int {
		n := 0
		for _, v := range perShard {
			for _, l := range collectLabels(t, v) {
				if l["shard"] == shard && l["namespace"] == namespace {
					n++
				}
			}
		}
		return n
	}

	t.Run("removes every per-shard series and leaves everything else", func(t *testing.T) {
		reset()
		populate("cluster-1", "shard-1", "default")
		populate("cluster-1", "shard-2", "default")
		// Name is a prefix-extension of the forgotten shard.
		populate("cluster-1", "shard-10", "default")
		// Same shard name in another namespace.
		populate("cluster-1", "shard-1", "other")
		SetClusterInfo("cluster-1", "default", "Ready", true)
		SetClusterTopology("cluster-1", "default", 1, 4)
		SetCellGatewayReplicas("zone-a", "default", 2, 2)
		SetTopoServerReplicas("topo-1", "default", 3, 3)

		want := countSeries("shard-1", "default")
		if want != 7 {
			t.Fatalf("expected 7 series for shard-1 before forgetting, got %d", want)
		}

		ForgetShard("shard-1", "default")

		if n := countSeries("shard-1", "default"); n != 0 {
			t.Errorf("expected no series for forgotten shard, got %d", n)
		}
		for _, k := range []struct{ shard, namespace string }{
			{"shard-2", "default"}, {"shard-10", "default"}, {"shard-1", "other"},
		} {
			if n := countSeries(k.shard, k.namespace); n != want {
				t.Errorf("series for %s/%s: expected %d, got %d", k.namespace, k.shard, want, n)
			}
		}
		if val := gaugeValue(
			t, rollingUpdateInProgress, "cluster-1", "shard-10", "primary", "zone-a", "default",
		); val != 1 {
			t.Errorf("expected shard-10 value preserved, got %f", val)
		}
		for _, v := range other {
			if n := len(collectLabels(t, v)); n == 0 {
				t.Errorf("non-shard gauge lost its series")
			}
		}
	})

	t.Run("unknown shard and empty registry are no-ops", func(t *testing.T) {
		reset()
		ForgetShard("missing", "default")
		populate("cluster-1", "shard-1", "default")
		ForgetShard("missing", "default")
		ForgetShard("shard-1", "missing")
		if n := countSeries("shard-1", "default"); n != 7 {
			t.Errorf("expected 7 series to remain, got %d", n)
		}
	})

	t.Run("series can be recreated after forgetting", func(t *testing.T) {
		reset()
		populate("cluster-1", "shard-1", "default")
		ForgetShard("shard-1", "default")
		SetRollingUpdateInProgress("cluster-1", "shard-1", "primary", "zone-a", "default", true)
		if val := gaugeValue(
			t, rollingUpdateInProgress, "cluster-1", "shard-1", "primary", "zone-a", "default",
		); val != 1 {
			t.Errorf("expected recreated series=1, got %f", val)
		}
	})
}

// TestForgetShardCoversAllShardGauges fails when a GaugeVec with a "shard"
// label is added to metrics.go without being cleaned up by ForgetShard.
func TestForgetShardCoversAllShardGauges(t *testing.T) {
	metricsSrc, err := os.ReadFile("metrics.go")
	if err != nil {
		t.Fatal(err)
	}
	recorderSrc, err := os.ReadFile("recorder.go")
	if err != nil {
		t.Fatal(err)
	}
	forget := string(recorderSrc[strings.Index(string(recorderSrc), "func ForgetShard"):])

	decl := regexp.MustCompile(`(?s)\t(\w+) = prometheus\.NewGaugeVec\(.*?\[\]string\{([^}]*)\}`)
	found := 0
	for _, m := range decl.FindAllStringSubmatch(string(metricsSrc), -1) {
		if !strings.Contains(m[2], `"shard"`) {
			continue
		}
		found++
		if !strings.Contains(forget, m[1]+".DeletePartialMatch") {
			t.Errorf("%s has a shard label but ForgetShard does not delete its series", m[1])
		}
	}
	if found < 6 {
		t.Fatalf("expected to find at least 6 per-shard gauges in metrics.go, found %d", found)
	}
}

func collectLabels(t *testing.T, vec *prometheus.GaugeVec) []map[string]string {
	t.Helper()
	ch := make(chan prometheus.Metric, 100)
	vec.Collect(ch)
	close(ch)
	var out []map[string]string
	for m := range ch {
		d := &dto.Metric{}
		if err := m.Write(d); err != nil {
			t.Fatalf("Write: %v", err)
		}
		l := map[string]string{}
		for _, p := range d.GetLabel() {
			l[p.GetName()] = p.GetValue()
		}
		out = append(out, l)
	}
	return out
}

// --- helpers ---

func TestSetShardHasPrimary(t *testing.T) {
	t.Cleanup(func() { shardHasPrimary.Reset() })

	SetShardHasPrimary("cluster-1", "shard-1", "default", true)
	if val := gaugeValue(t, shardHasPrimary, "cluster-1", "shard-1", "default"); val != 1 {
		t.Errorf("expected shardHasPrimary=1 when true, got %f", val)
	}

	SetShardHasPrimary("cluster-1", "shard-1", "default", false)
	if val := gaugeValue(t, shardHasPrimary, "cluster-1", "shard-1", "default"); val != 0 {
		t.Errorf("expected shardHasPrimary=0 when false, got %f", val)
	}
}

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
