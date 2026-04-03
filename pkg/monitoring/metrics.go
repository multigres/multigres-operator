package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Domain-specific metric collectors.
//
// These complement the generic controller-runtime metrics (reconcile counts,
// durations, work queue depth, etc.) with operator-specific state that the
// framework cannot know about.
var (
	clusterInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_cluster_info",
			Help: "Info-style metric for MultigresCluster discovery and phase tracking. Always 1.",
		},
		[]string{"name", "namespace", "phase"},
	)

	clusterCellsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_cluster_cells_total",
			Help: "Number of cells in a MultigresCluster.",
		},
		[]string{"cluster", "namespace"},
	)

	clusterShardsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_cluster_shards_total",
			Help: "Number of shards in a MultigresCluster.",
		},
		[]string{"cluster", "namespace"},
	)

	cellGatewayReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_cell_gateway_replicas",
			Help: "MultiGateway replica counts for a Cell.",
		},
		[]string{"cell", "namespace", "state"},
	)

	shardPoolReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_shard_pool_replicas",
			Help: "Pool replica counts for a Shard.",
		},
		[]string{"cluster", "shard", "pool", "cell", "namespace", "state"},
	)

	poolPodsDrifted = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_pool_pods_drifted",
			Help: "Number of pool pods with spec-hash mismatch requiring rolling update.",
		},
		[]string{"cluster", "shard", "pool", "cell", "namespace"},
	)

	toposerverReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_toposerver_replicas",
			Help: "TopoServer replica counts.",
		},
		[]string{"name", "namespace", "state"},
	)

	webhookRequestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "multigres_operator_webhook_request_total",
			Help: "Total number of webhook admission requests.",
		},
		[]string{"operation", "resource", "result"},
	)

	webhookRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "multigres_operator_webhook_request_duration_seconds",
			Help:    "Latency of webhook admission handling in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "resource"},
	)

	lastBackupAgeSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_last_backup_age_seconds",
			Help: "Age of the most recent completed backup in seconds.",
		},
		[]string{"cluster", "shard", "namespace"},
	)

	drainOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "multigres_operator_drain_operations_total",
			Help: "Total number of graceful pod drain operations.",
		},
		[]string{"cluster", "shard", "result"},
	)

	rollingUpdateInProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_rolling_update_in_progress",
			Help: "Indicates if a rolling update is currently in progress for a pool.",
		},
		[]string{"cluster", "shard", "pool", "cell", "namespace"},
	)

	backupRetainedCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_backup_retained_count",
			Help: "Number of retained backups per shard, partitioned by type.",
		},
		[]string{"cluster", "shard", "namespace", "type"},
	)

	backupOldestRetainedAge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_backup_oldest_retained_age_seconds",
			Help: "Age of the oldest retained backup in seconds.",
		},
		[]string{"cluster", "shard", "namespace"},
	)

	backupRestoreTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "multigres_operator_backup_restore_total",
			Help: "Total pod restoration events, partitioned by source and result.",
		},
		[]string{"cluster", "shard", "namespace", "source", "result"},
	)

	backupFenceActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "multigres_operator_backup_fence_active",
			Help: "Whether backup fencing is active for a shard (1=fenced, 0=unfenced).",
		},
		[]string{"cluster", "shard", "namespace"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		clusterInfo,
		clusterCellsTotal,
		clusterShardsTotal,
		cellGatewayReplicas,
		shardPoolReplicas,
		poolPodsDrifted,
		toposerverReplicas,
		webhookRequestTotal,
		webhookRequestDuration,
		lastBackupAgeSeconds,
		drainOperationsTotal,
		rollingUpdateInProgress,
		backupRetainedCount,
		backupOldestRetainedAge,
		backupRestoreTotal,
		backupFenceActive,
	)
}

// Collectors returns all registered metric collectors. This is useful for
// testing that metrics are properly registered.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		clusterInfo,
		clusterCellsTotal,
		clusterShardsTotal,
		cellGatewayReplicas,
		shardPoolReplicas,
		poolPodsDrifted,
		toposerverReplicas,
		webhookRequestTotal,
		webhookRequestDuration,
		lastBackupAgeSeconds,
		drainOperationsTotal,
		rollingUpdateInProgress,
		backupRetainedCount,
		backupOldestRetainedAge,
		backupRestoreTotal,
		backupFenceActive,
	}
}
