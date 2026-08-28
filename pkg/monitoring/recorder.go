package monitoring

import "time"

// SetClusterInfo sets the info-style gauge for a MultigresCluster.
// Old phase labels are automatically cleaned up via DeletePartialMatch.
func SetClusterInfo(name, namespace, phase string) {
	clusterInfo.DeletePartialMatch(map[string]string{
		"name":      name,
		"namespace": namespace,
	})
	clusterInfo.WithLabelValues(name, namespace, phase).Set(1)
}

// SetClusterTopology sets the cell and shard count gauges for a cluster.
func SetClusterTopology(cluster, namespace string, cells, shards int) {
	clusterCellsTotal.WithLabelValues(cluster, namespace).Set(float64(cells))
	clusterShardsTotal.WithLabelValues(cluster, namespace).Set(float64(shards))
}

// SetCellGatewayReplicas sets the desired and ready gateway replica gauges for a Cell.
func SetCellGatewayReplicas(cell, namespace string, desired, ready int32) {
	cellGatewayReplicas.WithLabelValues(cell, namespace, "desired").Set(float64(desired))
	cellGatewayReplicas.WithLabelValues(cell, namespace, "ready").Set(float64(ready))
}

// SetShardPoolReplicas sets the desired and ready replica gauges for a shard pool.
func SetShardPoolReplicas(cluster, shard, pool, cell, namespace string, desired, ready int32) {
	shardPoolReplicas.WithLabelValues(cluster, shard, pool, cell, namespace, "desired").
		Set(float64(desired))
	shardPoolReplicas.WithLabelValues(cluster, shard, pool, cell, namespace, "ready").
		Set(float64(ready))
}

// SetPoolPodsDrifted sets the count of pods with spec-hash mismatch in a pool/cell.
func SetPoolPodsDrifted(cluster, shard, pool, cell, namespace string, count int) {
	poolPodsDrifted.WithLabelValues(cluster, shard, pool, cell, namespace).Set(float64(count))
}

// SetTopoServerReplicas sets the desired and ready replica gauges for a TopoServer.
func SetTopoServerReplicas(name, namespace string, desired, ready int32) {
	toposerverReplicas.WithLabelValues(name, namespace, "desired").Set(float64(desired))
	toposerverReplicas.WithLabelValues(name, namespace, "ready").Set(float64(ready))
}

// RecordWebhookRequest records a webhook admission request's result and duration.
func RecordWebhookRequest(operation, resource string, err error, duration time.Duration) {
	result := "success"
	if err != nil {
		result = "error"
	}
	webhookRequestTotal.WithLabelValues(operation, resource, result).Inc()
	webhookRequestDuration.WithLabelValues(operation, resource).Observe(duration.Seconds())
}

// SetLastBackupAge sets the age of the most recent completed backup for a shard.
func SetLastBackupAge(cluster, shard, namespace string, age time.Duration) {
	lastBackupAgeSeconds.WithLabelValues(cluster, shard, namespace).Set(age.Seconds())
}

// IncrementDrainOperations increments the counter for drain operations.
func IncrementDrainOperations(cluster, shard, result string) {
	drainOperationsTotal.WithLabelValues(cluster, shard, result).Inc()
}

// SetRollingUpdateInProgress sets whether a rolling update is currently in progress for a pool.
func SetRollingUpdateInProgress(cluster, shard, pool, cell, namespace string, inProgress bool) {
	val := 0.0
	if inProgress {
		val = 1.0
	}
	rollingUpdateInProgress.WithLabelValues(cluster, shard, pool, cell, namespace).Set(val)
}

// SetShardPostureInconsistent records posture and topology disagreement.
func SetShardPostureInconsistent(cluster, shard, namespace string, inconsistent bool) {
	val := 0.0
	if inconsistent {
		val = 1.0
	}
	shardPostureInconsistent.WithLabelValues(cluster, shard, namespace).Set(val)
}
