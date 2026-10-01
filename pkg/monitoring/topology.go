package monitoring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// target_namespace and member identify the monitored workload without colliding
// with the namespace and pod labels attached to the operator's scrape target.
var (
	topologyLabels = []string{"cluster", "name", "target_namespace"}
	memberLabels   = []string{"cluster", "name", "target_namespace", "member"}
)

func topologyGauge(name, help string, labels []string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "multigres_operator_" + name, Help: help,
	}, labels)
}

var (
	topoQuorum = topologyGauge(
		"toposerver_quorum_available",
		"Etcd quorum is verified with no reported status errors: 1 yes, 0 no, -1 unknown.",
		append(append([]string{}, topologyLabels...), "reason"),
	)
	topoChecked = topologyGauge(
		"toposerver_health_checked_timestamp_seconds",
		"Unix timestamp of the latest completed etcd probe.",
		topologyLabels,
	)
	topoMemberUp = topologyGauge(
		"toposerver_member_up",
		"Whether this member answered a linearizable read.",
		memberLabels,
	)
	topoBackend = topologyGauge(
		"toposerver_backend_bytes",
		"Etcd backend size reported by the member.",
		memberLabels,
	)
	topoBackendInUse = topologyGauge(
		"toposerver_backend_in_use_bytes",
		"Etcd backend bytes in use reported by the member.",
		memberLabels,
	)
	topoRevision = topologyGauge(
		"toposerver_revision",
		"Current etcd MVCC revision; use deriv to observe growth.",
		memberLabels,
	)
	topoQuota = topologyGauge(
		"toposerver_backend_quota_bytes",
		"Backend quota configured on the running etcd pod.",
		memberLabels,
	)
	topoMemoryLimit = topologyGauge(
		"toposerver_memory_limit_bytes",
		"Memory limit of the etcd container, or zero when unlimited.",
		memberLabels,
	)
	topoRestarts = topologyGauge(
		"toposerver_member_restarts_total",
		"Observed etcd container restart count; resets when the pod is replaced.",
		memberLabels,
	)
	topoOOM = topologyGauge(
		"toposerver_member_oom_timestamp_seconds",
		"Unix timestamp of the most recent observed OOM termination, or zero.",
		memberLabels,
	)
	failoverReady = topologyGauge(
		"cluster_failover_ready",
		"Topology access and multiorch readiness: 1 ready, 0 unavailable, -1 unknown.",
		[]string{"cluster", "target_namespace", "reason"},
	)
	failoverChecked = topologyGauge(
		"cluster_failover_checked_timestamp_seconds",
		"Unix timestamp of the latest failover readiness observation.",
		[]string{"cluster", "target_namespace"},
	)
)

var topologyCollectors = []*prometheus.GaugeVec{
	topoQuorum, topoChecked, topoMemberUp, topoBackend, topoBackendInUse,
	topoRevision, topoQuota, topoMemoryLimit, topoRestarts, topoOOM, failoverReady, failoverChecked,
}

// TopologyMemberMetrics contains the latest observations of a managed etcd pod.
// Nil values mean an observation failed, rather than a zero-sized backend.
type TopologyMemberMetrics struct {
	Name                                                 string
	Up                                                   bool
	BackendBytes, BackendInUseBytes, Revision            *int64
	QuotaBytes, MemoryLimitBytes, Restarts, OOMTimestamp *int64
}

func conditionValue(status metav1.ConditionStatus) float64 {
	switch status {
	case metav1.ConditionTrue:
		return 1
	case metav1.ConditionFalse:
		return 0
	default:
		return -1
	}
}

// DeleteTopologyHealth removes observations after deletion and before replacement.
func DeleteTopologyHealth(name, namespace string) {
	for _, metric := range []*prometheus.GaugeVec{topoQuorum, topoChecked, topoMemberUp, topoBackend, topoBackendInUse, topoRevision, topoQuota, topoMemoryLimit, topoRestarts, topoOOM} {
		metric.DeletePartialMatch(prometheus.Labels{"name": name, "target_namespace": namespace})
	}
}

func SetTopologyHealth(
	cluster, name, namespace string,
	condition metav1.Condition,
	checked time.Time,
	members []TopologyMemberMetrics,
) {
	DeleteTopologyHealth(name, namespace)
	topoQuorum.WithLabelValues(cluster, name, namespace, condition.Reason).
		Set(conditionValue(condition.Status))
	topoChecked.WithLabelValues(cluster, name, namespace).Set(float64(checked.Unix()))
	for _, member := range members {
		labels := []string{cluster, name, namespace, member.Name}
		up := 0.0
		if member.Up {
			up = 1
		}
		topoMemberUp.WithLabelValues(labels...).Set(up)
		for metric, value := range map[*prometheus.GaugeVec]*int64{
			topoBackend: member.BackendBytes, topoBackendInUse: member.BackendInUseBytes,
			topoRevision: member.Revision, topoQuota: member.QuotaBytes,
			topoMemoryLimit: member.MemoryLimitBytes, topoRestarts: member.Restarts, topoOOM: member.OOMTimestamp,
		} {
			if value != nil {
				metric.WithLabelValues(labels...).Set(float64(*value))
			}
		}
	}
}

func DeleteFailoverHealth(cluster, namespace string) {
	labels := prometheus.Labels{"cluster": cluster, "target_namespace": namespace}
	failoverReady.DeletePartialMatch(labels)
	failoverChecked.DeletePartialMatch(labels)
}

func SetFailoverHealth(cluster, namespace string, condition metav1.Condition) {
	DeleteFailoverHealth(cluster, namespace)
	failoverReady.WithLabelValues(cluster, namespace, condition.Reason).
		Set(conditionValue(condition.Status))
	failoverChecked.WithLabelValues(cluster, namespace).Set(float64(time.Now().Unix()))
}
