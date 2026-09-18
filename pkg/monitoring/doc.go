// Package monitoring provides Prometheus metrics and recording helpers for
// the Multigres Operator. It exposes domain-specific gauges and counters
// that complement the generic controller-runtime metrics already registered
// by the framework.
//
// All metrics follow the naming convention multigres_<component>_<metric>_<unit>
// and are registered against controller-runtime's default Prometheus registry
// on import.
//
// Usage in controllers:
//
//	monitoring.SetClusterInfo(cluster.Name, cluster.Namespace, string(cluster.Status.Phase), initialized)
//	monitoring.SetCellGatewayReplicas(cell.Name, cell.Namespace, desired, ready)
//
// Usage in webhooks:
//
//	monitoring.RecordWebhookRequest("CREATE", "MultigresCluster", err, elapsed)
package monitoring
