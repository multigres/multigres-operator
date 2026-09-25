package multigrescluster

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

const (
	conditionTopologyQuorumAvailable = "TopologyQuorumAvailable"
	conditionFailoverReady           = "FailoverReady"
	clusterHealthInterval            = 30 * time.Second
	topologyHealthMaxAge             = 2 * time.Minute
)

type shardHealthIdentity struct {
	database   multigresv1alpha1.DatabaseName
	tableGroup multigresv1alpha1.TableGroupName
	shard      multigresv1alpha1.ShardName
}

func (r *MultigresClusterReconciler) updateHealthConditions(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	globalTopoSpec *multigresv1alpha1.GlobalTopoServerSpec,
	servers []multigresv1alpha1.TopoServer,
) {
	quorum := r.topologyQuorumCondition(ctx, cluster, globalTopoSpec, servers)
	meta.SetStatusCondition(&cluster.Status.Conditions, quorum)
	failover := metav1.Condition{
		Type:               conditionFailoverReady,
		ObservedGeneration: cluster.Generation,
		Status:             metav1.ConditionTrue,
		Reason:             "FailoverReady",
		Message:            "Topology access and all shard orchestrators are ready",
	}
	orch := failover
	shards := &multigresv1alpha1.ShardList{}
	if err := r.childStatusReader().
		List(ctx, shards, client.InNamespace(cluster.Namespace), client.MatchingLabels{metadata.LabelMultigresCluster: cluster.Name}); err != nil {
		orch.Status, orch.Reason, orch.Message = metav1.ConditionUnknown, "ObservationFailed", "Could not observe shard orchestrators: "+err.Error()
	} else {
		orch = orchestratorCondition(cluster, shards.Items, orch)
	}
	access := meta.FindStatusCondition(cluster.Status.Conditions, conditionTopologyReady)
	switch {
	case quorum.Status == metav1.ConditionFalse:
		failover.Status, failover.Reason, failover.Message = quorum.Status, quorum.Reason, quorum.Message
	case access != nil && access.ObservedGeneration == cluster.Generation && access.Status == metav1.ConditionFalse:
		failover.Status, failover.Reason = metav1.ConditionFalse, access.Reason
		failover.Message = "Failover protection is unavailable: " + access.Message
	case orch.Status == metav1.ConditionFalse:
		failover = orch
	case quorum.Status == metav1.ConditionUnknown && quorum.Reason != "ExternalTopology":
		failover.Status, failover.Reason, failover.Message = quorum.Status, quorum.Reason, quorum.Message
	case access == nil || access.ObservedGeneration != cluster.Generation || access.Status != metav1.ConditionTrue:
		failover.Status, failover.Reason, failover.Message = metav1.ConditionUnknown, "TopologyUnchecked", "Topology access has not been verified for the current configuration"
	default:
		failover = orch
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, failover)
	monitoring.SetFailoverHealth(cluster.Name, cluster.Namespace, failover)
}

func (r *MultigresClusterReconciler) topologyQuorumCondition(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	global *multigresv1alpha1.GlobalTopoServerSpec,
	servers []multigresv1alpha1.TopoServer,
) metav1.Condition {
	condition := metav1.Condition{
		Type:               conditionTopologyQuorumAvailable,
		ObservedGeneration: cluster.Generation,
		Status:             metav1.ConditionTrue,
		Reason:             "QuorumAvailable",
		Message:            "All managed topology servers have quorum",
	}
	var expected []string
	external := global.External != nil
	if global.Etcd != nil {
		expected = append(expected, cluster.Name+"-global-topo")
	}
	res := resolver.NewResolver(r.Client, cluster.Namespace)
	for _, cell := range cluster.Spec.Cells {
		cell.CellTemplate = cluster.Spec.EffectiveCellTemplate(cell.CellTemplate)
		_, _, local, err := res.ResolveCell(ctx, cluster, &cell)
		if err != nil {
			condition.Status, condition.Reason, condition.Message = metav1.ConditionUnknown, "ObservationFailed", "Could not resolve local topology: "+err.Error()
			return condition
		}
		if local != nil && local.Etcd != nil {
			cellName := name.JoinWithConstraints(
				name.DefaultConstraints,
				cluster.Name,
				string(cell.Name),
			)
			expected = append(expected, topo.ManagedLocalTopoServerName(cellName))
		}
		if local != nil && local.External != nil {
			external = true
		}
	}
	byName := make(map[string]*multigresv1alpha1.TopoServer, len(servers))
	for i := range servers {
		byName[servers[i].Name] = &servers[i]
	}
	for _, serverName := range expected {
		server := byName[serverName]
		if server == nil || !server.DeletionTimestamp.IsZero() {
			condition.Status, condition.Reason = metav1.ConditionFalse, "TopologyMissing"
			condition.Message = fmt.Sprintf(
				"Topology server %s is missing or being deleted; failover protection is unavailable",
				serverName,
			)
			return condition
		}
		observed := meta.FindStatusCondition(server.Status.Conditions, "QuorumAvailable")
		if observed == nil || observed.ObservedGeneration != server.Generation ||
			server.Status.HealthCheckedAt == nil ||
			time.Since(server.Status.HealthCheckedAt.Time) > topologyHealthMaxAge {
			condition.Status, condition.Reason, condition.Message = metav1.ConditionUnknown, "TopologyHealthStale", fmt.Sprintf(
				"No recent quorum observation for %s",
				serverName,
			)
			continue
		}
		if observed.Status == metav1.ConditionFalse {
			condition.Status, condition.Reason, condition.Message = observed.Status, observed.Reason, fmt.Sprintf(
				"%s: %s",
				serverName,
				observed.Message,
			)
			return condition
		}
		if observed.Status == metav1.ConditionUnknown {
			condition.Status, condition.Reason, condition.Message = observed.Status, observed.Reason, fmt.Sprintf(
				"%s: %s",
				serverName,
				observed.Message,
			)
		}
	}
	if external && condition.Status == metav1.ConditionTrue {
		condition.Status, condition.Reason, condition.Message = metav1.ConditionUnknown, "ExternalTopology", "External topology quorum is not monitored; topology registration reports access separately"
	}
	return condition
}

func orchestratorCondition(
	cluster *multigresv1alpha1.MultigresCluster,
	shards []multigresv1alpha1.Shard,
	condition metav1.Condition,
) metav1.Condition {
	observed := make(map[shardHealthIdentity]*multigresv1alpha1.Shard, len(shards))
	for i := range shards {
		s := &shards[i]
		observed[shardHealthIdentity{s.Spec.DatabaseName, s.Spec.TableGroupName, s.Spec.ShardName}] = s
	}
	for _, db := range cluster.Spec.Databases {
		for _, tg := range db.TableGroups {
			for _, desired := range tg.Shards {
				s := observed[shardHealthIdentity{db.Name, tg.Name, desired.Name}]
				if s != nil && s.DeletionTimestamp.IsZero() &&
					s.Status.ObservedGeneration != s.Generation {
					condition.Status, condition.Reason, condition.Message = metav1.ConditionUnknown, "OrchestratorHealthStale", fmt.Sprintf(
						"Shard %s readiness predates its current configuration",
						s.Name,
					)
					continue
				}
				if s == nil || !s.DeletionTimestamp.IsZero() || !s.Status.OrchReady {
					condition.Status, condition.Reason = metav1.ConditionFalse, "OrchestratorUnavailable"
					condition.Message = fmt.Sprintf(
						"Shard %s/%s/%s has no ready multiorch observation; failover protection is unavailable",
						db.Name,
						tg.Name,
						desired.Name,
					)
					return condition
				}

			}
		}
	}
	return condition
}
