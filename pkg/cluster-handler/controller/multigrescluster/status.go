package multigrescluster

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

type tableGroupStatusIdentity struct {
	database   multigresv1alpha1.DatabaseName
	tableGroup multigresv1alpha1.TableGroupName
}

func (r *MultigresClusterReconciler) childStatusReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// extractExternalEndpoint resolves the externally reachable endpoint for the
// global multigateway Service. It prefers explicitly assigned Service
// ExternalIPs, then falls back to load balancer ingress (hostname over IP).
// Returns empty string when no endpoint has been provisioned.
func extractExternalEndpoint(svc *corev1.Service) string {
	if svc == nil {
		return ""
	}
	if len(svc.Spec.ExternalIPs) > 0 && svc.Spec.ExternalIPs[0] != "" {
		return svc.Spec.ExternalIPs[0]
	}
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return ""
	}
	ing := svc.Status.LoadBalancer.Ingress[0]
	if ing.Hostname != "" {
		return ing.Hostname
	}
	return ing.IP
}

// computeGatewayCondition returns the GatewayExternalReady condition for the
// given inputs, or nil when external gateway is disabled (condition should be
// removed from the array).
func computeGatewayCondition(
	enabled bool,
	externalEndpoint string,
	aggregateReadyGateways int32,
	clusterGeneration int64,
) *metav1.Condition {
	if !enabled {
		return nil
	}

	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionGatewayExternalReady,
		ObservedGeneration: clusterGeneration,
		LastTransitionTime: metav1.Now(),
	}

	switch {
	case externalEndpoint == "":
		cond.Status = metav1.ConditionFalse
		cond.Reason = multigresv1alpha1.ReasonAwaitingEndpoint
		cond.Message = "Waiting for gateway service endpoint"
	case aggregateReadyGateways == 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = multigresv1alpha1.ReasonNoReadyGateways
		cond.Message = "External endpoint assigned but no multigateway pods are ready"
	default:
		cond.Status = metav1.ConditionTrue
		cond.Reason = multigresv1alpha1.ReasonEndpointReady
		cond.Message = fmt.Sprintf("External endpoint %s is serving traffic", externalEndpoint)
	}

	return &cond
}

// computeAdminWebCondition returns the AdminWebExternalReady condition for the
// given inputs, or nil when external admin web is disabled (condition should be
// removed from the array).
func computeAdminWebCondition(
	enabled bool,
	externalEndpoint string,
	readyReplicas int32,
	clusterGeneration int64,
) *metav1.Condition {
	if !enabled {
		return nil
	}

	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionAdminWebExternalReady,
		ObservedGeneration: clusterGeneration,
		LastTransitionTime: metav1.Now(),
	}

	switch {
	case externalEndpoint == "":
		cond.Status = metav1.ConditionFalse
		cond.Reason = multigresv1alpha1.ReasonAwaitingEndpoint
		cond.Message = "Waiting for admin web service endpoint"
	case readyReplicas == 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = multigresv1alpha1.ReasonNoReadyAdminWeb
		cond.Message = "External endpoint assigned but no multiadmin-web pods are ready"
	default:
		cond.Status = metav1.ConditionTrue
		cond.Reason = multigresv1alpha1.ReasonEndpointReady
		cond.Message = fmt.Sprintf("External endpoint %s is serving traffic", externalEndpoint)
	}

	return &cond
}

func (r *MultigresClusterReconciler) updateStatus(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	globalTopoSpec, err := resolver.NewResolver(r.Client, cluster.Namespace).
		ResolveGlobalTopo(ctx, cluster)
	if err != nil {
		return fmt.Errorf("failed to resolve global topo for status: %w", err)
	}
	childReader := r.childStatusReader()

	oldPhase := cluster.Status.Phase
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Status.Cells = make(map[multigresv1alpha1.CellName]multigresv1alpha1.CellStatusSummary)
	cluster.Status.Databases = make(
		map[multigresv1alpha1.DatabaseName]multigresv1alpha1.DatabaseStatusSummary,
	)

	cells := &multigresv1alpha1.CellList{}
	if err := childReader.List(
		ctx,
		cells,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"multigres.com/cluster": cluster.Name},
	); err != nil {
		return fmt.Errorf("failed to list cells for status: %w", err)
	}

	for _, c := range cells.Items {
		ready := false
		for _, cond := range c.Status.Conditions {
			if cond.Type == "Available" && cond.Status == "True" &&
				c.Status.ObservedGeneration == c.Generation {
				ready = true
				break
			}
		}
		cluster.Status.Cells[multigresv1alpha1.CellName(c.Spec.Name)] = multigresv1alpha1.CellStatusSummary{
			Ready:           ready,
			GatewayReplicas: c.Status.GatewayReplicas,
		}
	}

	tgs := &multigresv1alpha1.TableGroupList{}
	if err := childReader.List(
		ctx,
		tgs,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"multigres.com/cluster": cluster.Name},
	); err != nil {
		return fmt.Errorf("failed to list tablegroups for status: %w", err)
	}

	dbShards := make(map[multigresv1alpha1.DatabaseName]struct {
		Ready int32
		Total int32
	})

	for _, tg := range tgs.Items {
		stat := dbShards[multigresv1alpha1.DatabaseName(tg.Spec.DatabaseName)]
		// If TableGroup is stale, don't count it as providing ready shards?
		// Or simpler: just ensure we don't count ready shards if TG is stale.
		// However, TG.Status.ReadyShards might be stale itself.
		// Strict check:
		if tg.Status.ObservedGeneration == tg.Generation {
			stat.Ready += tg.Status.ReadyShards
		}
		stat.Total += tg.Status.TotalShards
		dbShards[multigresv1alpha1.DatabaseName(tg.Spec.DatabaseName)] = stat

	}

	for dbName, stat := range dbShards {
		cluster.Status.Databases[dbName] = multigresv1alpha1.DatabaseStatusSummary{
			ReadyShards: stat.Ready,
			TotalShards: stat.Total,
		}
	}

	// Calculate Cluster Phase
	var (
		anyDegraded bool
		allHealthy  = true
	)
	expectedCells := make(map[multigresv1alpha1.CellName]struct{}, len(cluster.Spec.Cells))
	for _, cell := range cluster.Spec.Cells {
		expectedCells[cell.Name] = struct{}{}
	}
	expectedTableGroups := make(map[tableGroupStatusIdentity]struct{})
	for _, database := range cluster.Spec.Databases {
		for _, tableGroup := range database.TableGroups {
			expectedTableGroups[tableGroupStatusIdentity{
				database:   database.Name,
				tableGroup: tableGroup.Name,
			}] = struct{}{}
		}
	}
	expectsGlobalTopoServer := globalTopoSpec.Etcd != nil

	for _, c := range cells.Items {
		delete(expectedCells, c.Spec.Name)
		switch {
		case c.Status.ObservedGeneration != c.Generation:
			allHealthy = false
		case c.Status.Phase == multigresv1alpha1.PhaseDegraded:
			anyDegraded = true
			allHealthy = false
		case c.Status.Phase == multigresv1alpha1.PhaseHealthy:
			// ok
		default:
			allHealthy = false
		}
	}

	for _, tg := range tgs.Items {
		delete(expectedTableGroups, tableGroupStatusIdentity{
			database:   tg.Spec.DatabaseName,
			tableGroup: tg.Spec.TableGroupName,
		})
		switch {
		case tg.Status.ObservedGeneration != tg.Generation:
			allHealthy = false
		case tg.Status.Phase == multigresv1alpha1.PhaseDegraded:
			anyDegraded = true
			allHealthy = false
		case tg.Status.Phase == multigresv1alpha1.PhaseHealthy:
			// ok
		default:
			allHealthy = false
		}
	}

	topoServers := &multigresv1alpha1.TopoServerList{}
	if err := childReader.List(
		ctx,
		topoServers,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{"multigres.com/cluster": cluster.Name},
	); err != nil {
		return fmt.Errorf("failed to list toposervers for status: %w", err)
	}

	for _, ts := range topoServers.Items {
		if expectsGlobalTopoServer && ts.Name == cluster.Name+"-global-topo" {
			expectsGlobalTopoServer = false
		}
		switch {
		case ts.Status.ObservedGeneration != ts.Generation:
			allHealthy = false
		case ts.Status.Phase == multigresv1alpha1.PhaseDegraded:
			anyDegraded = true
			allHealthy = false
		case ts.Status.Phase == multigresv1alpha1.PhaseHealthy:
			// ok
		default:
			allHealthy = false
		}
	}

	if len(expectedCells) > 0 || len(expectedTableGroups) > 0 || expectsGlobalTopoServer {
		allHealthy = false
	}

	switch {
	case anyDegraded:
		cluster.Status.Phase = multigresv1alpha1.PhaseDegraded
		cluster.Status.Message = "Cluster is degraded"
	case allHealthy:
		cluster.Status.Phase = multigresv1alpha1.PhaseHealthy
		cluster.Status.Message = "Ready"
	default:
		cluster.Status.Phase = multigresv1alpha1.PhaseProgressing
		cluster.Status.Message = "Cluster is progressing"
	}

	if cluster.Status.Phase == multigresv1alpha1.PhaseHealthy &&
		cluster.Status.InitializedAt == nil {
		now := metav1.Now()
		cluster.Status.InitializedAt = &now
	}

	allCellsReady := true
	for _, c := range cluster.Status.Cells {
		if !c.Ready {
			allCellsReady = false
			break
		}
	}

	statusStr := metav1.ConditionFalse
	if allCellsReady && len(cluster.Status.Cells) > 0 {
		statusStr = metav1.ConditionTrue
	}

	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               "Available",
		Status:             statusStr,
		Reason:             "AggregatedStatus",
		Message:            "Aggregation of cell availability",
		LastTransitionTime: metav1.Now(),
	})

	// Gateway external endpoint and condition
	gwEnabled := cluster.Spec.ExternalGateway != nil && cluster.Spec.ExternalGateway.Enabled
	var externalEndpoint string
	if gwEnabled {
		gwSvc := &corev1.Service{}
		gwSvcKey := types.NamespacedName{
			Name:      fmt.Sprintf("%s-multigateway", cluster.Name),
			Namespace: cluster.Namespace,
		}
		if err := r.Get(ctx, gwSvcKey, gwSvc); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to get global multigateway service for status: %w", err)
			}
			// NotFound is normal during first reconciliation; treat as empty endpoint.
		} else {
			externalEndpoint = extractExternalEndpoint(gwSvc)
		}
	}

	var aggregateReadyGateways int32
	for i := range cells.Items {
		c := cells.Items[i]
		if c.Status.ObservedGeneration == c.Generation {
			aggregateReadyGateways += c.Status.GatewayReadyReplicas
		}
	}

	if gwEnabled {
		cluster.Status.Gateway = &multigresv1alpha1.GatewayStatus{
			ExternalEndpoint: externalEndpoint,
		}
	} else {
		cluster.Status.Gateway = nil
	}

	gwCond := computeGatewayCondition(
		gwEnabled,
		externalEndpoint,
		aggregateReadyGateways,
		cluster.Generation,
	)
	if gwCond != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, *gwCond)
	} else {
		meta.RemoveStatusCondition(
			&cluster.Status.Conditions,
			multigresv1alpha1.ConditionGatewayExternalReady,
		)
	}

	// Admin Web external endpoint and condition
	awEnabled := cluster.Spec.ExternalAdminWeb != nil && cluster.Spec.ExternalAdminWeb.Enabled
	var awExternalEndpoint string
	if awEnabled {
		awSvc := &corev1.Service{}
		awSvcKey := types.NamespacedName{
			Name:      fmt.Sprintf("%s-multiadmin-web", cluster.Name),
			Namespace: cluster.Namespace,
		}
		if err := r.Get(ctx, awSvcKey, awSvc); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to get multiadmin-web service for status: %w", err)
			}
		} else {
			awExternalEndpoint = extractExternalEndpoint(awSvc)
		}
	}

	var awReadyReplicas int32
	if awEnabled {
		awDeploy := &appsv1.Deployment{}
		awDeployKey := types.NamespacedName{
			Name:      fmt.Sprintf("%s-multiadmin-web", cluster.Name),
			Namespace: cluster.Namespace,
		}
		if err := r.Get(ctx, awDeployKey, awDeploy); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to get multiadmin-web deployment for status: %w", err)
			}
		} else {
			awReadyReplicas = awDeploy.Status.ReadyReplicas
		}
	}

	if awEnabled {
		cluster.Status.AdminWeb = &multigresv1alpha1.AdminWebStatus{
			ExternalEndpoint: awExternalEndpoint,
		}
	} else {
		cluster.Status.AdminWeb = nil
	}

	awCond := computeAdminWebCondition(
		awEnabled,
		awExternalEndpoint,
		awReadyReplicas,
		cluster.Generation,
	)
	if awCond != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, *awCond)
	} else {
		meta.RemoveStatusCondition(
			&cluster.Status.Conditions,
			multigresv1alpha1.ConditionAdminWebExternalReady,
		)
	}

	// 1. Construct the Patch Object
	patchObj := &multigresv1alpha1.MultigresCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "MultigresCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
		Status: cluster.Status,
	}

	// 2. Apply the Patch
	if oldPhase != cluster.Status.Phase {
		r.Recorder.Eventf(
			cluster,
			"Normal",
			"PhaseChange",
			"Transitioned from '%s' to '%s'",
			oldPhase,
			cluster.Status.Phase,
		)
	}

	// Note: We rely on Server-Side Apply (SSA) to handle idempotency.
	// If the status hasn't changed, the API server will treat this Patch as a no-op,
	// so we don't need a manual DeepEqual check here.
	if err := r.Status().Patch(
		ctx,
		patchObj,
		client.Apply,
		client.FieldOwner("multigres-operator"),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to patch status: %w", err)
	}

	return nil
}
