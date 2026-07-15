package observer

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
)

func (o *Observer) checkResources(ctx context.Context) {
	o.checkOwnerReferences(ctx)
	o.checkLabelConsistency(ctx)
	o.checkOrphanedResources(ctx)
	o.checkFinalizersAndDeletion(ctx)
	o.checkPVCValidation(ctx)
	o.checkServiceEndpoints(ctx)
}

func (o *Observer) checkOwnerReferences(ctx context.Context) {
	// Verify CRD parent-child ownership chain.
	var clusters multigresv1alpha1.MultigresClusterList
	if err := o.client.List(ctx, &clusters, o.listOpts()...); err != nil {
		o.reporter.Report(report.Finding{
			Severity: report.SeverityError,
			Check:    "resource-validation",
			Message:  fmt.Sprintf("failed to list MultigresCluster CRDs: %v", err),
		})
		return
	}

	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		o.checkCellOwnership(ctx, cluster)
		o.checkTableGroupOwnership(ctx, cluster)
		o.checkTopoServerOwnership(ctx, cluster)
	}
}

func (o *Observer) checkCellOwnership(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) {
	var cells multigresv1alpha1.CellList
	if err := o.client.List(ctx, &cells,
		o.listOpts(client.MatchingLabels{common.LabelMultigresCluster: cluster.Name})...,
	); err != nil {
		return
	}

	for i := range cells.Items {
		cell := &cells.Items[i]
		if !hasOwnerRef(cell.OwnerReferences, cluster.UID) {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityFatal,
				Check:     "resource-validation",
				Component: fmt.Sprintf("cell/%s/%s", cell.Namespace, cell.Name),
				Message: fmt.Sprintf(
					"Cell %s missing ownerReference to cluster %s",
					cell.Name,
					cluster.Name,
				),
			})
		}

		// Check Multigateway resources owned by Cell.
		o.checkMultigatewayResources(ctx, cell)
	}
}

func (o *Observer) checkMultigatewayResources(ctx context.Context, cell *multigresv1alpha1.Cell) {
	// Use the short cell name from the Cell's label, not the CRD name.
	cellLabelValue := cell.Labels[common.LabelMultigresCell]

	var deploys appsv1.DeploymentList
	if err := o.client.List(ctx, &deploys,
		o.listOpts(client.MatchingLabels{
			common.LabelAppComponent:  common.ComponentMultigateway,
			common.LabelMultigresCell: cellLabelValue,
		})...,
	); err != nil {
		return
	}

	if len(deploys.Items) == 0 {
		if cell.DeletionTimestamp == nil {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityError,
				Check:     "resource-validation",
				Component: fmt.Sprintf("cell/%s/%s", cell.Namespace, cell.Name),
				Message:   fmt.Sprintf("No Multigateway deployment found for cell %s", cell.Name),
			})
		}
		return
	}

	var svcs corev1.ServiceList
	if err := o.client.List(ctx, &svcs,
		o.listOpts(client.MatchingLabels{
			common.LabelAppComponent:  common.ComponentMultigateway,
			common.LabelMultigresCell: cellLabelValue,
		})...,
	); err != nil {
		return
	}

	if len(svcs.Items) == 0 && cell.DeletionTimestamp == nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "resource-validation",
			Component: fmt.Sprintf("cell/%s/%s", cell.Namespace, cell.Name),
			Message:   fmt.Sprintf("No Multigateway service found for cell %s", cell.Name),
		})
	}
}

func (o *Observer) checkTableGroupOwnership(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) {
	var tgs multigresv1alpha1.TableGroupList
	if err := o.client.List(ctx, &tgs,
		o.listOpts(client.MatchingLabels{common.LabelMultigresCluster: cluster.Name})...,
	); err != nil {
		return
	}

	for i := range tgs.Items {
		tg := &tgs.Items[i]
		if !hasOwnerRef(tg.OwnerReferences, cluster.UID) {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityFatal,
				Check:     "resource-validation",
				Component: fmt.Sprintf("tablegroup/%s/%s", tg.Namespace, tg.Name),
				Message: fmt.Sprintf(
					"TableGroup %s missing ownerReference to cluster %s",
					tg.Name,
					cluster.Name,
				),
			})
		}

		o.checkShardOwnership(ctx, tg)
	}
}

func (o *Observer) checkShardOwnership(ctx context.Context, tg *multigresv1alpha1.TableGroup) {
	var shards multigresv1alpha1.ShardList
	if err := o.client.List(ctx, &shards,
		o.listOpts(client.MatchingLabels{common.LabelMultigresTableGroup: tg.Name})...,
	); err != nil {
		return
	}

	for i := range shards.Items {
		shard := &shards.Items[i]
		if !hasOwnerRef(shard.OwnerReferences, tg.UID) {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityFatal,
				Check:     "resource-validation",
				Component: fmt.Sprintf("shard/%s/%s", shard.Namespace, shard.Name),
				Message: fmt.Sprintf(
					"Shard %s missing ownerReference to tablegroup %s",
					shard.Name,
					tg.Name,
				),
			})
		}
	}
}

func (o *Observer) checkTopoServerOwnership(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) {
	var topos multigresv1alpha1.TopoServerList
	if err := o.client.List(ctx, &topos,
		o.listOpts(client.MatchingLabels{common.LabelMultigresCluster: cluster.Name})...,
	); err != nil {
		return
	}

	for i := range topos.Items {
		ts := &topos.Items[i]
		if !hasOwnerRef(ts.OwnerReferences, cluster.UID) {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityFatal,
				Check:     "resource-validation",
				Component: fmt.Sprintf("toposerver/%s/%s", ts.Namespace, ts.Name),
				Message: fmt.Sprintf(
					"TopoServer %s missing ownerReference to cluster %s",
					ts.Name,
					cluster.Name,
				),
			})
		}
	}
}

func (o *Observer) checkLabelConsistency(ctx context.Context) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{common.LabelAppManagedBy: common.ManagedByMultigres})...,
	); err != nil {
		return
	}

	for i := range pods.Items {
		pod := &pods.Items[i]

		if pod.Labels[common.LabelAppManagedBy] != common.ManagedByMultigres {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "resource-validation",
				Component: componentForPod(pod),
				Message:   fmt.Sprintf("Pod %s has incorrect managed-by label", pod.Name),
			})
		}

		// Pool pods should have a spec-hash annotation.
		if pod.Labels[common.LabelAppComponent] == common.ComponentPool {
			if _, ok := pod.Annotations[common.AnnotationSpecHash]; !ok {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityWarn,
					Check:     "resource-validation",
					Component: componentForPod(pod),
					Message:   fmt.Sprintf("Pool pod %s missing spec-hash annotation", pod.Name),
				})
			}
		}
	}
}

func (o *Observer) checkOrphanedResources(ctx context.Context) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{common.LabelAppManagedBy: common.ManagedByMultigres})...,
	); err != nil {
		return
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		if len(pod.OwnerReferences) == 0 && pod.DeletionTimestamp == nil {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityFatal,
				Check:     "resource-validation",
				Component: componentForPod(pod),
				Message:   fmt.Sprintf("Orphaned pod %s with no ownerReference", pod.Name),
				Details:   map[string]any{"pod": pod.Name},
			})
		}
	}
}

func (o *Observer) checkFinalizersAndDeletion(ctx context.Context) {
	now := time.Now()

	// Check for stuck CRDs with unexpected finalizers.
	checkStuckResource := func(obj metav1.ObjectMeta, kind string) {
		if obj.DeletionTimestamp == nil {
			return
		}
		if len(obj.Finalizers) > 0 {
			age := now.Sub(obj.DeletionTimestamp.Time)
			if age > common.TerminatingResourceTimeout {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityError,
					Check:     "resource-validation",
					Component: fmt.Sprintf("%s/%s/%s", kind, obj.Namespace, obj.Name),
					Message: fmt.Sprintf(
						"%s %s stuck in Terminating for %s with finalizers: %v",
						kind,
						obj.Name,
						age.Round(time.Second),
						obj.Finalizers,
					),
				})
			}
		}
	}

	var shards multigresv1alpha1.ShardList
	if err := o.client.List(ctx, &shards, o.listOpts()...); err == nil {
		for i := range shards.Items {
			checkStuckResource(shards.Items[i].ObjectMeta, "Shard")
		}
	}

	var cells multigresv1alpha1.CellList
	if err := o.client.List(ctx, &cells, o.listOpts()...); err == nil {
		for i := range cells.Items {
			checkStuckResource(cells.Items[i].ObjectMeta, "Cell")
		}
	}

	var tgs multigresv1alpha1.TableGroupList
	if err := o.client.List(ctx, &tgs, o.listOpts()...); err == nil {
		for i := range tgs.Items {
			checkStuckResource(tgs.Items[i].ObjectMeta, "TableGroup")
		}
	}

	var clusters multigresv1alpha1.MultigresClusterList
	if err := o.client.List(ctx, &clusters, o.listOpts()...); err == nil {
		for i := range clusters.Items {
			checkStuckResource(clusters.Items[i].ObjectMeta, "MultigresCluster")
		}
	}
}

func hasOwnerRef(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}

func (o *Observer) checkPVCValidation(ctx context.Context) {
	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy: common.ManagedByMultigres,
			common.LabelAppComponent: common.ComponentPool,
		})...,
	); err != nil {
		return
	}

	var pvcs corev1.PersistentVolumeClaimList
	if err := o.client.List(ctx, &pvcs, o.listOpts()...); err != nil {
		return
	}

	pvcByName := make(map[string]*corev1.PersistentVolumeClaim, len(pvcs.Items))
	for i := range pvcs.Items {
		pvcByName[pvcs.Items[i].Name] = &pvcs.Items[i]
	}

	// Track which PVCs are referenced by pods.
	referencedPVCs := make(map[string]bool)

	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}

		inGracePeriod := o.isPodInGracePeriod(pod.Name)

		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			claimName := vol.PersistentVolumeClaim.ClaimName
			referencedPVCs[claimName] = true

			if inGracePeriod {
				continue
			}

			pvc, exists := pvcByName[claimName]
			if !exists {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityFatal,
					Check:     "resource-validation",
					Component: componentForPod(pod),
					Message: fmt.Sprintf(
						"Pod %s references non-existent PVC %s",
						pod.Name, claimName,
					),
					Details: map[string]any{
						"pod":       pod.Name,
						"pvcName":   claimName,
						"volume":    vol.Name,
					},
				})
				continue
			}

			if pvc.Status.Phase != corev1.ClaimBound {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityError,
					Check:     "resource-validation",
					Component: componentForPod(pod),
					Message: fmt.Sprintf(
						"Pod %s PVC %s is %s (expected Bound)",
						pod.Name, claimName, pvc.Status.Phase,
					),
					Details: map[string]any{
						"pod":      pod.Name,
						"pvcName":  claimName,
						"pvcPhase": string(pvc.Status.Phase),
					},
				})
			}
		}
	}

	// Check for orphaned PVCs with multigres labels but no pod referencing them.
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if pvc.DeletionTimestamp != nil {
			continue
		}
		if pvc.Labels[common.LabelAppManagedBy] != common.ManagedByMultigres {
			continue
		}
		if referencedPVCs[pvc.Name] {
			continue
		}

		staleKey := "pvc/orphaned/" + pvc.Name
		now := time.Now()
		since, tracked := o.generationDivergeSince[staleKey]
		if !tracked {
			o.generationDivergeSince[staleKey] = now
			continue
		}
		if now.Sub(since) > common.StaleStatusEntryGracePeriod {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityInfo,
				Check:     "resource-validation",
				Component: fmt.Sprintf("pvc/%s/%s", pvc.Namespace, pvc.Name),
				Message: fmt.Sprintf(
					"Orphaned PVC %s has multigres labels but no pod references it",
					pvc.Name,
				),
				Details: map[string]any{
					"pvcName": pvc.Name,
					"labels":  pvc.Labels,
				},
			})
		}
	}
}

func (o *Observer) checkServiceEndpoints(ctx context.Context) {
	var svcs corev1.ServiceList
	if err := o.client.List(ctx, &svcs,
		o.listOpts(client.MatchingLabels{
			common.LabelAppManagedBy: common.ManagedByMultigres,
		})...,
	); err != nil {
		return
	}

	for i := range svcs.Items {
		svc := &svcs.Items[i]
		if svc.DeletionTimestamp != nil {
			continue
		}
		// Skip headless services (ClusterIP: None) — they don't have traditional Endpoints.
		if svc.Spec.ClusterIP == "None" {
			continue
		}

		comp := fmt.Sprintf("service/%s/%s", svc.Namespace, svc.Name)

		var endpoints corev1.Endpoints
		if err := o.client.Get(ctx, types.NamespacedName{
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}, &endpoints); err != nil {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "resource-validation",
				Component: comp,
				Message:   fmt.Sprintf("Service %s has no Endpoints object", svc.Name),
			})
			continue
		}

		readyAddresses := 0
		for _, subset := range endpoints.Subsets {
			readyAddresses += len(subset.Addresses)
		}

		if readyAddresses == 0 {
			// Skip if all pods for this component are in grace period.
			svcComponent := svc.Labels[common.LabelAppComponent]
			if svcComponent != "" && o.hasAnyPodInGracePeriod() {
				continue
			}

			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "resource-validation",
				Component: comp,
				Message: fmt.Sprintf(
					"Service %s Endpoints has zero ready addresses",
					svc.Name,
				),
				Details: map[string]any{
					"service":   svc.Name,
					"component": svcComponent,
				},
			})
		}
	}
}
