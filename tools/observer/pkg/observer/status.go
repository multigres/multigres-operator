package observer

import (
	"context"
	"fmt"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
)

func (o *Observer) checkCRDStatus(ctx context.Context) {
	o.checkClusterStatus(ctx)
	o.checkShardStatus(ctx)
	o.checkCellStatus(ctx)
	o.checkTopoServerStatus(ctx)
	o.collectCRDProbeData(ctx)
}

func (o *Observer) collectCRDProbeData(ctx context.Context) {
	if o.probes == nil {
		return
	}

	crdData := make(map[string]any)

	var clusters multigresv1alpha1.MultigresClusterList
	if err := o.client.List(ctx, &clusters, o.listOpts()...); err == nil {
		items := make([]map[string]any, 0, len(clusters.Items))
		for i := range clusters.Items {
			c := &clusters.Items[i]
			items = append(items, map[string]any{
				"name": c.Name, "namespace": c.Namespace,
				"phase": string(c.Status.Phase),
			})
		}
		crdData["clusters"] = items
	}

	var shards multigresv1alpha1.ShardList
	if err := o.client.List(ctx, &shards, o.listOpts()...); err == nil {
		items := make([]map[string]any, 0, len(shards.Items))
		for i := range shards.Items {
			s := &shards.Items[i]
			items = append(items, map[string]any{
				"name": s.Name, "namespace": s.Namespace,
				"phase":         string(s.Status.Phase),
				"readyReplicas": s.Status.ReadyReplicas,
				"podRoles":      s.Status.PodRoles,
			})
		}
		crdData["shards"] = items
	}

	var cells multigresv1alpha1.CellList
	if err := o.client.List(ctx, &cells, o.listOpts()...); err == nil {
		items := make([]map[string]any, 0, len(cells.Items))
		for i := range cells.Items {
			c := &cells.Items[i]
			items = append(items, map[string]any{
				"name": c.Name, "namespace": c.Namespace,
				"phase": string(c.Status.Phase),
			})
		}
		crdData["cells"] = items
	}

	o.probes.Set("crdStatus", crdData)
}

func (o *Observer) checkClusterStatus(ctx context.Context) {
	var clusters multigresv1alpha1.MultigresClusterList
	if err := o.client.List(ctx, &clusters, o.listOpts()...); err != nil {
		return
	}

	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		comp := fmt.Sprintf("cluster/%s/%s", cluster.Namespace, cluster.Name)
		o.checkPhase(comp, cluster.Status.Phase, cluster.DeletionTimestamp != nil)
		o.checkGenerationStaleness(comp, cluster.Generation, cluster.Status.ObservedGeneration)
		o.checkStatusMessage(comp, cluster.Status.Phase, cluster.Status.Message, cluster.DeletionTimestamp != nil)
	}
}

func (o *Observer) checkShardStatus(ctx context.Context) {
	var shards multigresv1alpha1.ShardList
	if err := o.client.List(ctx, &shards, o.listOpts()...); err != nil {
		return
	}

	for i := range shards.Items {
		shard := &shards.Items[i]
		comp := fmt.Sprintf("shard/%s/%s", shard.Namespace, shard.Name)

		o.checkPhase(comp, shard.Status.Phase, shard.DeletionTimestamp != nil)
		o.checkGenerationStaleness(comp, shard.Generation, shard.Status.ObservedGeneration)
		o.checkStatusMessage(comp, shard.Status.Phase, shard.Status.Message, shard.DeletionTimestamp != nil)
		o.checkShardPodRoles(ctx, shard)
		o.checkShardReadiness(ctx, shard)
		o.checkShardCellsList(shard)
		o.checkShardConditions(shard, comp)
		o.checkBackupStaleness(shard, comp)
	}
}

func (o *Observer) checkCellStatus(ctx context.Context) {
	var cells multigresv1alpha1.CellList
	if err := o.client.List(ctx, &cells, o.listOpts()...); err != nil {
		return
	}

	for i := range cells.Items {
		cell := &cells.Items[i]
		comp := fmt.Sprintf("cell/%s/%s", cell.Namespace, cell.Name)
		o.checkPhase(comp, cell.Status.Phase, cell.DeletionTimestamp != nil)
		o.checkGenerationStaleness(comp, cell.Generation, cell.Status.ObservedGeneration)
		o.checkStatusMessage(comp, cell.Status.Phase, cell.Status.Message, cell.DeletionTimestamp != nil)
		o.checkCellStatusFields(ctx, cell)
	}
}

func (o *Observer) checkTopoServerStatus(ctx context.Context) {
	var topos multigresv1alpha1.TopoServerList
	if err := o.client.List(ctx, &topos, o.listOpts()...); err != nil {
		return
	}

	for i := range topos.Items {
		ts := &topos.Items[i]
		comp := fmt.Sprintf("toposerver/%s/%s", ts.Namespace, ts.Name)
		o.checkPhase(comp, ts.Status.Phase, ts.DeletionTimestamp != nil)
		o.checkGenerationStaleness(comp, ts.Generation, ts.Status.ObservedGeneration)
	}
}

func (o *Observer) checkPhase(component string, phase multigresv1alpha1.Phase, deleting bool) {
	if deleting {
		delete(o.prevPhase, component)
		delete(o.progressingSince, component)
		return
	}

	now := time.Now()
	key := component

	// Detect invalid phase transitions.
	if prev, ok := o.prevPhase[component]; ok && prev != string(phase) {
		if prev == string(multigresv1alpha1.PhaseHealthy) && phase == multigresv1alpha1.PhaseInitializing {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityFatal,
				Check:     "crd-status",
				Component: component,
				Message: fmt.Sprintf(
					"%s invalid phase transition: %s → %s",
					component, prev, phase,
				),
				Details: map[string]any{
					"previousPhase": prev,
					"currentPhase":  string(phase),
				},
			})
		}
	}
	o.prevPhase[component] = string(phase)

	switch phase {
	case multigresv1alpha1.PhaseHealthy:
		delete(o.generationDivergeSince, key+"/phase")
		delete(o.progressingSince, component)
		return
	case multigresv1alpha1.PhaseDegraded, multigresv1alpha1.PhaseUnknown:
		delete(o.progressingSince, component)
		since, tracked := o.generationDivergeSince[key+"/phase"]
		if !tracked {
			o.generationDivergeSince[key+"/phase"] = now
			return
		}
		if now.Sub(since) > common.PhaseDegradedTimeout {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityError,
				Check:     "crd-status",
				Component: component,
				Message: fmt.Sprintf(
					"%s in %s phase for %s",
					component,
					phase,
					now.Sub(since).Round(time.Second),
				),
				Details: map[string]any{
					"phase":    string(phase),
					"duration": now.Sub(since).String(),
				},
			})
		}
	case multigresv1alpha1.PhaseProgressing:
		since, tracked := o.progressingSince[component]
		if !tracked {
			o.progressingSince[component] = now
			return
		}
		if now.Sub(since) > common.PhaseProgressingTimeout {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "crd-status",
				Component: component,
				Message: fmt.Sprintf(
					"%s stuck in Progressing phase for %s",
					component,
					now.Sub(since).Round(time.Second),
				),
				Details: map[string]any{
					"phase":    string(phase),
					"duration": now.Sub(since).String(),
				},
			})
		}
	case multigresv1alpha1.PhaseInitializing:
		// Expected transitional phase during initial creation.
	}
}

func (o *Observer) checkGenerationStaleness(component string, generation, observed int64) {
	now := time.Now()
	key := component + "/gen"

	if observed == generation {
		delete(o.generationDivergeSince, key)
		return
	}

	since, tracked := o.generationDivergeSince[key]
	if !tracked {
		o.generationDivergeSince[key] = now
		return
	}

	if now.Sub(since) > common.GenerationDivergeTimeout {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "crd-status",
			Component: component,
			Message: fmt.Sprintf(
				"%s observedGeneration (%d) behind metadata.generation (%d) for %s",
				component,
				observed,
				generation,
				now.Sub(since).Round(time.Second),
			),
			Details: map[string]any{
				"observedGeneration": observed,
				"generation":         generation,
			},
		})
	}
}

func (o *Observer) checkShardPodRoles(ctx context.Context, shard *multigresv1alpha1.Shard) {
	if shard.DeletionTimestamp != nil {
		return
	}
	comp := fmt.Sprintf("shard/%s/%s", shard.Namespace, shard.Name)
	now := time.Now()

	// Pod labels use the short shard name from the label, not the CRD name.
	shardLabelValue := shard.Labels[common.LabelMultigresShard]

	// Check for stale podRoles entries (pods that no longer exist).
	if shard.Status.PodRoles != nil {
		var existingPods map[string]bool
		var pods corev1.PodList
		if err := o.client.List(ctx, &pods,
			o.listOpts(client.MatchingLabels{common.LabelMultigresShard: shardLabelValue})...,
		); err == nil {
			existingPods = make(map[string]bool, len(pods.Items))
			for i := range pods.Items {
				existingPods[pods.Items[i].Name] = true
			}

			for podName := range shard.Status.PodRoles {
				if !existingPods[podName] {
					staleKey := comp + "/stale-role/" + podName
					since, tracked := o.generationDivergeSince[staleKey]
					if !tracked {
						o.generationDivergeSince[staleKey] = now
						continue
					}
					if now.Sub(since) > common.StaleStatusEntryGracePeriod {
						o.reporter.Report(report.Finding{
							Severity:  report.SeverityWarn,
							Check:     "crd-status",
							Component: comp,
							Message: fmt.Sprintf(
								"Stale podRoles entry for non-existent pod %s",
								podName,
							),
							Details: map[string]any{
								"pod":  podName,
								"role": shard.Status.PodRoles[podName],
							},
						})
					}
				}
			}
		}
	}

	// Check primary count per pool per cell.
	// We need actual pod labels to correlate podRoles entries with pool+cell.
	var poolPods corev1.PodList
	if err := o.client.List(ctx, &poolPods,
		o.listOpts(client.MatchingLabels{
			common.LabelMultigresShard: shardLabelValue,
			common.LabelAppComponent:   common.ComponentPool,
		})...,
	); err != nil {
		return
	}

	// Build a lookup from pod name to its pool+cell labels.
	podLabels := make(map[string]podPoolCell, len(poolPods.Items))
	for i := range poolPods.Items {
		p := &poolPods.Items[i]
		podLabels[p.Name] = podPoolCell{
			pool: p.Labels[common.LabelMultigresPool],
			cell: p.Labels[common.LabelMultigresCell],
		}
	}

	multiPool := len(shard.Spec.Pools) > 1
	if multiPool {
		// In a multi-pool shard all pods replicate from the same primary,
		// so there must be exactly 1 primary across the entire shard.
		violationKey := comp + "/shard-primary"
		primaryCount := countPrimariesInShard(shard.Status.PodRoles)
		if primaryCount != 1 {
			since, tracked := o.primaryViolationSince[violationKey]
			if !tracked {
				o.primaryViolationSince[violationKey] = now
			} else if now.Sub(since) > common.PrimaryGracePeriod {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityError,
					Check:     "crd-status",
					Component: comp,
					Message: fmt.Sprintf(
						"Shard has %d primaries across all pools (expected 1)",
						primaryCount,
					),
					Details: map[string]any{
						"primaryCount": primaryCount,
						"multiPool":    true,
					},
				})
			}
		} else {
			delete(o.primaryViolationSince, violationKey)
		}
	} else {
		for poolName, poolSpec := range shard.Spec.Pools {
			if len(poolSpec.Cells) > 1 {
				// Multi-cell: expect exactly 1 primary across ALL cells for this pool.
				violationKey := fmt.Sprintf("%s/%s-global", comp, poolName)
				primaryCount := countPrimariesForPool(
					shard.Status.PodRoles,
					podLabels,
					string(poolName),
				)
				if primaryCount != 1 {
					since, tracked := o.primaryViolationSince[violationKey]
					if !tracked {
						o.primaryViolationSince[violationKey] = now
						continue
					}
					if now.Sub(since) > common.PrimaryGracePeriod {
						o.reporter.Report(report.Finding{
							Severity:  report.SeverityError,
							Check:     "crd-status",
							Component: comp,
							Message: fmt.Sprintf(
								"Pool %s has %d primaries across all cells (expected 1)",
								poolName,
								primaryCount,
							),
							Details: map[string]any{
								"pool":         string(poolName),
								"primaryCount": primaryCount,
								"multiCell":    true,
							},
						})
					}
				} else {
					delete(o.primaryViolationSince, violationKey)
				}
			} else {
				// Single-cell: expect exactly 1 primary per cell.
				for _, cellName := range poolSpec.Cells {
					violationKey := fmt.Sprintf("%s/%s-%s", comp, poolName, cellName)
					primaryCount := countPrimariesForPoolCell(
						shard.Status.PodRoles,
						podLabels,
						string(poolName),
						string(cellName),
					)

					if primaryCount != 1 {
						since, tracked := o.primaryViolationSince[violationKey]
						if !tracked {
							o.primaryViolationSince[violationKey] = now
							continue
						}
						if now.Sub(since) > common.PrimaryGracePeriod {
							o.reporter.Report(report.Finding{
								Severity:  report.SeverityError,
								Check:     "crd-status",
								Component: comp,
								Message: fmt.Sprintf(
									"Pool %s cell %s has %d primaries (expected 1)",
									poolName,
									cellName,
									primaryCount,
								),
								Details: map[string]any{
									"pool":         string(poolName),
									"cell":         string(cellName),
									"primaryCount": primaryCount,
								},
							})
						}
					} else {
						delete(o.primaryViolationSince, violationKey)
					}
				}
			}
		}
	}
}

// podPoolCell holds the pool and cell labels for a pod.
type podPoolCell struct {
	pool string
	cell string
}

// countPrimariesInShard counts pods with a "primary" role across the entire
// shard, regardless of pool. Used for multi-pool shards where all pools
// share a single primary.
func countPrimariesInShard(podRoles map[string]string) int {
	count := 0
	for _, role := range podRoles {
		if role == "primary" || role == "PRIMARY" {
			count++
		}
	}
	return count
}

// countPrimariesForPool counts pods with a "primary" role across all cells
// for the specified pool. Used for multi-cell durability policies where
// exactly one primary exists across all cells.
func countPrimariesForPool(
	podRoles map[string]string,
	podLabels map[string]podPoolCell,
	poolName string,
) int {
	count := 0
	for podName, role := range podRoles {
		if role != "primary" && role != "PRIMARY" {
			continue
		}
		info, ok := podLabels[podName]
		if !ok {
			continue
		}
		if info.pool == poolName {
			count++
		}
	}
	return count
}

// countPrimariesForPoolCell counts pods with a "primary" role that belong to
// the specified pool and cell, using pod labels for accurate filtering.
func countPrimariesForPoolCell(
	podRoles map[string]string,
	podLabels map[string]podPoolCell,
	poolName, cellName string,
) int {
	count := 0
	for podName, role := range podRoles {
		if role != "primary" && role != "PRIMARY" {
			continue
		}
		info, ok := podLabels[podName]
		if !ok {
			continue
		}
		if info.pool == poolName && info.cell == cellName {
			count++
		}
	}
	return count
}

func (o *Observer) checkShardReadiness(ctx context.Context, shard *multigresv1alpha1.Shard) {
	if shard.DeletionTimestamp != nil {
		return
	}
	comp := fmt.Sprintf("shard/%s/%s", shard.Namespace, shard.Name)
	shardLabelValue := shard.Labels[common.LabelMultigresShard]

	var pods corev1.PodList
	if err := o.client.List(ctx, &pods,
		o.listOpts(client.MatchingLabels{
			common.LabelMultigresShard: shardLabelValue,
			common.LabelAppComponent:   common.ComponentPool,
		})...,
	); err != nil {
		return
	}

	actualReady := int32(0)
	for i := range pods.Items {
		for _, cs := range pods.Items[i].Status.ContainerStatuses {
			if cs.Ready {
				actualReady++
				break
			}
		}
	}

	if shard.Status.ReadyReplicas != actualReady {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "crd-status",
			Component: comp,
			Message: fmt.Sprintf(
				"status.readyReplicas (%d) != actual ready pods (%d)",
				shard.Status.ReadyReplicas,
				actualReady,
			),
		})
	}
}

func (o *Observer) checkShardCellsList(shard *multigresv1alpha1.Shard) {
	if shard.DeletionTimestamp != nil {
		return
	}
	comp := fmt.Sprintf("shard/%s/%s", shard.Namespace, shard.Name)

	// Collect expected cells from pool specs.
	expectedCells := make(map[string]bool)
	for _, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			expectedCells[string(cell)] = true
		}
	}

	statusCells := make(map[string]bool)
	for _, cell := range shard.Status.Cells {
		statusCells[string(cell)] = true
	}

	for cell := range expectedCells {
		if !statusCells[cell] {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "crd-status",
				Component: comp,
				Message:   fmt.Sprintf("Cell %s expected in status.cells but not found", cell),
			})
		}
	}

	for cell := range statusCells {
		if !expectedCells[cell] {
			o.reporter.Report(report.Finding{
				Severity:  report.SeverityWarn,
				Check:     "crd-status",
				Component: comp,
				Message:   fmt.Sprintf("Cell %s in status.cells but not in any pool spec", cell),
			})
		}
	}
}

func (o *Observer) checkShardConditions(shard *multigresv1alpha1.Shard, comp string) {
	for _, cond := range shard.Status.Conditions {
		switch cond.Type {
		case multigresv1alpha1.ConditionReadyForDeletion:
			if shard.DeletionTimestamp == nil && cond.Status == "True" {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityWarn,
					Check:     "crd-status",
					Component: comp,
					Message: fmt.Sprintf(
						"Shard %s has ReadyForDeletion=True but is not being deleted",
						shard.Name,
					),
				})
			}
		case "BackupHealthy":
			if cond.Status == "False" {
				sev := report.SeverityWarn
				if cond.Reason == "BackupFailed" {
					sev = report.SeverityError
				}
				o.reporter.Report(report.Finding{
					Severity:  sev,
					Check:     "crd-status",
					Component: comp,
					Message: fmt.Sprintf(
						"Shard %s backup unhealthy (%s): %s",
						shard.Name,
						cond.Reason,
						cond.Message,
					),
					Details: map[string]any{"reason": cond.Reason},
				})
			}
		}
	}
}

func (o *Observer) checkStatusMessage(
	comp string,
	phase multigresv1alpha1.Phase,
	message string,
	deleting bool,
) {
	if deleting {
		return
	}
	if (phase == multigresv1alpha1.PhaseDegraded || phase == multigresv1alpha1.PhaseUnknown) && message == "" {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "crd-status",
			Component: comp,
			Message: fmt.Sprintf(
				"%s in %s phase with empty status message",
				comp, phase,
			),
			Details: map[string]any{"phase": string(phase)},
		})
	}
}

func (o *Observer) checkBackupStaleness(shard *multigresv1alpha1.Shard, comp string) {
	if shard.DeletionTimestamp != nil {
		return
	}

	// Only check if backup is configured (has a BackupHealthy condition).
	hasBackupCondition := false
	for _, cond := range shard.Status.Conditions {
		if cond.Type == "BackupHealthy" {
			hasBackupCondition = true
			break
		}
	}
	if !hasBackupCondition {
		return
	}

	if shard.Status.LastBackupTime == nil {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "crd-status",
			Component: comp,
			Message:   fmt.Sprintf("Shard %s has backup configured but no backup has completed", shard.Name),
		})
		return
	}

	age := time.Since(shard.Status.LastBackupTime.Time)
	if age > common.BackupStalenessErrorAge {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "crd-status",
			Component: comp,
			Message: fmt.Sprintf(
				"Shard %s last backup was %s ago (threshold %s)",
				shard.Name,
				age.Round(time.Minute),
				common.BackupStalenessErrorAge,
			),
			Details: map[string]any{
				"lastBackupTime": shard.Status.LastBackupTime.Time.String(),
				"age":            age.String(),
			},
		})
	} else if age > common.BackupStalenessWarnAge {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "crd-status",
			Component: comp,
			Message: fmt.Sprintf(
				"Shard %s last backup was %s ago (threshold %s)",
				shard.Name,
				age.Round(time.Minute),
				common.BackupStalenessWarnAge,
			),
			Details: map[string]any{
				"lastBackupTime": shard.Status.LastBackupTime.Time.String(),
				"age":            age.String(),
			},
		})
	}

	if shard.Status.LastBackupType != "" && !slices.Contains(common.ValidBackupTypes, shard.Status.LastBackupType) {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "crd-status",
			Component: comp,
			Message: fmt.Sprintf(
				"Shard %s has unknown backup type %q",
				shard.Name,
				shard.Status.LastBackupType,
			),
			Details: map[string]any{
				"lastBackupType": shard.Status.LastBackupType,
				"validTypes":     common.ValidBackupTypes,
			},
		})
	}
}

func (o *Observer) checkCellStatusFields(ctx context.Context, cell *multigresv1alpha1.Cell) {
	if cell.DeletionTimestamp != nil {
		return
	}
	comp := fmt.Sprintf("cell/%s/%s", cell.Namespace, cell.Name)

	if cell.Status.GatewayReadyReplicas > cell.Status.GatewayReplicas {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityError,
			Check:     "crd-status",
			Component: comp,
			Message: fmt.Sprintf(
				"Cell %s gatewayReadyReplicas (%d) > gatewayReplicas (%d)",
				cell.Name,
				cell.Status.GatewayReadyReplicas,
				cell.Status.GatewayReplicas,
			),
			Details: map[string]any{
				"gatewayReplicas":      cell.Status.GatewayReplicas,
				"gatewayReadyReplicas": cell.Status.GatewayReadyReplicas,
			},
		})
	}

	// Cross-check with actual gateway Deployment readiness.
	cellLabel := cell.Labels[common.LabelMultigresCell]
	if cellLabel != "" {
		var deploys appsv1.DeploymentList
		if err := o.client.List(ctx, &deploys,
			o.listOpts(client.MatchingLabels{
				common.LabelAppManagedBy: common.ManagedByMultigres,
				common.LabelAppComponent: common.ComponentMultigateway,
				common.LabelMultigresCell: cellLabel,
			})...,
		); err == nil {
			for j := range deploys.Items {
				d := &deploys.Items[j]
				if d.Status.ReadyReplicas != cell.Status.GatewayReadyReplicas {
					o.reporter.Report(report.Finding{
						Severity:  report.SeverityWarn,
						Check:     "crd-status",
						Component: comp,
						Message: fmt.Sprintf(
							"Cell %s gatewayReadyReplicas (%d) != deployment %s readyReplicas (%d)",
							cell.Name,
							cell.Status.GatewayReadyReplicas,
							d.Name,
							d.Status.ReadyReplicas,
						),
						Details: map[string]any{
							"deployment":              d.Name,
							"deploymentReadyReplicas": d.Status.ReadyReplicas,
							"cellReadyReplicas":       cell.Status.GatewayReadyReplicas,
						},
					})
				}
			}
		}
	}

	if cell.Status.Phase == multigresv1alpha1.PhaseHealthy && cell.Status.GatewayServiceName == "" {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "crd-status",
			Component: comp,
			Message:   fmt.Sprintf("Cell %s is Healthy but gatewayServiceName is empty", cell.Name),
		})
	}
}
