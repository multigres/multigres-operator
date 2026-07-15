package observer

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/tools/observer/pkg/common"
	"github.com/multigres/multigres-operator/tools/observer/pkg/report"
)

// checkSpecCompliance verifies that running pods and PVCs match
// the fully resolved specs in Shard CRDs.
func (o *Observer) checkSpecCompliance(ctx context.Context) {
	var shards multigresv1alpha1.ShardList
	if err := o.client.List(ctx, &shards, o.listOpts()...); err != nil {
		o.logger.Error("spec-compliance: failed to list Shards", "error", err)
		return
	}

	probeData := make([]map[string]any, 0, len(shards.Items))
	for i := range shards.Items {
		shard := &shards.Items[i]

		shardLabels := client.MatchingLabels{
			common.LabelAppManagedBy:   common.ManagedByMultigres,
			common.LabelAppComponent:   common.ComponentPool,
			common.LabelMultigresCluster: shard.Labels[common.LabelMultigresCluster],
			common.LabelMultigresShard: shard.Labels[common.LabelMultigresShard],
		}

		var pods corev1.PodList
		if err := o.client.List(ctx, &pods, o.listOpts(shardLabels)...); err != nil {
			continue
		}

		var pvcs corev1.PersistentVolumeClaimList
		if err := o.client.List(ctx, &pvcs, o.listOpts(shardLabels)...); err != nil {
			continue
		}

		// Filter out pods in grace period.
		activePods := make([]corev1.Pod, 0, len(pods.Items))
		for j := range pods.Items {
			if !o.isPodInGracePeriod(pods.Items[j].Name) {
				activePods = append(activePods, pods.Items[j])
			}
		}

		o.checkPoolPodResources(shard, activePods)
		o.checkPoolPodTolerations(shard, activePods)
		o.checkPoolPodImages(shard, activePods)
		o.checkPVCSizes(shard, pvcs.Items)

		probeData = append(probeData, map[string]any{
			"shard":      shard.Name,
			"activePods": len(activePods),
			"pvcs":       len(pvcs.Items),
		})
	}

	if o.probes != nil {
		o.probes.Set("spec-compliance", map[string]any{"shards": probeData})
	}
}

// checkPoolPodResources compares pod container resources against the pool spec.
func (o *Observer) checkPoolPodResources(shard *multigresv1alpha1.Shard, pods []corev1.Pod) {
	for poolName, poolSpec := range shard.Spec.Pools {
		for i := range pods {
			pod := &pods[i]
			if pod.Labels[common.LabelMultigresPool] != string(poolName) {
				continue
			}

			for j := range pod.Spec.Containers {
				c := &pod.Spec.Containers[j]
				var expected corev1.ResourceRequirements
				switch c.Name {
				case "postgres":
					expected = poolSpec.Postgres.Resources
				case "multipooler":
					expected = poolSpec.Multipooler.Resources
				default:
					continue
				}

				comp := fmt.Sprintf("shard/%s/pool/%s/pod/%s/%s", shard.Name, poolName, pod.Name, c.Name)
				o.compareResources(comp, "requests.cpu", expected.Requests, c.Resources.Requests, corev1.ResourceCPU)
				o.compareResources(comp, "requests.memory", expected.Requests, c.Resources.Requests, corev1.ResourceMemory)
				o.compareResources(comp, "limits.cpu", expected.Limits, c.Resources.Limits, corev1.ResourceCPU)
				o.compareResources(comp, "limits.memory", expected.Limits, c.Resources.Limits, corev1.ResourceMemory)
			}
		}
	}
}

// compareResources reports a finding if the actual quantity differs from expected.
func (o *Observer) compareResources(
	component, field string,
	expected, actual corev1.ResourceList,
	name corev1.ResourceName,
) {
	exp, expOK := expected[name]
	act, actOK := actual[name]

	if !expOK {
		return // No spec expectation for this resource.
	}
	if !actOK {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "spec-compliance",
			Component: component,
			Message:   fmt.Sprintf("expected %s=%s but container has none", field, exp.String()),
		})
		return
	}
	if exp.Cmp(act) != 0 {
		o.reporter.Report(report.Finding{
			Severity:  report.SeverityWarn,
			Check:     "spec-compliance",
			Component: component,
			Message:   fmt.Sprintf("%s mismatch: spec=%s pod=%s", field, exp.String(), act.String()),
		})
	}
}

// checkPoolPodTolerations verifies expected tolerations exist on each pod (subset check).
func (o *Observer) checkPoolPodTolerations(shard *multigresv1alpha1.Shard, pods []corev1.Pod) {
	for poolName, poolSpec := range shard.Spec.Pools {
		if len(poolSpec.Tolerations) == 0 {
			continue
		}
		for i := range pods {
			pod := &pods[i]
			if pod.Labels[common.LabelMultigresPool] != string(poolName) {
				continue
			}
			for _, expected := range poolSpec.Tolerations {
				if !hasToleration(pod.Spec.Tolerations, expected) {
					o.reporter.Report(report.Finding{
						Severity:  report.SeverityWarn,
						Check:     "spec-compliance",
						Component: fmt.Sprintf("shard/%s/pool/%s/pod/%s", shard.Name, poolName, pod.Name),
						Message: fmt.Sprintf("missing toleration key=%s value=%s effect=%s",
							expected.Key, expected.Value, expected.Effect),
					})
				}
			}
		}
	}
}

// hasToleration checks if a pod's tolerations contain the expected one.
func hasToleration(podTolerations []corev1.Toleration, expected corev1.Toleration) bool {
	for _, t := range podTolerations {
		if t.Key == expected.Key &&
			t.Operator == expected.Operator &&
			t.Value == expected.Value &&
			t.Effect == expected.Effect {
			return true
		}
	}
	return false
}

// checkPoolPodImages verifies pod container images match the shard spec.
func (o *Observer) checkPoolPodImages(shard *multigresv1alpha1.Shard, pods []corev1.Pod) {
	for poolName := range shard.Spec.Pools {
		for i := range pods {
			pod := &pods[i]
			if pod.Labels[common.LabelMultigresPool] != string(poolName) {
				continue
			}
			for j := range pod.Spec.Containers {
				c := &pod.Spec.Containers[j]
				var expectedImage string
				switch c.Name {
				case "postgres":
					expectedImage = string(shard.Spec.Images.Postgres)
				case "multipooler":
					expectedImage = string(shard.Spec.Images.Multipooler)
				default:
					continue
				}
				if expectedImage != "" && c.Image != expectedImage {
					o.reporter.Report(report.Finding{
						Severity:  report.SeverityWarn,
						Check:     "spec-compliance",
						Component: fmt.Sprintf("shard/%s/pool/%s/pod/%s/%s", shard.Name, poolName, pod.Name, c.Name),
						Message:   fmt.Sprintf("image mismatch: spec=%s pod=%s", expectedImage, c.Image),
					})
				}
			}
		}
	}
}

// checkPVCSizes verifies PVC storage capacity is not smaller than the pool spec.
func (o *Observer) checkPVCSizes(shard *multigresv1alpha1.Shard, pvcs []corev1.PersistentVolumeClaim) {
	for poolName, poolSpec := range shard.Spec.Pools {
		if poolSpec.Storage.Size == "" {
			continue
		}
		specQty, err := resource.ParseQuantity(poolSpec.Storage.Size)
		if err != nil {
			o.logger.Debug("spec-compliance: cannot parse storage size",
				"shard", shard.Name, "pool", string(poolName), "size", poolSpec.Storage.Size, "error", err)
			continue
		}

		for i := range pvcs {
			pvc := &pvcs[i]
			if pvc.Labels[common.LabelMultigresPool] != string(poolName) {
				continue
			}
			pvcQty := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			if pvcQty.Cmp(specQty) < 0 {
				o.reporter.Report(report.Finding{
					Severity:  report.SeverityError,
					Check:     "spec-compliance",
					Component: fmt.Sprintf("shard/%s/pool/%s/pvc/%s", shard.Name, poolName, pvc.Name),
					Message: fmt.Sprintf("PVC storage %s is smaller than spec %s",
						pvcQty.String(), specQty.String()),
				})
			}
		}
	}
}
