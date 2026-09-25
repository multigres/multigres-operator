package shard

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// reconcilePoolerReadiness projects Multigres's own data-plane assessment onto
// the Pod readiness gate consumed by Kubernetes and the shard PDB. Missing
// observations fail closed so a newly-created or unreachable pooler is not
// counted toward the disruption budget.
func (r *ShardReconciler) reconcilePoolerReadiness(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	observations map[string]posture.Readiness,
) error {
	labels := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
		metadata.LabelAppComponent:        PoolComponentName,
	}
	pods := &corev1.PodList{}
	if err := r.List(
		ctx,
		pods,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(labels),
	); err != nil {
		return fmt.Errorf("list pool pods for readiness reconciliation: %w", err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		observation, ok := observations[pod.Name]
		if !ok {
			observation = posture.Readiness{
				Reason:  "ObservationUnavailable",
				Message: "Multigres data-plane readiness has not been observed",
			}
		}

		conditionStatus := corev1.ConditionFalse
		if observation.Ready {
			conditionStatus = corev1.ConditionTrue
		}
		if poolerReadinessConditionMatches(
			pod.Status.Conditions,
			conditionStatus,
			observation.Reason,
			observation.Message,
		) {
			continue
		}

		base := pod.DeepCopy()
		setPoolerReadinessCondition(pod, corev1.PodCondition{
			Type:               PoolerDataReadyCondition,
			Status:             conditionStatus,
			LastProbeTime:      metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             observation.Reason,
			Message:            observation.Message,
		})
		// Named apart from the Shard's own status manager: the claim here is over
		// one condition on a Pod whose status otherwise belongs to kubelet, not
		// over the Shard's status, and a manager name is the only record of which
		// concern took a field. Same reasoning as the storage-class guard.
		if err := r.Status().Patch(
			ctx,
			pod,
			client.MergeFrom(base),
			client.FieldOwner("multigres-resource-handler-readiness"),
		); err != nil {
			return fmt.Errorf("patch pooler readiness for pod %s: %w", pod.Name, err)
		}
	}
	return nil
}

func poolerReadinessConditionMatches(
	conditions []corev1.PodCondition,
	conditionStatus corev1.ConditionStatus,
	reason string,
	message string,
) bool {
	for _, condition := range conditions {
		if condition.Type != PoolerDataReadyCondition {
			continue
		}
		return condition.Status == conditionStatus &&
			condition.Reason == reason &&
			condition.Message == message
	}
	return false
}

func setPoolerReadinessCondition(pod *corev1.Pod, desired corev1.PodCondition) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type != PoolerDataReadyCondition {
			continue
		}
		if pod.Status.Conditions[i].Status == desired.Status {
			desired.LastTransitionTime = pod.Status.Conditions[i].LastTransitionTime
		}
		pod.Status.Conditions[i] = desired
		return
	}
	pod.Status.Conditions = append(pod.Status.Conditions, desired)
}
