package shard

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
		existing := findPoolerReadinessCondition(pod.Status.Conditions)
		if conditionMatches(existing, conditionStatus, observation.Reason, observation.Message) {
			continue
		}

		desired := corev1.PodCondition{
			Type:               PoolerDataReadyCondition,
			Status:             conditionStatus,
			LastProbeTime:      metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             observation.Reason,
			Message:            observation.Message,
		}
		if existing != nil && existing.Status == desired.Status {
			desired.LastTransitionTime = existing.LastTransitionTime
		}

		encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&desired)
		if err != nil {
			return fmt.Errorf("encode pooler readiness condition for pod %s: %w", pod.Name, err)
		}

		// A server-side apply whose payload carries exactly one entry of a
		// listType=map/listMapKey=type list (PodStatus.Conditions is one, per
		// its OpenAPI markers) merges that entry into the list by its `type`
		// key instead of replacing the array, which is what a JSON merge
		// patch (client.MergeFrom) does. That is the whole fix: kubelet's own
		// entries (Ready, ContainersReady, ...), written after this
		// reconcile's List, survive untouched even though this apply carries
		// only the snapshot this reconcile read.
		//
		// FieldOwner names the manager for this one condition apart from the
		// Shard's own status manager, since the claim is over a field on a
		// Pod whose status otherwise belongs to kubelet. Same reasoning as
		// the storage-class guard.
		//
		// ForceOwnership is required, not optional, and must not be removed:
		// the binary this replaces wrote this same field under this same
		// manager name via client.MergeFrom, which the API server records as
		// an Update-operation managedFields entry. SSA treats (manager,
		// operation) as distinct owners, so the first Apply from this
		// manager after an upgrade conflicts with its own leftover Update
		// entry unless forced. Without ForceOwnership, every write that
		// changes reason, message or status fails from that point on, and
		// the readiness gate sticks at its pre-upgrade value.
		// No metadata.uid precondition: a stale observation could in principle
		// land on a Pod recreated under the same name after this reconcile's
		// List. Not a regression versus the prior MergeFrom patch, which had
		// the same gap, so left as-is here.
		apply := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": corev1.SchemeGroupVersion.String(),
			"kind":       "Pod",
			"metadata": map[string]any{
				"name":      pod.Name,
				"namespace": pod.Namespace,
			},
			"status": map[string]any{
				"conditions": []any{encoded},
			},
		}}
		if err := r.Status().Patch(
			ctx,
			apply,
			client.Apply,
			client.FieldOwner("multigres-resource-handler-readiness"),
			client.ForceOwnership,
		); err != nil {
			return fmt.Errorf("apply pooler readiness for pod %s: %w", pod.Name, err)
		}
	}
	return nil
}

func findPoolerReadinessCondition(conditions []corev1.PodCondition) *corev1.PodCondition {
	for i := range conditions {
		if conditions[i].Type == PoolerDataReadyCondition {
			return &conditions[i]
		}
	}
	return nil
}

func conditionMatches(
	existing *corev1.PodCondition,
	conditionStatus corev1.ConditionStatus,
	reason string,
	message string,
) bool {
	return existing != nil &&
		existing.Status == conditionStatus &&
		existing.Reason == reason &&
		existing.Message == message
}
