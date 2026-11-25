package shard

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// resolvePodRole returns the role (e.g. "PRIMARY", "REPLICA", "QUARANTINED") for
// a pod by checking shard.Status.PodRoles. It checks both the exact pod name and
// FQDN prefix (podName.subdomain...) since the data-handler may store either.
func resolvePodRole(shard *multigresv1alpha1.Shard, podName string) string {
	if shard.Status.PodRoles == nil {
		return ""
	}
	if role, ok := shard.Status.PodRoles[podName]; ok {
		return role
	}
	for k, v := range shard.Status.PodRoles {
		if strings.HasPrefix(k, podName+".") {
			return v
		}
	}
	return ""
}

// clearDrainAnnotations removes all drain annotations from a pod via merge patch,
// cancelling a drain that is no longer needed (e.g. scale-down reversed).
func clearDrainAnnotations(ctx context.Context, k8sClient client.Client, pod *corev1.Pod) error {
	patch := client.MergeFrom(pod.DeepCopy())
	delete(pod.Annotations, metadata.AnnotationDrainState)
	delete(pod.Annotations, metadata.AnnotationDrainRequestedAt)
	delete(pod.Annotations, metadata.AnnotationDrainReason)
	if err := k8sClient.Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("failed to clear drain annotations for pod %s: %w", pod.Name, err)
	}
	return nil
}

// initiateDrain sets the drain-requested annotation on a pod via merge patch.
func (r *ShardReconciler) initiateDrain(ctx context.Context, pod *corev1.Pod) error {
	return r.requestDrain(ctx, pod, client.MergeFrom(pod.DeepCopy()), "")
}

// initiateJudgedDrain requests the drain of pod exactly as the disruption
// walk read it. The patch carries that read's resourceVersion, so it is
// refused if the pod has changed in any way since: replaced under the same
// name, scheduled, or turned Ready or not Ready. The walk allowed the removal
// of the pod it observed, and a pod that has changed is the next pass's to
// judge. reason is recorded when the pod's own state will not show why it is
// being drained.
func (r *ShardReconciler) initiateJudgedDrain(
	ctx context.Context,
	pod *corev1.Pod,
	reason string,
) error {
	patch := client.MergeFromWithOptions(pod.DeepCopy(), client.MergeFromWithOptimisticLock{})
	return r.requestDrain(ctx, pod, patch, reason)
}

func (r *ShardReconciler) requestDrain(
	ctx context.Context,
	pod *corev1.Pod,
	patch client.Patch,
	reason string,
) error {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateRequested
	pod.Annotations[metadata.AnnotationDrainRequestedAt] = time.Now().UTC().Format(time.RFC3339)
	// A reason left on the pod by an earlier drain is not this drain's.
	delete(pod.Annotations, metadata.AnnotationDrainReason)
	if reason != "" {
		pod.Annotations[metadata.AnnotationDrainReason] = reason
	}
	if err := r.Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("failed to initiate drain for pod %s: %w", pod.Name, err)
	}
	return nil
}
