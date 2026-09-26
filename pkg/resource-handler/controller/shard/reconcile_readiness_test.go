package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestReconcilePoolerReadiness(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	scheme := runtime.NewScheme()
	ck.NoError(corev1.AddToScheme(scheme), "add Pod scheme")
	ck.NoError(multigresv1alpha1.AddToScheme(scheme), "add Shard scheme")

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}
	labels := shardPDBLabels(shard)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pooler-0", Namespace: shard.Namespace, Labels: labels},
		Spec: corev1.PodSpec{ReadinessGates: []corev1.PodReadinessGate{
			{ConditionType: PoolerDataReadyCondition},
		}},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(pod).
		Build()
	r := &ShardReconciler{Client: c, Scheme: scheme}

	ck.NoError(r.reconcilePoolerReadiness(t.Context(), shard, map[string]posture.Readiness{
		pod.Name: {Ready: true, Reason: "DataPlaneReady", Message: "ready"},
	}), "reconcile readiness")

	updated := &corev1.Pod{}
	ck.NoError(c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated), "get updated pod")
	condition := readinessCondition(updated.Status.Conditions)
	ck.False(condition == nil ||
		condition.Status != corev1.ConditionTrue ||
		condition.Reason != "DataPlaneReady", "readiness condition = %#v, want true DataPlaneReady", condition)

	ck.NoError(r.reconcilePoolerReadiness(t.Context(), shard, nil), "reconcile missing observation")
	ck.NoError(c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated), "get unready pod")
	condition = readinessCondition(updated.Status.Conditions)
	ck.False(condition == nil ||
		condition.Status != corev1.ConditionFalse ||
		condition.Reason != "ObservationUnavailable", "readiness condition = %#v, want false ObservationUnavailable", condition)
}

func readinessCondition(conditions []corev1.PodCondition) *corev1.PodCondition {
	for i := range conditions {
		if conditions[i].Type == PoolerDataReadyCondition {
			return &conditions[i]
		}
	}
	return nil
}
