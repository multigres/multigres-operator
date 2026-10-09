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
)

func TestReconcilePoolerReadiness(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Pod scheme: %v", err)
	}
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Shard scheme: %v", err)
	}

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

	if err := r.reconcilePoolerReadiness(t.Context(), shard, map[string]posture.Readiness{
		pod.Name: {Observed: true, Ready: true, Reason: "DataPlaneReady", Message: "ready"},
	}); err != nil {
		t.Fatalf("reconcile readiness: %v", err)
	}

	updated := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated); err != nil {
		t.Fatalf("get updated pod: %v", err)
	}
	condition := readinessCondition(updated.Status.Conditions)
	if condition == nil ||
		condition.Status != corev1.ConditionTrue ||
		condition.Reason != "DataPlaneReady" {
		t.Fatalf("readiness condition = %#v, want true DataPlaneReady", condition)
	}

	if err := r.reconcilePoolerReadiness(t.Context(), shard, nil); err != nil {
		t.Fatalf("reconcile missing observation: %v", err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated); err != nil {
		t.Fatalf("get unready pod: %v", err)
	}
	condition = readinessCondition(updated.Status.Conditions)
	if condition == nil ||
		condition.Status != corev1.ConditionUnknown ||
		condition.Reason != "ObservationUnavailable" {
		t.Fatalf("readiness condition = %#v, want unknown ObservationUnavailable", condition)
	}

	// A real negative assessment from Multigres is a different claim from a
	// missing one, and must still report False rather than Unknown.
	if err := r.reconcilePoolerReadiness(t.Context(), shard, map[string]posture.Readiness{
		pod.Name: {
			Observed: true,
			Ready:    false,
			Reason:   "PostgresNotReady",
			Message:  "PostgreSQL is not accepting connections",
		},
	}); err != nil {
		t.Fatalf("reconcile negative observation: %v", err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated); err != nil {
		t.Fatalf("get not-ready pod: %v", err)
	}
	condition = readinessCondition(updated.Status.Conditions)
	if condition == nil ||
		condition.Status != corev1.ConditionFalse ||
		condition.Reason != "PostgresNotReady" {
		t.Fatalf("readiness condition = %#v, want false PostgresNotReady", condition)
	}

	// A map entry that exists but was never actually checked (e.g. its Status
	// RPC itself failed) must still read as Unknown, but keeps its own
	// Reason/Message rather than a generic placeholder: that diagnostic (here,
	// the RPC error) is the whole reason this entry is unobserved, and is lost
	// if overwritten.
	if err := r.reconcilePoolerReadiness(t.Context(), shard, map[string]posture.Readiness{
		pod.Name: {
			Observed: false,
			Ready:    false,
			Reason:   "StatusUnavailable",
			Message:  "multipooler status RPC failed: fake rpc failure",
		},
	}); err != nil {
		t.Fatalf("reconcile unobserved entry: %v", err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated); err != nil {
		t.Fatalf("get pod with unobserved entry: %v", err)
	}
	condition = readinessCondition(updated.Status.Conditions)
	if condition == nil ||
		condition.Status != corev1.ConditionUnknown ||
		condition.Reason != "StatusUnavailable" ||
		condition.Message != "multipooler status RPC failed: fake rpc failure" {
		t.Fatalf(
			"readiness condition = %#v, want unknown StatusUnavailable with the RPC error",
			condition,
		)
	}
}

func readinessCondition(conditions []corev1.PodCondition) *corev1.PodCondition {
	for i := range conditions {
		if conditions[i].Type == PoolerDataReadyCondition {
			return &conditions[i]
		}
	}
	return nil
}
