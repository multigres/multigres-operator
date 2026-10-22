package cell

import (
	"testing"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLiteCellReadinessFollowsItsPod(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(map[bool]string{true: "ready", false: "unready"}[ready], func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = multigresv1alpha1.AddToScheme(scheme)
			cell := &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "lite-a",
					Namespace:  "lite",
					Generation: 3,
					Labels:     map[string]string{metadata.LabelMultigresCluster: "lite"},
				},
				Spec: multigresv1alpha1.CellSpec{Name: "a", Mode: multigresv1alpha1.ModeLite},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pool",
					Namespace: "lite",
					Labels: map[string]string{
						metadata.LabelAppInstance:          "lite",
						metadata.LabelMultigresCell:        "a",
						metadata.LabelMultigresGatewayRole: metadata.GatewayRoleLite,
					},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			if ready {
				pod.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				}
			}
			other := pod.DeepCopy()
			other.Name = "other"
			other.Labels[metadata.LabelAppInstance] = "other"
			other.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}
			base := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cell, pod, other).
				WithStatusSubresource(cell).
				Build()
			cl := testutil.NewFakeClientWithFailures(base, nil)
			r := &CellReconciler{Client: cl, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
			if err := r.updateLiteStatus(t.Context(), cell); err != nil {
				t.Fatal(err)
			}
			got := &multigresv1alpha1.Cell{}
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(cell), got); err != nil {
				t.Fatal(err)
			}
			if got.Status.ObservedGeneration != 3 ||
				meta.IsStatusConditionTrue(got.Status.Conditions, "Available") != ready ||
				meta.IsStatusConditionTrue(got.Status.Conditions, "Ready") != ready {
				t.Fatalf("wrong Lite Cell status: %+v", got.Status)
			}
		})
	}
}
