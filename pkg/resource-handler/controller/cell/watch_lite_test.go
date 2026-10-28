package cell

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func liteWatchPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "lite", Namespace: "ns", Labels: map[string]string{
			metadata.LabelAppInstance: "cluster", metadata.LabelMultigresCell: "zone1",
			metadata.LabelMultigresGatewayRole: metadata.GatewayRoleLite,
		}},
	}
}

func TestLitePodWatchMapping(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	var objects []client.Object
	for _, tc := range []struct {
		name, ns, cluster, cell string
		lite                    bool
	}{
		{"match", "ns", "cluster", "zone1", true},
		{"other-namespace", "other", "cluster", "zone1", true},
		{"other-cluster", "ns", "other", "zone1", true},
		{"other-cell", "ns", "cluster", "zone2", true},
		{"ha", "ns", "cluster", "zone1", false},
	} {
		cell := &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name: tc.name, Namespace: tc.ns,
				Labels: map[string]string{metadata.LabelMultigresCluster: tc.cluster},
			},
			Spec: multigresv1alpha1.CellSpec{Name: multigresv1alpha1.CellName(tc.cell)},
		}
		if tc.lite {
			cell.Spec.Mode = multigresv1alpha1.ModeLite
		}
		objects = append(objects, cell)
	}
	r := &CellReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
	}
	pod := liteWatchPod()
	got := r.cellsForLitePod(t.Context(), pod)
	if len(got) != 1 || got[0].Name != "match" || got[0].Namespace != "ns" {
		t.Fatalf("unexpected requests: %v", got)
	}
	delete(pod.Labels, metadata.LabelMultigresGatewayRole)
	if got := r.cellsForLitePod(t.Context(), pod); len(got) != 0 {
		t.Fatalf("unrelated pod queued Cells: %v", got)
	}
}

func TestLitePodWatchPredicate(t *testing.T) {
	p := litePodChangedPredicate()
	pod := liteWatchPod()
	if !p.Create(event.CreateEvent{Object: pod}) || !p.Delete(event.DeleteEvent{Object: pod}) {
		t.Fatal("Lite lifecycle events must enqueue")
	}
	if p.Create(event.CreateEvent{Object: &corev1.Pod{}}) ||
		p.Delete(event.DeleteEvent{Object: &corev1.Pod{}}) {
		t.Fatal("unrelated pods must not enqueue")
	}
	for _, tc := range []struct {
		name   string
		change func(*corev1.Pod)
		want   bool
	}{
		{"unchanged", func(*corev1.Pod) {}, false},
		{"resource version", func(p *corev1.Pod) { p.ResourceVersion = "2" }, false},
		{"unrelated label", func(p *corev1.Pod) { p.Labels["other"] = "value" }, false},
		{"ready", func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}, true},
		{"terminating", func(p *corev1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now }, true},
		{"crashloop", func(p *corev1.Pod) {
			p.Status.ContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
		}, true},
		{"init crashloop", func(p *corev1.Pod) {
			p.Status.InitContainerStatuses = []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
		}, true},
		{"cell changed", func(p *corev1.Pod) { p.Labels[metadata.LabelMultigresCell] = "zone2" }, true},
		{"cluster changed", func(p *corev1.Pod) { p.Labels[metadata.LabelAppInstance] = "other" }, true},
		{"role removed", func(p *corev1.Pod) { delete(p.Labels, metadata.LabelMultigresGatewayRole) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := pod.DeepCopy()
			tc.change(next)
			for _, e := range []event.UpdateEvent{{ObjectOld: pod, ObjectNew: next}, {ObjectOld: next, ObjectNew: pod}} {
				if got := p.Update(e); got != tc.want {
					t.Fatalf("predicate=%v want %v", got, tc.want)
				}
			}
		})
	}
}
