package cell

import (
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestCellEventsOnlyOnChanges(t *testing.T) {
	for _, mode := range []string{multigresv1alpha1.ModeLite, ""} {
		t.Run(string(mode), func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, multigresv1alpha1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			cell := &multigresv1alpha1.Cell{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "cell",
					Namespace:  "ns",
					Generation: 1,
					Labels:     map[string]string{metadata.LabelMultigresCluster: "cluster"},
				},
				Spec: multigresv1alpha1.CellSpec{Name: "zone1", Mode: mode},
			}
			pod := liteWatchPod()
			base := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cell, pod).
				WithStatusSubresource(cell, pod).
				Build()
			cl := testutil.NewFakeClientWithFailures(base, nil)
			recorder := record.NewFakeRecorder(100)
			r := &CellReconciler{Client: withCellOwner(cl), Scheme: scheme, Recorder: recorder}
			reconcile := func() []string {
				t.Helper()
				if _, err := r.Reconcile(
					t.Context(),
					ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cell)},
				); err != nil {
					t.Fatal(err)
				}
				var events []string
				for len(recorder.Events) > 0 {
					events = append(events, <-recorder.Events)
				}
				return events
			}
			has := func(events []string, reason string) bool {
				for _, e := range events {
					if strings.Contains(e, " "+reason+" ") {
						return true
					}
				}
				return false
			}
			first := reconcile()
			if !has(first, "Synced") || !has(first, "PhaseChange") {
				t.Fatalf("missing initial events: %v", first)
			}
			if events := reconcile(); len(events) != 0 {
				t.Fatalf("unchanged reconcile emitted events: %v", events)
			}
			if err := cl.Get(t.Context(), client.ObjectKeyFromObject(cell), cell); err != nil {
				t.Fatal(err)
			}
			cell.Generation++
			if err := cl.Update(t.Context(), cell); err != nil {
				t.Fatal(err)
			}
			if events := reconcile(); !has(events, "Synced") || has(events, "Applied") {
				t.Fatalf("unexpected spec change events: %v", events)
			}
			if mode == multigresv1alpha1.ModeLite {
				pod.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				}
				if err := cl.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				if events := reconcile(); !has(events, "PhaseChange") || has(events, "Synced") {
					t.Fatalf("unexpected readiness events: %v", events)
				}
				if events := reconcile(); len(events) != 0 {
					t.Fatalf("unchanged ready reconcile emitted events: %v", events)
				}
			}
		})
	}
}
