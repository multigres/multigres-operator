package multigrescluster

import (
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// markTopologyFailed must land even when the in-memory cluster carries a stale
// resourceVersion, because the same pass may already have written status. An
// Update would conflict with that write and the failure would never reach the
// status, on the path that fires exactly when topology is unreachable. The fake
// client enforces optimistic concurrency on Update, so a write that carries the
// snapshot's resourceVersion fails this test.
func TestMarkTopologyFailed_SurvivesAStaleResourceVersion(t *testing.T) {
	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "stale", Namespace: "supabase", Generation: 1},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	key := client.ObjectKeyFromObject(cluster)

	// Something else advances the object, which in production is this same
	// controller writing status earlier in the pass.
	live := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(t.Context(), key, live); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	live.Status.Message = "written by someone else"
	if err := c.Status().Update(t.Context(), live); err != nil {
		t.Fatalf("priming the object failed: %v", err)
	}

	// cluster still carries the resourceVersion from before that write.
	r.markTopologyFailed(t.Context(), cluster, "TopoConnectFailed",
		errors.New("topology unreachable"), testLogger{})

	got := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(t.Context(), key, got); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Status.Phase != multigresv1alpha1.PhaseDegraded {
		t.Errorf("phase = %q, want %q: the topology failure did not reach the status",
			got.Status.Phase, multigresv1alpha1.PhaseDegraded)
	}
}
