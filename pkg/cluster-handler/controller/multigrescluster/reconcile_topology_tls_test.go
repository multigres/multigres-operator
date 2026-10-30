package multigrescluster

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// A topology connection failure has to be visible in the cluster status, not
// only in the logs, so an operator can see why the cluster is not becoming
// ready and which Secret is missing.
func TestMarkTopologyConnectFailed_SurfacesInStatus(t *testing.T) {
	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cluster",
			Namespace:  "supabase",
			Generation: 3,
		},
	}
	c := newTestClient(t, cluster)
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	cause := errors.New(
		`reading topology client TLS Secret "test-cluster-topo-client-tls" in namespace "supabase": not found`,
	)
	r.markTopologyFailed(context.Background(), cluster, "TopoConnectFailed", cause, testLogger{})

	got := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "test-cluster"},
		got,
	); err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.Status.Phase != multigresv1alpha1.PhaseDegraded {
		t.Errorf("phase = %q, want %q", got.Status.Phase, multigresv1alpha1.PhaseDegraded)
	}
	if !strings.Contains(got.Status.Message, "test-cluster-topo-client-tls") {
		t.Errorf("status message does not name the Secret: %q", got.Status.Message)
	}
	var found bool
	for _, cond := range got.Status.Conditions {
		if cond.Type == conditionTopologyReady {
			found = true
			if cond.Status != metav1.ConditionFalse {
				t.Errorf("%s status = %q, want False", conditionTopologyReady, cond.Status)
			}
			if !strings.Contains(cond.Message, "test-cluster-topo-client-tls") {
				t.Errorf("condition message does not name the Secret: %q", cond.Message)
			}
		}
	}
	if !found {
		t.Errorf("no %s condition set", conditionTopologyReady)
	}
}

type testLogger struct{}

func (testLogger) Error(error, string, ...any) {}

// markTopologyFailed must land even when the in-memory cluster carries a stale
// resourceVersion, because the same pass may already have written status. A
// bare Status().Update 409s against that write and the failure never reaches
// the status, on the path that fires exactly when topology is unreachable. A
// merge patch carries no resourceVersion. The fake client enforces optimistic
// concurrency on Update, so reverting to Update fails this test. The field
// owner is not visible to the fake client (it does not populate managedFields);
// TestMultigresCluster_WritesUnderOwnManager covers it against a real apiserver.
func TestMarkTopologyFailed_SurvivesAStaleResourceVersion(t *testing.T) {
	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stale", Namespace: "supabase", Generation: 1,
		},
	}
	c := newTestClient(t, cluster)
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	// Something else advances the object, which in production is this same
	// controller writing status earlier in the pass.
	live := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "stale"}, live); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	live.Status.Message = "written by someone else"
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("priming the object failed: %v", err)
	}

	// cluster still carries the resourceVersion from before that write.
	r.markTopologyFailed(
		context.Background(), cluster, "TopoConnectFailed",
		errors.New("topology unreachable"), testLogger{},
	)

	got := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "stale"}, got); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Status.Phase != multigresv1alpha1.PhaseDegraded {
		t.Errorf("phase = %q, want %q: the topology failure did not reach the "+
			"status, which is what a conflict against a stale resourceVersion "+
			"looks like from the outside", got.Status.Phase,
			multigresv1alpha1.PhaseDegraded)
	}
}
