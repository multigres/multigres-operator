package multigrescluster

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

// A topology connection failure has to be visible in the cluster status, not
// only in the logs, so an operator can see why the cluster is not becoming
// ready and which Secret is missing.
func TestMarkTopologyConnectFailed_SurfacesInStatus(t *testing.T) {
	ck := assert.NewCollecting(t)
	scheme := setupScheme()
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cluster",
			Namespace:  "supabase",
			Generation: 3,
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
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
	ck.Require().NoError(c.Get(
		context.Background(),
		client.ObjectKey{Namespace: "supabase", Name: "test-cluster"},
		got,
	), "Get() error =")

	ck.Eq(multigresv1alpha1.PhaseDegraded, got.Status.Phase, "phase")
	ck.StrContains(
		got.Status.Message,
		"test-cluster-topo-client-tls",
		"status message does not name the Secret",
	)
	var found bool
	for _, cond := range got.Status.Conditions {
		if cond.Type == conditionTopologyReady {
			found = true
			ck.Eq(
				metav1.ConditionFalse,
				cond.Status,
				"%s status = %q, want False",
				conditionTopologyReady,
				cond.Status,
			)
			ck.StrContains(
				cond.Message,
				"test-cluster-topo-client-tls",
				"condition message does not name the Secret",
			)
		}
	}
	ck.True(found, "no %s condition set", conditionTopologyReady)
}

type testLogger struct{}

func (testLogger) Error(error, string, ...any) {}
