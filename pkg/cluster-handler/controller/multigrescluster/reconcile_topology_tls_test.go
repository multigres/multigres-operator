package multigrescluster

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
	r.markTopologyConnectFailed(context.Background(), cluster, cause, testLogger{})

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
