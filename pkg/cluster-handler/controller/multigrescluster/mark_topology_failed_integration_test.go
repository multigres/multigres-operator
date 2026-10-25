//go:build integration
// +build integration

package multigrescluster

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/multigres/testkit/managedfields"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// updateStatus removes a condition by leaving it out of its apply. A
// markTopologyFailed working from a snapshot taken before that removal still
// carries the condition. Whatever it writes, the next status apply must leave
// the condition gone: it may reassert the stale condition only under an entry
// that apply prunes, never under an Update-operation entry, which no apply
// ever prunes.
func TestMarkTopologyFailed_DoesNotResurrectRemovedCondition(t *testing.T) {
	t.Parallel()

	scheme := setupScheme()
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths(
		filepath.Join("..", "..", "..", "..", "config", "crd", "bases"),
	))
	c := testutil.SetUpClient(t, cfg, scheme)
	ctx := t.Context()
	r := &MultigresClusterReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "resurrect", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: "multigres-admin-password", Key: "password",
			},
		},
	}
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("create: %v", err)
	}
	key := client.ObjectKeyFromObject(cluster)
	const removed = multigresv1alpha1.ConditionAdminWebExternalReady
	cond := func(typ string) metav1.Condition {
		return metav1.Condition{
			Type: typ, Status: metav1.ConditionTrue, Reason: "Test",
			LastTransitionTime: metav1.Now(),
		}
	}
	apply := func(conds ...metav1.Condition) {
		t.Helper()
		cur := &multigresv1alpha1.MultigresCluster{}
		if err := c.Get(ctx, key, cur); err != nil {
			t.Fatalf("get: %v", err)
		}
		cur.Status.Phase = multigresv1alpha1.PhaseHealthy
		cur.Status.Conditions = conds
		if err := ownership.ApplyStatus(
			ctx, c, statusApplyObject(cur), ownership.ManagerCluster,
		); err != nil {
			t.Fatalf("status apply: %v", err)
		}
	}

	apply(cond("Available"), cond(removed))
	stale := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(ctx, key, stale); err != nil {
		t.Fatalf("get: %v", err)
	}
	apply(cond("Available"))

	r.markTopologyFailed(ctx, stale, "TopoConnectFailed",
		errors.New("topology unreachable"), testLogger{})

	got := &multigresv1alpha1.MultigresCluster{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if !meta.IsStatusConditionFalse(got.Status.Conditions, conditionTopologyReady) {
		t.Fatalf("TopologyReady=False never reached the status: %+v", got.Status.Conditions)
	}
	for _, e := range got.ManagedFields {
		if e.Operation == metav1.ManagedFieldsOperationUpdate && e.Subresource == "status" {
			t.Errorf("status written by an Update-operation entry %s, which no apply prunes",
				e.Manager)
		}
	}

	// The next pass's status apply, computed afresh, omits the condition.
	apply(cond("Available"), cond(conditionTopologyReady))
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if meta.FindStatusCondition(got.Status.Conditions, removed) != nil {
		t.Fatalf("%s resurrected and never pruned; owners %q", removed,
			managedfields.Owners(got, ownership.ConditionPath(removed)))
	}
	if !slices.ContainsFunc(got.Status.Conditions, func(c metav1.Condition) bool {
		return c.Type == "Available"
	}) {
		t.Fatalf("a condition the apply sent was lost: %+v", got.Status.Conditions)
	}
}
