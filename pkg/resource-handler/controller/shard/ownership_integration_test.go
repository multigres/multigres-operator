//go:build integration
// +build integration

package shard_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/testkit/managedfields"
)

// operatorManagers returns the sorted manager names on obj that start with
// "multigres-", i.e. the operator's own. The creating test client and the
// apiserver appear under other names and are not the subject here.
func operatorManagers(obj client.Object) []string {
	return slices.DeleteFunc(managedfields.Managers(obj), func(m string) bool {
		return !strings.HasPrefix(m, "multigres-")
	})
}

// requireOnlyManagers asserts got names main and nothing but main and the
// facets, and no legacy name.
func requireOnlyManagers(t *testing.T, got []string, main string, facets ...string) {
	t.Helper()
	require.Contains(t, got, main)
	allowed := append([]string{main}, facets...)
	for _, m := range got {
		require.Contains(t, allowed, m, "unexpected operator field manager %q", m)
		require.False(t, ownership.IsLegacy(m), "legacy field manager %q present", m)
	}
}

// seedLegacyShard stores shard and the status fields the CRD requires through
// real server-side applies under the legacy manager name, so the object carries
// Apply entries for that name on the main resource and on the status
// subresource, as one last written by the pre-rename operator does. A fixture
// that hand-built managedFields would encode what the test expects of the
// apiserver. The controller must not be running yet, or its own reconcile races
// the seed.
func seedLegacyShard(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard, legacy string) {
	t.Helper()
	ctx := t.Context()
	main := shard.DeepCopy()
	main.Status = multigresv1alpha1.ShardStatus{}
	require.NoError(t, managedfields.Seed(ctx, c, main, legacy))
	status := shard.DeepCopy()
	status.Status = multigresv1alpha1.ShardStatus{ObservedGeneration: 1}
	require.NoError(t, managedfields.Seed(ctx, c, status, legacy, managedfields.OnStatus()))

	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(shard), shard))
	entries, err := managedfields.Entries(shard)
	require.NoError(t, err)
	for _, sub := range []string{"", "status"} {
		require.True(t, slices.ContainsFunc(entries, func(e managedfields.Entry) bool {
			return e.Manager == legacy && e.Subresource == sub
		}), "seed left no legacy entry on subresource %q", sub)
	}
}

// requireNoLegacyEventually waits for the controller's reconcile to retire
// every legacy entry on obj, which only DropLegacy does.
func requireNoLegacyEventually(t *testing.T, c client.Reader, obj client.Object) {
	t.Helper()
	require.Eventually(t, func() bool {
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj); err != nil {
			return false
		}
		return !slices.ContainsFunc(managedfields.Managers(obj), ownership.IsLegacy)
	}, 15*time.Second, 100*time.Millisecond, "legacy field manager entries never dropped")
}

func TestShard_WritesUnderOwnManager(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	raw := testutil.SetUpClient(t, cfg, scheme)
	mgr := testutil.SetUpManager(t, cfg, scheme)
	if err := (&shardcontroller.ShardReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("shard-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
		t.Fatal(err)
	}

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "own-shard", Namespace: "default",
			Labels: map[string]string{"multigres.com/cluster": "own-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0",
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "ghcr.io/multigres/multigres:main",
				Multipooler: "ghcr.io/multigres/multigres:main",
				Postgres:    "postgres:17",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "global-topo:2379", RootPath: "/multigres/global", Implementation: "etcd",
			},
			Multiorch: multigresv1alpha1.MultiorchSpec{
				Cells: []multigresv1alpha1.CellName{"zone-a"},
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone-a"},
					Type:            "readWrite",
					ReplicasPerCell: ptr.To(int32(1)),
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
	}
	setTestPostgresPasswordSecretRef(shard)
	ctx := t.Context()
	k8s := raw
	createTestPostgresPasswordSecret(t, ctx, k8s, shard.Namespace)
	seedLegacyShard(t, k8s, shard, ownership.LegacyResourceHandler)
	seededSpec := shard.Spec.DeepCopy()
	testutil.StartManager(t, mgr)

	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.Shard{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(shard), got); err != nil {
			return false
		}
		return slices.Contains(operatorManagers(got), ownership.ManagerShard)
	}, 15*time.Second, 100*time.Millisecond, "Shard never carried its own manager")

	requireNoLegacyEventually(t, k8s, &multigresv1alpha1.Shard{ObjectMeta: shard.ObjectMeta})
	got := &multigresv1alpha1.Shard{}
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(shard), got))
	require.Equal(t, *seededSpec, got.Spec, "spec changed across the legacy drop")
	requireOnlyManagers(
		t,
		operatorManagers(got),
		ownership.ManagerShard,
		ownership.ManagerShardGuard,
	)

	// Pool pods are created by this controller, so they too must carry only it.
	require.Eventually(t, func() bool {
		pods := &corev1.PodList{}
		if err := k8s.List(
			ctx,
			pods,
			client.InNamespace("default"),
		); err != nil ||
			len(pods.Items) == 0 {
			return false
		}
		for i := range pods.Items {
			for _, m := range operatorManagers(&pods.Items[i]) {
				if m != ownership.ManagerShard && m != ownership.ManagerShardReadiness {
					return false
				}
			}
		}
		return true
	}, 15*time.Second, 100*time.Millisecond, "pods carry a manager other than ManagerShard")
}
