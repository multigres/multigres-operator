//go:build integration
// +build integration

package tablegroup_test

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multigres/testkit/managedfields"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// operatorManagers narrows managedfields.Managers to the operator's own
// "multigres-" names: the creating test client and the apiserver also appear,
// and are not the subject here.
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

// hasLegacyManager reports whether obj carries an entry from a legacy manager.
func hasLegacyManager(obj client.Object) bool {
	return slices.ContainsFunc(managedfields.Managers(obj), ownership.IsLegacy)
}

// requireLegacyEntries asserts the seed left a legacy entry on the main
// resource and on status, as one last written by the pre-rename operator does.
func requireLegacyEntries(t *testing.T, obj client.Object) {
	t.Helper()
	entries, err := managedfields.Entries(obj)
	require.NoError(t, err)
	for _, sub := range []string{"", "status"} {
		require.True(t, slices.ContainsFunc(entries, func(e managedfields.Entry) bool {
			return ownership.IsLegacy(e.Manager) && e.Subresource == sub
		}), "seed left no legacy entry on subresource %q", sub)
	}
}

func TestTableGroup_WritesUnderOwnManager(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	raw := testutil.SetUpClient(t, cfg, scheme)
	mgr := testutil.SetUpManager(t, cfg, scheme)
	if err := (&tablegroup.TableGroupReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
		t.Fatal(err)
	}

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tg-own", Namespace: "default",
			Labels: map[string]string{"multigres.com/cluster": "own-cluster"},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName: "db1", TableGroupName: "tg1",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "etcd-client:2379", RootPath: "/multigres/global", Implementation: "etcd",
			},
			Images: multigresv1alpha1.ShardImages{
				Multiorch: "orch:latest", Multipooler: "pooler:latest", Postgres: "postgres:15",
			},
			Shards: []multigresv1alpha1.ShardResolvedSpec{{
				Name: "s0",
				Multiorch: multigresv1alpha1.MultiorchSpec{
					StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
			}},
		},
	}
	setTestPostgresPasswordSecretRef(tg)
	ctx := t.Context()
	k8s := raw
	// Applied twice, as the pre-rename operator wrote it: the main resource,
	// then status with the fields the CRD requires. The controller must not be
	// running yet, or its own reconcile races the seed.
	require.NoError(t, managedfields.Seed(ctx, k8s, tg, ownership.LegacyOperator))
	tg.Status = multigresv1alpha1.TableGroupStatus{ObservedGeneration: 1}
	require.NoError(t, managedfields.Seed(ctx, k8s, tg, ownership.LegacyOperator,
		managedfields.OnStatus()))
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(tg), tg))
	requireLegacyEntries(t, tg)
	seededSpec := tg.Spec.DeepCopy()
	testutil.StartManager(t, mgr)

	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.TableGroup{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(tg), got); err != nil {
			return false
		}
		return slices.Contains(operatorManagers(got), ownership.ManagerTableGroup)
	}, 10*time.Second, 100*time.Millisecond, "TableGroup never carried its own manager")

	// Only DropLegacy retires legacy entries, so this waits for the reconcile.
	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.TableGroup{}
		return k8s.Get(ctx, client.ObjectKeyFromObject(tg), got) == nil && !hasLegacyManager(got)
	}, 15*time.Second, 100*time.Millisecond, "legacy field manager entries never dropped")
	got := &multigresv1alpha1.TableGroup{}
	require.NoError(t, k8s.Get(ctx, client.ObjectKeyFromObject(tg), got))
	require.Equal(t, *seededSpec, got.Spec, "spec changed across the legacy drop")
	requireOnlyManagers(t, operatorManagers(got), ownership.ManagerTableGroup)

	shards := &multigresv1alpha1.ShardList{}
	require.Eventually(t, func() bool {
		return k8s.List(ctx, shards, client.InNamespace("default")) == nil && len(shards.Items) == 1
	}, 10*time.Second, 100*time.Millisecond, "child Shard never created")
	requireOnlyManagers(t, operatorManagers(&shards.Items[0]), ownership.ManagerTableGroup)
}
