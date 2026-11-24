//go:build integration
// +build integration

package tablegroup_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// TestTableGroupStatusFlapEndsAtTheLastStatus drives the TableGroup
// reconciler while its reads of the TableGroup are held at one snapshot, as a
// cache lagging the API server holds them: its status goes Healthy, then
// Degraded, then Healthy again before the cache sees the Degraded write. The
// last pass computes what the cache still holds, but the API server holds
// Degraded, so its apply must be sent. Nothing would reconcile the TableGroup
// again once the cache caught up: the controller ignores its own status
// writes.
func TestTableGroupStatusFlapEndsAtTheLastStatus(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cfg := testutil.SetUpEnvtest(t, testutil.WithCRDPaths("../../../../config/crd/bases"))
	direct := testutil.SetUpClient(t, cfg, scheme)
	reads := &testutil.HeldReads{Client: direct}
	r := &tablegroup.TableGroupReconciler{
		Client:   ownership.SkipNoopApplies(reads),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}
	ctx := t.Context()

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "flap",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "flap-cluster"},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "db1",
			TableGroupName: "tg1",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "etcd-client:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "orch:latest",
				Multipooler: "pooler:latest",
				Postgres:    "postgres:15",
			},
			Shards: []multigresv1alpha1.ShardResolvedSpec{{
				Name: "only",
				Multiorch: multigresv1alpha1.MultiorchSpec{
					StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
				},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{},
			}},
		},
	}
	setTestPostgresPasswordSecretRef(tg)
	require.NoError(t, direct.Create(ctx, tg))

	pass := func() {
		t.Helper()
		_, err := r.Reconcile(
			ctx,
			reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tg)},
		)
		require.NoError(t, err)
	}
	shardPhase := func(phase multigresv1alpha1.Phase) {
		t.Helper()
		shards := &multigresv1alpha1.ShardList{}
		require.NoError(t, direct.List(ctx, shards, client.InNamespace("default")))
		require.Len(t, shards.Items, 1)
		s := &shards.Items[0]
		s.Status.Phase = phase
		s.Status.ObservedGeneration = s.Generation
		require.NoError(t, direct.Status().Update(ctx, s))
	}
	livePhase := func() multigresv1alpha1.Phase {
		t.Helper()
		got := &multigresv1alpha1.TableGroup{}
		require.NoError(t, direct.Get(ctx, client.ObjectKeyFromObject(tg), got))
		return got.Status.Phase
	}

	pass()
	shardPhase(multigresv1alpha1.PhaseHealthy)
	pass()
	require.Equal(t, multigresv1alpha1.PhaseHealthy, livePhase())

	reads.Hold(t, &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{Name: tg.Name, Namespace: tg.Namespace},
	})
	shardPhase(multigresv1alpha1.PhaseDegraded)
	pass()
	require.Equal(t, multigresv1alpha1.PhaseDegraded, livePhase())

	shardPhase(multigresv1alpha1.PhaseHealthy)
	pass()
	require.Equal(t, multigresv1alpha1.PhaseHealthy, livePhase())
}
