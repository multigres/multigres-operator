//go:build integration
// +build integration

package toposerver_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	toposervercontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

func TestCellEventsEnqueueReferencedAuthTopoServer(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(
			filepath.Join("../../../../", "config", "crd", "bases"),
		),
	)
	ctx := t.Context()
	k8sClient := mgr.GetClient()

	const (
		storesNS   = "cell-watch-stores"
		projectsNS = "cell-watch-projects"
	)
	for _, name := range []string{storesNS, projectsNS} {
		require.NoError(t, k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		}))
	}

	// The reconcile boundary only counts, so nothing but the watch wiring can
	// move the numbers below.
	counted := testutil.NewCountingReconciler(reconcile.Func(
		func(context.Context, reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		},
	))
	require.NoError(t, (&toposervercontroller.TopoServerReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("toposerver-controller"),
	}).SetupWithManagerReconciler(mgr, counted, controller.Options{
		SkipNameValidation: ptr.To(true),
	}))

	authKey := types.NamespacedName{Namespace: storesNS, Name: "shared"}
	plainKey := types.NamespacedName{Namespace: storesNS, Name: "plain"}
	require.NoError(t, k8sClient.Create(ctx, &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: authKey.Name, Namespace: authKey.Namespace},
		Spec: multigresv1alpha1.TopoServerSpec{
			Etcd: &multigresv1alpha1.EtcdSpec{},
			Auth: &multigresv1alpha1.TopoServerAuthSpec{Enabled: true},
			TLS: &multigresv1alpha1.TopoTLSConfig{
				Enabled:    ptr.To(true),
				IssuerName: "test-issuer",
			},
		},
	}))
	require.NoError(t, k8sClient.Create(ctx, &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{Name: plainKey.Name, Namespace: plainKey.Namespace},
		Spec:       multigresv1alpha1.TopoServerSpec{Etcd: &multigresv1alpha1.EtcdSpec{}},
	}))
	for _, key := range []types.NamespacedName{authKey, plainKey} {
		require.Eventually(t, func() bool { return counted.CountFor(key) > 0 },
			10*time.Second, 100*time.Millisecond, "TopoServer %s was never reconciled", key)
		counted.WaitQuiescent(t, key)
	}

	requireEnqueued := func(before int, why string) {
		t.Helper()
		require.Eventually(t, func() bool { return counted.CountFor(authKey) > before },
			10*time.Second, 100*time.Millisecond, why)
		counted.WaitQuiescent(t, authKey)
	}

	before := counted.CountFor(authKey)
	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{Name: "zone1", Namespace: projectsNS},
		Spec: multigresv1alpha1.CellSpec{
			Name:   "zone1",
			ZoneID: "usw1-az1",
			Images: multigresv1alpha1.CellImages{
				Multigateway: "ghcr.io/multigres/multigres:main",
			},
			Multigateway: multigresv1alpha1.MultigatewaySpec{
				StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address:        "https://shared." + storesNS + ".svc:2379",
				RootPath:       "/multigres/global",
				Implementation: "etcd",
			},
			TopoServer: &multigresv1alpha1.LocalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{
						multigresv1alpha1.EndpointUrl("https://plain." + storesNS + ".svc:2379"),
					},
					RootPath: "/multigres/zone1",
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, cell))
	requireEnqueued(before, "creating a referencing Cell should enqueue the TopoServer")

	before = counted.CountFor(authKey)
	cell.Status.Phase = multigresv1alpha1.PhaseHealthy
	cell.Status.Message = "status only"
	require.NoError(t, k8sClient.Status().Update(ctx, cell))
	require.Never(t, func() bool { return counted.CountFor(authKey) != before },
		2*time.Second, 100*time.Millisecond,
		"a Cell status update must not enqueue the TopoServer")

	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(cell), cell))
	cell.Spec.Multigateway.Replicas = ptr.To(int32(2))
	require.NoError(t, k8sClient.Update(ctx, cell))
	requireEnqueued(before, "a Cell spec change should enqueue the TopoServer")

	before = counted.CountFor(authKey)
	require.NoError(t, k8sClient.Delete(ctx, cell))
	requireEnqueued(before, "deleting a referencing Cell should enqueue the TopoServer")

	// A cluster with no Cells leaves no Cell event behind when it goes away.
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant", Namespace: projectsNS},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			PostgresPasswordSecretRef: multigresv1alpha1.PostgresPasswordSecretRef{
				Name: "postgres-password", Key: "password",
			},
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{
						multigresv1alpha1.EndpointUrl("https://shared." + storesNS + ".svc:2379"),
					},
				},
			},
		},
	}
	before = counted.CountFor(authKey)
	require.NoError(t, k8sClient.Create(ctx, cluster))
	require.Never(t, func() bool { return counted.CountFor(authKey) != before },
		2*time.Second, 100*time.Millisecond,
		"creating or updating a cluster must not enqueue the TopoServer")

	before = counted.CountFor(authKey)
	require.NoError(t, k8sClient.Delete(ctx, cluster))
	requireEnqueued(before, "deleting a referencing cluster should enqueue the TopoServer")

	require.Equal(t, 1, counted.CountFor(plainKey),
		"a TopoServer without managed auth must not be enqueued by Cell events")
}
