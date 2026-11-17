//go:build integration
// +build integration

package multigrescluster_test

import (
	"context"
	"crypto/tls"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
	"github.com/multigres/multigres-operator/pkg/resolver"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
	multigreswebhook "github.com/multigres/multigres-operator/pkg/webhook"
)

// fakeCatalog is the sharding catalog as multiadmin lists it. dropOnClaim
// models a DROP TABLEGROUP that lands between the operator's first read and
// its claim: once the id is claimed, every read lists it dropping.
type fakeCatalog struct {
	mu          sync.Mutex
	topo        topoclient.Store
	entries     map[int64]multiadmin.TableGroup
	dropOnClaim map[int64]bool
}

func (c *fakeCatalog) set(tg multiadmin.TableGroup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[tg.ID] = tg
}

func (c *fakeCatalog) list(ctx context.Context, _ *multigresv1alpha1.MultigresCluster, db string) ([]multiadmin.TableGroup, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []multiadmin.TableGroup
	for id, tg := range c.entries {
		if c.dropOnClaim[id] {
			if _, err := c.topo.GetTableGroupProvisionClaim(ctx, db, id); err == nil {
				tg.State = multiadmin.StateDropping
				c.entries[id] = tg
			}
		}
		out = append(out, tg)
	}
	return out, nil
}

// TestDynamicTableGroups_RemovalWithWebhook runs the MultigresCluster
// controller against an API server with the operator's admission webhooks
// installed, as the operator's service account, so every TableGroup and
// Shard write the removal makes passes the same admission it does in a
// cluster. The test plays the TableGroup controller (ReadyForDeletion) and
// garbage collection (Shard deletion), which envtest does not run.
func TestDynamicTableGroups_RemovalWithWebhook(t *testing.T) {
	ctx := t.Context()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("../../../../", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("../../../../", "config", "webhook")},
		},
		ControlPlaneStartTimeout: 60 * time.Second,
		ControlPlaneStopTimeout:  60 * time.Second,
	}
	adminCfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { _ = env.Stop() })

	// The manager, and so the controller, acts as the operator's service
	// account: the only principal the child-resource webhook admits.
	operatorCfg := rest.CopyConfig(adminCfg)
	operatorCfg.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + testNamespace + ":multigres-operator",
		Groups:   []string{"system:masters"},
	}
	wh := env.WebhookInstallOptions
	mgr, err := ctrl.NewManager(operatorCfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    wh.LocalServingHost,
			Port:    wh.LocalServingPort,
			CertDir: wh.LocalServingCertDir,
			TLSOpts: []func(*tls.Config){func(*tls.Config) {}},
		}),
	})
	require.NoError(t, err)
	require.NoError(t, multigreswebhook.Setup(mgr,
		resolver.NewResolver(mgr.GetClient(), testNamespace),
		multigreswebhook.Options{Namespace: testNamespace}))

	topo, factory := memorytopo.NewServerAndFactory(ctx, "zone-a")
	t.Cleanup(func() { _ = topo.Close() })
	catalog := &fakeCatalog{
		topo:        topo,
		entries:     map[int64]multiadmin.TableGroup{},
		dropOnClaim: map[int64]bool{},
	}
	r := &multigrescluster.MultigresClusterReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorderFor("multigres-cluster-controller"),
		APIReader: mgr.GetAPIReader(),
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return topoclient.NewWithFactory(
				factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
			), nil
		},
		ListDesiredTableGroups: catalog.list,
	}
	require.NoError(t, r.SetupWithManagerReconciler(mgr, r, controller.Options{
		SkipNameValidation: ptr.To(true),
	}))
	go func() { _ = mgr.Start(ctx) }()
	require.True(t, mgr.GetCache().WaitForCacheSync(ctx))

	admin, err := client.New(adminCfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	operator := mgr.GetClient()

	for _, obj := range []client.Object{
		&multigresv1alpha1.CoreTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
			Spec: multigresv1alpha1.CoreTemplateSpec{
				GlobalTopoServer: &multigresv1alpha1.TopoServerSpec{
					Etcd: &multigresv1alpha1.EtcdSpec{Image: "etcd:default"},
				},
				Multiadmin: &multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
			},
		},
		&multigresv1alpha1.CellTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
			Spec: multigresv1alpha1.CellTemplateSpec{
				Multigateway: &multigresv1alpha1.MultigatewaySpec{
					StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(1))},
				},
			},
		},
		&multigresv1alpha1.ShardTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
		},
		&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "standard",
				Annotations: map[string]string{
					"storageclass.kubernetes.io/is-default-class": "true",
				},
			},
			Provisioner: "k8s.io/fake",
		},
	} {
		require.NoError(t, admin.Create(ctx, obj))
	}

	const clusterName = "dyn"
	catalog.set(multiadmin.TableGroup{
		ID: 7, Name: "commerce", Shards: []string{"0-inf"}, State: multiadmin.StateCreating,
	})
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterName, Namespace: testNamespace,
			Annotations: map[string]string{multigresv1alpha1.AnnotationDynamicTableGroups: "true"},
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate: "default", CellTemplate: "default", ShardTemplate: "default",
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
		},
	}
	setTestPostgresPasswordSecretRef(cluster)
	require.NoError(t, admin.Create(ctx, cluster))

	tgKey := func(tg string) client.ObjectKey {
		return client.ObjectKey{Namespace: testNamespace, Name: nameutil.JoinWithConstraints(
			nameutil.DefaultConstraints, clusterName, "postgres", tg)}
	}
	claimed := func(id int64) bool {
		_, err := topo.GetTableGroupProvisionClaim(ctx, "postgres", id)
		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			return false
		}
		require.NoError(t, err)
		return true
	}
	const wait, tick = 30 * time.Second, 200 * time.Millisecond

	// 1. Provisioned under a claim.
	commerce := &multigresv1alpha1.TableGroup{}
	require.Eventually(t, func() bool {
		return admin.Get(ctx, tgKey("commerce"), commerce) == nil
	}, wait, tick, "commerce was not provisioned")
	require.True(t, claimed(7), "commerce provisioned without a claim")

	// The webhook is live: nobody but the operator deletes a TableGroup.
	err = admin.Delete(ctx, commerce.DeepCopy())
	require.Error(t, err, "the admission webhook let an admin delete a TableGroup")
	require.Contains(t, err.Error(), "prohibited")

	// A Shard of commerce, as the TableGroup controller creates it.
	shard, err := tablegroup.BuildShard(commerce, &commerce.Spec.Shards[0], scheme)
	require.NoError(t, err)
	require.NoError(t, operator.Create(ctx, shard))

	// 2. DROP TABLEGROUP: the operator marks commerce for deletion.
	catalog.set(multiadmin.TableGroup{
		ID: 7, Name: "commerce", Shards: []string{"0-inf"}, State: multiadmin.StateDropping,
	})
	require.Eventually(t, func() bool {
		if err := admin.Get(ctx, tgKey("commerce"), commerce); err != nil {
			return false
		}
		return commerce.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != ""
	}, wait, tick, "commerce was not marked for deletion")

	// The TableGroup controller reports the drain done; the operator deletes
	// the TableGroup through the webhook.
	meta.SetStatusCondition(&commerce.Status.Conditions, metav1.Condition{
		Type:   multigresv1alpha1.ConditionReadyForDeletion,
		Status: metav1.ConditionTrue,
		Reason: "Drained",
	})
	require.NoError(t, operator.Status().Update(ctx, commerce))
	require.Eventually(t, func() bool {
		return apierrors.IsNotFound(admin.Get(ctx, tgKey("commerce"), &multigresv1alpha1.TableGroup{}))
	}, wait, tick, "commerce was not deleted")

	// Its Shard is still running its finalizer: the claim stays.
	time.Sleep(time.Second)
	require.True(t, claimed(7), "claim released while a Shard carrying the id exists")

	// 3. The Shard is gone: the poller sees the held claim and the reconcile
	// releases it.
	require.NoError(t, operator.Delete(ctx, shard))
	require.Eventually(t, func() bool { return !claimed(7) },
		wait, tick, "claim not released once nothing carries the id")

	// 4. A drop lands between the operator's read and its claim: the re-read
	// sees it, nothing is provisioned and the claim is released.
	catalog.mu.Lock()
	delete(catalog.entries, 7) // the catalog completed commerce's drop
	catalog.dropOnClaim[8] = true
	catalog.mu.Unlock()
	catalog.set(multiadmin.TableGroup{
		ID: 8, Name: "orders", Shards: []string{"0-inf"}, State: multiadmin.StateCreating,
	})
	require.Eventually(t, func() bool {
		l, _ := catalog.list(ctx, nil, "postgres")
		return len(l) == 1 && l[0].State == multiadmin.StateDropping
	}, wait, tick, "the operator never claimed orders")
	require.Eventually(t, func() bool { return !claimed(8) },
		wait, tick, "claim of orders not released")
	err = admin.Get(ctx, tgKey("orders"), &multigresv1alpha1.TableGroup{})
	require.True(t, apierrors.IsNotFound(err), "orders was provisioned: %v", err)

	// Reported nowhere as a problem.
	got := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, admin.Get(ctx, client.ObjectKeyFromObject(cluster), got))
	cond := meta.FindStatusCondition(got.Status.Conditions, multigresv1alpha1.ConditionDynamicTableGroups)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, cond.Message)
}
