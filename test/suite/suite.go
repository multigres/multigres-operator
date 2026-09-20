// Package suite is the multi-controller envtest harness for this operator: one
// manager running every reconciler the operator runs, so behaviour that is a
// protocol between controllers becomes testable.
//
// The generic half lives in pkg/ctrltest. What is left here is everything that
// is about this operator specifically: its scheme, its CRDs, its cache config,
// its five reconcilers, and the data plane doubles those reconcilers need.
//
// It has no build tag. Exclusion from the ordinary test targets is by path
// filter, and the entrypoint is `make test-suite`.
package suite

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cacheopts"
	multigresclustercontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	tablegroupcontroller "github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	cellcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/cell"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	toposervercontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/toposerver"
	"github.com/multigres/testkit/ctrltest"
)

// OperatorNamespace stands in for the namespace the operator deploys into. The
// cache treats it specially (unfiltered), so the suite has to have one for the
// production cache config to mean anything.
const OperatorNamespace = "multigres-operator-system"

// Suite is the suite-wide harness, booted once by TestMain. One envtest and one
// manager serve the whole package; isolate with Suite.Namespace(t).
var Suite *ctrltest.Suite

// The operator's data plane doubles. Deliberately not suite members: every
// operator's data plane is different, so ctrltest has no place to put these and
// a test reaching for them is reaching for something about this operator.
var (
	rpc     *rpcclient.FakeClient
	topo    *topoRegistry
	poolers *poolerSim
)

// Boot brings up envtest and the manager. The returned function tears both down
// and must run before any goroutine leak check, since the manager owns
// goroutines that only exit once its context is cancelled.
func Boot() (*ctrltest.Suite, func() error, error) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		multigresv1alpha1.AddToScheme,
		appsv1.AddToScheme,
		corev1.AddToScheme,
		policyv1.AddToScheme,
		networkingv1.AddToScheme,
		storagev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, nil, fmt.Errorf("add to scheme: %w", err)
		}
	}

	return ctrltest.Boot(ctrltest.Options{
		Scheme:       scheme,
		CRDPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		WatchedKinds: watchedKinds(),
		SimInterval:  250 * time.Millisecond,
		Managers: []ctrltest.ManagerOptions{{
			Name:              "multigres-operator",
			CacheOptions:      cacheopts.New(OperatorNamespace),
			OperatorNamespace: OperatorNamespace,
			// Matches main.go: the operator raises these to avoid client-side
			// throttling once several controllers are reconciling at once.
			QPS:      50,
			Burst:    100,
			Register: register,
		}},
	})
}

// watchedKinds is every kind the operator writes in a test namespace. A kind
// missing here is a kind whose churn RequireQuiescent cannot see.
func watchedKinds() []client.ObjectList {
	return []client.ObjectList{
		&multigresv1alpha1.MultigresClusterList{},
		&TopoServerList{},
		&multigresv1alpha1.CellList{},
		&TableGroupList{},
		&ShardList{},
		&corev1.PodList{},
		&appsv1.DeploymentList{},
		&appsv1.StatefulSetList{},
		&corev1.PersistentVolumeClaimList{},
		&corev1.ConfigMapList{},
		&corev1.ServiceList{},
		&policyv1.PodDisruptionBudgetList{},
	}
}

// register wires every reconciler exactly as cmd/multigres-operator/main.go
// does, differing only in the seams a test has to fake: the topology store and
// the multipooler RPC client, and in the interceptor each one's reconcile
// boundary is wrapped in.
//
// Each reconciler is a named variable rather than the anonymous composite
// literal this used to be, and that is load bearing rather than tidying.
// SetupWithManagerReconciler substitutes the reconcile boundary and nothing
// else: the receiver stays live on the enqueue path, because map functions and
// predicates bind to it when the builder runs and call its client, and
// ShardReconciler keeps mutable state on itself. So the same pointer has to be
// both the receiver and what the interceptor delegates to.
func register(ctx context.Context, mgr manager.Manager, s *ctrltest.Suite) error {
	base := mgr.GetClient()

	rpc = rpcclient.NewFakeClient()
	topo = newTopoRegistry(ctx)

	// The pooler fake runs for the life of the suite, across every namespace,
	// because the manager it feeds is also suite-wide.
	poolers = &poolerSim{c: s.Client, rpc: rpc, topo: topo, interval: 500 * time.Millisecond}
	go poolers.run(ctx)

	// Deliberately a bare option struct rather than each controller's
	// production options. Every controller sets MaxConcurrentReconciles to 20
	// in its own SetupWithManager and then lets a caller-supplied
	// controller.Options replace that wholesale, so this suite's options
	// decide the value; it is set to 1 here rather than inherited by omission
	// from the controller-runtime default, which is what used to happen.
	//
	// That divergence from production is load-bearing in both directions.
	// It is what makes the recorder's per-controller ordering assertions
	// meaningful: one reconcile goroutine per controller means writes are
	// issued and recorded in program order. It is also what this suite
	// therefore cannot catch, namely a controller racing itself across
	// concurrent reconciles of different objects. Raising this to match
	// production would silently turn every ordering assertion in the
	// scenario tests into a race that fails a few times a week and reads as operator
	// flakiness, and it would also break Interceptor.Ops, which identifies a
	// pass's writes by an op log range that only one in-flight reconcile per
	// controller can make unambiguous.
	opts := controller.Options{
		SkipNameValidation:      ptr.To(true),
		MaxConcurrentReconciles: 1,
	}

	cluster := &multigresclustercontroller.MultigresClusterReconciler{
		Client:          s.Ops.For("multigrescluster", base),
		Scheme:          mgr.GetScheme(),
		Recorder:        mgr.GetEventRecorderFor("multigrescluster-controller"),
		APIReader:       mgr.GetAPIReader(),
		CreateTopoStore: topo.ForClusterRef,
	}
	if err := cluster.SetupWithManagerReconciler(
		mgr, s.Reconciles.Wrap("multigrescluster", cluster), opts,
	); err != nil {
		return fmt.Errorf("setup multigrescluster: %w", err)
	}

	tableGroup := &tablegroupcontroller.TableGroupReconciler{
		Client:   s.Ops.For("tablegroup", base),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tablegroup-controller"),
	}
	if err := tableGroup.SetupWithManagerReconciler(
		mgr, s.Reconciles.Wrap("tablegroup", tableGroup), opts,
	); err != nil {
		return fmt.Errorf("setup tablegroup: %w", err)
	}

	cell := &cellcontroller.CellReconciler{
		Client:   s.Ops.For("cell", base),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("cell-controller"),
	}
	if err := cell.SetupWithManagerReconciler(
		mgr, s.Reconciles.Wrap("cell", cell), opts,
	); err != nil {
		return fmt.Errorf("setup cell: %w", err)
	}

	topoServer := &toposervercontroller.TopoServerReconciler{
		Client:   s.Ops.For("toposerver", base),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("toposerver-controller"),
	}
	if err := topoServer.SetupWithManagerReconciler(
		mgr, s.Reconciles.Wrap("toposerver", topoServer), opts,
	); err != nil {
		return fmt.Errorf("setup toposerver: %w", err)
	}

	shard := &shardcontroller.ShardReconciler{
		Client:          s.Ops.For("shard", base),
		Scheme:          mgr.GetScheme(),
		Recorder:        mgr.GetEventRecorderFor("shard-controller"),
		APIReader:       mgr.GetAPIReader(),
		PoolerClients:   poolerclient.Static(rpc),
		CreateTopoStore: topo.ForShard,
	}
	if err := shard.SetupWithManagerReconciler(
		mgr, s.Reconciles.Wrap("shard", shard), opts,
	); err != nil {
		return fmt.Errorf("setup shard: %w", err)
	}
	return nil
}
