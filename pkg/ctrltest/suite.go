package ctrltest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Options is everything Boot needs from the consumer. Nothing here has a
// harness-supplied default that could silently diverge from the operator the
// consumer is actually testing: the scheme, the CRDs, the cache config and the
// client rate limits are all the consumer's, because a harness that guessed
// any of them would make every test a claim about the guess.
type Options struct {
	// Scheme is used whole, by both the manager and the direct client. Boot
	// adds nothing to it, so a kind missing here is a kind neither the
	// controllers nor Watch can resolve.
	Scheme *runtime.Scheme

	// CRDPaths are directories of CRD manifests installed into envtest. Empty
	// is legal and means a consumer testing over built-in kinds only.
	CRDPaths []string

	// CacheOptions is the manager's cache config, which should be the same
	// one production uses. A default cache makes every cached read behave
	// differently from production and voids the premise that these are the
	// real controllers wired as in main.
	CacheOptions cache.Options

	// OperatorNamespace stands in for the namespace the operator deploys
	// into. A production cache usually treats it specially (unfiltered), so
	// the suite has to have one for that config to mean anything. Boot
	// creates it. Empty means the consumer's cache does not single one out.
	OperatorNamespace string

	// WatchedKinds is every kind RequireQuiescent watches for churn. A kind
	// missing here is a kind whose churn quiescence cannot see.
	WatchedKinds []client.ObjectList

	// QPS and Burst are applied to the rest config before the manager is
	// built, and should match what the operator's own main sets.
	QPS   float32
	Burst int

	// SimInterval is how often the data plane simulator sweeps. It runs for
	// the life of the suite across every namespace, because the manager it
	// feeds is also suite-wide. Non-positive means no simulator at all, for
	// a consumer that models no data plane.
	SimInterval time.Duration

	// Register wires the consumer's reconcilers onto mgr. It runs with the
	// suite fully built and the manager not yet started, which is the only
	// window in which both s.Ops.For and s.Reconciles.Wrap can be used and
	// the controllers can still be added.
	Register func(ctx context.Context, mgr manager.Manager, s *Suite) error
}

// Suite is one envtest apiserver, one manager, and whatever reconcilers the
// consumer registered, shared by every test in the consumer's package. Tests
// isolate themselves with Namespace, not by booting their own.
type Suite struct {
	Cfg    *rest.Config
	Scheme *runtime.Scheme
	Mgr    manager.Manager

	// Client reads and writes directly, bypassing the manager cache. Tests
	// want this: the cache is filtered exactly as in production, so a cached
	// read from a test would lie about anything the filter excludes. That
	// same reasoning is why Watch builds its event stream on this client
	// rather than on an informer, which is what the WithWatch is for.
	Client client.WithWatch

	// Ops attributes every write to the controller that issued it.
	Ops *Recorder

	// Reconciles sits at every wrapped controller's reconcile boundary: it
	// gates by namespace, compresses requeues, and records each pass. It
	// answers what Ops cannot, namely that a controller ran and decided to do
	// nothing.
	Reconciles *Interceptor

	env *envtest.Environment

	// watchedKinds is every kind RequireQuiescent watches for churn. Set once
	// in Boot from Options.
	watchedKinds []client.ObjectList

	cancel  context.CancelFunc
	stopped chan error
}

// Boot brings up envtest and the manager. The returned function tears both
// down and must run before any goroutine leak check, since the manager owns
// goroutines that only exit once its context is cancelled.
func Boot(opts Options) (*Suite, func() error, error) {
	log.SetLogger(zap.New(zap.UseDevMode(false), zap.WriteTo(os.Stderr)))

	env := &envtest.Environment{
		CRDDirectoryPaths: opts.CRDPaths,
		// Only meaningful when there are paths to miss. A consumer testing
		// over built-in kinds passes none and still boots.
		ErrorIfCRDPathMissing:    len(opts.CRDPaths) > 0,
		ControlPlaneStartTimeout: 120 * time.Second,
		ControlPlaneStopTimeout:  60 * time.Second,
	}
	cfg, err := env.Start()
	if err != nil {
		return nil, nil, fmt.Errorf("envtest start: %w", err)
	}

	// Matches main.go: the operator raises these to avoid client-side
	// throttling once several controllers are reconciling at once.
	cfg.QPS = opts.QPS
	cfg.Burst = opts.Burst

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         opts.Scheme,
		LeaderElection: false,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		Cache:          opts.CacheOptions,
	})
	if err != nil {
		_ = env.Stop()
		return nil, nil, fmt.Errorf("new manager: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	fail := func(err error) (*Suite, func() error, error) {
		cancel()
		_ = env.Stop()
		return nil, nil, err
	}

	ops := NewRecorder()
	s := &Suite{
		Cfg:          cfg,
		Scheme:       opts.Scheme,
		Mgr:          mgr,
		Ops:          ops,
		Reconciles:   NewInterceptor(ops, RequeueClamp),
		env:          env,
		watchedKinds: opts.WatchedKinds,
		cancel:       cancel,
		stopped:      make(chan error, 1),
	}

	// Built before Register runs, so the Suite a consumer is handed is whole:
	// a Register that wants to read or seed the API server can, and none of
	// its wiring has to be deferred to first use.
	s.Client, err = client.NewWithWatch(cfg, client.Options{Scheme: opts.Scheme})
	if err != nil {
		return fail(fmt.Errorf("direct client: %w", err))
	}

	if opts.Register != nil {
		if err := opts.Register(ctx, mgr, s); err != nil {
			return fail(fmt.Errorf("register: %w", err))
		}
	}

	go func() { s.stopped <- mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return fail(fmt.Errorf("cache sync failed"))
	}

	if opts.OperatorNamespace != "" {
		if err := s.Client.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: opts.OperatorNamespace},
		}); err != nil {
			return fail(fmt.Errorf("create operator namespace: %w", err))
		}
	}

	// The data plane fake runs for the life of the suite, across every
	// namespace, because the manager it feeds is also suite-wide. A
	// non-positive interval means no data plane to model, and starting it
	// anyway would panic in NewTicker from a goroutine with no t to blame.
	if opts.SimInterval > 0 {
		go NewDataPlaneSim(s.Client, opts.SimInterval).Run(ctx)
	}

	return s, s.teardown, nil
}

// teardown stops the manager and then envtest, in that order, and waits for
// both. A clean sequence leaks no goroutines; skipping the wait is what would
// make a leak check report the whole manager.
func (s *Suite) teardown() error {
	s.cancel()
	select {
	case err := <-s.stopped:
		if err != nil {
			return fmt.Errorf("manager stopped with error: %w", err)
		}
	case <-time.After(60 * time.Second):
		return fmt.Errorf("manager did not stop within 60s")
	}
	return s.env.Stop()
}

var nsSeq struct {
	sync.Mutex
	n int
}

// Namespace creates a namespace for one test, admits it at the reconcile
// boundary, and reverses both afterwards.
//
// Admission hangs off this helper rather than being a separate call a test
// makes, because the gate defaults to admitting nothing and a test that forgot
// to register would watch a manager quietly ignore everything it created. The
// corollary is that a namespace conjured up without this helper is invisible
// to every wrapped controller.
//
// Note it does not wait for deletion to finish: envtest runs no namespace
// controller, so a terminating namespace never actually goes away. Which is
// the other reason the gate closes here: the objects outlive the test, and
// under requeue compression an ungated finished test would keep every
// reconciler busy on them for the rest of the package.
func (s *Suite) Namespace(t *testing.T) string {
	t.Helper()
	nsSeq.Lock()
	nsSeq.n++
	name := fmt.Sprintf("t%d-%d", time.Now().UnixNano()%1e6, nsSeq.n)
	nsSeq.Unlock()

	s.Reconciles.Activate(name)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := s.Client.Create(t.Context(), ns); err != nil {
		s.Reconciles.Deactivate(name)
		t.Fatalf("create namespace %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = s.Client.Delete(context.Background(), ns)
	})
	// Registered last so it runs first: cleanups are LIFO, and closing the
	// gate before the delete keeps teardown quiet rather than kicking off a
	// round of deletion reconciles the test will never observe.
	t.Cleanup(func() {
		s.Reconciles.Deactivate(name)
	})
	return name
}
