package ctrltest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestQuiescenceHoldsOnAQuietNamespace is the negative control for the tests
// below: whatever they catch has to be the thing they set up and not the
// primitive firing at rest.
func TestQuiescenceHoldsOnAQuietNamespace(t *testing.T) {
	ns := shared.Namespace(t)

	if err := shared.TryQuiescent(t, ns, time.Second, 15*time.Second); err != nil {
		t.Fatalf("an empty namespace should be quiescent:\n%v", err)
	}
}

// TestQuiescenceFailsOnAChangingObject exercises the state half on its own. The
// writer is the suite's bare client rather than a recorded one, so the recorder
// never sees this loop and only the event stream can catch it.
//
// The loop flaps between two values rather than counting up, which is the shape
// of a real hot loop: two writers overwriting each other. That is also what the
// report has to render legibly, so this asserts on both transitions and not
// only on the path they moved on.
func TestQuiescenceFailsOnAChangingObject(t *testing.T) {
	ns := shared.Namespace(t)
	cm := createProbeConfigMap(t, ns, "churn")

	flap := [2]string{"alpha", "beta"}
	stop := writeLoop(100*time.Millisecond, func(i int) {
		patch := client.RawPatch(
			types.MergePatchType,
			fmt.Appendf(nil, `{"data":{"tick":%q}}`, flap[i%2]),
		)
		_ = shared.Client.Patch(context.Background(), cm, patch)
	})
	defer stop()

	err := shared.TryQuiescent(t, ns, time.Second, 6*time.Second)
	if err == nil {
		t.Fatal("an object changing every 100ms must not read as quiescent")
	}
	for _, want := range []string{
		`data.tick: "alpha" -> "beta"`,
		`data.tick: "beta" -> "alpha"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the report must name the values the field flapped between, "+
				"want a line containing %s, got:\n%v", want, err)
		}
	}
}

// TestQuiescenceFailsOnANoOpWriteLoop is the case that dies if quiescence is
// rebuilt on the event stream alone.
//
// The patch is empty, so the API server accepts it and writes nothing: no
// resourceVersion moves and the stream reports not one event. What makes it
// churn is that it was issued at all, thousands of times, which only the
// recorder can see. This is the shape of the tablegroup controller's 3,641
// no-op patches, reproduced small.
func TestQuiescenceFailsOnANoOpWriteLoop(t *testing.T) {
	ns := shared.Namespace(t)
	cm := createProbeConfigMap(t, ns, "noop")
	before := cm.GetResourceVersion()

	recorded := shared.Ops.For("quiescence-noop", shared.Client)
	stop := writeLoop(50*time.Millisecond, func(int) {
		_ = recorded.Patch(
			context.Background(),
			cm,
			client.RawPatch(types.MergePatchType, []byte(`{}`)),
		)
	})
	defer stop()

	err := shared.TryQuiescent(t, ns, time.Second, 6*time.Second)
	stop()

	after := &corev1.ConfigMap{}
	if getErr := shared.Client.Get(
		t.Context(), client.ObjectKeyFromObject(cm), after,
	); getErr != nil {
		t.Fatalf("re-read the probe ConfigMap: %v", getErr)
	}
	// Asserted rather than assumed: a loop that moved the resourceVersion
	// would be caught by the state half, and this test would then prove
	// nothing about the write half.
	if after.GetResourceVersion() != before {
		t.Fatalf("the write loop was not a no-op: resourceVersion moved %s -> %s",
			before, after.GetResourceVersion())
	}

	if err == nil {
		t.Fatal("a no-op write loop must not read as quiescent: " +
			"it produces no watch event, so passing here means the write half is gone")
	}
	if !strings.Contains(err.Error(), "quiescence-noop") {
		t.Fatalf("the report must name the writer, got:\n%v", err)
	}
}

// TestQuiescenceFailsOnARejectedWriteLoop is the case that dies if the recorder
// goes back to recording only the writes the API server accepted.
//
// A controller wedged on a refused write is the most important kind of
// "something is happening", and it used to be invisible to both halves of the
// measurement at once. The refusal changes no object, so no resourceVersion
// moves and the stream reports nothing; and the write never landed, so a
// success-only recorder had nothing to append either. A controller erroring
// forever therefore read as perfectly quiet. This is that shape reproduced
// small, and it is the property the wedged-shard pin in the transitions test
// depends on: without it that pin holds only because the same reconcile pass
// happens to make accepted writes earlier on.
func TestQuiescenceFailsOnARejectedWriteLoop(t *testing.T) {
	ns := shared.Namespace(t)

	// Patching an object that was never created is refused every time, for a
	// reason that has nothing to do with this suite's fakes, and it leaves the
	// namespace genuinely empty so there is nothing for the stream half to see.
	missing := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "never-created", Namespace: ns},
	}

	recorded := shared.Ops.For("quiescence-rejected", shared.Client)
	var (
		mu         sync.Mutex
		attempts   int
		rejections int
	)
	stop := writeLoop(50*time.Millisecond, func(int) {
		err := recorded.Patch(
			context.Background(),
			missing,
			client.RawPatch(types.MergePatchType, []byte(`{"data":{"tick":"1"}}`)),
		)
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if err != nil {
			rejections++
		}
	})
	defer stop()

	err := shared.TryQuiescent(t, ns, time.Second, 6*time.Second)
	stop()

	mu.Lock()
	defer mu.Unlock()
	// Asserted rather than assumed: a loop whose writes were being accepted
	// would be caught by the ordinary accepted-write path, and this test would
	// then prove nothing about the rejected one.
	if attempts == 0 || rejections != attempts {
		t.Fatalf("the loop must be refused every time to test anything: "+
			"%d of %d attempts rejected", rejections, attempts)
	}

	if err == nil {
		t.Fatal("a rejected write loop must not read as quiescent: it changes no " +
			"object, so passing here means the recorder is back to recording " +
			"only accepted writes and a wedged controller reads as quiet")
	}
	if !strings.Contains(err.Error(), "quiescence-rejected") {
		t.Fatalf("the report must name the writer, got:\n%v", err)
	}
	// The count and the marker both, since "12 attempted writes (12 rejected)"
	// is the line that tells a reader this is a wedge rather than a hot loop.
	for _, want := range []string{"rejected)", "(rejected: "} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the report must mark the writes as rejected, "+
				"want a line containing %q, got:\n%v", want, err)
		}
	}
}

// TestQuiescenceFailsOnAControllerWhoseLastPassErrored pins the level check. A
// reconciler that always errors goes quiet by itself once controller-runtime's
// backoff stretches past the window, so a purely rate-based quiescence check
// passes here. That is the defect this test exists to prevent, and it is
// exactly how the backup-PVC-shrink pin came to report a live defect as fixed.
//
// The wedged pass is driven straight through the interceptor rather than by
// registering a reconciler on the shared manager, for two reasons. It makes
// the silence exact instead of merely likely: an erroring controller only goes
// quiet once its backoff has stretched, so a test that waits for that is
// racing the very schedule it is asserting is unreliable. And a controller
// registered here would stay registered for the rest of the package, since
// nothing can unregister one from a running manager, leaving an
// always-erroring reconciler live under every later test's namespace. What is
// under test is what Unconverged concludes from the reconcile log, and that
// log has the same shape either way.
func TestQuiescenceFailsOnAControllerWhoseLastPassErrored(t *testing.T) {
	ns := shared.Namespace(t)
	key := client.ObjectKey{Namespace: ns, Name: "wedged-shard"}
	wedge := errors.New("persistentvolumeclaims: spec.resources.requests.storage: forbidden")

	stub := &stubReconciler{
		fn: func(context.Context, reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, wedge
		},
	}
	wrapped := shared.Reconciles.Wrap("quiescence-wedged", stub)
	if _, err := wrapped.Reconcile(
		t.Context(), reconcile.Request{NamespacedName: key},
	); err == nil {
		t.Fatal("the stub must error for this test to assert anything")
	}

	// The pass wrote nothing and changed nothing, so from here the namespace is
	// as silent as a wedge whose backoff has stretched past the window, which
	// is the state this check has to survive.
	err := shared.TryQuiescent(t, ns, time.Second, 10*time.Second)
	if err == nil {
		t.Fatal("a namespace whose last reconcile pass errored must not read as " +
			"quiescent: passing here means quiescence is a rate check again, and a " +
			"controller failing on a slow enough backoff reads as converged")
	}

	// Asserted rather than assumed: if the window half had fired, this test
	// would pass for the old reason and prove nothing about the level check.
	if strings.Contains(err.Error(), "never went quiet") {
		t.Fatalf("the namespace was not silent, so this test did not exercise the "+
			"level check at all, got:\n%v", err)
	}

	for _, want := range []string{"quiescence-wedged", key.String(), wedge.Error()} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the report must name the controller, the key and the error, "+
				"want a line containing %q, got:\n%v", want, err)
		}
	}
}

// TestQuiescenceIgnoresAControllerWhoseLastPassSucceeded pins the other
// direction: a controller that errored and then recovered is converged, and
// Unconverged must look at the last pass rather than at any pass.
func TestQuiescenceIgnoresAControllerWhoseLastPassSucceeded(t *testing.T) {
	ns := shared.Namespace(t)
	key := client.ObjectKey{Namespace: ns, Name: "recovered-shard"}

	var passes atomic.Int64
	stub := &stubReconciler{
		fn: func(context.Context, reconcile.Request) (reconcile.Result, error) {
			if passes.Add(1) == 1 {
				return reconcile.Result{}, errors.New("conflict: the object has been modified")
			}
			return reconcile.Result{}, nil
		},
	}
	wrapped := shared.Reconciles.Wrap("quiescence-recovered", stub)
	req := reconcile.Request{NamespacedName: key}

	if _, err := wrapped.Reconcile(t.Context(), req); err == nil {
		t.Fatal("the first pass must error, or the recovery below proves nothing")
	}
	// Checked here so that the nil below is the check changing its mind about
	// this key, rather than the check never having seen it.
	if got := shared.Unconverged(ns); len(got) != 1 {
		t.Fatalf("Unconverged after the failing pass = %v, want exactly one line", got)
	}

	if _, err := wrapped.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("the recovering pass: %v", err)
	}
	if got := shared.Unconverged(ns); got != nil {
		t.Errorf("Unconverged after the recovering pass = %v, want nil: the check "+
			"must read the last pass, not any pass", got)
	}

	if err := shared.TryQuiescent(t, ns, time.Second, 15*time.Second); err != nil {
		t.Errorf("a namespace whose controllers all last succeeded is quiescent:\n%v", err)
	}
}

// createProbeConfigMap makes an unowned ConfigMap for a churn loop to write to.
// Nothing owns it, so no reconciler reacts and the only activity in the
// namespace is the loop under test.
func createProbeConfigMap(t *testing.T, ns, name string) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string]string{"tick": "0"},
	}
	if err := shared.Client.Create(t.Context(), cm); err != nil {
		t.Fatalf("create probe ConfigMap %s/%s: %v", ns, name, err)
	}
	return cm
}

// writeLoop calls write every interval on its own goroutine until the returned
// stop is called, which joins it. Stopping on every path is the caller's job,
// since TestMain leak-checks with an empty ignore list. stop is idempotent so
// that a test can stop early to assert on the aftermath and still defer it.
func writeLoop(interval time.Duration, write func(i int)) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			case <-tick.C:
				write(i)
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		wg.Wait()
	}
}
