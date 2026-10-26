package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/multigres/multigres-operator/pkg/features"
)

// RequireNoWrites fails if obj's resourceVersion moves away from the value it
// holds on entry at any point during d. A gated object must get no writes at
// all (no status, no finalizer, no annotation), so an unchanged resourceVersion
// is the whole claim; checking for the absence of a condition would not notice a
// finalizer or a status field.
//
// obj must carry the resourceVersion returned by the caller's own last write,
// and reader must be uncached (mgr.GetAPIReader()): a cached read taken right
// after that write can still show the previous version and read as a spurious
// change.
//
// This is the assertion a ctrltest-style harness would offer as a primitive; it
// lives here until testkit grows one.
func RequireNoWrites(
	ctx context.Context,
	t testing.TB,
	reader client.Reader,
	obj client.Object,
	d time.Duration,
) {
	t.Helper()
	want := obj.GetResourceVersion()
	if want == "" {
		t.Fatalf("RequireNoWrites needs an object carrying a resourceVersion")
	}
	key := client.ObjectKeyFromObject(obj)
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		cur := obj.DeepCopyObject().(client.Object)
		if err := reader.Get(ctx, key, cur); err != nil {
			t.Fatalf("reading %s while asserting no writes: %v", key, err)
		}
		if got := cur.GetResourceVersion(); got != want {
			t.Fatalf("%s was written while gated: resourceVersion %s -> %s", key, want, got)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitForCachedLabel blocks until a read through c (the manager's cached client)
// shows label key with value want on obj's key, or absent when want is empty.
// The gate reads Namespace labels from the same cache, so a test that gates or
// un-gates a namespace and then acts must wait for the cache to agree first; the
// two informers are otherwise unordered and the action can race the label.
func WaitForCachedLabel(
	ctx context.Context,
	t testing.TB,
	c client.Client,
	obj client.Object,
	key, want string,
) {
	t.Helper()
	objKey := client.ObjectKeyFromObject(obj)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		cur := obj.DeepCopyObject().(client.Object)
		if err := c.Get(ctx, objKey, cur); err == nil && cur.GetLabels()[key] == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cache never showed label %s=%q on %s", key, want, objKey)
}

// NewWriteForbiddingClient returns a fake client seeded with objs that fails t
// on any write, status and subresource writes included, and passes reads
// through. It is how a unit test proves a gated Reconcile does nothing but read.
func NewWriteForbiddingClient(
	t testing.TB,
	scheme *runtime.Scheme,
	objs ...client.Object,
) client.Client {
	t.Helper()
	forbid := func(op string, obj client.Object) error {
		t.Errorf("gated reconcile wrote: %s %T %s", op, obj, client.ObjectKeyFromObject(obj))
		return fmt.Errorf("write forbidden: %s", op)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
				return forbid("create", obj)
			},
			Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
				return forbid("update", obj)
			},
			Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
				return forbid("patch", obj)
			},
			Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteOption) error {
				return forbid("delete", obj)
			},
			DeleteAllOf: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteAllOfOption) error {
				return forbid("deleteAllOf", obj)
			},
			SubResourceCreate: func(_ context.Context, _ client.Client, sub string, obj client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
				return forbid("subresource create "+sub, obj)
			},
			SubResourceUpdate: func(_ context.Context, _ client.Client, sub string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
				return forbid("subresource update "+sub, obj)
			},
			SubResourcePatch: func(_ context.Context, _ client.Client, sub string, obj client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
				return forbid("subresource patch "+sub, obj)
			},
		}).Build()
}

// RequireGatedReconcileIsNoop proves a gated object's Reconcile returns an empty
// Result and writes nothing, for each way an object can be gated. obj supplies
// only the kind; its metadata is set here. Reconcile is called directly, as a
// RequeueAfter scheduled before the label went on would. newR builds the
// reconciler under test around the write-forbidding client and the gate.
func RequireGatedReconcileIsNoop(
	t *testing.T,
	scheme *runtime.Scheme,
	obj client.Object,
	newR func(c client.Client, gate features.Gate) reconcile.Reconciler,
) {
	t.Helper()
	disabled := map[string]string{features.ReconcileLabel: features.Disabled}
	tests := map[string]struct {
		nsLabels  map[string]string
		objLabels map[string]string
		laptop    string
	}{
		"namespace gated":     {nsLabels: disabled},
		"object gated":        {objLabels: disabled},
		"laptop unclaimed ns": {laptop: "claimed"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", Labels: tc.nsLabels}}
			o := obj.DeepCopyObject().(client.Object)
			o.SetName("o")
			o.SetNamespace("ns")
			o.SetLabels(tc.objLabels)
			// The client fails the test on any write, status included.
			c := NewWriteForbiddingClient(t, scheme, ns, o)

			res, err := newR(c, features.NewGate(tc.laptop)).Reconcile(
				t.Context(),
				reconcile.Request{NamespacedName: client.ObjectKeyFromObject(o)},
			)
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if res != (reconcile.Result{}) {
				t.Fatalf("Reconcile() = %+v, want an empty Result", res)
			}
		})
	}
}

// CountingReconciler counts Reconcile calls per object key, so a test can
// observe the enqueue path (and its quiescence) without relying on side effects.
type CountingReconciler struct {
	delegate reconcile.Reconciler

	mu     sync.Mutex
	counts map[types.NamespacedName]int
}

// NewCountingReconciler wraps delegate.
func NewCountingReconciler(delegate reconcile.Reconciler) *CountingReconciler {
	return &CountingReconciler{delegate: delegate, counts: map[types.NamespacedName]int{}}
}

func (c *CountingReconciler) Reconcile(
	ctx context.Context,
	req reconcile.Request,
) (reconcile.Result, error) {
	c.mu.Lock()
	c.counts[req.NamespacedName]++
	c.mu.Unlock()
	return c.delegate.Reconcile(ctx, req)
}

// CountFor returns how many times key has been reconciled.
func (c *CountingReconciler) CountFor(key types.NamespacedName) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[key]
}

// WaitQuiescent blocks until the count for key stops climbing, so a test starts
// its next action from a settled controller.
func (c *CountingReconciler) WaitQuiescent(t testing.TB, key types.NamespacedName) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	last := c.CountFor(key)
	stable := 0
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		cur := c.CountFor(key)
		if cur != last {
			last, stable = cur, 0
			continue
		}
		if stable++; stable >= 3 {
			return
		}
	}
	t.Fatalf("reconciles for %s never quiesced (count=%d)", key, last)
}

// PatchLabel sets label k to v on obj with a merge patch, or removes it when v
// is empty. Only that label changes, so it drives a label-only transition; obj
// may be a Namespace.
func PatchLabel(t testing.TB, c client.Client, obj client.Object, k, v string) {
	t.Helper()
	ctx := t.Context()
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("reading %s to patch label %s: %v", client.ObjectKeyFromObject(obj), k, err)
	}
	base := obj.DeepCopyObject().(client.Object)
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if v == "" {
		delete(labels, k)
	} else {
		labels[k] = v
	}
	obj.SetLabels(labels)
	if err := c.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
		t.Fatalf("patching label %s on %s: %v", k, client.ObjectKeyFromObject(obj), err)
	}
}
