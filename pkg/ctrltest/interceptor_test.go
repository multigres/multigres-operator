package ctrltest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// stubReconciler stands in for a controller at the reconcile boundary: it
// records what it was asked and returns what the test told it to.
type stubReconciler struct {
	fn func(ctx context.Context, req reconcile.Request) (reconcile.Result, error)

	mu    sync.Mutex
	calls []reconcile.Request
}

func (s *stubReconciler) Reconcile(
	ctx context.Context,
	req reconcile.Request,
) (reconcile.Result, error) {
	s.mu.Lock()
	s.calls = append(s.calls, req)
	s.mu.Unlock()
	if s.fn == nil {
		return reconcile.Result{}, nil
	}
	return s.fn(ctx, req)
}

func (s *stubReconciler) called() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func request(ns, name string) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}
}

// TestInterceptorGateDefaultsToRefusingEverything is the assertion behind the
// claim that outside a test nothing reconciles: a fresh interceptor has an
// empty active set, and an empty set is a sentinel that matches nothing rather
// than a filter that is off.
func TestInterceptorGateDefaultsToRefusingEverything(t *testing.T) {
	stub := &stubReconciler{}
	ic := NewInterceptor(nil, RequeueClamp)
	wrapped := ic.Wrap("shard", stub)

	res, err := wrapped.Reconcile(t.Context(), request("ns1", "a"))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("Result = %+v, want the zero result", res)
	}
	if stub.called() != 0 {
		t.Errorf("the real reconciler ran %d times, want 0", stub.called())
	}

	// The refusal is recorded, because "the suite declined" and "the
	// controller never woke up" look identical from outside and are very
	// different diagnoses.
	log := ic.All()
	if len(log) != 1 {
		t.Fatalf("recorded %d passes, want 1", len(log))
	}
	if !log[0].Gated {
		t.Errorf("pass %v is not marked gated", log[0])
	}
	if log[0].Seq != 0 {
		t.Errorf("Seq = %d, want 0: a gated request is not a reconcile", log[0].Seq)
	}
}

func TestInterceptorGateAdmitsAndRefusesByNamespace(t *testing.T) {
	stub := &stubReconciler{}
	ic := NewInterceptor(nil, RequeueClamp)
	wrapped := ic.Wrap("shard", stub)

	ic.Activate("ns1")
	if !ic.Active("ns1") {
		t.Fatal("Active(ns1) = false after Activate")
	}
	if ic.Active("ns2") {
		t.Error("Active(ns2) = true, want only the activated namespace admitted")
	}
	// A cluster-scoped request carries no namespace and must not slip through
	// the gate just because its namespace is the zero value.
	if ic.Active("") {
		t.Error(`Active("") = true, want cluster-scoped requests refused`)
	}

	if _, err := wrapped.Reconcile(t.Context(), request("ns1", "a")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, err := wrapped.Reconcile(t.Context(), request("ns2", "a")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if stub.called() != 1 {
		t.Fatalf("the real reconciler ran %d times, want 1", stub.called())
	}

	ic.Deactivate("ns1")
	if _, err := wrapped.Reconcile(t.Context(), request("ns1", "a")); err != nil {
		t.Fatalf("Reconcile after Deactivate: %v", err)
	}
	if stub.called() != 1 {
		t.Errorf("the real reconciler ran %d times after Deactivate, want 1", stub.called())
	}
}

// TestInterceptorCompressesRequeue pins both halves of the bargain: the wait
// shrinks, and what was asked for survives. A clamp that dropped the original
// would buy speed by destroying the evidence of why the suite used to be slow.
func TestInterceptorCompressesRequeue(t *testing.T) {
	const clamp = 50 * time.Millisecond

	cases := []struct {
		name           string
		clamp          time.Duration
		asked          time.Duration
		wantReturned   time.Duration
		wantCompressed bool
	}{
		{name: "no requeue", clamp: clamp, asked: 0, wantReturned: 0},
		{
			name:         "already shorter than the clamp",
			clamp:        clamp,
			asked:        10 * time.Millisecond,
			wantReturned: 10 * time.Millisecond,
		},
		{
			name:         "exactly the clamp",
			clamp:        clamp,
			asked:        clamp,
			wantReturned: clamp,
		},
		{
			name:           "the shard controller's minute",
			clamp:          clamp,
			asked:          time.Minute,
			wantReturned:   clamp,
			wantCompressed: true,
		},
		// A zero clamp is off, not a clamp of zero. It is how the effect of
		// compression is isolated when somebody wants to measure what the
		// suite would cost without it, so a ceiling of zero seconds would
		// turn that experiment into a hot loop rather than a control.
		{
			name:         "a zero clamp disables compression",
			clamp:        0,
			asked:        time.Minute,
			wantReturned: time.Minute,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubReconciler{
				fn: func(context.Context, reconcile.Request) (reconcile.Result, error) {
					return reconcile.Result{RequeueAfter: tc.asked}, nil
				},
			}
			ic := NewInterceptor(nil, tc.clamp)
			ic.Activate("ns1")

			res, err := ic.Wrap("shard", stub).Reconcile(t.Context(), request("ns1", "a"))
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if res.RequeueAfter != tc.wantReturned {
				t.Errorf(
					"controller-runtime was told RequeueAfter = %s, want %s",
					res.RequeueAfter, tc.wantReturned,
				)
			}

			log := ic.All()
			if len(log) != 1 {
				t.Fatalf("recorded %d passes, want 1", len(log))
			}
			if log[0].RequestedAfter != tc.asked {
				t.Errorf(
					"RequestedAfter = %s, want the original %s",
					log[0].RequestedAfter, tc.asked,
				)
			}
			if log[0].Compressed() != tc.wantCompressed {
				t.Errorf("Compressed() = %v, want %v", log[0].Compressed(), tc.wantCompressed)
			}
		})
	}
}

func TestInterceptorRecordsResultAndError(t *testing.T) {
	wantErr := errors.New("boom")
	stub := &stubReconciler{
		fn: func(context.Context, reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{RequeueAfter: time.Hour}, wantErr
		},
	}
	ic := NewInterceptor(nil, RequeueClamp)
	ic.Activate("ns1")

	res, err := ic.Wrap("cell", stub).Reconcile(t.Context(), request("ns1", "a"))
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want it passed through unchanged", err)
	}
	// An error result is still compressed. controller-runtime ignores
	// RequeueAfter when an error is returned, but a controller that sets both
	// should not be the one case where the suite waits.
	if res.RequeueAfter != RequeueClamp {
		t.Errorf("RequeueAfter = %s, want %s", res.RequeueAfter, RequeueClamp)
	}

	log := ic.All()
	if len(log) != 1 {
		t.Fatalf("recorded %d passes, want 1", len(log))
	}
	if !errors.Is(log[0].Err, wantErr) {
		t.Errorf("recorded Err = %v, want %v", log[0].Err, wantErr)
	}
	if log[0].Seq != 1 {
		t.Errorf("Seq = %d, want 1", log[0].Seq)
	}
	if log[0].Duration() < 0 {
		t.Errorf("Duration() = %s, want a non-negative interval", log[0].Duration())
	}
}

// TestInterceptorGroupsOpsByReconcile covers the grouping job: a pass's writes
// are identifiable even though another controller wrote in the middle of it.
func TestInterceptorGroupsOpsByReconcile(t *testing.T) {
	rec := NewRecorder()
	base := fake.NewClientBuilder().Build()
	shardClient := rec.For("shard", base)
	cellClient := rec.For("cell", base)

	ic := NewInterceptor(rec, RequeueClamp)
	ic.Activate("ns1")

	stub := &stubReconciler{
		fn: func(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
			if err := shardClient.Create(ctx, configMap("ns1", "first")); err != nil {
				return reconcile.Result{}, err
			}
			// Another controller writing inside this pass's window is the
			// normal shape in this suite, and the grouping has to exclude it
			// by name rather than by position.
			if err := cellClient.Create(ctx, configMap("ns1", "interleaved")); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, shardClient.Create(ctx, configMap("ns1", "second"))
		},
	}

	// A write before the pass and one after it, to prove the range is bounded
	// at both ends rather than being "everything this controller ever wrote".
	if err := shardClient.Create(t.Context(), configMap("ns1", "before")); err != nil {
		t.Fatalf("create before: %v", err)
	}
	if _, err := ic.Wrap("shard", stub).Reconcile(t.Context(), request("ns1", "a")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := shardClient.Create(t.Context(), configMap("ns1", "after")); err != nil {
		t.Fatalf("create after: %v", err)
	}

	log := ic.All()
	if len(log) != 1 {
		t.Fatalf("recorded %d passes, want 1", len(log))
	}
	var names []string
	for _, op := range ic.Ops(log[0]) {
		names = append(names, op.Key.Name)
	}
	want := []string{"first", "second"}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Errorf("Ops named %v, want %v", names, want)
	}
}

// TestInterceptorConcurrentReconciles is the -race case, and it mirrors the
// suite's real concurrency: five controllers reconciling at once, each one
// sequential in itself. That is what makes the op-range grouping sound, so this
// asserts the grouping under concurrency rather than just the absence of a data
// race.
func TestInterceptorConcurrentReconciles(t *testing.T) {
	const perController = 25
	controllers := []string{"multigrescluster", "tablegroup", "cell", "toposerver", "shard"}

	rec := NewRecorder()
	base := fake.NewClientBuilder().Build()
	ic := NewInterceptor(rec, RequeueClamp)
	ic.Activate("ns1")
	// Left inactive so the gate is exercised concurrently with the admitted
	// path rather than in a quiet test of its own.
	const refusedNS = "ns-refused"

	var wg sync.WaitGroup
	for _, name := range controllers {
		wrapped := rec.For(name, base)
		stub := &stubReconciler{
			fn: func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
				return reconcile.Result{RequeueAfter: time.Minute},
					wrapped.Create(ctx, configMap(req.Namespace, req.Name))
			},
		}
		r := ic.Wrap(name, stub)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perController {
				res, err := r.Reconcile(context.Background(),
					request("ns1", fmt.Sprintf("%s-%d", name, i)))
				if err != nil {
					t.Errorf("%s reconcile %d: %v", name, i, err)
					return
				}
				if res.RequeueAfter != RequeueClamp {
					t.Errorf("%s reconcile %d: RequeueAfter = %s, want %s",
						name, i, res.RequeueAfter, RequeueClamp)
					return
				}
				if _, err := r.Reconcile(context.Background(),
					request(refusedNS, "ignored")); err != nil {
					t.Errorf("%s gated reconcile %d: %v", name, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	admitted := map[string][]Reconcile{}
	for _, r := range ic.All() {
		if r.Gated {
			if r.Request.Namespace != refusedNS {
				t.Errorf("pass %v is gated but names an active namespace", r)
			}
			continue
		}
		admitted[r.Controller] = append(admitted[r.Controller], r)
	}
	for _, name := range controllers {
		passes := admitted[name]
		if len(passes) != perController {
			t.Errorf("%s made %d admitted passes, want %d", name, len(passes), perController)
			continue
		}
		for i, r := range passes {
			if r.Seq != i+1 {
				t.Errorf("%s pass %d has Seq %d, want %d", name, i, r.Seq, i+1)
			}
			ops := ic.Ops(r)
			if len(ops) != 1 {
				t.Errorf("%s pass %d produced %d ops, want exactly its own write",
					name, r.Seq, len(ops))
				continue
			}
			if ops[0].Key.Name != r.Request.Name {
				t.Errorf("%s pass %d is credited with %s, want %s",
					name, r.Seq, ops[0].Key.Name, r.Request.Name)
			}
		}
	}
}

func TestInterceptorWaitForRequeue(t *testing.T) {
	ic := NewInterceptor(nil, RequeueClamp)
	ic.Activate("ns1")
	key := client.ObjectKey{Namespace: "ns1", Name: "a"}

	if _, ok := ic.findRequeue("shard", key, time.Minute); ok {
		t.Fatal("findRequeue found a requeue before anything reconciled")
	}

	stub := &stubReconciler{
		fn: func(context.Context, reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{RequeueAfter: time.Minute}, nil
		},
	}
	if _, err := ic.Wrap("shard", stub).Reconcile(t.Context(), request("ns1", "a")); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := ic.WaitForRequeue(t, "shard", key, time.Minute, time.Second)
	if got.RequestedAfter != time.Minute {
		t.Errorf("RequestedAfter = %s, want 1m", got.RequestedAfter)
	}

	// Scoped by key, so another object's identical requeue cannot satisfy it.
	other := client.ObjectKey{Namespace: "ns1", Name: "b"}
	if _, ok := ic.findRequeue("shard", other, time.Minute); ok {
		t.Error("findRequeue matched a requeue against a different object")
	}
	// And scoped by controller.
	if _, ok := ic.findRequeue("cell", key, time.Minute); ok {
		t.Error("findRequeue matched a requeue by a different controller")
	}
}
