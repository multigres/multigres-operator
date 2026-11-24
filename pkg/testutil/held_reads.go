package testutil

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HeldReads serves Gets of held objects from the copy Hold took, as an
// informer cache that has stopped receiving events for them would, and passes
// everything else to Client. It is how a test makes a controller's cache lag
// the API server deterministically.
//
// Testkit gap: ctrltest has no stale-read fault for a controller's cached
// client; this can go once it does.
type HeldReads struct {
	client.Client

	mu   sync.Mutex
	held map[client.ObjectKey]client.Object
}

// Hold reads obj, by its name and namespace, through Client and serves that
// copy for every later Get of an object of its type and key until Release.
func (h *HeldReads) Hold(t testing.TB, obj client.Object) {
	t.Helper()
	key := client.ObjectKeyFromObject(obj)
	if err := h.Client.Get(t.Context(), key, obj); err != nil {
		t.Fatalf("hold %s: %v", key, err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.held == nil {
		h.held = map[client.ObjectKey]client.Object{}
	}
	h.held[key] = obj.DeepCopyObject().(client.Object)
}

// Release stops serving the held copy of key.
func (h *HeldReads) Release(key client.ObjectKey) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.held, key)
}

func (h *HeldReads) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	h.mu.Lock()
	held, ok := h.held[key]
	h.mu.Unlock()
	if ok && reflect.TypeOf(held) == reflect.TypeOf(obj) {
		reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(held.DeepCopyObject()).Elem())
		return nil
	}
	return h.Client.Get(ctx, key, obj, opts...)
}
