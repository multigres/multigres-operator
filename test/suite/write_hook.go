package suite

import (
	"context"
	"sync"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PatchHook runs inside the shard controller's client, before a Patch of an
// object in the hook's namespace is sent. It runs on the reconcile goroutine,
// so whatever it writes commits strictly between the reconciler's read and
// its write.
type PatchHook func(ctx context.Context, obj client.Object, patch client.Patch)

// patchHooks maps namespace to the PatchHook a test installed there.
var patchHooks sync.Map

// hookedClient lets a test commit a write between a reconcile's read and its
// patch, the one interleaving the suite cannot otherwise force: a second
// writer racing the reconciler wins only by chance.
//
// This belongs in testkit (multigres/testkit ctrltest has fault injection for
// Patch errors, InvalidPatch, but no way to run a write of the test's own at
// that point); delete this once it has one.
type hookedClient struct {
	client.Client
}

func (h hookedClient) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	if hook, ok := patchHooks.Load(obj.GetNamespace()); ok {
		hook.(PatchHook)(ctx, obj, patch)
	}
	return h.Client.Patch(ctx, obj, patch, opts...)
}

// BeforeShardPatch installs hook for the case's namespace until the test ends.
func (c *C) BeforeShardPatch(t *testing.T, hook PatchHook) {
	patchHooks.Store(c.NS, hook)
	t.Cleanup(func() { patchHooks.Delete(c.NS) })
}
