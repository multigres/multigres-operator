package suite

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// cacheLag stands in for informer caches that have not yet observed one
// TableGroup's objects. While armed for a namespace and logical TableGroup, a
// List through a wrapped client leaves out every object of the wrapped kind
// labelled with that TableGroup, as a cache that has not yet seen their
// creation would answer. Reads through the API server are never affected,
// which is the distinction a drain depends on: a cached list of zero cannot
// tell "none exist" from "none observed yet".
//
// It hides the objects outright rather than only recent ones, and for as long
// as the test keeps it armed rather than for a propagation delay. That makes
// the window deterministic instead of sampled, and a read that must be
// authoritative is wrong under it whatever the window's length.
//
// Testkit gap: ctrltest models pod state (FaultPods) and patch errors
// (InvalidPatch) but has no stale-read fault for a controller's cached
// client. This wrapper can go once it does.
type cacheLag struct {
	mu    sync.Mutex
	armed map[lagKey]bool
}

type lagKey struct{ namespace, tableGroup string }

// lag is the one cacheLag every wrapped controller client reads through.
// Suite-wide like the manager it is wired into; scoped by Arm.
var lag = &cacheLag{}

// Arm hides tableGroup's objects in ns from every wrapped client until the
// returned function is called.
func (l *cacheLag) Arm(ns, tableGroup string) func() {
	k := lagKey{ns, tableGroup}
	l.mu.Lock()
	if l.armed == nil {
		l.armed = map[lagKey]bool{}
	}
	l.armed[k] = true
	l.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			delete(l.armed, k)
			l.mu.Unlock()
		})
	}
}

func (l *cacheLag) hides(ns string, labels map[string]string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.armed[lagKey{ns, labels[metadata.LabelMultigresTableGroup]}]
}

// wrap returns c with lists of kind (a list type such as &ShardList{})
// subject to the lag. Other kinds pass through untouched.
func (l *cacheLag) wrap(c client.Client, kind client.ObjectList) client.Client {
	return &laggedClient{Client: c, lag: l, kind: reflect.TypeOf(kind)}
}

type laggedClient struct {
	client.Client
	lag  *cacheLag
	kind reflect.Type
}

func (c *laggedClient) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if reflect.TypeOf(list) != c.kind {
		return nil
	}
	items, err := apimeta.ExtractList(list)
	if err != nil {
		return fmt.Errorf("cache lag: %w", err)
	}
	kept := make([]runtime.Object, 0, len(items))
	for _, item := range items {
		obj, ok := item.(client.Object)
		if ok && c.lag.hides(obj.GetNamespace(), obj.GetLabels()) {
			continue
		}
		kept = append(kept, item)
	}
	return apimeta.SetList(list, kept)
}
