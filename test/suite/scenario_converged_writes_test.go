package suite

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// convergedRounds is how many passes of every controller each case measures.
const convergedRounds = 5

// TestConvergedPassesDoNotWrite settles a cluster, distributed and Lite, and
// then makes every controller reconcile its objects again, five times over,
// by touching an annotation on each object it reconciles. Nothing about the
// desired state changes, so no pass may write: no apply of a child, no status
// patch and no event. At 5000 clusters every write a converged pass makes is a
// request the API server authenticates, admits and diffs on every pass of
// every object, whether or not it changes anything.
func TestConvergedPassesDoNotWrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
	}{{"distributed", ""}, {"lite", multigresv1alpha1.ModeLite}} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCase(t)
			cluster := c.newCluster("quiet", func(s *MultigresClusterSpec) { s.Mode = tc.mode })
			c.WaitForClusterHealthy(cluster)
			c.RequireQuiescent(5*time.Second, 60*time.Second)

			controllers, touched := c.reconciledObjects()
			stop := c.captureEvents()
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
			defer cancel()
			since := time.Now()
			ops := c.WritesOverPasses(controllers, convergedRounds, 2*time.Minute,
				func(round int) {
					for _, obj := range touched {
						c.touch(ctx, obj, round)
					}
				})
			events := stop()
			// A pass that failed before reaching its writes would read as
			// quiet.
			for _, r := range Suite.Reconciles.InNamespace(c.NS) {
				if r.Start.After(since) && r.Err != nil {
					t.Errorf("a pass of %s over %s failed: %v", r.Controller, r.Request.Name, r.Err)
				}
			}
			t.Logf("%d writes over %d rounds of %v", len(ops), convergedRounds, controllers)
			if len(ops) > 0 {
				t.Errorf("converged passes wrote %d times:\n  %s", len(ops), opSummary(ops))
			}
			if len(events) > 0 {
				t.Errorf("converged passes recorded events:\n  %s", strings.Join(events, "\n  "))
			}
		})
	}
}

// reconciledObjects returns the controllers that reconcile something in the
// case's namespace, and the objects to touch so that each of them runs a
// pass: the objects they reconcile, and the Services they own. Most of the
// controllers ignore a change to their own object that leaves its generation
// alone, but none filters events from a child it owns, and every one but the
// TableGroup controller, which owns the Shards, owns a Service. Services are
// touched rather than workloads because the workload simulators restart a
// rollout on any change.
func (c *C) reconciledObjects() ([]string, []client.Object) {
	c.Helper()
	var controllers []string
	var touched []client.Object
	for _, k := range []struct {
		controller string
		list       client.ObjectList
	}{
		{"multigrescluster", &multigresv1alpha1.MultigresClusterList{}},
		{"tablegroup", &multigresv1alpha1.TableGroupList{}},
		{"shard", &multigresv1alpha1.ShardList{}},
		{"cell", &multigresv1alpha1.CellList{}},
		{"toposerver", &multigresv1alpha1.TopoServerList{}},
		{"", &corev1.ServiceList{}},
	} {
		c.NoError(c.List(k.list))
		items, err := apimeta.ExtractList(k.list)
		c.NoError(err)
		if len(items) > 0 && k.controller != "" {
			controllers = append(controllers, k.controller)
		}
		for _, item := range items {
			touched = append(touched, item.(client.Object))
		}
	}
	return controllers, touched
}

// touch sets a test annotation on obj, through the suite's own client, which
// the write log does not attribute to any controller. A metadata change
// reaches every watch on the object without changing anything a controller
// reads.
func (c *C) touch(ctx context.Context, obj client.Object, round int) {
	c.Helper()
	patch := client.RawPatch(
		"application/merge-patch+json",
		fmt.Appendf(nil, `{"metadata":{"annotations":{"test.multigres.com/touch":"%d"}}}`, round),
	)
	c.NoError(Suite.Client.Patch(ctx, obj, patch), "touch %s", obj.GetName())
}
