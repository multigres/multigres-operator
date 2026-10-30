package suite

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/testkit/ctrltest"
	"github.com/multigres/testkit/etcdsim"
	"github.com/multigres/testkit/managedfields"
)

// wipeManager is the field owner of the status wipe, so its managedFields
// entry is identifiable and can be checked for leftover claims.
const wipeManager = "status-wipe-test"

// wipeKinds are the five CR kinds in dependency order, roots first. The
// leaves-first pass walks it backwards.
var wipeKinds = []string{"MultigresCluster", "TableGroup", "Cell", "TopoServer", "Shard"}

// TestStatusWipe proves the classes in pkg/ownership: every status field of
// every CR is replaced with an empty one, and the cluster must reconverge with
// every field that is not WorkflowMemory back to its prior value.
//
//   - Observation: reconstructible from the world, so identical after the wipe.
//   - Gate: a gate whose input was wiped must still unblock, or the cluster
//     never returns to Healthy. The Eventually below is that liveness check.
//   - WorkflowMemory: may differ, but the loss must be safe. Nothing rolls and
//     the one-shot stamp is not re-issued while the cluster is converging.
//     EtcdMaintenance is exercised separately by TestStatusWipeEtcdMaintenance.
//
// Three passes over every kind, because the order decides which Gate reads an
// empty input: a parent wiped after its children sees them populated, a child
// wiped after its parent sees the parent's stale view, and under the namespace
// gate every reader sees every input empty at once. Two more wipe a single
// kind, which is the only way to leave a status-cleared predicate as the one
// thing that can wake the object.
func TestStatusWipe(t *testing.T) {
	// The four non-Shard controllers admit no status-only update, so without
	// the status-cleared predicate an ungated wipe strands them. The all-kinds
	// passes cannot pin the TableGroup and MultigresCluster predicates, because
	// a rebuilt Shard or Cell wakes both through Owns(); wiping each of them
	// alone leaves its predicate as the only trigger.
	passes := []struct {
		name  string
		order []string
		gated bool
	}{
		{"roots first", wipeKinds, false},
		{"leaves first", reversed(wipeKinds), false},
		{"all at once", wipeKinds, true},
		{"MultigresCluster alone", []string{"MultigresCluster"}, false},
		{"TableGroup alone", []string{"TableGroup"}, false},
	}
	for _, p := range passes {
		t.Run(p.name, func(t *testing.T) {
			c := newCase(t)
			cluster := c.MinimalCluster("wipe")
			c.WaitForClusterHealthy(cluster)
			c.RequireQuiescent(5*time.Second, 30*time.Second)

			before := c.statusSnapshot()
			c.NotEmpty(before, "no status to wipe")
			world := c.worldSnapshot()
			opsBefore := len(Suite.Ops.OpsInNamespace(c.NS))

			if p.gated {
				c.setNamespaceGate(true)
				c.RequireQuiescent(2*time.Second, 30*time.Second)
			}
			c.wipeStatus(p.order)
			if p.gated {
				c.setNamespaceGate(false)
			}

			// A timeout here is a Gate that never unblocks with its input
			// wiped, which is a defect in the controller, never in this wait.
			c.Eventually(
				60*time.Second,
				"cluster to reconverge to Healthy after the wipe",
				func() error {
					got := &MultigresCluster{}
					if err := c.Get(client.ObjectKeyFromObject(cluster), got); err != nil {
						return err
					}
					if got.Status.Phase != multigresv1alpha1.PhaseHealthy {
						return fmt.Errorf("phase is %q; %s", got.Status.Phase, c.phases())
					}
					return nil
				},
			)
			c.RequireQuiescent(5*time.Second, 30*time.Second)
			after := c.statusSnapshot()
			ops := Suite.Ops.OpsInNamespace(c.NS)[opsBefore:]

			t.Run("observations and gates return unchanged", func(t *testing.T) {
				c := c.Sub(t).Check()
				c.EqDiff(sortedKeys(before), sortedKeys(after), "set of objects with a status")
				for _, key := range sortedKeys(before) {
					c.compareStatus(key, before[key], after[key])
				}
			})

			t.Run("workflow memory loss is safe", func(t *testing.T) {
				c := c.Sub(t).Check()
				c.assertNothingRolled(world, ops)
				c.logWorkflowMemory(before, after, ops)
			})

			t.Run("wipe claims nothing under status", func(t *testing.T) {
				c := c.Sub(t).Check()
				for _, o := range c.crObjects() {
					entries, err := managedfields.Entries(o)
					c.NoError(err, "decode managedFields of %s", objKey(o))
					for _, e := range entries {
						if e.Manager != wipeManager {
							continue
						}
						for _, path := range e.Paths {
							c.False(
								underStatus(path),
								"%s: %s still claims %s",
								objKey(o),
								wipeManager,
								path,
							)
						}
					}
				}
			})
		})
	}
}

// setNamespaceGate sets or clears the production reconcile gate on the case's
// namespace. It deliberately gates through the production namespace label
// rather than the harness Pause: lifting the label makes the namespace watch
// enqueue every object, a production liveness path that Pause and Activate do
// not exercise. Moving the "all at once" pass onto Pause would silently stop
// covering it, since only the status-cleared predicates would wake the objects.
func (c *C) setNamespaceGate(on bool) {
	c.Helper()
	ns := &corev1.Namespace{}
	c.NoError(c.Client().Get(c.Context(), client.ObjectKey{Name: c.NS}, ns), "get namespace")
	patch := client.MergeFrom(ns.DeepCopy())
	if on {
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[features.ReconcileLabel] = features.Disabled
	} else {
		delete(ns.Labels, features.ReconcileLabel)
	}
	c.NoError(c.Client().Patch(c.Context(), ns, patch), "patch namespace gate")
}

// etcdMaintenanceRecord reads the global TopoServer's maintenance record, nil
// when it has none.
func (c *C) etcdMaintenanceRecord(name string) *multigresv1alpha1.EtcdMaintenanceStatus {
	c.Helper()
	ts := &multigresv1alpha1.TopoServer{}
	c.NoError(c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, ts), "get TopoServer")
	return ts.Status.EtcdMaintenance
}

// wipeTopoServerStatus replaces the TopoServer's status with an empty one.
func (c *C) wipeTopoServerStatus(name string) {
	c.Helper()
	c.Eventually(15*time.Second, "wipe status of TopoServer "+name, func() error {
		ts := &multigresv1alpha1.TopoServer{}
		if err := c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, ts); err != nil {
			return err
		}
		ts.Status = multigresv1alpha1.TopoServerStatus{}
		return c.Client().Status().Update(c.Context(), ts, client.FieldOwner(wipeManager))
	})
}

// TestStatusWipeEtcdMaintenance covers the one WorkflowMemory row the passes
// above leave out, TopoServer .status.etcdMaintenance, whose Readers text in
// pkg/ownership says a wiped record permits a new defragmentation at once and
// that maintenance does not resume mid-step.
func TestStatusWipeEtcdMaintenance(t *testing.T) {
	// defragmenting starts a case with maintenance armed: a cluster whose etcd
	// pods are old enough, a simulator behind the TopoServer, and the first
	// defragmentation held inside the simulator until released.
	type armed struct {
		c       *C
		cluster *MultigresCluster
		topo    string
		sim     *etcdsim.Cluster
		held    chan string
		release func()
		calls   *atomic.Int32
	}
	arm := func(t *testing.T, hold bool) *armed {
		c := newCase(t)
		cluster := c.MinimalCluster("wipe-etcd")
		topo := globalTopoName(cluster)
		a := &armed{c: c, cluster: cluster, topo: topo, calls: &atomic.Int32{}}
		a.sim = c.EtcdSim(topo, etcdMembers)
		a.held = make(chan string, 8)
		released := make(chan struct{})
		var once sync.Once
		a.release = func() { once.Do(func() { close(released) }) }
		// A failing test must never leave the controller's reconcile blocked
		// inside the simulator.
		t.Cleanup(a.release)
		a.sim.Hook("Defragment", func(endpoint string) {
			if a.calls.Add(1) == 1 && hold {
				a.held <- endpoint
				<-released
			}
		})
		c.WaitForClusterHealthy(cluster)
		c.ageEtcdPods(topo)
		c.enableEtcdDefragmentation(cluster)
		return a
	}

	t.Run("settled record wiped permits a new defragmentation at once", func(t *testing.T) {
		a := arm(t, false)
		c := a.c
		c.Eventually(30*time.Second, "first defragmentation to settle in status", func() error {
			if got := len(a.sim.DefragLog()); got != 1 {
				return fmt.Errorf("%d defragmentations, want 1", got)
			}
			rec := c.etcdMaintenanceRecord(a.topo)
			if rec == nil || rec.InProgress {
				return fmt.Errorf("record is %+v, want a settled one", rec)
			}
			return nil
		})
		first := a.sim.DefragLog()[0]
		// The record alone is what holds the next defragmentation back for an
		// hour: the other members are still reclaimable. No RequireQuiescent
		// here: the harness shortens the controller's hour-long requeue to
		// 50ms, so a TopoServer with maintenance enabled never goes quiet.
		time.Sleep(3 * time.Second)
		c.Eq(1, len(a.sim.DefragLog()), "defragmentations while the record is intact")

		c.wipeTopoServerStatus(a.topo)

		c.Eventually(30*time.Second, "a new defragmentation after the wipe", func() error {
			if got := len(a.sim.DefragLog()); got != 2 {
				return fmt.Errorf("%d defragmentations, want 2", got)
			}
			rec := c.etcdMaintenanceRecord(a.topo)
			if rec == nil || rec.InProgress {
				return fmt.Errorf("record is %+v, want a settled one", rec)
			}
			return nil
		})
		log := a.sim.DefragLog()
		// Regression guard only: etcdsim leaves a defragmented member with no
		// free bytes, so target selection cannot pick it again and no controller
		// mutation can fail this. It would catch a simulator that stops doing so.
		c.NotEq(first, log[1], "the wipe re-defragmented the member it had just done")
		c.Eq(log[1], c.etcdMaintenanceRecord(a.topo).Endpoint, "record names the second member")
	})

	t.Run("in-flight record wiped neither overlaps nor resumes", func(t *testing.T) {
		a := arm(t, true)
		c := a.c
		var target string
		select {
		case target = <-a.held:
		case <-time.After(60 * time.Second):
			c.Fail("no defragmentation reached the simulator")
			return
		}
		rec := c.etcdMaintenanceRecord(a.topo)
		c.NotNil(rec, "record while a defragmentation is in flight")
		c.True(rec.InProgress, "record reserved before the call")
		c.Eq(target, rec.Endpoint, "reservation names the member being defragmented")

		c.wipeTopoServerStatus(a.topo)

		// Regression guard only: controller-runtime never runs one key
		// concurrently, so the held call blocks the TopoServer's reconcile and
		// no controller mutation short of removing that serialisation can fail
		// this. It would catch a future concurrent-reconcile change.
		time.Sleep(5 * time.Second)
		c.Eq(int32(1), a.calls.Load(), "defragmentations started while one is in flight")

		a.release()
		c.Eventually(60*time.Second, "maintenance to settle after the release", func() error {
			if got := len(a.sim.DefragLog()); got != 2 {
				return fmt.Errorf("%d defragmentations, want 2", got)
			}
			rec := c.etcdMaintenanceRecord(a.topo)
			if rec == nil || rec.InProgress {
				return fmt.Errorf("record is %+v, want a settled one", rec)
			}
			return nil
		})
		// The wiped record permitting a new defragmentation at once is pinned by
		// the wait above. Hold well past the harness's compressed 50ms requeue,
		// which is ample for any further pass to start, and require exactly 2:
		// without the interval guard a third defragmentation begins within one
		// requeue of the second settling, a window one poll can land in.
		time.Sleep(3 * time.Second)
		c.Eq(2, len(a.sim.DefragLog()), "defragmentations after the new record settled")
		log := a.sim.DefragLog()
		// Regression guard only: etcdsim leaves a defragmented member with no
		// free bytes, so the step cannot be resumed on the same member whatever
		// the controller does. It would catch a simulator that stops doing so.
		seen := map[string]bool{}
		for _, ep := range log {
			c.False(
				seen[ep],
				"member %s defragmented twice (%v): the wiped step was resumed",
				ep,
				log,
			)
			seen[ep] = true
		}
		c.Eq(
			log[len(log)-1],
			c.etcdMaintenanceRecord(a.topo).Endpoint,
			"record names the last defragmented member",
		)
	})
}

// phases names every CR's phase and message, for a timeout's diagnosis.
func (c *C) phases() string {
	c.Helper()
	var out []string
	for key, st := range c.statusSnapshot() {
		out = append(
			out,
			fmt.Sprintf("%s phase=%v msg=%v", key, st[".status.phase"], st[".status.message"]),
		)
	}
	sort.Strings(out)
	return strings.Join(out, "; ")
}

func reversed(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[len(in)-1-i] = s
	}
	return out
}

func kindOfObject(o client.Object) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", o), "*v1alpha1.")
}

// wipeStatus replaces the status of each CR whose kind is in order with an
// empty one, kinds in the given order. Update replaces the subresource, so no other manager's field survives
// the way it would under an apply.
func (c *C) wipeStatus(order []string) {
	c.Helper()
	rank := map[string]int{}
	for i, k := range order {
		rank[k] = i
	}
	objs := c.crObjects()
	sort.SliceStable(objs, func(i, j int) bool {
		return rank[kindOfObject(objs[i])] < rank[kindOfObject(objs[j])]
	})
	for _, o := range objs {
		if _, ok := rank[kindOfObject(o)]; !ok {
			continue
		}
		key := client.ObjectKeyFromObject(o)
		c.Eventually(15*time.Second, "wipe status of "+objKey(o), func() error {
			fresh := o.DeepCopyObject().(client.Object)
			if err := c.Get(key, fresh); err != nil {
				return err
			}
			clearStatus(fresh)
			return c.Client().Status().Update(c.Context(), fresh, client.FieldOwner(wipeManager))
		})
	}
}

func clearStatus(o client.Object) {
	switch t := o.(type) {
	case *multigresv1alpha1.MultigresCluster:
		t.Status = multigresv1alpha1.MultigresClusterStatus{}
	case *multigresv1alpha1.TableGroup:
		t.Status = multigresv1alpha1.TableGroupStatus{}
	case *multigresv1alpha1.Cell:
		t.Status = multigresv1alpha1.CellStatus{}
	case *multigresv1alpha1.TopoServer:
		t.Status = multigresv1alpha1.TopoServerStatus{}
	case *multigresv1alpha1.Shard:
		t.Status = multigresv1alpha1.ShardStatus{}
	default:
		panic(fmt.Sprintf("clearStatus: unhandled %T", o))
	}
}

// statusSnapshot maps "kind/name" to each status field, flattened to the path
// strings pkg/ownership uses. observedGeneration and lastTransitionTime are
// dropped: a rewrite legitimately moves both.
func (c *C) statusSnapshot() map[string]map[string]any {
	c.Helper()
	out := map[string]map[string]any{}
	for _, o := range c.crObjects() {
		raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
		c.NoError(err, "convert %s", objKey(o))
		flat := map[string]any{}
		if st, ok := raw["status"].(map[string]any); ok {
			flattenStatus(".status", st, flat)
		}
		out[kindOfObject(o)+"/"+o.GetName()] = flat
	}
	return out
}

func flattenStatus(prefix string, m map[string]any, out map[string]any) {
	for k, v := range m {
		if k == "lastTransitionTime" || k == "observedGeneration" {
			continue
		}
		path := prefix + "." + k
		switch t := v.(type) {
		case map[string]any:
			flattenStatus(path, t, out)
		case []any:
			if k == "conditions" {
				for _, e := range t {
					cm, ok := e.(map[string]any)
					if !ok {
						continue
					}
					flattenStatus(fmt.Sprintf(`%s[type=%q]`, path, cm["type"]), cm, out)
				}
				continue
			}
			out[path] = t
		default:
			out[path] = v
		}
	}
}

// compareStatus asserts every field that is not WorkflowMemory came back equal.
func (c *C) compareStatus(key string, before, after map[string]any) {
	c.Helper()
	kind := key[:strings.Index(key, "/")]
	paths := map[string]bool{}
	for p := range before {
		paths[p] = true
	}
	for p := range after {
		paths[p] = true
	}
	sorted := make([]string, 0, len(paths))
	for p := range paths {
		sorted = append(sorted, p)
	}
	sort.Strings(sorted)
	for _, p := range sorted {
		row, ok := ownership.OwnerOf(kind, p)
		if !ok {
			c.Fail("%s %s has no ownership row", key, p)
			continue
		}
		if row.Class == ownership.WorkflowMemory {
			continue
		}
		b, bok := before[p]
		a, aok := after[p]
		c.True(bok == aok && fmt.Sprint(b) == fmt.Sprint(a),
			"%s %s (%s) before=%v after=%v", key, p, row.Class, b, a)
	}
}

// world is what a rollout would change: pool pod identities and the
// generations of the Deployments and StatefulSets the operator owns.
type world struct {
	pods        map[string]types.UID
	generations map[string]int64
}

func (c *C) worldSnapshot() world {
	c.Helper()
	w := world{pods: map[string]types.UID{}, generations: map[string]int64{}}
	pods := &corev1.PodList{}
	c.NoError(c.List(pods, client.InNamespace(c.NS)), "list pods")
	for _, p := range pods.Items {
		w.pods[p.Name] = p.UID
	}
	deps := &appsv1.DeploymentList{}
	c.NoError(c.List(deps, client.InNamespace(c.NS)), "list deployments")
	for _, d := range deps.Items {
		w.generations["Deployment/"+d.Name] = d.Generation
	}
	sts := &appsv1.StatefulSetList{}
	c.NoError(c.List(sts, client.InNamespace(c.NS)), "list statefulsets")
	for _, s := range sts.Items {
		w.generations["StatefulSet/"+s.Name] = s.Generation
	}
	return w
}

// assertNothingRolled is the safe outcome for Images, PostgresConfig and every
// other WorkflowMemory field: losing the record must not restart anything.
func (c *C) assertNothingRolled(before world, ops []ctrltest.Op) {
	c.Helper()
	after := c.worldSnapshot()
	c.EqDiff(before.pods, after.pods, "pool pods were replaced by the wipe")
	c.EqDiff(before.generations, after.generations, "a workload was re-templated by the wipe")
	for _, op := range ops {
		if op.Verb == "delete" &&
			(op.Kind == "Pod" || op.Kind == "StatefulSet" || op.Kind == "Deployment") {
			c.Fail("wipe caused %s", op)
		}
	}
}

// logWorkflowMemory records, per WorkflowMemory field, what it was and what it
// became, so the table's Readers text can be checked against behaviour.
func (c *C) logWorkflowMemory(before, after map[string]map[string]any, ops []ctrltest.Op) {
	c.Helper()
	for _, key := range sortedKeys(before) {
		kind := key[:strings.Index(key, "/")]
		paths := map[string]bool{}
		for p := range before[key] {
			paths[p] = true
		}
		for p := range after[key] {
			paths[p] = true
		}
		sorted := make([]string, 0, len(paths))
		for p := range paths {
			sorted = append(sorted, p)
		}
		sort.Strings(sorted)
		for _, p := range sorted {
			row, ok := ownership.OwnerOf(kind, p)
			if !ok || row.Class != ownership.WorkflowMemory {
				continue
			}
			c.Logf(
				"WorkflowMemory %s %s: before=%v after=%v",
				key,
				p,
				before[key][p],
				after[key][p],
			)
		}
	}
	for _, op := range ops {
		if op.Kind == "Pod" || op.Kind == "StatefulSet" || op.Kind == "Deployment" {
			c.Logf("op after wipe: %s", op)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
