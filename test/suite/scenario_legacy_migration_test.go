package suite

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/testkit/managedfields"
)

const (
	// legacyOnlyAnnotation is a key no controller ever sends, so the only
	// manager that can own it is the one the test applies it under.
	legacyOnlyAnnotation = "test.multigres.com/legacy-only"
	// legacyBumpValue is written to the reconcile label of every object just
	// before the pause lifts. Only "disabled" gates anything, so any other value
	// is inert but still a change to the one label every controller's event
	// predicate admits.
	legacyBumpValue = "migration-bump"
	// legacyOnlyManager owns legacyOnlyAnnotation. It is a legacy name that
	// no entry is renamed to, so on a Shard the apiserver cannot merge its
	// Apply entry with a renamed one (entries are keyed by manager,
	// operation and subresource).
	legacyOnlyManager = "multigres-resource-handler-readiness"
)

// legacyNames are the pre-rename field managers, spelled out rather than read
// from ownership so that a regression in ownership.IsLegacy cannot hide itself
// from this scenario.
var legacyNames = []string{
	"multigres-operator",
	"multigres-resource-handler",
	"multigres-resource-handler-guard",
	"multigres-resource-handler-readiness",
	"multigres-operator-guard",
}

// legacyOf maps each manager a CR-kind controller now writes under to the name
// that wrote the same fields before the rename.
var legacyOf = map[string]string{
	ownership.ManagerCluster:         "multigres-operator",
	ownership.ManagerTableGroup:      "multigres-operator",
	ownership.ManagerCell:            "multigres-operator",
	ownership.ManagerTopoServer:      "multigres-operator",
	ownership.ManagerShard:           "multigres-resource-handler",
	ownership.ManagerShardGuard:      "multigres-resource-handler-guard",
	ownership.ManagerTopoServerGuard: "multigres-operator-guard",
}

// TestLegacyManagerMigration proves objects written before the field-manager
// rename shed their legacy entries on the first reconcile after upgrade, and
// that shedding never changes a value.
//
// The pre-upgrade state is built from the real controllers' own writes, with
// only the manager names rewritten: a hand-built managedFields entry would
// encode whatever the test believed the controllers claim.
func TestLegacyManagerMigration(t *testing.T) {
	c := newCase(t)
	cluster := c.MinimalCluster("legacy")
	c.WaitForClusterHealthy(cluster)
	c.RequireQuiescent(5*time.Second, 30*time.Second)

	// Hold the namespace so the rename is not undone by the controllers the
	// patches wake: a Shard has no event predicate, so renaming its entries
	// would otherwise trigger the very reconcile under test before the
	// scenario had asserted the legacy state exists. Activate lifts the pause
	// and the controllers' own retries then reconcile every object, which is
	// the first reconcile after "upgrade". Those retries exist only because
	// bumpAll queues a request per object while the pause is held; without it
	// nothing would be refused, so nothing would be retried.
	//
	// Pause does not wait for reconciles already admitted. After it returns no
	// request is admitted, and the quiescence wait guarantees any pass still
	// running has gone at least the quiet window without a write. It does not
	// count running passes, so a pass silent for longer than that can still
	// write (see Interceptor.Pause in testkit ctrltest).
	Suite.Reconciles.Pause(c.NS)
	c.RequireQuiescent(2*time.Second, 30*time.Second)

	before := c.snapshotSpecStatus()
	c.NotEmpty(before, "no objects to migrate")

	c.True(
		slices.Contains(legacyNames, legacyOnlyManager),
		"%s is not a legacy name, so dropping legacy managers would not touch it",
		legacyOnlyManager,
	)
	for _, l := range legacyOf {
		c.True(
			l != legacyOnlyManager,
			"%s is a rename target, so its entry would merge with the annotation's",
			legacyOnlyManager,
		)
	}

	shard := c.firstShard()
	c.applyLegacyOnlyAnnotation(shard)
	c.True(claimsLegacyOnly(c.getShard(shard), legacyOnlyManager),
		"%s does not claim %s after the apply", legacyOnlyManager, legacyOnlyAnnotation)

	statsBefore := c.managedStats()
	renamed := c.renameToLegacy()
	c.True(renamed > 0, "no operator manager entries were renamed")
	c.EqDiff(statsBefore, c.managedStats(),
		"renaming managers changed an object's entry count or claimed path count")
	c.True(claimsLegacyOnly(c.getShard(shard), legacyOnlyManager),
		"%s lost its claim on %s in the rename", legacyOnlyManager, legacyOnlyAnnotation)
	c.NotEmpty(c.objectsCarryingLegacy(),
		"no object carries a legacy manager after the rename, so the scenario would pass vacuously")

	liftedAt := time.Now()
	c.bumpAll()
	Suite.Reconciles.Activate(c.NS)

	c.Eventually(30*time.Second, "every legacy field manager entry to be dropped", func() error {
		if got := c.objectsCarryingLegacy(); len(got) > 0 {
			return fmt.Errorf("legacy managers remain: %s", strings.Join(got, "; "))
		}
		return nil
	})

	c.Eventually(30*time.Second, "status fields to be owned by their table owner", func() error {
		got, err := ownershipViolations(c.Context(), Suite.Client, c.NS)
		if err != nil {
			return err
		}
		if len(got) > 0 {
			return fmt.Errorf("ownership violations:\n  %s", strings.Join(got, "\n  "))
		}
		return nil
	})

	c.RequireQuiescent(5*time.Second, 30*time.Second)

	// A failed tidy-up must be logged, never returned: a stale resourceVersion
	// on the drop surfaces as a 422, and returning it would abort the
	// reconcile. That failure is rare here, so a Cell unit test pins the
	// behaviour and this only catches a regression that fails every time.
	for _, r := range Suite.Reconciles.InNamespace(c.NS) {
		if r.Start.After(liftedAt) {
			c.NoError(r.Err, "reconcile errored after the gate lifted: %s", r)
		}
	}

	t.Run("values never change", func(t *testing.T) {
		c := c.Sub(t)
		after := c.snapshotSpecStatus()
		c.EqDiff(before, after, "spec or status changed by dropping legacy managers")
	})

	t.Run("field only a legacy entry owns stays, unowned", func(t *testing.T) {
		c := c.Sub(t)
		got := c.getShard(shard)
		c.Eq("x", got.Annotations[legacyOnlyAnnotation],
			"annotation applied under the legacy manager was removed")
		c.Empty(managedfields.Owners(got, legacyOnlyPath),
			"managers still claim %s", legacyOnlyAnnotation)
	})
}

// crObjects lists every object of the five CR kinds in the namespace, sorted
// by kind then name.
func (c *C) crObjects() []client.Object {
	c.Helper()
	lists := []client.ObjectList{
		&multigresv1alpha1.MultigresClusterList{},
		&multigresv1alpha1.TableGroupList{},
		&multigresv1alpha1.CellList{},
		&multigresv1alpha1.TopoServerList{},
		&multigresv1alpha1.ShardList{},
	}
	var out []client.Object
	for _, l := range lists {
		c.NoError(c.List(l), "list %T", l)
		c.NoError(meta.EachListItem(l, func(o runtime.Object) error {
			obj, ok := o.(client.Object)
			if !ok {
				return fmt.Errorf("%T is not a client.Object", o)
			}
			out = append(out, obj)
			return nil
		}))
	}
	sort.Slice(out, func(i, j int) bool { return objKey(out[i]) < objKey(out[j]) })
	return out
}

func objKey(o client.Object) string {
	return fmt.Sprintf("%T/%s", o, o.GetName())
}

func (c *C) firstShard() *Shard {
	c.Helper()
	l := &ShardList{}
	c.NoError(c.List(l), "list shards")
	c.NotEmpty(l.Items, "no shards")
	return &l.Items[0]
}

// snapshotSpecStatus is every CR object's spec and status as generic JSON with
// lastTransitionTime removed, keyed by kind and name.
func (c *C) snapshotSpecStatus() map[string]map[string]any {
	c.Helper()
	out := map[string]map[string]any{}
	for _, o := range c.crObjects() {
		raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
		c.NoError(err, "convert %s", objKey(o))
		snap := map[string]any{}
		for _, f := range []string{"spec", "status"} {
			if v, ok := raw[f]; ok {
				snap[f] = v
			}
		}
		stripKey(snap, "lastTransitionTime")
		out[objKey(o)] = snap
	}
	return out
}

func stripKey(v any, key string) {
	switch t := v.(type) {
	case map[string]any:
		delete(t, key)
		for _, e := range t {
			stripKey(e, key)
		}
	case []any:
		for _, e := range t {
			stripKey(e, key)
		}
	}
}

// applyLegacyOnlyAnnotation applies an annotation under legacyOnlyManager. No
// controller sends it, so afterwards only that entry owns it.
func (c *C) applyLegacyOnlyAnnotation(shard *Shard) {
	c.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("Shard"))
	u.SetName(shard.Name)
	u.SetNamespace(shard.Namespace)
	u.SetAnnotations(map[string]string{legacyOnlyAnnotation: "x"})
	c.NoError(
		c.Patch(u, client.Apply, client.FieldOwner(legacyOnlyManager), client.ForceOwnership),
		"apply %s under the legacy manager", legacyOnlyAnnotation,
	)
}

// renameToLegacy rewrites every operator manager entry on every CR object to
// its pre-rename name and returns how many entries it rewrote. Each object is
// re-read and the rename retried on a lost race; a rename that already landed
// no longer matches on the retry, so it is neither redone nor recounted.
func (c *C) renameToLegacy() int {
	c.Helper()
	current := make([]string, 0, len(legacyOf))
	for m := range legacyOf {
		current = append(current, m)
	}
	slices.Sort(current)

	total := 0
	for _, o := range c.crObjects() {
		key := client.ObjectKeyFromObject(o)
		c.Eventually(15*time.Second, "rename managers on "+objKey(o), func() error {
			fresh := o.DeepCopyObject().(client.Object)
			if err := c.Get(key, fresh); err != nil {
				return err
			}
			for _, from := range current {
				n := 0
				for _, e := range fresh.GetManagedFields() {
					if e.Manager == from {
						n++
					}
				}
				err := managedfields.RenameManager(
					c.Context(), c.Client(), fresh, from, legacyOf[from],
				)
				if errors.Is(err, managedfields.ErrNoMatch) {
					continue
				}
				if err != nil {
					return err
				}
				total += n
			}
			return nil
		})
	}
	return total
}

// objectsCarryingLegacy lists, sorted, each "kind/name: manager" pair where a
// legacy manager still has an entry.
func (c *C) objectsCarryingLegacy() []string {
	c.Helper()
	var out []string
	for _, o := range c.crObjects() {
		for _, e := range o.GetManagedFields() {
			for _, l := range legacyNames {
				if e.Manager == l {
					out = append(out, objKey(o)+": "+e.Manager)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// bumpAll changes the reconcile label on every CR object while the namespace
// is paused. Pause only keeps alive a request that was already queued, and a
// settled object has none, so without a trigger the controllers would stay idle
// after Activate. The refused request is then retried until Activate admits it.
// Spec is never touched.
func (c *C) bumpAll() {
	c.Helper()
	for _, o := range c.crObjects() {
		key := client.ObjectKeyFromObject(o)
		c.Eventually(15*time.Second, "bump "+objKey(o), func() error {
			fresh := o.DeepCopyObject().(client.Object)
			if err := c.Get(key, fresh); err != nil {
				return err
			}
			patch := client.MergeFrom(fresh.DeepCopyObject().(client.Object))
			labels := fresh.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels[features.ReconcileLabel] = legacyBumpValue
			fresh.SetLabels(labels)
			return c.Patch(fresh, patch)
		})
	}
}

func (c *C) getShard(shard *Shard) *Shard {
	c.Helper()
	got := &Shard{}
	c.NoError(c.Get(client.ObjectKeyFromObject(shard), got), "get shard")
	return got
}

// legacyOnlyPath is how fieldpath renders a map key: as a field name, so the
// path is not .metadata.annotations["key"].
const legacyOnlyPath = ".metadata.annotations." + legacyOnlyAnnotation

// claimsLegacyOnly reports whether the named manager claims the legacy-only
// annotation.
func claimsLegacyOnly(o client.Object, manager string) bool {
	return slices.Contains(managedfields.Owners(o, legacyOnlyPath), manager)
}

// managedStats is, per object, its managedFields entry count and the total
// number of paths those entries claim.
func (c *C) managedStats() map[string][2]int {
	c.Helper()
	out := map[string][2]int{}
	for _, o := range c.crObjects() {
		paths := 0
		entries, err := managedfields.Entries(o)
		c.NoError(err, "decode managedFields of %s", objKey(o))
		for _, mf := range entries {
			paths += len(mf.Paths)
		}
		out[objKey(o)] = [2]int{len(entries), paths}
	}
	return out
}
