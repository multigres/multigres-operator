package suite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/features"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/testkit/managedfields"
)

const (
	// legacyOnlyAnnotation is a key no controller ever sends, so the only
	// manager that can own it is the one the test applies it under.
	legacyOnlyAnnotation = "test.multigres.com/legacy-only"
	// legacyBumpValue, with the round appended, is written to the reconcile
	// label of every object just before the pause lifts. Only "disabled"
	// gates anything, so any other value is inert but still a change to the
	// one label every controller's event predicate admits.
	legacyBumpValue = "migration-bump"
	// legacyOnlyManager owns legacyOnlyAnnotation. It is a legacy name that
	// no entry is renamed to, so on a Shard the apiserver cannot merge its
	// Apply entry with a renamed one (entries are keyed by manager,
	// operation and subresource).
	legacyOnlyManager = "multigres-resource-handler-readiness"
)

// TestLegacyManagerMigration proves objects whose status was written under the
// retired shared names shed every legacy status entry a current writer covers
// on the first reconcile, keep the ones that are a field's only owner, keep the
// child writer's live entries, and never change a value.
//
// The legacy state is built from the real controllers' own writes, with only
// the status manager names rewritten: a hand-built managedFields entry would
// encode whatever the test believed the controllers claim. The child writer's
// main-resource entries need no rewriting, since that name is live.

// legacyNames are the retired shared status names,
// spelled out rather than read from ownership so that a regression in
// ownership.IsLegacyEntry cannot hide itself from this scenario.
var legacyNames = []string{
	"multigres-operator",
	"multigres-resource-handler",
	"multigres-resource-handler-guard",
	"multigres-resource-handler-readiness",
	"multigres-operator-guard",
}

// childWriter is the name every child object and child CR spec is applied
// under. It is legacy only on status.
const childWriter = "multigres-operator"

// isLegacyEntry is this scenario's own statement of which entries the upgrade
// must retire: any entry of a retired facet name, and the shared names only
// on status. The shared names stay live on the main resource.
func isLegacyEntry(e metav1.ManagedFieldsEntry) bool {
	if !slices.Contains(legacyNames, e.Manager) {
		return false
	}
	switch e.Manager {
	case childWriter, "multigres-resource-handler":
		return e.Subresource == "status"
	}
	return true
}

// legacyOf maps each status manager to the retired shared name for the same
// status fields.
var legacyOf = map[string]string{
	ownership.ManagerCluster:         "multigres-operator",
	ownership.ManagerTableGroup:      "multigres-operator",
	ownership.ManagerCell:            "multigres-operator",
	ownership.ManagerTopoServer:      "multigres-operator",
	ownership.ManagerShard:           "multigres-resource-handler",
	ownership.ManagerShardGuard:      "multigres-resource-handler-guard",
	ownership.ManagerTopoServerGuard: "multigres-operator-guard",
}

func TestLegacyManagerMigration(t *testing.T) {
	c := newCase(t)
	c.seedsLegacy()
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

	// An operator version that wrote status with Status().Update and no field
	// owner leaves this entry on a MultigresCluster. The tidy-up must leave it
	// alone, and the docs give the procedure that clears it by hand, which the
	// cleanup runs (cleanups run last-registered first, so before the case's
	// ownership check).
	implicit := c.seedImplicitManagerEntry(c.getCluster(cluster))
	t.Cleanup(func() { c.removeImplicitManagerEntry(cluster) })

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

	// Round 0 is the upgrade: every status entry the controllers wrote is
	// renamed to the retired shared name for the same fields, which is the
	// state of an object only an operator writing under those names wrote. Round 1 is a
	// rollback and the upgrade after it: an operator writing under the shared
	// names writes status again, through the API, while the current entries
	// stay.
	for round := range 2 {
		if round == 0 {
			c.True(c.renameToLegacy() > 0, "no operator manager entries were renamed")
		} else {
			Suite.Reconciles.Pause(c.NS)
			c.RequireQuiescent(2*time.Second, 30*time.Second)
			c.True(c.writeAsSharedNames() > 0, "the shared-name writer wrote nothing")
			c.EqDiff(before, c.snapshotSpecStatus(), "the shared-name writes changed a value")
			for _, o := range c.crObjects() {
				entries := o.GetManagedFields()
				c.True(slices.ContainsFunc(entries, isLegacyEntry) &&
					slices.ContainsFunc(entries, func(e metav1.ManagedFieldsEntry) bool {
						return slices.Contains(ownership.Managers(), e.Manager)
					}),
					"%s does not carry legacy and current status entries side by side: %+v",
					objKey(o), entries)
			}
		}
		c.True(claimsLegacyOnly(c.getShard(shard), legacyOnlyManager),
			"%s lost its claim on %s", legacyOnlyManager, legacyOnlyAnnotation)
		c.NotEmpty(
			c.objectsCarryingLegacy(),
			"no object carries a legacy manager, so the scenario would pass vacuously",
		)

		liftedAt := time.Now()
		c.bumpAll(fmt.Sprintf("%s-%d", legacyBumpValue, round))
		Suite.Reconciles.Activate(c.NS)

		// Every status writer re-applies its whole status each pass, which
		// covers every legacy status entry, so only the legacy-only
		// annotation's entry may stay: nothing current claims the annotation.
		c.Eventually(
			30*time.Second,
			"status fields to be owned by their table owner",
			func() error {
				got, checked, err := checkOwnership(c.Context(), Suite.Client, c.NS, keptLegacy)
				if err != nil {
					return err
				}
				if checked == 0 {
					return errors.New("no CR objects in the namespace: nothing was checked")
				}
				got = slices.DeleteFunc(got, isImplicitManagerLeftover)
				if len(got) > 0 {
					return fmt.Errorf("ownership violations:\n  %s", strings.Join(got, "\n  "))
				}
				keep := objKey(shard) + ": " + legacyOnlyManager + " on main"
				if left := c.objectsCarryingLegacy(); !slices.Equal(left, []string{keep}) {
					return fmt.Errorf("legacy entries left: %s; want only %s",
						strings.Join(left, "; "), keep)
				}
				return nil
			},
		)
		c.True(claimsLegacyOnly(c.getShard(shard), legacyOnlyManager),
			"%s, the only owner of %s, was removed", legacyOnlyManager, legacyOnlyAnnotation)

		// The child writer's main-resource entry on each child CR is live: the
		// tidy-up after a status apply must never remove it, even though the same
		// name's status entry on that object was legacy and is gone.
		for _, o := range c.crObjects() {
			if _, ok := o.(*MultigresCluster); ok {
				continue
			}
			c.True(
				slices.ContainsFunc(o.GetManagedFields(), func(e metav1.ManagedFieldsEntry) bool {
					return e.Manager == childWriter && e.Subresource == ""
				}),
				"%s lost the child writer's main-resource entry: %+v",
				objKey(o),
				o.GetManagedFields(),
			)
		}

		c.RequireQuiescent(5*time.Second, 30*time.Second)
		c.EqDiff(before, c.snapshotSpecStatus(), "round %d changed a spec or status value", round)

		// TidyLegacy returns nothing, so a failed tidy-up cannot fail a
		// reconcile; this catches any other error the migration provokes.
		for _, r := range Suite.Reconciles.InNamespace(c.NS) {
			if r.Start.After(liftedAt) {
				c.NoError(r.Err, "reconcile errored after the gate lifted: %s", r)
			}
		}
	}

	t.Run("implicit manager entry is left alone", func(t *testing.T) {
		c := c.Sub(t)
		got := c.getCluster(cluster)
		i := slices.IndexFunc(got.GetManagedFields(), isImplicitManagerEntry)
		c.True(i >= 0, "the implicit manager's status entry was removed: %+v",
			got.GetManagedFields())
		c.Eq(implicit.FieldsV1.String(), got.GetManagedFields()[i].FieldsV1.String(),
			"the implicit manager's claims changed")
	})

	t.Run("field only a legacy entry owns keeps that owner", func(t *testing.T) {
		c := c.Sub(t)
		got := c.getShard(shard)
		c.Eq("x", got.Annotations[legacyOnlyAnnotation],
			"annotation applied under the legacy manager was removed")
		owners := managedfields.Owners(got, legacyOnlyPath)
		c.True(slices.Equal([]string{legacyOnlyManager}, owners),
			"owners of %s are %q: its only owner was dropped, leaving it ownerless",
			legacyOnlyAnnotation, owners)
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
	slices.SortFunc(
		out,
		func(a, b client.Object) int { return strings.Compare(objKey(a), objKey(b)) },
	)
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
// its retired shared name, and returns how many entries it rewrote. Each object is
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

// objectsCarryingLegacy lists, sorted, each "kind/name: manager on
// subresource" for a legacy entry that remains.
func (c *C) objectsCarryingLegacy() []string {
	c.Helper()
	var out []string
	for _, o := range c.crObjects() {
		for _, e := range o.GetManagedFields() {
			if isLegacyEntry(e) {
				sub := e.Subresource
				if sub == "" {
					sub = "main"
				}
				out = append(out, objKey(o)+": "+e.Manager+" on "+sub)
			}
		}
	}
	slices.Sort(out)
	return out
}

// bumpAll changes the reconcile label on every CR object while the namespace
// is paused. Pause only keeps alive a request that was already queued, and a
// settled object has none, so without a trigger the controllers would stay idle
// after Activate. The refused request is then retried until Activate admits it.
// Spec is never touched.
func (c *C) bumpAll(value string) {
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
			labels[features.ReconcileLabel] = value
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

// implicitManager is the name the apiserver gives a write with no field
// owner: the program name from the client's User-Agent. That is `manager` in
// the shipped image and the test binary's name (suite.test) here.
var implicitManager = strings.SplitN(rest.DefaultKubernetesUserAgent(), "/", 2)[0]

func isImplicitManagerEntry(e metav1.ManagedFieldsEntry) bool {
	return e.Manager == implicitManager &&
		e.Operation == metav1.ManagedFieldsOperationUpdate &&
		e.Subresource == "status"
}

// isImplicitManagerLeftover matches the violation the seeded implicit
// manager entry raises: it claims status fields the table gives to the
// cluster's status manager. That is the documented leftover, not something
// the tidy-up may fix.
func isImplicitManagerLeftover(v string) bool {
	return strings.Contains(v, "claimed by "+implicitManager) &&
		strings.HasPrefix(v, "MultigresCluster ")
}

func (c *C) getCluster(cluster *MultigresCluster) *MultigresCluster {
	c.Helper()
	got := &MultigresCluster{}
	c.NoError(c.Get(client.ObjectKeyFromObject(cluster), got), "get cluster")
	return got
}

// seedImplicitManagerEntry makes the status writes an operator version that
// wrote status with Status().Update and no field owner makes on a
// MultigresCluster: an Update that changes the applied images and the
// ImageRolloutPending condition, then one that sets them back. The second
// leaves every value as it was, with the implicit manager's entry owning those
// fields, which is the state such an operator leaves behind.
func (c *C) seedImplicitManagerEntry(cluster *MultigresCluster) metav1.ManagedFieldsEntry {
	c.Helper()
	orig := c.getCluster(cluster)
	c.True(orig.Status.Images != nil && orig.Status.Images.Applied.Postgres != "",
		"the cluster has no status.images.applied.postgres to claim")
	pending := meta.FindStatusCondition(orig.Status.Conditions,
		multigresv1alpha1.ConditionImageRolloutPending)
	c.True(pending != nil, "the cluster has no ImageRolloutPending condition to claim")

	changed := orig.DeepCopy()
	changed.Status.Images.Applied.Postgres += "-other"
	meta.FindStatusCondition(changed.Status.Conditions,
		multigresv1alpha1.ConditionImageRolloutPending).Message += " (other)"
	c.NoError(c.Client().Status().Update(c.Context(), changed), "status update with no owner")
	restored := c.getCluster(cluster)
	restored.Status = orig.Status
	c.NoError(c.Client().Status().Update(c.Context(), restored), "status update with no owner")

	got := c.getCluster(cluster)
	c.EqDiff(orig.Status, got.Status, "the seeding changed a status value")
	i := slices.IndexFunc(got.GetManagedFields(), isImplicitManagerEntry)
	c.True(i >= 0, "no %s/Update/status entry: %+v", implicitManager, got.GetManagedFields())
	for _, path := range []string{
		".status.images.applied.postgres",
		ownership.ConditionPath(multigresv1alpha1.ConditionImageRolloutPending) + ".message",
	} {
		c.True(slices.Contains(managedfields.Owners(got, path), implicitManager),
			"%s does not own %s: %+v", implicitManager, path, got.GetManagedFields())
	}
	return got.GetManagedFields()[i]
}

// removeImplicitManagerEntry runs the documented manual clean-up: remove the
// entry by index, guarded by a test op on the whole entry as read, so a list
// that changed since fails the patch instead of removing another entry or one
// with new claims. A lost race is retried on a fresh read.
func (c *C) removeImplicitManagerEntry(cluster *MultigresCluster) {
	c.Helper()
	ctx := context.Background()
	key := client.ObjectKeyFromObject(cluster)
	deadline := time.Now().Add(15 * time.Second)
	for {
		got := &MultigresCluster{}
		c.NoError(Suite.Client.Get(ctx, key, got), "get cluster")
		i := slices.IndexFunc(got.GetManagedFields(), isImplicitManagerEntry)
		if i < 0 {
			return
		}
		p := "/metadata/managedFields/" + strconv.Itoa(i)
		body, err := json.Marshal([]map[string]any{
			{"op": "test", "path": p, "value": got.GetManagedFields()[i]},
			{"op": "remove", "path": p},
		})
		c.NoError(err, "marshal managedFields patch")
		err = Suite.Client.Patch(ctx, got, client.RawPatch(types.JSONPatchType, body))
		if err == nil || time.Now().After(deadline) {
			c.NoError(err, "remove the implicit manager's status entry")
			return
		}
	}
}

// writeAsSharedNames makes the status writes an operator writing under the
// shared names makes, on every CR object, while the current entries stay:
// each status field and condition is applied with its current value under the
// shared name of its table owner. A field whose owner had no shared name (the
// etcd maintenance record) is left out. It returns how many applies it made.
func (c *C) writeAsSharedNames() int {
	c.Helper()
	n := 0
	for _, o := range c.crObjects() {
		gvk, err := apiutil.GVKForObject(o, Suite.Client.Scheme())
		c.NoError(err, "kind of %s", objKey(o))
		raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
		c.NoError(err, "convert %s", objKey(o))
		status, _ := raw["status"].(map[string]any)
		payloads := map[string]map[string]any{}
		add := func(path, key string, v any, cond bool) {
			row, ok := ownership.OwnerOf(gvk.Kind, path)
			if !ok || legacyOf[row.Manager] == "" {
				return
			}
			p := payloads[legacyOf[row.Manager]]
			if p == nil {
				p = map[string]any{}
				payloads[legacyOf[row.Manager]] = p
			}
			if cond {
				list, _ := p["conditions"].([]any)
				p["conditions"] = append(list, v)
				return
			}
			p[key] = v
		}
		for k, v := range status {
			if k != "conditions" {
				add(statusPath+"."+k, k, v, false)
			}
		}
		conds, _ := status["conditions"].([]any)
		for _, cond := range conds {
			t, _ := cond.(map[string]any)["type"].(string)
			add(ownership.ConditionPath(t), "", cond, true)
		}
		for manager, p := range payloads {
			u := &unstructured.Unstructured{Object: map[string]any{"status": p}}
			u.SetGroupVersionKind(gvk)
			u.SetName(o.GetName())
			u.SetNamespace(o.GetNamespace())
			c.NoError(c.Client().Status().Patch(c.Context(), u, client.Apply,
				client.FieldOwner(manager), client.ForceOwnership),
				"apply %s status as %s", objKey(o), manager)
			n++
		}
	}
	return n
}
