package ownership

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Class says what a status field is for, which decides the rules it must obey.
type Class int

const (
	Observation Class = iota + 1
	Gate
	WorkflowMemory
)

// Field is one row of the contract.
type Field struct {
	Kind    string // "MultigresCluster", "TableGroup", "Cell", "TopoServer", "Shard", "Pod"
	Path    string // fieldpath.Path String() prefix, e.g. `.status.conditions[type="StorageClassValid"]`
	Manager string
	Class   Class
	Changes string // when the field is expected to change
	Readers string // Gate and WorkflowMemory: who reads it to decide, and what unblocks the reader when it is degraded, unreadable or stale
}

const (
	ManagerCluster         = "multigres-cluster"
	ManagerTableGroup      = "multigres-tablegroup"
	ManagerCell            = "multigres-cell"
	ManagerTopoServer      = "multigres-toposerver"
	ManagerShard           = "multigres-shard"
	ManagerShardGuard      = "multigres-shard-guard"
	ManagerShardReadiness  = "multigres-shard-readiness"
	ManagerTopoServerGuard = "multigres-toposerver-guard"
	ManagerEtcdMaintenance = "multigres-etcd-maintenance"
)

// Names that managers used before the split. They are only ever removed.
const (
	LegacyOperator                 = "multigres-operator"
	LegacyResourceHandler          = "multigres-resource-handler"
	LegacyResourceHandlerGuard     = "multigres-resource-handler-guard"
	LegacyResourceHandlerReadiness = "multigres-resource-handler-readiness"
	LegacyOperatorGuard            = "multigres-operator-guard"
)

// Managers returns the current manager names, sorted.
func Managers() []string {
	m := []string{
		ManagerCluster, ManagerTableGroup, ManagerCell, ManagerTopoServer,
		ManagerShard, ManagerShardGuard, ManagerShardReadiness,
		ManagerTopoServerGuard, ManagerEtcdMaintenance,
	}
	slices.Sort(m)
	return m
}

// IsLegacy reports whether manager is a pre-split field manager name.
func IsLegacy(manager string) bool {
	switch manager {
	case LegacyOperator, LegacyResourceHandler, LegacyResourceHandlerGuard,
		LegacyResourceHandlerReadiness, LegacyOperatorGuard:
		return true
	}
	return false
}

// OwnerOf returns the row of Fields for kind whose Path is the longest prefix
// of path. A prefix only matches at a path-element boundary, so `.status.phase`
// does not claim `.status.phaseX`.
func OwnerOf(kind, path string) (Field, bool) {
	var best Field
	found := false
	for _, f := range Fields {
		if f.Kind != kind || !hasPathPrefix(path, f.Path) {
			continue
		}
		if !found || len(f.Path) > len(best.Path) {
			best, found = f, true
		}
	}
	return best, found
}

func hasPathPrefix(path, prefix string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if len(path) == len(prefix) {
		return true
	}
	next := path[len(prefix)]
	return next == '.' || next == '['
}

type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value,omitempty"`
}

// DropLegacy removes the managedFields entries of legacy managers from obj in
// one JSON patch on the main resource.
//
// A field a renamed writer sends is reclaimed by its next forced apply. A
// field only a legacy entry owned is stranded if the first apply under the new
// name omits it: nobody owns it afterwards, so nobody will prune it, whereas
// before the rename the same manager would have pruned it on that apply. The
// window is one reconcile around the upgrade. Temporary: delete once the
// fleet query reports no legacy managers in any environment.
//
// Force does not retire a legacy co-owner that applied the same value, which
// is why the entry has to be removed rather than overwritten. The patch is
// guarded by the resourceVersion and by each entry's manager name, so a stale
// obj cannot remove the wrong index; a stale obj fails the guard with a 422.
//
// The tidy-up is best-effort: callers use TidyLegacy, which logs a failure and
// lets the next reconcile retry.
func DropLegacy(ctx context.Context, c client.Client, obj client.Object) error {
	entries := obj.GetManagedFields()
	var legacy []int
	for i, e := range entries {
		if IsLegacy(e.Manager) {
			legacy = append(legacy, i)
		}
	}
	// Nothing to do, or nothing left to keep: the apiserver ignores an emptied
	// managedFields, and a renamed writer that has not written yet has not
	// claimed anything.
	if len(legacy) == 0 || len(legacy) == len(entries) {
		return nil
	}

	ops := []patchOp{
		{Op: "test", Path: "/metadata/resourceVersion", Value: obj.GetResourceVersion()},
	}
	for j := len(legacy) - 1; j >= 0; j-- {
		i := legacy[j]
		p := "/metadata/managedFields/" + strconv.Itoa(i)
		ops = append(ops,
			patchOp{Op: "test", Path: p + "/manager", Value: entries[i].Manager},
			patchOp{Op: "remove", Path: p},
		)
	}
	body, err := json.Marshal(ops)
	if err != nil {
		return fmt.Errorf("marshal managedFields patch: %w", err)
	}

	// Patch writes the response into its argument; patching a copy keeps the
	// caller's object, often a shared cache entry, untouched.
	target, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("deep copy of %T is not a client.Object", obj)
	}
	err = c.Patch(ctx, target, client.RawPatch(types.JSONPatchType, body))
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// TidyLegacy runs DropLegacy and logs a failure instead of returning it: a
// leftover legacy entry costs nothing until the next reconcile retries, so it
// must never fail or delay the write it follows.
func TidyLegacy(ctx context.Context, c client.Client, obj client.Object) {
	err := DropLegacy(ctx, c, obj)
	if err == nil {
		return
	}
	l := log.FromContext(ctx).WithValues("object", client.ObjectKeyFromObject(obj))
	// The apiserver answers a failed guard (obj was stale, usually from the
	// cache) with 422, which is routine and fixes itself on the next pass.
	if apierrors.IsInvalid(err) {
		l.Info(
			"Legacy field manager tidy-up skipped, object changed; retrying next reconcile",
			"error",
			err.Error(),
		)
		return
	}
	l.Error(err, "Failed to drop legacy field managers; retrying next reconcile")
}
