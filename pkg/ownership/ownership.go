package ownership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
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
}

// Status managers: one per controller, plus facets for status a controller
// writes apart from its main status apply.
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

// ManagerChildWriter is the manager every controller applies child objects
// under: Deployments, Services, StatefulSets, ConfigMaps, PVCs, PDBs,
// NetworkPolicies, Certificates, and the spec of the child CRs (Cell,
// TableGroup, Shard, TopoServer). Each child object has one writer, so a
// per-controller name would separate nobody. It also cannot be renamed in
// place: an apply under a new name leaves the old entry co-owning every field
// it set, so fields the new apply omits are never pruned, and a spec change
// that removes a field the old entry owns (a CPU limit dropped while the
// request rises) can fail validation on every apply, so no tidy-up after the
// apply is ever reached.
const ManagerChildWriter = "multigres-operator"

// Status managers that are retired. A status entry under one of them is left
// over from an operator version that wrote status under a shared name.
const (
	LegacyResourceHandler          = "multigres-resource-handler"
	LegacyResourceHandlerGuard     = "multigres-resource-handler-guard"
	LegacyResourceHandlerReadiness = "multigres-resource-handler-readiness"
	LegacyOperatorGuard            = "multigres-operator-guard"
)

// Managers returns the status manager names, sorted.
func Managers() []string {
	m := []string{
		ManagerCluster, ManagerTableGroup, ManagerCell, ManagerTopoServer,
		ManagerShard, ManagerShardGuard, ManagerShardReadiness,
		ManagerTopoServerGuard, ManagerEtcdMaintenance,
	}
	slices.Sort(m)
	return m
}

// IsLegacyEntry reports whether e is left over from a retired status writer.
//
// Legacy is a property of the entry, not of the name: ManagerChildWriter is
// the live writer of every child object and of the child CRs' spec, and is
// legacy only on the status subresource, where the per-controller managers
// replaced it. LegacyResourceHandler only ever wrote status, and is treated
// the same way so an entry on a main resource is never removed on a guess.
// The retired facet names wrote nothing but status conditions, so any entry
// under them is legacy.
func IsLegacyEntry(e metav1.ManagedFieldsEntry) bool {
	switch e.Manager {
	case LegacyResourceHandlerGuard, LegacyResourceHandlerReadiness, LegacyOperatorGuard:
		return true
	case ManagerChildWriter, LegacyResourceHandler:
		return e.Subresource == "status"
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
		if f.Kind != kind || !HasPathPrefix(path, f.Path) {
			continue
		}
		if !found || len(f.Path) > len(best.Path) {
			best, found = f, true
		}
	}
	return best, found
}

// HasPathPrefix reports whether prefix is path or an ancestor of it, matching
// only at a path-element boundary.
func HasPathPrefix(path, prefix string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	if len(path) == len(prefix) {
		return true
	}
	next := path[len(prefix)]
	return next == '.' || next == '['
}

// ConditionPath is the managedFields path of the status condition of type t.
func ConditionPath(t string) string {
	return `.status.conditions[type="` + t + `"]`
}

// Owns reports whether obj's managedFields record manager as claiming path
// or something below it. A claim on an ancestor alone does not count: the
// apiserver records an applied empty object, or what remains of a granular
// map, as a claimed container with nothing below it, which owns none of the
// fields that may sit there. An undecodable entry counts as not owning, so a
// caller deciding whether to skip a write writes instead. No subresource is
// matched: a .status path is recorded only for status writes.
func Owns(obj client.Object, manager, path string) bool {
	for _, e := range obj.GetManagedFields() {
		if e.Manager != manager {
			continue
		}
		set, err := decode(e)
		if err != nil {
			continue
		}
		found := false
		set.Iterate(func(p fieldpath.Path) {
			found = found || HasPathPrefix(p.String(), path)
		})
		if found {
			return true
		}
	}
	return false
}

func decode(e metav1.ManagedFieldsEntry) (*fieldpath.Set, error) {
	set := &fieldpath.Set{}
	if e.FieldsV1 == nil {
		return set, nil
	}
	if err := set.FromJSON(e.FieldsV1.GetRawReader()); err != nil {
		return nil, fmt.Errorf("decode managedFields entry of %s: %w", e.Manager, err)
	}
	return set, nil
}

// ApplyStatus server-side applies obj's status under manager and then tidies
// legacy status entries off the result. Every status apply goes through here
// so no return path can skip the tidy-up.
//
// ForceOwnership lets the first apply under a status manager take fields a
// legacy entry holds rather than conflict with it.
// A cleanup conflict is returned even after a successful status apply so the
// controller retries instead of leaving a healthy object with no future event.
func ApplyStatus(ctx context.Context, c client.Client, obj client.Object, manager string) error {
	if err := c.Status().Patch(
		ctx,
		obj,
		client.Apply,
		client.FieldOwner(manager),
		client.ForceOwnership,
	); err != nil {
		return err
	}
	return TidyLegacy(ctx, c, obj)
}

// SkipUnchanged reports whether a status writer may skip writing path under
// manager: the stored value is unchanged and manager's own entry claims it. A
// value only another entry holds is written once so manager claims it, or a
// legacy entry holding it could never be removed.
//
// A skip also skips the tidy-up ApplyStatus would run, so it runs TidyLegacy
// itself: a writer whose value never changes (a reconcile that stops at a
// missing StorageClass) would otherwise never retry a tidy-up that failed.
// A cleanup conflict falls through to ApplyStatus, whose response refreshes
// managedFields and whose cleanup can return a conflict to the reconciler.
func SkipUnchanged(
	ctx context.Context,
	c client.Client,
	obj client.Object,
	manager, path string,
	unchanged bool,
) bool {
	if !unchanged || !Owns(obj, manager, path) {
		return false
	}
	return TidyLegacy(ctx, c, obj) == nil
}

type patchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// ErrManagedFieldsChanged wraps a DropLegacy patch error that has the shape
// of a failed test op; see isPatchNotApplied.
var ErrManagedFieldsChanged = errors.New("managedFields changed since they were read")

// covers reports whether set claims p or something below it. An ancestor
// claim alone never covers p (see Owns).
func covers(set *fieldpath.Set, p fieldpath.Path) bool {
	if set.Has(p) {
		return true
	}
	for _, pe := range p {
		child, ok := set.Children.Get(pe)
		if !ok {
			return false
		}
		set = child
	}
	return !set.Empty()
}

// isStatusRoot is the bare .status container, which names no field.
func isStatusRoot(p fieldpath.Path) bool {
	return len(p) == 1 && p[0].FieldName != nil && *p[0].FieldName == "status"
}

// DropLegacy removes, in one JSON patch on the main resource, each entry
// IsLegacyEntry selects from obj whose every claimed field (the bare .status
// container aside) is covered by a current entry. Force does not retire a
// legacy co-owner that applied the same value, so the entry has to be removed.
// Any other legacy entry is kept, without error: it is the only owner of a
// field, and removing it would leave that field populated and invisible to
// every later apply.
//
// The patch has no resourceVersion precondition, which a busy object (a
// kubelet's Pod) would fail every time. It tests what the decision read
// instead: each removed index still holds the identical entry, and for each
// field of a removed entry, one current entry covering it still has the same
// manager, operation, subresource and fields. A current writer withdrawing a
// shared field in between fails the patch with a 422, keeping the legacy
// owner. Entries the decision did not read cannot uncover a covered field and
// are not tested.
//
// Callers use TidyLegacy, which returns cleanup conflicts to request a retry.
func DropLegacy(ctx context.Context, c client.Client, obj client.Object) error {
	entries := obj.GetManagedFields()
	claims := make([]*fieldpath.Set, len(entries))
	var legacy, current []int
	for i, e := range entries {
		set, err := decode(e)
		if err != nil {
			return err
		}
		claims[i] = set
		if IsLegacyEntry(e) {
			legacy = append(legacy, i)
		} else {
			current = append(current, i)
		}
	}
	// Nothing to do, or nothing current yet to cover anything.
	if len(legacy) == 0 || len(current) == 0 {
		return nil
	}
	// The status managers first, so a guard prefers an entry this operator
	// writes over one a busy foreign writer rewrites.
	statusManagers := Managers()
	slices.SortStableFunc(current, func(a, b int) int {
		_, ia := slices.BinarySearch(statusManagers, entries[a].Manager)
		_, ib := slices.BinarySearch(statusManagers, entries[b].Manager)
		switch {
		case ia == ib:
			return 0
		case ia:
			return -1
		}
		return 1
	})

	remove := map[int]bool{}
	guard := map[int]bool{}
	for _, i := range legacy {
		used := map[int]bool{}
		covered := true
		claims[i].Iterate(func(p fieldpath.Path) {
			if !covered || isStatusRoot(p) {
				return
			}
			j := slices.IndexFunc(current, func(j int) bool { return covers(claims[j], p) })
			if j < 0 {
				covered = false
				return
			}
			used[current[j]] = true
		})
		if !covered {
			continue
		}
		remove[i] = true
		for j := range used {
			guard[j] = true
		}
	}
	if len(remove) == 0 {
		return nil
	}

	var ops []patchOp
	for i := len(entries) - 1; i >= 0; i-- {
		p := "/metadata/managedFields/" + strconv.Itoa(i)
		switch {
		case remove[i]:
			ops = append(ops,
				patchOp{Op: "test", Path: p, Value: entries[i]},
				patchOp{Op: "remove", Path: p})
		case guard[i]:
			e := entries[i]
			ops = append(ops,
				patchOp{Op: "test", Path: p + "/manager", Value: e.Manager},
				patchOp{Op: "test", Path: p + "/operation", Value: e.Operation},
				patchOp{Op: "test", Path: p + "/fieldsV1", Value: e.FieldsV1})
			// A test op cannot assert an absent key. A covering entry claims
			// status fields, so it is a status-subresource entry in practice.
			if e.Subresource != "" {
				ops = append(ops,
					patchOp{Op: "test", Path: p + "/subresource", Value: e.Subresource})
			}
		}
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
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case isPatchNotApplied(err):
		return fmt.Errorf("%w: %w", ErrManagedFieldsChanged, err)
	}
	return err
}

// isPatchNotApplied reports whether err has the shape the apiserver gives a
// JSON patch it cannot apply to the stored object: 422, reason Invalid, empty
// details. For DropLegacy's patch, built from obj, that means a test op found
// a different entry. Schema and CEL failures, ValidatingAdmissionPolicy
// denials and controller-runtime webhook denials (403, or 422 naming the
// object) do not match. An admission webhook that answers 422 Invalid with
// no details has the same shape and cannot be told apart; it is reported as
// a changed list.
func isPatchNotApplied(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) || !apierrors.IsInvalid(err) {
		return false
	}
	d := status.Status().Details
	return d == nil || (d.Name == "" && d.Kind == "" && len(d.Causes) == 0)
}

// TidyLegacy returns a changed-list error so the controller's work queue
// schedules another pass, even for an otherwise healthy object. Other cleanup
// failures remain best-effort: admission denials must not block reconciliation
// after a successful status write.
func TidyLegacy(ctx context.Context, c client.Client, obj client.Object) error {
	err := DropLegacy(ctx, c, obj)
	if err == nil {
		return nil
	}
	l := log.FromContext(ctx).WithValues("object", client.ObjectKeyFromObject(obj))
	if errors.Is(err, ErrManagedFieldsChanged) {
		l.Info(
			"Legacy field manager tidy-up skipped, managedFields changed; retrying next reconcile",
			"error",
			err.Error(),
		)
		return err
	}
	l.Error(err, "Failed to drop legacy field managers; cleanup deferred until a future reconcile")
	return nil
}
