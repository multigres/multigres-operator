package suite

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/value"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// ownerLookup resolves the manager that owns a status path of a kind.
type ownerLookup func(kind, path string) (manager string, ok bool)

var crKinds = map[string]bool{
	"MultigresCluster": true,
	"TableGroup":       true,
	"Cell":             true,
	"TopoServer":       true,
	"Shard":            true,
}

const statusPath = ".status"

// underStatus is true strictly below .status. An Apply entry does not claim
// the bare container (envtest records no "." at .status for SSA appliers), but
// Update-op entries and the harness's own claims can, and the container names
// no field, so it is never checked against the table.
func underStatus(path string) bool {
	return strings.HasPrefix(path, statusPath+".") || strings.HasPrefix(path, statusPath+"[")
}

// checkedObject is what the rules read from one object: its managedFields,
// and the status leaves that hold a value and so must have an owner.
type checkedObject struct {
	kind, name string
	entries    []metav1.ManagedFieldsEntry
	populated  []fieldpath.Path
}

// legacyRule says which legacy entries an object may carry.
type legacyRule int

const (
	// noLegacy: none. No live writer uses a retired name, so in a case that
	// did not seed legacy entries any one of them is a misattributed write.
	noLegacy legacyRule = iota
	// keptLegacy: only an entry DropLegacy must keep, one that claims a
	// field no current entry covers. An entry whose every claim is covered
	// was removable, so its presence means the tidy-up missed it.
	keptLegacy
)

// claims is one entry's decoded fields.
type claims struct {
	set *fieldpath.Set
}

// covers reports whether the entry claims p itself or anything below it (an
// element of a list). A claim on an ancestor alone never covers p: the
// apiserver records an applied empty object, or what remains of a granular
// map, as a claimed container with nothing below it.
func (c claims) covers(p fieldpath.Path) bool {
	if c.set.Has(p) {
		return true
	}
	node := c.set
	for _, pe := range p {
		child, ok := node.Children.Get(pe)
		if !ok {
			return false
		}
		node = child
	}
	return !node.Empty()
}

// violationsFor returns every ownership violation on one object, sorted.
//
// The status rule applies to every manager, operator or not: an operator write
// that carries no field owner lands under the process's implicit manager name,
// which is not an operator name, and must still fail here. Legacy entries are
// judged by rule alone, since a kept one claims fields of another owner by
// definition.
func violationsFor(
	o checkedObject,
	ownerOf ownerLookup,
	operator func(manager string) bool,
	legacy func(metav1.ManagedFieldsEntry) bool,
	rule legacyRule,
) []string {
	var out []string
	who := o.kind + " " + o.name
	isCR := crKinds[o.kind]

	count := 0
	decoded := make([]*claims, len(o.entries))
	// path -> distinct operator managers claiming it, for the overlap rule.
	specClaimants := map[string]map[string]bool{}

	for i, mf := range o.entries {
		if mf.FieldsV1 == nil {
			continue
		}
		set := &fieldpath.Set{}
		if err := set.FromJSON(mf.FieldsV1.GetRawReader()); err != nil {
			out = append(out, fmt.Sprintf(
				"%s: undecodable managedFields for manager %s: %v", who, mf.Manager, err))
			continue
		}
		decoded[i] = &claims{set: set}
		isLegacy := legacy(mf)
		set.Iterate(func(p fieldpath.Path) {
			count++
			path := p.String()
			switch {
			case isLegacy:
			case underStatus(path) && (isCR || o.kind == "Pod"):
				owner, ok := ownerOf(o.kind, path)
				switch {
				case !ok && (isCR || operator(mf.Manager)):
					out = append(out, fmt.Sprintf(
						"%s: unowned status field %s claimed by %s", who, path, mf.Manager))
				case ok && owner != mf.Manager:
					out = append(out, fmt.Sprintf(
						"%s: status field %s owned by %s, claimed by %s",
						who, path, owner, mf.Manager))
				}
			case isCR && path != statusPath && !underStatus(path) && operator(mf.Manager):
				if specClaimants[path] == nil {
					specClaimants[path] = map[string]bool{}
				}
				specClaimants[path][mf.Manager] = true
			}
		})
	}

	for i, mf := range o.entries {
		if !legacy(mf) {
			continue
		}
		sub := mf.Subresource
		if sub == "" {
			sub = "main"
		}
		if rule == noLegacy {
			out = append(out, fmt.Sprintf("%s: legacy manager %s on %s", who, mf.Manager, sub))
			continue
		}
		if decoded[i] != nil && !coveredByCurrent(o.entries, decoded, legacy, decoded[i]) {
			continue
		}
		out = append(out, fmt.Sprintf(
			"%s: legacy manager %s on %s claims nothing a current entry does not cover",
			who, mf.Manager, sub))
	}

	for path, managers := range specClaimants {
		if len(managers) < 2 {
			continue
		}
		names := make([]string, 0, len(managers))
		for m := range managers {
			names = append(names, m)
		}
		slices.Sort(names)
		out = append(out, fmt.Sprintf("%s: field %s claimed by more than one operator manager: %s",
			who, path, strings.Join(names, ", ")))
	}

	// A populated leaf nobody claims is invisible to every writer: no apply
	// can prune it, and a migration that dropped its last owner would
	// otherwise pass every rule above. Each leaf is checked on its own, so a
	// claim on a sibling (lastAppliedAt beside error) owns nothing.
	for _, want := range o.populated {
		if !slices.ContainsFunc(
			decoded,
			func(c *claims) bool { return c != nil && c.covers(want) },
		) {
			out = append(out, fmt.Sprintf(
				"%s: populated status field %s has no owner", who, want.String()))
		}
	}

	// Without this, an object whose managedFields all failed to decode would
	// pass every rule above having compared nothing.
	if count == 0 {
		out = append(
			out,
			fmt.Sprintf("%s: no decodable managedFields claims (%d entries)", who, len(o.entries)),
		)
	}

	slices.Sort(out)
	return out
}

// coveredByCurrent reports whether every field the legacy entry l claims,
// other than the bare .status container, is covered by a non-legacy entry.
func coveredByCurrent(
	entries []metav1.ManagedFieldsEntry,
	decoded []*claims,
	legacy func(metav1.ManagedFieldsEntry) bool,
	l *claims,
) bool {
	covered := true
	l.set.Iterate(func(p fieldpath.Path) {
		if !covered || p.String() == statusPath {
			return
		}
		covered = false
		for i, e := range entries {
			if decoded[i] != nil && !legacy(e) && decoded[i].covers(p) {
				covered = true
				return
			}
		}
	})
	return covered
}

// populatedStatusLeaves lists the status leaves of obj that hold a value: each
// scalar, each list that is not a list of conditions (whose elements are
// matched as a whole), and each field of each condition, keyed by type. On a
// Pod only conditions whose type has a table row are read, so kubelet's own
// status (phase, its conditions) is never checked.
//
// Zero values count as unpopulated, which is the one exemption: the typed
// read cannot tell a zero from an absent field (a required counter at 0),
// and a zero carries no state to strand.
func populatedStatusLeaves(kind string, obj client.Object) ([]fieldpath.Path, error) {
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("convert %s %s: %w", kind, obj.GetName(), err)
	}
	status, _ := raw["status"].(map[string]any)
	root := fieldpath.MakePathOrDie("status")
	var out []fieldpath.Path
	if kind != "Pod" {
		sets, err := statusSets()
		if err != nil {
			return nil, err
		}
		walkLeaves(root, status, sets[kind], &out)
		return out, nil
	}
	list, _ := status["conditions"].([]any)
	for _, c := range list {
		m, _ := c.(map[string]any)
		t, _ := m["type"].(string)
		if _, ok := ownership.OwnerOf(kind, ownership.ConditionPath(t)); !ok {
			continue
		}
		walkLeaves(conditionPath(root, t), m, nil, &out)
	}
	return out, nil
}

func conditionPath(status fieldpath.Path, t string) fieldpath.Path {
	return append(
		status.Copy(),
		fieldpath.PathElement{FieldName: ptr.To("conditions")},
		fieldpath.PathElement{
			Key: &value.FieldList{{Name: "type", Value: value.NewValueInterface(t)}},
		},
	)
}

// walkLeaves appends the populated leaves below at. A list the schema
// declares a set (sets holds their paths) is walked by member, since each
// member is its own field to the apiserver; any other list other than
// conditions is one leaf.
func walkLeaves(at fieldpath.Path, v any, sets map[string]bool, out *[]fieldpath.Path) {
	switch t := v.(type) {
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(t)) {
			next := append(at.Copy(), fieldpath.PathElement{FieldName: ptr.To(k)})
			walkLeaves(next, t[k], sets, out)
		}
		return
	case []any:
		switch {
		case len(at) == 2 && at.String() == statusPath+".conditions":
			for _, c := range t {
				m, _ := c.(map[string]any)
				if ty, ok := m["type"].(string); ok {
					walkLeaves(conditionPath(at[:1], ty), m, sets, out)
				}
			}
			return
		case sets[at.String()]:
			for _, m := range t {
				if !isZero(m) {
					*out = append(*out, append(at.Copy(),
						fieldpath.PathElement{Value: ptr.To(value.NewValueInterface(m))}))
				}
			}
			return
		}
	}
	if !isZero(v) {
		*out = append(*out, at)
	}
}

// statusSets reads, per kind, the status paths the CRD schemas declare
// x-kubernetes-list-type=set.
var statusSets = sync.OnceValues(func() (map[string]map[string]bool, error) {
	crds, err := testutil.ReadCRDs(filepath.Join("..", "..", "config", "crd", "bases"))
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]bool{}
	for _, crd := range crds {
		kind := crd.Spec.Names.Kind
		for _, v := range crd.Spec.Versions {
			if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
				continue
			}
			status, ok := v.Schema.OpenAPIV3Schema.Properties["status"]
			if !ok {
				continue
			}
			var walk func(path string, s apiextensionsv1.JSONSchemaProps)
			walk = func(path string, s apiextensionsv1.JSONSchemaProps) {
				if s.XListType != nil && *s.XListType == "set" {
					if out[kind] == nil {
						out[kind] = map[string]bool{}
					}
					out[kind][path] = true
				}
				for name, p := range s.Properties {
					walk(path+"."+name, p)
				}
			}
			walk(statusPath, status)
		}
	}
	return out, nil
})

func isZero(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case bool:
		return !t
	case int64:
		return t == 0
	case float64:
		return t == 0
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	}
	return false
}

// checkOwnership lists every object of the five CR kinds and every Pod in ns
// and returns all violations, sorted, plus how many CR objects it examined
// (Pods are not counted). Zero is not an error here: a case that creates no CR
// has nothing to violate. The caller passes the reader: it must be the API
// reader, since a cached one can lag the managedFields it checks.
func checkOwnership(
	ctx context.Context,
	c client.Reader,
	ns string,
	rule legacyRule,
) (violations []string, checked int, err error) {
	owner := func(kind, path string) (string, bool) {
		f, ok := ownership.OwnerOf(kind, path)
		return f.Manager, ok
	}
	// The child writer counts as an operator manager: a status manager that
	// also claims a spec field the parent applies is an overlap, and the child
	// writer claiming Pod status is a misattributed write.
	operators := map[string]bool{ownership.ManagerChildWriter: true}
	for _, m := range ownership.Managers() {
		operators[m] = true
	}
	isOperator := func(m string) bool { return operators[m] }

	lists := []struct {
		kind string
		list client.ObjectList
	}{
		{"MultigresCluster", &multigresv1alpha1.MultigresClusterList{}},
		{"TableGroup", &multigresv1alpha1.TableGroupList{}},
		{"Cell", &multigresv1alpha1.CellList{}},
		{"TopoServer", &multigresv1alpha1.TopoServerList{}},
		{"Shard", &multigresv1alpha1.ShardList{}},
		{"Pod", &corev1.PodList{}},
	}

	var out []string
	for _, l := range lists {
		if err := c.List(ctx, l.list, client.InNamespace(ns)); err != nil {
			return nil, 0, fmt.Errorf("list %s in %s: %w", l.kind, ns, err)
		}
		err := meta.EachListItem(l.list, func(o runtime.Object) error {
			obj, ok := o.(client.Object)
			if !ok {
				return fmt.Errorf("%s list item %T is not a client.Object", l.kind, o)
			}
			if crKinds[l.kind] {
				checked++
			}
			populated, err := populatedStatusLeaves(l.kind, obj)
			if err != nil {
				return err
			}
			out = append(out, violationsFor(checkedObject{
				kind:      l.kind,
				name:      obj.GetName(),
				entries:   obj.GetManagedFields(),
				populated: populated,
			}, owner, isOperator, ownership.IsLegacyEntry, rule)...)
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
	}
	slices.Sort(out)
	return out, checked, nil
}
