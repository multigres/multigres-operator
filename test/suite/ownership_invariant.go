package suite

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
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

// violationsFor returns every ownership violation on one object, sorted.
//
// Rule 2 applies to every manager, operator or not: an operator write that
// carries no field owner lands under the process's implicit manager name, which
// is not an operator name, and must still fail here.
func violationsFor(
	kind, name string,
	entries []metav1.ManagedFieldsEntry,
	ownerOf ownerLookup,
	operator func(manager string) bool,
	legacy func(manager string) bool,
) []string {
	var out []string
	who := kind + " " + name
	isCR := crKinds[kind]

	claims := 0
	// path -> distinct operator managers claiming it, for the overlap rule.
	specClaimants := map[string]map[string]bool{}

	for _, mf := range entries {
		if legacy(mf.Manager) {
			out = append(out, fmt.Sprintf("%s: legacy manager %s", who, mf.Manager))
		}
		if mf.FieldsV1 == nil {
			continue
		}
		set := &fieldpath.Set{}
		if err := set.FromJSON(mf.FieldsV1.GetRawReader()); err != nil {
			out = append(
				out,
				fmt.Sprintf(
					"%s: undecodable managedFields for manager %s: %v",
					who,
					mf.Manager,
					err,
				),
			)
			continue
		}
		checkPod := kind == "Pod" && operator(mf.Manager)
		set.Iterate(func(p fieldpath.Path) {
			claims++
			path := p.String()
			switch {
			case underStatus(path) && (isCR || checkPod):
				owner, ok := ownerOf(kind, path)
				switch {
				case !ok:
					out = append(
						out,
						fmt.Sprintf(
							"%s: unowned status field %s claimed by %s",
							who,
							path,
							mf.Manager,
						),
					)
				case owner != mf.Manager:
					out = append(
						out,
						fmt.Sprintf(
							"%s: status field %s owned by %s, claimed by %s",
							who,
							path,
							owner,
							mf.Manager,
						),
					)
				}
			case isCR && path != statusPath && !underStatus(path) && operator(mf.Manager):
				if specClaimants[path] == nil {
					specClaimants[path] = map[string]bool{}
				}
				specClaimants[path][mf.Manager] = true
			}
		})
	}

	for path, managers := range specClaimants {
		if len(managers) < 2 {
			continue
		}
		names := make([]string, 0, len(managers))
		for m := range managers {
			names = append(names, m)
		}
		sort.Strings(names)
		out = append(out, fmt.Sprintf("%s: field %s claimed by more than one operator manager: %s",
			who, path, strings.Join(names, ", ")))
	}

	// Without this, an object whose managedFields all failed to decode would
	// pass every rule above having compared nothing.
	if claims == 0 {
		out = append(
			out,
			fmt.Sprintf("%s: no decodable managedFields claims (%d entries)", who, len(entries)),
		)
	}

	sort.Strings(out)
	return out
}

// ownershipViolations is the strict form: it errors when ns holds no CR object,
// so a namespace passed by hand (a mistype, or a cleanup that ran after the
// objects were gone) cannot pass having checked nothing. The per-case
// invariant uses checkOwnership instead, because a case that legitimately
// creates no CR (a harness probe that only makes a Secret) has nothing to
// violate and must not fail for it.
func ownershipViolations(ctx context.Context, c client.Reader, ns string) ([]string, error) {
	out, checked, err := checkOwnership(ctx, c, ns)
	if err != nil {
		return nil, err
	}
	if checked == 0 {
		return nil, fmt.Errorf(
			"no objects of the five CR kinds in namespace %q: nothing was checked",
			ns,
		)
	}
	return out, nil
}

// checkOwnership lists every object of the five CR kinds and every Pod in ns
// and returns all violations, sorted, plus how many CR objects it examined
// (Pods are not counted). Zero is not an error here. The caller passes the
// reader: it must be the API reader, since a cached one can lag the
// managedFields it checks.
func checkOwnership(
	ctx context.Context,
	c client.Reader,
	ns string,
) (violations []string, checked int, err error) {
	owner := func(kind, path string) (string, bool) {
		f, ok := ownership.OwnerOf(kind, path)
		return f.Manager, ok
	}
	operators := map[string]bool{}
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
			out = append(out, violationsFor(l.kind, obj.GetName(), obj.GetManagedFields(),
				owner, isOperator, ownership.IsLegacy)...)
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
	}
	sort.Strings(out)
	return out, checked, nil
}
