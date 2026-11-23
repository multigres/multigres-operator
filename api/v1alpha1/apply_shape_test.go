package v1alpha1

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// applyExempt lists the blocks of a child spec that may still be applied as
// {} after ForApply, keyed by the JSON path with list items as [] and map
// values as *, each with the reason the empty object cannot reach a child
// apply over a block that had entries, or is meant. A key starting with *
// covers every path that ends in the rest of it.
var applyExempt = map[string]string{
	"logLevels": "the resolver sets every level, so the block is never empty",
	"multiorch": "required; ForApply names the pool cells, and a shard whose pools " +
		"name no cell is refused by the Shard controller",
	"shards[].multiorch": "required; ForApply names the pool cells, and a shard " +
		"whose pools name no cell is refused by the Shard controller",
	"multigateway": "required; ResolveCell defaults buffer.enabled and " +
		"clusterViews.enabled in both modes, so the block is never empty",
	"pools": "a map, which an apply merges key by key, so {} removes this " +
		"manager's pools rather than replacing a block that had entries",
	"pools.*.storage":          "resolution always sets the pool storage size",
	"shards[].pools.*.storage": "resolution always sets the pool storage size",
	"etcd.storage":             "resolution always sets the etcd storage size",
	"topoServer.etcd.storage":  "resolution always sets the etcd storage size",
	"tls": "copied from MultigresCluster topoTLS, which the CRD makes immutable, " +
		"so it is never edited to {} after the TopoServer exists",
	"cellTopologyLabels.*": "built by the operator with a zone or region label for " +
		"each cell it lists, so no cell's labels are empty",
	"*imagePullSecrets[]": "an atomic list, replaced whole by an apply, so an " +
		"empty item is not merged into an earlier one",
	"*nodeSelectorTerms[]": "an atomic list, replaced whole by an apply, so an " +
		"empty item is not merged into an earlier one",
	"*tolerations[]": "an atomic list, replaced whole by an apply, so an " +
		"empty item is not merged into an earlier one",
}

// TestChildSpecsApplyNoEmptyBlocks requires ForApply to leave no {} outside
// applyExempt in what a child spec applies, and every exemption to still be
// needed. It builds two kinds of input for each of the four child specs:
//
//   - for every block found by reflection that encodes as {} when it is the
//     zero value (a pointer to a struct, or a struct not tagged omitzero), a
//     spec holding only that block, empty;
//   - each spec in the deep-empty forms of deepEmptyForms (see deepEmpty):
//     every list and map non-nil, with or without one deep-empty element, and
//     pointers allocated or left nil. This reaches blocks that are not zero
//     yet encode as {}, such as a storage block holding only an empty
//     accessModes list, an omitzero struct whose scalar pointer is unset and
//     whose list is empty, one whose list of API structs is empty, or one
//     whose struct pointer is unset beside an empty list, and blocks inside
//     omitzero structs, list items and map values.
//
// A new field that can be applied as {} therefore fails here until it is
// shaped in apply_shape.go or exempted with a reason.
func TestChildSpecsApplyNoEmptyBlocks(t *testing.T) {
	t.Parallel()

	exempted := map[string]bool{}
	check := func(typ reflect.Type, input string, in reflect.Value) {
		t.Helper()
		ptr := reflect.New(typ)
		ptr.Elem().Set(in)
		out := ptr.MethodByName("ForApply").Call(nil)[0].Interface()
		raw, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		for _, path := range emptyObjects(doc, "") {
			key, ok := exemptKey(path)
			exempted[key] = true
			if !ok {
				t.Errorf("%s with %s still applies %s as {}: shape it in "+
					"ForApply, or add it to applyExempt with a reason",
					typ.Name(), input, path)
			}
		}
	}
	for _, spec := range []any{TableGroupSpec{}, ShardSpec{}, CellSpec{}, TopoServerSpec{}} {
		typ := reflect.TypeOf(spec)
		var blocks [][]applyStep
		emptyBlocks(typ, nil, map[reflect.Type]bool{}, &blocks)
		if len(blocks) == 0 {
			t.Fatalf("%s: found no blocks; the walk is broken", typ.Name())
		}
		for _, block := range blocks {
			zero := reflect.New(typ).Elem()
			check(typ, stepsPath(block)+" empty", withEmptyBlock(zero, block))
		}
		for _, form := range deepEmptyForms {
			check(typ, "every block deep-empty ("+form.name+")",
				deepEmpty(typ, form, map[reflect.Type]bool{}))
		}
	}
	for key := range applyExempt {
		if !exempted[key] {
			t.Errorf("applyExempt lists %s, which no child spec applies as {}; remove it", key)
		}
	}
}

// exemptKey returns the applyExempt key that covers path, and whether one
// does. A key starting with "*" covers every path ending in the rest of it.
func exemptKey(path string) (string, bool) {
	if _, ok := applyExempt[path]; ok {
		return path, true
	}
	for key := range applyExempt {
		if suffix, ok := strings.CutPrefix(key, "*"); ok && strings.HasSuffix(path, suffix) {
			return key, true
		}
	}
	return path, false
}

// emptyForm chooses how deepEmpty fills what it may leave unset.
type emptyForm struct {
	name string
	// scalars allocates pointers to anything but a struct.
	scalars bool
	// structs allocates pointers to structs.
	structs bool
	// elements gives a list or map one deep-empty element when the element
	// is one of this package's structs or encodes as {}.
	elements bool
}

// deepEmptyForms are the forms TestChildSpecsApplyNoEmptyBlocks tries. Each
// reaches shapes the others cannot: an element hides the empty list it is in,
// and an allocated struct pointer hides the {} its parent would encode as
// without it.
var deepEmptyForms = []emptyForm{
	{name: "all pointers set, one element per list", scalars: true, structs: true, elements: true},
	{name: "scalar pointers unset, one element per list", structs: true, elements: true},
	{name: "scalar pointers unset, lists empty", structs: true},
	{name: "all pointers unset, lists empty"},
}

// deepEmpty returns the deep-empty value of t in form: pointers allocated as
// form says, lists and maps non-nil and empty, except that with
// form.elements a list or map gets one deep-empty element, under the map key
// "k", when the element is one of this package's structs or encodes as {}. A
// type already being built further up is left zero, which ends recursive
// types.
func deepEmpty(t reflect.Type, form emptyForm, building map[reflect.Type]bool) reflect.Value {
	out := reflect.New(t).Elem()
	if building[t] {
		return out
	}
	building[t] = true
	defer delete(building, t)
	switch t.Kind() {
	case reflect.Pointer:
		if isStruct := t.Elem().Kind() == reflect.Struct; isStruct && !form.structs ||
			!isStruct && !form.scalars {
			break
		}
		elem := reflect.New(t.Elem())
		elem.Elem().Set(deepEmpty(t.Elem(), form, building))
		out.Set(elem)
	case reflect.Struct:
		for i := range t.NumField() {
			f := t.Field(i)
			if !f.IsExported() || f.Tag.Get("json") == "-" {
				continue
			}
			out.Field(i).Set(deepEmpty(f.Type, form, building))
		}
	case reflect.Slice:
		elem := deepEmpty(t.Elem(), form, building)
		if form.elements && (ownStruct(t.Elem()) || encodesEmpty(elem)) {
			out.Set(reflect.Append(reflect.MakeSlice(t, 0, 1), elem))
		} else {
			out.Set(reflect.MakeSlice(t, 0, 0))
		}
	case reflect.Map:
		out.Set(reflect.MakeMap(t))
		elem := deepEmpty(t.Elem(), form, building)
		if form.elements && (ownStruct(t.Elem()) || encodesEmpty(elem)) &&
			t.Key().Kind() == reflect.String {
			key := reflect.New(t.Key()).Elem()
			key.SetString("k")
			out.SetMapIndex(key, elem)
		}
	}
	return out
}

// applyStep is one step of a path into a spec: a struct field, or the
// element of a list or map.
type applyStep struct {
	field int
	name  string
	elem  bool
}

func stepsPath(steps []applyStep) string {
	var parts []string
	for _, s := range steps {
		switch {
		case s.elem:
			parts[len(parts)-1] += s.name
		case s.name != "":
			parts = append(parts, s.name)
		}
	}
	return strings.Join(parts, ".")
}

// emptyBlocks appends the path of every block under t whose empty value
// encodes as {}. It walks this package's types and the parts of an affinity
// the API server merges field by field; below those, Kubernetes types are
// replaced whole by an apply.
func emptyBlocks(t reflect.Type, path []applyStep, seen map[reflect.Type]bool, out *[][]applyStep) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || seen[t] {
		return
	}
	seen[t] = true
	defer delete(seen, t)
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		name, opts, _ := strings.Cut(tag, ",")
		if name == "-" || !f.IsExported() {
			continue
		}
		step := applyStep{field: i, name: name}
		if f.Anonymous && name == "" {
			step.name = ""
		}
		here := append(slices.Clone(path), step)
		ft := f.Type
		walkInto := func(et reflect.Type, p []applyStep) {
			if et.Kind() == reflect.Pointer {
				et = et.Elem()
			}
			if et.PkgPath() == reflect.TypeFor[ShardSpec]().PkgPath() ||
				et == reflect.TypeFor[corev1.Affinity]() {
				emptyBlocks(et, p, seen, out)
			}
		}
		switch ft.Kind() {
		case reflect.Pointer:
			if ft.Elem().Kind() == reflect.Struct && encodesEmpty(reflect.New(ft.Elem())) {
				*out = append(*out, here)
			}
			walkInto(ft, here)
		case reflect.Struct:
			if !f.Anonymous && !strings.Contains(opts, "omitzero") &&
				encodesEmpty(reflect.New(ft)) {
				*out = append(*out, here)
			}
			walkInto(ft, here)
		case reflect.Slice, reflect.Map:
			elem := append(slices.Clone(here), applyStep{name: "[]", elem: true})
			if ft.Kind() == reflect.Map {
				elem[len(elem)-1].name = ".*"
			}
			walkInto(ft.Elem(), elem)
		}
	}
}

// ownStruct reports whether t, or what it points to, is a struct of this
// package.
func ownStruct(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct && t.PkgPath() == reflect.TypeFor[ShardSpec]().PkgPath()
}

func encodesEmpty(v reflect.Value) bool {
	if !v.CanInterface() {
		return false
	}
	b, err := json.Marshal(v.Interface())
	return err == nil && string(b) == "{}"
}

// withEmptyBlock returns a copy of v with the block at steps set to its empty
// value, creating whatever holds it: a pointer, one list item, or one map
// value under the key "k".
func withEmptyBlock(v reflect.Value, steps []applyStep) reflect.Value {
	if len(steps) == 0 {
		if v.Kind() == reflect.Pointer {
			return reflect.New(v.Type().Elem())
		}
		return reflect.Zero(v.Type())
	}
	out := reflect.New(v.Type()).Elem()
	out.Set(v)
	switch v.Kind() {
	case reflect.Pointer:
		elem := reflect.New(v.Type().Elem())
		if !v.IsNil() {
			elem.Elem().Set(v.Elem())
		}
		elem.Elem().Set(withEmptyBlock(elem.Elem(), steps))
		return elem
	case reflect.Struct:
		f := out.Field(steps[0].field)
		f.Set(withEmptyBlock(f, steps[1:]))
	case reflect.Slice:
		item := reflect.New(v.Type().Elem()).Elem()
		list := reflect.MakeSlice(v.Type(), 1, 1)
		list.Index(0).Set(withEmptyBlock(item, steps[1:]))
		return list
	case reflect.Map:
		key := reflect.New(v.Type().Key()).Elem()
		key.SetString("k")
		m := reflect.MakeMap(v.Type())
		m.SetMapIndex(key, withEmptyBlock(reflect.New(v.Type().Elem()).Elem(), steps[1:]))
		return m
	}
	return out
}

// emptyObjects returns the paths of the empty objects in a decoded JSON
// document, with list items as [] and the values under the map key "k" as *.
func emptyObjects(doc any, path string) []string {
	var out []string
	switch v := doc.(type) {
	case map[string]any:
		if len(v) == 0 && path != "" {
			return []string{path}
		}
		for key, child := range v {
			p := key
			if key == "k" {
				p = "*"
			}
			if path != "" {
				p = path + "." + p
			}
			out = append(out, emptyObjects(child, p)...)
		}
	case []any:
		for _, child := range v {
			out = append(out, emptyObjects(child, path+"[]")...)
		}
	}
	return out
}

// walkLeaf is not zero when its list is non-nil, yet encodes as {} when the
// list is empty.
type walkLeaf struct {
	X string   `json:"x,omitempty"`
	Y []string `json:"y,omitempty"`
}

type WalkEmbedded struct {
	Config *walkLeaf `json:"config,omitempty"`
}

// walkPartial encodes as {} only when its pointer is unset and its list is
// empty.
type walkPartial struct {
	N *int32   `json:"n,omitempty"`
	L []string `json:"l,omitempty"`
}

// walkRefs encodes as {} only when its list of API structs is empty.
type walkRefs struct {
	Refs []corev1.LocalObjectReference `json:"refs,omitempty"`
}

// walkHolder encodes as {} only when its struct pointer is unset and its list
// is empty.
type walkHolder struct {
	Leaf *walkLeaf `json:"leaf,omitempty"`
	L    []string  `json:"l,omitempty"`
}

type walkSpec struct {
	WalkEmbedded
	List    []walkLeaf          `json:"list,omitempty"`
	Map     map[string]walkLeaf `json:"map,omitempty"`
	Kept    walkLeaf            `json:"kept,omitzero"`
	Partial walkPartial         `json:"partial,omitzero"`
	Refs    walkRefs            `json:"refs,omitzero"`
	Holder  walkHolder          `json:"holder,omitzero"`
}

// TestApplyGuardWalk checks the two inputs TestChildSpecsApplyNoEmptyBlocks
// builds on a type made to hold each shape the walk must reach: a pointer
// block in an embedded struct, and leaf structs that are list items, map
// values and an omitzero field.
func TestApplyGuardWalk(t *testing.T) {
	t.Parallel()

	var blocks [][]applyStep
	emptyBlocks(reflect.TypeFor[walkSpec](), nil, map[reflect.Type]bool{}, &blocks)
	var found []string
	for _, b := range blocks {
		found = append(found, stepsPath(b))
	}
	if !slices.Equal(found, []string{"config", "holder.leaf"}) {
		t.Errorf("zero-value blocks found %v, want [config holder.leaf]", found)
	}

	wants := [][]string{
		{"config", "holder.leaf", "kept", "list[]", "map.*", "refs.refs[]"},
		{"config", "holder.leaf", "kept", "list[]", "map.*", "partial", "refs.refs[]"},
		{"config", "holder.leaf", "kept", "partial", "refs"},
		{"holder", "kept", "partial", "refs"},
	}
	for i, form := range deepEmptyForms {
		want := wants[i]
		raw, err := json.Marshal(
			deepEmpty(reflect.TypeFor[walkSpec](), form, map[reflect.Type]bool{}).Interface(),
		)
		if err != nil {
			t.Fatal(err)
		}
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		got := emptyObjects(doc, "")
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("deep-empty input (%s) applies %v as {}, want %v (from %s)",
				form.name, got, want, raw)
		}
	}
}
