package ownership

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// SkipNoopApplies wraps c so that a server-side apply that would change
// nothing is not sent. An apply that changes nothing still costs a request the
// API server authenticates, admits and diffs, and a reconciler applies every
// child on every pass, so at thousands of objects a fleet at rest would keep
// the API server busy for nothing.
//
// An apply is skipped when the object as the cache holds it already is what
// the apply would leave: the field manager's managedFields entry claims
// exactly the fields the apply sets, and each of them holds the applied value
// (see applyChangesNothing). The skipped call fills obj from the cache, as a
// sent apply fills it from the response. Every other request passes through,
// and so does an apply carrying a resourceVersion or UID, whose precondition
// only the API server can check, and a raw apply patch, whose content need not
// be obj's.
//
// The comparison is against managedFields rather than a hash the operator
// stores on each child, because ownership is what the API server itself
// changes when someone else writes: an update by another client that changes
// a field this manager set takes that field off this manager's entry, so the
// next pass sees the entry no longer matching and applies again, putting the
// value back. A stored hash would say nothing had changed and leave the edit
// in place. It also needs no annotation on any child, so nothing is written to
// adopt it.
//
// The cache can lag the API server, and a lagging cache can hold exactly what
// this pass wants while the API server holds something else: a status that
// went A, then B, then A again before the cache saw B. Nothing need reconcile
// the object again once the cache catches up, since a controller usually
// ignores its own status writes, so the wrapper remembers the resourceVersion
// each of its applies returned, per object, manager and subresource, and
// compares only against a cached object at least that new. An apply that
// failed may still have been stored, its response lost, so after a failure
// the next apply is sent whatever the cache holds. Until the cache has seen
// this manager's last write the apply is sent. The memory forgets an object
// that is deleted through the wrapper, and any write older than writeMemory,
// by which time an informer has long since delivered it. An object the cache
// does not hold is not forgotten: one just created reads that way, and
// forgetting a later write to it would let the cache's view of the create
// stand in for what is stored.
//
// A change someone else made that the cache has not yet seen can still let one
// apply be skipped. For an object the controller watches, the informer then
// delivers the change and the controller reconciles again. For a status the
// controller does not watch, the change stands until the object's next pass,
// as one made just after a pass does when every apply is sent.
//
// Controllers wrap their client with it at setup, so every write goes
// through it, and record events the same way: Applied only for an apply that
// changed something (see Apply), and Synced once per generation, when a pass
// completes for it (see SyncedEvents). A pass that finds the object already in its desired state
// has nothing to report, and an event per pass per object is an API write per
// pass.
//
// resourceVersions are compared as integers. The API server documents them as
// opaque; with the etcd3 storage backend, which every API server here and
// envtest run, they are etcd revisions and increase with every write. A
// resourceVersion that does not parse as an integer means the apply is sent.
func SkipNoopApplies(c client.Client) client.Client {
	if _, ok := c.(*noopApplyClient); ok {
		return c
	}
	return newNoopApplyClient(c)
}

// writeMemory is how long the wrapper remembers a write the cache has not
// shown it.
const writeMemory = 10 * time.Minute

func newNoopApplyClient(c client.Client) *noopApplyClient {
	return &noopApplyClient{Client: c, written: map[objectKey]map[writeSlot]writeRecord{}}
}

type noopApplyClient struct {
	client.Client

	mu sync.Mutex
	// written holds this client's last apply to each object, per manager and
	// subresource, until the cache has caught up with it.
	written   map[objectKey]map[writeSlot]writeRecord
	lastSweep time.Time
}

type objectKey struct {
	gk  schema.GroupKind
	key client.ObjectKey
}

type writeSlot struct {
	manager     string
	subresource string
}

// writeRecord is one remembered apply: the resourceVersion it returned, or
// unknown when it failed and may still have been stored.
type writeRecord struct {
	rv      uint64
	unknown bool
	at      time.Time
}

func (c *noopApplyClient) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	if patch.Type() != types.ApplyPatchType {
		return c.Client.Patch(ctx, obj, patch, opts...)
	}
	o := &client.PatchOptions{}
	o.ApplyOptions(opts)
	if patch == client.Apply {
		if live, ok := c.unchanged(ctx, obj, o.FieldManager, ""); ok {
			fill(obj, live)
			return nil
		}
	}
	err := c.Client.Patch(ctx, obj, patch, opts...)
	c.sent(obj, o.FieldManager, "", err)
	return err
}

func (c *noopApplyClient) Delete(
	ctx context.Context,
	obj client.Object,
	opts ...client.DeleteOption,
) error {
	err := c.Client.Delete(ctx, obj, opts...)
	c.forget(obj)
	return err
}

func (c *noopApplyClient) Status() client.SubResourceWriter {
	return &noopApplyStatusWriter{SubResourceWriter: c.Client.Status(), c: c}
}

type noopApplyStatusWriter struct {
	client.SubResourceWriter
	c *noopApplyClient
}

func (w *noopApplyStatusWriter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.SubResourcePatchOption,
) error {
	if patch.Type() != types.ApplyPatchType {
		return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}
	o := &client.SubResourcePatchOptions{}
	o.ApplyOptions(opts)
	if patch == client.Apply {
		if live, ok := w.c.unchanged(ctx, obj, o.FieldManager, "status"); ok {
			fill(obj, live)
			return nil
		}
	}
	err := w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	w.c.sent(obj, o.FieldManager, "status", err)
	return err
}

// unchanged reads obj from the cache and returns it when applying obj under
// manager to subresource would change nothing. Anything that keeps it from
// deciding, such as an object the cache cannot see, means the apply is sent.
func (c *noopApplyClient) unchanged(
	ctx context.Context,
	obj client.Object,
	manager, subresource string,
) (client.Object, bool) {
	live, skip := c.compare(ctx, obj, manager, subresource)
	if !skip {
		return nil, false
	}
	return live, true
}

// compare reads obj from the cache, returning what it holds (nil when it holds
// nothing) and whether applying obj under manager to subresource would change
// nothing.
func (c *noopApplyClient) compare(
	ctx context.Context,
	obj client.Object,
	manager, subresource string,
) (client.Object, bool) {
	if manager == "" {
		return nil, false
	}
	live, ok := c.read(ctx, obj)
	if !ok {
		return nil, false
	}
	conditional := obj.GetResourceVersion() != "" || obj.GetUID() != ""
	return live, !conditional && c.caughtUp(obj, live, manager, subresource) &&
		applyChangesNothing(obj, live, manager, subresource)
}

func (c *noopApplyClient) objectKey(obj client.Object) (objectKey, bool) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if gvk.Kind == "" {
		var err error
		if gvk, err = apiutil.GVKForObject(obj, c.Scheme()); err != nil {
			return objectKey{}, false
		}
	}
	return objectKey{gk: gvk.GroupKind(), key: client.ObjectKeyFromObject(obj)}, true
}

// sent records the outcome of an apply that was sent: the resourceVersion it
// returned, or, when it failed, that whatever it may have stored is unknown.
func (c *noopApplyClient) sent(obj client.Object, manager, subresource string, err error) {
	k, ok := c.objectKey(obj)
	if !ok || manager == "" {
		return
	}
	rec := writeRecord{unknown: true, at: time.Now()}
	if err == nil {
		rv, perr := strconv.ParseUint(obj.GetResourceVersion(), 10, 64)
		rec = writeRecord{rv: rv, unknown: perr != nil, at: rec.at}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweep(rec.at)
	slots := c.written[k]
	if slots == nil {
		slots = map[writeSlot]writeRecord{}
		c.written[k] = slots
	}
	slots[writeSlot{manager, subresource}] = rec
}

// sweep drops writes older than writeMemory, at most once a minute. It is
// called with c.mu held.
func (c *noopApplyClient) sweep(now time.Time) {
	if now.Sub(c.lastSweep) < time.Minute {
		return
	}
	c.lastSweep = now
	for k, slots := range c.written {
		for s, rec := range slots {
			if now.Sub(rec.at) > writeMemory {
				delete(slots, s)
			}
		}
		if len(slots) == 0 {
			delete(c.written, k)
		}
	}
}

// forget drops what is remembered of obj, which was deleted.
func (c *noopApplyClient) forget(obj client.Object) {
	k, ok := c.objectKey(obj)
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.written, k)
}

// lastWrite returns the resourceVersion of this client's last apply to obj
// under manager that the cache may not yet have seen, or 0 when there is none
// or it is unknown.
func (c *noopApplyClient) lastWrite(obj client.Object, manager, subresource string) uint64 {
	k, ok := c.objectKey(obj)
	if !ok {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.written[k][writeSlot{manager, subresource}].rv
}

// caughtUp reports whether live, the cached object, is at least as new as
// this client's last apply to it under manager, and forgets that apply once it
// is, so the memory holds only writes the cache has yet to show. After a
// failed apply it is never caught up: only a sent apply settles what is
// stored.
func (c *noopApplyClient) caughtUp(obj, live client.Object, manager, subresource string) bool {
	k, ok := c.objectKey(obj)
	if !ok {
		return false
	}
	rv, err := strconv.ParseUint(live.GetResourceVersion(), 10, 64)
	if err != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	slot := writeSlot{manager, subresource}
	rec, found := c.written[k][slot]
	if !found {
		return true
	}
	if rec.unknown || rv < rec.rv {
		return false
	}
	delete(c.written[k], slot)
	if len(c.written[k]) == 0 {
		delete(c.written, k)
	}
	return true
}

// read returns obj as the cache holds it, and whether it does.
func (c *noopApplyClient) read(ctx context.Context, obj client.Object) (client.Object, bool) {
	var live client.Object
	var err error
	if u, ok := obj.(*unstructured.Unstructured); ok {
		live, err = c.readUnstructured(ctx, u)
	} else if l, ok := obj.DeepCopyObject().(client.Object); ok {
		live, err = l, c.Get(ctx, client.ObjectKeyFromObject(obj), l)
	} else {
		return nil, false
	}
	return live, err == nil
}

// readUnstructured reads u through its typed kind. A client reads an
// unstructured object from the API server, not the cache, so reading it as
// given would make every apply of a partial object built as unstructured cost
// a live read.
func (c *noopApplyClient) readUnstructured(
	ctx context.Context,
	u *unstructured.Unstructured,
) (client.Object, error) {
	gvk := u.GroupVersionKind()
	typed, err := c.Scheme().New(gvk)
	if err != nil {
		return nil, err
	}
	t, ok := typed.(client.Object)
	if !ok {
		return nil, fmt.Errorf("%s is not a client.Object", gvk)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(u), t); err != nil {
		return nil, err
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(t)
	if err != nil {
		return nil, err
	}
	live := &unstructured.Unstructured{Object: m}
	live.SetGroupVersionKind(gvk)
	return live, nil
}

// fill makes obj what the cache holds, keeping its kind, as a sent apply
// fills it from the response.
func fill(obj, live client.Object) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(live).Elem())
	obj.GetObjectKind().SetGroupVersionKind(gvk)
}

// Apply server-side applies obj under ManagerChildWriter, taking fields that
// other managers hold, and reports whether it changed anything, so a caller
// records an event for a change and not for every pass. An apply that would
// change nothing is not sent (see SkipNoopApplies). One that is sent changed
// something unless the resourceVersion it returns is one already known: the
// cached object's, or that of this client's own last apply, which a lagging
// cache may not hold yet. Only a client from SkipNoopApplies remembers its
// applies; with any other client a call knows only the cache.
func Apply(ctx context.Context, c client.Client, obj client.Object) (bool, error) {
	n, ok := c.(*noopApplyClient)
	if !ok {
		n = newNoopApplyClient(c)
	}
	live, skip := n.compare(ctx, obj, ManagerChildWriter, "")
	if skip {
		fill(obj, live)
		return false, nil
	}
	known := map[string]bool{}
	if live != nil {
		known[live.GetResourceVersion()] = true
	}
	if last := n.lastWrite(obj, ManagerChildWriter, ""); last != 0 {
		known[strconv.FormatUint(last, 10)] = true
	}
	err := n.Client.Patch(
		ctx,
		obj,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner(ManagerChildWriter),
	)
	n.sent(obj, ManagerChildWriter, "", err)
	if err != nil {
		return false, err
	}
	return !known[obj.GetResourceVersion()], nil
}

// MayHold reports whether manager may hold fields of obj's subresource ("" for
// the object itself) on the API server: the cache shows it holding some, or c
// remembers a write of manager's to obj that the cache has not yet shown,
// stored or possibly stored. An empty apply removes what manager holds, so a
// caller skips one only when this is false.
func MayHold(
	ctx context.Context,
	c client.Client,
	obj client.Object,
	manager, subresource string,
) bool {
	n, ok := c.(*noopApplyClient)
	if !ok {
		n = newNoopApplyClient(c)
	}
	live, found := n.read(ctx, obj)
	if !found {
		return true
	}
	if !n.caughtUp(obj, live, manager, subresource) {
		return true
	}
	for _, e := range live.GetManagedFields() {
		if e.Manager == manager && e.Subresource == subresource {
			return true
		}
	}
	return false
}

// applyChangesNothing reports whether applying desired to subresource ("" for
// the object itself) under manager would leave live as it is: manager's apply
// entry in live's managedFields claims exactly the fields desired sets, each
// of them holds in live the value desired gives it, and the items of each
// keyed list desired sets stand in live in the order desired gives them.
//
// Both of the first two are needed. Equal values alone would miss a field the
// manager set before and has since dropped from desired, which only an apply
// removes, and a field desired sets that the manager does not yet own. The
// entry alone would miss a value that changed under a field the manager still
// owns, which happens when the manager's own earlier apply set it. Order is
// needed because an apply puts a keyed list's items in the order it gives
// them, and that order means something: an env var can refer only to the vars
// before it.
func applyChangesNothing(desired, live client.Object, manager, subresource string) bool {
	var entry *metav1.ManagedFieldsEntry
	for _, e := range live.GetManagedFields() {
		if e.Manager == manager && e.Operation == metav1.ManagedFieldsOperationApply &&
			e.Subresource == subresource {
			entry = &e
		}
	}
	want, err := applyContent(desired, subresource)
	if err != nil {
		return false
	}
	if entry == nil {
		// An apply that sets nothing, by a manager that holds nothing,
		// changes nothing.
		return setsNothing(want, subresource)
	}
	owned, err := decode(*entry)
	if err != nil {
		return false
	}
	have, err := toJSON(live)
	if err != nil {
		return false
	}
	return matches(want, have, owned)
}

// matches reports whether want, the applied value at one position, is owned
// exactly by owned, the managed paths below that position, and holds the
// values have holds there.
//
// A value owned as a whole compares exactly, {} included. Whether applying {}
// replaces what is stored or leaves the defaults the API server filled in
// depends on whether the schema makes the field atomic, which this does not
// know, so {} matches only {}: a struct a builder leaves empty whose stored
// value the API server defaults is sent on every pass, and the builder sets
// the defaults instead. Erring that way costs a write; the other way would
// leave an atomic value, such as a label selector cleared to {}, unchanged.
func matches(want, have any, owned *fieldpath.Set) bool {
	switch w := want.(type) {
	case map[string]any:
		h, _ := have.(map[string]any)
		return matchesMap(w, h, owned)
	case []any:
		h, _ := have.([]any)
		return matchesList(w, h, owned)
	}
	return owned.Empty()
}

func matchesMap(want, have map[string]any, owned *fieldpath.Set) bool {
	ok := true
	check := func(pe fieldpath.PathElement) {
		if !ok {
			return
		}
		if pe.FieldName == nil {
			ok = false
			return
		}
		w, inWant := want[*pe.FieldName]
		h, inHave := have[*pe.FieldName]
		child, isChild := owned.Children.Get(pe)
		switch {
		case !inWant || !inHave:
			ok = false
		case w == nil || h == nil:
			// An apply of null sets the field to null and owns it. It
			// matches only a null stored there, with nothing owned below.
			ok = w == nil && h == nil && !isChild
		case isChild:
			ok = matches(w, h, child)
		default:
			ok = reflect.DeepEqual(w, h)
		}
	}
	// A field owned both as a whole and through its subfields is a member and
	// a child; the child pass compares it, so the member pass skips it.
	owned.Members.Iterate(func(pe fieldpath.PathElement) {
		if _, isChild := owned.Children.Get(pe); !isChild {
			check(pe)
		}
	})
	owned.Children.Iterate(check)
	if !ok {
		return false
	}
	// Every field the apply sets must be owned, null included: the API server
	// stores null for a field applied as null, taking it from any other
	// manager.
	for k := range want {
		pe := fieldpath.PathElement{FieldName: &k}
		if _, isChild := owned.Children.Get(pe); !isChild && !owned.Members.Has(pe) {
			return false
		}
	}
	return true
}

// matchesList compares a list whose items the manager owns one by one: by key
// fields, by value or by position. Each list is indexed once, so the cost is
// linear in its length.
func matchesList(want, have []any, owned *fieldpath.Set) bool {
	var keys []string
	elements := map[string]fieldpath.PathElement{}
	collect := func(pe fieldpath.PathElement) {
		if keys == nil && pe.Key != nil {
			for _, f := range *pe.Key {
				keys = append(keys, f.Name)
			}
		}
		if id, ok := elementID(pe); ok {
			elements[id] = pe
		}
	}
	owned.Members.Iterate(collect)
	owned.Children.Iterate(collect)
	if len(elements) != owned.Members.Size()+childrenOnly(owned) {
		return false
	}
	haveAt := map[string]int{}
	for i, item := range have {
		if id, ok := itemID(item, i, keys); ok {
			if _, dup := haveAt[id]; !dup {
				haveAt[id] = i
			}
		}
	}
	seen := 0
	last := -1
	for i, item := range want {
		id, ok := itemID(item, i, keys)
		if !ok {
			return false
		}
		pe, owns := elements[id]
		h, inHave := haveAt[id]
		if !owns || !inHave || h <= last {
			return false
		}
		last = h
		seen++
		if child, isChild := owned.Children.Get(pe); isChild {
			if !matches(item, have[h], child) {
				return false
			}
		} else if !reflect.DeepEqual(item, have[h]) {
			return false
		}
	}
	return seen == len(elements)
}

// childrenOnly counts the paths of owned that have children but are not
// members themselves.
func childrenOnly(owned *fieldpath.Set) int {
	n := 0
	owned.Children.Iterate(func(pe fieldpath.PathElement) {
		if !owned.Members.Has(pe) {
			n++
		}
	})
	return n
}

// elementID names a list item path element the way itemID names an item.
func elementID(pe fieldpath.PathElement) (string, bool) {
	switch {
	case pe.Key != nil:
		vals := make([]any, 0, len(*pe.Key))
		for _, f := range *pe.Key {
			vals = append(vals, f.Value.Unstructured())
		}
		return encode("k", vals)
	case pe.Value != nil:
		return encode("v", (*pe.Value).Unstructured())
	case pe.Index != nil:
		return "i" + strconv.Itoa(*pe.Index), true
	}
	return "", false
}

// itemID names item, at index i of a list, by its key fields when the list
// is keyed, else by its value; elementID uses the same names. An item a keyed
// list cannot name has no ID.
func itemID(item any, i int, keys []string) (string, bool) {
	if keys == nil {
		if _, isMap := item.(map[string]any); isMap {
			return "i" + strconv.Itoa(i), true
		}
		return encode("v", item)
	}
	m, ok := item.(map[string]any)
	if !ok {
		return "", false
	}
	vals := make([]any, 0, len(keys))
	for _, k := range keys {
		vals = append(vals, m[k])
	}
	return encode("k", vals)
}

// encode renders v as JSON with a prefix, so an int64 from a managedFields
// key and a json.Number from a decoded object name the same value.
func encode(prefix string, v any) (string, bool) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	return prefix + string(b), true
}

// setsNothing reports whether want, an apply's content, sets no field: no
// content at all, or for the status subresource an empty status.
func setsNothing(want any, subresource string) bool {
	m, ok := want.(map[string]any)
	if !ok {
		return want == nil
	}
	if subresource == "status" {
		status, _ := m["status"].(map[string]any)
		return len(status) == 0
	}
	return len(m) == 0
}

// applyContent is what applying obj sets, as decoded JSON: the object less
// its identity and server-set metadata, and less its status when applied to
// the object itself, since the API server ignores status there; only its
// status when applied to the status subresource.
func applyContent(obj client.Object, subresource string) (any, error) {
	doc, err := toJSON(obj)
	if err != nil {
		return nil, err
	}
	m, ok := doc.(map[string]any)
	if !ok {
		return nil, nil
	}
	if subresource == "status" {
		return map[string]any{"status": m["status"]}, nil
	}
	delete(m, "apiVersion")
	delete(m, "kind")
	delete(m, "status")
	if meta, ok := m["metadata"].(map[string]any); ok {
		for _, k := range []string{
			"name", "namespace", "uid", "resourceVersion", "generation",
			"creationTimestamp", "managedFields", "selfLink",
		} {
			delete(meta, k)
		}
		// Identity is all an apply's metadata carries when it sets no
		// labels, annotations or owners, and an apply owns no field for it.
		if len(meta) == 0 {
			delete(m, "metadata")
		}
	}
	return m, nil
}

// toJSON decodes v's JSON encoding, keeping numbers exact: a float64 holds
// only 53 bits, so two int64 values that differ could otherwise compare equal.
func toJSON(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var doc any
	return doc, dec.Decode(&doc)
}

// ApplyRecorded applies obj as Apply does and, when the apply changed
// something, records an Applied event for it on parent.
func ApplyRecorded(
	ctx context.Context,
	c client.Client,
	recorder record.EventRecorder,
	parent runtime.Object,
	obj client.Object,
) error {
	changed, err := Apply(ctx, c, obj)
	if err != nil {
		return err
	}
	if changed {
		recorder.Eventf(parent, "Normal", "Applied", "Applied %s %s",
			obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName())
	}
	return nil
}
