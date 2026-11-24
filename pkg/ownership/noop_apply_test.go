package ownership

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

func set(t *testing.T, fields string) *fieldpath.Set {
	t.Helper()
	s := &fieldpath.Set{}
	require.NoError(t, s.FromJSON(strings.NewReader(fields)))
	return s
}

func doc(t *testing.T, raw string) any {
	t.Helper()
	v, err := toJSON(json.RawMessage(raw))
	require.NoError(t, err)
	return v
}

// ownerRef is the managedFields entry of an apply that sets one
// ownerReference, keyed by uid, owning each of its fields.
const ownerRef = `{"f:metadata":{"f:ownerReferences":{"k:{\"uid\":\"u\"}":{".":{},` +
	`"f:apiVersion":{},"f:controller":{},"f:kind":{},"f:name":{},"f:uid":{}}}}}`

func ownerRefDoc(controller bool) string {
	return `{"metadata":{"ownerReferences":[{"apiVersion":"v1","kind":"K","name":"n","uid":"u",` +
		`"controller":` + strconv.FormatBool(controller) + `}]}}`
}

// TestMatches covers the comparison an apply is skipped on, for each shape a
// skipped change could hide in: an object set to {} where it was absent or
// populated and the reverse, a field applied as null, which the API server
// stores as null and takes ownership of, a list item's content, keyed (an
// ownerReference), atomic or a set value, a keyed list reordered, and
// integers too large for a float64 to tell apart.
func TestMatches(t *testing.T) {
	const selector = `{"f:spec":{"f:selector":{}}}`
	const env = `{"f:env":{"k:{\"name\":\"A\"}":{".":{},"f:name":{},"f:value":{}},` +
		`"k:{\"name\":\"B\"}":{".":{},"f:name":{},"f:value":{}}}}`
	tests := map[string]struct {
		owned, want, have string
		match             bool
	}{
		"{} stored as {}": {
			selector, `{"spec":{"selector":{}}}`, `{"spec":{"selector":{}}}`, true,
		},
		"{} over a populated value": {
			selector,
			`{"spec":{"selector":{}}}`,
			`{"spec":{"selector":{"matchLabels":{"a":"b"}}}}`,
			false,
		},
		"a populated value over {}": {
			selector,
			`{"spec":{"selector":{"matchLabels":{"a":"b"}}}}`,
			`{"spec":{"selector":{}}}`,
			false,
		},
		"{} where the field is absent and not owned": {
			`{"f:spec":{"f:replicas":{}}}`,
			`{"spec":{"replicas":1,"selector":{}}}`,
			`{"spec":{"replicas":1}}`,
			false,
		},
		"a map of nils where the field is absent": {
			`{"f:spec":{"f:replicas":{}}}`,
			`{"spec":{"replicas":1,"selector":{"matchLabels":null}}}`,
			`{"spec":{"replicas":1}}`,
			false,
		},
		"null where another manager's value is stored": {
			`{"f:spec":{"f:replicas":{}}}`,
			`{"spec":{"replicas":1,"paused":null}}`,
			`{"spec":{"replicas":1,"paused":true}}`,
			false,
		},
		"null over an owned value": {
			`{"f:spec":{"f:paused":{}}}`,
			`{"spec":{"paused":null}}`,
			`{"spec":{"paused":true}}`,
			false,
		},
		"null where null is stored and owned": {
			`{"f:spec":{"f:paused":{}}}`,
			`{"spec":{"paused":null}}`,
			`{"spec":{"paused":null}}`,
			true,
		},
		"null where the owned field is absent": {
			`{"f:spec":{"f:paused":{}}}`, `{"spec":{"paused":null}}`, `{"spec":{}}`, false,
		},
		"absent where {} is stored and owned": {
			selector, `{"spec":{}}`, `{"spec":{"selector":{}}}`, false,
		},
		"a keyed list in order": {
			env,
			`{"env":[{"name":"A","value":"1"},{"name":"B","value":"$(A)"}]}`,
			`{"env":[{"name":"A","value":"1"},{"name":"B","value":"$(A)"}]}`,
			true,
		},
		"a keyed list reordered": {
			env,
			`{"env":[{"name":"B","value":"$(A)"},{"name":"A","value":"1"}]}`,
			`{"env":[{"name":"A","value":"1"},{"name":"B","value":"$(A)"}]}`,
			false,
		},
		"another writer's item between ours": {
			env,
			`{"env":[{"name":"A","value":"1"},{"name":"B","value":"$(A)"}]}`,
			`{"env":[{"name":"A","value":"1"},{"name":"X","value":"x"},{"name":"B","value":"$(A)"}]}`,
			true,
		},
		"an item dropped from desired": {
			env,
			`{"env":[{"name":"A","value":"1"}]}`,
			`{"env":[{"name":"A","value":"1"},{"name":"B","value":"$(A)"}]}`,
			false,
		},
		"a scalar where subfields are owned": {
			`{"f:x":{"f:y":{}}}`, `{"x":"s"}`, `{"x":{"y":1}}`, false,
		},
		"an owned element a map cannot hold": {
			`{"f:spec":{"f:a":{},"i:0":{}}}`, `{"spec":{"a":1}}`, `{"spec":{"a":1}}`, false,
		},
		"null where subfields are owned": {
			`{"f:spec":{"f:x":{"f:y":{}}}}`, `{"spec":{"x":null}}`, `{"spec":{"x":null}}`, false,
		},
		"an owned element a list cannot hold": {
			`{"f:l":{"f:x":{},"v:\"a\"":{}}}`, `{"l":["a"]}`, `{"l":["a"]}`, false,
		},
		"a keyed list item without its key": {
			`{"f:l":{"k:{\"name\":\"a\"}":{".":{},"f:name":{}}}}`,
			`{"l":[{"name":"a"},"x"]}`,
			`{"l":[{"name":"a"}]}`,
			false,
		},
		"an ownerReference whose controller flag changed": {
			ownerRef, ownerRefDoc(true), ownerRefDoc(false), false,
		},
		"an ownerReference unchanged": {
			ownerRef, ownerRefDoc(true), ownerRefDoc(true), true,
		},
		"an atomic list item whose content changed": {
			`{"f:l":{"i:0":{}}}`, `{"l":[{"a":1}]}`, `{"l":[{"a":2}]}`, false,
		},
		"an atomic list item unchanged": {
			`{"f:l":{"i:0":{}}}`, `{"l":[{"a":1}]}`, `{"l":[{"a":1}]}`, true,
		},
		"atomic list items in their positions": {
			`{"f:l":{"i:0":{},"i:1":{}}}`,
			`{"l":[{"a":1},{"b":2}]}`,
			`{"l":[{"a":1},{"b":2}]}`,
			true,
		},
		"a set item whose value changed": {
			`{"f:l":{"v:\"a\"":{}}}`, `{"l":["b"]}`, `{"l":["a"]}`, false,
		},
		"an owned item swapped for another writer's": {
			env,
			`{"env":[{"name":"A","value":"1"},{"name":"X","value":"x"}]}`,
			`{"env":[{"name":"A","value":"1"},{"name":"X","value":"x"},{"name":"B","value":"$(A)"}]}`,
			false,
		},
		"an owned item no longer stored": {
			env,
			`{"env":[{"name":"A","value":"1"},{"name":"B","value":"$(A)"}]}`,
			`{"env":[]}`,
			false,
		},
		"a keyed item stored twice compares the first": {
			`{"f:l":{"k:{\"name\":\"a\"}":{".":{},"f:name":{},"f:v":{}}}}`,
			`{"l":[{"name":"a","v":1}]}`,
			`{"l":[{"name":"a","v":1},{"name":"a","v":2}]}`,
			true,
		},
		"int64 values a float64 cannot tell apart": {
			`{"f:n":{}}`, `{"n":9007199254740993}`, `{"n":9007199254740992}`, false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := matches(doc(t, tc.want), doc(t, tc.have), set(t, tc.owned))
			require.Equal(t, tc.match, got)
		})
	}
}

// TestMatchesLargeKeyedList checks that a keyed list is indexed once rather
// than searched per owned item, by comparing a list large enough that a
// quadratic comparison would stand out.
func TestMatchesLargeKeyedList(t *testing.T) {
	const n = 20000
	var owned, items strings.Builder
	owned.WriteString(`{"f:env":{`)
	items.WriteString(`{"env":[`)
	for i := range n {
		if i > 0 {
			owned.WriteString(",")
			items.WriteString(",")
		}
		name := "V" + strconv.Itoa(i)
		owned.WriteString(`"k:{\"name\":\"` + name + `\"}":{".":{},"f:name":{}}`)
		items.WriteString(`{"name":"` + name + `"}`)
	}
	owned.WriteString(`}}`)
	items.WriteString(`]}`)
	d := doc(t, items.String())
	require.True(t, matches(d, d, set(t, owned.String())))
}

// The write memory holds only what it must: an object deleted through the
// wrapper is forgotten, and so is any write older than writeMemory, so
// objects deleted by anything else do not stay in it for the life of the
// process.
func TestNoopApplyWriteMemoryBounds(t *testing.T) {
	configMap := func(name string) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", ResourceVersion: "5"},
		}
	}
	n := newNoopApplyClient(fake.NewClientBuilder().WithObjects(configMap("deleted")).Build())

	n.sent(configMap("deleted"), "m", "", nil)
	require.Len(t, n.written, 1)
	require.NoError(t, n.Delete(t.Context(), configMap("deleted")))
	require.Empty(t, n.written, "a deleted object is forgotten")

	n.sent(configMap("old"), "m", "", nil)
	k, ok := n.objectKey(configMap("old"))
	require.True(t, ok)
	n.written[k][writeSlot{"m", ""}] = writeRecord{
		rv: 5,
		at: time.Now().Add(-writeMemory - time.Second),
	}
	n.lastSweep = time.Time{}
	n.sent(configMap("new"), "m", "", nil)
	require.NotContains(t, n.written, k, "a write older than writeMemory is swept")
	require.Len(t, n.written, 1)
}

// TestApplyChangesNothing covers what an apply of a typed object is compared
// as: the object less its identity and status, and only the manager's own
// Apply entry for the subresource being written.
func TestApplyChangesNothing(t *testing.T) {
	const owned = `{"f:data":{"f:k":{}}}`
	configMap := func(entries ...metav1.ManagedFieldsEntry) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", ManagedFields: entries},
			Data:       map[string]string{"k": "v"},
		}
	}
	entry := func(op metav1.ManagedFieldsOperationType, sub string) metav1.ManagedFieldsEntry {
		return metav1.ManagedFieldsEntry{
			Manager: "m", Operation: op, Subresource: sub,
			FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(owned),
		}
	}
	live := configMap(entry(metav1.ManagedFieldsOperationApply, ""))
	desired := func() *corev1.ConfigMap { return configMap() }

	require.True(t, applyChangesNothing(desired(), live, "m", ""),
		"name and namespace are identity, not fields the apply owns")

	withStatus := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	livePod := withStatus.DeepCopy()
	livePod.ManagedFields = []metav1.ManagedFieldsEntry{{
		Manager: "m", Operation: metav1.ManagedFieldsOperationApply,
		FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(`{"f:spec":{}}`),
	}}
	require.True(t, applyChangesNothing(withStatus, livePod, "m", ""),
		"status in an apply to the object is ignored by the API server")

	require.False(t, applyChangesNothing(desired(), configMap(), "m", ""),
		"a manager with no entry sets what the apply holds")
	require.False(t, applyChangesNothing(withStatus, withStatus, "m", "status"),
		"a status apply by a manager with no entry sets what it holds")
	require.False(
		t,
		applyChangesNothing(
			desired(),
			configMap(entry(metav1.ManagedFieldsOperationUpdate, "")),
			"m",
			"",
		),
		"an Update entry is not what an apply owns",
	)
	require.False(
		t,
		applyChangesNothing(
			desired(),
			configMap(entry(metav1.ManagedFieldsOperationApply, "status")),
			"m",
			"",
		),
		"an entry for another subresource is not compared",
	)

	empty := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"},
	}
	n := newNoopApplyClient(fake.NewClientBuilder().WithObjects(empty.DeepCopy()).Build())
	_, skip := n.compare(t.Context(), empty, "", "")
	require.False(t, skip, "an apply with no field manager is always sent")
	_, skip = n.compare(t.Context(), empty, "m", "")
	require.True(t, skip, "an empty apply by a manager holding nothing changes nothing")
}
