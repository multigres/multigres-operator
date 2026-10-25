package ownership

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/go-logr/logr"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/certs"
)

func TestOwnerOf(t *testing.T) {
	orig := Fields
	t.Cleanup(func() { Fields = orig })
	Fields = []Field{
		{Kind: "Shard", Path: ".status.phase", Manager: "phase-owner"},
		{Kind: "Shard", Path: ".status.conditions", Manager: "conditions-owner"},
		{
			Kind:    "Shard",
			Path:    `.status.conditions[type="StorageClassValid"]`,
			Manager: "storage-owner",
		},
		{Kind: "Cell", Path: ".status.phase", Manager: "cell-phase-owner"},
	}

	tests := map[string]struct {
		kind, path string
		want       string
	}{
		"exact match": {"Shard", ".status.phase", "phase-owner"},
		"longest prefix wins": {
			"Shard",
			`.status.conditions[type="StorageClassValid"].status`,
			"storage-owner",
		},
		"shorter row for other type": {
			"Shard",
			`.status.conditions[type="Other"].status`,
			"conditions-owner",
		},
		"sub-field by dot":         {"Shard", ".status.phase.detail", "phase-owner"},
		"boundary miss":            {"Shard", ".status.phaseX", ""},
		"wrong kind misses":        {"TopoServer", ".status.phase", ""},
		"kind selects its own row": {"Cell", ".status.phase", "cell-phase-owner"},
		"unknown path misses":      {"Shard", ".status.message", ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := OwnerOf(tc.kind, tc.path)
			require.Equal(t, tc.want != "", ok)
			require.Equal(t, tc.want, got.Manager)
		})
	}
}

func TestManagersSortedAndDistinct(t *testing.T) {
	m := Managers()
	require.Len(t, m, 9)
	require.True(t, slices.IsSorted(m))
	require.Len(t, slices.Compact(slices.Clone(m)), len(m))
}

func TestIsLegacyEntry(t *testing.T) {
	entry := func(manager, sub string) metav1.ManagedFieldsEntry {
		return metav1.ManagedFieldsEntry{
			Manager:     manager,
			Operation:   metav1.ManagedFieldsOperationApply,
			Subresource: sub,
		}
	}
	for _, tc := range []struct {
		manager, sub string
		want         bool
	}{
		{ManagerChildWriter, "", false},
		{ManagerChildWriter, "status", true},
		{LegacyResourceHandler, "", false},
		{LegacyResourceHandler, "status", true},
		{LegacyResourceHandlerGuard, "", true},
		{LegacyResourceHandlerGuard, "status", true},
		{LegacyResourceHandlerReadiness, "status", true},
		{LegacyOperatorGuard, "status", true},
		{"kubelet", "status", false},
	} {
		require.Equal(t, tc.want, IsLegacyEntry(entry(tc.manager, tc.sub)),
			"%s on %q", tc.manager, tc.sub)
	}
	for _, m := range Managers() {
		require.False(t, IsLegacyEntry(entry(m, "status")), m)
		require.False(t, IsLegacyEntry(entry(m, "")), m)
	}
	// Certificates are applied under the child writer's name on the main
	// resource, which is live.
	require.Equal(t, ManagerChildWriter, certs.FieldOwner)
}

func countingClient(t *testing.T) (client.Client, *int) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	calls := 0
	c := interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
				calls++
				return nil
			},
		},
	)
	return c, &calls
}

// shardWithStatusManagers builds a Shard whose status entries, one per
// manager, all claim .status.phase, so every legacy entry is co-owned.
func shardWithStatusManagers(managers ...string) *multigresv1alpha1.Shard {
	s := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "default", ResourceVersion: "5"},
	}
	for _, m := range managers {
		s.ManagedFields = append(s.ManagedFields, metav1.ManagedFieldsEntry{
			Manager:     m,
			Operation:   metav1.ManagedFieldsOperationApply,
			Subresource: "status",
			FieldsType:  "FieldsV1",
			FieldsV1:    metav1.NewFieldsV1(`{"f:status":{"f:phase":{}}}`),
		})
	}
	return s
}

func TestOwns(t *testing.T) {
	s := shardWithStatusManagers(ManagerShard)
	require.True(t, Owns(s, ManagerShard, ".status.phase"))
	require.True(t, Owns(s, ManagerShard, ".status"))
	require.False(t, Owns(s, ManagerShardGuard, ".status.phase"), "wrong manager")
	require.False(t, Owns(s, ManagerShard, ".status.phaseX"))
	require.Equal(t, `.status.conditions[type="A/b.c"]`, ConditionPath("A/b.c"))

	// A claim on a container alone owns nothing below it: the apiserver
	// records an applied empty object, or the rest of a granular map, that way.
	container := shardWithStatusManagers()
	container.ManagedFields = []metav1.ManagedFieldsEntry{
		currentEntry(ManagerShardGuard, `{"f:status":{"f:conditions":{}}}`),
	}
	require.False(t, Owns(container, ManagerShardGuard, ConditionPath("X")))
	granular := shardWithStatusManagers()
	granular.ManagedFields = []metav1.ManagedFieldsEntry{currentEntry(ManagerShardGuard,
		`{"f:status":{"f:conditions":{".":{},"k:{\"type\":\"Y\"}":{".":{}}}}}`)}
	require.True(t, Owns(granular, ManagerShardGuard, ConditionPath("Y")))
	require.False(t, Owns(granular, ManagerShardGuard, ConditionPath("X")))
}

// patchCapture records the body of every Patch and lets it succeed.
func patchCapture(t *testing.T) (client.Client, *[]string) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	var bodies []string
	c := interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		interceptor.Funcs{
			Patch: func(
				_ context.Context, _ client.WithWatch, obj client.Object,
				p client.Patch, _ ...client.PatchOption,
			) error {
				raw, err := p.Data(obj)
				require.NoError(t, err)
				bodies = append(bodies, string(raw))
				return nil
			},
		},
	)
	return c, &bodies
}

func legacyEntry(
	manager string,
	op metav1.ManagedFieldsOperationType,
	fields string,
) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager:     manager,
		Operation:   op,
		APIVersion:  "multigres.com/v1alpha1",
		Subresource: "status",
		FieldsType:  "FieldsV1",
		FieldsV1:    metav1.NewFieldsV1(fields),
	}
}

func currentEntry(manager, fields string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager:     manager,
		Operation:   metav1.ManagedFieldsOperationApply,
		APIVersion:  "multigres.com/v1alpha1",
		Subresource: "status",
		FieldsType:  "FieldsV1",
		FieldsV1:    metav1.NewFieldsV1(fields),
	}
}

// dropOps runs DropLegacy over entries and returns the ops of the one patch
// it sent, or nil when it sent none.
func dropOps(t *testing.T, entries ...metav1.ManagedFieldsEntry) []patchOp {
	t.Helper()
	s := shardWithStatusManagers()
	s.ManagedFields = entries
	c, bodies := patchCapture(t)
	require.NoError(t, DropLegacy(t.Context(), c, s))
	if len(*bodies) == 0 {
		return nil
	}
	require.Len(t, *bodies, 1)
	var ops []patchOp
	require.NoError(t, json.Unmarshal([]byte((*bodies)[0]), &ops))
	return ops
}

func removed(ops []patchOp) []string {
	var out []string
	for _, op := range ops {
		if op.Op == "remove" {
			out = append(out, op.Path)
		}
	}
	return out
}

// The coverage rule: a legacy field is covered by a current claim on it or on
// anything below it; never by an ancestor, a sibling, or the bare .status.
func TestDropLegacy_Coverage(t *testing.T) {
	update := metav1.ManagedFieldsOperationUpdate
	for name, tc := range map[string]struct {
		legacy, current string
		want            bool
	}{
		"same field": {
			`{"f:status":{"f:phase":{}}}`, `{"f:status":{"f:phase":{}}}`, true,
		},
		"bare status container ignored": {
			`{"f:status":{".":{},"f:phase":{}}}`, `{"f:status":{"f:phase":{}}}`, true,
		},
		"parent covered by a claim below it": {
			`{"f:status":{".":{},"f:podRoles":{".":{},"f:pod-0":{}}}}`,
			`{"f:status":{"f:podRoles":{"f:pod-0":{}}}}`,
			true,
		},
		"container claim covers nothing below it": {
			`{"f:status":{"f:postgresConfig":{"f:error":{}}}}`,
			`{"f:status":{"f:postgresConfig":{}}}`,
			false,
		},
		"sibling does not cover": {
			`{"f:status":{"f:postgresConfig":{"f:error":{},"f:lastAppliedAt":{}}}}`,
			`{"f:status":{"f:postgresConfig":{"f:lastAppliedAt":{}}}}`,
			false,
		},
		"another key does not cover": {
			`{"f:status":{"f:podRoles":{"f:pod-0":{},"f:pod-1":{}}}}`,
			`{"f:status":{"f:podRoles":{"f:pod-0":{}}}}`,
			false,
		},
		"bare status container covers nothing": {
			`{"f:status":{"f:phase":{}}}`, `{"f:status":{".":{}}}`, false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ops := dropOps(t,
				currentEntry(ManagerShard, tc.current),
				legacyEntry(LegacyResourceHandler, update, tc.legacy),
			)
			if !tc.want {
				require.Nil(t, ops, "a legacy entry with an uncovered field must stay")
				return
			}
			require.Equal(t, []string{"/metadata/managedFields/1"}, removed(ops))
		})
	}
}

// Each removal tests the removed entry whole and the current entry the
// decision relied on, by manager, operation, subresource and fields, so a
// current writer withdrawing a shared field in between fails the patch. An
// entry the decision did not rely on is not tested, so a busy foreign writer
// cannot fail it.
func TestDropLegacy_GuardsTheCoveringEntries(t *testing.T) {
	kubelet := metav1.ManagedFieldsEntry{
		Manager: "kubelet", Operation: metav1.ManagedFieldsOperationUpdate,
		Subresource: "status", FieldsType: "FieldsV1",
		FieldsV1: metav1.NewFieldsV1(`{"f:status":{"f:phase":{},"f:message":{}}}`),
	}
	cur := currentEntry(ManagerShard, `{"f:status":{"f:phase":{}}}`)
	unrelated := currentEntry(ManagerShardGuard, `{"f:status":{"f:cells":{}}}`)
	old := legacyEntry(LegacyResourceHandler, metav1.ManagedFieldsOperationUpdate,
		`{"f:status":{".":{},"f:phase":{}}}`)
	ops := dropOps(t, kubelet, cur, unrelated, old)

	fields, err := json.Marshal(cur.FieldsV1)
	require.NoError(t, err)
	var curFields any
	require.NoError(t, json.Unmarshal(fields, &curFields))
	oldRaw, err := json.Marshal(old)
	require.NoError(t, err)
	var oldValue any
	require.NoError(t, json.Unmarshal(oldRaw, &oldValue))

	require.Equal(t, []patchOp{
		{Op: "test", Path: "/metadata/managedFields/3", Value: oldValue},
		{Op: "remove", Path: "/metadata/managedFields/3"},
		{Op: "test", Path: "/metadata/managedFields/1/manager", Value: ManagerShard},
		{Op: "test", Path: "/metadata/managedFields/1/operation", Value: "Apply"},
		{Op: "test", Path: "/metadata/managedFields/1/fieldsV1", Value: curFields},
		{Op: "test", Path: "/metadata/managedFields/1/subresource", Value: "status"},
	}, ops)
}

// Removals and guards are emitted from the highest index down, so a removal
// never shifts an index a later op names.
func TestDropLegacy_OpsDescend(t *testing.T) {
	update := metav1.ManagedFieldsOperationUpdate
	ops := dropOps(t,
		legacyEntry(LegacyResourceHandler, update, `{"f:status":{"f:phase":{}}}`),
		currentEntry(ManagerShard, `{"f:status":{"f:phase":{},"f:message":{}}}`),
		legacyEntry(ManagerChildWriter, update, `{"f:status":{"f:message":{}}}`),
	)
	require.Equal(t, []string{"/metadata/managedFields/2", "/metadata/managedFields/0"},
		removed(ops))
	last := -1
	for _, op := range ops {
		i, err := strconv.Atoi(strings.Split(strings.TrimPrefix(op.Path,
			"/metadata/managedFields/"), "/")[0])
		require.NoError(t, err)
		if last >= 0 {
			require.LessOrEqual(t, i, last, "ops out of order: %+v", ops)
		}
		last = i
	}
}

// logSink records whether each call was Info or Error.
type logSink struct{ calls *[]string }

func (logSink) Init(logr.RuntimeInfo) {}
func (logSink) Enabled(int) bool      { return true }
func (s logSink) Info(int, string, ...any) {
	*s.calls = append(*s.calls, "info")
}

func (s logSink) Error(error, string, ...any) {
	*s.calls = append(*s.calls, "error")
}
func (s logSink) WithValues(...any) logr.LogSink { return s }
func (s logSink) WithName(string) logr.LogSink   { return s }

func statusError(code int32, reason metav1.StatusReason, d *metav1.StatusDetails) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure, Code: code, Reason: reason, Details: d,
	}}
}

// DropLegacy wraps only the shape the apiserver gives a JSON patch it cannot
// apply (422 Invalid, empty details) in ErrManagedFieldsChanged and returns
// every other error as it came; TidyLegacy logs the first at Info and the rest
// at Error. The real errors are pinned in the envtest suite.
func TestTidyLegacy_ClassifiesPatchErrors(t *testing.T) {
	unapplicable := statusError(http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
		&metav1.StatusDetails{})
	for name, tc := range map[string]struct {
		err         error
		wantChanged bool
		wantLog     string
	}{
		"patch not applicable": {unapplicable, true, "info"},
		"422 naming the object": {
			statusError(http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
				&metav1.StatusDetails{Name: "s", Kind: "shards"}),
			false, "error",
		},
		"422 with only a name": {
			statusError(http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
				&metav1.StatusDetails{Name: "s"}),
			false, "error",
		},
		"422 with only causes": {
			statusError(http.StatusUnprocessableEntity, metav1.StatusReasonInvalid,
				&metav1.StatusDetails{Causes: []metav1.StatusCause{{Message: "denied"}}}),
			false, "error",
		},
		"webhook denial": {
			statusError(http.StatusForbidden, metav1.StatusReasonForbidden, nil), false, "error",
		},
		"transport": {errors.New("connection reset"), false, "error"},
	} {
		t.Run(name, func(t *testing.T) {
			s := shardWithStatusManagers(ManagerChildWriter, ManagerShard)
			c := patchFails(t, tc.err)
			err := DropLegacy(t.Context(), c, s)
			require.Equal(t, tc.wantChanged, errors.Is(err, ErrManagedFieldsChanged), "%v", err)
			if !tc.wantChanged {
				require.Same(t, tc.err, err, "other errors are returned unchanged")
			}
			var calls []string
			TidyLegacy(log.IntoContext(t.Context(), logr.New(logSink{calls: &calls})), c, s)
			require.Equal(t, []string{tc.wantLog}, calls)
		})
	}
}

func patchFails(t *testing.T, err error) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	return interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		interceptor.Funcs{
			Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
				return err
			},
		},
	)
}

// A skipped write still tidies: the skip is the only thing a writer whose
// value never changes runs, so a tidy-up that failed once is retried there.
func TestSkipUnchanged(t *testing.T) {
	cond := `{"f:status":{"f:conditions":{"k:{\"type\":\"X\"}":{".":{},"f:status":{}}}}}`
	owned := shardWithStatusManagers()
	owned.ManagedFields = []metav1.ManagedFieldsEntry{
		currentEntry(ManagerShardGuard, cond),
		currentEntry(LegacyResourceHandlerGuard, cond),
	}
	notOwned := shardWithStatusManagers()
	notOwned.ManagedFields = []metav1.ManagedFieldsEntry{
		currentEntry(LegacyResourceHandlerGuard, cond),
	}
	for name, tc := range map[string]struct {
		obj       *multigresv1alpha1.Shard
		unchanged bool
		wantSkip  bool
	}{
		"unchanged and owned skips and tidies": {owned, true, true},
		"changed writes":                       {owned, false, false},
		"owned by another entry writes":        {notOwned, true, false},
	} {
		t.Run(name, func(t *testing.T) {
			c, calls := countingClient(t)
			got := SkipUnchanged(t.Context(), c, tc.obj, ManagerShardGuard, ConditionPath("X"),
				tc.unchanged)
			require.Equal(t, tc.wantSkip, got)
			want := 0
			if tc.wantSkip {
				want = 1
			}
			require.Equal(t, want, *calls, "tidy-up patches")
		})
	}
}
