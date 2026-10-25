package suite

import (
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func mfEntry(manager, fieldsV1 string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager:    manager,
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: "multigres.com/v1alpha1",
		FieldsType: "FieldsV1",
		FieldsV1:   metav1.NewFieldsV1(fieldsV1),
	}
}

// fakeOwners maps a status path prefix to its owning manager, matching on the
// prefix the way ownership.OwnerOf does, without depending on the real table.
func fakeOwners(table map[string]string) ownerLookup {
	return func(_, path string) (string, bool) {
		for prefix, m := range table {
			if path == prefix || strings.HasPrefix(path, prefix+".") ||
				strings.HasPrefix(path, prefix+"[") {
				return m, true
			}
		}
		return "", false
	}
}

var (
	testOperators = map[string]bool{"op-a": true, "op-b": true}
	testLegacy    = map[string]bool{"old-op": true}
	isTestOp      = func(m string) bool { return testOperators[m] }
	isTestLegacy  = func(m string) bool { return testLegacy[m] }
	testOwners    = fakeOwners(map[string]string{
		".status.phase":      "op-a",
		".status.conditions": "op-b",
	})
)

func violations(kind string, entries ...metav1.ManagedFieldsEntry) []string {
	return violationsFor(kind, "obj", entries, testOwners, isTestOp, isTestLegacy)
}

func requireViolation(t *testing.T, got []string, wants ...string) {
	t.Helper()
	for _, line := range got {
		ok := true
		for _, w := range wants {
			if !strings.Contains(line, w) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
	}
	t.Fatalf("no violation containing all of %q in %q", wants, got)
}

func requireNone(t *testing.T, got []string) {
	t.Helper()
	if len(got) != 0 {
		t.Fatalf("want no violations, got %q", got)
	}
}

const specReplicas = `{"f:spec":{"f:replicas":{}}}`

func TestOwnershipViolationsLegacyManager(t *testing.T) {
	got := violations("Shard", mfEntry("old-op", specReplicas))
	requireViolation(t, got, "Shard obj", "legacy manager", "old-op")
}

func TestOwnershipViolationsUnownedStatusField(t *testing.T) {
	got := violations("Cell", mfEntry("op-a", `{"f:status":{".":{},"f:mystery":{}}}`))
	requireViolation(t, got, "Cell obj", "unowned status field", ".status.mystery", "op-a")
}

func TestOwnershipViolationsStatusOwnerMismatch(t *testing.T) {
	got := violations("TableGroup", mfEntry("op-b", `{"f:status":{"f:phase":{}}}`))
	requireViolation(t, got, "TableGroup obj", ".status.phase", "owned by op-a, claimed by op-b")
}

// An operator write without a field owner lands under the process's implicit
// manager name, which is not in the operator set.
func TestOwnershipViolationsStatusRuleCoversNonOperatorManagers(t *testing.T) {
	got := violations("TopoServer", mfEntry("some-binary", `{"f:status":{"f:phase":{}}}`))
	requireViolation(t, got, "TopoServer obj", "owned by op-a, claimed by some-binary")

	got = violations("TopoServer", mfEntry("some-binary", `{"f:status":{"f:other":{}}}`))
	requireViolation(t, got, "unowned status field", "some-binary")
}

func TestOwnershipViolationsMatchingStatusOwnerIsClean(t *testing.T) {
	requireNone(t, violations(
		"MultigresCluster",
		mfEntry("op-a", `{"f:status":{".":{},"f:phase":{}}}`),
		mfEntry(
			"op-b",
			`{"f:status":{".":{},"f:conditions":{"k:{\"type\":\"Ready\"}":{".":{},"f:status":{}}}}}`,
		),
	))
}

func TestOwnershipViolationsBareStatusPathIsIgnored(t *testing.T) {
	// Both managers claim the container; neither claims a field below it that
	// the other owns.
	requireNone(t, violations("Shard",
		mfEntry("op-a", `{"f:status":{".":{},"f:phase":{}}}`),
		mfEntry("op-b", `{"f:status":{".":{},"f:conditions":{"k:{\"type\":\"Ready\"}":{}}}}`)))

	// Only the container claimed: no owner is registered for it, and it must
	// not be reported as unowned.
	requireNone(t, violations("Shard", mfEntry("op-a", `{"f:status":{".":{}}}`)))
}

func TestOwnershipViolationsTwoOperatorsClaimOneSpecField(t *testing.T) {
	got := violations("Shard",
		mfEntry("op-a", specReplicas),
		mfEntry("op-b", specReplicas))
	requireViolation(t, got, "Shard obj", ".spec.replicas", "op-a", "op-b")
}

func TestOwnershipViolationsNonOperatorSpecOverlapIsIgnored(t *testing.T) {
	requireNone(t, violations("Shard",
		mfEntry("op-a", specReplicas),
		mfEntry("test-harness", specReplicas),
		mfEntry("test-harness-2", specReplicas)))
}

func TestOwnershipViolationsPodOperatorStatusMustResolveToThatOperator(t *testing.T) {
	cond := `{"f:status":{"f:conditions":{"k:{\"type\":\"Ready\"}":{}}}}`
	got := violations("Pod", mfEntry("op-a", cond))
	requireViolation(t, got, "Pod obj", "owned by op-b, claimed by op-a")

	got = violations("Pod", mfEntry("op-a", `{"f:status":{"f:message":{}}}`))
	requireViolation(t, got, "Pod obj", "unowned status field", "op-a")

	requireNone(t, violations("Pod", mfEntry("op-b", cond)))
}

func TestOwnershipViolationsPodKubeletStatusIsIgnored(t *testing.T) {
	requireNone(t, violations(
		"Pod",
		mfEntry(
			"kubelet",
			`{"f:status":{"f:conditions":{"k:{\"type\":\"Ready\"}":{}},"f:phase":{}}}`,
		),
	))
}

func TestOwnershipViolationsPodSpecOverlapIsNotChecked(t *testing.T) {
	requireNone(t, violations("Pod",
		mfEntry("op-a", specReplicas),
		mfEntry("op-b", specReplicas)))
}

func TestOwnershipViolationsNoDecodableClaims(t *testing.T) {
	got := violations("Cell")
	requireViolation(t, got, "Cell obj", "no decodable managedFields claims")

	got = violations("Cell", metav1.ManagedFieldsEntry{Manager: "op-a"}, mfEntry("op-a", `{}`))
	requireViolation(t, got, "no decodable managedFields claims")

	got = violations("Cell", mfEntry("op-a", `not json`))
	requireViolation(t, got, "undecodable managedFields", "op-a")
	requireViolation(t, got, "no decodable managedFields claims")
}

func TestOwnershipViolationsAreSorted(t *testing.T) {
	// Emitted in the reverse of sorted order: unowned, mismatch, legacy.
	got := violationsFor("Shard", "obj", []metav1.ManagedFieldsEntry{
		mfEntry("op-a", `{"f:status":{"f:aaa":{}}}`),
		mfEntry("op-b", `{"f:status":{"f:phase":{}}}`),
		mfEntry("zzz-legacy", specReplicas),
	}, testOwners, isTestOp, func(m string) bool { return m == "zzz-legacy" })

	if len(got) != 3 {
		t.Fatalf("want 3 violations, got %q", got)
	}
	if !slices.IsSorted(got) {
		t.Fatalf("violations not sorted: %q", got)
	}
	requireViolation(t, got[:1], "legacy manager")
	requireViolation(t, got[2:], "unowned status field")
}

// The fake client drops managedFields on create (it has no server-side apply
// bookkeeping), so objects reach the checker with none. That is exactly the
// vacuous-pass case the checker reports, which makes it a usable probe that
// every kind is listed and that the namespace is respected. A testkit fake that
// models managedFields would let this assert real violations instead.
func TestOwnershipViolationsListsEveryKindInTheNamespace(t *testing.T) {
	ns := "ns1"
	objs := []client.Object{
		&MultigresCluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "mc"}},
		&multigresv1alpha1.TableGroup{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "tg"}},
		&multigresv1alpha1.Cell{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ce"}},
		&multigresv1alpha1.TopoServer{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "ts"}},
		shardWithRoles(ns, "sh", nil),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "po"}},
		shardWithRoles("elsewhere", "other", nil),
	}

	c := newBareCase(t)
	got, err := ownershipViolations(c.Context(), identityFakeClient(objs...), ns)
	c.NoError(err, "ownershipViolations")

	want := []string{
		"Cell ce: no decodable managedFields claims (0 entries)",
		"MultigresCluster mc: no decodable managedFields claims (0 entries)",
		"Pod po: no decodable managedFields claims (0 entries)",
		"Shard sh: no decodable managedFields claims (0 entries)",
		"TableGroup tg: no decodable managedFields claims (0 entries)",
		"TopoServer ts: no decodable managedFields claims (0 entries)",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestOwnershipViolationsErrorsWhenNoCRObjectsExist(t *testing.T) {
	c := newBareCase(t)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "po"}}
	for name, objs := range map[string][]client.Object{
		"empty namespace": nil,
		"pods only":       {pod},
		"other namespace": {shardWithRoles("elsewhere", "other", nil)},
	} {
		got, err := ownershipViolations(c.Context(), identityFakeClient(objs...), "ns1")
		if err == nil {
			t.Fatalf("%s: want an error, got violations %q", name, got)
		}
	}
}

func TestCheckOwnershipCountsCRObjectsWithoutErroring(t *testing.T) {
	c := newBareCase(t)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "po"}}

	for name, tc := range map[string]struct {
		objs []client.Object
		want int
	}{
		"empty namespace": {nil, 0},
		"pods only":       {[]client.Object{pod}, 0},
		"other namespace": {[]client.Object{shardWithRoles("elsewhere", "other", nil)}, 0},
		"two CRs and a pod": {
			[]client.Object{
				shardWithRoles("ns1", "sh1", nil),
				shardWithRoles("ns1", "sh2", nil),
				pod,
			},
			2,
		},
	} {
		_, checked, err := checkOwnership(c.Context(), identityFakeClient(tc.objs...), "ns1")
		if err != nil {
			t.Fatalf("%s: lenient check errored: %v", name, err)
		}
		if checked != tc.want {
			t.Fatalf("%s: checked %d CR objects, want %d", name, checked, tc.want)
		}
	}
}
