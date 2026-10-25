package suite

import (
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/value"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
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
// A Pod has only one row, a gate condition, as in the real table.
func fakeOwners(table map[string]string) ownerLookup {
	pod := map[string]string{podGatePath: "op-b"}
	return func(kind, path string) (string, bool) {
		rows := table
		if kind == "Pod" {
			rows = pod
		}
		for prefix, m := range rows {
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
	isTestLegacy  = func(e metav1.ManagedFieldsEntry) bool { return testLegacy[e.Manager] }
	testOwners    = fakeOwners(map[string]string{
		".status.phase":          "op-a",
		".status.postgresConfig": "op-a",
		".status.cells":          "op-a",
		".status.conditions":     "op-b",
	})
)

const (
	podGatePath = `.status.conditions[type="Gate"]`
	podGate     = `{"f:status":{"f:conditions":{"k:{\"type\":\"Gate\"}":{"f:status":{}}}}}`
)

func violations(kind string, entries ...metav1.ManagedFieldsEntry) []string {
	return violationsFor(
		checkedObject{kind: kind, name: "obj", entries: entries},
		testOwners, isTestOp, isTestLegacy, noLegacy,
	)
}

func violationsPopulated(
	kind string,
	populated []fieldpath.Path,
	entries ...metav1.ManagedFieldsEntry,
) []string {
	return violationsFor(
		checkedObject{kind: kind, name: "obj", entries: entries, populated: populated},
		testOwners, isTestOp, isTestLegacy, noLegacy,
	)
}

func statusLeaf(fields ...any) fieldpath.Path {
	return fieldpath.MakePathOrDie(append([]any{"status"}, fields...)...)
}

func condLeaf(t string, field string) fieldpath.Path {
	return append(conditionPath(statusLeaf(), t), fieldpath.PathElement{FieldName: &field})
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
	got := violations("Pod", mfEntry("op-a", podGate))
	requireViolation(t, got, "Pod obj", "owned by op-b, claimed by op-a")

	got = violations("Pod", mfEntry("op-a", `{"f:status":{"f:message":{}}}`))
	requireViolation(t, got, "Pod obj", "unowned status field", "op-a")

	requireNone(t, violations("Pod", mfEntry("op-b", podGate)))
}

func TestOwnershipViolationsPodKubeletStatusIsIgnored(t *testing.T) {
	requireNone(t, violations(
		"Pod",
		mfEntry(
			"kubelet",
			`{"f:status":{"f:conditions":{".":{},"k:{\"type\":\"Ready\"}":{}},"f:phase":{}}}`,
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
	got := violationsFor(
		checkedObject{kind: "Shard", name: "obj", entries: []metav1.ManagedFieldsEntry{
			mfEntry("op-a", `{"f:status":{"f:aaa":{}}}`),
			mfEntry("op-b", `{"f:status":{"f:phase":{}}}`),
			mfEntry("zzz-legacy", specReplicas),
		}},
		testOwners,
		isTestOp,
		func(e metav1.ManagedFieldsEntry) bool {
			return e.Manager == "zzz-legacy"
		},
		noLegacy,
	)

	if len(got) != 3 {
		t.Fatalf("want 3 violations, got %q", got)
	}
	if !slices.IsSorted(got) {
		t.Fatalf("violations not sorted: %q", got)
	}
	requireViolation(t, got[:1], "legacy manager")
	requireViolation(t, got[2:], "unowned status field")
}

// identityFakeClient is built without WithReturnManagedFields, so objects
// reach the checker with no managedFields. That is exactly the vacuous-pass
// case the checker reports, which makes it a usable probe that every kind is
// listed and that the namespace is respected. Real violations are covered by
// the synthetic-entry tests above, since the fake records no subresource and
// treats CRD condition lists as atomic, unlike the apiserver.
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
	got, checked, err := checkOwnership(c.Context(), identityFakeClient(objs...), ns, noLegacy)
	c.NoError(err, "checkOwnership")
	c.Eq(5, checked, "CR objects checked")

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
		_, checked, err := checkOwnership(
			c.Context(),
			identityFakeClient(tc.objs...),
			"ns1",
			noLegacy,
		)
		if err != nil {
			t.Fatalf("%s: lenient check errored: %v", name, err)
		}
		if checked != tc.want {
			t.Fatalf("%s: checked %d CR objects, want %d", name, checked, tc.want)
		}
	}
}

// The legacy rule reads the whole entry: the child writer's name is live on
// the main resource and legacy only on status.
func TestOwnershipViolationsLegacyIsPerEntry(t *testing.T) {
	entry := func(sub string) metav1.ManagedFieldsEntry {
		e := mfEntry(ownership.ManagerChildWriter, specReplicas)
		e.Subresource = sub
		return e
	}
	check := func(e metav1.ManagedFieldsEntry) []string {
		return violationsFor(checkedObject{
			kind: "Shard", name: "obj",
			entries: []metav1.ManagedFieldsEntry{e},
		},
			testOwners, isTestOp, ownership.IsLegacyEntry, noLegacy)
	}
	requireNone(t, check(entry("")))
	requireViolation(
		t,
		check(entry("status")),
		"legacy manager",
		ownership.ManagerChildWriter,
		"status",
	)
}

func TestOwnershipViolationsPopulatedFieldWithoutOwner(t *testing.T) {
	got := violationsPopulated("Shard",
		[]fieldpath.Path{statusLeaf("phase"), statusLeaf("message")},
		mfEntry("op-a", `{"f:status":{"f:phase":{}}}`))
	requireViolation(t, got, "populated status field .status.message has no owner")
	if slices.ContainsFunc(
		got,
		func(v string) bool { return strings.Contains(v, ".status.phase") },
	) {
		t.Fatalf("an owned field was reported: %q", got)
	}

	// A claim on the leaf owns it; a claim on its container alone does not.
	leaf := condLeaf("Ready", "status")
	requireNone(t, violationsPopulated(
		"Shard",
		[]fieldpath.Path{leaf},
		mfEntry(
			"op-b",
			`{"f:status":{"f:conditions":{"k:{\"type\":\"Ready\"}":{"f:status":{}}}}}`,
		),
	))
	got = violationsPopulated("Shard", []fieldpath.Path{leaf},
		mfEntry("op-b", `{"f:status":{"f:conditions":{}}}`))
	requireViolation(t, got, "populated status field "+leaf.String()+" has no owner")
	got = violationsPopulated("Shard", []fieldpath.Path{leaf},
		mfEntry("op-b", `{"f:status":{"f:conditions":{".":{},"k:{\"type\":\"Other\"}":{}}}}`))
	requireViolation(t, got, "populated status field "+leaf.String()+" has no owner")

	got = violationsPopulated("Shard", []fieldpath.Path{statusLeaf("message")},
		mfEntry("op-a", `{"f:status":{".":{}}}`))
	requireViolation(t, got, "populated status field .status.message has no owner")
}

// Each populated leaf needs its own owner: a claim on a sibling under the
// same table row owns nothing.
func TestOwnershipViolationsPopulatedLeafBesideAnOwnedSibling(t *testing.T) {
	populated := []fieldpath.Path{
		statusLeaf("postgresConfig", "error"),
		statusLeaf("postgresConfig", "lastAppliedAt"),
	}
	got := violationsPopulated("Shard", populated,
		mfEntry("op-a", `{"f:status":{"f:postgresConfig":{"f:lastAppliedAt":{}}}}`))
	if !slices.Equal(got, []string{
		"Shard obj: populated status field .status.postgresConfig.error has no owner",
	}) {
		t.Fatalf("got %q", got)
	}
	// An applied empty object claims the container, not its fields.
	got = violationsPopulated("Shard", populated[:1],
		mfEntry("op-a", `{"f:status":{"f:postgresConfig":{}}}`))
	requireViolation(t, got, ".status.postgresConfig.error has no owner")
	// Each member of a set list is its own leaf.
	zoneA := append(statusLeaf("cells"),
		fieldpath.PathElement{Value: ptr.To(value.NewValueInterface("zone-a"))})
	zoneB := append(statusLeaf("cells"),
		fieldpath.PathElement{Value: ptr.To(value.NewValueInterface("zone-b"))})
	got = violationsPopulated("Shard", []fieldpath.Path{zoneA, zoneB},
		mfEntry("op-a", `{"f:status":{"f:cells":{"v:\"zone-a\"":{}}}}`))
	if !slices.Equal(got, []string{
		`Shard obj: populated status field .status.cells[="zone-b"] has no owner`,
	}) {
		t.Fatalf("got %q", got)
	}
}

// The Pod row's owner is enforced on every manager: a write without a field
// owner lands under the implicit name (`manager` in the shipped image), which
// is not an operator name.
func TestOwnershipViolationsPodRowOwnerIsEnforcedOnEveryManager(t *testing.T) {
	got := violations("Pod", mfEntry("manager", podGate))
	requireViolation(t, got, "Pod obj", "owned by op-b, claimed by manager")
	requireNone(t, violations("Pod", mfEntry("op-b", podGate)))
	// A path with no row stays unchecked for a non-operator (kubelet).
	requireNone(t, violations("Pod",
		mfEntry("kubelet", `{"f:status":{"f:message":{},"f:conditions":{".":{}}}}`)))
}

func keptRuleViolations(entries ...metav1.ManagedFieldsEntry) []string {
	return violationsFor(
		checkedObject{kind: "Shard", name: "obj", entries: entries},
		testOwners, isTestOp, isTestLegacy, keptLegacy,
	)
}

// Under keptLegacy a legacy entry may remain only while it claims a field no
// current entry covers; one whose every claim is covered was removable.
func TestOwnershipViolationsKeptLegacyRule(t *testing.T) {
	phase := `{"f:status":{".":{},"f:phase":{}}}`
	phaseAndMessage := `{"f:status":{"f:phase":{},"f:message":{}}}`

	got := keptRuleViolations(mfEntry("op-a", `{"f:status":{"f:phase":{}}}`),
		mfEntry("old-op", phase))
	requireViolation(
		t,
		got,
		"legacy manager old-op",
		"claims nothing a current entry does not cover",
	)

	// Kept: message is claimed only by the legacy entry. Its claim on phase,
	// owned by op-a in the table, is not an owner mismatch.
	requireNone(t, keptRuleViolations(mfEntry("op-a", `{"f:status":{"f:phase":{}}}`),
		mfEntry("old-op", phaseAndMessage)))

	// Another legacy entry covers nothing.
	got = keptRuleViolations(mfEntry("op-a", `{"f:status":{"f:phase":{}}}`),
		mfEntry("old-op", phaseAndMessage), mfEntry("old-op", `{"f:status":{"f:message":{}}}`))
	if len(got) != 0 {
		t.Fatalf("legacy entries covering each other are kept: %q", got)
	}

	// Under noLegacy any legacy entry is a violation.
	got = violations("Shard", mfEntry("op-a", `{"f:status":{"f:phase":{}}}`),
		mfEntry("old-op", phaseAndMessage))
	requireViolation(t, got, "legacy manager old-op on main")
}

func TestPopulatedStatusLeaves(t *testing.T) {
	shard := shardWithRoles("ns", "sh", nil)
	shard.Status.Phase = multigresv1alpha1.PhaseHealthy
	shard.Status.Conditions = []metav1.Condition{
		{Type: "StorageClassValid", Status: metav1.ConditionTrue},
		{Type: "Unlisted", Status: metav1.ConditionTrue, Reason: "R"},
	}
	shard.Status.PostgresConfig = &multigresv1alpha1.PostgresConfigStatus{Error: "boom"}
	shard.Status.Cells = []multigresv1alpha1.CellName{"zone-a", "zone-b"}
	leaves, err := populatedStatusLeaves("Shard", shard)
	if err != nil {
		t.Fatal(err)
	}
	got := pathStrings(leaves)
	for _, want := range []string{
		".status.phase",
		`.status.conditions[type="StorageClassValid"].status`,
		`.status.conditions[type="StorageClassValid"].type`,
		`.status.conditions[type="Unlisted"].reason`,
		".status.postgresConfig.error",
		`.status.cells[="zone-a"]`,
		`.status.cells[="zone-b"]`,
	} {
		if !slices.Contains(got, want) {
			t.Fatalf("missing %s in %q", want, got)
		}
	}
	// Zero values are exempt.
	for _, absent := range []string{
		".status.message",
		".status.orchReady",
		`.status.conditions[type="Unlisted"].message`,
	} {
		if slices.Contains(got, absent) {
			t.Fatalf("zero-valued %s reported as populated: %q", absent, got)
		}
	}

	pod := &corev1.Pod{}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		{Type: "multigres.com/pooler-data-ready", Status: corev1.ConditionTrue},
	}
	leaves, err = populatedStatusLeaves("Pod", pod)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`.status.conditions[type="multigres.com/pooler-data-ready"].status`,
		`.status.conditions[type="multigres.com/pooler-data-ready"].type`,
	}
	if got := pathStrings(leaves); !slices.Equal(got, want) {
		t.Fatalf("pod: got %q, want only the table row's fields %q (kubelet fields exempt)",
			got, want)
	}
}

func pathStrings(ps []fieldpath.Path) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.String())
	}
	slices.Sort(out)
	return out
}
