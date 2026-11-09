package multigrescluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// dynamicFixture is a cluster with database postgres whose default
// tablegroup resolves from the fixture ShardTemplate, plus the reconciler
// under test reading desired tablegroups from list.
type dynamicFixture struct {
	t       *testing.T
	c       client.Client
	r       *MultigresClusterReconciler
	cluster *multigresv1alpha1.MultigresCluster
	calls   int
}

func newDynamicFixture(
	t *testing.T,
	enabled bool,
	list func(database string) ([]multiadmin.TableGroup, error),
	extra ...client.Object,
) *dynamicFixture {
	t.Helper()
	coreTpl, cellTpl, shardTpl, cluster, _, _ := setupFixtures(t)
	cluster.Spec.Databases = []multigresv1alpha1.DatabaseConfig{{
		Name: "postgres",
		TableGroups: []multigresv1alpha1.TableGroupConfig{{
			Name:    "default",
			Default: true,
			Shards:  []multigresv1alpha1.ShardConfig{{Name: "0-inf"}},
		}},
	}}
	if enabled {
		cluster.Annotations = map[string]string{
			multigresv1alpha1.AnnotationDynamicTableGroups: "true",
		}
	}
	objs := append([]client.Object{coreTpl, cellTpl, shardTpl, cluster}, extra...)
	c := fake.NewClientBuilder().
		WithScheme(setupScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}, &multigresv1alpha1.TableGroup{}).
		Build()
	f := &dynamicFixture{t: t, c: c, cluster: cluster}
	f.r = &MultigresClusterReconciler{
		Client:   c,
		Scheme:   setupScheme(),
		Recorder: record.NewFakeRecorder(100),
		ListDesiredTableGroups: func(_ context.Context, _ *multigresv1alpha1.MultigresCluster, db string) ([]multiadmin.TableGroup, error) {
			f.calls++
			return list(db)
		},
	}
	return f
}

func (f *dynamicFixture) reconcile() {
	f.t.Helper()
	if _, err := f.r.reconcileDatabases(
		f.t.Context(),
		f.cluster,
		resolver.NewResolver(f.c, f.cluster.Namespace),
	); err != nil {
		f.t.Fatalf("reconcileDatabases: %v", err)
	}
}

func (f *dynamicFixture) tableGroup(tg string) *multigresv1alpha1.TableGroup {
	f.t.Helper()
	obj := &multigresv1alpha1.TableGroup{}
	err := f.c.Get(f.t.Context(), types.NamespacedName{
		Namespace: f.cluster.Namespace,
		Name: name.JoinWithConstraints(
			name.DefaultConstraints,
			f.cluster.Name,
			"postgres",
			tg,
		),
	}, obj)
	if err != nil {
		return nil
	}
	return obj
}

// existingDynamic is a dynamic TableGroup as an earlier reconcile left it.
func existingDynamic(cluster, tg string, id string) *multigresv1alpha1.TableGroup {
	labels := metadata.BuildStandardLabels(cluster, metadata.ComponentTableGroup)
	metadata.AddClusterLabel(labels, cluster)
	metadata.AddDatabaseLabel(labels, "postgres")
	metadata.AddTableGroupLabel(labels, multigresv1alpha1.TableGroupName(tg))
	labels[multigresv1alpha1.LabelDynamicTableGroup] = "true"
	return &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name.JoinWithConstraints(name.DefaultConstraints, cluster, "postgres", tg),
			Namespace:   "default",
			Labels:      labels,
			Annotations: map[string]string{multigresv1alpha1.AnnotationTableGroupID: id},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "postgres",
			TableGroupName: multigresv1alpha1.TableGroupName(tg),
			// Bare shard names, as if provisioned by a build whose template
			// differed from the current default tablegroup.
			Shards: []multigresv1alpha1.ShardResolvedSpec{{Name: "0-80"}, {Name: "80-inf"}},
		},
	}
}

// assertShapedLikeDefault checks that every shard of tg copies the default
// tablegroup's resolved shard, name aside.
func (f *dynamicFixture) assertShapedLikeDefault(tg *multigresv1alpha1.TableGroup) {
	f.t.Helper()
	def := f.tableGroup("default")
	if def == nil || tg == nil {
		f.t.Fatalf("default=%v tablegroup=%v, want both", def != nil, tg != nil)
	}
	if len(tg.Spec.Shards) == 0 {
		f.t.Fatal("tablegroup has no shards")
	}
	for _, s := range tg.Spec.Shards {
		want := def.Spec.Shards[0].DeepCopy()
		want.Name = s.Name
		if !equality.Semantic.DeepEqual(*want, s) {
			f.t.Errorf("shard %s differs from the default tablegroup's resolved shard", s.Name)
		}
	}
	if !equality.Semantic.DeepEqual(def.Spec.Images, tg.Spec.Images) ||
		!equality.Semantic.DeepEqual(def.Spec.Backup, tg.Spec.Backup) {
		f.t.Error("images or backup differ from the default tablegroup")
	}
}

func desiredCommerce(id int64) func(string) ([]multiadmin.TableGroup, error) {
	return func(string) ([]multiadmin.TableGroup, error) {
		return []multiadmin.TableGroup{
			{ID: id, Name: "commerce", Shards: []string{"0-80", "80-inf"}},
		}, nil
	}
}

func pendingDeletion(tg *multigresv1alpha1.TableGroup) bool {
	return tg != nil && tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != ""
}

func TestDynamicTableGroups_GateOff(t *testing.T) {
	f := newDynamicFixture(
		t,
		false,
		desiredCommerce(7),
		existingDynamic("test-cluster", "orders", "3"),
	)
	f.reconcile()

	if f.calls != 0 {
		t.Errorf("multiadmin read %d times with the opt-in off", f.calls)
	}
	if f.tableGroup("commerce") != nil {
		t.Error("commerce provisioned with the opt-in off")
	}
	orders := f.tableGroup("orders")
	if pendingDeletion(orders) {
		t.Error("turning the opt-in off must not tear down what was provisioned")
	}
	// Retained tablegroups keep following cluster-level configuration.
	f.assertShapedLikeDefault(orders)
	if !isDynamic(orders) || orders.Annotations[multigresv1alpha1.AnnotationTableGroupID] != "3" {
		t.Errorf("re-applying orders lost its dynamic label or id: %v %v", orders.Labels, orders.Annotations)
	}
	if meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	) != nil {
		t.Error("condition set with the opt-in off")
	}
}

func TestDynamicTableGroups_LiteIgnoresOptIn(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.cluster.Spec.Mode = multigresv1alpha1.ModeLite
	if dynamicTableGroupsEnabled(f.cluster) {
		t.Fatal("lite cluster reported dynamic tablegroups enabled")
	}
}

func TestDynamicTableGroups_ProvisionFromDefaultTemplate(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.reconcile()

	def := f.tableGroup("default")
	got := f.tableGroup("commerce")
	if def == nil || got == nil {
		t.Fatalf("default=%v commerce=%v, want both", def != nil, got != nil)
	}
	if got.Labels[multigresv1alpha1.LabelDynamicTableGroup] != "true" {
		t.Errorf("missing dynamic label: %v", got.Labels)
	}
	if got.Annotations[multigresv1alpha1.AnnotationTableGroupID] != "7" {
		t.Errorf(
			"tablegroup id annotation = %q, want 7",
			got.Annotations[multigresv1alpha1.AnnotationTableGroupID],
		)
	}
	if got.Spec.TableGroupName != "commerce" || got.Spec.IsDefault {
		t.Errorf("name=%q default=%v", got.Spec.TableGroupName, got.Spec.IsDefault)
	}
	if len(got.Spec.Shards) != 2 || got.Spec.Shards[0].Name != "0-80" ||
		got.Spec.Shards[1].Name != "80-inf" {
		t.Fatalf("shards = %+v", got.Spec.Shards)
	}
	for _, s := range got.Spec.Shards {
		want := def.Spec.Shards[0].DeepCopy()
		want.Name = s.Name
		if !equality.Semantic.DeepEqual(*want, s) {
			t.Errorf("shard %s differs from the default tablegroup's resolved shard", s.Name)
		}
	}
	if !equality.Semantic.DeepEqual(def.Spec.Images, got.Spec.Images) ||
		!equality.Semantic.DeepEqual(def.Spec.Backup, got.Spec.Backup) {
		t.Error("images or backup differ from the default tablegroup")
	}
	c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("condition = %+v, want True", c)
	}
}

func TestDynamicTableGroups_FailedReadRemovesNothing(t *testing.T) {
	failing := func(string) ([]multiadmin.TableGroup, error) {
		return nil, errors.New("HTTP 503: catalog primary unreachable")
	}
	f := newDynamicFixture(t, true, failing, existingDynamic("test-cluster", "commerce", "7"))
	f.reconcile()

	if pendingDeletion(f.tableGroup("commerce")) {
		t.Error("a failed read marked a dynamic tablegroup for deletion")
	}
	c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	)
	if c == nil || c.Status != metav1.ConditionFalse || !strings.Contains(c.Message, "503") {
		t.Errorf("condition = %+v, want False naming the error", c)
	}
}

func TestDynamicTableGroups_AbsentFromSuccessfulReadIsKept(t *testing.T) {
	empty := func(string) ([]multiadmin.TableGroup, error) { return nil, nil }
	f := newDynamicFixture(t, true, empty, existingDynamic("test-cluster", "commerce", "7"))
	f.reconcile()

	tg := f.tableGroup("commerce")
	if tg == nil || pendingDeletion(tg) {
		t.Fatal("a provisioned dynamic tablegroup was removed")
	}
	f.assertShapedLikeDefault(tg)
	if len(tg.Spec.Shards) != 2 || tg.Spec.Shards[0].Name != "0-80" || tg.Spec.Shards[1].Name != "80-inf" {
		t.Errorf("re-applying commerce changed its shard layout: %+v", tg.Spec.Shards)
	}
	if pendingDeletion(f.tableGroup("default")) {
		t.Error("the spec-declared default tablegroup was marked for deletion")
	}
}

func TestDynamicTableGroups_UnchangedTableGroupsAreNotReapplied(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.reconcile()
	before := map[string]string{}
	for _, n := range []string{"default", "commerce"} {
		tg := f.tableGroup(n)
		if tg == nil || tg.Annotations[metadata.AnnotationSpecHash] == "" {
			t.Fatalf("%s: not applied with a spec hash: %+v", n, tg)
		}
		before[n] = tg.ResourceVersion
	}
	drain(f.r.Recorder.(*record.FakeRecorder))

	f.reconcile()
	for n, rv := range before {
		if got := f.tableGroup(n).ResourceVersion; got != rv {
			t.Errorf("%s: resourceVersion %s -> %s on a converged reconcile", n, rv, got)
		}
	}
	for _, e := range drain(f.r.Recorder.(*record.FakeRecorder)) {
		if strings.Contains(e, "Applied TableGroup") {
			t.Errorf("event on a converged reconcile: %s", e)
		}
	}

	// A template change reaches both the spec and the dynamic tablegroup.
	f.cluster.Spec.Images.Postgres = "postgres:changed"
	f.reconcile()
	for n, rv := range before {
		if got := f.tableGroup(n).ResourceVersion; got == rv {
			t.Errorf("%s: not re-applied after a template change", n)
		}
	}
}

// drain returns the events recorded so far.
func drain(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestDynamicTableGroups_ProblemsTurnTheConditionFalseOnce(t *testing.T) {
	list := func(string) ([]multiadmin.TableGroup, error) {
		return []multiadmin.TableGroup{
			{ID: 1, Name: "Has_Upper", Shards: []string{"0-inf"}},
			{ID: 2, Name: "ok", Shards: []string{"0-inf"}},
		}, nil
	}
	f := newDynamicFixture(t, true, list)
	warnings := func() int {
		n := 0
		for _, e := range drain(f.r.Recorder.(*record.FakeRecorder)) {
			if strings.HasPrefix(e, "Warning DynamicTableGroupsFailed") {
				n++
			}
		}
		return n
	}

	f.reconcile()
	c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "ProvisioningFailed" {
		t.Fatalf("condition = %+v, want False/ProvisioningFailed", c)
	}
	if f.tableGroup("ok") == nil {
		t.Error("the valid entry was not provisioned next to the failing one")
	}
	if got := warnings(); got != 1 {
		t.Errorf("%d Warning events on the first failing pass, want 1", got)
	}

	// The problem persists: no new event.
	f.reconcile()
	if got := warnings(); got != 0 {
		t.Errorf("%d Warning events on a pass with the same lasting problem, want 0", got)
	}

	// The problem clears, then returns: one more event.
	f.r.ListDesiredTableGroups = func(context.Context, *multigresv1alpha1.MultigresCluster, string) ([]multiadmin.TableGroup, error) {
		return []multiadmin.TableGroup{{ID: 2, Name: "ok", Shards: []string{"0-inf"}}}, nil
	}
	f.reconcile()
	if c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("condition = %+v, want True once the problem cleared", c)
	}
	f.r.ListDesiredTableGroups = func(context.Context, *multigresv1alpha1.MultigresCluster, string) ([]multiadmin.TableGroup, error) {
		return list("")
	}
	f.reconcile()
	if got := warnings(); got != 1 {
		t.Errorf("%d Warning events after the problem returned, want 1", got)
	}
}

func TestDynamicTableGroups_ConditionMessageIsBounded(t *testing.T) {
	long := strings.Repeat("x", 4000)
	list := func(string) ([]multiadmin.TableGroup, error) {
		var out []multiadmin.TableGroup
		for i := range 60 {
			out = append(out, multiadmin.TableGroup{
				ID: int64(i + 1), Name: fmt.Sprintf("%s%d", long, i), Shards: []string{"0-inf"},
			})
		}
		return out, nil
	}
	f := newDynamicFixture(t, true, list)
	f.reconcile()
	c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	)
	if c == nil {
		t.Fatal("no condition")
	}
	if len(c.Message) > 4096 {
		t.Errorf("message is %d bytes", len(c.Message))
	}
	if strings.Contains(c.Message, long[:maxNameLen+1]) {
		t.Error("an unvalidated name was quoted in full")
	}
	if !strings.Contains(c.Message, "and 40 more") {
		t.Errorf("message does not say how many problems were cut: %s", c.Message)
	}
}

func TestDynamicTableGroups_IDChangeWaitsForThePreviousIncarnation(t *testing.T) {
	f := newDynamicFixture(
		t,
		true,
		desiredCommerce(9),
		existingDynamic("test-cluster", "commerce", "7"),
	)
	f.reconcile()

	old := f.tableGroup("commerce")
	if old.Annotations[multigresv1alpha1.AnnotationTableGroupID] != "7" {
		t.Fatalf("id 9 was applied over the id 7 incarnation: %v", old.Annotations)
	}
	if pendingDeletion(old) {
		t.Fatal("the id 7 incarnation was marked for deletion")
	}
	if c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	); c == nil ||
		!strings.Contains(c.Message, "waiting for tablegroup id 7") {
		t.Errorf("condition does not say what it waits for: %+v", c)
	}

	// The TableGroup is gone, but a Shard of id 7 remains: id 9 must keep
	// waiting.
	if err := f.c.Delete(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	shard := &multigresv1alpha1.Shard{ObjectMeta: metav1.ObjectMeta{
		Name: "test-cluster-postgres-commerce-0-80", Namespace: "default",
		Labels: map[string]string{
			metadata.LabelMultigresCluster:    "test-cluster",
			metadata.LabelMultigresDatabase:   "postgres",
			metadata.LabelMultigresTableGroup: "commerce",
		},
		Annotations: map[string]string{multigresv1alpha1.AnnotationTableGroupID: "7"},
	}}
	if err := f.c.Create(t.Context(), shard); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if f.tableGroup("commerce") != nil {
		t.Fatal("id 9 provisioned while a shard of id 7 remained")
	}
	if c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	); c == nil ||
		!strings.Contains(c.Message, "previous incarnation") {
		t.Errorf("condition does not say what it waits for: %+v", c)
	}

	if err := f.c.Delete(t.Context(), shard); err != nil {
		t.Fatal(err)
	}
	f.reconcile()
	if got := f.tableGroup(
		"commerce",
	); got == nil ||
		got.Annotations[multigresv1alpha1.AnnotationTableGroupID] != "9" {
		t.Fatalf("id 9 not provisioned once id 7 was gone: %+v", got)
	}
}

func TestDynamicTableGroups_SpecNameConflict(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.cluster.Spec.Databases[0].TableGroups = append(
		f.cluster.Spec.Databases[0].TableGroups,
		multigresv1alpha1.TableGroupConfig{
			Name:   "commerce",
			Shards: []multigresv1alpha1.ShardConfig{{Name: "0-inf"}},
		},
	)
	f.reconcile()

	got := f.tableGroup("commerce")
	if got == nil {
		t.Fatal("spec-declared commerce missing")
	}
	if got.Labels[multigresv1alpha1.LabelDynamicTableGroup] != "" ||
		got.Annotations[multigresv1alpha1.AnnotationTableGroupID] != "" ||
		len(got.Spec.Shards) != 1 {
		t.Errorf(
			"spec-declared commerce was merged with the catalog one: labels=%v annotations=%v shards=%d",
			got.Labels,
			got.Annotations,
			len(got.Spec.Shards),
		)
	}
	c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroupConflict,
	)
	if c == nil || c.Status != metav1.ConditionTrue ||
		!strings.Contains(c.Message, "postgres/commerce") {
		t.Errorf("conflict condition = %+v", c)
	}
}

func TestDynamicTableGroups_InvalidEntriesSkipped(t *testing.T) {
	bad := func(string) ([]multiadmin.TableGroup, error) {
		return []multiadmin.TableGroup{
			{ID: 1, Name: "Has_Upper", Shards: []string{"0-inf"}},
			{ID: 2, Name: "this-name-is-far-too-long-for-k8s", Shards: []string{"0-inf"}},
			{ID: 0, Name: "zeroid", Shards: []string{"0-inf"}},
			{ID: 4, Name: "dup", Shards: []string{"0-80", "0-80"}},
			{ID: 5, Name: "ok", Shards: []string{"0-inf"}},
		}, nil
	}
	f := newDynamicFixture(t, true, bad)
	f.reconcile()
	for _, n := range []string{"has_upper", "this-name-is-far-too-long-for-k8s", "zeroid", "dup"} {
		if f.tableGroup(n) != nil {
			t.Errorf("invalid entry %q provisioned", n)
		}
	}
	if f.tableGroup("ok") == nil {
		t.Error("valid entry not provisioned next to invalid ones")
	}
}

func TestBuildCell_PropagatesDynamicTableGroups(t *testing.T) {
	_, _, _, cluster, _, _ := setupFixtures(t)
	cluster.Annotations = map[string]string{multigresv1alpha1.AnnotationDynamicTableGroups: "true"}
	gw := &multigresv1alpha1.MultigatewaySpec{}
	build := func() *multigresv1alpha1.Cell {
		cell, err := BuildCell(
			cluster,
			&cluster.Spec.Cells[0],
			gw,
			&multigresv1alpha1.PodPlacementSpec{},
			&multigresv1alpha1.LocalTopoServerSpec{},
			multigresv1alpha1.GlobalTopoServerRef{},
			nil,
			setupScheme(),
		)
		if err != nil {
			t.Fatal(err)
		}
		return cell
	}
	if !multigresv1alpha1.DynamicTableGroupsEnabled(build()) {
		t.Error("opted-in cluster's Cell lacks the annotation")
	}
	cluster.Spec.Mode = multigresv1alpha1.ModeLite
	if multigresv1alpha1.DynamicTableGroupsEnabled(build()) {
		t.Error("lite cluster's Cell carries the annotation")
	}
	cluster.Spec.Mode = ""
	cluster.Annotations = nil
	if multigresv1alpha1.DynamicTableGroupsEnabled(build()) {
		t.Error("Cell carries the annotation without the opt-in")
	}
}

func TestDynamicTableGroups_SpecNeverOverwritesAProvisionedOne(t *testing.T) {
	f := newDynamicFixture(t, true, desiredCommerce(7))
	f.reconcile()
	if got := f.tableGroup("commerce"); got == nil || len(got.Spec.Shards) != 2 {
		t.Fatalf("catalog commerce not provisioned: %+v", got)
	}

	f.cluster.Spec.Databases[0].TableGroups = append(
		f.cluster.Spec.Databases[0].TableGroups,
		multigresv1alpha1.TableGroupConfig{
			Name:   "commerce",
			Shards: []multigresv1alpha1.ShardConfig{{Name: "0-inf"}},
		},
	)
	f.reconcile()

	got := f.tableGroup("commerce")
	if got == nil || pendingDeletion(got) ||
		got.Labels[multigresv1alpha1.LabelDynamicTableGroup] != "true" ||
		got.Annotations[multigresv1alpha1.AnnotationTableGroupID] != "7" ||
		len(got.Spec.Shards) != 2 {
		t.Fatalf("the spec replaced the provisioned commerce: %+v", got)
	}
	if c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroupConflict,
	); c == nil || !strings.Contains(c.Message, "postgres/commerce") {
		t.Errorf("conflict condition = %+v", c)
	}
}

func TestDynamicTableGroups_RejectedEntriesDoNotStopTheRest(t *testing.T) {
	tooMany := make([]string, maxDynamicShards+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("s%d-x", i)
	}
	list := func(string) ([]multiadmin.TableGroup, error) {
		return []multiadmin.TableGroup{
			{ID: 3, Name: "wide", Shards: tooMany},
			{ID: 4, Name: "twice", Shards: []string{"0-inf"}},
			{ID: 5, Name: "twice", Shards: []string{"0-inf"}},
			{ID: 6, Name: "ok", Shards: []string{"0-inf"}},
		}, nil
	}
	f := newDynamicFixture(t, true, list)
	f.reconcile()
	if f.tableGroup("wide") != nil {
		t.Error("a layout over the TableGroup shard limit was provisioned")
	}
	if got := f.tableGroup("twice"); got == nil ||
		got.Annotations[multigresv1alpha1.AnnotationTableGroupID] != "4" {
		t.Errorf("a name listed twice was not provisioned once, from its first entry: %+v", got)
	}
	if f.tableGroup("ok") == nil {
		t.Error("a valid entry was not provisioned next to rejected ones")
	}
	c := meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	)
	if c == nil || !strings.Contains(c.Message, "more than the 32") ||
		!strings.Contains(c.Message, "listed more than once") {
		t.Errorf("condition does not report the rejected entries: %+v", c)
	}
}
