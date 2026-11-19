package multigrescluster

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/multiadmin"
	"k8s.io/apimachinery/pkg/api/meta"
)

func desiredBound(id int64, connection string) func(string) ([]multiadmin.TableGroup, error) {
	return func(string) ([]multiadmin.TableGroup, error) {
		return []multiadmin.TableGroup{
			{
				ID: id, Name: "legacy", Shards: []string{"0-inf"}, State: multiadmin.StateCreating,
				ShardBackingConnections: map[string]string{"0-inf": connection},
			},
			{
				ID: id + 1, Name: "commerce", Shards: []string{"0-80", "80-inf"},
				State: multiadmin.StateCreating,
			},
		}, nil
	}
}

func (f *dynamicFixture) dynamicCondition() *metav1.Condition {
	return meta.FindStatusCondition(
		f.cluster.Status.Conditions,
		multigresv1alpha1.ConditionDynamicTableGroups,
	)
}

func TestDynamicTableGroups_BoundGetsOnePoolerShard(t *testing.T) {
	f := newDynamicFixture(t, true, desiredBound(9, "orders-db"))
	f.reconcile()

	def := f.tableGroup("default")
	got := f.tableGroup("legacy")
	if got == nil {
		t.Fatalf("legacy not provisioned: %+v", f.dynamicCondition())
	}
	if got.Annotations[multigresv1alpha1.AnnotationBackingConnection] != "orders-db" ||
		got.Annotations[multigresv1alpha1.AnnotationTableGroupID] != "9" {
		t.Errorf("annotations = %v, want the connection and id 9", got.Annotations)
	}
	if len(got.Spec.Shards) != 1 || got.Spec.Shards[0].Name != "0-inf" {
		t.Fatalf("shards = %+v, want only 0-inf", got.Spec.Shards)
	}
	s := got.Spec.Shards[0]
	if len(s.Pools) != 1 {
		t.Fatalf("pools = %+v, want one", s.Pools)
	}
	wantPool, wantCell := firstPoolAndCell(def.Spec.Shards[0].Pools)
	if wantCell == "" {
		t.Fatal("the default tablegroup's pools list no cell")
	}
	p, ok := s.Pools[wantPool]
	if !ok || len(p.Cells) != 1 || p.Cells[0] != wantCell {
		t.Errorf("pools = %+v, want only %s in %s", s.Pools, wantPool, wantCell)
	}
	if p.ReplicasPerCell != nil || p.Storage.Size != "" {
		t.Errorf("pool = %+v: a bound pooler has one pod and no storage", p)
	}
	if s.Backup != nil || s.PostgresConfig != nil || s.InitdbArgs != "" {
		t.Errorf("a bound shard carries PostgreSQL settings: %+v", s)
	}

	// The managed entry beside it is untouched.
	f.assertShapedLikeDefault(f.tableGroup("commerce"))
	if c := f.dynamicCondition(); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("condition = %+v, want True", c)
	}
}

func TestDynamicTableGroups_BoundNeedsExactlyTheOneShard(t *testing.T) {
	for name, want := range map[string]multiadmin.TableGroup{
		"a sharded tablegroup": {
			ID: 9, Name: "legacy", Shards: []string{"0-80", "80-inf"}, State: multiadmin.StateCreating,
			ShardBackingConnections: map[string]string{"0-80": "orders-db"},
		},
		"a shard not in the layout": {
			ID: 9, Name: "legacy", Shards: []string{"0-inf"}, State: multiadmin.StateCreating,
			ShardBackingConnections: map[string]string{"80-inf": "orders-db"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newDynamicFixture(t, true, func(string) ([]multiadmin.TableGroup, error) {
				return []multiadmin.TableGroup{want}, nil
			})
			f.reconcile()

			if f.tableGroup("legacy") != nil {
				t.Error("the tablegroup was provisioned")
			}
			if c := f.dynamicCondition(); c == nil || c.Status != metav1.ConditionFalse {
				t.Errorf("condition = %+v, want False", c)
			}
		})
	}
}

func TestFirstPoolAndCell(t *testing.T) {
	pools := map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
		"b":     {Cells: []multigresv1alpha1.CellName{"z9", "z1"}},
		"a":     {Cells: []multigresv1alpha1.CellName{"z5", "z2"}},
		"empty": {},
	}
	// The first pool by name, and the first cell in the order it lists them.
	if pool, cell := firstPoolAndCell(pools); pool != "a" || cell != "z5" {
		t.Errorf("got %s/%s, want a/z5", pool, cell)
	}
	delete(pools, "a")
	if pool, cell := firstPoolAndCell(pools); pool != "b" || cell != "z9" {
		t.Errorf("got %s/%s, want b/z9", pool, cell)
	}
	delete(pools, "b")
	if pool, cell := firstPoolAndCell(pools); pool != "" || cell != "" {
		t.Errorf("got %s/%s from pools with no cell", pool, cell)
	}
}

// A bound tablegroup's pooler stays where it was first placed, even when the
// default tablegroup's pools change.
func TestDynamicTableGroups_BoundKeepsItsPlacement(t *testing.T) {
	f := newDynamicFixture(t, true, desiredBound(9, "orders-db"))
	f.reconcile()
	first := f.tableGroup("legacy")
	if first == nil {
		t.Fatal("legacy not provisioned")
	}
	pool, cell := pinnedPoolAndCell(first)
	if cell == "" {
		t.Fatal("legacy has no pinned cell")
	}

	tmpl := dynamicTemplate{shard: *f.tableGroup("default").Spec.Shards[0].DeepCopy()}
	for name, p := range tmpl.shard.Pools {
		p.Cells = append([]multigresv1alpha1.CellName{"aaa-new-first"}, p.Cells...)
		tmpl.shard.Pools[name] = p
	}
	want := multiadmin.TableGroup{
		ID: 9, Name: "legacy", Shards: []string{"0-inf"},
		ShardBackingConnections: map[string]string{"0-inf": "orders-db"},
	}
	place, problem := resolveBoundPlacement(tmpl, first, want)
	if problem != "" || place.cell != cell || place.pool != pool {
		t.Fatalf("placement = %+v (%q), want to stay in %s/%s", place, problem, pool, cell)
	}
	// A new tablegroup would take the new first cell.
	place, _ = resolveBoundPlacement(tmpl, nil, want)
	if place.cell != "aaa-new-first" {
		t.Errorf("new placement = %s, want aaa-new-first", place.cell)
	}
}

func TestResolveBoundPlacement_ManagedAndNoCell(t *testing.T) {
	if place, problem := resolveBoundPlacement(dynamicTemplate{}, nil,
		multiadmin.TableGroup{ID: 1, Name: "commerce"}); place != nil || problem != "" {
		t.Errorf("managed entry got %+v %q", place, problem)
	}
	if place, problem := resolveBoundPlacement(
		dynamicTemplate{},
		nil,
		multiadmin.TableGroup{
			ID:                      1,
			Name:                    "legacy",
			Shards:                  []string{"0-inf"},
			ShardBackingConnections: map[string]string{"0-inf": "c"},
		},
	); place != nil || problem == "" {
		t.Errorf("bound entry with no cell got %+v %q, want a problem", place, problem)
	}
}
