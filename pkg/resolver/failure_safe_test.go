package resolver

import (
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func cellNames(n int) []multigresv1alpha1.CellName {
	cells := make([]multigresv1alpha1.CellName, n)
	for i := range cells {
		cells[i] = multigresv1alpha1.CellName(fmt.Sprintf("zone-%d", i+1))
	}
	return cells
}

// TestFailureSafeReplicasPerCell_MatchesMultiorch pins the default against
// multiorch's own bootstrap rule, so a change to that rule upstream fails
// here instead of leaving new shards without a primary.
func TestFailureSafeReplicasPerCell_MatchesMultiorch(t *testing.T) {
	t.Parallel()

	for _, policy := range []string{"AT_LEAST_2", "MULTI_CELL_AT_LEAST_2"} {
		for cells := 1; cells <= 5; cells++ {
			if policy == "MULTI_CELL_AT_LEAST_2" && cells < 2 {
				continue // already rejected by the cell-count warning
			}
			t.Run(fmt.Sprintf("%s/%d-cells", policy, cells), func(t *testing.T) {
				t.Parallel()
				n := failureSafeReplicasPerCell(cells)
				pools := func(r int32) map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec {
					return map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"default": {Cells: cellNames(cells), ReplicasPerCell: ptr.To(r)},
					}
				}
				safe, err := cohortIsFailureSafe(policy, pools(n))
				if err != nil {
					t.Fatal(err)
				}
				if !safe {
					t.Errorf("replicasPerCell %d across %d cell(s) is not failure-safe", n, cells)
				}
				if n > 1 {
					safe, err := cohortIsFailureSafe(policy, pools(n-1))
					if err != nil {
						t.Fatal(err)
					}
					if safe {
						t.Errorf(
							"replicasPerCell %d across %d cell(s) is already failure-safe; default %d is larger than needed",
							n-1,
							cells,
							n,
						)
					}
				}
			})
		}
	}
}

func TestResolveShard_DefaultReplicasBootstrap(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	r := NewResolver(fake.NewClientBuilder().WithScheme(scheme).Build(), "default")

	for cells, want := range map[int]*int32{
		1: ptr.To(int32(3)),
		2: ptr.To(int32(2)),
		3: ptr.To(int32(1)), // defaultPoolSpec's per-cell default
		4: ptr.To(int32(1)),
	} {
		t.Run(fmt.Sprintf("%d-cells", cells), func(t *testing.T) {
			t.Parallel()
			shardCfg := &multigresv1alpha1.ShardConfig{
				Name: "0-inf",
				Spec: &multigresv1alpha1.ShardInlineSpec{
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"default": {Type: "readWrite"},
					},
				},
			}
			got, err := r.ResolveShard(t.Context(), shardCfg, ResolveShardOptions{
				AllCellNames:            cellNames(cells),
				MaterializeCellDefaults: true,
			})
			if err != nil {
				t.Fatalf("ResolveShard() error = %v", err)
			}
			pool := got.Pools["default"]
			if pool.ReplicasPerCell == nil || *pool.ReplicasPerCell != *want {
				t.Errorf("replicasPerCell = %v, want %d", pool.ReplicasPerCell, *want)
			}
		})
	}
}
