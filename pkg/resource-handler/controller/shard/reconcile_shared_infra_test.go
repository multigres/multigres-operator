package shard

import (
	"fmt"
	"sort"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestPgBackRestPoolDNSNames(t *testing.T) {
	baseShard := func() *multigresv1alpha1.Shard {
		return &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-shard",
				Namespace: "test-ns",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "testdb",
				TableGroupName: "default",
			},
		}
	}

	t.Run("no pools yields no DNS names", func(t *testing.T) {
		shard := baseShard()
		assert.NewCollecting(t).Empty(pgBackRestPoolDNSNames(shard), "expected no DNS names, got")
	})

	t.Run("single pool, single cell yields a scoped wildcard for that svc", func(t *testing.T) {
		shard := baseShard()
		shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
			"primary": {Cells: []multigresv1alpha1.CellName{"zone1"}},
		}

		headlessName := buildPoolHeadlessServiceName(shard, "primary", "zone1")
		want := []string{
			fmt.Sprintf("*.%s.%s.svc", headlessName, shard.Namespace),
			fmt.Sprintf("*.%s.%s.svc.cluster.local", headlessName, shard.Namespace),
		}

		got := pgBackRestPoolDNSNames(shard)
		assertSameStringSet(t, got, want)
	})

	t.Run("multiple pools and cells yield one scoped wildcard pair each", func(t *testing.T) {
		shard := baseShard()
		shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
			"primary": {Cells: []multigresv1alpha1.CellName{"zone1", "zone2"}},
			"replica": {Cells: []multigresv1alpha1.CellName{"zone1"}},
		}

		var want []string
		for poolName, cellNames := range map[string][]string{
			"primary": {"zone1", "zone2"},
			"replica": {"zone1"},
		} {
			for _, cellName := range cellNames {
				headlessName := buildPoolHeadlessServiceName(shard, poolName, cellName)
				want = append(want,
					fmt.Sprintf("*.%s.%s.svc", headlessName, shard.Namespace),
					fmt.Sprintf("*.%s.%s.svc.cluster.local", headlessName, shard.Namespace),
				)
			}
		}

		got := pgBackRestPoolDNSNames(shard)
		assertSameStringSet(t, got, want)
	})

	t.Run("pool with no cells contributes no DNS names", func(t *testing.T) {
		shard := baseShard()
		shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
			"empty": {},
		}

		assert.NewCollecting(t).
			Empty(pgBackRestPoolDNSNames(shard), "expected no DNS names for a pool with no cells, got")
	})
}

// assertSameStringSet fails the test if got and want don't contain the same
// elements, ignoring order.
func assertSameStringSet(t *testing.T, got, want []string) {
	t.Helper()
	c := assert.NewAborting(t)
	gotSorted := append([]string(nil), got...)
	wantSorted := append([]string(nil), want...)
	sort.Strings(gotSorted)
	sort.Strings(wantSorted)

	c.Len(gotSorted, len(wantSorted), "got %v, want %v", got, want)
	for i := range gotSorted {
		c.Eq(wantSorted[i], gotSorted[i], "got %v, want %v", got, want)
	}
}
