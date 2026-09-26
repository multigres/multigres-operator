//go:build e2e

package postgresconfig_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"

	"github.com/multigres/testkit/assert"
)

// TestBaselineWinsOverRef verifies end-to-end that the operator's own
// resource-derived baseline wins over the deprecated PostgresConfigRef.
//
// This is the real-project shape that failed: a shard is created with a
// postgresConfigRef still present (the worker's create-time wiring) that sets a
// resource-derived key — here effective_cache_size — to a value the operator's
// sizing would never produce, and NO inline override. With a 512Mi pool memory
// limit the operator sizes effective_cache_size to mem*3/4 = 384MB; the ref sets
// 999MB.
//
//   - Before the fix (baseline rendered BEFORE ref): ref wins last-write-wins, so
//     SHOW effective_cache_size == "999MB" and this test FAILS.
//   - After the fix (baseline rendered AFTER ref): the operator's 384MB wins and
//     this test PASSES.
//
// It also asserts a ref-only key (seq_page_cost, which the baseline does not set)
// still applies, proving the fix layers the ref UNDER the baseline rather than
// ignoring it.
func TestBaselineWinsOverRef(t *testing.T) {
	ck := assert.NewAborting(t)
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	ck.NoError(err, "create CR client")
	ctx := context.Background()

	// Deprecated ref (key "postgresql.conf", like the real project): sets a
	// resource-derived baseline key to a value the operator would never size to,
	// plus a ref-only key that the baseline does not set.
	refCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-ref", Namespace: ns},
		Data: map[string]string{
			// effective_cache_size is set by the operator's baseline (the contested
			// key); seq_page_cost is NOT set by the baseline (a genuinely ref-only
			// key, to prove the ref is still layered in, not discarded).
			"postgresql.conf": "effective_cache_size = '999MB'\nseq_page_cost = '2.5'",
		},
	}
	ck.NoError(c.Create(ctx, refCM), "create ref ConfigMap")

	// Create with the ref set and NO inline postgresConfig, and pin the pool
	// memory to 512Mi so the operator's sized effective_cache_size is a
	// deterministic 384MB (= mem*3/4).
	cr := framework.MustLoadCluster("config/samples/no-templates.yaml", ns)
	framework.WithCIResources(&cr.Spec)
	shard := &cr.Spec.Databases[0].TableGroups[0].Shards[0]
	shard.Spec.PostgresConfigRef = &multigresv1alpha1.PostgresConfigRef{
		Name: "pg-ref",
		Key:  "postgresql.conf",
	}
	for name, pool := range shard.Spec.Pools {
		if pool.Postgres.Resources.Limits == nil {
			pool.Postgres.Resources.Limits = corev1.ResourceList{}
		}
		pool.Postgres.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("512Mi")
		shard.Spec.Pools[name] = pool
	}
	ck.NoError(c.Create(ctx, cr), "create MultigresCluster")

	framework.WaitForPod(t, c, ns, "postgres")
	cluster.WaitForAllPodsReady(t, ns)
	gw := framework.FindGatewayService(t, cluster, ns)
	framework.WaitForQueryServing(t, cluster, ns, gw)

	// The operator's resource-derived baseline must win over the ref: 384MB, not
	// the ref's 999MB. This is the assertion that fails before the fix.
	framework.WaitForPsqlValue(t, cluster, ns, gw, "SHOW effective_cache_size", "384MB")

	// The ref is still honored for keys the baseline does not set — it is layered
	// UNDER the baseline, not discarded.
	framework.WaitForPsqlValue(t, cluster, ns, gw, "SHOW seq_page_cost", "2.5")
}
