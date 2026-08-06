//go:build e2e

package postgresconfig_test

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/test/e2e/framework"
)

// TestPostgresConfigManagement exercises the whole PostgreSQL-config feature
// end-to-end against a real cluster: the operator renders a baseline, scales it
// from resources, applies the inline spec.postgresConfig map (over the legacy
// ref), validates GUCs at admission, and rolls out + reports config changes.
//
// Every positive assertion reads the *effective* value with `SHOW <param>`
// through the gateway, so it proves the setting took effect in the running
// server — not merely that a ConfigMap has the right text.
func TestPostgresConfigManagement(t *testing.T) {
	ns := cluster.CreateNamespace(t)
	c, err := cluster.CRClient()
	if err != nil {
		t.Fatalf("create CR client: %v", err)
	}
	ctx := context.Background()

	// Legacy postgresConfigRef ConfigMap: sets random_page_cost (not in the
	// inline map, to prove the ref is honored) and work_mem (which the inline map
	// overrides, to prove precedence).
	refCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-ref", Namespace: ns},
		Data: map[string]string{
			"custom.conf": "random_page_cost = '2.5'\nwork_mem = '64MB'",
		},
	}
	if err := c.Create(ctx, refCM); err != nil {
		t.Fatalf("create ref ConfigMap: %v", err)
	}

	cr := configCluster(ns)
	if err := c.Create(ctx, cr); err != nil {
		t.Fatalf("create MultigresCluster: %v", err)
	}

	// Wait for Postgres to come up and serve queries.
	framework.WaitForPod(t, c, ns, "postgres")
	cluster.WaitForAllPodsReady(t, ns)
	gw := framework.FindGatewayService(t, cluster, ns)
	framework.WaitForQueryServing(t, cluster, ns, gw)

	t.Run("operator owns the baseline", func(t *testing.T) {
		// wal_level comes from the operator's rendered baseline, not a user override.
		framework.WaitForPsqlValue(t, cluster, ns, gw, "SHOW wal_level", "logical")
	})

	t.Run("config scales with pool resources", func(t *testing.T) {
		// 512Mi memory limit → shared_buffers = mem/4 = 128MB (distinct from the
		// static 64MB baseline, so this proves sizing is active).
		framework.WaitForPsqlValue(t, cluster, ns, gw, "SHOW shared_buffers", "128MB")
	})

	t.Run("inline spec.postgresConfig is applied", func(t *testing.T) {
		framework.WaitForPsqlValue(t, cluster, ns, gw, "SHOW max_connections", "150")
	})

	t.Run("inline map overrides the legacy ref", func(t *testing.T) {
		// The ref sets work_mem=64MB; the inline map sets 16MB and must win.
		framework.WaitForPsqlValue(t, cluster, ns, gw, "SHOW work_mem", "16MB")
	})

	t.Run("legacy postgresConfigRef is still honored", func(t *testing.T) {
		// random_page_cost is only in the ref (not the map), so it must apply.
		framework.WaitForPsqlValue(t, cluster, ns, gw, "SHOW random_page_cost", "2.5")
	})

	t.Run("operator renders a per-shard ConfigMap", func(t *testing.T) {
		cms := &corev1.ConfigMapList{}
		if err := c.List(ctx, cms, client.InNamespace(ns)); err != nil {
			t.Fatalf("list ConfigMaps: %v", err)
		}
		found := false
		for i := range cms.Items {
			if strings.HasSuffix(cms.Items[i].Name, "-postgres-config") {
				found = true
			}
		}
		if !found {
			t.Error("expected an operator-rendered <shard>-postgres-config ConfigMap")
		}
	})

	// Changing spec.postgresConfig on a running cluster must actually roll out
	// and take effect — not merely apply at initial creation. max_connections is
	// a postmaster (restart-context) GUC, so this drives the primary-last restart
	// path end to end and reads the effective value back from Postgres.
	t.Run("changing spec.postgresConfig takes effect on the running cluster", func(t *testing.T) {
		// Send the full databases array with the one changed value: JSON
		// merge-patch replaces arrays wholesale, so we mutate the live object to
		// preserve every server-defaulted field.
		live := framework.GetCluster(t, c, ns, cr.Name)
		live.Spec.Databases[0].TableGroups[0].Shards[0].Spec.PostgresConfig["max_connections"] = "200"
		patch := framework.MustMarshal(map[string]any{
			"spec": map[string]any{"databases": live.Spec.Databases},
		})
		framework.PatchCluster(t, c, live, patch)

		// Poll the effective value until the rollout lands the change.
		framework.WaitForPsqlValue(t, cluster, ns, gw, "SHOW max_connections", "200")
	})

	// Admission-time behavior (GUC validation rejection) is covered by the
	// webhook envtest integration test — the e2e operator deployment does not
	// install admission webhooks — and config-change rollout uses the same
	// primary-last restart the scaling suite already exercises.
}

// configCluster builds the inline sample with minimal CI resources, then sets a
// 512Mi memory limit on the postgres pool (for a deterministic shared_buffers)
// plus an inline postgresConfig map and the legacy postgresConfigRef.
func configCluster(ns string) *multigresv1alpha1.MultigresCluster {
	cr := framework.MustLoadCluster("config/samples/no-templates.yaml", ns)
	framework.WithCIResources(&cr.Spec)

	shard := &cr.Spec.Databases[0].TableGroups[0].Shards[0]
	shard.Spec.PostgresConfig = map[string]string{
		"work_mem":        "16MB",
		"max_connections": "150",
	}
	shard.Spec.PostgresConfigRef = &multigresv1alpha1.PostgresConfigRef{
		Name: "pg-ref",
		Key:  "custom.conf",
	}
	for name, pool := range shard.Spec.Pools {
		if pool.Postgres.Resources.Limits == nil {
			pool.Postgres.Resources.Limits = corev1.ResourceList{}
		}
		pool.Postgres.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("512Mi")
		shard.Spec.Pools[name] = pool
	}
	return cr
}
