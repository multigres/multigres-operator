package suite

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// TestClusterConvergesUnderAllControllers is the harness proof: one
// MultigresCluster, five live reconcilers, and a faked data plane, reaching a
// terminal healthy state.
//
// It asserts nothing about behaviour that single-controller tests already
// cover. Its job is to fail loudly if the harness itself stops working.
func TestClusterConvergesUnderAllControllers(t *testing.T) {
	c := newCase(t)
	ns := c.NS
	cluster := c.MinimalCluster("minimal")

	c.Eventually(30*time.Second, "child CRs to be created", func() error {
		topos := &TopoServerList{}
		if err := c.List(topos); err != nil {
			return err
		}
		cells := &multigresv1alpha1.CellList{}
		if err := c.List(cells); err != nil {
			return err
		}
		tgs := &TableGroupList{}
		if err := c.List(tgs); err != nil {
			return err
		}
		shards := &ShardList{}
		if err := c.List(shards); err != nil {
			return err
		}
		if len(topos.Items) == 0 || len(cells.Items) == 0 ||
			len(tgs.Items) == 0 || len(shards.Items) == 0 {
			return fmt.Errorf("have %d TopoServer, %d Cell, %d TableGroup, %d Shard",
				len(topos.Items), len(cells.Items), len(tgs.Items), len(shards.Items))
		}
		return nil
	})

	c.WaitForClusterHealthy(cluster)

	// Attribution is the other half of the harness: a test that cannot say
	// which controller wrote cannot assert a protocol between controllers.
	wrote := map[string]bool{}
	for _, op := range Suite.Ops.OpsInNamespace(ns) {
		wrote[op.Controller] = true
	}
	for _, name := range []string{"multigrescluster", "cell", "toposerver", "tablegroup", "shard"} {
		c.Check().True(wrote[name], "no recorded writes from the %s controller; "+
			"either it never ran or attribution is broken", name)
	}
}

// TestNamespacesAreIsolated runs two clusters at once to prove the isolation
// boundary holds, since every fake behind the suite (the topology store above
// all) is shared process-wide and keyed by namespace.
func TestNamespacesAreIsolated(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"iso-a", "iso-b"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := newCase(t)
			ns := c.NS
			cluster := c.MinimalCluster(strings.TrimPrefix(name, "iso-"))

			c.WaitForClusterHealthy(cluster)

			shards := &ShardList{}
			c.NoError(c.List(shards))
			c.Len(shards.Items, 1, "in %s", ns)
		})
	}
}

// TestProductionCacheConfigIsInEffect guards the fidelity trap: the manager
// must cache exactly what cmd/multigres-operator/main.go caches.
//
// Under the production config an unlabelled Secret outside the operator's own
// namespace is invisible to the cached client, which is why the reconcilers
// carry an APIReader at all. A manager built with default cache options makes
// every cached read behave differently from production and quietly voids the
// premise that these are the real controllers wired as in main.go.
//
// This assertion is only possible because the config lives in pkg/cacheopts
// rather than being copied out of package main, which cannot be imported.
func TestProductionCacheConfigIsInEffect(t *testing.T) {
	c := newCase(t)
	ns := c.NS

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "unlabelled", Namespace: ns},
		StringData: map[string]string{"password": "postgres"},
	}
	c.NoError(c.Create(secret), "create secret")
	key := client.ObjectKeyFromObject(secret)

	c.Eventually(30*time.Second, "the APIReader to see the secret", func() error {
		return Suite.Manager("multigres-operator").
			Mgr.GetAPIReader().
			Get(c.Context(), key, &corev1.Secret{})
	})

	err := Suite.Manager("multigres-operator").
		Mgr.GetClient().
		Get(c.Context(), key, &corev1.Secret{})
	c.True(apierrors.IsNotFound(err),
		"cached client should not see an unlabelled Secret outside %s, got err=%v",
		OperatorNamespace, err)
}
