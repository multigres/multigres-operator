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
	"github.com/multigres/multigres-operator/pkg/ctrltest"
)

// TestClusterConvergesUnderAllControllers is the harness proof: one
// MultigresCluster, five live reconcilers, and a faked data plane, reaching a
// terminal healthy state.
//
// It asserts nothing about behaviour that single-controller tests already
// cover. Its job is to fail loudly if the harness itself stops working.
func TestClusterConvergesUnderAllControllers(t *testing.T) {
	ns := Suite.Namespace(t)
	cluster := MinimalCluster(t, ns, "minimal")

	inNS := client.InNamespace(ns)

	ctrltest.Eventually(t, 30*time.Second, "child CRs to be created", func() error {
		topos := &multigresv1alpha1.TopoServerList{}
		if err := Suite.Client.List(t.Context(), topos, inNS); err != nil {
			return err
		}
		cells := &multigresv1alpha1.CellList{}
		if err := Suite.Client.List(t.Context(), cells, inNS); err != nil {
			return err
		}
		tgs := &multigresv1alpha1.TableGroupList{}
		if err := Suite.Client.List(t.Context(), tgs, inNS); err != nil {
			return err
		}
		shards := &multigresv1alpha1.ShardList{}
		if err := Suite.Client.List(t.Context(), shards, inNS); err != nil {
			return err
		}
		if len(topos.Items) == 0 || len(cells.Items) == 0 ||
			len(tgs.Items) == 0 || len(shards.Items) == 0 {
			return fmt.Errorf("have %d TopoServer, %d Cell, %d TableGroup, %d Shard",
				len(topos.Items), len(cells.Items), len(tgs.Items), len(shards.Items))
		}
		return nil
	})

	ctrltest.Eventually(t, 30*time.Second, "cluster to report Healthy", func() error {
		got := &multigresv1alpha1.MultigresCluster{}
		if err := Suite.Client.Get(
			t.Context(),
			client.ObjectKeyFromObject(cluster),
			got,
		); err != nil {
			return err
		}
		if got.Status.Phase != multigresv1alpha1.PhaseHealthy {
			return fmt.Errorf("phase is %q", got.Status.Phase)
		}
		return nil
	})

	// Attribution is the other half of the harness: a test that cannot say
	// which controller wrote cannot assert a protocol between controllers.
	wrote := map[string]bool{}
	for _, op := range Suite.Ops.OpsInNamespace(ns) {
		wrote[op.Controller] = true
	}
	for _, name := range []string{"multigrescluster", "cell", "toposerver", "tablegroup", "shard"} {
		if !wrote[name] {
			t.Errorf("no recorded writes from the %s controller; "+
				"either it never ran or attribution is broken", name)
		}
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
			ns := Suite.Namespace(t)
			cluster := MinimalCluster(t, ns, strings.TrimPrefix(name, "iso-"))

			ctrltest.Eventually(t, 30*time.Second, "cluster to report Healthy", func() error {
				got := &multigresv1alpha1.MultigresCluster{}
				if err := Suite.Client.Get(
					t.Context(), client.ObjectKeyFromObject(cluster), got,
				); err != nil {
					return err
				}
				if got.Status.Phase != multigresv1alpha1.PhaseHealthy {
					return fmt.Errorf("phase is %q", got.Status.Phase)
				}
				return nil
			})

			shards := &multigresv1alpha1.ShardList{}
			if err := Suite.Client.List(
				t.Context(), shards, client.InNamespace(ns),
			); err != nil {
				t.Fatal(err)
			}
			if len(shards.Items) != 1 {
				t.Fatalf("want 1 Shard in %s, got %d", ns, len(shards.Items))
			}
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
	ns := Suite.Namespace(t)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "unlabelled", Namespace: ns},
		StringData: map[string]string{"password": "postgres"},
	}
	if err := Suite.Client.Create(t.Context(), secret); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	key := client.ObjectKeyFromObject(secret)

	ctrltest.Eventually(t, 30*time.Second, "the APIReader to see the secret", func() error {
		return Suite.Mgr.GetAPIReader().Get(t.Context(), key, &corev1.Secret{})
	})

	err := Suite.Mgr.GetClient().Get(t.Context(), key, &corev1.Secret{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("cached client should not see an unlabelled Secret outside %s, got err=%v",
			OperatorNamespace, err)
	}
}
