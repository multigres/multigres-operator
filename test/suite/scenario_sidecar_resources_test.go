package suite

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// sidecarResources returns the resources of the pod's pgBackRest sidecar.
func sidecarResources(pod *corev1.Pod) (corev1.ResourceRequirements, bool) {
	for _, ctr := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		if ctr.Name == sidecarName {
			return ctr.Resources, true
		}
	}
	return corev1.ResourceRequirements{}, false
}

// TestSidecarResourcesAbsentAndEmptied follows a pool's pgbackrestResources
// from the MultigresCluster to the pod, for the two states that carry no
// entries. A cluster that does not set the block gets the sidecar defaults:
// they are resolved into the TableGroup and the Shard and reach the
// container. A block set to `{limits: {}}` is not defaulted: the Shard
// carries no block, and a pod created from it has a sidecar with no requests
// or limits.
//
// The emptied block is written through unstructured content, because a typed
// client drops a non-nil empty map. A two-pod pool is not rolled by the
// simulated data plane, so the pod built from the emptied block is the one
// the controller creates in place of a pod the test deletes.
func TestSidecarResourcesAbsentAndEmptied(t *testing.T) {
	c := newCase(t)
	cluster := c.lifecycleCluster("sidecar-res")
	c.WaitForClusterHealthy(cluster)

	shardPool := func() (multigresv1alpha1.PoolSpec, error) {
		shards := &multigresv1alpha1.ShardList{}
		if err := c.List(shards); err != nil {
			return multigresv1alpha1.PoolSpec{}, err
		}
		if len(shards.Items) != 1 {
			return multigresv1alpha1.PoolSpec{}, fmt.Errorf("%d shards, want 1", len(shards.Items))
		}
		return shards.Items[0].Spec.Pools[resolver.DefaultPoolName], nil
	}

	defaults := resolver.DefaultResourcesPgBackRest()
	pool, err := shardPool()
	c.NoError(err)
	c.Check().True(apiequality.Semantic.DeepEqual(pool.PgBackRestResources, defaults),
		"the Shard's pool carries %+v, want the defaults", pool.PgBackRestResources)
	pods := c.awaitPoolPods(2, "the pool's two pods")
	for i := range pods {
		got, ok := sidecarResources(&pods[i])
		c.True(ok, "pod %s has no pgBackRest sidecar", pods[i].Name)
		c.Check().True(apiequality.Semantic.DeepEqual(got, defaults),
			"pod %s sidecar resources %+v, want the defaults", pods[i].Name, got)
	}

	c.NoError(testutil.UpdateUnstructured(
		c.Context(), c.Client(),
		multigresv1alpha1.GroupVersion.WithKind("MultigresCluster"),
		client.ObjectKeyFromObject(cluster),
		func(content map[string]any) error {
			return testutil.SetUnstructuredPath(content, []any{
				"spec", "databases", 0, "tablegroups", 0, "shards", 0, "spec",
				"pools", string(resolver.DefaultPoolName), "pgbackrestResources",
			}, map[string]any{"limits": map[string]any{}})
		}), "empty the pool's pgbackrestResources")

	c.Eventually(30*time.Second, "the Shard to carry no pgbackrestResources", func() error {
		pool, err := shardPool()
		if err != nil {
			return err
		}
		if !testutil.IsEmptyResources(pool.PgBackRestResources) {
			return fmt.Errorf("the Shard's pool still carries %+v", pool.PgBackRestResources)
		}
		return nil
	})

	// The pod with the highest ordinal: the pool's last replica.
	gone := pods[len(pods)-1]
	for i := range pods {
		if pods[i].Name > gone.Name {
			gone = pods[i]
		}
	}
	c.NoError(c.Delete(gone.DeepCopy()), "delete pod %s", gone.Name)
	c.Eventually(60*time.Second, "pod "+gone.Name+" to be created again", func() error {
		pod := &corev1.Pod{}
		if err := c.Get(client.ObjectKeyFromObject(&gone), pod); err != nil {
			return err
		}
		if pod.UID == gone.UID {
			return fmt.Errorf("pod %s is still the original", gone.Name)
		}
		got, ok := sidecarResources(pod)
		if !ok {
			return fmt.Errorf("pod %s has no pgBackRest sidecar", pod.Name)
		}
		if !testutil.IsEmptyResources(got) {
			return fmt.Errorf("pod %s sidecar resources %+v, want none", pod.Name, got)
		}
		return nil
	})
}
