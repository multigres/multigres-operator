package suite

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// setTableGroups rewrites the database's TableGroup list to exactly the
// default TableGroup plus names, in one update.
func (c *C) setTableGroups(cluster *MultigresCluster, names ...string) {
	c.Helper()
	c.updateCluster(cluster, func(got *MultigresCluster) {
		db := &got.Spec.Databases[0]
		db.TableGroups = slices.DeleteFunc(db.TableGroups, func(tg TableGroupConfig) bool {
			return !tg.Default
		})
		for _, n := range names {
			db.TableGroups = append(db.TableGroups, tableGroupConfig(n))
		}
	})
}

// tgLabels selects one logical TableGroup of cluster, and its children.
func tgLabels(cluster *MultigresCluster, tg string) client.MatchingLabels {
	return client.MatchingLabels{
		"multigres.com/cluster":    cluster.Name,
		"multigres.com/tablegroup": tg,
	}
}

// oneTableGroup returns the single TableGroup object for logical name tg, or
// an error if there is not exactly one.
func (c *C) oneTableGroup(
	cluster *MultigresCluster,
	tg string,
) (*multigresv1alpha1.TableGroup, error) {
	list := &TableGroupList{}
	if err := c.List(list, tgLabels(cluster, tg)); err != nil {
		return nil, err
	}
	if len(list.Items) != 1 {
		return nil, fmt.Errorf("got %d TableGroups for %q, want 1", len(list.Items), tg)
	}
	return &list.Items[0], nil
}

// oneShard returns the single Shard of logical TableGroup tg.
func (c *C) oneShard(cluster *MultigresCluster, tg string) (*Shard, error) {
	list := &ShardList{}
	if err := c.List(list, tgLabels(cluster, tg)); err != nil {
		return nil, err
	}
	if len(list.Items) != 1 {
		return nil, fmt.Errorf("got %d Shards for %q, want 1", len(list.Items), tg)
	}
	return &list.Items[0], nil
}

// waitForTableGroupGone waits until no TableGroup or Shard of logical
// TableGroup tg remains.
func (c *C) waitForTableGroupGone(cluster *MultigresCluster, tg string, within time.Duration) {
	c.Helper()
	c.Eventually(within, "TableGroup "+tg+" and its Shard to be deleted", func() error {
		tgs := &TableGroupList{}
		if err := c.List(tgs, tgLabels(cluster, tg)); err != nil {
			return err
		}
		shards := &ShardList{}
		if err := c.List(shards, tgLabels(cluster, tg)); err != nil {
			return err
		}
		if n := len(tgs.Items) + len(shards.Items); n != 0 {
			return fmt.Errorf(
				"%d TableGroups and %d Shards remain",
				len(tgs.Items),
				len(shards.Items),
			)
		}
		return nil
	})
}

// holdFinalizer is a finalizer no controller knows, put on a pool pod so its
// deletion stays pending until the test lets go. It stands in for a slow
// kubelet: the Shard's drain counts a terminating pod as not yet drained, so
// the whole handshake is held at a known point.
const holdFinalizer = "suite.multigres.com/hold"

// holdOnePod puts holdFinalizer on one pool pod of logical TableGroup tg and
// returns a function that removes it again. The release is also registered
// as a cleanup, through a context that outlives the test's own, so a failing
// test cannot leave a pod the namespace teardown waits on forever.
func (c *C) holdOnePod(t *testing.T, cluster *MultigresCluster, tg string) func() {
	c.Helper()
	pods := &corev1.PodList{}
	c.NoError(c.List(pods, tgLabels(cluster, tg),
		client.MatchingLabels{metadata.LabelAppComponent: shardcontroller.PoolComponentName}),
		"list pool pods of "+tg)
	c.NotEmpty(pods.Items, "TableGroup %s has no pool pods to hold", tg)
	key := client.ObjectKeyFromObject(&pods.Items[0])
	c.NoError(editPod(context.Background(), key, func(pod *corev1.Pod) {
		controllerutil.AddFinalizer(pod, holdFinalizer)
	}), "hold pod "+key.Name)

	release := func() error {
		return editPod(context.Background(), key, func(pod *corev1.Pod) {
			controllerutil.RemoveFinalizer(pod, holdFinalizer)
		})
	}
	t.Cleanup(func() {
		if err := release(); err != nil {
			t.Errorf("release pod %s: %v", key.Name, err)
		}
	})
	return func() {
		c.Helper()
		c.NoError(release(), "release pod "+key.Name)
	}
}

// editPod applies mutate to a fresh read of the pod and writes it back,
// retrying on conflict. A pod that is already gone needs no edit.
func editPod(ctx context.Context, key client.ObjectKey, mutate func(*corev1.Pod)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod := &corev1.Pod{}
		if err := Suite.Client.Get(ctx, key, pod); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		mutate(pod)
		return Suite.Client.Update(ctx, pod)
	})
}

// waitForDrainHeld waits until logical TableGroup tg is mid-drain and held
// there: the TableGroup and its Shard both carry PendingDeletion, at least one
// pool pod is terminating behind holdFinalizer, and neither object has
// reported ReadyForDeletion.
func (c *C) waitForDrainHeld(cluster *MultigresCluster, tg string) {
	c.Helper()
	c.Eventually(60*time.Second, "TableGroup "+tg+" drain to reach the held pod", func() error {
		got, err := c.oneTableGroup(cluster, tg)
		if err != nil {
			return err
		}
		if got.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			return fmt.Errorf("TableGroup %s not yet PendingDeletion", got.Name)
		}
		shard, err := c.oneShard(cluster, tg)
		if err != nil {
			return err
		}
		if shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			return fmt.Errorf("Shard %s not yet PendingDeletion", shard.Name)
		}
		pods := &corev1.PodList{}
		if err := c.List(pods, tgLabels(cluster, tg)); err != nil {
			return err
		}
		for i := range pods.Items {
			p := &pods.Items[i]
			if !p.DeletionTimestamp.IsZero() && controllerutil.ContainsFinalizer(p, holdFinalizer) {
				return nil
			}
		}
		return fmt.Errorf("held pod not yet terminating")
	})
	shard, err := c.oneShard(cluster, tg)
	c.NoError(err, "re-read held Shard")
	c.False(
		meta.IsStatusConditionTrue(
			shard.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion,
		),
		"Shard %s reported ReadyForDeletion while one of its pods was still terminating",
		shard.Name,
	)
	got, err := c.oneTableGroup(cluster, tg)
	c.NoError(err, "re-read held TableGroup")
	c.False(
		meta.IsStatusConditionTrue(
			got.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion,
		),
		"TableGroup %s reported ReadyForDeletion while a pod was still terminating",
		got.Name,
	)
}

// TestTableGroupRemovalAloneStartsHandshake removes a TableGroup from a
// settled cluster and nothing else. multigrescluster's only write to the
// removed TableGroup is the PendingDeletion annotation, which changes neither
// its generation nor anything else the TableGroup controller's For predicate
// watched, and on a settled cluster no Shard event comes along to enqueue it
// instead. The annotation alone must start the TableGroup's reconcile, and so
// the whole drain, through to deletion.
func TestTableGroupRemovalAloneStartsHandshake(t *testing.T) {
	c := newCase(t)
	cluster := c.newMultiTableGroupCluster("tg-annotation", removableTG)

	c.setTableGroups(cluster)
	c.waitForTableGroupGone(cluster, removableTG, 60*time.Second)
	c.WaitForClusterHealthy(cluster)
}

// TestTableGroupDrainDoesNotTrustALaggingCache removes a TableGroup while the
// TableGroup controller's cache shows none of its Shards and the Shard
// controller's cache shows none of that Shard's pods. Both exist and are
// serving; only the caches have not caught up. Each level of the drain must
// still wait for the children the API server holds: the Shard drains its
// pods and the TableGroup waits for the Shard. A level that trusts its cache
// lists zero children, reports a vacuous ReadyForDeletion, and its parent
// deletes it undrained.
//
// One pod is held terminating by holdFinalizer, so the drain cannot finish.
// That turns the question into a state at a known point, rather than an
// ordering between two kinds' watch events: while the pod is held, neither
// the Shard nor the TableGroup may report ReadyForDeletion, and the
// TableGroup must still exist.
func TestTableGroupDrainDoesNotTrustALaggingCache(t *testing.T) {
	c := newCase(t)
	cluster := c.newMultiTableGroupCluster("tg-cache-lag", removableTG)

	release := c.holdOnePod(t, cluster, removableTG)
	unlag := lag.Arm(c.NS, removableTG)
	t.Cleanup(unlag)

	c.setTableGroups(cluster)
	c.waitForDrainHeld(cluster, removableTG)

	release()
	c.waitForTableGroupGone(cluster, removableTG, 60*time.Second)
	unlag()
	c.WaitForClusterHealthy(cluster)
}
