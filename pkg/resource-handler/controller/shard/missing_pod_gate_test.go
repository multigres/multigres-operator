package shard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// While a desired pod of the shard does not exist, no Ready pod is drained
// for a rollout, in any pool. The pass that fails to create the missing pod
// ends there, but pools are reconciled in no fixed order, so a pool that
// comes earlier in the pass than the one with the missing pod relies on the
// health check alone. That check must see the missing pod also when its pool
// holds a pod beyond the replica count: replicasPerCell lowered while the
// create keeps failing leaves exactly that, and the extra is not drained
// itself a desired pod. The extra is what scale-down removes, and it is
// removed although a desired pod of its pool is missing and that pod's create
// fails: in a namespace at its quota the extra holds the room the missing pod
// needs.
func TestRolloutWaitsForAMissingPodInAnotherPool(t *testing.T) {
	const quota = "exceeded quota: full, requested: pods=1, used: pods=100, limited: pods=100"
	for name, tc := range map[string]struct {
		// missing leaves pod 0 of pool "other" out, and extra adds its pod 3,
		// one beyond the replica count.
		missing, extra bool
		drained        bool
	}{
		"every desired pod exists":            {drained: true},
		"a pod beyond the replica count":      {extra: true, drained: true},
		"a desired pod is missing":            {missing: true},
		"missing, and a pod beyond the count": {missing: true, extra: true},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			f := newReplacementFixture(t)
			require.NoError(t, f.client.Delete(ctx, f.pod))
			pool := f.pool
			pool.ReplicasPerCell = ptr.To(int32(3))
			pool.Cells = []multigresv1alpha1.CellName{"z1"}
			f.shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": pool, "other": pool,
			}
			ready := []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			for _, poolName := range []string{"main", "other"} {
				for i := range 4 {
					if i == 3 && (poolName != "other" || !tc.extra) ||
						i == 0 && poolName == "other" && tc.missing {
						continue
					}
					pod, err := BuildPoolPod(f.shard, poolName, "z1", pool, i, f.scheme)
					require.NoError(t, err)
					if poolName == "main" {
						pod.Annotations[metadata.AnnotationSpecHash] = "an-earlier-spec"
					}
					pod.Status.Conditions = ready
					require.NoError(t, f.client.Create(ctx, pod))
				}
			}
			f.shard.Status.PodRoles = map[string]string{
				BuildPoolPodName(f.shard, "main", "z1", 2): "PRIMARY",
			}
			observeHealthyDisruption(t, f.reconciler, f.shard, "main", "z1")
			backup, err := BuildSharedBackupPVC(f.shard, true, f.scheme)
			require.NoError(t, err)
			require.NoError(t, f.client.Create(ctx, backup))
			// The namespace's quota admits no pod, as a dry run or for real.
			f.reconciler.Client = interceptor.NewClient(
				f.client.(client.WithWatch), interceptor.Funcs{
					Create: func(
						ctx context.Context, c client.WithWatch, obj client.Object,
						opts ...client.CreateOption,
					) error {
						if _, isPod := obj.(*corev1.Pod); isPod {
							return forbiddenPod(quota)
						}
						return c.Create(ctx, obj, opts...)
					},
				})

			missing := missingDesiredPods(f.shard, f.listPods(t))
			healthy, err := f.reconciler.isShardHealthy(ctx, f.shard)
			require.NoError(t, err)
			assert.Equal(t, tc.missing, len(missing) > 0)
			assert.Equal(t, !tc.missing, healthy,
				"the shard is healthy exactly when no desired pod is missing")

			// The pool with out-of-date pods is reconciled first.
			rollout := &shardRolloutTracker{}
			require.NoError(
				t,
				f.reconciler.reconcilePoolPods(ctx, f.shard, "main", "z1", pool, rollout),
			)
			var draining []string
			for _, pod := range f.listPods(t) {
				if pod.Annotations[metadata.AnnotationDrainState] != "" {
					draining = append(draining, pod.Name)
				}
			}
			if tc.drained {
				assert.Len(t, draining, 1, "a rollout of a healthy shard drains one pod")
			} else {
				assert.Empty(t, draining, "a pod was drained while %v does not exist", missing)
				assert.False(t, rollout.HasStarted())
			}

			err = f.reconciler.reconcilePoolPods(ctx, f.shard, "other", "z1", pool, rollout)
			extraName := BuildPoolPodName(f.shard, "other", "z1", 3)
			switch {
			case tc.missing && tc.extra:
				// The failed create is carried to the end of the pass, and
				// the pod beyond the replica count starts to drain.
				require.NoError(t, err)
				require.ErrorContains(t, rollout.createFailure, quota)
				for _, pod := range f.listPods(t) {
					assert.Equal(t, pod.Name == extraName,
						pod.Annotations[metadata.AnnotationDrainState] != "", pod.Name)
				}
			case tc.missing:
				require.ErrorContains(t, err, quota)
				assert.NoError(t, rollout.createFailure)
			default:
				require.NoError(t, err)
				assert.NoError(t, rollout.createFailure)
			}
		})
	}
}

func (f *replacementFixture) listPods(t *testing.T) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	require.NoError(t, f.client.List(t.Context(), pods, client.InNamespace(f.shard.Namespace)))
	return pods.Items
}

// A desired pod can also be missing without a failing create: it is not
// created while a PVC it mounts is being deleted. The pod beyond the replica
// count is scaled down all the same, and no desired pod is drained.
func TestScaleDownGoesOnWhileADesiredPodWaitsForItsPVC(t *testing.T) {
	ctx := t.Context()
	f := newReplacementFixture(t)
	require.NoError(t, f.client.Delete(ctx, f.pod))
	pool := f.pool
	pool.ReplicasPerCell = ptr.To(int32(3))
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	f.shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	for _, i := range []int{0, 2, 3} {
		pod, err := BuildPoolPod(f.shard, "main", "z1", pool, i, f.scheme)
		require.NoError(t, err)
		pod.Annotations[metadata.AnnotationSpecHash] = "an-earlier-spec"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		require.NoError(t, f.client.Create(ctx, pod))
	}
	// Pod 1's data PVC is being deleted and something still holds it.
	pvc, err := BuildPoolDataPVC(f.shard, "main", "z1", pool, 1, false, f.scheme)
	require.NoError(t, err)
	pvc.Finalizers = []string{"example.com/held"}
	require.NoError(t, f.client.Create(ctx, pvc))
	require.NoError(t, f.client.Delete(ctx, pvc))
	f.shard.Status.PodRoles = map[string]string{
		BuildPoolPodName(f.shard, "main", "z1", 0): "PRIMARY",
	}
	observeHealthyDisruption(t, f.reconciler, f.shard, "main", "z1")
	backup, err := BuildSharedBackupPVC(f.shard, true, f.scheme)
	require.NoError(t, err)
	require.NoError(t, f.client.Create(ctx, backup))

	rollout := &shardRolloutTracker{}
	require.NoError(t, f.reconciler.reconcilePoolPods(ctx, f.shard, "main", "z1", pool, rollout))
	missing := BuildPoolPodName(f.shard, "main", "z1", 1)
	extra := BuildPoolPodName(f.shard, "main", "z1", 3)
	for _, pod := range f.listPods(t) {
		require.NotEqual(t, missing, pod.Name, "the pod was created on a PVC being deleted")
		assert.Equal(t, pod.Name == extra,
			pod.Annotations[metadata.AnnotationDrainState] != "", pod.Name)
	}
	assert.NoError(t, rollout.createFailure)
}
