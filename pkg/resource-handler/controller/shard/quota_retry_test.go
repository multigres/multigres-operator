package shard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// quotaLimiterOf returns the shard controller's rate limiter around r.
func quotaLimiterOf(r *ShardReconciler) quotaRetryLimiter {
	return quotaRetryLimiter{
		TypedRateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			5*time.Millisecond, 1000*time.Second),
		waiting: &r.quotaWaiting, now: r.now,
	}
}

// requireSpacedRetries reconciles a Shard that waits on a quota ten times,
// each time after the delay the rate limiter gives for it, as the queue
// does when nothing else reconciles the Shard. Every one of those passes
// must send a create, and the ten must take at least ten times
// quotaRetryFloor: a retry that is not for a create the Shard can send comes
// back within a millisecond, sends nothing and is given the same delay again.
//
// pass reconciles the Shard once and ends the pass; sent counts the creates
// that have reached the API server; now is the reconciler's clock.
func requireSpacedRetries(
	t *testing.T,
	r *ShardReconciler,
	shard client.ObjectKey,
	now *time.Time,
	sent func() int,
	pass func(),
) {
	t.Helper()
	limiter := quotaLimiterOf(r)
	request := reconcile.Request{NamespacedName: shard}
	start := *now
	for retry := range 10 {
		after := limiter.When(request)
		*now = now.Add(after)
		before := sent()
		pass()
		require.Greater(t, sent(), before,
			"retry %d, after %s, sent no create", retry, after)
	}
	require.GreaterOrEqual(t, now.Sub(start), 10*quotaRetryFloor)
}

// A Shard has a create of each kind on record, and its passes end at one of
// them before they reach the other. The other's due time passes, and nothing
// sends its create. The Shard's retries must follow the create its passes
// end at.
//
// Here a pool's new claim waits on a storage quota, and then a pod before it
// in the pool is lost and a pod quota turns its create down, so that the pass
// ends at the pod. In the second case the pool is scaled back as well, and
// no pass asks for the claim again.
func TestQuotaRetryFollowsTheCreateThePassEndsAt(t *testing.T) {
	const storageQuota = "exceeded quota: full, requested: requests.storage=10Gi, " +
		"used: requests.storage=95Gi, limited: requests.storage=100Gi"
	const podQuota = "exceeded quota: full, requested: pods=1, used: pods=5, limited: pods=5"
	for name, scaledBack := range map[string]bool{
		"the claim is still wanted":           false,
		"the spec no longer asks for a claim": true,
	} {
		t.Run(name, func(t *testing.T) {
			f := newTwoPoolFixture(t, []int{0, 1, 2}, []int{0, 1, 2})
			f.podLimit = 100
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			f.reconciler.Clock = func() time.Time { return now }
			pool := f.pool
			pool.ReplicasPerCell = ptr.To(int32(4))
			f.shard.Spec.Pools["main"] = pool
			newClaim := BuildPoolDataPVCName(f.shard, "main", "z1", 3)
			creates, podsFull := 0, false
			f.reconciler.Client = interceptor.NewClient(
				f.reconciler.Client.(client.WithWatch), interceptor.Funcs{
					Create: func(
						ctx context.Context,
						c client.WithWatch,
						obj client.Object,
						opts ...client.CreateOption,
					) error {
						creates++
						switch obj.(type) {
						case *corev1.PersistentVolumeClaim:
							if obj.GetName() == newClaim {
								return forbiddenClaim(storageQuota)
							}
						case *corev1.Pod:
							if podsFull {
								return forbidden(podQuota)
							}
						}
						return c.Create(ctx, obj, opts...)
					},
				})
			key := client.ObjectKeyFromObject(f.shard)
			pass := func() error {
				ctx := withReplacementPass(t.Context())
				err := f.reconciler.reconcilePoolPods(ctx, f.shard, "main", "z1", pool,
					&shardRolloutTracker{})
				f.reconciler.endPass(ctx, key, err)
				return err
			}
			// The claim's waits double to 64s.
			for _, step := range []time.Duration{0, 1, 1, 2, 4, 8, 16, 32} {
				now = now.Add(step * time.Second)
				require.ErrorContains(t, pass(), storageQuota)
			}

			if scaledBack {
				pool.ReplicasPerCell = ptr.To(int32(3))
				f.shard.Spec.Pools["main"] = pool
			}
			lost := &corev1.Pod{}
			require.NoError(t, f.client.Get(t.Context(), client.ObjectKey{
				Namespace: f.shard.Namespace, Name: BuildPoolPodName(f.shard, "main", "z1", 1),
			}, lost))
			require.NoError(t, f.client.Delete(t.Context(), lost))
			podsFull = true
			// The pod's waits double past the claim's due time.
			for _, step := range []time.Duration{1, 1, 1, 2, 4, 8, 16, 32} {
				now = now.Add(step * time.Second)
				require.ErrorContains(t, pass(), podQuota)
			}

			requireSpacedRetries(t, f.reconciler, key, &now, func() int { return creates },
				func() { require.ErrorContains(t, pass(), podQuota) })
		})
	}
}

// The shared backup claim is applied before any pool is reconciled, so a
// quota that turns it down ends every pass there, and a pod create that a
// quota turned down earlier is never sent. The Shard's retries follow the
// claim, also after a quota event has made the creates of both kinds due at
// once.
func TestQuotaRetryFollowsTheSharedBackupClaim(t *testing.T) {
	for name, quotaEvent := range map[string]bool{"after a quota event": true, "without": false} {
		t.Run(name, func(t *testing.T) {
			f := newReplacementFixture(t)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			r := f.reconciler
			r.Clock = func() time.Time { return now }
			sent := 0
			r.Client = fake.NewClientBuilder().WithScheme(f.scheme).
				WithInterceptorFuncs(interceptor.Funcs{
					Create: func(
						context.Context, client.WithWatch, client.Object, ...client.CreateOption,
					) error {
						sent++
						return errOverQuota
					},
					Patch: func(
						context.Context, client.WithWatch, client.Object, client.Patch,
						...client.PatchOption,
					) error {
						sent++
						return errOverQuota
					},
				}).Build()
			r.Recorder = record.NewFakeRecorder(1000)
			key := client.ObjectKeyFromObject(f.shard)

			// A pod create is turned down. A second later the shared
			// backup claim is missing, and its apply is turned down too.
			ctx := withReplacementPass(t.Context())
			err := r.createUnderQuota(ctx, f.shard, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "missing", Namespace: f.shard.Namespace},
			})
			require.ErrorIs(t, err, errOverQuota)
			r.endPass(ctx, key, err)
			pass := func() {
				ctx := withReplacementPass(t.Context())
				err := r.reconcileSharedBackupPVC(ctx, f.shard)
				require.ErrorIs(t, err, errOverQuota)
				r.endPass(ctx, key, err)
			}
			now = now.Add(time.Second)
			pass()
			if quotaEvent {
				now = now.Add(time.Minute)
				require.Len(t, r.enqueueQuotaWaiters(t.Context(), &corev1.ResourceQuota{
					ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "quota"},
				}), 1)
				pass()
			}

			requireSpacedRetries(t, r, key, &now, func() int { return sent }, pass)
		})
	}
}

// A spec edit can make room that no quota event reports: the claim is asked
// for with a smaller size. The quota's answer to the larger claim says
// nothing about the smaller one, so the pass after the edit sends the create
// at once, however long the Shard has waited, and its error does not quote
// the answer to the claim the spec no longer asks for. A pass of the same
// generation is still held back.
func TestQuotaWaitEndsWhenTheShardSpecChanges(t *testing.T) {
	const noRoom = "exceeded quota: full, requested: requests.storage=10Gi, " +
		"used: requests.storage=95Gi, limited: requests.storage=100Gi"
	f := newTwoPoolFixture(t, []int{0, 1, 2}, []int{0, 1, 2})
	f.podLimit = 100
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	pool := f.pool
	pool.ReplicasPerCell = ptr.To(int32(4))
	f.shard.Spec.Pools["main"] = pool
	newClaim := BuildPoolDataPVCName(f.shard, "main", "z1", 3)
	room := resource.MustParse("5Gi")
	sent := 0
	f.reconciler.Client = interceptor.NewClient(
		f.reconciler.Client.(client.WithWatch), interceptor.Funcs{
			Create: func(
				ctx context.Context,
				c client.WithWatch,
				obj client.Object,
				opts ...client.CreateOption,
			) error {
				if claim, isClaim := obj.(*corev1.PersistentVolumeClaim); isClaim &&
					claim.Name == newClaim {
					sent++
					if claim.Spec.Resources.Requests.Storage().Cmp(room) > 0 {
						return forbiddenClaim(noRoom)
					}
				}
				return c.Create(ctx, obj, opts...)
			},
		})
	key := client.ObjectKeyFromObject(f.shard)
	pass := func() error {
		ctx := withReplacementPass(t.Context())
		err := f.reconciler.reconcilePoolPods(ctx, f.shard, "main", "z1", pool,
			&shardRolloutTracker{})
		f.reconciler.endPass(ctx, key, err)
		return err
	}
	// The claim's waits double to the cap.
	for _, step := range []time.Duration{0, 1, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000} {
		now = now.Add(step * time.Second)
		require.ErrorContains(t, pass(), noRoom)
	}
	require.Equal(t, 13, sent)

	// Another pass of the same generation, a second later, is held back.
	now = now.Add(time.Second)
	require.ErrorContains(t, pass(), "last claim create")
	require.Equal(t, 13, sent)

	// A spec edit that still asks for too much: one create, and its own
	// answer. The waits go on from where they were.
	pool.Storage.Size = "8Gi"
	f.shard.Spec.Pools["main"] = pool
	f.shard.Generation++
	err := pass()
	require.ErrorContains(t, err, noRoom)
	require.NotContains(t, err.Error(), "last claim create")
	require.Equal(t, 14, sent)
	now = now.Add(time.Minute)
	require.ErrorContains(t, pass(), "last claim create")
	require.Equal(t, 14, sent, "a second create for one spec edit")
	after, waiting := f.reconciler.quotaWaiting.retryAfter(key, now)
	require.True(t, waiting)
	require.Equal(t, quotaRetryCap-time.Minute, after)

	// The edit that makes the claim fit.
	pool.Storage.Size = "5Gi"
	f.shard.Spec.Pools["main"] = pool
	f.shard.Generation++
	require.NoError(t, pass())
	require.Equal(t, 15, sent)
	require.False(t, f.reconciler.quotaWaiting.waiting(key))
}

// Two pools are each short of a pod at a full quota, and a pass ends at the
// first create that is turned down. Which pool that is changes from pass to
// pass, since pools are reconciled in map order. What status says about the
// wait must not follow it: sixty retries, each when a create is due and with
// the pools in alternating order, give one text.
func TestQuotaWaitInStatusDoesNotFollowPoolOrder(t *testing.T) {
	f := newTwoPoolFixture(t, []int{1, 2}, []int{1, 2})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	key := client.ObjectKeyFromObject(f.shard)
	orders := [][]string{{"main", "other"}, {"other", "main"}}
	texts := map[string]int{}
	for retry := range 60 {
		ctx := withReplacementPass(t.Context())
		f.reconciler.setConditions(f.shard, 6, 4)
		f.reconciler.nameQuotaWaits(f.shard, quotaObjectsSeen{})
		texts[meta.FindStatusCondition(f.shard.Status.Conditions, "Available").Message]++
		var err error
		for _, poolName := range orders[retry%2] {
			err = f.reconciler.reconcilePoolPods(ctx, f.shard, poolName, "z1", f.pool,
				&shardRolloutTracker{})
			if err != nil {
				break
			}
		}
		require.ErrorContains(t, err, fullQuotaMessage)
		require.NotContains(t, err.Error(), "not sent again", "retry %d sent no create", retry)
		f.reconciler.endPass(ctx, key, err)
		now = now.Add(quotaRetryCap + time.Second)
	}
	delete(texts, "4/6 pods ready")
	require.Len(t, texts, 1, "the text changed while the wait did not: %v", texts)
	for text := range texts {
		require.Equal(t, "4/6 pods ready; pod "+BuildPoolPodName(f.shard, "main", "z1", 0)+
			" waiting on ResourceQuota full (pods) since 2026-01-01T00:00:00Z", text)
	}
}

// poolStatusText computes the Available message the way the status update
// does: from the pods it lists, and with the quota waits checked against
// them.
func poolStatusText(t *testing.T, r *ShardReconciler, shard *multigresv1alpha1.Shard) string {
	t.Helper()
	ps, err := r.updatePoolsStatus(
		t.Context(),
		shard,
		map[multigresv1alpha1.CellName]bool{},
		"",
		"",
	)
	require.NoError(t, err)
	r.setConditions(shard, ps.totalPods, ps.readyPods)
	r.nameQuotaWaits(shard, ps.seen)
	return meta.FindStatusCondition(shard.Status.Conditions, "Available").Message
}

// A maintenance surge pod is turned down by a full pod quota and named in
// status. The surge is then withdrawn, which changes no generation, while
// another pod of the shard is still missing and keeps the wait going. Status
// must stop naming the surge pod, which nothing tries to create any more,
// and name the pod whose create is turned down, with the wait's start time.
func TestQuotaWaitInStatusLeavesAWithdrawnSurgePod(t *testing.T) {
	f := newTwoPoolFixture(t, []int{0}, []int{0, 1})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	f.reconciler.Recorder = record.NewFakeRecorder(1000)
	key := client.ObjectKeyFromObject(f.shard)
	ctx := t.Context()
	pool := f.pool
	pool.ReplicasPerCell = ptr.To(int32(1))
	pool.Cells = []multigresv1alpha1.CellName{"z1", "z2"}
	f.shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy
	for _, pod := range f.listPods(t) {
		if pod.Labels[metadata.LabelMultigresPool] == "other" {
			require.NoError(t, f.client.Delete(ctx, &pod))
		}
	}
	f.podLimit = 1
	serving := BuildPoolPodName(f.shard, "main", "z1", 0)
	maintenance := func(requested bool) {
		pod := &corev1.Pod{}
		require.NoError(t, f.client.Get(ctx,
			client.ObjectKey{Namespace: f.shard.Namespace, Name: serving}, pod))
		if requested {
			pod.Annotations[metadata.AnnotationMaintenanceRequested] = "true"
		} else {
			delete(pod.Annotations, metadata.AnnotationMaintenanceRequested)
		}
		require.NoError(t, f.client.Update(ctx, pod))
	}
	pass := func() (text string, err error) {
		passCtx := withReplacementPass(ctx)
		text = poolStatusText(t, f.reconciler, f.shard)
		rollout := &shardRolloutTracker{}
		for _, cell := range []string{"z1", "z2"} {
			if err = f.reconciler.reconcilePoolPods(passCtx, f.shard, "main", cell, pool,
				rollout); err != nil {
				break
			}
		}
		f.reconciler.endPass(passCtx, key, err)
		now = now.Add(quotaRetryCap + time.Second)
		return text, err
	}
	const since = " waiting on ResourceQuota full (pods) since 2026-01-01T00:00:00Z"
	surge := BuildPoolPodName(f.shard, "main", "z1", 1)
	missing := BuildPoolPodName(f.shard, "main", "z2", 0)

	maintenance(true)
	_, err := pass()
	require.ErrorContains(t, err, "create maintenance surge pod "+surge)
	text, err := pass()
	require.ErrorContains(t, err, "create maintenance surge pod "+surge)
	require.Equal(t, "1/2 pods ready; pod "+surge+since, text,
		"a surge pod whose create is the one turned down is named")

	maintenance(false)
	_, err = pass()
	require.ErrorContains(t, err, "failed to create pod "+missing)
	for range 4 {
		text, err = pass()
		require.ErrorContains(t, err, "failed to create pod "+missing)
		require.Equal(t, "1/2 pods ready; pod "+missing+since, text)
	}
}

// The pod named stays missing, and every pass ends at another pod's create
// before it reaches the named one, so its own answer is never renewed. When
// the quota that refused it is replaced by another, status names the quota
// that turns the Shard's pods down now, for the same pod and start time.
func TestQuotaWaitInStatusFollowsAReplacedQuota(t *testing.T) {
	f := newTwoPoolFixture(t, []int{1, 2}, []int{1, 2})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	f.reconciler.Recorder = record.NewFakeRecorder(1000)
	key := client.ObjectKeyFromObject(f.shard)
	answer := "exceeded quota: small, requested: pods=1, used: pods=4, limited: pods=4"
	f.reconciler.Client = interceptor.NewClient(
		f.client.(client.WithWatch), interceptor.Funcs{
			Create: func(
				ctx context.Context,
				c client.WithWatch,
				obj client.Object,
				opts ...client.CreateOption,
			) error {
				if _, isPod := obj.(*corev1.Pod); isPod {
					return forbiddenPod(answer)
				}
				return c.Create(ctx, obj, opts...)
			},
		})
	pass := func(first string) string {
		ctx := withReplacementPass(t.Context())
		text := poolStatusText(t, f.reconciler, f.shard)
		err := f.reconciler.reconcilePoolPods(ctx, f.shard, first, "z1", f.pool,
			&shardRolloutTracker{})
		require.ErrorContains(t, err, "exceeded quota")
		f.reconciler.endPass(ctx, key, err)
		return text
	}
	named := BuildPoolPodName(f.shard, "other", "z1", 0)
	want := func(quota string) string {
		return "4/6 pods ready; pod " + named + " waiting on ResourceQuota " + quota +
			" since 2026-01-01T00:00:00Z"
	}
	pass("other")
	for _, step := range []time.Duration{1, 1, 2, 4, 8} {
		now = now.Add(step * time.Second)
		require.Equal(t, want("small (pods)"), pass("main"))
	}

	answer = "exceeded quota: team, requested: requests.memory=1Gi, " +
		"used: requests.memory=8Gi, limited: requests.memory=8Gi"
	require.Len(t, f.reconciler.enqueueQuotaWaiters(t.Context(), &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "small"},
	}), 1)
	now = now.Add(16 * time.Second)
	pass("main")
	now = now.Add(time.Second)
	require.Equal(t, want("team (requests.memory)"), pass("main"))
}

// Two pools in two cells each need a maintenance surge, and a full pod quota
// turns both surge pods down. A pass ends at the first of them, and which
// that is follows the pools' map order. Neither pod is within a replica
// count, so neither is a better name than the other: status keeps the first
// one named, whichever is turned down after it.
func TestQuotaWaitInStatusKeepsOneOfTwoSurgePods(t *testing.T) {
	ctx := t.Context()
	f := newTwoPoolFixture(t, []int{0, 1, 2}, nil)
	for _, pod := range f.listPods(t) {
		require.NoError(t, f.client.Delete(ctx, &pod))
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	f.reconciler.Recorder = record.NewFakeRecorder(10000)
	f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy
	f.podLimit = 2
	cells := map[string]string{"main": "z1", "other": "z2"}
	for pool, cell := range cells {
		spec := f.pool
		spec.Cells = []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cell)}
		spec.ReplicasPerCell = ptr.To(int32(1))
		f.shard.Spec.Pools[multigresv1alpha1.PoolName(pool)] = spec
		pod, err := BuildPoolPod(f.shard, pool, cell, spec, 0, f.scheme)
		require.NoError(t, err)
		pod.Annotations[metadata.AnnotationMaintenanceRequested] = "true"
		require.NoError(t, f.client.Create(ctx, pod))
	}
	for _, pod := range f.listPods(t) {
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		require.NoError(t, f.client.Status().Update(ctx, &pod))
	}
	key := client.ObjectKeyFromObject(f.shard)
	// pass reconciles the pools in the given order, as one reconcile of the
	// Shard does, and returns the status it starts with.
	pass := func(order ...string) string {
		passCtx := withReplacementPass(ctx)
		text := poolStatusText(t, f.reconciler, f.shard)
		rollout := &shardRolloutTracker{}
		var err error
		for _, pool := range order {
			spec := f.shard.Spec.Pools[multigresv1alpha1.PoolName(pool)]
			if err = f.reconciler.reconcilePoolPods(passCtx, f.shard, pool, cells[pool], spec,
				rollout); err != nil {
				break
			}
		}
		require.ErrorContains(t, err,
			"create maintenance surge pod "+BuildPoolPodName(f.shard, order[0], cells[order[0]], 1))
		require.NotContains(t, err.Error(), "not sent again")
		f.reconciler.endPass(passCtx, key, err)
		now = now.Add(quotaRetryCap + time.Second)
		return text
	}
	want := "All 2 pods are ready; pod " + BuildPoolPodName(f.shard, "main", "z1", 1) +
		" waiting on ResourceQuota full (pods) since 2026-01-01T00:00:00Z"

	pass("main", "other")
	// Ten due retries with both pools in alternating order, then eight
	// that each reach one pool only.
	for retry := range 10 {
		order := [][]string{{"other", "main"}, {"main", "other"}}[retry%2]
		require.Equal(t, want, pass(order...), "retry %d", retry)
	}
	for retry := range 8 {
		require.Equal(t, want, pass([]string{"other", "main"}[retry%2]), "retry %d", retry)
	}
}
