package shard

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// twoPoolFixture is a shard with pools "main" and "other" of three replicas
// in one cell, production-built Ready pods at the given ordinals, and a pod
// quota that admits a create only while fewer than podLimit pods exist.
type twoPoolFixture struct {
	*replacementFixture
	pool     multigresv1alpha1.PoolSpec
	podLimit int
	// desired are the UIDs of the pods within the replica count as the
	// fixture was built.
	desired map[types.UID]string
}

const fullQuotaMessage = "exceeded quota: full, requested: pods=1, used: pods=6, limited: pods=6"

func newTwoPoolFixture(t *testing.T, main, other []int) *twoPoolFixture {
	t.Helper()
	ctx := t.Context()
	f := &twoPoolFixture{
		replacementFixture: newReplacementFixture(t),
		desired:            map[types.UID]string{},
	}
	require.NoError(t, f.client.Delete(ctx, f.pod))
	f.pool = f.replacementFixture.pool
	f.pool.ReplicasPerCell = ptr.To(int32(3))
	f.pool.Cells = []multigresv1alpha1.CellName{"z1"}
	f.shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
		"main": f.pool, "other": f.pool,
	}
	for poolName, ordinals := range map[string][]int{"main": main, "other": other} {
		for _, i := range ordinals {
			pod, err := BuildPoolPod(f.shard, poolName, "z1", f.pool, i, f.scheme)
			require.NoError(t, err)
			pod.UID = types.UID(pod.Name)
			require.NoError(t, f.client.Create(ctx, pod))
			if i < 3 {
				f.desired[pod.UID] = pod.Name
			}
		}
	}
	f.podLimit = len(main) + len(other)
	f.shard.Status.PodRoles = map[string]string{
		BuildPoolPodName(f.shard, "main", "z1", 2): "PRIMARY",
	}
	backup, err := BuildSharedBackupPVC(f.shard, true, f.scheme)
	require.NoError(t, err)
	require.NoError(t, f.client.Create(ctx, backup))
	base := f.client.(client.WithWatch)
	f.reconciler.Client = interceptor.NewClient(base, interceptor.Funcs{
		Create: func(
			ctx context.Context, c client.WithWatch, obj client.Object,
			opts ...client.CreateOption,
		) error {
			if _, isPod := obj.(*corev1.Pod); isPod && len(f.listPods(t)) >= f.podLimit {
				return forbiddenPod(fullQuotaMessage)
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	f.settle(t)
	return f
}

// settle stands in for the kubelet, the drain state machine and multiorch
// between two passes: created pods become Ready, a pod whose drain was
// requested is gone, and the cohort is observed healthy again. A drain
// takes longer than the wait between two creates of an object a quota
// turned down, so those waits are over as well.
func (f *twoPoolFixture) settle(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	for _, pod := range f.listPods(t) {
		if pod.Annotations[metadata.AnnotationDrainState] != "" {
			require.NoError(t, f.client.Delete(ctx, &pod))
			f.reconciler.quotaWaiting.retryNow(client.ObjectKeyFromObject(f.shard))
			continue
		}
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		require.NoError(t, f.client.Status().Update(ctx, &pod))
	}
	observeHealthyDisruption(t, f.reconciler, f.shard, "main", "z1")
}

// pass reconciles the pools in the given order with one tracker, as one
// reconcile of the Shard does, and returns the tracker and the first error.
func (f *twoPoolFixture) pass(t *testing.T, order ...string) (*shardRolloutTracker, error) {
	t.Helper()
	rollout := &shardRolloutTracker{}
	for _, poolName := range order {
		if err := f.reconciler.reconcilePoolPods(
			withReplacementPass(t.Context()), f.shard, poolName, "z1", f.pool, rollout,
		); err != nil {
			return rollout, err
		}
	}
	return rollout, nil
}

// draining are the names of the pods whose drain was requested, and it fails
// the test if one of them is a pod within its replica count.
func (f *twoPoolFixture) draining(t *testing.T) []string {
	t.Helper()
	var names []string
	for _, pod := range f.listPods(t) {
		if pod.Annotations[metadata.AnnotationDrainState] == "" {
			continue
		}
		names = append(names, pod.Name)
		if name, desired := f.desired[pod.UID]; desired {
			t.Fatalf("desired pod %s was drained while a desired pod is missing", name)
		}
	}
	slices.Sort(names)
	return names
}

func (f *twoPoolFixture) podNames(t *testing.T) []string {
	t.Helper()
	var names []string
	for _, pod := range f.listPods(t) {
		names = append(names, pod.Name)
	}
	slices.Sort(names)
	return names
}

// A pod beyond its pool's replica count is scaled down while a desired pod
// is missing in another pool of the shard. The owner of a namespace at its
// quota makes room by lowering whichever pool they choose, and the missing
// pod's create fails for the room that pod holds, so waiting for the missing
// pod before removing it would wait on the removal itself. The failed create
// must not end the pass either, in whichever order the pools come.
func TestScaleDownMakesRoomForAMissingPodInAnotherPool(t *testing.T) {
	for _, order := range [][]string{{"main", "other"}, {"other", "main"}} {
		t.Run(order[0]+" first", func(t *testing.T) {
			f := newTwoPoolFixture(t, []int{1, 2}, []int{0, 1, 2, 3})
			extra := BuildPoolPodName(f.shard, "other", "z1", 3)
			missing := BuildPoolPodName(f.shard, "main", "z1", 0)

			rollout, err := f.pass(t, order...)
			require.NoError(t, err, "the failed create ended the pass before the scale-down")
			require.ErrorContains(t, rollout.createFailure, "failed to create pod "+missing)
			require.ErrorContains(t, rollout.createFailure, fullQuotaMessage)
			assert.Equal(t, []string{extra}, f.draining(t))

			// The drain ends, and the missing pod fits.
			f.settle(t)
			rollout, err = f.pass(t, order...)
			require.NoError(t, err)
			require.NoError(t, rollout.createFailure)
			assert.Empty(t, f.draining(t))
			assert.Contains(t, f.podNames(t), missing)
			assert.NotContains(t, f.podNames(t), extra)
		})
	}
}

// Only a quota's answer is carried to the end of the pass. A desired pod's
// create that fails for another reason is not waiting for the room an extra
// pod holds, so it ends the pass like any other error, as a claim's does.
func TestAPodCreateFailingForAnotherReasonEndsThePass(t *testing.T) {
	f := newTwoPoolFixture(t, []int{1, 2}, []int{0, 1, 2, 3})
	missing := BuildPoolPodName(f.shard, "main", "z1", 0)
	broken := errors.New("the API server is not answering")
	f.reconciler.Client = interceptor.NewClient(
		f.reconciler.Client.(client.WithWatch), interceptor.Funcs{
			Create: func(
				ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption,
			) error {
				if obj.GetName() == missing {
					return broken
				}
				return c.Create(ctx, obj, opts...)
			},
		})

	rollout, err := f.pass(t, "main", "other")
	require.ErrorIs(t, err, broken)
	require.NoError(t, rollout.createFailure, "a failure that is not a quota's was carried")
	assert.Empty(t, f.draining(t))
}

// Two pools that are each short of a desired pod and each hold a pod beyond
// their replica count do not wait on each other: one extra is drained at a
// time, each removal lets one missing pod be created, and the shard ends
// with its six desired pods. No pod within a replica count is drained on the
// way.
func TestScaleDownConvergesWhenTwoPoolsAreEachShortOfAPod(t *testing.T) {
	f := newTwoPoolFixture(t, []int{1, 2, 3}, []int{1, 2, 3})
	var want []string
	for _, poolName := range []string{"main", "other"} {
		for i := range 3 {
			want = append(want, BuildPoolPodName(f.shard, poolName, "z1", i))
		}
	}
	slices.Sort(want)

	rollout, err := f.pass(t, "main", "other")
	require.NoError(t, err)
	require.ErrorContains(t, rollout.createFailure, fullQuotaMessage)
	require.Len(t, f.draining(t), 1, "one pod beyond a replica count is drained at a time")

	for round := 0; round < 8 && !slices.Equal(f.podNames(t), want); round++ {
		f.settle(t)
		_, err := f.pass(t, "main", "other")
		require.NoError(t, err)
		require.LessOrEqual(t, len(f.draining(t)), 1)
	}
	f.settle(t)
	assert.Equal(t, want, f.podNames(t))
	for uid, name := range f.desired {
		pod := &corev1.Pod{}
		require.NoError(t, f.client.Get(t.Context(),
			client.ObjectKey{Namespace: f.shard.Namespace, Name: name}, pod))
		assert.Equal(t, uid, pod.UID, "desired pod %s was replaced", name)
	}
}

// A Ready pod within its replica count is still not drained for a rollout
// while a desired pod is missing anywhere in the shard, whatever scale-down
// may do.
func TestRolloutStillWaitsWhileScaleDownGoesOn(t *testing.T) {
	f := newTwoPoolFixture(t, []int{1, 2}, []int{0, 1, 2, 3})
	for _, pod := range f.listPods(t) {
		pod.Annotations[metadata.AnnotationSpecHash] = "an-earlier-spec"
		require.NoError(t, f.client.Update(t.Context(), &pod))
	}
	healthy, err := f.reconciler.isShardHealthy(t.Context(), f.shard)
	require.NoError(t, err)
	require.False(t, healthy)
	forScaleDown, err := f.reconciler.isShardHealthyForScaleDown(t.Context(), f.shard)
	require.NoError(t, err)
	require.True(t, forScaleDown)

	_, err = f.pass(t, "main", "other")
	require.NoError(t, err)
	assert.Equal(t, []string{BuildPoolPodName(f.shard, "other", "z1", 3)}, f.draining(t))
}

// A claim counts against a quota as a pod does. A claim's create that a
// quota turns down does not end the pass while the shard holds a pod beyond
// a replica count: that pod's claim may be the room. The pod that would
// mount the missing claim is not created. Any other failure of a claim's
// create, and a quota failure with nothing to scale down, ends the pass.
func TestScaleDownGoesOnWhenAQuotaTurnsDownAClaim(t *testing.T) {
	const storageQuota = "exceeded quota: full, requested: persistentvolumeclaims=1, " +
		"used: persistentvolumeclaims=4, limited: persistentvolumeclaims=4"
	for name, tc := range map[string]struct {
		other    []int
		claimErr error
		deferred bool
	}{
		"quota, a pod beyond the count in another pool": {
			other:    []int{0, 1, 2, 3},
			claimErr: forbiddenClaim(storageQuota), deferred: true,
		},
		"quota, nothing to scale down": {
			other: []int{0, 1, 2}, claimErr: forbiddenClaim(storageQuota),
		},
		"another failure": {
			other:    []int{0, 1, 2, 3},
			claimErr: apierrors.NewInternalError(errors.New("storage backend down")),
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newTwoPoolFixture(t, []int{1, 2}, tc.other)
			f.podLimit = 100
			missing := BuildPoolPodName(f.shard, "main", "z1", 0)
			base := f.reconciler.Client.(client.WithWatch)
			f.reconciler.Client = interceptor.NewClient(base, interceptor.Funcs{
				Create: func(
					ctx context.Context, c client.WithWatch, obj client.Object,
					opts ...client.CreateOption,
				) error {
					if _, isClaim := obj.(*corev1.PersistentVolumeClaim); isClaim {
						return tc.claimErr
					}
					return c.Create(ctx, obj, opts...)
				},
			})

			ctx := withReplacementPass(t.Context())
			rollout := &shardRolloutTracker{}
			err := f.reconciler.reconcilePoolPods(ctx, f.shard, "main", "z1", f.pool, rollout)
			assert.NotContains(t, f.podNames(t), missing,
				"the pod was created without its claim")
			if !tc.deferred {
				require.ErrorIs(t, err, tc.claimErr)
				require.NoError(t, rollout.createFailure)
				return
			}
			require.NoError(t, err)
			require.ErrorIs(t, rollout.createFailure, tc.claimErr)
			pass, _ := ctx.Value(replacementPassKey{}).(*replacementPass)
			assert.True(t, pass.quotaWait, "the Shard must wait on the quota")

			require.NoError(t,
				f.reconciler.reconcilePoolPods(ctx, f.shard, "other", "z1", f.pool, rollout))
			assert.Equal(t, []string{BuildPoolPodName(f.shard, "other", "z1", 3)}, f.draining(t))
		})
	}
}

func forbiddenClaim(detail string) error {
	return apierrors.NewForbidden(
		schema.GroupResource{Resource: "persistentvolumeclaims"}, "claim", errors.New(detail))
}

// A Lite shard whose data claim a quota turns down waits on the quota like
// one whose pod does.
func TestLiteClaimTurnedDownByAQuotaIsAQuotaWait(t *testing.T) {
	shard, pool := newTestLiteShard(), newTestPoolSpec()
	scheme := reloadTestScheme(t)
	turnedDown := forbiddenClaim("exceeded quota: full, requested: requests.storage=10Gi, " +
		"used: requests.storage=95Gi, limited: requests.storage=100Gi")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(
				context.Context, client.WithWatch, client.Object, ...client.CreateOption,
			) error {
				return turnedDown
			},
		}).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	ctx := withReplacementPass(t.Context())
	_, err := r.ensureLiteDataPVC(ctx, shard, "main", "z1", pool)
	require.ErrorIs(t, err, turnedDown)
	pass, _ := ctx.Value(replacementPassKey{}).(*replacementPass)
	assert.True(t, pass.quotaWait)
}

// A maintenance surge's claim and pod are creates of the shard like any
// other: one that a quota turns down puts the Shard on record as waiting on
// the quota, and it is not sent again before its wait is over, while every
// pass in between ends on the quota's answer as if it had been.
func TestMaintenanceSurgeCreateTurnedDownByAQuotaIsAQuotaWait(t *testing.T) {
	for name, claimExists := range map[string]bool{"claim": false, "pod": true} {
		t.Run(name, func(t *testing.T) {
			f := newReplacementFixture(t)
			claims := map[string]*corev1.PersistentVolumeClaim{}
			if claimExists {
				claim, err := BuildPoolDataPVC(f.shard, "main", "z1", f.pool, 1, false, f.scheme)
				require.NoError(t, err)
				require.NoError(t, f.client.Create(t.Context(), claim))
				claims[claim.Name] = claim
			}
			turnedDown := forbiddenPod(fullQuotaMessage)
			sent := 0
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			f.reconciler.Clock = func() time.Time { return now }
			f.reconciler.Client = interceptor.NewClient(
				f.client.(client.WithWatch), interceptor.Funcs{
					Create: func(
						context.Context, client.WithWatch, client.Object, ...client.CreateOption,
					) error {
						sent++
						return turnedDown
					},
				})
			for attempt, wantSent := range []int{1, 1, 2, 2} {
				ctx := withReplacementPass(t.Context())
				pass, _ := ctx.Value(replacementPassKey{}).(*replacementPass)
				err := f.reconciler.createOrAdoptMaintenanceSurge(
					ctx, f.shard, "main", "z1", f.pool, map[string]*corev1.Pod{}, claims, 1)
				require.ErrorIs(t, err, turnedDown)
				require.True(t, quotaExceeded(err))
				require.Equal(t, wantSent, sent, "attempt %d", attempt)
				now = now.Add(quotaHoldFloor / 2)
				assert.True(t, pass.quotaWait)
				assert.True(t,
					f.reconciler.quotaWaiting.waiting(client.ObjectKeyFromObject(f.shard)))
			}
		})
	}
}

// A quota may count claims and not pods: a cap on requests.storage or on
// the number of claims is a common way to bound a namespace. A pool whose
// scale-up waits on such a quota, for the claim of the new ordinal, must
// still recreate a pod within its count that is lost: that pod's claim
// exists and the quota does not look at the pod.
func TestLostPodIsRecreatedWhileAClaimWaitsOnAQuota(t *testing.T) {
	const storageQuota = "exceeded quota: full, requested: requests.storage=10Gi, " +
		"used: requests.storage=95Gi, limited: requests.storage=100Gi"
	f := newTwoPoolFixture(t, []int{0, 1, 2}, []int{0, 1, 2})
	f.podLimit = 100
	pool := f.pool
	pool.ReplicasPerCell = ptr.To(int32(4))
	f.shard.Spec.Pools["main"] = pool
	newClaim := BuildPoolDataPVCName(f.shard, "main", "z1", 3)
	podCreates, claimCreates := 0, 0
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f.reconciler.Clock = func() time.Time { return now }
	base := f.reconciler.Client.(client.WithWatch)
	f.reconciler.Client = interceptor.NewClient(base, interceptor.Funcs{
		Create: func(
			ctx context.Context, c client.WithWatch, obj client.Object,
			opts ...client.CreateOption,
		) error {
			if _, isClaim := obj.(*corev1.PersistentVolumeClaim); isClaim &&
				obj.GetName() == newClaim {
				claimCreates++
				return forbiddenClaim(storageQuota)
			}
			if _, isPod := obj.(*corev1.Pod); isPod {
				podCreates++
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	pass := func() error {
		return f.reconciler.reconcilePoolPods(
			withReplacementPass(t.Context()), f.shard, "main", "z1", pool,
			&shardRolloutTracker{})
	}
	// The scale-up has waited for a while.
	for range 11 {
		require.ErrorContains(t, pass(), storageQuota)
		now = now.Add(quotaHoldCap)
	}
	require.Equal(t, 11, claimCreates)
	require.Zero(t, podCreates)

	lost := BuildPoolPodName(f.shard, "main", "z1", 1)
	pod := &corev1.Pod{}
	require.NoError(t, f.client.Get(t.Context(),
		client.ObjectKey{Namespace: f.shard.Namespace, Name: lost}, pod))
	require.NoError(t, f.client.Delete(t.Context(), pod))
	require.ErrorContains(t, pass(), storageQuota)
	assert.Equal(t, 1, podCreates, "the lost pod must be recreated on the pass after its loss")
	assert.Contains(t, f.podNames(t), lost)
}

// The relaxed count in scale-down's own checks does not decide whether the
// cohort can spare the pod: the disruption preflight does. A desired pod
// that is gone but still a member of the committed cohort is a member
// nobody can observe, so the pod beyond the count is not drained, however
// much room it would free; the pass asks to look again instead. Once the
// missing member is no longer in the cohort, the removal goes ahead.
func TestScaleDownWaitsWhileTheMissingPodIsStillInTheCohort(t *testing.T) {
	f := newTwoPoolFixture(t, []int{0, 1, 2, 3}, []int{0, 1, 2})
	missing := BuildPoolPodName(f.shard, "main", "z1", 0)
	extra := BuildPoolPodName(f.shard, "main", "z1", 3)
	// The cohort was committed with all seven pods. One is lost, and the
	// full quota does not let it back in.
	pod := &corev1.Pod{}
	require.NoError(t, f.client.Get(t.Context(),
		client.ObjectKey{Namespace: f.shard.Namespace, Name: missing}, pod))
	require.NoError(t, f.client.Delete(t.Context(), pod))
	f.podLimit = 6

	for range 3 {
		rollout, err := f.pass(t, "main", "other")
		require.NoError(t, err)
		require.ErrorContains(t, rollout.createFailure, "failed to create pod "+missing)
		require.Empty(t, f.draining(t),
			"a pod was drained while a member of the committed cohort is missing")
		require.True(t, rollout.waitingForRecovery, "the pass must ask to look again")
	}

	// The missing member is evicted from the cohort.
	f.settle(t)
	rollout, err := f.pass(t, "main", "other")
	require.NoError(t, err)
	require.Error(t, rollout.createFailure)
	assert.Equal(t, []string{extra}, f.draining(t))
}

// A maintenance surge's create is turned down by a full quota in the first
// cell of a pool, and the pod that holds the room is a pod beyond the
// replica count in a later cell. The surge's failure must not end the pass
// before that cell is reconciled: nothing but the scale-down there makes
// room, and no event comes while every pass ends at the surge. The surge is
// not counted as started, and maintenance is not granted, until its pod
// exists. The same holds when it is the surge's claim that is turned down.
func TestScaleDownGoesOnWhenAQuotaTurnsDownAMaintenanceSurge(t *testing.T) {
	const claimQuota = "exceeded quota: full, requested: persistentvolumeclaims=1, " +
		"used: persistentvolumeclaims=4, limited: persistentvolumeclaims=4"
	for _, tc := range []struct {
		name            string
		claimTurnedDown bool
		// extraCell and extraOrdinal place the pod beyond the replica count.
		extraCell    string
		extraOrdinal int
	}{
		{name: "pod", extraCell: "z2", extraOrdinal: 1},
		{name: "claim", claimTurnedDown: true, extraCell: "z2", extraOrdinal: 1},
		// The surge takes the first place past the replica count of its own
		// cell, so a pod past that place there is scaled down too.
		{name: "extra in the surge's own cell", extraCell: "z1", extraOrdinal: 2},
	} {
		claimTurnedDown := tc.claimTurnedDown
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			f := newTwoPoolFixture(t, []int{0, 1, 2}, nil)
			for _, pod := range f.listPods(t) {
				require.NoError(t, f.client.Delete(ctx, &pod))
			}
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			f.reconciler.Clock = func() time.Time { return now }
			f.reconciler.Recorder = record.NewFakeRecorder(1000)
			f.shard.Spec.DurabilityPolicy = multiCellAtLeast2Policy
			spec := f.pool
			spec.Cells = []multigresv1alpha1.CellName{"z1", "z2"}
			spec.ReplicasPerCell = ptr.To(int32(1))
			// A second pool keeps the cohort at three pods once the extra is
			// gone.
			other := spec
			other.Cells = []multigresv1alpha1.CellName{"z2"}
			f.shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": spec, "other": other,
			}
			f.podLimit = 4
			serving := BuildPoolPodName(f.shard, "main", "z1", 0)
			extra := BuildPoolPodName(f.shard, "main", tc.extraCell, tc.extraOrdinal)
			surge := BuildPoolPodName(f.shard, "main", "z1", 1)
			for _, at := range []struct {
				pool, cell string
				ordinal    int
			}{
				{"main", "z1", 0},
				{"main", "z2", 0},
				{"main", tc.extraCell, tc.extraOrdinal},
				{"other", "z2", 0},
			} {
				pod, err := BuildPoolPod(f.shard, at.pool, at.cell, spec, at.ordinal, f.scheme)
				require.NoError(t, err)
				if pod.Name == serving {
					pod.Annotations[metadata.AnnotationMaintenanceRequested] = "true"
				}
				require.NoError(t, f.client.Create(ctx, pod))
			}
			f.shard.Status.PodRoles = map[string]string{serving: "PRIMARY"}
			if claimTurnedDown {
				surgeClaim := BuildPoolDataPVCName(f.shard, "main", "z1", 1)
				quota := f.reconciler.Client.(client.WithWatch)
				f.reconciler.Client = interceptor.NewClient(quota, interceptor.Funcs{
					Create: func(
						ctx context.Context, c client.WithWatch, obj client.Object,
						opts ...client.CreateOption,
					) error {
						if obj.GetName() == surgeClaim {
							return forbiddenClaim(claimQuota)
						}
						return c.Create(ctx, obj, opts...)
					},
				})
			}
			f.settle(t)
			if tc.extraCell == "z1" {
				// A second ready pod in the cell would stand in for the
				// surge, and none would be asked for.
				unready := &corev1.Pod{}
				require.NoError(t, f.client.Get(ctx,
					client.ObjectKey{Namespace: f.shard.Namespace, Name: extra}, unready))
				unready.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				}
				require.NoError(t, f.client.Status().Update(ctx, unready))
			}
			key := client.ObjectKeyFromObject(f.shard)
			pass := func() error {
				passCtx := withReplacementPass(ctx)
				rollout := &shardRolloutTracker{}
				err := f.reconciler.reconcilePool(passCtx, f.shard, "main", spec, rollout)
				if err == nil {
					_, err = deferredCreateResult(reconcile.Result{}, rollout.waitingForRecovery,
						rollout.createFailure)
				}
				f.reconciler.endPass(passCtx, key, err)
				return err
			}

			// A surge that fails on anything else still ends the pass.
			broken := errors.New("the API server is not answering")
			quota := f.reconciler.Client
			f.reconciler.Client = interceptor.NewClient(quota.(client.WithWatch), interceptor.Funcs{
				Create: func(
					ctx context.Context, c client.WithWatch, obj client.Object,
					opts ...client.CreateOption,
				) error {
					if obj.GetName() == surge ||
						obj.GetName() == BuildPoolDataPVCName(f.shard, "main", "z1", 1) {
						return broken
					}
					return c.Create(ctx, obj, opts...)
				},
			})
			require.ErrorIs(t, pass(), broken)
			require.Empty(t, f.draining(t), "a surge that failed for another reason was carried")
			f.reconciler.Client = quota

			err := pass()
			require.ErrorContains(t, err, "create maintenance surge")
			require.ErrorContains(t, err, "exceeded quota")
			require.Equal(t, []string{extra}, f.draining(t),
				"the surge's failure ended the pass before the extra pod's cell")
			granted := &corev1.Pod{}
			require.NoError(t, f.client.Get(ctx,
				client.ObjectKey{Namespace: f.shard.Namespace, Name: serving}, granted))
			require.Empty(t, granted.Annotations[metadata.AnnotationMaintenanceReady],
				"maintenance was granted without a surge pod")

			// The cell whose surge was turned down never reaches the rolling
			// update step, and still reports its progress for the shard's
			// RollingUpdate condition. The probe has a reconciler of its own
			// so that its create leaves no record in the one under test, and
			// it is left out where the cell has
			// a scale-down of its own to carry on.
			if tc.extraCell != "z1" {
				probe := &ShardReconciler{
					Client: f.reconciler.Client, Scheme: f.scheme,
					Recorder: record.NewFakeRecorder(1000), Clock: f.reconciler.Clock,
				}
				noted := &shardRolloutTracker{}
				require.NoError(t, probe.reconcilePoolPods(
					withReplacementPass(ctx), f.shard, "main", "z1", spec, noted))
				require.Error(t, noted.createFailure, "the probe's surge was not turned down")
				require.Contains(t, noted.pools, "main")
				require.Zero(t, noted.pools["main"].drifted)

				// What it reports is counted against the pod the operator
				// would build, with its own options: under another sandbox
				// mode the cell's one desired pod is stale.
				probe.PgBackRestSandbox = SandboxAuto
				noted = &shardRolloutTracker{}
				require.NoError(t, probe.reconcilePoolPods(
					withReplacementPass(ctx), f.shard, "main", "z1", spec, noted))
				require.Error(t, noted.createFailure, "the probe's surge was not turned down")
				require.Equal(t, 1, noted.pools["main"].drifted)
			}

			// The extra is gone, which the quota reports, and the surge pod
			// is created on the pass that follows, unless it is its claim
			// that has no room.
			f.settle(t)
			err = pass()
			if claimTurnedDown {
				require.ErrorContains(t, err, claimQuota)
				require.NotContains(t, f.podNames(t), surge)
				return
			}
			require.NoError(t, err)
			require.Contains(t, f.podNames(t), surge)
		})
	}
}
