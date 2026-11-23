//go:build integration

package shard

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// shardUnderQuota is a shard in an envtest API server, reconciled by
// calling Reconcile, in a namespace whose ResourceQuotas the test writes.
// Envtest has no quota controller, so the test sets each quota's status;
// admission is the API server's own.
type shardUnderQuota struct {
	c       client.WithWatch
	r       *ShardReconciler
	shard   *multigresv1alpha1.Shard
	request reconcile.Request
	now     time.Time
	// sent counts the pod creates and claim applies sent to the API server.
	sent int
}

// newShardUnderQuota stores a shard with one pool of one replica, "default",
// and as many more like it as morePools names. With lite it is a Lite shard.
func newShardUnderQuota(
	t *testing.T,
	name string,
	lite bool,
	morePools ...string,
) *shardUnderQuota {
	t.Helper()
	cfg := testutil.SetUpEnvtest(t,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")))
	scheme := testScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, policyv1.AddToScheme(scheme))
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := t.Context()
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multigres-admin-password", Namespace: "default"},
		Data:       map[string][]byte{PostgresPasswordSecretKey: []byte("pw")},
	}))
	stored := storedByPreviousOperator(name, lite)
	require.NoError(t, c.Create(ctx, stored))
	shard := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(stored), shard))
	shard.Spec.Images.PgBackRestd = "ghcr.io/multigres/multigres:new"
	shard.Spec.Backup.Filesystem.Storage.Class = ""
	shard.Spec.SidecarBackupLocation = shard.Spec.Backup.DeepCopy()
	for _, pool := range morePools {
		shard.Spec.Pools[multigresv1alpha1.PoolName(pool)] = shard.Spec.Pools["default"]
	}
	require.NoError(t, c.Update(ctx, shard))

	f := &shardUnderQuota{
		c: c, shard: shard, request: requestOf(shard),
		now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	f.r = &ShardReconciler{
		APIReader: c, Scheme: scheme, Recorder: record.NewFakeRecorder(1000),
		CreateTopoStore: newMemoryTopoFactory(), Clock: func() time.Time { return f.now },
	}
	f.r.Client = interceptor.NewClient(c, interceptor.Funcs{
		Create: func(
			ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption,
		) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				f.sent++
			}
			return c.Create(ctx, obj, opts...)
		},
		Patch: func(
			ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch,
			opts ...client.PatchOption,
		) error {
			if _, isClaim := obj.(*corev1.PersistentVolumeClaim); isClaim &&
				p.Type() == types.ApplyPatchType {
				f.sent++
			}
			return c.Patch(ctx, obj, p, opts...)
		},
	})
	return f
}

// quota writes the ResourceQuota called name, with one resource, and its
// status.
func (f *shardUnderQuota) quota(
	t *testing.T,
	name string,
	resourceName corev1.ResourceName,
	hard, used string,
) *corev1.ResourceQuota {
	t.Helper()
	ctx := t.Context()
	limits := corev1.ResourceList{resourceName: resource.MustParse(hard)}
	quota := &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	if err := f.c.Get(ctx, client.ObjectKeyFromObject(quota), quota); err != nil {
		quota.Spec.Hard = limits
		require.NoError(t, f.c.Create(ctx, quota))
	} else {
		quota.Spec.Hard = limits
		require.NoError(t, f.c.Update(ctx, quota))
	}
	quota.Status = corev1.ResourceQuotaStatus{
		Hard: limits, Used: corev1.ResourceList{resourceName: resource.MustParse(used)},
	}
	require.NoError(t, f.c.Status().Update(ctx, quota))
	return quota
}

// reconcileUntilOverQuota reconciles the shard until a pass ends on a
// quota's answer, and returns that error.
func (f *shardUnderQuota) reconcileUntilOverQuota(t *testing.T) error {
	t.Helper()
	var err error
	require.Eventually(t, func() bool {
		_, err = f.r.Reconcile(t.Context(), f.request)
		return quotaExceeded(err)
	}, 10*time.Second, 100*time.Millisecond)
	return err
}

// A Lite shard's pod create is turned down by a pod quota. Its shared backup
// claim then goes missing under a claim quota with no room, and a quota
// event makes the creates of both kinds due. Every whole Reconcile now ends
// at the claim's apply, before the pod. The Shard's retries must follow the
// claim: each sends one apply, a quotaRetryFloor or more after the last.
func TestQuotaRetryFollowsTheSharedBackupClaim_WholeReconcile(t *testing.T) {
	f := newShardUnderQuota(t, "quota-retry", true)
	ctx, c, r := t.Context(), f.c, f.r
	f.quota(t, "pods", corev1.ResourcePods, "0", "0")
	require.ErrorContains(t, f.reconcileUntilOverQuota(t), "failed to create pod")

	// The shared backup claim goes missing. Envtest has no controller that
	// completes a claim's deletion, and no pod uses this one, so the test
	// removes the finalizers itself.
	backup, err := BuildSharedBackupPVC(f.shard, true, r.Scheme)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(backup), backup))
	backup.Finalizers = nil
	require.NoError(t, c.Update(ctx, backup))
	require.NoError(t, c.Delete(ctx, backup))
	require.Error(t, c.Get(ctx, client.ObjectKeyFromObject(backup),
		&corev1.PersistentVolumeClaim{}))
	claims := f.quota(t, "claims", corev1.ResourcePersistentVolumeClaims, "0", "1")
	require.Len(t, r.enqueueQuotaWaiters(ctx, claims), 1)
	require.ErrorContains(t, f.reconcileUntilOverQuota(t), "failed to apply shared backup PVC")

	requireSpacedRetries(t, r, f.request.NamespacedName, &f.now,
		func() int { return f.sent }, func() {
			_, err := r.Reconcile(ctx, f.request)
			require.ErrorContains(t, err, "failed to apply shared backup PVC")
		})
}

// A pod that a ResourceQuota keeps out is named in the Shard's status, with
// the quota and the resource, since the events that report it are rate
// limited per object and may never reach the API server. The text does not
// change while the wait goes on, also when the quota's usage figures and
// with them the API server's answer do, so the wait costs no status write
// per pass. It is gone on the pass after the pod is created.
func TestQuotaWaitIsNamedInShardStatus(t *testing.T) {
	f := newShardUnderQuota(t, "quota-status", true)
	ctx, c, r := t.Context(), f.c, f.r
	available := func() (string, multigresv1alpha1.ShardStatus) {
		shard := &multigresv1alpha1.Shard{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f.shard), shard))
		cond := meta.FindStatusCondition(shard.Status.Conditions, "Available")
		require.NotNil(t, cond)
		return cond.Message, shard.Status
	}
	f.quota(t, "team-pods", corev1.ResourcePods, "5", "5")
	require.ErrorContains(t, f.reconcileUntilOverQuota(t), "failed to create pod")
	turnedDownAt := f.now

	// Status is written at the start of a pass, so the next one reports it.
	f.now = f.now.Add(time.Second)
	_, err := r.Reconcile(ctx, f.request)
	require.ErrorContains(t, err, "exceeded quota: team-pods")
	podName := BuildPoolPodName(f.shard, "default", "zone1", 0)
	want := "0/1 pods ready; pod " + podName +
		" waiting on ResourceQuota team-pods (pods) since " +
		turnedDownAt.UTC().Format(time.RFC3339)
	message, status := available()
	require.Equal(t, want, message)

	// More passes, with creates held back and sent, and other usage.
	f.quota(t, "team-pods", corev1.ResourcePods, "5", "7")
	sent := f.sent
	for _, step := range []time.Duration{0, 1, 2, 4, 8} {
		f.now = f.now.Add(step * time.Second)
		_, err := r.Reconcile(ctx, f.request)
		require.Error(t, err)
	}
	require.Greater(t, f.sent, sent+1, "the passes sent no further creates")
	message, sameStatus := available()
	require.Equal(t, want, message)
	require.Equal(t, status, sameStatus, "the waiting passes changed the Shard's status")

	// Room is made; the pod is created, and the next pass says so.
	quota := f.quota(t, "team-pods", corev1.ResourcePods, "10", "5")
	require.Len(t, r.enqueueQuotaWaiters(ctx, quota), 1)
	require.Eventually(t, func() bool {
		_, _ = r.Reconcile(ctx, f.request)
		return c.Get(ctx, client.ObjectKey{Namespace: "default", Name: podName},
			&corev1.Pod{}) == nil
	}, 10*time.Second, 100*time.Millisecond)
	_, _ = r.Reconcile(ctx, f.request)
	message, _ = available()
	require.Equal(t, "0/1 pods ready", message)
}

// Two pools of a shard are each short of their pod under a pod quota with no
// room. Every whole Reconcile ends at the first pool's create, and which
// pool is first follows Go's map order. Sixty retries, each when a create is
// due, must leave one text in the Shard's stored Available condition: the
// pod named is the first one turned down, not the last.
func TestQuotaWaitInStatusDoesNotFollowPoolOrder_WholeReconcile(t *testing.T) {
	f := newShardUnderQuota(t, "quota-pools", false, "other")
	ctx := t.Context()
	f.quota(t, "team-pods", corev1.ResourcePods, "0", "0")
	f.reconcileUntilOverQuota(t)

	texts := map[string]int{}
	sent := f.sent
	for range 60 {
		f.now = f.now.Add(quotaRetryCap + time.Second)
		_, err := f.r.Reconcile(ctx, f.request)
		require.True(t, quotaExceeded(err), "%v", err)
		shard := &multigresv1alpha1.Shard{}
		require.NoError(t, f.c.Get(ctx, client.ObjectKeyFromObject(f.shard), shard))
		cond := meta.FindStatusCondition(shard.Status.Conditions, "Available")
		require.NotNil(t, cond)
		require.Contains(t, cond.Message, "waiting on ResourceQuota team-pods (pods)")
		texts[cond.Message]++
	}
	require.GreaterOrEqual(t, f.sent, sent+60, "the retries did not each send a create")
	require.Len(t, texts, 1, "the text changed while the wait did not: %v", texts)
}

// Two pools are each short of their pod under a full pod quota, and status
// names the first one turned down. Something other than the operator then
// creates that pod, so no create of the operator's succeeds and the wait
// goes on for the other pool's pod. Status must name that one instead: the
// status update lists the shard's pods and sees that the pod it names
// exists. Whole Reconciles, real quota admission.
func TestQuotaWaitInStatusLeavesAPodSomethingElseCreated(t *testing.T) {
	f := newShardUnderQuota(t, "quota-recreated", false, "other")
	ctx := t.Context()
	f.quota(t, "team-pods", corev1.ResourcePods, "0", "0")
	f.reconcileUntilOverQuota(t)
	available := func() string {
		shard := &multigresv1alpha1.Shard{}
		require.NoError(t, f.c.Get(ctx, client.ObjectKeyFromObject(f.shard), shard))
		cond := meta.FindStatusCondition(shard.Status.Conditions, "Available")
		require.NotNil(t, cond)
		return cond.Message
	}
	retry := func() {
		f.now = f.now.Add(quotaRetryCap + time.Second)
		_, err := f.r.Reconcile(ctx, f.request)
		require.True(t, quotaExceeded(err), "%v", err)
	}
	const waiting = " waiting on ResourceQuota team-pods (pods) since 2026-01-01T00:00:00Z"
	pods := map[string]string{}
	for pool := range f.shard.Spec.Pools {
		pods[BuildPoolPodName(f.shard, string(pool), "zone1", 0)] = string(pool)
	}
	retry()
	var named, other string
	for name := range pods {
		if available() == "0/2 pods ready; pod "+name+waiting {
			named = name
		} else {
			other = name
		}
	}
	require.NotEmpty(t, named, available())

	// Room for one pod, which is created from outside and uses it up.
	f.quota(t, "team-pods", corev1.ResourcePods, "1", "0")
	pool := f.shard.Spec.Pools[multigresv1alpha1.PoolName(pods[named])]
	pod, err := BuildPoolPod(f.shard, pods[named], "zone1", pool, 0, f.r.Scheme)
	require.NoError(t, err)
	require.NoError(t, f.c.Create(ctx, pod))
	f.quota(t, "team-pods", corev1.ResourcePods, "1", "1")

	// The first retry turns the other pool's pod down, and the status of
	// the one after it has it.
	retry()
	sent := f.sent
	for range 6 {
		retry()
		require.Equal(t, "0/2 pods ready; pod "+other+waiting, available())
	}
	require.GreaterOrEqual(t, f.sent, sent+6, "the retries did not each send a create")
}
