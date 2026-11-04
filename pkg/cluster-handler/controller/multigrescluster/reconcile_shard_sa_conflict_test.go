package multigrescluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// accountConflictHarness reconciles one cluster that names a shard
// ServiceAccount, and counts what the reconciles cost: applies to the
// account, reads of it from the API server, and Warning events.
type accountConflictHarness struct {
	t   *testing.T
	r   *MultigresClusterReconciler
	c   client.Client
	rec *record.FakeRecorder
	key client.ObjectKey
	now time.Time

	// applyErr is what an apply to the account answers; nil lets it through.
	applyErr error
	applies  int
	liveGets int
}

// newAccountConflictHarness builds the harness. cached are the objects the
// informer holds besides the fixtures, live the ones only the API server has.
func newAccountConflictHarness(
	t *testing.T,
	cached, live []client.Object,
) *accountConflictHarness {
	t.Helper()
	scheme := setupScheme()
	coreTpl, cellTpl, shardTpl, cluster, _, _ := setupFixtures(t)
	cluster.Spec.ShardServiceAccount = &multigresv1alpha1.ShardServiceAccountSpec{
		Name: shardSAName,
	}
	h := &accountConflictHarness{
		t:   t,
		rec: record.NewFakeRecorder(1000),
		key: client.ObjectKeyFromObject(cluster),
		now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	h.c = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(append([]client.Object{coreTpl, cellTpl, shardTpl, cluster}, cached...)...).
		WithStatusSubresource(
			&multigresv1alpha1.MultigresCluster{},
			&multigresv1alpha1.Cell{},
			&multigresv1alpha1.TableGroup{},
		).
		WithIndex(&multigresv1alpha1.MultigresCluster{},
			ShardServiceAccountNameIndex, ShardServiceAccountNames).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(
				ctx context.Context, c client.WithWatch, obj client.Object,
				patch client.Patch, opts ...client.PatchOption,
			) error {
				if _, ok := obj.(*corev1.ServiceAccount); ok {
					h.applies++
					if h.applyErr != nil {
						return h.applyErr
					}
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	apiServer := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(live...).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(
				ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption,
			) error {
				if _, ok := obj.(*corev1.ServiceAccount); ok {
					h.liveGets++
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	h.r = &MultigresClusterReconciler{
		Client: h.c, APIReader: apiServer, Scheme: scheme, Recorder: h.rec,
		Now: func() time.Time { return h.now },
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			_, factory := memorytopo.NewServerAndFactory(t.Context())
			return topoclient.NewWithFactory(
				factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
			), nil
		},
	}
	return h
}

func (h *accountConflictHarness) reconcile() (ctrl.Result, error) {
	h.t.Helper()
	return h.r.Reconcile(h.t.Context(), ctrl.Request{NamespacedName: h.key})
}

func (h *accountConflictHarness) condition() *metav1.Condition {
	h.t.Helper()
	got := &multigresv1alpha1.MultigresCluster{}
	require.NoError(h.t, h.c.Get(h.t.Context(), h.key, got))
	return meta.FindStatusCondition(
		got.Status.Conditions, multigresv1alpha1.ConditionShardServiceAccountReady)
}

// warnings drains the recorder and returns the Warning events about the
// account.
func (h *accountConflictHarness) warnings() []string {
	var got []string
	for len(h.rec.Events) > 0 {
		if e := <-h.rec.Events; strings.Contains(e, "ShardServiceAccount") {
			got = append(got, e)
		}
	}
	return got
}

func (h *accountConflictHarness) tableGroups() []multigresv1alpha1.TableGroup {
	h.t.Helper()
	list := &multigresv1alpha1.TableGroupList{}
	require.NoError(h.t, h.c.List(h.t.Context(), list))
	return list.Items
}

var errOneController = errors.New("only one reference can have Controller set to true")

// A cluster whose shard ServiceAccount another object controls is reconciled
// on every status change of its children for as long as the conflict stands.
// When the informer does not hold the account (it lacks the managed-by
// label), those reconciles must not each apply to it, read it from the API
// server and submit a Warning: one apply and one read find the conflict, one
// Warning reports it with the TableGroups it keeps from being created, and
// the next apply waits for the recheck time.
func TestShardSA_StableConflictOnAnAccountOutsideTheInformer(t *testing.T) {
	h := newAccountConflictHarness(t, nil, []client.Object{preOwnedSA()})
	h.applyErr = errOneController

	first, err := h.reconcile()
	require.NoError(t, err, "a conflict nothing can retry away is not a work queue error")
	limit := time.Duration(float64(accountConflictRecheckInterval) * 1.2)
	require.Positive(t, first.RequeueAfter)
	require.LessOrEqual(t, first.RequeueAfter, limit)
	cond := h.condition()
	require.NotNil(t, cond)
	require.Equal(t, multigresv1alpha1.ReasonShardServiceAccountConflict, cond.Reason)
	require.Contains(t, cond.Message, `Deployment "other"`)
	require.Contains(t, cond.Message, "TableGroup ")
	require.Empty(t, h.tableGroups())
	events := h.warnings()
	require.Len(t, events, 1, "%v", events)
	require.Contains(t, events[0], "ShardServiceAccountConflict")
	require.Contains(t, events[0], "TableGroup ")
	require.Equal(t, 1, h.applies)
	require.Equal(t, 1, h.liveGets)

	for range 5 {
		h.now = h.now.Add(time.Second)
		result, err := h.reconcile()
		require.NoError(t, err)
		require.Positive(t, result.RequeueAfter)
		require.LessOrEqual(t, result.RequeueAfter, limit)
	}
	assert.Equal(t, 1, h.applies, "an unchanged conflict was applied again")
	assert.Equal(t, 1, h.liveGets, "an unchanged conflict was read from the API server again")
	assert.Empty(t, h.warnings(), "an unchanged conflict was reported again")
	assert.Equal(t, cond, h.condition())

	// At the recheck time the account is applied once more, and a conflict
	// that still stands is not reported again.
	h.now = h.now.Add(limit)
	_, err = h.reconcile()
	require.NoError(t, err)
	assert.Equal(t, 2, h.applies)
	assert.Equal(t, 2, h.liveGets)
	assert.Empty(t, h.warnings())
	assert.Equal(t, cond, h.condition())

	// An edit to what the cluster applies does not wait for the recheck.
	got := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, h.c.Get(t.Context(), h.key, got))
	got.Spec.ShardServiceAccount.Annotations = map[string]string{irsaKey: "arn:aws:iam::1:role/x"}
	require.NoError(t, h.c.Update(t.Context(), got))
	_, err = h.reconcile()
	require.NoError(t, err)
	assert.Equal(t, 3, h.applies, "an edit to the account's annotations waited for the recheck")

	// The other object lets go of the account: the next recheck applies it,
	// and the TableGroup is created with the account's name.
	h.applyErr = nil
	h.now = h.now.Add(limit)
	result, err := h.reconcile()
	require.NoError(t, err)
	assert.Equal(t, 4, h.applies)
	assert.Equal(t, metav1.ConditionTrue, h.condition().Status)
	require.Len(t, h.tableGroups(), 1)
	assert.Equal(t, shardSAName, h.tableGroups()[0].Spec.ShardServiceAccountName)
	assert.Empty(t, h.warnings())
	assert.LessOrEqual(t, result.RequeueAfter, time.Minute,
		"the recheck requeue outlived the conflict")
}

// The informer holds an account that carries the managed-by label, so a
// conflict on one is decided from the cache: no apply, no read from the API
// server, one Warning, and no requeue, because the account's next event
// reaches the waiting cluster through enqueueAccountWaiters.
func TestShardSA_StableConflictOnAnAccountInTheInformer(t *testing.T) {
	foreign := preOwnedSA()
	foreign.Labels = map[string]string{metadata.LabelAppManagedBy: metadata.ManagedByMultigres}
	h := newAccountConflictHarness(t, []client.Object{foreign}, nil)
	h.applyErr = errOneController

	// The account can be released before the cluster's status says it
	// waits: while the first pass over the conflict is running, or before
	// the informer has that pass's status write. The account's event must
	// reach the cluster then too, because it is the only one there will be.
	require.Nil(t, h.condition())
	require.Equal(t, []reconcile.Request{{NamespacedName: h.key}},
		h.r.enqueueAccountWaiters(t.Context(), foreign))

	for range 5 {
		result, err := h.reconcile()
		require.NoError(t, err)
		require.Zero(t, result.RequeueAfter, "a watched account needs no timed recheck")
	}
	assert.Zero(t, h.applies)
	assert.Zero(t, h.liveGets)
	events := h.warnings()
	require.Len(t, events, 1, "%v", events)
	require.Contains(t, events[0], `Deployment "other"`)
	require.Equal(t, multigresv1alpha1.ReasonShardServiceAccountConflict, h.condition().Reason)

	// An event of the account wakes the cluster that waits for it, and only
	// that one.
	requests := h.r.enqueueAccountWaiters(t.Context(), foreign)
	require.Len(t, requests, 1)
	require.Equal(t, h.key, requests[0].NamespacedName)
	other := foreign.DeepCopy()
	other.Name = "another-account"
	require.Empty(t, h.r.enqueueAccountWaiters(t.Context(), other))

	// Released, the account is applied on that reconcile.
	h.applyErr = nil
	require.NoError(t, h.c.Get(t.Context(), client.ObjectKeyFromObject(foreign), foreign))
	foreign.OwnerReferences = nil
	require.NoError(t, h.c.Update(t.Context(), foreign))
	_, err := h.reconcile()
	require.NoError(t, err)
	assert.Equal(t, 1, h.applies)
	assert.Equal(t, metav1.ConditionTrue, h.condition().Status)
	// The applied account's own events reach its cluster as well. That pass
	// decides from the cached account that nothing is to be applied
	// (shardServiceAccountApplied); the fake client keeps no managedFields,
	// so that part is not shown here.
	require.Len(t, h.r.enqueueAccountWaiters(t.Context(), foreign), 1)
}

// An apply that fails for a reason nobody controls the account for stays an
// error, so the work queue retries it with backoff, and every retry applies
// again. It is still reported once.
func TestShardSA_TransientApplyErrorIsRetried(t *testing.T) {
	h := newAccountConflictHarness(t, nil, nil)
	h.applyErr = errors.New("etcdserver: request timed out")

	for range 3 {
		_, err := h.reconcile()
		require.ErrorContains(t, err, "request timed out")
		require.NotErrorIs(t, err, errShardServiceAccountControlled)
	}
	assert.Equal(t, 3, h.applies)
	events := h.warnings()
	require.Len(t, events, 1, "%v", events)
	require.Contains(t, events[0], "ShardServiceAccountApplyFailed")
	require.Equal(t, multigresv1alpha1.ReasonShardServiceAccountApplyFailed, h.condition().Reason)

	h.applyErr = nil
	_, err := h.reconcile()
	require.NoError(t, err)
	assert.Equal(t, metav1.ConditionTrue, h.condition().Status)
}
