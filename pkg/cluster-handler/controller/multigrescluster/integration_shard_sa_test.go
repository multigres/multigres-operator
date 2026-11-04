//go:build integration
// +build integration

package multigrescluster_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/multigrescluster"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// Adopting a platform-created ServiceAccount goes through the real apiserver's
// server-side apply: other annotations survive, ours win, the MultigresCluster
// becomes the controller owner, and a later reconcile sends no request for the
// account at all: an apply that changes nothing still costs the API server an
// authenticated, admitted request on every reconcile of every cluster. An
// annotation dropped from the spec afterwards is still removed.
//
// Neither envtest nor the fake client runs the garbage collector (a testkit
// gap: no GC sim), so this does not exercise deletion of a Shard; the owner
// shape asserted here is what protects the SA from it.
func TestShardSA_AdoptsExistingThroughApiserver(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "shard-sa-adopt"
		saName      = "shard-sa-adopt-irsa"
		irsaKey     = "eks.amazonaws.com/role-arn"
	)

	var reconciles, applies atomic.Int64
	k8sClient, _, reader := setupIntegrationCore(t,
		func(inner reconcile.Reconciler) reconcile.Reconciler {
			return reconcile.Func(
				func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
					if req.Name == clusterName {
						defer reconciles.Add(1)
					}
					return inner.Reconcile(ctx, req)
				},
			)
		},
		func(r *multigrescluster.MultigresClusterReconciler) {
			r.Client = serviceAccountPatchCounter{Client: r.Client, name: saName, n: &applies}
		})

	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: testNamespace,
			Annotations: map[string]string{
				irsaKey:              "arn:aws:iam::1:role/platform",
				"platform.example/x": "keep",
			},
		},
		AutomountServiceAccountToken: ptr.To(true),
	}
	require.NoError(t, k8sClient.Create(t.Context(), existing))

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default",
				CellTemplate:  "default",
				ShardTemplate: "default",
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			ShardServiceAccount: &multigresv1alpha1.ShardServiceAccountSpec{
				Name:        saName,
				Annotations: map[string]string{irsaKey: "arn:aws:iam::1:role/ours"},
			},
		},
	}
	setTestPostgresPasswordSecretRef(cluster)
	require.NoError(t, k8sClient.Create(t.Context(), cluster))

	key := types.NamespacedName{Name: saName, Namespace: testNamespace}
	sa := &corev1.ServiceAccount{}
	require.Eventually(t, func() bool {
		if err := reader.Get(t.Context(), key, sa); err != nil {
			return false
		}
		return len(sa.OwnerReferences) == 1
	}, 30*time.Second, 100*time.Millisecond, "SA was not adopted")

	assert.Equal(t, map[string]string{
		irsaKey:              "arn:aws:iam::1:role/ours",
		"platform.example/x": "keep",
	}, sa.Annotations)
	assert.Equal(t, ptr.To(false), sa.AutomountServiceAccountToken)
	ref := sa.OwnerReferences[0]
	assert.Equal(t, "MultigresCluster", ref.Kind)
	assert.Equal(t, clusterName, ref.Name)
	assert.Equal(t, cluster.UID, ref.UID)
	assert.Equal(t, ptr.To(true), ref.Controller)

	// The controller decides from its informer, so wait until the informer
	// holds the adopted account before counting.
	require.Eventually(t, func() bool {
		cached := &corev1.ServiceAccount{}
		return k8sClient.Get(t.Context(), key, cached) == nil &&
			cached.ResourceVersion == sa.ResourceVersion
	}, 30*time.Second, 100*time.Millisecond, "informer never saw the adopted SA")

	// Force reconciles of the cluster with unrelated spec changes and wait for
	// each to finish; the SA must be neither rewritten nor applied again.
	forceReconcile := func(mutate func(*multigresv1alpha1.MultigresClusterSpec)) {
		t.Helper()
		before := reconciles.Load()
		require.Eventually(t, func() bool {
			latest := &multigresv1alpha1.MultigresCluster{}
			if err := reader.Get(
				t.Context(), client.ObjectKeyFromObject(cluster), latest,
			); err != nil {
				return false
			}
			mutate(&latest.Spec)
			return k8sClient.Update(t.Context(), latest) == nil
		}, 10*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool { return reconciles.Load() > before },
			30*time.Second, 100*time.Millisecond, "no further reconcile ran")
	}
	rv := sa.ResourceVersion
	require.Positive(t, applies.Load(), "the adoption itself is an apply")
	forceReconcile(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.Images.ImagePullPolicy = corev1.PullAlways
	})
	settled := applies.Load()
	forceReconcile(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.Images.ImagePullPolicy = corev1.PullIfNotPresent
	})
	forceReconcile(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.Images.ImagePullPolicy = corev1.PullAlways
	})
	assert.Equal(t, settled, applies.Load(),
		"an unchanged ServiceAccount was applied again")

	after := &corev1.ServiceAccount{}
	require.NoError(t, reader.Get(t.Context(), key, after))
	assert.Equal(t, rv, after.ResourceVersion, "a later reconcile rewrote the ServiceAccount")

	// Not applying an unchanged account must not also skip the apply that
	// removes an annotation the spec no longer sets.
	forceReconcile(func(spec *multigresv1alpha1.MultigresClusterSpec) {
		spec.ShardServiceAccount.Annotations = nil
	})
	require.Eventually(t, func() bool {
		if err := reader.Get(t.Context(), key, after); err != nil {
			return false
		}
		_, present := after.Annotations[irsaKey]
		return !present
	}, 30*time.Second, 100*time.Millisecond, "the dropped annotation was not removed")
	assert.Equal(t, map[string]string{"platform.example/x": "keep"}, after.Annotations)
}

// serviceAccountPatchCounter counts the patches the controller sends for one
// ServiceAccount.
type serviceAccountPatchCounter struct {
	client.Client
	name string
	n    *atomic.Int64
}

func (c serviceAccountPatchCounter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	if _, ok := obj.(*corev1.ServiceAccount); ok && obj.GetName() == c.name {
		c.n.Add(1)
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

// An account that another controller already owns is rejected by the real
// apiserver. That must surface as a condition and event, and must not stop the
// cluster's cells from being created.
func TestShardSA_ConflictingControllerDoesNotHaltCells(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "shard-sa-conflict"
		saName      = "shard-sa-conflict-irsa"
	)

	k8sClient, _, reader := setupIntegrationCore(t, nil, nil)

	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: testNamespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "other-owner",
				UID:        "11111111-2222-3333-4444-555555555555",
				Controller: ptr.To(true),
			}},
		},
	}
	require.NoError(t, k8sClient.Create(t.Context(), existing))

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			TemplateDefaults: multigresv1alpha1.TemplateDefaults{
				CoreTemplate:  "default",
				CellTemplate:  "default",
				ShardTemplate: "default",
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
			ShardServiceAccount: &multigresv1alpha1.ShardServiceAccountSpec{
				Name: saName,
			},
		},
	}
	setTestPostgresPasswordSecretRef(cluster)
	require.NoError(t, k8sClient.Create(t.Context(), cluster))

	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.MultigresCluster{}
		if err := reader.Get(t.Context(), client.ObjectKeyFromObject(cluster), got); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(
			got.Status.Conditions,
			multigresv1alpha1.ConditionShardServiceAccountReady,
		)
		return cond != nil &&
			cond.Status == metav1.ConditionFalse &&
			cond.Reason == multigresv1alpha1.ReasonShardServiceAccountConflict &&
			strings.Contains(cond.Message, `Deployment "other-owner"`)
	}, 30*time.Second, 100*time.Millisecond, "conflict condition was not set")

	require.Eventually(t, func() bool {
		cells := &multigresv1alpha1.CellList{}
		if err := reader.List(
			t.Context(), cells, client.InNamespace(testNamespace),
			client.MatchingLabels{metadata.LabelMultigresCluster: clusterName},
		); err != nil {
			return false
		}
		return len(cells.Items) > 0
	}, 30*time.Second, 100*time.Millisecond, "cells were not created despite the conflict")

	sa := &corev1.ServiceAccount{}
	require.NoError(t, reader.Get(t.Context(),
		types.NamespacedName{Name: saName, Namespace: testNamespace}, sa))
	assert.Len(t, sa.OwnerReferences, 1, "the other controller's ownership must be untouched")
}

// Two clusters name the same shard ServiceAccount. The account stays the
// first cluster's, with its controller reference and its role annotation,
// the second cluster reports the conflict naming the first, no TableGroup of
// the second carries the account's name, and the account is not written
// again: each cluster applies under its own field manager, so the second
// apply would add a second controller reference, which the API server
// refuses, where one shared manager would replace the first cluster's
// reference and the two would rewrite the account at each other.
func TestShardSA_TwoClustersNamingOneAccount(t *testing.T) {
	t.Parallel()
	twoClustersNamingOneAccount(t, nil)
}

// accountsUnseen is a client whose reads never find a ServiceAccount, as a
// cache that has not caught up does not.
type accountsUnseen struct{ client.Client }

func (c accountsUnseen) Get(
	ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption,
) error {
	if _, isAccount := obj.(*corev1.ServiceAccount); isAccount {
		return apierrors.NewNotFound(corev1.Resource("serviceaccounts"), key.Name)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// The same two clusters when neither sees the account before it applies,
// which is what two first applies racing each other look like. The API
// server has to decide it: each cluster applies under a field manager of its
// own, so the second apply adds a second controller reference and is
// refused. Under one shared manager it would replace the first cluster's
// reference and role, and the two would take the account from each other on
// every pass.
func TestShardSA_TwoClustersNamingOneAccountUnseen(t *testing.T) {
	t.Parallel()
	twoClustersNamingOneAccount(t, func(r *multigrescluster.MultigresClusterReconciler) {
		// Naming the other owner reads the account live, as in production.
		r.APIReader = r.Client
		r.Client = accountsUnseen{r.Client}
	})
}

func twoClustersNamingOneAccount(
	t *testing.T,
	mutate func(*multigrescluster.MultigresClusterReconciler),
) {
	const (
		saName = "shard-sa-shared"
		role   = "eks.amazonaws.com/role-arn"
	)
	k8sClient, _, reader := setupIntegrationCore(t, nil, mutate)
	ctx := t.Context()
	newCluster := func(name, arn string) *multigresv1alpha1.MultigresCluster {
		cluster := &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				TemplateDefaults: multigresv1alpha1.TemplateDefaults{
					CoreTemplate: "default", CellTemplate: "default", ShardTemplate: "default",
				},
				Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "use1-az1"}},
				ShardServiceAccount: &multigresv1alpha1.ShardServiceAccountSpec{
					Name: saName, Annotations: map[string]string{role: arn},
				},
			},
		}
		setTestPostgresPasswordSecretRef(cluster)
		require.NoError(t, k8sClient.Create(ctx, cluster))
		return cluster
	}
	condition := func(cluster *multigresv1alpha1.MultigresCluster) *metav1.Condition {
		got := &multigresv1alpha1.MultigresCluster{}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(cluster), got); err != nil {
			return nil
		}
		return meta.FindStatusCondition(
			got.Status.Conditions, multigresv1alpha1.ConditionShardServiceAccountReady)
	}
	account := func() *corev1.ServiceAccount {
		sa := &corev1.ServiceAccount{}
		require.NoError(t, reader.Get(ctx,
			types.NamespacedName{Name: saName, Namespace: testNamespace}, sa))
		return sa
	}

	first := newCluster("shard-sa-first", "arn:aws:iam::1:role/first")
	require.Eventually(t, func() bool {
		cond := condition(first)
		return cond != nil && cond.Status == metav1.ConditionTrue
	}, 30*time.Second, 100*time.Millisecond, "the first cluster's account was not applied")

	second := newCluster("shard-sa-second", "arn:aws:iam::2:role/second")
	require.Eventually(t, func() bool {
		cond := condition(second)
		return cond != nil && cond.Status == metav1.ConditionFalse &&
			cond.Reason == multigresv1alpha1.ReasonShardServiceAccountConflict &&
			strings.Contains(cond.Message, `MultigresCluster "shard-sa-first"`)
	}, 30*time.Second, 100*time.Millisecond, "the second cluster did not report the conflict")

	settled := account()
	require.Never(t, func() bool {
		return account().ResourceVersion != settled.ResourceVersion
	}, 3*time.Second, 100*time.Millisecond, "the account is still being rewritten")
	sa := account()
	require.Len(t, sa.OwnerReferences, 1)
	require.Equal(t, first.Name, sa.OwnerReferences[0].Name)
	require.Equal(t, "arn:aws:iam::1:role/first", sa.Annotations[role])
	require.Equal(t, metav1.ConditionTrue, condition(first).Status)

	tgs := &multigresv1alpha1.TableGroupList{}
	require.NoError(t, reader.List(ctx, tgs, client.InNamespace(testNamespace),
		client.MatchingLabels{metadata.LabelMultigresCluster: second.Name}))
	for _, tg := range tgs.Items {
		require.NotEqual(t, saName, tg.Spec.ShardServiceAccountName,
			"TableGroup %s of the second cluster carries the first cluster's account", tg.Name)
	}
}
