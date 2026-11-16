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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// Adopting a platform-created ServiceAccount goes through the real apiserver's
// server-side apply: other annotations survive, ours win, the MultigresCluster
// becomes the controller owner, and a later reconcile changes nothing.
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

	var reconciles atomic.Int64
	k8sClient, _, reader := setupIntegrationCore(t,
		func(inner reconcile.Reconciler) reconcile.Reconciler {
			return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
				if req.Name == clusterName {
					defer reconciles.Add(1)
				}
				return inner.Reconcile(ctx, req)
			})
		}, nil)

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

	// Force another reconcile of the cluster with an unrelated spec change and
	// wait for it to finish; the SA must not be rewritten.
	rv := sa.ResourceVersion
	before := reconciles.Load()
	require.Eventually(t, func() bool {
		latest := &multigresv1alpha1.MultigresCluster{}
		if err := reader.Get(t.Context(), client.ObjectKeyFromObject(cluster), latest); err != nil {
			return false
		}
		latest.Spec.Images.ImagePullPolicy = corev1.PullAlways
		return k8sClient.Update(t.Context(), latest) == nil
	}, 10*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return reconciles.Load() > before },
		30*time.Second, 100*time.Millisecond, "no further reconcile ran")

	after := &corev1.ServiceAccount{}
	require.NoError(t, reader.Get(t.Context(), key, after))
	assert.Equal(t, rv, after.ResourceVersion, "second reconcile rewrote the ServiceAccount")
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
