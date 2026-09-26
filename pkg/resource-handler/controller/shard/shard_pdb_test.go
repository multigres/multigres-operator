package shard

import (
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestShardMinAvailable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		replicas int32
		want     int32
	}{
		{name: "one replica still preserves durability floor", replicas: 1, want: 2},
		{name: "two replicas block voluntary disruption", replicas: 2, want: 2},
		{name: "three replicas allow one disruption", replicas: 3, want: 2},
		{name: "four replicas still allow only one disruption", replicas: 4, want: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shard := &multigresv1alpha1.Shard{Spec: multigresv1alpha1.ShardSpec{
				Replicas: ptr.To(tc.replicas),
			}}
			assert.NewAborting(t).Eq(tc.want, shardMinAvailable(shard), "shardMinAvailable()")
		})
	}
}

func TestBuildShardPodDisruptionBudgetsDoNotOverlap(t *testing.T) {
	t.Parallel()
	c := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	c.Require().NoError(multigresv1alpha1.AddToScheme(scheme), "add Shard scheme")

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       types.UID("shard-uid"),
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "postgres",
			TableGroupName:   "default",
			ShardName:        "0-inf",
			DurabilityPolicy: multiCellAtLeast2Policy,
			Replicas:         ptr.To(int32(4)),
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone-b", "zone-a"},
					ReplicasPerCell: ptr.To(int32(2)),
				},
			},
		},
	}

	pdbs, err := BuildShardPodDisruptionBudgets(shard, scheme)
	c.Require().NoError(err, "build PDBs")
	c.Require().Len(pdbs, 1, "PDB count = %d, want one shard-wide budget", len(pdbs))
	c.Eq(3, pdbs[0].Spec.MinAvailable.IntValue(), "shard minAvailable")
	selector := pdbs[0].Spec.Selector.MatchLabels
	_, scopedToCell := selector[metadata.LabelMultigresCell]
	c.False(scopedToCell, "shard PDB must not select a cell: %#v", selector)
	_, scopedToPool := selector[metadata.LabelMultigresPool]
	c.False(scopedToPool, "shard PDB must not select a pool: %#v", selector)
}

func TestReconcileShardPDBReplacesLegacyPoolCellPDBs(t *testing.T) {
	ck := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	ck.Require().NoError(multigresv1alpha1.AddToScheme(scheme), "add Shard scheme")
	ck.Require().NoError(policyv1.AddToScheme(scheme), "add policy scheme")
	ck.Require().NoError(corev1.AddToScheme(scheme), "add Pod scheme")

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       types.UID("shard-uid"),
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}

	desired, err := BuildShardPodDisruptionBudget(shard, scheme)
	ck.Require().NoError(err, "build desired PDB")

	legacy := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-pool-cell-pdb",
			Namespace: shard.Namespace,
			Labels:    maps.Clone(desired.Labels),
		},
	}
	legacy.Labels[metadata.LabelMultigresPool] = "primary"
	legacy.Labels[metadata.LabelMultigresCell] = "zone1"
	ck.Require().
		NoError(ctrl.SetControllerReference(shard, legacy, scheme), "set legacy owner reference")

	unmanaged := legacy.DeepCopy()
	unmanaged.Name = "unmanaged-pdb"
	unmanaged.OwnerReferences = nil

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard, legacy, unmanaged).
		Build()
	r := &ShardReconciler{Client: c, Scheme: scheme}

	ck.Require().NoError(r.reconcileShardPDB(t.Context(), shard), "reconcile shard PDB")

	ck.NoError(c.Get(
		t.Context(),
		client.ObjectKeyFromObject(desired),
		&policyv1.PodDisruptionBudget{},
	), "shard-wide PDB should exist")
	if err := c.Get(
		t.Context(),
		client.ObjectKeyFromObject(legacy),
		&policyv1.PodDisruptionBudget{},
	); !apierrors.IsNotFound(err) {
		t.Errorf("legacy PDB should be deleted, got: %v", err)
	}
	ck.NoError(c.Get(
		t.Context(),
		client.ObjectKeyFromObject(unmanaged),
		&policyv1.PodDisruptionBudget{},
	), "unmanaged PDB should be preserved")
}

func TestReconcileShardPDBCountsMaintenanceSurge(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	scheme := maintenanceSurgeTestScheme(t)
	shard := maintenanceSurgeTestShard()
	shard.Spec.Replicas = ptr.To(int32(3))
	surge := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "maintenance-surge",
			Namespace:   shard.Namespace,
			Labels:      shardPDBLabels(shard),
			Annotations: map[string]string{metadata.AnnotationMaintenanceSurge: "true"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, surge).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme}

	ck.NoError(r.reconcileShardPDB(t.Context(), shard), "reconcile shard PDB")
	desired, err := BuildShardPodDisruptionBudget(shard, scheme)
	ck.NoError(err, "build shard PDB")
	actual := &policyv1.PodDisruptionBudget{}
	ck.NoError(c.Get(t.Context(), client.ObjectKeyFromObject(desired), actual), "get shard PDB")
	ck.Eq(3, actual.Spec.MinAvailable.IntValue(), "minAvailable with one surge")
}
