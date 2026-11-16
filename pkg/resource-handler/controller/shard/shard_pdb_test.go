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
			if got := shardMinAvailable(shard); got != tc.want {
				t.Fatalf("shardMinAvailable() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestBuildShardPodDisruptionBudgetsDoNotOverlap(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Shard scheme: %v", err)
	}

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
			Images: multigresv1alpha1.ShardImages{
				PgBackRestd: "registry/multigres@sha256:abc",
			},
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
	if err != nil {
		t.Fatalf("build PDBs: %v", err)
	}
	if len(pdbs) != 1 {
		t.Fatalf("PDB count = %d, want one shard-wide budget", len(pdbs))
	}
	if got := pdbs[0].Spec.MinAvailable.IntValue(); got != 3 {
		t.Errorf("shard minAvailable = %d, want 3", got)
	}
	selector := pdbs[0].Spec.Selector.MatchLabels
	if _, scopedToCell := selector[metadata.LabelMultigresCell]; scopedToCell {
		t.Errorf("shard PDB must not select a cell: %#v", selector)
	}
	if _, scopedToPool := selector[metadata.LabelMultigresPool]; scopedToPool {
		t.Errorf("shard PDB must not select a pool: %#v", selector)
	}
}

func TestReconcileShardPDBReplacesLegacyPoolCellPDBs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Shard scheme: %v", err)
	}
	if err := policyv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add policy scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add Pod scheme: %v", err)
	}

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
			Images: multigresv1alpha1.ShardImages{
				PgBackRestd: "registry/multigres@sha256:abc",
			},
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
		},
	}

	desired, err := BuildShardPodDisruptionBudget(shard, scheme)
	if err != nil {
		t.Fatalf("build desired PDB: %v", err)
	}

	legacy := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "legacy-pool-cell-pdb",
			Namespace: shard.Namespace,
			Labels:    maps.Clone(desired.Labels),
		},
	}
	legacy.Labels[metadata.LabelMultigresPool] = "primary"
	legacy.Labels[metadata.LabelMultigresCell] = "zone1"
	if err := ctrl.SetControllerReference(shard, legacy, scheme); err != nil {
		t.Fatalf("set legacy owner reference: %v", err)
	}

	unmanaged := legacy.DeepCopy()
	unmanaged.Name = "unmanaged-pdb"
	unmanaged.OwnerReferences = nil

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shard, legacy, unmanaged).
		Build()
	r := &ShardReconciler{Client: c, Scheme: scheme}

	if err := r.reconcileShardPDB(t.Context(), shard); err != nil {
		t.Fatalf("reconcile shard PDB: %v", err)
	}

	if err := c.Get(
		t.Context(),
		client.ObjectKeyFromObject(desired),
		&policyv1.PodDisruptionBudget{},
	); err != nil {
		t.Errorf("shard-wide PDB should exist: %v", err)
	}
	if err := c.Get(
		t.Context(),
		client.ObjectKeyFromObject(legacy),
		&policyv1.PodDisruptionBudget{},
	); !apierrors.IsNotFound(err) {
		t.Errorf("legacy PDB should be deleted, got: %v", err)
	}
	if err := c.Get(
		t.Context(),
		client.ObjectKeyFromObject(unmanaged),
		&policyv1.PodDisruptionBudget{},
	); err != nil {
		t.Errorf("unmanaged PDB should be preserved: %v", err)
	}
}

func TestReconcileShardPDBCountsMaintenanceSurge(t *testing.T) {
	t.Parallel()
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

	if err := r.reconcileShardPDB(t.Context(), shard); err != nil {
		t.Fatalf("reconcile shard PDB: %v", err)
	}
	desired, err := BuildShardPodDisruptionBudget(shard, scheme)
	if err != nil {
		t.Fatalf("build shard PDB: %v", err)
	}
	actual := &policyv1.PodDisruptionBudget{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(desired), actual); err != nil {
		t.Fatalf("get shard PDB: %v", err)
	}
	if got := actual.Spec.MinAvailable.IntValue(); got != 3 {
		t.Fatalf("minAvailable with one surge = %d, want 3", got)
	}
}
