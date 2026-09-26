package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestBuildPoolDataPVC_BasicStructure(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	pool := newTestPoolSpec()

	pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Eq("default", pvc.Namespace, "namespace")

	c.Require().
		Empty(pvc.OwnerReferences, "expected 0 owner references with deleteOnShardRemoval=false, got %d", len(pvc.OwnerReferences))

	expectedLabels := map[string]string{
		"app.kubernetes.io/component": PoolComponentName,
		"multigres.com/cluster":       "test-cluster",
		"multigres.com/cell":          "z1",
		"multigres.com/pool":          "main",
		"multigres.com/shard":         "0-inf",
	}
	for k, want := range expectedLabels {
		got := pvc.Labels[k]
		c.Eq(want, got, "label %q = %q, want", k, got)
	}
}

func TestBuildPoolDataPVC_StorageDefaults(t *testing.T) {
	c := assert.NewCollecting(t)
	pool := multigresv1alpha1.PoolSpec{
		Storage: multigresv1alpha1.StorageSpec{},
	}

	pvc, err := BuildPoolDataPVC(newTestShard(), "main", "z1", pool, 0, false, testScheme())
	c.Require().NoError(err, "unexpected error")

	// Default size
	got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	want := resource.MustParse(DefaultDataVolumeSize)
	c.Eq(0, got.Cmp(want), "storage size = %s, want %s", got.String(), want.String())

	// Default access mode
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Errorf("access modes = %v, want [ReadWriteOnce]", pvc.Spec.AccessModes)
	}

	// No storage class by default
	if pvc.Spec.StorageClassName != nil {
		t.Errorf("storage class = %q, want nil", *pvc.Spec.StorageClassName)
	}
}

func TestBuildPoolDataPVC_CustomStorage(t *testing.T) {
	c := assert.NewCollecting(t)
	pool := multigresv1alpha1.PoolSpec{
		Storage: multigresv1alpha1.StorageSpec{
			Class:       "fast-ssd",
			Size:        "50Gi",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		},
	}

	pvc, err := BuildPoolDataPVC(newTestShard(), "main", "z1", pool, 0, false, testScheme())
	c.Require().NoError(err, "unexpected error")

	// Custom size
	got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	want := resource.MustParse("50Gi")
	c.Eq(0, got.Cmp(want), "storage size = %s, want %s", got.String(), want.String())

	// Custom access mode
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteMany {
		t.Errorf("access modes = %v, want [ReadWriteMany]", pvc.Spec.AccessModes)
	}

	// Custom storage class
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast-ssd" {
		t.Errorf("storage class = %v, want %q", pvc.Spec.StorageClassName, "fast-ssd")
	}
}

func TestBuildPoolDataPVC_NameConsistency(t *testing.T) {
	shard := newTestShard()
	pool := newTestPoolSpec()

	pvc, _ := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, testScheme())
	pod, _ := BuildPoolPod(shard, "main", "z1", pool, 0, testScheme())

	// Find the data volume in the pod
	var dataPVCRef string
	for _, v := range pod.Spec.Volumes {
		if v.Name == DataVolumeName && v.PersistentVolumeClaim != nil {
			dataPVCRef = v.PersistentVolumeClaim.ClaimName
		}
	}

	assert.NewCollecting(t).Eq(pvc.Name, dataPVCRef, "pod references PVC")
}

func TestBuildPoolDataPVCName_MatchesPodReference(t *testing.T) {
	tests := []struct {
		name     string
		shard    *multigresv1alpha1.Shard
		poolName string
		cellName string
		index    int
	}{
		{"index 0", newTestShard(), "main", "z1", 0},
		{"index 5", newTestShard(), "main", "z1", 5},
		{
			"long names",
			func() *multigresv1alpha1.Shard {
				s := newTestShard()
				s.Labels["multigres.com/cluster"] = "long-cluster"
				return s
			}(),
			"replica-pool", "us-east-1a", 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvcName := BuildPoolDataPVCName(tt.shard, tt.poolName, tt.cellName, tt.index)
			pod, _ := BuildPoolPod(
				tt.shard,
				tt.poolName,
				tt.cellName,
				newTestPoolSpec(),
				tt.index,
				testScheme(),
			)

			var podPVCRef string
			for _, v := range pod.Spec.Volumes {
				if v.Name == DataVolumeName && v.PersistentVolumeClaim != nil {
					podPVCRef = v.PersistentVolumeClaim.ClaimName
				}
			}

			assert.NewCollecting(t).Eq(podPVCRef, pvcName, "BuildPoolDataPVCName()")
		})
	}
}

func TestBuildPoolDataPVC_OwnerRefWithDeletePolicy(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	pool := newTestPoolSpec()

	pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, true, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Require().
		Len(pvc.OwnerReferences, 1, "expected 1 owner reference with deleteOnShardRemoval=true, got %d", len(pvc.OwnerReferences))

	ref := pvc.OwnerReferences[0]
	c.Eq(shard.Name, ref.Name, "ownerRef name")
	c.Eq(shard.UID, ref.UID, "ownerRef UID")
	c.False(ref.Controller == nil || !*ref.Controller, "ownerRef Controller should be true")
}

func TestBuildPoolDataPVC_NoOwnerRefWithRetainPolicy(t *testing.T) {
	c := assert.NewAborting(t)
	shard := newTestShard()
	pool := newTestPoolSpec()

	pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, testScheme())
	c.NoError(err, "unexpected error")

	c.Empty(
		pvc.OwnerReferences,
		"expected 0 owner references with deleteOnShardRemoval=false, got %d",
		len(pvc.OwnerReferences),
	)
}

func TestBuildSharedBackupPVC_OwnerRefWithDeletePolicy(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := newTestShard()
	shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Storage: multigresv1alpha1.StorageSpec{Size: "5Gi"},
		},
	}

	pvc, err := BuildSharedBackupPVC(shard, true, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Require().
		Len(pvc.OwnerReferences, 1, "expected 1 owner reference with deleteOnShardRemoval=true, got %d", len(pvc.OwnerReferences))

	ref := pvc.OwnerReferences[0]
	c.Eq(shard.Name, ref.Name, "ownerRef name")
}

func TestBuildSharedBackupPVC_NoOwnerRefWithRetainPolicy(t *testing.T) {
	c := assert.NewAborting(t)
	shard := newTestShard()
	shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Storage: multigresv1alpha1.StorageSpec{Size: "5Gi"},
		},
	}

	pvc, err := BuildSharedBackupPVC(shard, false, testScheme())
	c.NoError(err, "unexpected error")

	c.Empty(
		pvc.OwnerReferences,
		"expected 0 owner references with deleteOnShardRemoval=false, got %d",
		len(pvc.OwnerReferences),
	)
}

func TestBuildShardPodDisruptionBudget(t *testing.T) {
	c := assert.NewCollecting(t)
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       "test-uid",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
			ShardName:      "0-inf",
			Replicas:       ptr.To(int32(3)),
		},
	}

	pdb, err := BuildShardPodDisruptionBudget(shard, testScheme())
	c.Require().NoError(err, "unexpected error")

	c.Eq("default", pdb.Namespace, "namespace")

	// Three desired members preserve two available members.
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntValue() != 2 {
		t.Errorf("minAvailable = %v, want 2", pdb.Spec.MinAvailable)
	}
	c.Nil(pdb.Spec.MaxUnavailable, "maxUnavailable")

	// Selector should match all pool pods in the shard, not one pool or cell.
	c.Require().NotNil(pdb.Spec.Selector, "selector is nil")
	sel := pdb.Spec.Selector.MatchLabels
	if _, ok := sel["multigres.com/cell"]; ok {
		t.Errorf("selector must not be scoped to a cell: %#v", sel)
	}
	_, ok := sel["multigres.com/pool"]
	c.False(ok, "selector must not be scoped to a pool: %#v", sel)
	c.Eq("0-inf", sel["multigres.com/shard"], "selector shard")
	c.Eq(PoolComponentName, sel["app.kubernetes.io/component"], "selector component")

	// Owner reference
	c.Require().
		Len(pdb.OwnerReferences, 1, "expected 1 owner reference, got %d", len(pdb.OwnerReferences))
}
