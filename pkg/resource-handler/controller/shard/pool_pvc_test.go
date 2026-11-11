package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestBuildPoolDataPVC_BasicStructure(t *testing.T) {
	shard := newTestShard()
	pool := newTestPoolSpec()

	pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pvc.Namespace != "default" {
		t.Errorf("namespace = %q, want %q", pvc.Namespace, "default")
	}

	if len(pvc.OwnerReferences) != 0 {
		t.Fatalf(
			"expected 0 owner references with deleteOnShardRemoval=false, got %d",
			len(pvc.OwnerReferences),
		)
	}

	expectedLabels := map[string]string{
		"app.kubernetes.io/component": PoolComponentName,
		"multigres.com/cluster":       "test-cluster",
		"multigres.com/cell":          "z1",
		"multigres.com/pool":          "main",
		"multigres.com/shard":         "0-inf",
	}
	for k, want := range expectedLabels {
		if got := pvc.Labels[k]; got != want {
			t.Errorf("label %q = %q, want %q", k, got, want)
		}
	}
}

func TestBuildPoolDataPVC_StorageDefaults(t *testing.T) {
	pool := multigresv1alpha1.PoolSpec{
		Storage: multigresv1alpha1.StorageSpec{},
	}

	pvc, err := BuildPoolDataPVC(newTestShard(), "main", "z1", pool, 0, false, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Default size
	got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	want := resource.MustParse(DefaultDataVolumeSize)
	if got.Cmp(want) != 0 {
		t.Errorf("storage size = %s, want %s", got.String(), want.String())
	}

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
	pool := multigresv1alpha1.PoolSpec{
		Storage: multigresv1alpha1.StorageSpec{
			Class:       "fast-ssd",
			Size:        "50Gi",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		},
	}

	pvc, err := BuildPoolDataPVC(newTestShard(), "main", "z1", pool, 0, false, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Custom size
	got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	want := resource.MustParse("50Gi")
	if got.Cmp(want) != 0 {
		t.Errorf("storage size = %s, want %s", got.String(), want.String())
	}

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

	if dataPVCRef != pvc.Name {
		t.Errorf("pod references PVC %q but BuildPoolDataPVC created %q", dataPVCRef, pvc.Name)
	}
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

			if pvcName != podPVCRef {
				t.Errorf("BuildPoolDataPVCName() = %q, pod references %q", pvcName, podPVCRef)
			}
		})
	}
}

func TestBuildPoolDataPVC_OwnerRefWithDeletePolicy(t *testing.T) {
	shard := newTestShard()
	pool := newTestPoolSpec()

	pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, true, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pvc.OwnerReferences) != 1 {
		t.Fatalf(
			"expected 1 owner reference with deleteOnShardRemoval=true, got %d",
			len(pvc.OwnerReferences),
		)
	}

	ref := pvc.OwnerReferences[0]
	if ref.Name != shard.Name {
		t.Errorf("ownerRef name = %q, want %q", ref.Name, shard.Name)
	}
	if ref.UID != shard.UID {
		t.Errorf("ownerRef UID = %q, want %q", ref.UID, shard.UID)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Error("ownerRef Controller should be true")
	}
}

func TestBuildPoolDataPVC_NoOwnerRefWithRetainPolicy(t *testing.T) {
	shard := newTestShard()
	pool := newTestPoolSpec()

	pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pvc.OwnerReferences) != 0 {
		t.Fatalf(
			"expected 0 owner references with deleteOnShardRemoval=false, got %d",
			len(pvc.OwnerReferences),
		)
	}
}

func TestBuildSharedBackupPVC_OwnerRefWithDeletePolicy(t *testing.T) {
	shard := newTestShard()
	shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Storage: multigresv1alpha1.StorageSpec{Size: "5Gi"},
		},
	}

	pvc, err := BuildSharedBackupPVC(shard, true, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pvc.OwnerReferences) != 1 {
		t.Fatalf(
			"expected 1 owner reference with deleteOnShardRemoval=true, got %d",
			len(pvc.OwnerReferences),
		)
	}

	ref := pvc.OwnerReferences[0]
	if ref.Name != shard.Name {
		t.Errorf("ownerRef name = %q, want %q", ref.Name, shard.Name)
	}
}

func TestBuildSharedBackupPVC_NoOwnerRefWithRetainPolicy(t *testing.T) {
	shard := newTestShard()
	shard.Spec.Backup = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Storage: multigresv1alpha1.StorageSpec{Size: "5Gi"},
		},
	}

	pvc, err := BuildSharedBackupPVC(shard, false, testScheme())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pvc.OwnerReferences) != 0 {
		t.Fatalf(
			"expected 0 owner references with deleteOnShardRemoval=false, got %d",
			len(pvc.OwnerReferences),
		)
	}
}

func TestBuildShardPodDisruptionBudget(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pdb.Namespace != "default" {
		t.Errorf("namespace = %q, want %q", pdb.Namespace, "default")
	}

	// Three desired members preserve two available members.
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntValue() != 2 {
		t.Errorf("minAvailable = %v, want 2", pdb.Spec.MinAvailable)
	}
	if pdb.Spec.MaxUnavailable != nil {
		t.Errorf("maxUnavailable = %v, want nil", pdb.Spec.MaxUnavailable)
	}

	// Selector should match all pool pods in the shard, not one pool or cell.
	if pdb.Spec.Selector == nil {
		t.Fatal("selector is nil")
	}
	sel := pdb.Spec.Selector.MatchLabels
	if _, ok := sel["multigres.com/cell"]; ok {
		t.Errorf("selector must not be scoped to a cell: %#v", sel)
	}
	if _, ok := sel["multigres.com/pool"]; ok {
		t.Errorf("selector must not be scoped to a pool: %#v", sel)
	}
	if sel["multigres.com/shard"] != "0-inf" {
		t.Errorf("selector shard = %q, want %q", sel["multigres.com/shard"], "0-inf")
	}
	if sel["app.kubernetes.io/component"] != PoolComponentName {
		t.Errorf(
			"selector component = %q, want %q",
			sel["app.kubernetes.io/component"],
			PoolComponentName,
		)
	}

	// Owner reference
	if len(pdb.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(pdb.OwnerReferences))
	}
}

// A shard of a dynamic tablegroup names its volumes with the catalog id, so
// a tablegroup recreated under a dropped one's name never binds its PVCs.
func TestPVCNames_DynamicTableGroupCarriesCatalogID(t *testing.T) {
	plain := newTestShard()
	dynamic := newTestShard()
	dynamic.Annotations = map[string]string{multigresv1alpha1.AnnotationTableGroupID: "7"}
	recreated := newTestShard()
	recreated.Annotations = map[string]string{multigresv1alpha1.AnnotationTableGroupID: "9"}

	for _, tc := range []struct {
		name string
		of   func(*multigresv1alpha1.Shard) string
	}{
		{"data", func(s *multigresv1alpha1.Shard) string { return BuildPoolDataPVCName(s, "main", "z1", 0) }},
		{"backup", BuildSharedBackupPVCName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.of(plain) != tc.of(newTestShard()) {
				t.Error("a spec-declared shard's PVC name changed")
			}
			if tc.of(dynamic) == tc.of(plain) {
				t.Error("a dynamic shard shares its PVC name with a spec-declared one")
			}
			if tc.of(dynamic) == tc.of(recreated) {
				t.Error("two incarnations of a tablegroup share a PVC name")
			}
		})
	}

	// isShardBuiltPVC recognises the id-bearing names as the shard's own.
	pvc, err := BuildPoolDataPVC(dynamic, "main", "z1", newTestPoolSpec(), 0, false, testScheme())
	if err != nil {
		t.Fatal(err)
	}
	if !isShardBuiltPVC(dynamic, pvc) {
		t.Errorf("data PVC %q not recognised as the dynamic shard's", pvc.Name)
	}
	backup, err := BuildSharedBackupPVC(dynamic, false, testScheme())
	if err != nil {
		t.Fatal(err)
	}
	if !isShardBuiltPVC(dynamic, backup) {
		t.Errorf("backup PVC %q not recognised as the dynamic shard's", backup.Name)
	}
}
