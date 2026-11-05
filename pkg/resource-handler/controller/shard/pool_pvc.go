package shard

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
)

const (
	// DefaultDataVolumeSize is the minimal viable backup/data size
	DefaultDataVolumeSize = "1Gi"
)

// BuildPoolDataPVCName constructs the PVC name for a specific pod index.
func BuildPoolDataPVCName(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	index int,
) string {
	return poolDataPVCNamePrefix(shard, poolName, cellName) + strconv.Itoa(index)
}

// poolDataPVCNamePrefix is every BuildPoolDataPVCName result for one pool and
// cell, up to and excluding the pod index.
func poolDataPVCNamePrefix(
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	baseName := nameutil.JoinWithConstraints(
		nameutil.PodConstraints,
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		"pool",
		poolName,
		cellName,
	)
	return "data-" + baseName + "-"
}

// isShardBuiltPVC reports whether pvc carries a name the shard's builders
// produce or once produced, for the pool and cell on its labels: BuildPoolDataPVCName at some
// index for a PVC with a pool label, and for one without, the shared backup
// PVC's name or legacyCellBackupPVCName.
//
// Labels say which shard a PVC claims to belong to, not who made it, so
// anything acting on a PVC selected by the shard's identity labels alone can
// reach a stranger's volume. The name is the strongest provenance available:
// it is what the shard's pods mount, so a PVC with one of these names is the
// shard's storage whoever created it, including one retained from a deleted
// Shard of the same identity, or one a scale up is reclaiming. Pool and cell
// come from the PVC's labels rather than the spec, so PVCs of pools and cells
// since removed from the spec still qualify.
func isShardBuiltPVC(shard *multigresv1alpha1.Shard, pvc *corev1.PersistentVolumeClaim) bool {
	poolName := pvc.Labels[metadata.LabelMultigresPool]
	cellName := pvc.Labels[metadata.LabelMultigresCell]
	if poolName == "" {
		if pvc.Name == BuildSharedBackupPVCName(shard) {
			return true
		}
		return cellName != "" && pvc.Name == legacyCellBackupPVCName(shard, cellName)
	}
	if cellName == "" {
		return false
	}
	suffix, ok := strings.CutPrefix(pvc.Name, poolDataPVCNamePrefix(shard, poolName, cellName))
	if !ok {
		return false
	}
	index, err := strconv.Atoi(suffix)
	return err == nil && index >= 0 && strconv.Itoa(index) == suffix
}

// BuildPoolDataPVC creates a PersistentVolumeClaim for a pool pod's data volume.
// When deleteOnShardRemoval is true, a controller ownerRef is set so that
// Kubernetes GC cascade-deletes the PVC when the Shard is removed.
func BuildPoolDataPVC(
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
	index int,
	deleteOnShardRemoval bool,
	scheme *runtime.Scheme,
) (*corev1.PersistentVolumeClaim, error) {
	pvcName := BuildPoolDataPVCName(shard, poolName, cellName, index)
	labels := buildPoolLabelsWithCell(shard, poolName, cellName)
	clusterName := shard.Labels["multigres.com/cluster"]
	metadata.AddClusterLabel(labels, clusterName)

	var storageClass *string
	storageSize := DefaultDataVolumeSize
	accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}

	if poolSpec.Storage.Class != "" {
		storageClass = &poolSpec.Storage.Class
	}
	if poolSpec.Storage.Size != "" {
		storageSize = poolSpec.Storage.Size
	}
	if len(poolSpec.Storage.AccessModes) > 0 {
		accessModes = poolSpec.Storage.AccessModes
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: shard.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(storageSize),
				},
			},
		},
	}

	if storageClass != nil {
		pvc.Spec.StorageClassName = storageClass
	}

	if deleteOnShardRemoval {
		if err := ctrl.SetControllerReference(shard, pvc, scheme); err != nil {
			return nil, fmt.Errorf("failed to set controller reference on PVC %s: %w", pvcName, err)
		}
	}

	return pvc, nil
}

// BuildSharedBackupPVCName builds the deterministic name for the shard-level shared backup PVC.
func BuildSharedBackupPVCName(shard *multigresv1alpha1.Shard) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	return nameutil.JoinWithConstraints(
		nameutil.ServiceConstraints, // Using service constraints since PVC names follow similar rules
		"backup-data",
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
	)
}

// legacyCellBackupPVCName is the per-cell backup PVC name of a shard whose
// filesystem backup repository is cell-scoped rather than shared. Nothing
// creates or mounts one now, but shards created with a cell-scoped repository
// still carry them, with a cell label and no pool label, and their ownerRef
// follows PVCDeletionPolicy like any other PVC the shard made.
func legacyCellBackupPVCName(shard *multigresv1alpha1.Shard, cellName string) string {
	clusterName := shard.Labels["multigres.com/cluster"]
	return nameutil.JoinWithConstraints(
		nameutil.ServiceConstraints,
		"backup-data",
		clusterName,
		string(shard.Spec.DatabaseName),
		string(shard.Spec.TableGroupName),
		string(shard.Spec.ShardName),
		cellName,
	)
}

// BuildSharedBackupPVC creates a PersistentVolumeClaim
// shared by all pooler pods in the shard.
// When deleteOnShardRemoval is true, a controller ownerRef is set so that
// Kubernetes GC cascade-deletes the PVC when the Shard is removed.
func BuildSharedBackupPVC(
	shard *multigresv1alpha1.Shard,
	deleteOnShardRemoval bool,
	scheme *runtime.Scheme,
) (*corev1.PersistentVolumeClaim, error) {
	pvcName := BuildSharedBackupPVCName(shard)

	clusterName := shard.Labels["multigres.com/cluster"]
	labels := metadata.BuildStandardLabels(clusterName, PoolComponentName)
	metadata.AddClusterLabel(labels, clusterName)
	metadata.AddDatabaseLabel(labels, shard.Spec.DatabaseName)
	metadata.AddTableGroupLabel(labels, shard.Spec.TableGroupName)
	metadata.AddShardLabel(labels, shard.Spec.ShardName)

	storageSize := "10Gi"
	var pvcClass *string
	accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}

	if shard.Spec.Backup != nil &&
		shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeFilesystem &&
		shard.Spec.Backup.Filesystem != nil {

		if shard.Spec.Backup.Filesystem.Storage.Size != "" {
			storageSize = shard.Spec.Backup.Filesystem.Storage.Size
		}
		if shard.Spec.Backup.Filesystem.Storage.Class != "" {
			pvcClass = &shard.Spec.Backup.Filesystem.Storage.Class
		}
		if len(shard.Spec.Backup.Filesystem.Storage.AccessModes) > 0 {
			accessModes = shard.Spec.Backup.Filesystem.Storage.AccessModes
		}
	}

	qty, err := resource.ParseQuantity(storageSize)
	if err != nil {
		return nil, fmt.Errorf("invalid storage size '%s': %w", storageSize, err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: shard.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: qty,
				},
			},
		},
	}

	if pvcClass != nil && *pvcClass != "" {
		pvc.Spec.StorageClassName = pvcClass
	}

	if deleteOnShardRemoval {
		if err := ctrl.SetControllerReference(shard, pvc, scheme); err != nil {
			return nil, fmt.Errorf("failed to set controller reference on PVC %s: %w", pvcName, err)
		}
	}

	return pvc, nil
}
