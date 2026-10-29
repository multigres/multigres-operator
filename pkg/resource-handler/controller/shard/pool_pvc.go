package shard

import (
	"fmt"

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
	return fmt.Sprintf("data-%s-%d", baseName, index)
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

	if backup := sidecarLocation(shard); backup.Type == multigresv1alpha1.BackupTypeFilesystem &&
		backup.Filesystem != nil {
		if backup.Filesystem.Storage.Size != "" {
			storageSize = backup.Filesystem.Storage.Size
		}
		if backup.Filesystem.Storage.Class != "" {
			pvcClass = &backup.Filesystem.Storage.Class
		}
		if len(backup.Filesystem.Storage.AccessModes) > 0 {
			accessModes = backup.Filesystem.Storage.AccessModes
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
