package storage

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// BuildPVCTemplate creates a PersistentVolumeClaim for StatefulSet's volumeClaimTemplates.
// Builds from simple storage class name and size parameters.
//
// Parameters:
//   - name: Name for the volume claim (e.g., "data")
//   - storageClassName: Optional storage class name (nil uses cluster default)
//   - storageSize: Size of the volume (e.g., "10Gi")
//   - accessModes: List of access modes (e.g., [ReadWriteOnce]). Defaults to ReadWriteOnce if empty.
func BuildPVCTemplate(
	name string,
	storageClassName *string,
	storageSize string,
	accessModes []corev1.PersistentVolumeAccessMode,
) corev1.PersistentVolumeClaim {
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{
			corev1.ReadWriteOnce,
		}
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(storageSize),
				},
			},
			// The API server defaults the volume mode and the phase on a
			// stored template. A StatefulSet's templates are an atomic list,
			// compared whole, so the template is built as stored and an
			// apply that changes nothing is recognised as one; see
			// ownership.SkipNoopApplies.
			VolumeMode: ptr.To(corev1.PersistentVolumeFilesystem),
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}

	if storageClassName != nil {
		pvc.Spec.StorageClassName = storageClassName
	}

	return pvc
}
