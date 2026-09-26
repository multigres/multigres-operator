package storage

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/multigres/testkit/assert"
)

func TestBuildPVCTemplate(t *testing.T) {
	storageClassStandard := "standard"
	storageClassFast := "fast-ssd"

	tests := map[string]struct {
		name             string
		storageClassName *string
		storageSize      string
		accessModes      []corev1.PersistentVolumeAccessMode
		want             corev1.PersistentVolumeClaim
	}{
		"basic case with storage class and size": {
			name:             "data",
			storageClassName: &storageClassStandard,
			storageSize:      "10Gi",
			want: corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "data",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &storageClassStandard,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
				},
			},
		},
		"nil storage class - uses cluster default": {
			name:             "data",
			storageClassName: nil,
			storageSize:      "20Gi",
			want: corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "data",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("20Gi"),
						},
					},
				},
			},
		},
		"different storage class": {
			name:             "postgres-data",
			storageClassName: &storageClassFast,
			storageSize:      "100Gi",
			want: corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "postgres-data",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &storageClassFast,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("100Gi"),
						},
					},
				},
			},
		},
		"small storage size": {
			name:             "data",
			storageClassName: nil,
			storageSize:      "1Gi",
			want: corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "data",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
		"large storage size": {
			name:             "data",
			storageClassName: &storageClassStandard,
			storageSize:      "1Ti",
			want: corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "data",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					StorageClassName: &storageClassStandard,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Ti"),
						},
					},
				},
			},
		},
		"custom access modes": {
			name:             "backup-data",
			storageClassName: &storageClassStandard,
			storageSize:      "50Gi",
			accessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteMany,
			},
			want: corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "backup-data",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteMany,
					},
					StorageClassName: &storageClassStandard,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("50Gi"),
						},
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := BuildPVCTemplate(tc.name, tc.storageClassName, tc.storageSize, tc.accessModes)
			assert.NewCollecting(t).EqDiff(tc.want, got, "BuildPVCTemplate() mismatch")
		})
	}
}
