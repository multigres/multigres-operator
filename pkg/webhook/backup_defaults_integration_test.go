//go:build integration

package webhook_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

func TestWebhook_BackupPrefixAllocation(t *testing.T) {
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		name := "backup-prefix-ha"
		if mode != "" {
			name = "backup-prefix-lite"
		}
		t.Run(name, func(t *testing.T) {
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Mode:  mode,
					Cells: []multigresv1alpha1.CellConfig{{Name: "zone-a", ZoneID: "us-east-1a"}},
					Backup: &multigresv1alpha1.BackupConfig{
						Type: multigresv1alpha1.BackupTypeS3,
						S3: &multigresv1alpha1.S3BackupConfig{
							Bucket: "shared",
							Region: "us-east-1",
						},
					},
				},
			}
			if mode == multigresv1alpha1.ModeLite {
				cluster.Spec.Cells[0].Spec = &multigresv1alpha1.CellInlineSpec{
					Multigateway: multigresv1alpha1.MultigatewaySpec{
						StatelessSpec: multigresv1alpha1.StatelessSpec{Replicas: ptr.To(int32(0))},
					},
				}
			}
			setTestPostgresPasswordSecretRef(cluster)
			require.NoError(t, k8sClient.Create(t.Context(), cluster))
			require.Empty(t, cluster.Spec.Backup.S3.KeyPrefix)
			before := cluster.DeepCopy()
			want := "clusters/" + string(cluster.UID)
			if mode == multigresv1alpha1.ModeLite {
				want = "clusters/previous-uid"
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "data-" + name + "-global-topo-0",
						Namespace: cluster.Namespace,
						Labels:    map[string]string{"multigres.com/cluster": name},
						Annotations: map[string]string{
							multigresv1alpha1.AnnotationDefaultBackupKeyPrefix: want,
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						}},
					},
				}
				require.NoError(t, k8sClient.Create(t.Context(), pvc))
			}
			if cluster.Annotations == nil {
				cluster.Annotations = make(map[string]string)
			}
			cluster.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix] = want
			require.ErrorContains(
				t,
				k8sClient.Patch(t.Context(), cluster, client.MergeFrom(before)),
				"cleanup finalizer",
			)
			cluster.Finalizers = []string{multigresv1alpha1.FinalizerClusterCleanup}
			// Exercise admission for the atomic patch the controller sends.
			require.NoError(
				t,
				k8sClient.Patch(
					t.Context(),
					cluster,
					client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}),
				),
			)
			require.Equal(t, want, cluster.Spec.Backup.S3.KeyPrefix)
			before = cluster.DeepCopy()
			delete(cluster.Annotations, multigresv1alpha1.AnnotationDefaultBackupKeyPrefix)
			require.ErrorContains(
				t,
				k8sClient.Patch(t.Context(), cluster, client.MergeFrom(before)),
				"immutable",
			)
		})
	}
}
