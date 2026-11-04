//go:build integration

package multigrescluster_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// This harness runs the real cluster controller without admission webhooks.
func TestBackupPrefixesWithoutWebhook(t *testing.T) {
	c, _ := setupIntegration(t)
	prefixes := make(map[string]bool)
	for _, mode := range []string{"", multigresv1alpha1.ModeLite} {
		for _, legacy := range []bool{false, true} {
			name := "prefix-ha"
			if mode != "" {
				name = "prefix-lite"
			}
			if legacy {
				name += "-existing"
			}
			t.Run(name, func(t *testing.T) {
				cluster := &multigresv1alpha1.MultigresCluster{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
					Spec: multigresv1alpha1.MultigresClusterSpec{
						Mode: mode,
						Cells: []multigresv1alpha1.CellConfig{
							{Name: "zone-a", ZoneID: "us-east-1a"},
						},
						Backup: &multigresv1alpha1.BackupConfig{
							Type: multigresv1alpha1.BackupTypeS3,
							S3: &multigresv1alpha1.S3BackupConfig{
								Bucket:            "shared",
								Region:            "us-east-1",
								CredentialsSecret: "s3-creds",
							},
						},
					},
				}
				if legacy {
					cluster.Finalizers = []string{multigresv1alpha1.FinalizerClusterCleanup}
				}
				setTestPostgresPasswordSecretRef(cluster)
				if !legacy {
					// Reapply this manifest while its previous topology PVC is terminating.
					deleteClusterWithTopologyPVC(t, c, cluster.DeepCopy())
				}
				require.NoError(t, c.Create(t.Context(), cluster))
				want := ""
				if !legacy {
					want = "clusters/" + string(cluster.UID)
					require.False(
						t,
						prefixes[want],
						"independent clusters must get different prefixes",
					)
					prefixes[want] = true
				}
				require.Eventually(t, func() bool {
					stored := &multigresv1alpha1.MultigresCluster{}
					if err := c.Get(
						t.Context(),
						client.ObjectKeyFromObject(cluster),
						stored,
					); err != nil {
						return false
					}
					if stored.Spec.Backup.S3.KeyPrefix != want ||
						stored.Annotations[multigresv1alpha1.AnnotationDefaultBackupKeyPrefix] != want {
						return false
					}
					groups := &multigresv1alpha1.TableGroupList{}
					if err := c.List(
						t.Context(),
						groups,
						client.InNamespace(testNamespace),
						client.MatchingLabels{metadata.LabelMultigresCluster: name},
					); err != nil ||
						len(groups.Items) != 1 {
						return false
					}
					group := &groups.Items[0]
					return group.Spec.Backup != nil && group.Spec.Backup.S3 != nil &&
						group.Spec.Backup.S3.KeyPrefix == want &&
						group.Spec.SidecarBackupLocation != nil &&
						group.Spec.SidecarBackupLocation.S3 != nil &&
						group.Spec.SidecarBackupLocation.S3.KeyPrefix == want &&
						len(group.Spec.Shards) == 1 &&
						group.Spec.Shards[0].Backup.S3.KeyPrefix == want
				}, testTimeout, 100*time.Millisecond)
			})
		}
	}
}

func deleteClusterWithTopologyPVC(
	t *testing.T,
	c client.Client,
	cluster *multigresv1alpha1.MultigresCluster,
) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, c.Create(ctx, cluster))
	key := client.ObjectKeyFromObject(cluster)
	require.Eventually(t, func() bool {
		var stored multigresv1alpha1.MultigresCluster
		return c.Get(ctx, key, &stored) == nil &&
			stored.Spec.Backup.S3.KeyPrefix == "clusters/"+string(cluster.UID)
	}, testTimeout, 100*time.Millisecond)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-" + cluster.Name + "-global-topo-0", Namespace: cluster.Namespace,
			Labels: map[string]string{
				metadata.LabelMultigresCluster: cluster.Name,
				metadata.LabelAppComponent:     metadata.ComponentTopoServer,
			},
			Finalizers: []string{"test.multigres.com/hold"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}},
		},
	}
	require.NoError(t, c.Create(ctx, pvc))
	require.Eventually(t, func() bool {
		return c.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{}) == nil
	}, testTimeout, 100*time.Millisecond)
	require.NoError(t, c.Delete(ctx, cluster))
	require.Eventually(t, func() bool {
		return apierrors.IsNotFound(c.Get(ctx, key, &multigresv1alpha1.MultigresCluster{}))
	}, testTimeout, 100*time.Millisecond)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc))
	require.False(t, pvc.DeletionTimestamp.IsZero())
}
