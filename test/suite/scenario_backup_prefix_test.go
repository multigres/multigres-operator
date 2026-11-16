package suite

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestBackupPrefixSurvivesClusterRecreation(t *testing.T) {
	for _, tc := range []struct{ name, mode string }{{"ha", ""}, {"lite", api.ModeLite}} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCase(t)
			cluster := c.newCluster("restore", func(spec *MultigresClusterSpec) {
				spec.Mode = tc.mode
				spec.PVCDeletionPolicy.WhenDeleted = api.RetainPVCRetentionPolicy
				spec.Backup = &api.BackupConfig{Type: api.BackupTypeS3, S3: &api.S3BackupConfig{
					Bucket: "shared", Region: "us-east-1", UseEnvCredentials: true,
				}}
			})
			manifest := &MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: cluster.Name, Namespace: c.NS},
				Spec:       *cluster.Spec.DeepCopy(),
			}
			c.Eq("", manifest.Spec.Backup.S3.KeyPrefix)
			want := "clusters/" + string(cluster.UID)
			var data corev1.PersistentVolumeClaim
			c.Eventually(
				30*time.Second,
				"PostgreSQL data PVC to record the original allocation",
				func() error {
					var claims corev1.PersistentVolumeClaimList
					if err := c.List(&claims); err != nil {
						return err
					}
					for _, claim := range claims.Items {
						if claim.Labels[metadata.LabelMultigresPool] != "" &&
							claim.Annotations[api.AnnotationDefaultBackupKeyPrefix] == want {
							data = claim
							return nil
						}
					}
					return fmt.Errorf("no pool PVC records %s", want)
				},
			)
			key := client.ObjectKeyFromObject(cluster)
			c.NoError(c.Delete(cluster))
			c.Eventually(30*time.Second, "original cluster to finish deletion", func() error {
				if err := c.Get(key, &MultigresCluster{}); apierrors.IsNotFound(err) {
					return nil
				}
				return fmt.Errorf("cluster %s still exists", key)
			})
			c.NoError(c.Create(manifest))
			c.Eventually(
				30*time.Second,
				"replacement shard to mount the retained data with its original prefix",
				func() error {
					var shards ShardList
					if err := c.List(&shards); err != nil {
						return err
					}
					if len(shards.Items) != 1 || shards.Items[0].Spec.Backup == nil ||
						shards.Items[0].Spec.Backup.S3 == nil ||
						shards.Items[0].Spec.Backup.S3.KeyPrefix != want {
						return fmt.Errorf("waiting for replacement shard to use %s", want)
					}
					var pods corev1.PodList
					if err := c.List(&pods); err != nil {
						return err
					}
					for _, pod := range pods.Items {
						if !metav1.IsControlledBy(&pod, &shards.Items[0]) ||
							!pod.DeletionTimestamp.IsZero() {
							continue
						}
						for _, vol := range pod.Spec.Volumes {
							if vol.PersistentVolumeClaim != nil &&
								vol.PersistentVolumeClaim.ClaimName == data.Name {
								return nil
							}
						}
					}
					return fmt.Errorf("no replacement pod mounts %s", data.Name)
				},
			)
			var stored MultigresCluster
			c.NoError(c.Get(key, &stored))
			c.Eq(want, stored.Spec.Backup.S3.KeyPrefix)
			c.Eq(want, stored.Annotations[api.AnnotationDefaultBackupKeyPrefix])
			var claim corev1.PersistentVolumeClaim
			c.NoError(c.Get(client.ObjectKeyFromObject(&data), &claim))
			c.Eq(data.UID, claim.UID)
		})
	}
}
