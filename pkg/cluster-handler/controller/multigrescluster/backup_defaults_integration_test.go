//go:build integration

package multigrescluster_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
								Bucket: "shared",
								Region: "us-east-1",
							},
						},
					},
				}
				if legacy {
					cluster.Finalizers = []string{multigresv1alpha1.FinalizerClusterCleanup}
				}
				setTestPostgresPasswordSecretRef(cluster)
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
						len(group.Spec.Shards) == 1 &&
						group.Spec.Shards[0].Backup.S3.KeyPrefix == want
				}, testTimeout, 100*time.Millisecond)
			})
		}
	}
}
