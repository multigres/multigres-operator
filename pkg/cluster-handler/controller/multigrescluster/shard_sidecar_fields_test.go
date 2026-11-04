package multigrescluster

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/cluster-handler/controller/tablegroup"
	"github.com/multigres/multigres-operator/pkg/images"
)

// TestShardCarriesSidecarFields runs the production resolution and builders
// and fails if a field the shard controller needs to build the pgBackRest
// sidecar does not reach the Shard. The shard controller refuses a Shard
// without the image, so a field dropped here would stop every pod from rolling.
func TestShardCarriesSidecarFields(t *testing.T) {
	backup := &multigresv1alpha1.BackupConfig{
		Type:       multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{Path: "/backups"},
	}
	defaults := testImagesConfig(images.UpdateImmediate).Defaults

	cases := map[string]struct {
		images    multigresv1alpha1.ClusterImages
		sa        *multigresv1alpha1.ShardServiceAccountSpec
		wantImage multigresv1alpha1.ImageRef
		wantSA    string
	}{
		"default images follow the default multipooler": {
			wantImage: defaults.Multipooler,
		},
		"a pinned multipooler pins the sidecar": {
			images:    multigresv1alpha1.ClusterImages{Multipooler: "pinned/multigres:v1"},
			wantImage: "pinned/multigres:v1",
		},
		"an explicit sidecar image wins": {
			images: multigresv1alpha1.ClusterImages{
				Multipooler: "pinned/multigres:v1",
				PgBackRestd: "custom/multigres:v9",
			},
			wantImage: "custom/multigres:v9",
		},
		"a named shard service account is passed down": {
			sa:        &multigresv1alpha1.ShardServiceAccountSpec{Name: "shard-sa"},
			wantImage: defaults.Multipooler,
			wantSA:    "shard-sa",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "default"},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					Images:              tc.images,
					Backup:              backup,
					ShardServiceAccount: tc.sa,
				},
			}

			scheme := setupScheme()
			r := &MultigresClusterReconciler{
				Client: fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(cluster).
					WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
					Build(),
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
				Images:   testImagesConfig(images.UpdateImmediate),
			}
			if err := r.resolveImages(context.Background(), cluster); err != nil {
				t.Fatal(err)
			}

			if got := cluster.Status.Images.Effective.PgBackRestd; got != tc.wantImage {
				t.Errorf("status.images.effective.pgbackrestd = %q, want %q", got, tc.wantImage)
			}
			if got := cluster.Status.Images.Applied.PgBackRestd; got != "" {
				t.Errorf("status.images.applied.pgbackrestd = %q, want empty", got)
			}

			dbCfg := multigresv1alpha1.DatabaseConfig{Name: "postgres"}
			tgCfg := &multigresv1alpha1.TableGroupConfig{Name: "default", Default: true}
			shards := []multigresv1alpha1.ShardResolvedSpec{{Name: "0-inf"}}
			tg, err := BuildTableGroup(
				cluster, dbCfg, tgCfg, shards, multigresv1alpha1.GlobalTopoServerRef{}, scheme,
			)
			if err != nil {
				t.Fatal(err)
			}
			shard, err := tablegroup.BuildShard(tg, &shards[0], scheme)
			if err != nil {
				t.Fatal(err)
			}

			if tg.Spec.Images.PgBackRestd != tc.wantImage ||
				tg.Spec.SidecarBackupLocation == nil ||
				tg.Spec.ShardServiceAccountName != tc.wantSA {
				t.Errorf("TableGroup sidecar fields: images=%q location=%v sa=%q",
					tg.Spec.Images.PgBackRestd, tg.Spec.SidecarBackupLocation,
					tg.Spec.ShardServiceAccountName)
			}
			if shard.Spec.Images.PgBackRestd != tc.wantImage ||
				shard.Spec.SidecarBackupLocation == nil ||
				shard.Spec.ShardServiceAccountName != tc.wantSA {
				t.Errorf("Shard sidecar fields: images=%q location=%v sa=%q",
					shard.Spec.Images.PgBackRestd, shard.Spec.SidecarBackupLocation,
					shard.Spec.ShardServiceAccountName)
			}
		})
	}
}
