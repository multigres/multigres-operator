package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDefaultBackupKeyPrefixes(t *testing.T) {
	s3 := func(prefix string) *BackupConfig {
		return &BackupConfig{
			Type: BackupTypeS3,
			S3:   &S3BackupConfig{Bucket: "shared", Region: "us-east-1", KeyPrefix: prefix},
		}
	}
	for _, mode := range []string{"", ModeLite} {
		for _, tc := range []struct {
			name    string
			configs [4]*BackupConfig
			want    string
		}{
			{"cluster", [4]*BackupConfig{s3("")}, "clusters/uid"},
			{"database", [4]*BackupConfig{nil, s3("")}, "clusters/uid"},
			{"tablegroup", [4]*BackupConfig{nil, nil, s3("")}, "clusters/uid"},
			{"shard", [4]*BackupConfig{nil, nil, nil, s3("")}, "clusters/uid"},
			{"explicit inherited", [4]*BackupConfig{s3("/original/"), nil, nil, s3("")}, "/original/"},
			{"explicit child", [4]*BackupConfig{s3(""), nil, nil, s3("child")}, "child"},
			{"slash-only override", [4]*BackupConfig{s3("original"), nil, nil, s3("//")}, "clusters/uid"},
			{"partial inherited config", [4]*BackupConfig{s3(""), {S3: &S3BackupConfig{Bucket: "other"}}}, "clusters/uid"},
			{"backend override", [4]*BackupConfig{{Type: BackupTypeFilesystem}, nil, nil, s3("")}, "clusters/uid"},
		} {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				cluster := &MultigresCluster{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							AnnotationDefaultBackupKeyPrefix: "clusters/uid",
						},
					},
					Spec: MultigresClusterSpec{
						Mode:   mode,
						Backup: tc.configs[0],
						Databases: []DatabaseConfig{{
							Backup: tc.configs[1], TableGroups: []TableGroupConfig{
								{
									Backup: tc.configs[2],
									Shards: []ShardConfig{{Backup: tc.configs[3]}},
								},
							},
						}},
					},
				}
				DefaultBackupKeyPrefixes(cluster)
				db := cluster.Spec.Databases[0]
				tg := db.TableGroups[0]
				effective := MergeBackupConfig(
					tg.Shards[0].Backup,
					MergeBackupConfig(tg.Backup, MergeBackupConfig(db.Backup, cluster.Spec.Backup)),
				)
				require.Equal(t, tc.want, effective.S3.KeyPrefix)
				before := cluster.DeepCopy()
				DefaultBackupKeyPrefixes(cluster)
				require.Equal(t, before, cluster, "defaulting must be idempotent")
			})
		}
	}
	t.Run("legacy paths and filesystem configs are unchanged", func(t *testing.T) {
		for _, backup := range []*BackupConfig{nil, s3(""), s3("//"), s3("existing"), {Type: BackupTypeFilesystem}} {
			cluster := &MultigresCluster{Spec: MultigresClusterSpec{Backup: backup}}
			before := cluster.DeepCopy()
			DefaultBackupKeyPrefixes(cluster)
			require.Equal(t, before, cluster)
		}
	})
}
