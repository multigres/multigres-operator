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
			{"explicit legacy root", [4]*BackupConfig{s3("/"), nil, nil, s3("")}, "/"},
			{"restore shard from root", [4]*BackupConfig{s3("parent"), nil, nil, s3("/")}, "/"},
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

func TestRepositoryKeyPrefix(t *testing.T) {
	for keyPrefix, want := range map[string]string{
		"":                "",
		"/":               "",
		"//":              "//",
		"clusters/uid":    "clusters/uid",
		"prod/cluster-1/": "prod/cluster-1/",
		"/leading":        "/leading",
	} {
		got := (&S3BackupConfig{KeyPrefix: keyPrefix}).RepositoryKeyPrefix()
		require.Equal(t, want, got, "keyPrefix %q", keyPrefix)
	}
}

func TestNormalizeEndpoint(t *testing.T) {
	for in, want := range map[string]string{
		"":                             "",
		"https://s3.example.com":       "https://s3.example.com",
		"https://s3.example.com/":      "https://s3.example.com",
		"HTTPS://S3.Example.COM":       "https://s3.example.com",
		"s3.Example.com:9000/":         "s3.example.com:9000",
		"https://S3.example.com/Path/": "https://s3.example.com/Path/",
		"https://s3.example.com/a?X=Y": "https://s3.example.com/a?X=Y",
		"https://Me@S3.Example":        "https://Me@s3.example",
	} {
		if got := normalizeEndpoint(in); got != want {
			t.Errorf("normalizeEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
	// Endpoints that may be different places stay different.
	for _, pair := range [][2]string{
		{"https://a.example", "https://b.example"},
		{"https://a.example", "http://a.example"},
		{"https://a.example", "https://a.example:443"},
		{"https://a.example/x", "https://a.example/X"},
		{"https://a.example/proxy", "https://a.example/proxy/"},
		{"https://a.example", "https://a.example//"},
		{"https://a.example/?token=x", "https://a.example/?token=x/"},
		{"https://a.example/?Token=x", "https://a.example/?token=x"},
		{"https://Me@a.example", "https://me@a.example"},
		{"https://a.example/#A", "https://a.example/#a"},
	} {
		if normalizeEndpoint(pair[0]) == normalizeEndpoint(pair[1]) {
			t.Errorf("%q and %q were normalized to one endpoint", pair[0], pair[1])
		}
	}
}

// A block or a location built without a type is a filesystem config, which is
// what the CRD's default would have written.
func TestUnappliedBackupOverride_NoTypeIsFilesystem(t *testing.T) {
	filesystem := &BackupConfig{Type: BackupTypeFilesystem}
	s3 := &BackupConfig{Type: BackupTypeS3, S3: &S3BackupConfig{Bucket: "b"}}
	require.Nil(t, UnappliedBackupOverride(&BackupConfig{}, filesystem))
	require.Nil(t, UnappliedBackupOverride(filesystem, &BackupConfig{}))
	require.Nil(t, UnappliedBackupOverride(filesystem, nil))
	got := UnappliedBackupOverride(&BackupConfig{}, s3)
	require.NotNil(t, got)
	require.Equal(t, BackupOverride{Field: "type", Value: "filesystem", Applied: "s3"}, *got)
}
