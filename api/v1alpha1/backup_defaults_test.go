package v1alpha1

import (
	"strings"
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

// An S3 endpoint is pgBackRest's own host[:port], which it connects to with
// TLS, or an https URL. An explicit scheme other than https is refused, and
// so is anything that is neither form. The refusal never quotes the
// endpoint: each one here carries a marker where a credential can be.
func TestS3EndpointProblem(t *testing.T) {
	for _, ok := range []string{
		"", "minio", "minio:9000", "minio.storage.svc.cluster.local:9000",
		"10.0.0.7:9000", "[fd00::7]:9000", "s3.eu-west-1.amazonaws.com",
		"https://minio:9000", "HTTPS://minio.storage.svc", "https://minio:9000/",
	} {
		if got := S3EndpointProblem(ok); got != "" {
			t.Errorf("S3EndpointProblem(%q) = %q, want none", ok, got)
		}
	}
	const marker = "hunter2-MARKER"
	//nolint:gosec // Made-up endpoints: what must not be quoted has to be in the input.
	for _, bad := range []string{
		"http://minio:9000",
		"http://key:" + marker + "@minio:9000",
		"//key:" + marker + "@minio:9000",
		"key:" + marker + "@minio:9000",
		"s3://" + marker,
		"https://",
		"https://?sig=" + marker,
		"minio:9000/path?" + marker,
		"minio:9000#" + marker,
		"ftp://minio/" + marker,
		"http://%zz" + marker,
		" minio:9000",
		"minio:port",
	} {
		got := S3EndpointProblem(bad)
		if got == "" {
			t.Errorf("S3EndpointProblem(%q) accepted it", bad)
		}
		if strings.Contains(got, marker) || strings.Contains(got, "minio") {
			t.Errorf("S3EndpointProblem(%q) quotes the endpoint: %q", bad, got)
		}
	}
}

// An endpoint shown in a condition, an event or a log has no user
// information, no query and no fragment, and says where something was
// removed.
func TestRedactEndpoint(t *testing.T) {
	//nolint:gosec // Made-up endpoints: what is redacted has to be in the input.
	for in, want := range map[string]string{
		"":                                   "",
		"https://s3.example.com":             "https://s3.example.com",
		"https://s3.example.com/path":        "https://s3.example.com/path",
		"https://key:secret@s3.example.com":  "https://<redacted>@s3.example.com",
		"https://s3.example.com/p?token=abc": "https://s3.example.com/p?<redacted>",
		"https://u:p@h:9000/p?sig=1#frag":    "https://<redacted>@h:9000/p?<redacted>#<redacted>",
		"key:secret@minio:9000":              "<redacted>@minio:9000",
		"https://s3.example.com/p#frag?notq": "https://s3.example.com/p#<redacted>",
		"https://s3.example.com#token":       "https://s3.example.com#<redacted>",
		// An authority follows "//" also where no scheme precedes it.
		"//key:secret@minio:9000/p":      "//<redacted>@minio:9000/p",
		"//key:secret@minio:9000?sig=ab": "//<redacted>@minio:9000?<redacted>",
	} {
		if got := RedactEndpoint(in); got != want {
			t.Errorf("RedactEndpoint(%q) = %q, want %q", in, got, want)
		}
	}

	// A made-up endpoint: what the test redacts has to be in its input.
	endpoint := "https://key:secret@minio:9000" //nolint:gosec
	location := &BackupConfig{Type: BackupTypeS3, S3: &S3BackupConfig{
		Bucket: "b", Region: "r", Endpoint: endpoint,
	}}
	block := location.DeepCopy()
	block.S3.Endpoint = "https://other:hunter2@minio:9000/?X-Amz-Signature=abc"
	o := UnappliedBackupOverride(block, location)
	if o == nil {
		t.Fatal("two endpoints that differ were read as one")
	}
	for _, shown := range []string{o.Value, o.Applied} {
		for _, secret := range []string{"secret", "hunter2", "abc"} {
			if strings.Contains(shown, secret) {
				t.Errorf("%q shows %q", shown, secret)
			}
		}
	}
}

// A block that names an S3 key prefix other than the location's asks for a
// repository the sidecar does not serve, in the same bucket: the sidecar and
// topology both take the prefix from the location. The prefixes are compared
// as pgBackRest is given them, so "/" and the bucket root are one prefix, and
// a block without a prefix inherits the location's.
func TestUnappliedBackupOverride_KeyPrefix(t *testing.T) {
	s3 := func(prefix string) *BackupConfig {
		return &BackupConfig{Type: BackupTypeS3, S3: &S3BackupConfig{
			Bucket: "b", Region: "r", KeyPrefix: prefix,
		}}
	}
	for name, tc := range map[string]struct {
		block, location string
		want            *BackupOverride
	}{
		"same prefix":                             {"clusters/a", "clusters/a", nil},
		"no prefix in the block":                  {"", "clusters/a", nil},
		"bucket root in both":                     {"/", "/", nil},
		"another prefix":                          {"clusters/b", "clusters/a", &BackupOverride{Field: "s3.keyPrefix", Value: "clusters/b", Applied: "clusters/a"}},
		"bucket root under a prefix":              {"/", "clusters/a", &BackupOverride{Field: "s3.keyPrefix", Value: "/", Applied: "clusters/a"}},
		"a prefix under the bucket root":          {"clusters/b", "/", &BackupOverride{Field: "s3.keyPrefix", Value: "clusters/b", Applied: "/"}},
		"a prefix where the location has none":    {"clusters/b", "", &BackupOverride{Field: "s3.keyPrefix", Value: "clusters/b", Applied: ""}},
		"bucket root where the location has none": {"/", "", nil},
	} {
		got := UnappliedBackupOverride(s3(tc.block), s3(tc.location))
		require.Equal(t, tc.want, got, name)
	}
}

// How the S3 endpoint is verified is inherited by a level that sets neither
// a CA nor verifyTLS. A CA and verifyTLS: false exclude each other, so each
// set below replaces the other set above. verifyTLS: true below says only
// "verify": it turns an inherited false back on, and keeps an inherited CA,
// without which an endpoint behind a private CA would fail verification.
func TestMergeBackupConfig_S3TLS(t *testing.T) {
	yes, no := true, false
	ca := &S3CAReference{Secret: &S3CAKeyReference{Name: "ca"}}
	s3 := func(ca *S3CAReference, verify *bool) *BackupConfig {
		return &BackupConfig{Type: BackupTypeS3, S3: &S3BackupConfig{CA: ca, VerifyTLS: verify}}
	}
	for name, tc := range map[string]struct {
		child, parent, want *BackupConfig
	}{
		"inherited":                   {s3(nil, nil), s3(ca, nil), s3(ca, nil)},
		"verification off inherited":  {s3(nil, nil), s3(nil, &no), s3(nil, &no)},
		"a CA below verification off": {s3(ca, nil), s3(nil, &no), s3(ca, nil)},
		"verification off below a CA": {s3(nil, &no), s3(ca, nil), s3(nil, &no)},
		"verification on below a CA":  {s3(nil, &yes), s3(ca, nil), s3(ca, &yes)},
		"verification on below off":   {s3(nil, &yes), s3(nil, &no), s3(nil, &yes)},
		"another CA below a CA": {
			s3(&S3CAReference{ConfigMap: &S3CAKeyReference{Name: "other"}}, nil), s3(ca, &yes),
			s3(&S3CAReference{ConfigMap: &S3CAKeyReference{Name: "other"}}, nil),
		},
	} {
		got := MergeBackupConfig(tc.child, tc.parent)
		require.False(t, got.S3.CA != nil && got.S3.SkipsTLSVerification(), name)
		require.Equal(t, tc.want.S3.CA, got.S3.CA, name)
		require.Equal(t, tc.want.S3.VerifyTLS, got.S3.VerifyTLS, name)
	}
}

// A tablegroup or shard block of a distributed cluster that names another CA,
// or turns verification off or on, where the database's location does
// otherwise would not be applied (verifyTLS: true where the location has a CA
// asks for nothing else): every sidecar of the database is started
// from the database's location.
func TestUnappliedBackupOverride_S3TLS(t *testing.T) {
	yes, no := true, false
	secret := func(name string) *S3CAReference {
		return &S3CAReference{Secret: &S3CAKeyReference{Name: name}}
	}
	s3 := func(ca *S3CAReference, verify *bool) *BackupConfig {
		return &BackupConfig{Type: BackupTypeS3, S3: &S3BackupConfig{
			Bucket: "b", Region: "r", CA: ca, VerifyTLS: verify,
		}}
	}
	for name, tc := range map[string]struct {
		block, location *BackupConfig
		refused         bool
	}{
		"neither set in the block":                  {s3(nil, nil), s3(secret("ca"), nil), false},
		"the same CA":                               {s3(secret("ca"), nil), s3(secret("ca"), nil), false},
		"the same CA with the default key spelled":  {s3(&S3CAReference{Secret: &S3CAKeyReference{Name: "ca", Key: "ca.crt"}}, nil), s3(secret("ca"), nil), false},
		"verifyTLS true where nothing is set":       {s3(nil, &yes), s3(nil, nil), false},
		"another CA":                                {s3(secret("other"), nil), s3(secret("ca"), nil), true},
		"a ConfigMap of the same name":              {s3(&S3CAReference{ConfigMap: &S3CAKeyReference{Name: "ca"}}, nil), s3(secret("ca"), nil), true},
		"verification off where it is on":           {s3(nil, &no), s3(nil, nil), true},
		"verification on where it is off":           {s3(nil, &yes), s3(nil, &no), true},
		"a CA where verification is off":            {s3(secret("ca"), nil), s3(nil, &no), true},
		"verification on where the location has CA": {s3(nil, &yes), s3(secret("ca"), nil), false},
	} {
		got := UnappliedBackupOverride(tc.block, tc.location)
		if !tc.refused {
			require.Nil(t, got, name)
			continue
		}
		require.NotNil(t, got, name)
		require.Equal(t, "s3.ca/verifyTLS", got.Field, name)
	}
}
