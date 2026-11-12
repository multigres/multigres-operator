package v1alpha1

import "testing"

func TestUsesDefaultS3Repository(t *testing.T) {
	s3 := func(prefix string) *BackupConfig {
		return &BackupConfig{
			Type: BackupTypeS3,
			S3:   &S3BackupConfig{Bucket: "b", Region: "r", KeyPrefix: prefix},
		}
	}
	for _, tc := range []struct {
		name   string
		backup *BackupConfig
		want   bool
	}{
		{"no backup config", nil, false},
		{"filesystem", &BackupConfig{Type: BackupTypeFilesystem}, false},
		{"s3 type without s3 settings", &BackupConfig{Type: BackupTypeS3}, false},
		{"s3 without prefix", s3(""), true},
		{"s3 with slash-only prefix", s3("//"), true},
		{"s3 with prefix", s3("tenant-a"), false},
		{"s3 with prefix and slashes", s3("/tenant-a/"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := UsesDefaultS3Repository(tc.backup); got != tc.want {
				t.Errorf("UsesDefaultS3Repository() = %v, want %v", got, tc.want)
			}
		})
	}
}
