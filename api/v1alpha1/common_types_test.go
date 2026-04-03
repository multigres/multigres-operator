package v1alpha1

import (
	"testing"

	"k8s.io/utils/ptr"
)

func TestMergeBackupConfig_Retention(t *testing.T) {
	t.Parallel()

	t.Run("child overrides parent", func(t *testing.T) {
		t.Parallel()
		parent := &BackupConfig{
			Type: BackupTypeS3,
			Retention: &RetentionPolicy{
				FullCount:         ptr.To(int32(4)),
				DifferentialCount: ptr.To(int32(1)),
			},
		}
		child := &BackupConfig{
			Type: BackupTypeS3,
			Retention: &RetentionPolicy{
				FullCount: ptr.To(int32(7)),
			},
		}
		merged := MergeBackupConfig(child, parent)
		if merged.Retention == nil {
			t.Fatal("expected Retention to be set")
		}
		if *merged.Retention.FullCount != 7 {
			t.Errorf("FullCount = %d, want 7", *merged.Retention.FullCount)
		}
		if *merged.Retention.DifferentialCount != 1 {
			t.Errorf(
				"DifferentialCount = %d, want 1 (from parent)",
				*merged.Retention.DifferentialCount,
			)
		}
	})

	t.Run("parent preserved when child unset", func(t *testing.T) {
		t.Parallel()
		parent := &BackupConfig{
			Type: BackupTypeS3,
			S3:   &S3BackupConfig{Bucket: "b", Region: "r"},
			Retention: &RetentionPolicy{
				FullCount:         ptr.To(int32(5)),
				DifferentialCount: ptr.To(int32(3)),
			},
		}
		child := &BackupConfig{
			Type: BackupTypeS3,
			S3:   &S3BackupConfig{Bucket: "b", Region: "r"},
		}
		merged := MergeBackupConfig(child, parent)
		if merged.Retention == nil {
			t.Fatal("expected parent Retention to be preserved")
		}
		if *merged.Retention.FullCount != 5 {
			t.Errorf("FullCount = %d, want 5", *merged.Retention.FullCount)
		}
		if *merged.Retention.DifferentialCount != 3 {
			t.Errorf("DifferentialCount = %d, want 3", *merged.Retention.DifferentialCount)
		}
	})

	t.Run("nil child retention preserves parent", func(t *testing.T) {
		t.Parallel()
		parent := &BackupConfig{
			Type: BackupTypeS3,
			Retention: &RetentionPolicy{
				FullCount:         ptr.To(int32(4)),
				DifferentialCount: ptr.To(int32(2)),
			},
		}
		child := &BackupConfig{
			Type: BackupTypeS3,
			S3:   &S3BackupConfig{Bucket: "child-bucket", Region: "us-west-2"},
		}
		merged := MergeBackupConfig(child, parent)
		if merged.Retention == nil {
			t.Fatal("expected Retention from parent to be preserved")
		}
		if *merged.Retention.FullCount != 4 {
			t.Errorf("FullCount = %d, want 4", *merged.Retention.FullCount)
		}
	})
}

func TestMergeBackupConfig_S3PointerBools(t *testing.T) {
	t.Parallel()

	t.Run("parent preserved when child unset", func(t *testing.T) {
		t.Parallel()
		parent := &BackupConfig{
			Type: BackupTypeS3,
			S3: &S3BackupConfig{
				Bucket:                     "parent-bucket",
				Region:                     "us-east-1",
				CleanupOnDelete:            ptr.To(true),
				AllowStaleMetadataRecovery: ptr.To(true),
			},
		}
		child := &BackupConfig{
			Type: BackupTypeS3,
			S3:   &S3BackupConfig{Bucket: "child-bucket", Region: "us-east-1"},
		}
		merged := MergeBackupConfig(child, parent)
		if merged.S3.CleanupOnDelete == nil || !*merged.S3.CleanupOnDelete {
			t.Error("expected parent CleanupOnDelete=true to be preserved")
		}
		if merged.S3.AllowStaleMetadataRecovery == nil || !*merged.S3.AllowStaleMetadataRecovery {
			t.Error("expected parent AllowStaleMetadataRecovery=true to be preserved")
		}
		if merged.S3.Bucket != "child-bucket" {
			t.Errorf("Bucket = %s, want child-bucket", merged.S3.Bucket)
		}
	})

	t.Run("child overrides parent", func(t *testing.T) {
		t.Parallel()
		parent := &BackupConfig{
			Type: BackupTypeS3,
			S3: &S3BackupConfig{
				Bucket:          "b",
				Region:          "r",
				CleanupOnDelete: ptr.To(true),
			},
		}
		child := &BackupConfig{
			Type: BackupTypeS3,
			S3: &S3BackupConfig{
				Bucket:          "b",
				Region:          "r",
				CleanupOnDelete: ptr.To(false),
			},
		}
		merged := MergeBackupConfig(child, parent)
		if merged.S3.CleanupOnDelete == nil || *merged.S3.CleanupOnDelete {
			t.Error("expected child CleanupOnDelete=false to override parent true")
		}
	})
}

func TestBackupConfig_DeepCopyPreservesRetention(t *testing.T) {
	t.Parallel()
	original := &BackupConfig{
		Type: BackupTypeS3,
		S3:   &S3BackupConfig{Bucket: "b", Region: "r"},
		Retention: &RetentionPolicy{
			FullCount: ptr.To(int32(4)),
		},
	}
	copied := original.DeepCopy()
	*copied.Retention.FullCount = 99
	if *original.Retention.FullCount != 4 {
		t.Errorf(
			"DeepCopy leaked pointer: original FullCount changed to %d",
			*original.Retention.FullCount,
		)
	}
}
