package backuphealth_test

import (
	"testing"
	"time"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
)

func newTestShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-shard",
			Namespace:  "default",
			Generation: 1,
			Labels:     map[string]string{"multigres.com/cluster": "test-cluster"},
		},
	}
}

// recentBackupID generates a pgBackRest-style backup ID for a backup that
// completed `age` ago. Uses UTC because ParseTime (which uses time.Parse
// without a location) returns UTC timestamps.
func recentBackupID(age time.Duration) string {
	return time.Now().UTC().Add(-age).Format("20060102-150405")
}

func TestEvaluateBackupsExtended_BackupCounts(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	backups := []*multipoolermanagerdatapb.BackupMetadata{
		{
			BackupId: recentBackupID(1 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
		{
			BackupId: recentBackupID(2 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
		{
			BackupId: recentBackupID(30 * time.Minute),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "diff",
		},
		{
			BackupId: recentBackupID(3 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_INCOMPLETE,
			Type:     "full",
		},
	}
	retention := &multigresv1alpha1.RetentionPolicy{FullCount: ptr.To(int32(4))}

	result := backuphealth.EvaluateBackupsExtended(shard, backups, retention, nil)

	if result.FullBackupCount != 2 {
		t.Errorf("expected 2 full backups, got %d", result.FullBackupCount)
	}
	if result.DiffBackupCount != 1 {
		t.Errorf("expected 1 diff backup, got %d", result.DiffBackupCount)
	}
}

func TestEvaluateBackupsExtended_OldestBackupAge(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	backups := []*multipoolermanagerdatapb.BackupMetadata{
		{
			BackupId: recentBackupID(1 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
		{
			BackupId: recentBackupID(48 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}

	result := backuphealth.EvaluateBackupsExtended(shard, backups, nil, nil)

	if result.OldestBackupAge < 47*time.Hour || result.OldestBackupAge > 50*time.Hour {
		t.Errorf("expected oldest age ~48h, got %v", result.OldestBackupAge)
	}
}

func TestEvaluateBackupsExtended_NoBackups_OldestAgeZero(t *testing.T) {
	t.Parallel()
	shard := newTestShard()

	result := backuphealth.EvaluateBackupsExtended(shard, nil, nil, nil)

	if result.OldestBackupAge != 0 {
		t.Errorf("expected zero oldest age with no backups, got %v", result.OldestBackupAge)
	}
}

func TestEvaluateBackupsExtended_IntegrityCheckPass(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	backups := []*multipoolermanagerdatapb.BackupMetadata{
		{
			BackupId: recentBackupID(1 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}
	check := &backuphealth.IntegrityCheckResult{Passed: true}

	result := backuphealth.EvaluateBackupsExtended(shard, backups, nil, check)

	if result.RepositoryHealthy == nil || !*result.RepositoryHealthy {
		t.Error("expected RepositoryHealthy=true when integrity check passes")
	}
	if result.RepositoryReason != backuphealth.ReasonHealthy {
		t.Errorf("expected reason %q, got %q", backuphealth.ReasonHealthy, result.RepositoryReason)
	}
}

func TestEvaluateBackupsExtended_IntegrityCheckFail(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	backups := []*multipoolermanagerdatapb.BackupMetadata{
		{
			BackupId: recentBackupID(1 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}
	check := &backuphealth.IntegrityCheckResult{Passed: false, Error: "stanza mismatch"}

	result := backuphealth.EvaluateBackupsExtended(shard, backups, nil, check)

	if result.RepositoryHealthy == nil || *result.RepositoryHealthy {
		t.Error("expected RepositoryHealthy=false when integrity check fails")
	}
	if result.RepositoryReason != backuphealth.ReasonIntegrityCheckFailed {
		t.Errorf(
			"expected reason %q, got %q",
			backuphealth.ReasonIntegrityCheckFailed,
			result.RepositoryReason,
		)
	}
}

func TestEvaluateBackupsExtended_NoIntegrityCheck_Nil(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	backups := []*multipoolermanagerdatapb.BackupMetadata{
		{
			BackupId: recentBackupID(1 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}

	result := backuphealth.EvaluateBackupsExtended(shard, backups, nil, nil)

	if result.RepositoryHealthy != nil {
		t.Errorf(
			"expected RepositoryHealthy=nil when no integrity check, got %v",
			*result.RepositoryHealthy,
		)
	}
}

func TestEvaluateBackupsExtended_RetentionCountWarning(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	backups := []*multipoolermanagerdatapb.BackupMetadata{
		{
			BackupId: recentBackupID(1 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}
	retention := &multigresv1alpha1.RetentionPolicy{FullCount: ptr.To(int32(4))}

	result := backuphealth.EvaluateBackupsExtended(shard, backups, retention, nil)

	if !result.RetentionCountWarning {
		t.Error("expected retention count warning when 1 full backup vs retention=4")
	}
}

func TestEvaluateBackupsExtended_RetentionCountOK(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	backups := []*multipoolermanagerdatapb.BackupMetadata{
		{
			BackupId: recentBackupID(1 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
		{
			BackupId: recentBackupID(25 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
		{
			BackupId: recentBackupID(49 * time.Hour),
			Status:   multipoolermanagerdatapb.BackupMetadata_COMPLETE,
			Type:     "full",
		},
	}
	retention := &multigresv1alpha1.RetentionPolicy{FullCount: ptr.To(int32(3))}

	result := backuphealth.EvaluateBackupsExtended(shard, backups, retention, nil)

	if result.RetentionCountWarning {
		t.Error("expected no retention count warning when count matches retention")
	}
}

func TestApplyExtended_SetsConditionTrue(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	healthy := true
	result := &backuphealth.ExtendedResult{
		RepositoryHealthy: &healthy,
		RepositoryReason:  backuphealth.ReasonHealthy,
		RepositoryMessage: "OK",
	}

	backuphealth.ApplyExtended(shard, result)

	found := false
	for _, c := range shard.Status.Conditions {
		if c.Type == backuphealth.ConditionRepositoryHealthy {
			found = true
			if c.Status != metav1.ConditionTrue {
				t.Errorf("expected True, got %s", c.Status)
			}
		}
	}
	if !found {
		t.Error("BackupRepositoryHealthy condition not set")
	}
}

func TestApplyExtended_SetsConditionFalse(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	unhealthy := false
	result := &backuphealth.ExtendedResult{
		RepositoryHealthy: &unhealthy,
		RepositoryReason:  backuphealth.ReasonIntegrityCheckFailed,
		RepositoryMessage: "check failed",
	}

	backuphealth.ApplyExtended(shard, result)

	for _, c := range shard.Status.Conditions {
		if c.Type == backuphealth.ConditionRepositoryHealthy {
			if c.Status != metav1.ConditionFalse {
				t.Errorf("expected False, got %s", c.Status)
			}
			if c.Reason != backuphealth.ReasonIntegrityCheckFailed {
				t.Errorf(
					"expected reason %q, got %q",
					backuphealth.ReasonIntegrityCheckFailed,
					c.Reason,
				)
			}
			return
		}
	}
	t.Error("BackupRepositoryHealthy condition not set")
}

func TestApplyExtended_RemovesStaleCondition(t *testing.T) {
	t.Parallel()
	shard := newTestShard()
	// Pre-set a condition
	shard.Status.Conditions = []metav1.Condition{
		{
			Type:   backuphealth.ConditionRepositoryHealthy,
			Status: metav1.ConditionTrue,
			Reason: "Healthy",
		},
		{Type: "Available", Status: metav1.ConditionTrue, Reason: "AllPodsReady"},
	}

	// Apply with nil RepositoryHealthy — should remove the stale condition
	result := &backuphealth.ExtendedResult{RepositoryHealthy: nil}
	backuphealth.ApplyExtended(shard, result)

	for _, c := range shard.Status.Conditions {
		if c.Type == backuphealth.ConditionRepositoryHealthy {
			t.Error(
				"expected BackupRepositoryHealthy condition to be removed, but it's still present",
			)
		}
	}
	// Other conditions should be preserved
	if len(shard.Status.Conditions) != 1 {
		t.Errorf("expected 1 remaining condition, got %d", len(shard.Status.Conditions))
	}
}
