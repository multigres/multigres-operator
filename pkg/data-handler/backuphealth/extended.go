package backuphealth

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

const (
	ConditionRepositoryHealthy = "BackupRepositoryHealthy"

	ReasonHealthy               = "Healthy"
	ReasonIntegrityCheckFailed  = "IntegrityCheckFailed"
	ReasonWALArchiveIncomplete  = "WALArchiveIncomplete"
	ReasonStaleMetadata         = "StaleMetadata"
	ReasonRepositoryUnreachable = "RepositoryUnreachable"
	ReasonAllPodsCorrupted      = "AllPodsCorrupted"
)

// IntegrityCheckResult holds the result of a pgbackrest check.
type IntegrityCheckResult struct {
	Passed bool
	Error  string
}

// ExtendedResult adds repository health fields to Result.
type ExtendedResult struct {
	Result // embedded existing staleness result

	// RepositoryHealthy is nil when no integrity result is available.
	RepositoryHealthy *bool
	RepositoryReason  string
	RepositoryMessage string

	// Metrics data
	FullBackupCount int
	DiffBackupCount int
	OldestBackupAge time.Duration

	// RetentionCountWarning reports an out-of-range full backup count.
	RetentionCountWarning bool
	RetentionCountMessage string
}

// EvaluateBackupsExtended evaluates backup state, integrity, and retention signals.
func EvaluateBackupsExtended(
	shard *multigresv1alpha1.Shard,
	backups []*multipoolermanagerdatapb.BackupMetadata,
	retention *multigresv1alpha1.RetentionPolicy,
	integrityCheck *IntegrityCheckResult,
) *ExtendedResult {
	// Run existing staleness evaluation.
	base := EvaluateBackups(shard, backups)

	ext := &ExtendedResult{
		Result: *base,
	}

	// Count backups by type.
	for _, b := range backups {
		if b.Status != multipoolermanagerdatapb.BackupMetadata_COMPLETE {
			continue
		}
		switch b.Type {
		case "full":
			ext.FullBackupCount++
		case "diff":
			ext.DiffBackupCount++
		}
	}

	// Compute oldest backup age.
	var oldest time.Time
	for _, b := range backups {
		if b.Status != multipoolermanagerdatapb.BackupMetadata_COMPLETE {
			continue
		}
		t := ParseTime(b.BackupId)
		if t.IsZero() {
			continue
		}
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	if !oldest.IsZero() {
		ext.OldestBackupAge = time.Since(oldest)
	}

	// Check integrity result from Multipooler State RPC.
	if integrityCheck != nil {
		if integrityCheck.Passed {
			healthy := true
			ext.RepositoryHealthy = &healthy
			ext.RepositoryReason = ReasonHealthy
			ext.RepositoryMessage = "Repository integrity check passed"
		} else {
			unhealthy := false
			ext.RepositoryHealthy = &unhealthy
			ext.RepositoryReason = ReasonIntegrityCheckFailed
			ext.RepositoryMessage = fmt.Sprintf(
				"pgbackrest check failed: %s",
				integrityCheck.Error,
			)
		}
	}
	// Check retention count bounds.
	if retention != nil && retention.FullCount != nil {
		expected := int(*retention.FullCount)
		if ext.FullBackupCount < expected-1 || ext.FullBackupCount > expected+1 {
			ext.RetentionCountWarning = true
			ext.RetentionCountMessage = fmt.Sprintf(
				"Full backup count %d is outside expected range [%d, %d] for retention fullCount=%d",
				ext.FullBackupCount,
				expected-1,
				expected+1,
				expected,
			)
		}
	}

	// Emit metrics.
	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	monitoring.SetBackupRetainedCount(
		clusterName,
		shard.Name,
		shard.Namespace,
		ext.FullBackupCount,
		ext.DiffBackupCount,
	)
	// Always set oldest age so the gauge does not retain a stale value.
	monitoring.SetBackupOldestRetainedAge(
		clusterName,
		shard.Name,
		shard.Namespace,
		ext.OldestBackupAge,
	)

	return ext
}

// ApplyExtended updates the BackupRepositoryHealthy condition.
func ApplyExtended(shard *multigresv1alpha1.Shard, result *ExtendedResult) {
	if result == nil {
		return
	}

	if result.RepositoryHealthy == nil {
		filtered := make([]metav1.Condition, 0, len(shard.Status.Conditions))
		for _, c := range shard.Status.Conditions {
			if c.Type != ConditionRepositoryHealthy {
				filtered = append(filtered, c)
			}
		}
		shard.Status.Conditions = filtered
		return
	}

	condition := metav1.Condition{
		Type:               ConditionRepositoryHealthy,
		ObservedGeneration: shard.Generation,
		LastTransitionTime: metav1.Now(),
		Message:            result.RepositoryMessage,
	}

	if *result.RepositoryHealthy {
		condition.Status = metav1.ConditionTrue
		condition.Reason = ReasonHealthy
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Reason = result.RepositoryReason
	}

	status.SetCondition(&shard.Status.Conditions, condition)
}
