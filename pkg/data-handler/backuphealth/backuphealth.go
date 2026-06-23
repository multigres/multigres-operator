// Package backuphealth evaluates the health of database backups by querying
// the primary pooler via gRPC for backup metadata and determining whether
// the most recent backup is within acceptable staleness thresholds.
package backuphealth

import (
	"context"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

const (
	// BackupStaleThreshold is the maximum age of a completed backup before it
	// is considered stale. Set to 25h to accommodate daily backup schedules
	// with a 1-hour buffer for execution time.
	BackupStaleThreshold = 25 * time.Hour

	// backupQueryLimit caps the number of backup records fetched per RPC call.
	backupQueryLimit = 10

	// ConditionHealthy is the condition type for backup health.
	ConditionHealthy = "BackupHealthy"
)

// Result holds the computed backup health information.
type Result struct {
	Healthy        bool
	LastBackupTime *metav1.Time
	LastBackupType string
	Message        string
}

// Evaluate discovers the primary pooler from the topology and
// queries it for backup metadata. Returns nil (no error) if no primary pooler
// is registered yet, which is normal during cluster bootstrap.
func Evaluate(
	ctx context.Context,
	store topoclient.Store,
	rpcClient rpcclient.MultiPoolerClient,
	shard *multigresv1alpha1.Shard,
) (*Result, error) {
	logger := log.FromContext(ctx)

	cells := topo.CollectCells(shard)
	primary, err := topo.FindPrimaryPooler(ctx, store, shard, cells)
	if err != nil {
		return nil, fmt.Errorf("finding primary pooler: %w", err)
	}
	if primary == nil {
		logger.V(1).Info("No primary pooler registered in topology, skipping backup health check")
		return nil, nil
	}

	resp, err := rpcClient.GetBackups(ctx, primary,
		&multipoolermanagerdatapb.GetBackupsRequest{Limit: backupQueryLimit})
	if err != nil {
		return nil, fmt.Errorf("querying backups from primary %s: %w",
			topoclient.ClusterIDString(primary.Id), err)
	}
	if resp == nil {
		return nil, fmt.Errorf("nil response from primary %s",
			topoclient.ClusterIDString(primary.Id))
	}

	return EvaluateBackups(shard, resp.Backups), nil
}

// EvaluateBackups inspects backup metadata and produces a health result.
// It looks for the most recent COMPLETE backup.
func EvaluateBackups(
	shard *multigresv1alpha1.Shard,
	backups []*multipoolermanagerdatapb.BackupMetadata,
) *Result {
	if len(backups) == 0 {
		return &Result{
			Healthy: false,
			Message: "No backups found",
		}
	}

	var latest *multipoolermanagerdatapb.BackupMetadata
	for _, b := range backups {
		if b.Status != multipoolermanagerdatapb.BackupMetadata_COMPLETE {
			continue
		}
		if latest == nil {
			latest = b
			continue
		}
		if b.BackupId > latest.BackupId {
			latest = b
		}
	}

	if latest == nil {
		return &Result{
			Healthy: false,
			Message: "No completed backups found",
		}
	}

	backupTime := ParseTime(latest.BackupId)
	if backupTime.IsZero() {
		return &Result{
			Healthy: false,
			Message: fmt.Sprintf("Failed to parse backup timestamp from ID %q", latest.BackupId),
		}
	}

	now := time.Now()
	age := now.Sub(backupTime)

	clusterName := shard.Labels[metadata.LabelMultigresCluster]
	monitoring.SetLastBackupAge(clusterName, shard.Name, shard.Namespace, age)

	result := &Result{
		LastBackupTime: &metav1.Time{Time: backupTime},
		LastBackupType: latest.Type,
	}

	if age > BackupStaleThreshold {
		result.Healthy = false
		result.Message = fmt.Sprintf("Last backup is %.1fh old (threshold: %.0fh)",
			age.Hours(), BackupStaleThreshold.Hours())
	} else {
		result.Healthy = true
		result.Message = fmt.Sprintf("Last %s backup completed %.1fh ago", latest.Type, age.Hours())
	}

	return result
}

// ParseTime extracts the timestamp from a pgBackRest backup ID.
// Format: YYYYMMDD-HHMMSSF (F = fractional seconds suffix).
func ParseTime(backupID string) time.Time {
	if len(backupID) < 15 {
		return time.Time{}
	}
	t, err := time.Parse("20060102-150405", backupID[:15])
	if err != nil {
		return time.Time{}
	}
	return t
}

// Apply updates the shard status with backup health information
// and sets the BackupHealthy condition.
func Apply(shard *multigresv1alpha1.Shard, result *Result) {
	if result == nil {
		return
	}

	shard.Status.LastBackupTime = result.LastBackupTime
	shard.Status.LastBackupType = result.LastBackupType

	condition := metav1.Condition{
		Type:               ConditionHealthy,
		ObservedGeneration: shard.Generation,
		LastTransitionTime: metav1.Now(),
		Message:            result.Message,
	}

	if result.Healthy {
		condition.Status = metav1.ConditionTrue
		condition.Reason = "BackupRecent"
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "BackupStale"
	}

	status.SetCondition(&shard.Status.Conditions, condition)
}
