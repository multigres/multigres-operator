package backuphandler

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// ShardLabels are the labels that identify a backup's cluster and shard. The
// per-shard queue lists MultigresBackups by them.
func ShardLabels(cluster, database, tableGroup, shard string) map[string]string {
	return map[string]string{
		metadata.LabelMultigresCluster:    cluster,
		metadata.LabelMultigresDatabase:   database,
		metadata.LabelMultigresTableGroup: tableGroup,
		metadata.LabelMultigresShard:      shard,
	}
}

// BackupShardLabels returns ShardLabels for a MultigresBackup's spec.
func BackupShardLabels(b *multigresv1alpha1.MultigresBackup) map[string]string {
	return ShardLabels(b.Spec.ClusterRef.Name, b.Spec.Database, b.Spec.TableGroup, b.Spec.Shard)
}

// ScheduleLabelValue returns the value of the backup-schedule label for a
// schedule name. Label values are limited to 63 characters, so longer names
// are truncated with a hash suffix.
func ScheduleLabelValue(schedule string) string {
	if len(schedule) <= 63 {
		return schedule
	}
	return name.JoinWithConstraints(name.Constraints{
		MaxLength:      63,
		ValidFirstChar: func(r rune) bool { return r >= 'a' && r <= 'z' || r >= '0' && r <= '9' },
	}, schedule)
}

// ListShardBackups lists every MultigresBackup for one shard. It matches on
// spec rather than labels: a manual backup is only labeled on its first
// reconcile, and must still hold its place in the queue before that.
func ListShardBackups(
	ctx context.Context,
	c client.Reader,
	namespace, cluster, database, tableGroup, shard string,
) ([]multigresv1alpha1.MultigresBackup, error) {
	list := &multigresv1alpha1.MultigresBackupList{}
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("listing MultigresBackups: %w", err)
	}
	var out []multigresv1alpha1.MultigresBackup
	for _, b := range list.Items {
		if b.Spec.ClusterRef.Name == cluster && b.Spec.Database == database &&
			b.Spec.TableGroup == tableGroup && b.Spec.Shard == shard {
			out = append(out, b)
		}
	}
	return out, nil
}

// QueuedBefore reports whether a is ahead of b in the per-shard
// first-come-first-served queue: created earlier, ties broken by name.
func QueuedBefore(a, b *multigresv1alpha1.MultigresBackup) bool {
	at, bt := a.CreationTimestamp.Time, b.CreationTimestamp.Time
	if !at.Equal(bt) {
		return at.Before(bt)
	}
	return a.Name < b.Name
}

// CoveringBackup returns an unfinished backup of the same or a stronger type
// than kind, if there is one. A scheduled run is skipped while such a backup
// exists, since it already gives the window a fresh backup.
func CoveringBackup(
	backups []multigresv1alpha1.MultigresBackup,
	kind multigresv1alpha1.BackupKind,
) *multigresv1alpha1.MultigresBackup {
	for i := range backups {
		b := &backups[i]
		if b.Status.Phase.Finished() || !b.DeletionTimestamp.IsZero() {
			continue
		}
		if b.Spec.Type.Strength() >= kind.Strength() {
			return b
		}
	}
	return nil
}

// ShardBootstrapped reports whether a shard has finished bootstrapping: its
// first backup exists and a primary is known. Poolers report SERVING before
// the bootstrap backup completes, so without this gate a scheduled backup
// could take the shard's backup lease and cancel the bootstrap backup.
func ShardBootstrapped(shard *multigresv1alpha1.Shard) (bool, string) {
	if shard.Status.LastBackupTime == nil {
		return false, "shard has no completed backup yet (bootstrap in progress)"
	}
	for _, role := range shard.Status.PodRoles {
		if role == "PRIMARY" {
			return true, ""
		}
	}
	for _, c := range shard.Status.Conditions {
		if c.Type == multigresv1alpha1.ConditionPrimary && c.Status == "True" {
			return true, ""
		}
	}
	return false, "shard has no primary"
}
