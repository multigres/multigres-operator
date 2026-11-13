package multigrescluster

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/ownership"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// BackupScheduleName is the name of the MultigresBackupSchedule rendered from
// a spec.backupSchedules entry.
func BackupScheduleName(cluster *multigresv1alpha1.MultigresCluster, entry string) string {
	return cluster.Name + "-" + entry
}

// BuildBackupSchedule renders one spec.backupSchedules entry.
func BuildBackupSchedule(
	cluster *multigresv1alpha1.MultigresCluster,
	entry *multigresv1alpha1.BackupScheduleEntry,
	scheme *runtime.Scheme,
) (*multigresv1alpha1.MultigresBackupSchedule, error) {
	labels := metadata.BuildStandardLabels(cluster.Name, "backup-schedule")
	metadata.AddClusterLabel(labels, cluster.Name)
	sched := &multigresv1alpha1.MultigresBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      BackupScheduleName(cluster, entry.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: multigresv1alpha1.MultigresBackupScheduleSpec{
			ClusterRef:             multigresv1alpha1.BackupClusterReference{Name: cluster.Name},
			BackupScheduleTemplate: *entry.BackupScheduleTemplate.DeepCopy(),
		},
	}
	if err := controllerutil.SetControllerReference(cluster, sched, scheme); err != nil {
		return nil, err
	}
	return sched, nil
}

// reconcileBackupSchedules renders spec.backupSchedules into
// MultigresBackupSchedule objects and deletes the ones whose entry was
// removed. Schedules users created directly are left alone: they are not
// controlled by the cluster.
func (r *MultigresClusterReconciler) reconcileBackupSchedules(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	desired := make(map[string]bool, len(cluster.Spec.BackupSchedules))
	for i := range cluster.Spec.BackupSchedules {
		sched, err := BuildBackupSchedule(cluster, &cluster.Spec.BackupSchedules[i], r.Scheme)
		if err != nil {
			return fmt.Errorf("building backup schedule %q: %w",
				cluster.Spec.BackupSchedules[i].Name, err)
		}
		desired[sched.Name] = true
		sched.SetGroupVersionKind(
			multigresv1alpha1.GroupVersion.WithKind("MultigresBackupSchedule"),
		)
		if err := r.Patch(
			ctx,
			sched,
			client.Apply,
			client.ForceOwnership,
			client.FieldOwner(ownership.ManagerChildWriter),
		); err != nil {
			return fmt.Errorf("applying backup schedule %s: %w", sched.Name, err)
		}
	}

	existing := &multigresv1alpha1.MultigresBackupScheduleList{}
	if err := r.List(ctx, existing,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{metadata.LabelMultigresCluster: cluster.Name},
	); err != nil {
		return fmt.Errorf("listing backup schedules: %w", err)
	}
	for i := range existing.Items {
		item := &existing.Items[i]
		owner := metav1.GetControllerOf(item)
		if owner == nil || owner.UID != cluster.UID || desired[item.Name] {
			continue
		}
		if err := r.Delete(ctx, item); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("deleting backup schedule %s: %w", item.Name, err)
		}
		r.Recorder.Eventf(cluster, "Normal", "Deleted",
			"Deleted MultigresBackupSchedule %s", item.Name)
	}
	return nil
}
