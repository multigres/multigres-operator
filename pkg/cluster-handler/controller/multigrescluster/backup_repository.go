package multigrescluster

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

// syncBackupRepositoryCondition sets ConditionBackupRepositoryValid from the
// defaulted spec and returns the refusal, if any. It applies every rule
// admission applies, so the refusal holds with the webhook disabled and for a
// stored cluster that already violates one. The event is emitted when the
// refusal starts or its reason changes.
//
// The caller does not register databases in topology or apply TableGroups for
// a refused cluster: either would start, or keep, backups going somewhere
// other than the repository its configuration asks for, or build Shards whose
// sidecars cannot be started.
func (r *MultigresClusterReconciler) syncBackupRepositoryCondition(
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionBackupRepositoryValid,
		Status:             metav1.ConditionTrue,
		Message:            "Backups go to the configured repository",
		ObservedGeneration: cluster.Generation,
	}
	reason, err := resolver.SidecarConfigProblem(cluster)
	cond.Reason = reason
	if err != nil {
		cond.Status = metav1.ConditionFalse
		cond.Message = err.Error()
		prev := meta.FindStatusCondition(
			cluster.Status.Conditions, multigresv1alpha1.ConditionBackupRepositoryValid,
		)
		if r.Recorder != nil &&
			(prev == nil || prev.Status != metav1.ConditionFalse || prev.Reason != reason) {
			r.Recorder.Event(cluster, "Warning", "BackupRepositoryInvalid", cond.Message)
		}
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, cond)
	return err
}
