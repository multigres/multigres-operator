package multigrescluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// buildShardServiceAccount returns the one ServiceAccount every shard pod of
// the cluster runs as. The owner is the MultigresCluster, never a Shard: the
// name merges into every shard, so a per-Shard owner would let deleting one
// shard garbage-collect the account the others still use.
func buildShardServiceAccount(
	cluster *multigresv1alpha1.MultigresCluster,
	scheme *runtime.Scheme,
) (*corev1.ServiceAccount, error) {
	spec := cluster.Spec.ShardServiceAccount
	sa := &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        spec.Name,
			Namespace:   cluster.Namespace,
			Annotations: spec.Annotations,
			Labels: map[string]string{
				metadata.LabelAppManagedBy: metadata.ManagedByMultigres,
			},
		},
		// Shard pods get their credentials from the sidecar, not from a mounted
		// API token.
		AutomountServiceAccountToken: ptr.To(false),
	}
	if err := controllerutil.SetControllerReference(cluster, sa, scheme); err != nil {
		return nil, err
	}
	return sa, nil
}

// reconcileShardServiceAccount applies the shard ServiceAccount when the
// cluster names one.
//
// Server-side apply makes adoption of a pre-existing account (an existing
// cluster's platform-created IRSA account) the same operation as creation: we
// own only the annotations we set, so the account's other annotations survive
// and ours win on conflict. Removing the field, or renaming the account,
// deletes nothing, since the account may be shared with things we did not
// create.
func (r *MultigresClusterReconciler) reconcileShardServiceAccount(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	if cluster.Spec.ShardServiceAccount == nil {
		return nil
	}

	sa, err := buildShardServiceAccount(cluster, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build shard service account: %w", err)
	}
	if err := r.Patch(
		ctx,
		sa,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply shard service account %s: %w", sa.Name, err)
	}
	return nil
}

// syncShardServiceAccount applies the shard ServiceAccount and reports a
// failure without halting the cluster reconcile. A conflict (for example the
// account already has a different controller owner) is a per-cluster problem
// that a human must resolve, and it must not stop cells, tablegroups and
// status from converging. The caller continues to the cells and returns the
// error at the end, so the work queue requeues with backoff.
func (r *MultigresClusterReconciler) syncShardServiceAccount(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	if cluster.Spec.ShardServiceAccount == nil {
		meta.RemoveStatusCondition(
			&cluster.Status.Conditions,
			multigresv1alpha1.ConditionShardServiceAccountReady,
		)
		return nil
	}

	err := r.reconcileShardServiceAccount(ctx, cluster)
	if err == nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               multigresv1alpha1.ConditionShardServiceAccountReady,
			Status:             metav1.ConditionTrue,
			Reason:             multigresv1alpha1.ReasonShardServiceAccountApplied,
			Message:            "Shard ServiceAccount is applied",
			ObservedGeneration: cluster.Generation,
		})
		return nil
	}

	name := cluster.Spec.ShardServiceAccount.Name
	reason := multigresv1alpha1.ReasonShardServiceAccountApplyFailed
	eventReason := "ShardServiceAccountApplyFailed"
	msg := fmt.Sprintf("Failed to apply shard ServiceAccount %q: %v", name, err)
	if other := r.otherController(ctx, cluster, name); other != "" {
		reason = multigresv1alpha1.ReasonShardServiceAccountConflict
		eventReason = "ShardServiceAccountConflict"
		msg = fmt.Sprintf(
			"Shard ServiceAccount %q is controlled by %s; remove its controller ownerReference or "+
				"choose another name: %v",
			name, other, err,
		)
	}
	log.FromContext(ctx).
		Error(err, "Failed to reconcile shard ServiceAccount", "serviceAccount", name)
	r.Recorder.Event(cluster, "Warning", eventReason, msg)
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               multigresv1alpha1.ConditionShardServiceAccountReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: cluster.Generation,
	})
	return err
}

// otherController names the controller owner of the account when it is not
// this cluster, best effort: a read failure just yields the generic message.
// It reads through the APIReader because the informer only holds ServiceAccounts
// carrying the managed-by label, and a conflicting account by definition does
// not carry ours.
func (r *MultigresClusterReconciler) otherController(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	saName string,
) string {
	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}
	sa := &corev1.ServiceAccount{}
	if err := reader.Get(
		ctx,
		types.NamespacedName{Name: saName, Namespace: cluster.Namespace},
		sa,
	); err != nil {
		return ""
	}
	if ref := metav1.GetControllerOf(sa); ref != nil && ref.UID != cluster.UID {
		return fmt.Sprintf("%s %q", ref.Kind, ref.Name)
	}
	return ""
}
