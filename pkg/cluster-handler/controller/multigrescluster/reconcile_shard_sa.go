package multigrescluster

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// shardServiceAccountFieldOwner is the server-side-apply field manager the
// shard ServiceAccount is applied under.
const shardServiceAccountFieldOwner = "multigres-operator"

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
	// The cluster is reconciled on every status change of its children, and
	// an apply that changes nothing is still a request the API server has to
	// authenticate, admit and merge. The cached account is enough to decide:
	// an account the operator has applied carries its label and so is in the
	// informer, and a stale read is corrected by the reconcile the informer's
	// update starts.
	observed := &corev1.ServiceAccount{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(sa), observed); err == nil &&
		shardServiceAccountApplied(observed, sa) {
		return nil
	}
	if err := r.Patch(
		ctx,
		sa,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner(shardServiceAccountFieldOwner),
	); err != nil {
		return fmt.Errorf("failed to apply shard service account %s: %w", sa.Name, err)
	}
	return nil
}

// shardServiceAccountApplied reports whether applying desired would leave
// observed as it is. That needs more than equal values: server-side apply
// also removes an annotation the operator applied earlier and no longer
// sets, so the annotations the operator's field manager owns must be exactly
// the desired ones. An account the operator has never applied has no such
// entry and is applied, which is how a pre-existing account is adopted.
func shardServiceAccountApplied(observed, desired *corev1.ServiceAccount) bool {
	if observed.AutomountServiceAccountToken == nil || *observed.AutomountServiceAccountToken {
		return false
	}
	for k, v := range desired.Labels {
		if observed.Labels[k] != v {
			return false
		}
	}
	for k, v := range desired.Annotations {
		if got, ok := observed.Annotations[k]; !ok || got != v {
			return false
		}
	}
	for _, want := range desired.OwnerReferences {
		if !slices.ContainsFunc(observed.OwnerReferences, func(got metav1.OwnerReference) bool {
			return apiequality.Semantic.DeepEqual(got, want)
		}) {
			return false
		}
	}
	owned, ok := appliedAnnotationKeys(observed)
	if !ok || len(owned) != len(desired.Annotations) {
		return false
	}
	for k := range owned {
		if _, wanted := desired.Annotations[k]; !wanted {
			return false
		}
	}
	return true
}

// appliedAnnotationKeys returns the annotation keys the operator's
// server-side-apply field manager owns on the account. ok is false when the
// account has no readable entry of that manager.
func appliedAnnotationKeys(sa *corev1.ServiceAccount) (keys map[string]struct{}, ok bool) {
	for _, e := range sa.GetManagedFields() {
		if e.Manager != shardServiceAccountFieldOwner ||
			e.Operation != metav1.ManagedFieldsOperationApply || e.Subresource != "" {
			continue
		}
		set := &fieldpath.Set{}
		if e.FieldsV1 == nil || set.FromJSON(e.FieldsV1.GetRawReader()) != nil {
			return nil, false
		}
		keys = map[string]struct{}{}
		set.Iterate(func(p fieldpath.Path) {
			if len(p) == 3 && fieldNamed(p[0], "metadata") && fieldNamed(p[1], "annotations") &&
				p[2].FieldName != nil {
				keys[*p[2].FieldName] = struct{}{}
			}
		})
		return keys, true
	}
	return nil, false
}

func fieldNamed(e fieldpath.PathElement, name string) bool {
	return e.FieldName != nil && *e.FieldName == name
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
