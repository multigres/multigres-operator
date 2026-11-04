package multigrescluster

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
)

// shardServiceAccountFieldOwner is the server-side-apply field manager a
// cluster's shard ServiceAccount is applied under. It is one per cluster.
//
// Two clusters that name the same account each apply their own controller
// ownerReference. Under one shared manager the second apply replaces the
// first cluster's reference and annotations with its own, the first cluster
// puts them back, and the two rewrite the account at each other for as long
// as both exist. Under separate managers the second apply adds a second
// controller reference, which the API server rejects, so the account stays
// the first cluster's and the second reports the conflict.
func shardServiceAccountFieldOwner(cluster *multigresv1alpha1.MultigresCluster) string {
	return "multigres-operator-" + string(cluster.UID)
}

// errShardServiceAccountControlled marks an account that is controlled by
// something other than this cluster.
var errShardServiceAccountControlled = errors.New(
	"shard service account is controlled by another object")

// accountConflictRecheckInterval is how long a cluster waits before it applies
// again to an account that another object controls and the informer does not
// hold. Nothing announces that such an account was released or deleted, so
// this is the only way the cluster notices; an account the informer holds is
// rechecked on its events instead (see enqueueAccountWaiters).
const accountConflictRecheckInterval = 5 * time.Minute

// accountConflictError is the failure to apply the shard ServiceAccount
// because another object controls it. Nothing the operator can retry changes
// that, so the reconcile does not return it to the work queue's backoff.
type accountConflictError struct {
	// other names the controlling object, as `Kind "name"`.
	other string
	// recheckAt is when to apply again. It is zero for an account the
	// informer holds, whose events start the next attempt.
	recheckAt time.Time
	// cause is what the API server answered, or nil when the informer's
	// copy of the account was enough to tell.
	cause error
}

func (e *accountConflictError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return errShardServiceAccountControlled.Error()
}

func (e *accountConflictError) Is(target error) bool {
	return target == errShardServiceAccountControlled
}

// accountConflict is one refused apply to an account the informer does not
// hold, kept so that the reconciles its cluster's children start in the
// meantime neither apply again nor read the account from the API server.
type accountConflict struct {
	// desired identifies the cluster and what it applies, so that a
	// recreated cluster or an edit to the account's name or annotations is
	// applied at once.
	desired string
	err     *accountConflictError
}

func accountConflictKey(cluster *multigresv1alpha1.MultigresCluster) types.NamespacedName {
	return client.ObjectKeyFromObject(cluster)
}

// desiredAccountIdentity is what accountConflict.desired holds for sa.
func desiredAccountIdentity(
	cluster *multigresv1alpha1.MultigresCluster,
	sa *corev1.ServiceAccount,
) string {
	keys := slices.Sorted(maps.Keys(sa.Annotations))
	parts := make([]string, 0, 2+2*len(keys))
	parts = append(parts, string(cluster.UID), sa.Name)
	for _, k := range keys {
		parts = append(parts, k, sa.Annotations[k])
	}
	return name.Hash(parts)
}

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
	manager := shardServiceAccountFieldOwner(cluster)
	key := accountConflictKey(cluster)
	desired := desiredAccountIdentity(cluster, sa)
	observed := &corev1.ServiceAccount{}
	cached := r.Get(ctx, client.ObjectKeyFromObject(sa), observed) == nil
	if cached {
		r.accountConflicts.Delete(key)
		if shardServiceAccountApplied(observed, sa, manager) {
			return nil
		}
		// The API server refuses the apply below for such an account. The
		// cached read says so without a request per reconcile.
		if other := otherControllerOf(observed, cluster); other != "" {
			return &accountConflictError{other: other}
		}
	} else if held, ok := r.accountConflicts.Load(key); ok {
		// The account is not in the informer, so nothing has reported a
		// change to it since the apply that was refused.
		conflict := held.(accountConflict)
		if conflict.desired == desired && r.now().Before(conflict.err.recheckAt) {
			return conflict.err
		}
	}
	if err := r.Patch(
		ctx,
		sa,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner(manager),
	); err != nil {
		err = fmt.Errorf("failed to apply shard service account %s: %w", sa.Name, err)
		// The informer only holds ServiceAccounts carrying the managed-by
		// label, and an account something else created does not carry it, so
		// who controls it is read from the API server: once per refused
		// apply, and applies to such an account are spaced by
		// accountConflictRecheckInterval.
		other := r.otherController(ctx, cluster, sa.Name)
		if other == "" {
			r.accountConflicts.Delete(key)
			return err
		}
		conflict := &accountConflictError{other: other, cause: err}
		if !cached {
			conflict.recheckAt = r.now().Add(wait.Jitter(accountConflictRecheckInterval, 0.2))
			r.accountConflicts.Store(key, accountConflict{desired: desired, err: conflict})
		}
		return conflict
	}
	r.accountConflicts.Delete(key)
	return nil
}

func (r *MultigresClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// otherControllerOf names the controller owner of sa when it is not cluster.
func otherControllerOf(
	sa *corev1.ServiceAccount,
	cluster *multigresv1alpha1.MultigresCluster,
) string {
	if ref := metav1.GetControllerOf(sa); ref != nil && ref.UID != cluster.UID {
		return fmt.Sprintf("%s %q", ref.Kind, ref.Name)
	}
	return ""
}

// shardServiceAccountApplied reports whether applying desired would leave
// observed as it is. That needs more than equal values: server-side apply
// also removes an annotation the operator applied earlier and no longer
// sets, so the annotations the operator's field manager owns must be exactly
// the desired ones. An account the operator has never applied has no such
// entry and is applied, which is how a pre-existing account is adopted.
func shardServiceAccountApplied(observed, desired *corev1.ServiceAccount, manager string) bool {
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
	owned, ok := appliedAnnotationKeys(observed, manager)
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
func appliedAnnotationKeys(
	sa *corev1.ServiceAccount,
	manager string,
) (keys map[string]struct{}, ok bool) {
	return appliedMetadataKeys(sa, manager, "annotations")
}

// appliedMetadataKeys returns the keys of obj's metadata map named field
// ("labels" or "annotations") that manager owns through server-side apply. ok
// is false when obj has no readable apply entry of that manager.
func appliedMetadataKeys(
	obj client.Object,
	manager, field string,
) (keys map[string]struct{}, ok bool) {
	for _, e := range obj.GetManagedFields() {
		if e.Manager != manager ||
			e.Operation != metav1.ManagedFieldsOperationApply || e.Subresource != "" {
			continue
		}
		set := &fieldpath.Set{}
		if e.FieldsV1 == nil || set.FromJSON(e.FieldsV1.GetRawReader()) != nil {
			return nil, false
		}
		keys = map[string]struct{}{}
		set.Iterate(func(p fieldpath.Path) {
			if len(p) == 3 && fieldNamed(p[0], "metadata") && fieldNamed(p[1], field) &&
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

// syncShardServiceAccount applies the shard ServiceAccount and records the
// outcome in the ShardServiceAccountReady condition, without halting the
// cluster reconcile: a failure is a per-cluster problem that must not stop
// cells, tablegroups and status from converging. The caller continues to the
// cells and decides at the end how the failure is retried: an
// accountConflictError waits for the account to change, any other error goes
// back to the work queue's backoff.
//
// It emits nothing. reportShardServiceAccount does, once the pass knows which
// TableGroups the failure kept from being created.
func (r *MultigresClusterReconciler) syncShardServiceAccount(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) error {
	if cluster.Spec.ShardServiceAccount == nil {
		r.accountConflicts.Delete(accountConflictKey(cluster))
		r.accountReports.Delete(accountConflictKey(cluster))
		meta.RemoveStatusCondition(
			&cluster.Status.Conditions,
			multigresv1alpha1.ConditionShardServiceAccountReady,
		)
		return nil
	}

	err := r.reconcileShardServiceAccount(ctx, cluster)
	if err == nil {
		r.accountReports.Delete(accountConflictKey(cluster))
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               multigresv1alpha1.ConditionShardServiceAccountReady,
			Status:             metav1.ConditionTrue,
			Reason:             multigresv1alpha1.ReasonShardServiceAccountApplied,
			Message:            "Shard ServiceAccount is applied",
			ObservedGeneration: cluster.Generation,
		})
		return nil
	}

	saName := cluster.Spec.ShardServiceAccount.Name
	reason := multigresv1alpha1.ReasonShardServiceAccountApplyFailed
	msg := fmt.Sprintf("Failed to apply shard ServiceAccount %q: %v", saName, err)
	var conflict *accountConflictError
	if errors.As(err, &conflict) {
		reason = multigresv1alpha1.ReasonShardServiceAccountConflict
		msg = fmt.Sprintf(
			"Shard ServiceAccount %q is controlled by %s; remove its controller ownerReference or "+
				"choose another name: %v",
			saName, conflict.other, err,
		)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               multigresv1alpha1.ConditionShardServiceAccountReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: cluster.Generation,
	})
	return err
}

// shardServiceAccountCondition returns a copy of the cluster's
// ShardServiceAccountReady condition, or nil.
func shardServiceAccountCondition(cluster *multigresv1alpha1.MultigresCluster) *metav1.Condition {
	if c := meta.FindStatusCondition(
		cluster.Status.Conditions, multigresv1alpha1.ConditionShardServiceAccountReady,
	); c != nil {
		return c.DeepCopy()
	}
	return nil
}

// reportShardServiceAccount emits the Warning for a shard ServiceAccount that
// could not be applied, and logs it, when the ShardServiceAccountReady
// condition differs from before, the condition the pass started with, and
// from the last one this process reported. A
// cluster in an unchanged failure is reconciled on every status change of its
// children, and would otherwise submit the same Warning each time.
//
// notCreated are the TableGroups the pass did not create because of the
// failure. They are added to the condition's message, so one event names
// them all and a change to the list is reported.
func (r *MultigresClusterReconciler) reportShardServiceAccount(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	before *metav1.Condition,
	notCreated []string,
	saErr error,
) {
	cond := meta.FindStatusCondition(
		cluster.Status.Conditions, multigresv1alpha1.ConditionShardServiceAccountReady)
	if cond == nil || cond.Status != metav1.ConditionFalse {
		return
	}
	if len(notCreated) > 0 {
		slices.Sort(notCreated)
		cond.Message += fmt.Sprintf(
			"; not created until the shard ServiceAccount is applied: TableGroup %s",
			strings.Join(notCreated, ", "))
	}
	// The stored condition says what an earlier pass reported, also after a
	// restart, but the informer delivers it late: the reconcile a child
	// starts right after the first report still reads the cluster without
	// it. So the last report is also kept in memory.
	reported := cond.Reason + "\n" + cond.Message
	key := accountConflictKey(cluster)
	last, _ := r.accountReports.Swap(key, reported)
	if last == reported ||
		(before != nil && before.Status == cond.Status && before.Reason == cond.Reason &&
			before.Message == cond.Message) {
		return
	}
	eventReason := "ShardServiceAccountApplyFailed"
	if cond.Reason == multigresv1alpha1.ReasonShardServiceAccountConflict {
		eventReason = "ShardServiceAccountConflict"
	}
	log.FromContext(ctx).Error(saErr, "Failed to reconcile shard ServiceAccount",
		"serviceAccount", cluster.Spec.ShardServiceAccount.Name)
	r.Recorder.Event(cluster, "Warning", eventReason, cond.Message)
}

// ShardServiceAccountNameIndex is the cache index of MultigresClusters by the
// name of the shard ServiceAccount their spec asks for.
const ShardServiceAccountNameIndex = "spec.shardServiceAccount.name"

// ShardServiceAccountNames is the index function of
// ShardServiceAccountNameIndex.
func ShardServiceAccountNames(obj client.Object) []string {
	cluster, ok := obj.(*multigresv1alpha1.MultigresCluster)
	if !ok || cluster.Spec.ShardServiceAccount == nil ||
		cluster.Spec.ShardServiceAccount.Name == "" {
		return nil
	}
	return []string{cluster.Spec.ShardServiceAccount.Name}
}

// enqueueAccountWaiters maps a ServiceAccount event to the clusters in its
// namespace that name the account as their shard ServiceAccount. The owner
// reference only leads to the cluster that controls the account; a cluster
// refused because something else controls it is waiting for exactly these
// events: the account being released, relabelled or deleted.
//
// The cluster's ShardServiceAccountReady condition is not consulted. The
// account can be released while the pass that found the conflict is still
// running, or before the informer has that pass's status write, and a
// cluster skipped then has no other event to wait for. A cluster whose
// account is applied decides so from the cache and sends nothing.
func (r *MultigresClusterReconciler) enqueueAccountWaiters(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	// Through the index, so that an event costs the clusters that name the
	// account and not a copy of every cluster in the namespace.
	clusters := &multigresv1alpha1.MultigresClusterList{}
	if err := r.List(ctx, clusters, client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{ShardServiceAccountNameIndex: obj.GetName()}); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list clusters waiting for a ServiceAccount",
			"serviceAccount", obj.GetName())
		return nil
	}
	var requests []reconcile.Request
	for i := range clusters.Items {
		requests = append(requests,
			reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&clusters.Items[i])})
	}
	slices.SortFunc(requests, func(a, b reconcile.Request) int {
		return strings.Compare(a.Name, b.Name)
	})
	return requests
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
	return otherControllerOf(sa, cluster)
}
