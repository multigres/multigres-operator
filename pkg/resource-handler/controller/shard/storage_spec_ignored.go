package shard

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

const (
	storageSpecIgnoredReason = "PVCFieldsUnchangeable"
	storageSpecAppliedReason = "NoStorageChangeIgnored"

	// maxIgnoredStorageDetails bounds the condition message. A pool's PVCs
	// usually differ from the spec in the same way, so the first few name the
	// change and the rest are counted.
	maxIgnoredStorageDetails = 5

	// maxIgnoredStorageMessage bounds the message in bytes as well, well under
	// the 32768 the condition schema allows, because a detail quotes spec
	// values (class names, access modes) whose length the CRD does not bound.
	// A message over the schema limit would fail every status write.
	maxIgnoredStorageMessage = 4096
)

// pvcStorageWant is what the spec asks of one PVC. An empty class or nil
// access modes means the spec leaves that field to a default, so any value on
// the PVC satisfies it.
type pvcStorageWant struct {
	request resource.Quantity
	class   string
	modes   []corev1.PersistentVolumeAccessMode
}

// setStorageSpecIgnoredCondition sets ConditionStorageSpecIgnored from the
// shard's live PVCs. The operator never writes a storage request the API
// server would refuse, another storage class or other access modes to an
// existing PVC (see keepUnchangeablePVCFields and expandPVCIfNeeded), so
// without this condition such an edit would be accepted at admission and then
// silently have no effect.
//
// It is derived from the spec and the PVCs rather than from the outcome of an
// apply, so it is current on every status write, including passes that stop
// before the PVCs are reconciled.
func (r *ShardReconciler) setStorageSpecIgnoredCondition(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	ignored, err := r.ignoredStorageChanges(ctx, shard)
	if err != nil {
		return err
	}

	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionStorageSpecIgnored,
		Status:             metav1.ConditionFalse,
		Reason:             storageSpecAppliedReason,
		Message:            "No PVC is keeping a value that differs from the spec",
		ObservedGeneration: shard.Generation,
	}
	if len(ignored) > 0 {
		shown := ignored[:min(len(ignored), maxIgnoredStorageDetails)]
		msg := "Existing PVCs keep values the API server does not allow to change: " +
			strings.Join(shown, "; ")
		if rest := len(ignored) - len(shown); rest > 0 {
			msg += fmt.Sprintf("; and %d more", rest)
		}
		if len(msg) > maxIgnoredStorageMessage {
			msg = strings.ToValidUTF8(msg[:maxIgnoredStorageMessage-len(" (truncated)")], "") +
				" (truncated)"
		}
		cond.Status = metav1.ConditionTrue
		cond.Reason = storageSpecIgnoredReason
		cond.Message = msg
	}

	if !meta.IsStatusConditionTrue(
		shard.Status.Conditions,
		multigresv1alpha1.ConditionStorageSpecIgnored,
	) && cond.Status == metav1.ConditionTrue {
		r.Recorder.Event(
			shard,
			"Warning",
			multigresv1alpha1.ConditionStorageSpecIgnored,
			cond.Message,
		)
	}
	meta.SetStatusCondition(&shard.Status.Conditions, cond)
	return nil
}

// ignoredStorageChanges describes, in PVC name order, every difference between
// a live PVC of the shard and what the spec asks of it that the operator is
// leaving in place. Orphaned and terminating PVCs are not compared: neither is
// in service, and an orphan comes back under the spec of the pod it is reused
// for.
func (r *ShardReconciler) ignoredStorageChanges(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) ([]string, error) {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(
		ctx,
		pvcs,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels{
			metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
			metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
			metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
			metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
		},
	); err != nil {
		return nil, fmt.Errorf("failed to list PVCs for storage spec check: %w", err)
	}
	slices.SortFunc(pvcs.Items, func(a, b corev1.PersistentVolumeClaim) int {
		return strings.Compare(a.Name, b.Name)
	})

	var ignored []string
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if pvcutil.HasOrphanLabel(pvc) || !pvc.DeletionTimestamp.IsZero() {
			continue
		}
		want, ok, err := storageWantFor(shard, pvc)
		if err != nil {
			// A size the builders cannot parse fails the PVC's own step, so
			// it is reported here rather than failing the status write too.
			ignored = append(ignored, fmt.Sprintf("%s: %v", pvc.Name, err))
			continue
		}
		if !ok {
			continue
		}
		details, err := r.describeIgnoredStorage(ctx, pvc, want)
		if err != nil {
			return nil, err
		}
		ignored = append(ignored, details...)
	}
	return ignored, nil
}

// storageRequestFor returns the storage request the operator writes to the
// existing pvc when the spec asks for want, and, when that is not want, why.
//
// The API server refuses a request below the current one, and a larger one
// unless the PVC is bound and its StorageClass allows expansion (the
// PersistentVolumeClaimResize admission plugin). A refused write fails on
// every reconcile and stops everything after it, so in those cases the
// current request is kept and the reason is reported instead.
//
// The floor is the current request rather than status.capacity. Lowering a
// request to recover from a failed expansion is only accepted above the
// capacity, and only on API servers with RecoverVolumeExpansionFailure, so
// writing it could be refused in the same way.
func (r *ShardReconciler) storageRequestFor(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	want resource.Quantity,
) (resource.Quantity, string, error) {
	current := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	switch want.Cmp(current) {
	case 0:
		return want, "", nil
	case -1:
		return current, "a PVC cannot shrink", nil
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return current, "only a bound PVC can be expanded", nil
	}
	className := ptr.Deref(pvc.Spec.StorageClassName, "")
	if className == "" {
		return current, "the PVC has no StorageClass, so it cannot be expanded", nil
	}
	sc := &storagev1.StorageClass{}
	if err := r.Get(ctx, client.ObjectKey{Name: className}, sc); err != nil {
		if apierrors.IsNotFound(err) {
			return current, fmt.Sprintf("StorageClass %q does not exist", className), nil
		}
		return current, "", fmt.Errorf("failed to get StorageClass %q: %w", className, err)
	}
	if !ptr.Deref(sc.AllowVolumeExpansion, false) {
		return current, fmt.Sprintf(
			"StorageClass %q does not allow volume expansion",
			className,
		), nil
	}
	return want, "", nil
}

// storageWantFor resolves which part of the spec pvc is built from. It reports
// false for a PVC the spec no longer describes: a pool removed from the spec,
// or a filesystem backup PVC left behind after a switch to S3.
func storageWantFor(
	shard *multigresv1alpha1.Shard,
	pvc *corev1.PersistentVolumeClaim,
) (pvcStorageWant, bool, error) {
	if poolName := pvc.Labels[metadata.LabelMultigresPool]; poolName != "" {
		poolSpec, ok := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
		if !ok {
			return pvcStorageWant{}, false, nil
		}
		request, err := poolStorageRequest(poolSpec)
		if err != nil {
			return pvcStorageWant{}, false, err
		}
		return pvcStorageWant{
			request: request,
			class:   poolSpec.Storage.Class,
			modes:   poolSpec.Storage.AccessModes,
		}, true, nil
	}

	if pvc.Name != BuildSharedBackupPVCName(shard) ||
		(shard.Spec.Backup != nil && shard.Spec.Backup.Type == multigresv1alpha1.BackupTypeS3) {
		return pvcStorageWant{}, false, nil
	}
	desired, err := BuildSharedBackupPVC(shard, false, nil)
	if err != nil {
		return pvcStorageWant{}, false, err
	}
	want := pvcStorageWant{
		request: desired.Spec.Resources.Requests[corev1.ResourceStorage],
		modes:   desired.Spec.AccessModes,
	}
	if desired.Spec.StorageClassName != nil {
		want.class = *desired.Spec.StorageClassName
	}
	return want, true, nil
}

func (r *ShardReconciler) describeIgnoredStorage(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	want pvcStorageWant,
) ([]string, error) {
	var out []string
	request, reason, err := r.storageRequestFor(ctx, pvc, want.request)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		out = append(out, fmt.Sprintf(
			"%s keeps storage request %s, spec asks for %s (%s)",
			pvc.Name, request.String(), want.request.String(), reason,
		))
	}
	if current := pvc.Spec.StorageClassName; want.class != "" &&
		(current == nil || *current != want.class) {
		out = append(out, fmt.Sprintf(
			"%s keeps storageClassName %q, spec asks for %q",
			pvc.Name, ptr.Deref(current, ""), want.class,
		))
	}
	// Compared as sets: the order of access modes carries no meaning.
	if len(want.modes) > 0 && !sameAccessModes(pvc.Spec.AccessModes, want.modes) {
		out = append(out, fmt.Sprintf(
			"%s keeps accessModes %v, spec asks for %v",
			pvc.Name, pvc.Spec.AccessModes, want.modes,
		))
	}
	return out, nil
}

func sameAccessModes(a, b []corev1.PersistentVolumeAccessMode) bool {
	return slices.Equal(slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b)))
}

// enqueueFromStorageClass wakes the shards that report StorageSpecIgnored and
// have a PVC on the StorageClass that changed. A growth kept because its class
// is missing or does not allow expansion produces no error and no requeue, so
// without this it would wait for an unrelated event after an admin creates the
// class or sets allowVolumeExpansion.
//
// Shards are matched through their live PVCs' class rather than through the
// condition message, which is bounded and so may not name every class.
func (r *ShardReconciler) enqueueFromStorageClass(
	ctx context.Context,
	o client.Object,
) []reconcile.Request {
	logger := log.FromContext(ctx)
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs); err != nil {
		logger.Error(err, "Failed to list PVCs for StorageClass event", "storageClass", o.GetName())
		return nil
	}
	type shardIdentity struct{ namespace, cluster, database, tableGroup, shard string }
	onClass := map[shardIdentity]bool{}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if ptr.Deref(pvc.Spec.StorageClassName, "") != o.GetName() {
			continue
		}
		onClass[shardIdentity{
			namespace:  pvc.Namespace,
			cluster:    pvc.Labels[metadata.LabelMultigresCluster],
			database:   pvc.Labels[metadata.LabelMultigresDatabase],
			tableGroup: pvc.Labels[metadata.LabelMultigresTableGroup],
			shard:      pvc.Labels[metadata.LabelMultigresShard],
		}] = true
	}
	if len(onClass) == 0 {
		return nil
	}

	shards := &multigresv1alpha1.ShardList{}
	if err := r.List(ctx, shards); err != nil {
		logger.Error(
			err,
			"Failed to list Shards for StorageClass event",
			"storageClass",
			o.GetName(),
		)
		return nil
	}
	var requests []reconcile.Request
	for i := range shards.Items {
		shard := &shards.Items[i]
		if !meta.IsStatusConditionTrue(
			shard.Status.Conditions,
			multigresv1alpha1.ConditionStorageSpecIgnored,
		) {
			continue
		}
		if onClass[shardIdentity{
			namespace:  shard.Namespace,
			cluster:    shard.Labels[metadata.LabelMultigresCluster],
			database:   string(shard.Spec.DatabaseName),
			tableGroup: string(shard.Spec.TableGroupName),
			shard:      string(shard.Spec.ShardName),
		}] {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(shard),
			})
		}
	}
	return requests
}
