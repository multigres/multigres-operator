package shard

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

// A PVC that is deleted while a pod mounts it is not removed: the
// kubernetes.io/pvc-protection finalizer holds it, data intact, until the last
// pod using it stops. Then the claim and, under a Delete reclaim policy, its
// volume are gone, and anything that applies or creates a PVC of the same name
// gets a new, empty one. The shard owns every pod that can be holding such a
// PVC, so each pod it deletes or replaces is what decides when the data goes.
//
// pvc-protection counts every pod bound to a node that references the claim,
// whatever the pod's phase: a Failed or Succeeded pod object holds the PVC
// until the pod object itself is deleted.
//
// So while any shard pod references a PVC that is being deleted, the shard
// reports it (condition, Degraded phase, Warning event), starts no voluntary
// disruption, deletes no pod that references such a PVC, terminal or not, and
// creates no pod on one. Once a person deletes the pods the PVC finishes
// deleting and the ordinary create paths make a new, empty one: that deletion
// is the explicit decision to give up the data.

const (
	// stuckDeletionSlack is how long past its deletion deadline a pod may
	// linger before it counts as stuck rather than on its way out.
	stuckDeletionSlack = 2 * time.Minute

	reasonMountedPVCBeingDeleted   = "MountedPVCBeingDeleted"
	reasonNoMountedPVCBeingDeleted = "NoMountedPVCBeingDeleted"
)

// shardSelector matches every Pod and PVC the shard creates: pool pods, the
// Lite pod, pool data PVCs and the shared backup PVC.
func shardSelector(shard *multigresv1alpha1.Shard) client.MatchingLabels {
	return client.MatchingLabels{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}
}

// mountedTerminatingPVCs returns, sorted, the shard's PVCs that are being
// deleted while a shard pod still references them. Pass an uncached reader
// when the answer gates a destructive action.
func mountedTerminatingPVCs(
	ctx context.Context,
	reader client.Reader,
	shard *multigresv1alpha1.Shard,
) ([]string, error) {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(
		ctx, pvcs, client.InNamespace(shard.Namespace), shardSelector(shard),
	); err != nil {
		return nil, fmt.Errorf("list shard PVCs: %w", err)
	}
	if !slices.ContainsFunc(pvcs.Items, func(p corev1.PersistentVolumeClaim) bool {
		return pvcutil.IsTerminating(&p)
	}) {
		return nil, nil
	}
	pods := &corev1.PodList{}
	if err := reader.List(
		ctx, pods, client.InNamespace(shard.Namespace), shardSelector(shard),
	); err != nil {
		return nil, fmt.Errorf("list shard pods: %w", err)
	}
	return terminatingClaimsInUse(pods.Items, pvcs.Items, time.Now()), nil
}

// terminatingClaimsInUse is the pure half of mountedTerminatingPVCs. A pod
// counts in any phase, as pvc-protection counts it. A pod that is itself being
// deleted does not, until it is stuck: whoever deleted it has already let the
// PVC go, and the operator's own quarantine wipe, which deletes a pod and then
// its PVC on purpose, is not news to report. One still present
// stuckDeletionSlack past its deletion deadline (a pod on a lost node, which
// only a force delete removes) counts again, because it is what still holds the
// PVC and someone has to act on it. pvc-protection ignores a pod not yet bound
// to a node; counting one here only reports a PVC that is about to finish
// deleting anyway, and the condition clears once it has.
func terminatingClaimsInUse(
	pods []corev1.Pod,
	pvcs []corev1.PersistentVolumeClaim,
	now time.Time,
) []string {
	terminating := map[string]bool{}
	for i := range pvcs {
		if pvcutil.IsTerminating(&pvcs[i]) {
			terminating[pvcs[i].Name] = true
		}
	}
	var names []string
	for i := range pods {
		pod := &pods[i]
		if !pod.DeletionTimestamp.IsZero() &&
			now.Before(pod.DeletionTimestamp.Add(stuckDeletionSlack)) {
			continue
		}
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			name := vol.PersistentVolumeClaim.ClaimName
			if terminating[name] && !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	return names
}

// podTerminatingClaim returns the first PVC pod references that is being
// deleted, or "". Every pod delete in the shard asks this first, since
// deleting such a pod may be what lets the PVC, and its data, go; pod creates
// go through claimPodVolumes. Pass an uncached reader.
func podTerminatingClaim(
	ctx context.Context,
	reader client.Reader,
	pod *corev1.Pod,
) (string, error) {
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}
		pvc := &corev1.PersistentVolumeClaim{}
		err := reader.Get(ctx, client.ObjectKey{
			Namespace: pod.Namespace,
			Name:      vol.PersistentVolumeClaim.ClaimName,
		}, pvc)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("get PVC %s: %w", vol.PersistentVolumeClaim.ClaimName, err)
		}
		if pvcutil.IsTerminating(pvc) {
			return pvc.Name, nil
		}
	}
	return "", nil
}

// claimPodVolumes readies every PVC pod references for a pod about to be
// created on it, reading each uncached. It returns the first one being
// deleted, in which case the pod must not be created, and clears the orphan
// mark from any that carry it.
//
// This is the reclaim that counts. The PVC-level paths reclaim from what the
// cache shows, and a cache that has not seen a PVC (its Create then returns
// AlreadyExists) or not seen its orphan mark skips them; a pod started on such
// a PVC would leave multigres-gc's Delete, conditioned on the resourceVersion
// it listed, still able to succeed. Clearing the mark here, under the
// resourceVersion just read, is what makes that Delete fail.
func (r *ShardReconciler) claimPodVolumes(
	ctx context.Context,
	pod *corev1.Pod,
) (terminating string, err error) {
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}
		pvc := &corev1.PersistentVolumeClaim{}
		err := r.disruptionReader().Get(ctx, client.ObjectKey{
			Namespace: pod.Namespace,
			Name:      vol.PersistentVolumeClaim.ClaimName,
		}, pvc)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("get PVC %s: %w", vol.PersistentVolumeClaim.ClaimName, err)
		}
		if pvcutil.IsTerminating(pvc) {
			return pvc.Name, nil
		}
		if err := pvcutil.ClearOrphan(ctx, log.FromContext(ctx), r.Client, pvc); err != nil {
			return "", fmt.Errorf("reclaim PVC %s: %w", pvc.Name, err)
		}
	}
	return "", nil
}

// reclaimPVC puts a PVC that a previous scale-down or Shard incarnation
// marked orphan back into service by clearing the mark. It re-reads the PVC
// uncached first, so the resourceVersion pvcutil.ClearOrphan conditions its
// patch on is current and a Conflict means a real race with multigres-gc,
// not a lagging cache. pvc is updated in place to what was read and written.
// It reports terminating, with no error, when that read finds the PVC being
// deleted after all, so callers treat it as they treat a PVC their own read
// already showed as being deleted.
func (r *ShardReconciler) reclaimPVC(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) (terminating bool, err error) {
	if !pvcutil.HasOrphanLabel(pvc) {
		return false, nil
	}
	fresh := &corev1.PersistentVolumeClaim{}
	if err := r.disruptionReader().Get(ctx, client.ObjectKeyFromObject(pvc), fresh); err != nil {
		return false, fmt.Errorf("get PVC %s: %w", pvc.Name, err)
	}
	*pvc = *fresh
	if pvcutil.IsTerminating(fresh) {
		return true, nil
	}
	return false, pvcutil.ClearOrphan(ctx, log.FromContext(ctx), r.Client, pvc)
}

// enqueueShardForPVC maps a PVC to the Shard whose labels it carries. Owns()
// only maps PVCs with the Shard's owner reference; one under a Retain policy,
// or one orphaned and then reclaimed, has none, and its deletion would
// otherwise go unnoticed until an unrelated event.
func (r *ShardReconciler) enqueueShardForPVC(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	lbls := obj.GetLabels()
	shardName := lbls[metadata.LabelMultigresShard]
	if shardName == "" {
		return nil
	}
	shards := &multigresv1alpha1.ShardList{}
	if err := r.List(ctx, shards, client.InNamespace(obj.GetNamespace()), client.MatchingLabels{
		metadata.LabelMultigresCluster:    lbls[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   lbls[metadata.LabelMultigresDatabase],
		metadata.LabelMultigresTableGroup: lbls[metadata.LabelMultigresTableGroup],
		metadata.LabelMultigresShard:      shardName,
	}); err != nil {
		log.FromContext(ctx).Error(err, "Failed to map PVC to Shard", "pvc", obj.GetName())
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(shards.Items))
	for i := range shards.Items {
		reqs = append(reqs, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&shards.Items[i]),
		})
	}
	return reqs
}

func mountedPVCBeingDeletedMessage(names []string) string {
	return fmt.Sprintf(
		"PVCs being deleted while shard pods still use them: %s. Each one's "+
			"data is released when the last pod using it is deleted, and the PVC "+
			"is then recreated empty. The operator will not start a drain, "+
			"delete, replace or add pods that use them. To keep the data, copy it "+
			"off or set the bound PersistentVolume's reclaim policy to Retain, "+
			"then delete the pods that use them",
		strings.Join(names, ", "),
	)
}

// setPVCTerminatingCondition records names (from mountedTerminatingPVCs) in
// the PVCTerminating condition and emits the matching events. It returns
// whether the shard has a mounted PVC being deleted.
func (r *ShardReconciler) setPVCTerminatingCondition(
	shard *multigresv1alpha1.Shard,
	names []string,
) bool {
	previous := meta.FindStatusCondition(
		shard.Status.Conditions,
		multigresv1alpha1.ConditionPVCTerminating,
	)
	cond := metav1.Condition{
		Type:               multigresv1alpha1.ConditionPVCTerminating,
		Status:             metav1.ConditionFalse,
		Reason:             reasonNoMountedPVCBeingDeleted,
		Message:            "No shard pod mounts a PVC that is being deleted",
		ObservedGeneration: shard.Generation,
	}
	wasTrue := previous != nil && previous.Status == metav1.ConditionTrue
	// Events go out on a change only. The recorder rate-limits events per
	// object, so one per reconcile would crowd out every other event this
	// shard emits.
	if len(names) > 0 {
		cond.Status = metav1.ConditionTrue
		cond.Reason = reasonMountedPVCBeingDeleted
		cond.Message = mountedPVCBeingDeletedMessage(names)
		if !wasTrue || previous.Message != cond.Message {
			r.Recorder.Event(
				shard,
				corev1.EventTypeWarning,
				reasonMountedPVCBeingDeleted,
				cond.Message,
			)
		}
	} else if wasTrue {
		r.Recorder.Eventf(
			shard,
			corev1.EventTypeWarning,
			"MountedPVCReleased",
			"No shard pod mounts a PVC that is being deleted any more. Each such "+
				"PVC is recreated empty once it is gone. Previously: %s",
			previous.Message,
		)
	}
	meta.SetStatusCondition(&shard.Status.Conditions, cond)
	return len(names) > 0
}
