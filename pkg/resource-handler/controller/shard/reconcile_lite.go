package shard

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	// liteTopoTimeout bounds the topology write reconcileLite makes, matching
	// the deadline the data-plane phase puts on its topology connection.
	liteTopoTimeout = 30 * time.Second

	// litePrimaryRequeue is how often a Lite shard is reconciled while its
	// pooler does not report PRIMARY. The role is read over RPC, and a change
	// to it produces no Kubernetes event.
	litePrimaryRequeue = 10 * time.Second

	// Retained PVCs have no owner reference, so their CSI status changes do
	// not enqueue the shard. Poll until the requested expansion completes.
	liteStorageRequeue = 10 * time.Second
)

// reconcileLite reconciles a Lite shard after the prelude every shard runs. A
// Lite shard is one pod running minigres and pgctld, plus its data PVC.
// minigres serves the cell's gateway and the shard's only pooler as a static
// leader. The rest of Reconcile does not run for it: there is no multiorch,
// PodDisruptionBudget, headless Service, rolling update, drain or data-plane
// phase. A pod whose spec drifts is deleted and created again.
func (r *ShardReconciler) reconcileLite(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rendered renderedConfig,
	storageClasses storageClassCheck,
) (ctrl.Result, error) {
	ctx, span := monitoring.StartChildSpan(ctx, "Shard.ReconcileLite")
	defer span.End()
	logger := log.FromContext(ctx)

	poolName, cellName, poolSpec, err := litePool(shard)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(shard, "Warning", "ConfigError", "%v", err)
		return ctrl.Result{}, err
	}

	// The pod mounts the shared backup PVC when backups go to a filesystem,
	// and the rendered postgresql.conf, so both exist before the pod does.
	if err := r.reconcileSharedBackupPVC(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(
			shard,
			"Warning",
			"FailedApply",
			"Failed to reconcile shared backup PVC: %v",
			err,
		)
		return ctrl.Result{}, err
	}
	if isMissingStorageClassDependency(storageClasses.poolDependency) {
		r.Recorder.Event(shard, "Warning", storageClassNotFoundReason, storageClasses.message)
		logger.Info(
			"StorageClass dependency missing for the Lite pod; requeueing",
			"after",
			storageClassDependencyRequeue,
		)
		return ctrl.Result{RequeueAfter: storageClassDependencyRequeue}, nil
	}
	if err := r.reconcilePostgresConfig(ctx, shard, rendered); err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ConfigError",
			"Failed to reconcile postgres config: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// The pooler reads its backup location from the database record when it
	// bootstraps, so the record is registered before the pod is created.
	registered, err := r.registerLiteDatabase(ctx, shard)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		return ctrl.Result{}, err
	}
	if !registered {
		return ctrl.Result{RequeueAfter: topoUnavailableRequeueDelay}, nil
	}

	pvcWait, err := r.ensureLiteDataPVC(ctx, shard, poolName, cellName, poolSpec)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(shard, "Warning", "FailedApply", "Failed to reconcile data PVC: %v", err)
		return ctrl.Result{}, err
	}

	if err := r.ensureLitePod(ctx, shard, poolName, cellName, poolSpec); err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(shard, "Warning", "FailedApply", "Failed to reconcile Lite pod: %v", err)
		return ctrl.Result{}, err
	}

	wait, err := r.reconcileLiteReload(ctx, shard, rendered)
	if err != nil {
		return ctrl.Result{}, err
	}
	if wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}

	if !meta.IsStatusConditionTrue(shard.Status.Conditions, multigresv1alpha1.ConditionPrimary) {
		return ctrl.Result{RequeueAfter: litePrimaryRequeue}, nil
	}
	return ctrl.Result{RequeueAfter: pvcWait}, nil
}

// litePool returns the pool and cell of a Lite shard. Admission allows exactly
// one pool with exactly one cell in Lite mode.
func litePool(
	shard *multigresv1alpha1.Shard,
) (poolName, cellName string, poolSpec multigresv1alpha1.PoolSpec, err error) {
	if len(shard.Spec.Pools) != 1 {
		return "", "", multigresv1alpha1.PoolSpec{}, fmt.Errorf(
			"lite shard %s has %d pools, want 1",
			shard.Name,
			len(shard.Spec.Pools),
		)
	}
	for name, pool := range shard.Spec.Pools {
		poolName, poolSpec = string(name), pool
	}
	if len(poolSpec.Cells) != 1 {
		return "", "", multigresv1alpha1.PoolSpec{}, fmt.Errorf(
			"lite pool %s has %d cells, want 1",
			poolName,
			len(poolSpec.Cells),
		)
	}
	return poolName, string(poolSpec.Cells[0]), poolSpec, nil
}

// registerLiteDatabase writes the shard's database record to the global
// topology with RegisterDatabase: its cells, the backup location from
// spec.backup and the durability policy. It reports false when the topology
// server cannot be reached yet, so the caller requeues as the data-plane phase
// does.
func (r *ShardReconciler) registerLiteDatabase(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (bool, error) {
	logger := log.FromContext(ctx)
	ctx, cancel := context.WithTimeout(ctx, liteTopoTimeout)
	defer cancel()

	store, err := r.topoStore(ctx, shard)
	if err != nil {
		if !topo.IsTopoUnavailable(err) {
			r.Recorder.Eventf(shard, "Warning", "TopologyError",
				"Failed to connect to topology store: %v", err)
		}
		logger.Error(err, "Failed to get topo store, cannot register database")
		return false, nil
	}
	defer func() { _ = store.Close() }()

	if err := topo.RegisterDatabase(ctx, store, r.Recorder, shard); err != nil {
		if topo.IsTopoUnavailable(err) {
			logger.Info("Topology server unavailable; requeueing", "error", err.Error())
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ensureLiteDataPVC creates or expands the Lite pod's data PVC. It never
// deletes it: removal follows the PVC deletion policy when the Shard is deleted.
// It requests a retry until the CSI driver reports the requested capacity and
// clears any pending filesystem resize, including for retained PVCs.
func (r *ShardReconciler) ensureLiteDataPVC(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) (time.Duration, error) {
	name := BuildPoolDataPVCName(shard, poolName, cellName, 0)
	existing := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, client.ObjectKey{Namespace: shard.Namespace, Name: name}, existing)
	if err == nil {
		if err := r.expandPVCIfNeeded(ctx, shard, existing, poolSpec); err != nil {
			return 0, err
		}
		capacity := existing.Status.Capacity.Storage()
		requested := existing.Spec.Resources.Requests.Storage()
		pendingResize := pvcNeedsFilesystemResize(
			map[string]*corev1.PersistentVolumeClaim{name: existing},
			name,
		)
		if capacity.Cmp(*requested) < 0 || pendingResize {
			return liteStorageRequeue, nil
		}
		return 0, nil
	}
	if !apierrors.IsNotFound(err) {
		return 0, fmt.Errorf("failed to get PVC %s: %w", name, err)
	}

	pvc, err := BuildPoolDataPVC(
		shard,
		poolName,
		cellName,
		poolSpec,
		0,
		ShouldDeletePVCOnShardRemoval(shard, poolSpec),
		r.Scheme,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to build PVC %s: %w", name, err)
	}
	// Only one pod may use the data volume. ReadWriteOncePod has the kubelet
	// enforce that across nodes, so a replacement pod cannot start while the
	// old one still holds the volume.
	if len(poolSpec.Storage.AccessModes) == 0 {
		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
	}
	if err := r.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return 0, fmt.Errorf("failed to create PVC %s: %w", name, err)
	}
	log.FromContext(ctx).Info("Created Lite data PVC", "pvc", name)
	return liteStorageRequeue, nil
}

// ensureLitePod creates the Lite pod when it is missing and deletes it when it
// is terminal, its spec-hash no longer matches, or its data PVC requires a
// filesystem resize. The next reconcile creates it again. There is no
// disruption preflight and no drain: the pod is the shard's only pooler, so
// there is no cohort to protect.
func (r *ShardReconciler) ensureLitePod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	logger := log.FromContext(ctx)

	desired, err := r.buildLitePod(shard, poolName, cellName, poolSpec)
	if err != nil {
		return fmt.Errorf("failed to build Lite pod: %w", err)
	}

	existing := &corev1.Pod{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create pod %s: %w", desired.Name, err)
		}
		logger.Info("Created Lite pod", "pod", desired.Name)
		r.Recorder.Eventf(shard, "Normal", "PodCreated", "Created pod %s", desired.Name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get pod %s: %w", desired.Name, err)
	}

	// A pod that is already terminating is replaced once it is gone; its
	// deletion requeues the shard.
	if !existing.DeletionTimestamp.IsZero() {
		return nil
	}
	reason := "apply a spec change"
	if existing.Status.Phase == corev1.PodFailed || existing.Status.Phase == corev1.PodSucceeded {
		reason = "replace a terminal pod"
	} else if existing.Annotations[metadata.AnnotationSpecHash] ==
		desired.Annotations[metadata.AnnotationSpecHash] {
		// Wait for a replacement to finish mounting its volume before another
		// resize restart, as the regular pool path waits for healthy pods.
		if !isPodReady(existing) {
			return nil
		}
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(
			ctx,
			client.ObjectKey{Namespace: shard.Namespace, Name: pvcName},
			pvc,
		); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get PVC %s for filesystem resize: %w", pvcName, err)
		}
		if !pvcNeedsFilesystemResize(
			map[string]*corev1.PersistentVolumeClaim{pvcName: pvc},
			pvcName,
		) {
			return nil
		}
		reason = "complete data PVC filesystem expansion"
	}

	logger.Info(
		"Deleting Lite pod",
		"reason", reason,
		"pod", existing.Name,
		"currentHash", existing.Annotations[metadata.AnnotationSpecHash],
		"desiredHash", desired.Annotations[metadata.AnnotationSpecHash],
	)
	if err := r.Delete(
		ctx,
		existing,
		client.Preconditions{UID: &existing.UID},
	); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete pod %s: %w", existing.Name, err)
	}
	r.Recorder.Eventf(
		shard,
		"Normal",
		"PodReplaced",
		"Deleted pod %s to %s",
		existing.Name,
		reason,
	)
	return nil
}

// reconcileLiteReload uses the same verified ReloadConfig path as a full shard,
// without running the other data-plane phases that require a replica cohort.
func (r *ShardReconciler) reconcileLiteReload(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	rendered renderedConfig,
) (time.Duration, error) {
	if rendered.reloadHash == "" || rendered.err != nil {
		return 0, nil
	}
	if r.PoolerClients == nil {
		return reloadRetryDelay, nil
	}
	ctx, cancel := context.WithTimeout(ctx, liteTopoTimeout)
	defer cancel()
	rpc, err := r.PoolerClients.ClientFor(ctx, shard)
	if err != nil || rpc == nil {
		log.FromContext(ctx).Info("Waiting for the Lite reload RPC client", "error", err)
		return reloadRetryDelay, nil
	}
	store, err := r.topoStore(ctx, shard)
	if err != nil {
		if topo.IsTopoUnavailable(err) {
			return reloadRetryDelay, nil
		}
		return 0, fmt.Errorf("open topology for Lite config reload: %w", err)
	}
	defer func() { _ = store.Close() }()
	return r.reconcileReloadState(ctx, store, shard, rendered, rpc)
}
