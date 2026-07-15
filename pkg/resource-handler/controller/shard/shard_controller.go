package shard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

const (
	// topoUnavailableRequeueDelay is the delay before retrying when the topology
	// server is unavailable during the grace period.
	topoUnavailableRequeueDelay = 5 * time.Second

	// shardFinalizer gates Shard deletion until the operator has stripped its
	// ownerRef from associated PVCs and labelled them orphan, so the downstream
	// multigres-gc cronjob can clean them up instead of k8s cascade-GC
	// nuking them the moment the Shard CR is deleted.
	shardFinalizer = "multigres.com/shard-pvc-orphan"

	// pvcOrphanReplicasThreshold is the number of pool pod PVCs that are
	// kept (orphaned, deferred to the multigres-gc cronjob) rather than deleted
	// in-line when a pod is scaled down, drained, or the shard is removed.
	// Keeping a few volumes around lets an accidental scale-down/removal be
	// rolled back, beyond the threshold the excess is deleted immediately.
	pvcOrphanReplicasThreshold = 3
)

// orphanByRemainingCount decides between orphaning and in-line deletion based
// on how many sibling PVCs currently exist.
//
// liveCount includes the PVC being cleaned up. After it is removed, liveCount-1
// PVCs remain: if that is still >= pvcOrphanReplicasThreshold we have plenty of
// volumes left, so this one is excess and is hard-deleted, otherwise we keep it
// as an orphan so the data can be recovered.
func orphanByRemainingCount(liveCount int) bool {
	return liveCount-1 < pvcOrphanReplicasThreshold
}

// ShardReconciler reconciles a Shard object.
type ShardReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// APIReader is an uncached client that reads directly from the API server.
	// The default cached client (r.Get) only sees Secrets labeled with
	// "app.kubernetes.io/managed-by: multigres-operator" due to the informer
	// cache's label filter. External Secrets (e.g., cert-manager) lack this
	// label, so we need APIReader to validate user-provided pgBackRest TLS Secrets
	// and external postgres password Secrets.
	APIReader       client.Reader
	RPCClient       rpcclient.MultipoolerClient
	CreateTopoStore func(*multigresv1alpha1.Shard) (topoclient.Store, error)
}

// Reconcile manages pool pods, PVCs, services, and data-plane topology for a Shard.
func (r *ShardReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	start := time.Now()
	ctx, span := monitoring.StartReconcileSpan(
		ctx,
		"Shard.Reconcile",
		req.Name,
		req.Namespace,
		"Shard",
	)
	defer span.End()
	ctx = monitoring.EnrichLoggerWithTrace(ctx)

	logger := log.FromContext(ctx)
	logger.V(1).Info("Reconcile started for shard", "shard", req.Name)

	// Fetch the Shard instance
	shard := &multigresv1alpha1.Shard{}
	if err := r.Get(ctx, req.NamespacedName, shard); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Shard resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to get Shard")
		return ctrl.Result{}, err
	}

	// Finalizer gates deletion so PVCs can be detached + labelled orphan
	// before Kubernetes cascade-GC fires.
	if !shard.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, shard)
	}
	if !slices.Contains(shard.Finalizers, shardFinalizer) {
		patch := client.MergeFrom(shard.DeepCopy())
		shard.Finalizers = append(shard.Finalizers, shardFinalizer)
		if err := r.Patch(ctx, shard, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add shard finalizer: %w", err)
		}
	}

	// Handle graceful deletion via PendingDeletion annotation.
	// The TableGroup controller sets this annotation when a shard is removed from
	// spec. The shard controller drains all pods and sets ReadyForDeletion
	// condition, at which point the TableGroup controller calls Delete.
	if shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" {
		return r.handlePendingDeletion(ctx, shard)
	}

	// Update status early so observedGeneration and pod-health phase are
	// always current. Later steps (especially reconcileDataPlane) may block
	// on the topo connection for an extended period; placing updateStatus
	// here guarantees the status subresource is written every reconcile.
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Shard.UpdateStatus")
		if err := r.updateStatus(ctx, shard); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to update status")
			r.Recorder.Eventf(shard, "Warning", "StatusError", "Failed to update status: %v", err)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Reconcile pg_hba ConfigMap first (required by all pools before starting)
	if err := r.reconcilePgHbaConfigMap(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile pg_hba ConfigMap")
		r.Recorder.Eventf(shard, "Warning", "ConfigError", "Failed to generate pg_hba: %v", err)
		return ctrl.Result{}, err
	}

	// Reconcile postgres password Secret (required by pgctld and multipooler)
	if err := r.reconcilePostgresPasswordSecret(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile postgres password Secret")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ConfigError",
			"Failed to reconcile postgres password Secret: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Reconcile pgBackRest TLS certificates (required for inter-node backup communication)
	if err := r.reconcilePgBackRestCerts(ctx, shard); err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to reconcile pgBackRest TLS certificates")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"CertError",
			"Failed to reconcile pgBackRest TLS certificates: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Compute Multiorch cells
	multiorchCells, err := getMultiorchCells(shard)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to determine Multiorch cells")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"ConfigError",
			"Failed to determine Multiorch cells: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Compute pool cells for shared backup PVCs (only cells with pool pods need backup storage)
	poolCells := getPoolCells(shard)

	if err := r.validateBackupStorageClassDependency(ctx, shard); err != nil {
		if isMissingStorageClassDependency(err) {
			logger.Info(
				"StorageClass dependency missing for shared backup PVC; requeueing",
				"after",
				storageClassDependencyRequeue,
			)
			return ctrl.Result{RequeueAfter: storageClassDependencyRequeue}, nil
		}
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to validate backup StorageClass")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"FailedApply",
			"Failed to validate backup StorageClass: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Reconcile Multiorch - one Deployment and Service per cell
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileMultiorch")
		for _, cell := range multiorchCells {
			cellName := string(cell)

			// Reconcile Multiorch Deployment for this cell
			if err := r.reconcileMultiorchDeployment(ctx, shard, cellName); err != nil {
				monitoring.RecordSpanError(childSpan, err)
				childSpan.End()
				logger.Error(err, "Failed to reconcile Multiorch Deployment", "cell", cellName)
				r.Recorder.Eventf(
					shard,
					"Warning",
					"FailedApply",
					"Failed to supply Multiorch Deployment for cell %s: %v",
					cellName,
					err,
				)
				return ctrl.Result{}, err
			}

			// Reconcile Multiorch Service for this cell
			if err := r.reconcileMultiorchService(ctx, shard, cellName); err != nil {
				monitoring.RecordSpanError(childSpan, err)
				childSpan.End()
				logger.Error(err, "Failed to reconcile Multiorch Service", "cell", cellName)
				r.Recorder.Eventf(
					shard,
					"Warning",
					"FailedApply",
					"Failed to supply Multiorch Service for cell %s: %v",
					cellName,
					err,
				)
				return ctrl.Result{}, err
			}
		}
		childSpan.End()
	}

	// Reconcile Shared Backup PVCs (one per cell)
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcileBackupPVCs")
		for _, cell := range poolCells {
			cellName := string(cell)
			if err := r.reconcileSharedBackupPVC(ctx, shard, cellName); err != nil {
				monitoring.RecordSpanError(childSpan, err)
				childSpan.End()
				logger.Error(err, "Failed to reconcile shared backup PVC", "cell", cellName)
				r.Recorder.Eventf(
					shard,
					"Warning",
					"FailedApply",
					"Failed to reconcile shared backup PVC for cell %s: %v",
					cellName,
					err,
				)
				return ctrl.Result{}, err
			}
		}
		childSpan.End()
	}

	if err := r.validatePoolStorageClassDependencies(ctx, shard); err != nil {
		if isMissingStorageClassDependency(err) {
			logger.Info(
				"StorageClass dependency missing for pool resources; requeueing",
				"after",
				storageClassDependencyRequeue,
			)
			return ctrl.Result{RequeueAfter: storageClassDependencyRequeue}, nil
		}
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to validate pool StorageClass dependencies")
		r.Recorder.Eventf(
			shard,
			"Warning",
			"FailedApply",
			"Failed to validate pool StorageClass dependencies: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// Compute postgres config hash for rolling update detection.
	// The hash is set as an in-memory annotation on the shard so that
	// BuildPoolPod can propagate it to each pod without extra parameters.
	if shard.Spec.PostgresConfigRef != nil {
		configHash, err := r.computePostgresConfigHash(ctx, shard)
		if err != nil {
			logger.Error(err, "Failed to compute postgres config hash")
			r.Recorder.Eventf(
				shard,
				"Warning",
				"ConfigError",
				"Failed to read postgres config ConfigMap %q: %v",
				shard.Spec.PostgresConfigRef.Name,
				err,
			)
			return ctrl.Result{}, err
		}
		if shard.Annotations == nil {
			shard.Annotations = make(map[string]string)
		}
		shard.Annotations[metadata.AnnotationPostgresConfigHash] = configHash
	}

	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcilePools")
		for poolName, pool := range shard.Spec.Pools {
			if err := r.reconcilePool(ctx, shard, string(poolName), pool); err != nil {
				monitoring.RecordSpanError(childSpan, err)
				childSpan.End()
				logger.Error(err, "Failed to reconcile pool", "poolName", poolName)
				r.Recorder.Eventf(
					shard,
					"Warning",
					"FailedApply",
					"Failed to reconcile pool %s: %v",
					poolName,
					err,
				)
				return ctrl.Result{}, err
			}
		}
		childSpan.End()
	}

	// Ensure PVC ownerRefs match the current PVCDeletionPolicy.
	// This handles mid-lifecycle policy changes (e.g., Retain → Delete).
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Shard.ReconcilePVCOwnerRefs")
		if err := r.reconcilePVCOwnerRefs(ctx, shard); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile PVC ownerRefs")
			r.Recorder.Eventf(
				shard,
				"Warning",
				"PVCOwnerRefError",
				"Failed to reconcile PVC ownerRefs: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Data-plane phases: open a single topo connection shared across all phases.
	// Use a deadline so a hanging topo dial cannot block the reconcile
	// goroutine indefinitely, which would prevent future reconciles for
	// this shard and leave observedGeneration stale.
	dataPlaneCtx, dataPlaneCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dataPlaneCancel()
	result, err := r.reconcileDataPlane(dataPlaneCtx, shard)
	if err != nil || result.RequeueAfter > 0 {
		return result, err
	}

	logger.V(1).Info("reconcile complete", "duration", time.Since(start).String())
	r.Recorder.Event(shard, "Normal", "Synced", "Successfully reconciled Shard")
	return ctrl.Result{}, nil
}

// reconcilePool creates or updates the Pods, PVCs and headless Service for a pool.
// For pools spanning multiple cells, this creates resources per cell.
func (r *ShardReconciler) reconcilePool(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName string,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	// Pools must have cells specified
	if len(poolSpec.Cells) == 0 {
		return fmt.Errorf(
			"pool %s has no cells specified - cannot deploy without cell information",
			poolName,
		)
	}

	// Create Pods and PVCs per cell
	for _, cell := range poolSpec.Cells {
		cellName := string(cell)

		// Reconcile pool Pods and PVCs for this cell
		if err := r.reconcilePoolPods(ctx, shard, poolName, cellName, poolSpec); err != nil {
			return fmt.Errorf("failed to reconcile pool pods for cell %s: %w", cellName, err)
		}

		// Reconcile pool PDB for this cell
		if err := r.reconcilePoolPDB(ctx, shard, poolName, cellName); err != nil {
			return fmt.Errorf("failed to reconcile pool PDB for cell %s: %w", cellName, err)
		}

		// Reconcile pool headless Service for this cell
		if err := r.reconcilePoolHeadlessService(
			ctx,
			shard,
			poolName,
			cellName,
			poolSpec,
		); err != nil {
			return fmt.Errorf(
				"failed to reconcile pool headless Service for cell %s: %w",
				cellName,
				err,
			)
		}
	}

	return nil
}

// getMultiorchCells returns the list of cells where Multiorch should be deployed.
// If Multiorch.Cells is specified, it uses that.
// Otherwise, it infers cells from all pools (union of pool cells).
func getMultiorchCells(shard *multigresv1alpha1.Shard) ([]multigresv1alpha1.CellName, error) {
	cells := shard.Spec.Multiorch.Cells

	// If Multiorch specifies cells explicitly, use them
	if len(cells) > 0 {
		return cells, nil
	}

	// Otherwise, collect unique cells from all pools
	cellSet := make(map[multigresv1alpha1.CellName]bool)
	for _, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			cellSet[cell] = true
		}
	}

	// Convert set to slice
	cells = make([]multigresv1alpha1.CellName, 0, len(cellSet))
	for cell := range cellSet {
		cells = append(cells, cell)
	}

	// If still no cells found, error
	if len(cells) == 0 {
		return nil, fmt.Errorf(
			"multiorch has no cells specified and no cells found in pools - cannot deploy without cell information",
		)
	}

	slices.Sort(cells)
	return cells, nil
}

// getPoolCells returns the deduplicated, sorted set of cells from all pools.
// Used for infrastructure that only needs to exist where pool pods run
// (e.g., shared backup PVCs).
func getPoolCells(shard *multigresv1alpha1.Shard) []multigresv1alpha1.CellName {
	cellSet := make(map[multigresv1alpha1.CellName]bool)
	for _, pool := range shard.Spec.Pools {
		for _, cell := range pool.Cells {
			cellSet[cell] = true
		}
	}

	cells := make([]multigresv1alpha1.CellName, 0, len(cellSet))
	for cell := range cellSet {
		cells = append(cells, cell)
	}

	slices.Sort(cells)
	return cells
}

// ShouldDeletePVCOnShardRemoval returns true when the effective PVCDeletionPolicy
// for a pool resolves to Delete. Used by PVC builders to conditionally set
// a controller ownerRef so Kubernetes GC cascade-deletes the PVC with the Shard.
func ShouldDeletePVCOnShardRemoval(
	shard *multigresv1alpha1.Shard,
	poolSpec multigresv1alpha1.PoolSpec,
) bool {
	policy := multigresv1alpha1.MergePVCDeletionPolicy(
		poolSpec.PVCDeletionPolicy,
		shard.Spec.PVCDeletionPolicy,
	)
	return policy != nil && policy.WhenDeleted == multigresv1alpha1.DeletePVCRetentionPolicy
}

// ShouldDeleteShardLevelPVCOnRemoval returns true when the shard-level
// PVCDeletionPolicy resolves to Delete. Used for shared infrastructure PVCs
// (e.g., backup PVCs) that are not pool-specific.
func ShouldDeleteShardLevelPVCOnRemoval(shard *multigresv1alpha1.Shard) bool {
	p := shard.Spec.PVCDeletionPolicy
	return p != nil && p.WhenDeleted == multigresv1alpha1.DeletePVCRetentionPolicy
}

// reconcilePVCOwnerRefs ensures ownerRefs on existing PVCs match the current
// PVCDeletionPolicy. When policy is Delete, a controller ownerRef is added so
// Kubernetes GC cascade-deletes PVCs with the Shard. When policy is Retain,
// any existing controller ownerRef is removed so PVCs survive Shard deletion.
func (r *ShardReconciler) reconcilePVCOwnerRefs(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) error {
	logger := log.FromContext(ctx)

	selector := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}

	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(
		ctx,
		pvcList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return fmt.Errorf("failed to list PVCs for ownerRef reconciliation: %w", err)
	}

	shardUID := shard.GetUID()

	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]

		// Skip PVCs already marked orphan. Their Shard ownerRef was deliberately
		// stripped so the multigres-gc cronjob owns the delayed deletion.
		if pvcutil.HasOrphanLabel(pvc) {
			continue
		}

		// Resolve effective policy for this PVC.
		poolName := pvc.Labels[metadata.LabelMultigresPool]
		wantOwnerRef := false

		if poolName != "" {
			if poolSpec, exists := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]; exists {
				wantOwnerRef = ShouldDeletePVCOnShardRemoval(shard, poolSpec)
			} else {
				// Pool removed from spec — fall back to shard-level policy.
				wantOwnerRef = ShouldDeleteShardLevelPVCOnRemoval(shard)
			}
		} else {
			// Shared backup PVC — use shard-level policy.
			wantOwnerRef = ShouldDeleteShardLevelPVCOnRemoval(shard)
		}

		hasOwnerRef := false
		for _, ref := range pvc.OwnerReferences {
			if ref.UID == shardUID {
				hasOwnerRef = true
				break
			}
		}

		if wantOwnerRef && !hasOwnerRef {
			patch := client.MergeFrom(pvc.DeepCopy())
			if err := ctrl.SetControllerReference(shard, pvc, r.Scheme); err != nil {
				return fmt.Errorf("failed to add ownerRef to PVC %s: %w", pvc.Name, err)
			}
			if err := r.Patch(ctx, pvc, patch); err != nil {
				return fmt.Errorf("failed to patch PVC %s ownerRef: %w", pvc.Name, err)
			}
			logger.Info("Added ownerRef to PVC for Delete policy", "pvc", pvc.Name)
		} else if !wantOwnerRef && hasOwnerRef {
			patch := client.MergeFrom(pvc.DeepCopy())
			filtered := make([]metav1.OwnerReference, 0, len(pvc.OwnerReferences))
			for _, ref := range pvc.OwnerReferences {
				if ref.UID != shardUID {
					filtered = append(filtered, ref)
				}
			}
			pvc.OwnerReferences = filtered
			if err := r.Patch(ctx, pvc, patch); err != nil {
				return fmt.Errorf("failed to patch PVC %s ownerRef: %w", pvc.Name, err)
			}
			logger.Info("Removed ownerRef from PVC for Retain policy", "pvc", pvc.Name)
		}
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ShardReconciler) SetupWithManager(mgr ctrl.Manager, opts ...controller.Options) error {
	controllerOpts := controller.Options{
		MaxConcurrentReconciles: 20,
	}
	if len(opts) > 0 {
		controllerOpts = opts[0]
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&multigresv1alpha1.Shard{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueFromPostgresConfigMap),
		).
		WithOptions(controllerOpts).
		Complete(r)
}

// computePostgresConfigHash fetches the referenced ConfigMap and returns a
// SHA-256 hex digest of the referenced key's data. This hash is placed on each
// pod as an annotation so that changes to the ConfigMap content produce a
// different spec-hash, triggering the existing rolling update mechanism.
func (r *ShardReconciler) computePostgresConfigHash(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
) (string, error) {
	ref := shard.Spec.PostgresConfigRef

	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      ref.Name,
	}, cm); err != nil {
		return "", fmt.Errorf("failed to get ConfigMap %q: %w", ref.Name, err)
	}

	data, ok := cm.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %q not found in ConfigMap %q", ref.Key, ref.Name)
	}

	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:]), nil
}

// enqueueFromPostgresConfigMap returns reconcile requests for Shards that
// reference the changed ConfigMap via spec.postgresConfigRef.name.
func (r *ShardReconciler) enqueueFromPostgresConfigMap(
	ctx context.Context,
	o client.Object,
) []reconcile.Request {
	shards := &multigresv1alpha1.ShardList{}
	if err := r.List(ctx, shards, client.InNamespace(o.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, s := range shards.Items {
		if s.Spec.PostgresConfigRef != nil && s.Spec.PostgresConfigRef.Name == o.GetName() {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&s),
			})
		}
	}
	return requests
}
