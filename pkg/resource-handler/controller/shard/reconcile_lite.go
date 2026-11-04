package shard

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
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
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
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

	// A template update can bypass cluster admission. Never provision another
	// static primary alongside a pod or retained data volume with another identity.
	if err := r.validateLiteResourceIdentity(ctx, shard, poolName, cellName); err != nil {
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

	// Reclaim active storage and apply retention changes even when topology is
	// unavailable. Both normal reconciliation and outage recovery may reuse it.
	if err := r.reconcileLitePVCMetadata(ctx, shard, poolName, cellName); err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(
			shard,
			"Warning",
			"FailedApply",
			"Failed to reconcile PVC metadata: %v",
			err,
		)
		return ctrl.Result{}, err
	}

	// The pooler reads its backup location from the database record when it
	// bootstraps, so the record is registered before the pod is created. A
	// pod that runs on another spec keeps the location it was started with
	// until it is gone.
	keepLocation, err := r.litePodOnAnotherSpec(ctx, shard, poolName, cellName, poolSpec)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		return ctrl.Result{}, err
	}
	registered, err := r.registerLiteDatabase(ctx, shard, keepLocation)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		return ctrl.Result{}, err
	}
	if !registered {
		// A bootstrapped Minigres can restart from topo-database.pb on its
		// existing data PVC. Do not let an etcd outage prevent pod recovery,
		// but leave provisioning, rollouts and reloads until topology returns.
		err := r.recoverLitePod(ctx, shard, poolName, cellName, poolSpec)
		return ctrl.Result{RequeueAfter: topoUnavailableRequeueDelay}, err
	}

	pvcWait, err := r.ensureLiteDataPVC(ctx, shard, poolName, cellName, poolSpec)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(shard, "Warning", "FailedApply", "Failed to reconcile data PVC: %v", err)
		return ctrl.Result{}, err
	}

	unanswered, err := r.ensureLitePod(ctx, shard, poolName, cellName, poolSpec)
	if err != nil {
		monitoring.RecordSpanError(span, err)
		r.Recorder.Eventf(shard, "Warning", "FailedApply", "Failed to reconcile Lite pod: %v", err)
		return ctrl.Result{}, err
	}

	wait, err := r.reconcileLiteReload(ctx, shard, rendered)
	if err != nil {
		return ctrl.Result{}, err
	}
	primary := meta.IsStatusConditionTrue(
		shard.Status.Conditions, multigresv1alpha1.ConditionPrimary,
	)
	requeue := liteRequeue(wait, primary, pvcWait)
	if unanswered && requeue == 0 {
		// Nothing else brings the shard back, so it comes back when its
		// question may be asked again.
		requeue = r.liteUnanswered.untilDue(client.ObjectKeyFromObject(shard), r.now())
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// reconcileLitePVCMetadata removes the GC marker from the data PVC before
// restoring ownership, since reconcilePVCOwnerRefs deliberately skips orphans.
func (r *ShardReconciler) reconcileLitePVCMetadata(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
) error {
	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      BuildPoolDataPVCName(shard, poolName, cellName, 0),
	}
	if err := r.Get(ctx, key, pvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("read Lite data PVC metadata: %w", err)
		}
	} else if pvc.DeletionTimestamp.IsZero() {
		if _, err := r.reclaimPVC(ctx, pvc); err != nil {
			return fmt.Errorf("clear orphan label on Lite data PVC %s: %w", pvc.Name, err)
		}
	}
	return r.reconcilePVCOwnerRefs(ctx, shard)
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
// topology with RegisterDatabaseFromSpec, built from the shard: its cells, the
// backup location from spec.sidecarBackupLocation (the one the sidecar renders)
// and the durability policy. In lite mode the Shard owns this record because
// its pool cells may differ from the cluster defaults, and the cluster
// controller skips it. It reports
// false when the topology server cannot be reached yet, so the caller requeues
// as the data-plane phase does.
//
// With keepLocation the backup location of an existing record is left as it
// is (see litePodOnAnotherSpec).
func (r *ShardReconciler) registerLiteDatabase(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	keepLocation bool,
) (bool, error) {
	logger := log.FromContext(ctx)
	ctx, cancel := context.WithTimeout(ctx, liteTopoTimeout)
	defer cancel()

	store, err := r.topoStore(ctx, shard)
	if err != nil {
		if isLiteTopoUnavailable(err) {
			logger.Info("Topology server unavailable; requeueing", "error", err)
			return false, nil
		}
		r.Recorder.Eventf(shard, "Warning", "TopologyError",
			"Failed to connect to topology store: %v", err)
		return false, fmt.Errorf("open topology for Lite database registration: %w", err)
	}
	defer func() { _ = store.Close() }()

	dbConfig := multigresv1alpha1.DatabaseConfig{
		Name:             shard.Spec.DatabaseName,
		DurabilityPolicy: shard.Spec.DurabilityPolicy,
	}
	var opts []topo.RegisterDatabaseOption
	if keepLocation {
		opts = append(opts, topo.KeepBackupLocation())
	}
	if err := topo.RegisterDatabaseFromSpec(
		ctx, store, r.Recorder, shard, dbConfig, topo.CollectCells(shard),
		shard.Spec.SidecarBackupLocation, shard.Spec.DurabilityPolicy, opts...,
	); err != nil {
		if isLiteTopoUnavailable(err) {
			logger.Info("Topology server unavailable; requeueing", "error", err.Error())
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// litePodOnAnotherSpec reports whether the shard's pod exists and was built
// from another spec than the one the shard has now, whether it is running,
// waiting for a replacement the API server refuses, or terminating.
//
// Minigres compares the backup location in the database record with what
// the sidecar next to it was started with, the endpoint and how its
// certificate is verified included, each time it starts, and does not start
// when they differ. The pod's containers can restart at any time, and a pod
// kept because its replacement was refused stays for as long as the refusal
// does. So the record keeps the location such a pod was started with, and
// moves to the spec's location in the pass that finds the pod gone, which is
// the pass that creates its successor.
func (r *ShardReconciler) litePodOnAnotherSpec(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) (bool, error) {
	desired, err := r.buildLitePod(shard, poolName, cellName, poolSpec)
	if err != nil {
		return false, fmt.Errorf("failed to build Lite pod: %w", err)
	}
	existing := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get pod %s: %w", desired.Name, err)
	}
	return existing.Annotations[metadata.AnnotationSpecHash] !=
		desired.Annotations[metadata.AnnotationSpecHash], nil
}

// isLiteTopoUnavailable also recognizes the deadline reached when etcd accepts
// a connection but cannot answer. Configuration and permission errors must not
// authorize recovery using cached topology.
func isLiteTopoUnavailable(err error) bool {
	return topo.IsTopoUnavailable(err) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, &topoclient.TopoError{Code: topoclient.Timeout}) ||
		grpcstatus.Code(err) == codes.Unavailable || grpcstatus.Code(err) == codes.DeadlineExceeded
}

// recoverLitePod only replaces missing or terminal pods using an existing data
// volume. The volume's existence is not proof of bootstrap: Minigres itself must
// find and validate its cached database record, and first bootstrap still needs
// etcd. Never replace a live pod for spec drift or resize during an outage.
func (r *ShardReconciler) recoverLitePod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) error {
	pvc := &corev1.PersistentVolumeClaim{}
	key := client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      BuildPoolDataPVCName(shard, poolName, cellName, 0),
	}
	if err := r.Get(ctx, key, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // A fresh deployment must first register in topology.
		}
		return fmt.Errorf("read Lite recovery PVC: %w", err)
	}
	if !pvc.DeletionTimestamp.IsZero() || pvc.Status.Phase != corev1.ClaimBound {
		return nil
	}

	pod := &corev1.Pod{}
	key.Name = BuildPoolPodName(shard, poolName, cellName, 0)
	if err := r.Get(ctx, key, pod); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("read Lite recovery pod: %w", err)
		}
	} else if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
		return nil
	}
	// The caller comes back when topology does, and asks again then about
	// a replacement that got no answer.
	_, err := r.ensureLitePod(ctx, shard, poolName, cellName, poolSpec)
	return err
}

// liteRequeue is when a Lite shard is reconciled again without an event: for
// a reload still in progress, for a pod that is not primary yet, or for its
// data PVC, in that order of precedence. Zero means none of them applies.
func liteRequeue(reloadWait time.Duration, primary bool, pvcWait time.Duration) time.Duration {
	switch {
	case reloadWait > 0:
		return reloadWait
	case !primary:
		return litePrimaryRequeue
	}
	return pvcWait
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
	if err == nil && pvcutil.IsTerminating(existing) {
		// Neither expanded nor waited on: its deletion finishing enqueues the
		// shard, and the branch below then creates its replacement.
		return 0, nil
	}
	if err == nil {
		if err := r.reconcilePoolDataPVC(ctx, shard, existing, poolSpec); err != nil {
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
	if err := r.createUnderQuota(ctx, shard, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return 0, fmt.Errorf("failed to create PVC %s: %w", name, err)
	}
	log.FromContext(ctx).Info("Created Lite data PVC", "pvc", name)
	return liteStorageRequeue, nil
}

// ensureLitePod creates the Lite pod when it is missing and deletes it when it
// is terminal, its spec-hash no longer matches, or its data PVC requires a
// filesystem resize. The next reconcile creates it again. There is no
// health preflight and no drain: the pod is the shard's only pooler, so
// there is no cohort to protect. The one check before the delete is that the
// API server accepts the pod that will be created in its place. When the
// API server does not answer that question, the pod is kept and unanswered
// is true: the delete would be an outage, and a question that was not
// answered is no reason for one. A question that cannot be asked at all
// does not keep the pod (see replacementProceeds).
func (r *ShardReconciler) ensureLitePod(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
	poolSpec multigresv1alpha1.PoolSpec,
) (unanswered bool, err error) {
	logger := log.FromContext(ctx)

	desired, err := r.buildLitePod(shard, poolName, cellName, poolSpec)
	if err != nil {
		return false, fmt.Errorf("failed to build Lite pod: %w", err)
	}

	existing := &corev1.Pod{}
	err = r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		// The shard's only pooler would start on storage that is going away.
		if claim, err := r.claimPodVolumes(ctx, shard, desired); err != nil {
			return false, err
		} else if claim != "" {
			logger.Info("Not creating the Lite pod while a PVC it mounts is being deleted",
				"pvc", claim)
			return false, nil
		}
		if err := r.createUnderQuota(ctx, shard, desired); err != nil &&
			!apierrors.IsAlreadyExists(err) {
			return false, fmt.Errorf("failed to create pod %s: %w", desired.Name, err)
		}
		logger.Info("Created Lite pod", "pod", desired.Name)
		r.Recorder.Eventf(shard, "Normal", "PodCreated", "Created pod %s", desired.Name)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get pod %s: %w", desired.Name, err)
	}

	// A pod that is already terminating is replaced once it is gone; its
	// deletion requeues the shard.
	if !existing.DeletionTimestamp.IsZero() {
		return false, nil
	}
	// The pod's own references, not the desired pod's: those are what hold a
	// PVC being deleted, and its data, in place. See terminating_pvc.go.
	if claim, err := podTerminatingClaim(ctx, r.disruptionReader(), existing); err != nil {
		return false, err
	} else if claim != "" {
		logger.Info("Leaving the Lite pod alone while a PVC it mounts is being deleted",
			"pvc", claim)
		return false, nil
	}
	reason := "apply a spec change"
	if existing.Status.Phase == corev1.PodFailed || existing.Status.Phase == corev1.PodSucceeded {
		reason = "replace a terminal pod"
	} else if existing.Annotations[metadata.AnnotationSpecHash] ==
		desired.Annotations[metadata.AnnotationSpecHash] {
		// Wait for a replacement to finish mounting its volume before another
		// resize restart, as the regular pool path waits for healthy pods.
		if !isPodReady(existing) {
			return false, nil
		}
		pvcName := BuildPoolDataPVCName(shard, poolName, cellName, 0)
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(
			ctx,
			client.ObjectKey{Namespace: shard.Namespace, Name: pvcName},
			pvc,
		); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("failed to get PVC %s for filesystem resize: %w", pvcName, err)
		}
		if !pvcNeedsFilesystemResize(
			map[string]*corev1.PersistentVolumeClaim{pvcName: pvc},
			pvcName,
		) {
			return false, nil
		}
		reason = "complete data PVC filesystem expansion"
	}

	// The pod is the shard's only one, so it is deleted only for a
	// replacement the API server says it will take.
	verdict, err := r.judgeReplacement(ctx, shard, existing, desired)
	if err != nil {
		return false, err
	}
	if !replacementProceeds(verdict, shard, existing.Name, true) {
		return verdict == replacementUnanswered, nil
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
		return false, fmt.Errorf("failed to delete pod %s: %w", existing.Name, err)
	}
	r.Recorder.Eventf(
		shard,
		"Normal",
		"PodReplaced",
		"Deleted pod %s to %s",
		existing.Name,
		reason,
	)
	return false, nil
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
		if isLiteTopoUnavailable(err) {
			return reloadRetryDelay, nil
		}
		return 0, fmt.Errorf("open topology for Lite config reload: %w", err)
	}
	defer func() { _ = store.Close() }()
	return r.reconcileReloadState(ctx, store, shard, rendered, rpc)
}

// validateLiteResourceIdentity also protects retained PVCs without owner refs
// and blocks identity changes while the old pod is absent or terminating.
func (r *ShardReconciler) validateLiteResourceIdentity(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	pool, cell string,
) error {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	selector := client.MatchingLabels{
		metadata.LabelMultigresCluster: shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelAppComponent:     PoolComponentName,
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(shard.Namespace), selector); err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if pod.Name != BuildPoolPodName(shard, pool, cell, 0) {
			return fmt.Errorf(
				"lite pool identity is immutable: existing pod %s would be replaced by a different identity",
				pod.Name,
			)
		}
	}
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(ctx, pvcs, client.InNamespace(shard.Namespace), selector); err != nil {
		return err
	}
	for _, pvc := range pvcs.Items {
		if pvc.Labels[metadata.LabelMultigresPool] != "" &&
			pvc.Name != BuildPoolDataPVCName(shard, pool, cell, 0) {
			return fmt.Errorf(
				"lite pool identity is immutable: existing data PVC %s would be replaced by a different identity",
				pvc.Name,
			)
		}
	}
	return nil
}
