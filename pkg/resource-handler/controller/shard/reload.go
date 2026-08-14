package shard

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/multigres/multigres/go/common/topoclient"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	// reloadConfigSyncCeiling bounds how long after the reload target changes the
	// operator waits before reloading a pod, so a reload is guaranteed to re-read
	// the desired postgresql.conf rather than the pre-update file the kubelet has
	// not yet projected. It must exceed the kubelet's ConfigMap propagation
	// latency (its sync period plus cache TTL, ~1 min by default); 90s is a
	// conservative margin. The ReloadConfig RPC proves a reload happened, not that
	// the file it read was current, so the operator gates the timing itself.
	reloadConfigSyncCeiling = 90 * time.Second

	// reloadRetryDelay is the requeue delay while a reload cannot yet complete
	// (PostgreSQL not running on a pod, or its pooler not yet in topology).
	reloadRetryDelay = 10 * time.Second
)

// reconcileReloadState applies reload-safe postgresql.conf changes in place via
// the multipooler ReloadConfig RPC (SIGHUP), so a change confined to reload-safe
// settings does not recreate pods. It returns a requeue delay (0 when nothing is
// pending) and an error.
//
// A pod is reloaded when its AnnotationPostgresReloadHash differs from the
// shard's desired reload-hash. Because a ConfigMap update reaches a pod's
// mounted file only after a kubelet sync lag, the operator first waits
// reloadConfigSyncCeiling from the moment the target last changed (tracked on
// the ConfigMap) before reloading — otherwise the SIGHUP would re-read the stale
// file and the pod would be marked current on a config it is not running.
//
// Reload is non-disruptive (no connection drop), so pods are reloaded without
// ordering. Pods that are draining, terminating, or due for a restart (which
// recreates them with the current config anyway) are skipped.
func (r *ShardReconciler) reconcileReloadState(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
) (time.Duration, error) {
	logger := log.FromContext(ctx)

	desired := shard.Annotations[metadata.AnnotationPostgresReloadHash]
	if desired == "" || r.RPCClient == nil {
		return 0, nil
	}

	// Wait out the kubelet sync lag before reloading, so the SIGHUP re-reads the
	// updated file. Any target change re-stamps the marker, restarting the wait
	// for every pod — this correctly handles a second change during the window.
	ready, wait, err := r.reloadTargetSynced(ctx, shard, desired)
	if err != nil {
		return 0, err
	}
	if !ready {
		logger.V(1).Info("reload: waiting for config sync before reload",
			"reloadHash", desired, "wait", wait.String())
		return wait, nil
	}

	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
	}
	podList := &corev1.PodList{}
	if err := r.List(
		ctx,
		podList,
		client.InNamespace(shard.Namespace),
		client.MatchingLabels(lbls),
	); err != nil {
		return 0, fmt.Errorf("listing pods for reload: %w", err)
	}

	desiredRestart := shard.Annotations[metadata.AnnotationPostgresConfigHash]
	poolersByCell := map[string][]*topoclient.MultipoolerInfo{}
	requeue := time.Duration(0)
	var current, stale, reloaded, skippedRestart, poolerMissing int

	for i := range podList.Items {
		pod := &podList.Items[i]

		if pod.Annotations[metadata.AnnotationPostgresReloadHash] == desired {
			current++
			continue // already current
		}
		if !pod.DeletionTimestamp.IsZero() ||
			pod.Annotations[metadata.AnnotationDrainState] != "" {
			continue // terminating or draining
		}
		stale++
		// A pod whose restart-hash is stale will be recreated by the rolling
		// update with the current config, so leave it to that path rather than
		// reloading a pod that is about to be replaced.
		if pod.Annotations[metadata.AnnotationPostgresConfigHash] != desiredRestart {
			skippedRestart++
			continue
		}

		cellName := pod.Labels[metadata.LabelMultigresCell]
		poolers, ok := poolersByCell[cellName]
		if !ok {
			poolers, err = store.GetMultipoolersByCell(ctx, cellName, topo.ShardFilter(shard))
			if err != nil {
				if topo.IsTopoUnavailable(err) {
					return reloadRetryDelay, nil
				}
				return 0, fmt.Errorf("listing poolers in cell %q for reload: %w", cellName, err)
			}
			poolersByCell[cellName] = poolers
		}

		var pooler *topoclient.MultipoolerInfo
		for _, p := range poolers {
			if topo.PodMatchesPooler(pod.Name, p) {
				pooler = p
				break
			}
		}
		if pooler == nil {
			// Pod not yet registered in topology; retry later.
			logger.Info("reload: pod not matched to a pooler in topology, will retry",
				"pod", pod.Name, "cell", cellName, "poolersInCell", len(poolers))
			poolerMissing++
			requeue = reloadRetryDelay
			continue
		}

		resp, rerr := r.RPCClient.ReloadConfig(
			ctx,
			pooler.Multipooler,
			&multipoolermanagerdatapb.ReloadConfigRequest{},
		)
		if rerr != nil {
			logger.Error(rerr, "ReloadConfig RPC failed", "pod", pod.Name)
			requeue = reloadRetryDelay
			continue
		}
		if resp.GetConfigLoadTime() == nil {
			// PostgreSQL was not running, so no reload happened. Retryable.
			logger.Info(
				"ReloadConfig reported no reload (postgres not running), will retry",
				"pod", pod.Name,
			)
			requeue = reloadRetryDelay
			continue
		}

		if err := r.stampReloadHash(ctx, pod, desired); err != nil {
			return 0, err
		}
		reloaded++
		r.Recorder.Eventf(
			shard,
			"Normal",
			"ConfigReloaded",
			"Reloaded postgresql.conf on pod %s without restart",
			pod.Name,
		)
	}

	if len(podList.Items) > 0 {
		logger.V(1).Info("reload: evaluated pods",
			"reloadHash", desired,
			"desiredRestart", desiredRestart,
			"totalPods", len(podList.Items),
			"current", current,
			"stale", stale,
			"reloaded", reloaded,
			"skippedRestartPending", skippedRestart,
			"poolerMissing", poolerMissing,
		)
	}

	return requeue, nil
}

// reloadTargetSynced reports whether the desired reload target has been present
// on the operator-owned ConfigMap long enough (reloadConfigSyncCeiling) that the
// kubelet has certainly projected the updated postgresql.conf into the pods.
//
// The delivery path (applyPostgresConfigMap) stamps the target reload-hash and
// the time it last changed on the ConfigMap; this only reads them, so it never
// races the ForceOwnership SSA that re-applies the ConfigMap each reconcile
// (which would otherwise strip a separately-patched annotation and reset the
// timer forever).
func (r *ShardReconciler) reloadTargetSynced(
	ctx context.Context,
	shard *multigresv1alpha1.Shard,
	desired string,
) (ready bool, wait time.Duration, err error) {
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{
		Namespace: shard.Namespace,
		Name:      PostgresConfigMapName(shard.Name),
	}, cm); err != nil {
		if errors.IsNotFound(err) {
			// Delivery creates the ConfigMap earlier in the reconcile; if it is not
			// visible yet, retry shortly.
			return false, reloadRetryDelay, nil
		}
		return false, 0, fmt.Errorf("getting postgres ConfigMap for reload timing: %w", err)
	}

	// The delivery path stamps `desired` on the ConfigMap; until it has caught up
	// (cache lag, or a change still propagating) the target is not yet timed —
	// wait and re-check.
	if cm.Annotations[metadata.AnnotationPostgresReloadHash] != desired {
		return false, reloadRetryDelay, nil
	}

	updatedAt, perr := time.Parse(
		time.RFC3339Nano,
		cm.Annotations[metadata.AnnotationPostgresReloadHashUpdatedAt],
	)
	if perr != nil {
		// Timestamp not yet stamped or unparseable; retry shortly.
		return false, reloadRetryDelay, nil
	}

	if elapsed := time.Now().UTC().Sub(updatedAt); elapsed < reloadConfigSyncCeiling {
		return false, reloadConfigSyncCeiling - elapsed, nil
	}
	return true, 0, nil
}

// stampReloadHash records the confirmed-applied reload-hash on a pod after a
// successful reload, so status and the reload loop see it as current.
func (r *ShardReconciler) stampReloadHash(
	ctx context.Context,
	pod *corev1.Pod,
	hash string,
) error {
	base := client.MergeFrom(pod.DeepCopy())
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[metadata.AnnotationPostgresReloadHash] = hash
	if err := r.Patch(ctx, pod, base); err != nil {
		return fmt.Errorf("stamping reload-hash on pod %s: %w", pod.Name, err)
	}
	return nil
}
