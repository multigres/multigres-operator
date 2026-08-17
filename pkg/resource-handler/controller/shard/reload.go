package shard

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/multigres/multigres/go/common/topoclient"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// reloadRetryDelay is the requeue delay while a reload cannot yet complete: the
// pod's mounted ConfigMap has not caught up to the desired settings (the RPC
// reports a mismatch), PostgreSQL is not running, or the pooler is not yet in
// topology.
const reloadRetryDelay = 10 * time.Second

// reconcileReloadState applies reload-safe postgresql.conf changes in place via
// the multipooler ReloadConfig RPC (SIGHUP), so a change confined to reload-safe
// settings does not recreate pods. It returns a requeue delay (0 when nothing is
// pending) and an error.
//
// A pod is reloaded when its AnnotationPostgresReloadHash differs from the
// shard's desired reload-hash. The desired reload-safe settings are passed to
// the RPC as expected_settings: the multipooler reads pg_file_settings and
// reloads ONLY if the file it would re-read already carries every one of them,
// so a not-yet-synced ConfigMap mount is reported as a mismatch (retry) rather
// than silently reloaded. This is what makes the reload correct without guessing
// the kubelet sync lag: a pod is stamped current only once the running server
// provably carries the change.
//
// Reload is non-disruptive (no connection drop), so pods are reloaded without
// ordering. Pods that are draining, terminating, or due for a restart (which
// recreates them with the current config anyway) are skipped. A setting that the
// server reports needs a restart (needs_restart) is surfaced without stamping —
// the classifier put it in the reload set but PostgreSQL requires a restart.
func (r *ShardReconciler) reconcileReloadState(
	ctx context.Context,
	store topoclient.Store,
	shard *multigresv1alpha1.Shard,
	rendered renderedConfig,
) (time.Duration, error) {
	logger := log.FromContext(ctx)

	desired := rendered.reloadHash
	if desired == "" || rendered.err != nil || r.RPCClient == nil {
		return 0, nil
	}

	lbls := map[string]string{
		metadata.LabelMultigresCluster:    shard.Labels[metadata.LabelMultigresCluster],
		metadata.LabelMultigresDatabase:   string(shard.Spec.DatabaseName),
		metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName),
		metadata.LabelMultigresShard:      string(shard.Spec.ShardName),
		metadata.LabelAppComponent:        PoolComponentName,
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

	// Use the restart-hash from this reconcile's render (not the in-memory shard
	// annotation, which a shard re-fetch in the data-plane path may have cleared —
	// an empty desiredRestart would wrongly mark every pod restart-pending and
	// suppress all reloads).
	desiredRestart := rendered.hash
	req := &multipoolermanagerdatapb.ReloadConfigRequest{
		ExpectedSettings: rendered.reloadSettings,
	}
	poolersByCell := map[string][]*topoclient.MultipoolerInfo{}
	requeue := time.Duration(0)
	var stale, reloaded, skippedRestart, notSynced, needsRestart int

	var err error
	for i := range podList.Items {
		pod := &podList.Items[i]
		podReload := pod.Annotations[metadata.AnnotationPostgresReloadHash]
		podCfg := pod.Annotations[metadata.AnnotationPostgresConfigHash]

		if podReload == desired {
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
		if podCfg != desiredRestart {
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
			requeue = reloadRetryDelay
			continue
		}

		resp, rerr := r.RPCClient.ReloadConfig(ctx, pooler.Multipooler, req)
		if rerr != nil {
			logger.Error(rerr, "ReloadConfig RPC failed", "pod", pod.Name)
			requeue = reloadRetryDelay
			continue
		}
		logger.V(1).Info("reload: ReloadConfig verdict", "pod", pod.Name,
			"applied", resp.GetConfigLoadTime() != nil,
			"needsRestart", resp.GetNeedsRestart(),
			"mismatches", mismatchDetail(resp.GetMismatches()))

		switch {
		case resp.GetConfigLoadTime() != nil:
			// The file carried every expected value and the reload took effect.
			if err := r.stampReloadHash(ctx, pod, desired); err != nil {
				return 0, err
			}
			reloaded++
			r.Recorder.Eventf(shard, "Normal", "ConfigReloaded",
				"Reloaded postgresql.conf on pod %s without restart", pod.Name)

		case resp.GetNeedsRestart():
			// A setting we classified reload-safe actually needs a restart. Surface
			// it (the catalog classification is the real fix) and do not stamp, so
			// status stays in progress rather than falsely reporting current.
			needsRestart++
			requeue = reloadRetryDelay
			logger.Info("reload: setting needs a restart, not reloadable in place",
				"pod", pod.Name, "settings", mismatchNames(resp.GetMismatches()))
			r.Recorder.Eventf(shard, "Warning", "ConfigReloadNeedsRestart",
				"pod %s: reload-classified setting(s) require a restart: %v",
				pod.Name, mismatchNames(resp.GetMismatches()))

		default:
			// No reload: the pod's mounted file has not caught up to the desired
			// settings yet (or PostgreSQL is not running). Retry.
			notSynced++
			requeue = reloadRetryDelay
		}
	}

	if len(podList.Items) > 0 {
		logger.V(1).Info("reload: evaluated pods",
			"reloadHash", desired,
			"desiredRestart", desiredRestart,
			"totalPods", len(podList.Items),
			"stale", stale,
			"reloaded", reloaded,
			"skippedRestartPending", skippedRestart,
			"notSynced", notSynced,
			"needsRestart", needsRestart,
		)
	}

	return requeue, nil
}

// mismatchNames extracts the GUC names from a ReloadConfig mismatch list for
// logging and events.
func mismatchNames(ms []*multipoolermanagerdatapb.SettingMismatch) []string {
	names := make([]string, 0, len(ms))
	for _, m := range ms {
		names = append(names, m.GetName())
	}
	return names
}

// mismatchDetail renders name/error/requires_restart for each mismatch for debug
// logs.
func mismatchDetail(ms []*multipoolermanagerdatapb.SettingMismatch) string {
	parts := make([]string, 0, len(ms))
	for _, m := range ms {
		parts = append(
			parts,
			fmt.Sprintf("%s{err=%q,restart=%v}", m.GetName(), m.GetError(), m.GetRequiresRestart()),
		)
	}
	return strings.Join(parts, ",")
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
