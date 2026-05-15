package multigrescluster

import (
	"context"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/resolver"
)

const (
	// topoUnavailableGracePeriod is the duration after resource creation during
	// which topology UNAVAILABLE errors are silently requeued instead of being
	// reported as reconcile errors. This prevents noisy error metrics during
	// normal cluster startup while the toposerver is still initializing.
	topoUnavailableGracePeriod = 2 * time.Minute

	// topoUnavailableRequeueDelay is the delay before retrying when the topology
	// server is unavailable during the grace period.
	topoUnavailableRequeueDelay = 5 * time.Second

	// topoOperationTimeout bounds all topo operations within a single reconcile.
	// TODO: switch to per-entity timeouts when multi-database support lands.
	// With many cells/databases, later operations could hit the deadline.
	topoOperationTimeout = 20 * time.Second
)

// reconcileTopology registers cells and databases in the topology server
// and optionally prunes stale entries. This centralizes topology management
// that was previously split across individual cell and shard controllers.
func (r *MultigresClusterReconciler) reconcileTopology(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	globalTopoRef, err := r.globalTopoRef(ctx, cluster, res)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get global topo ref: %w", err)
	}

	store, err := r.openTopoStore(globalTopoRef)
	if err != nil {
		if topo.IsTopoUnavailable(err) {
			return r.handleTopoUnavailable(cluster, logger)
		}
		r.Recorder.Eventf(cluster, "Warning", "TopoConnectFailed",
			"Failed to connect to topology server: %v", err)
		return ctrl.Result{}, fmt.Errorf("failed to open topology store: %w", err)
	}
	defer func() { _ = store.Close() }()

	topoCtx, cancel := context.WithTimeout(ctx, topoOperationTimeout)
	defer cancel()

	// Register all cells.
	for _, cellCfg := range cluster.Spec.Cells {
		cellCfgForResolution := cellCfg
		if cellCfgForResolution.CellTemplate == "" &&
			cluster.Spec.TemplateDefaults.CellTemplate != "" {
			cellCfgForResolution.CellTemplate = cluster.Spec.TemplateDefaults.CellTemplate
		}
		_, _, localTopoSpec, err := res.ResolveCell(ctx, &cellCfgForResolution)
		if err != nil {
			r.Recorder.Event(cluster, "Warning", "TemplateMissing", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to resolve cell '%s': %w", cellCfg.Name, err)
		}

		if err := topo.RegisterCellFromSpec(
			topoCtx, store, r.Recorder, cluster, cellCfg, localTopoSpec, globalTopoRef,
		); err != nil {
			if topo.IsTopoUnavailable(err) {
				return r.handleTopoUnavailable(cluster, logger)
			}
			return ctrl.Result{}, fmt.Errorf("failed to register cell '%s' in topology: %w",
				cellCfg.Name, err)
		}
	}

	// Collect cell names for database registration.
	allCellNames := make([]string, 0, len(cluster.Spec.Cells))
	for _, c := range cluster.Spec.Cells {
		allCellNames = append(allCellNames, string(c.Name))
	}

	// Register all databases.
	for _, dbConfig := range cluster.Spec.Databases {
		dbBackup := multigresv1alpha1.MergeBackupConfig(dbConfig.Backup, cluster.Spec.Backup)
		if err := topo.RegisterDatabaseFromSpec(
			topoCtx, store, r.Recorder, cluster, dbConfig, allCellNames, dbBackup,
			cluster.Spec.DurabilityPolicy,
		); err != nil {
			if topo.IsTopoUnavailable(err) {
				return r.handleTopoUnavailable(cluster, logger)
			}
			return ctrl.Result{}, fmt.Errorf("failed to register database '%s' in topology: %w",
				dbConfig.Name, err)
		}
	}

	// Prune stale topology entries unless disabled.
	if isPruningEnabled(cluster) {
		specDBNames := make([]string, 0, len(cluster.Spec.Databases))
		for _, db := range cluster.Spec.Databases {
			specDBNames = append(specDBNames, string(db.Name))
		}
		if err := topo.PruneDatabases(
			topoCtx, store, r.Recorder, cluster, specDBNames,
		); err != nil {
			if topo.IsTopoUnavailable(err) {
				return r.handleTopoUnavailable(cluster, logger)
			}
			return ctrl.Result{}, fmt.Errorf("failed to prune databases: %w", err)
		}

		specCellNames := make([]string, 0, len(cluster.Spec.Cells))
		for _, c := range cluster.Spec.Cells {
			specCellNames = append(specCellNames, string(c.Name))
		}
		if err := topo.PruneCells(topoCtx, store, r.Recorder, cluster, specCellNames); err != nil {
			if topo.IsTopoUnavailable(err) {
				return r.handleTopoUnavailable(cluster, logger)
			}
			return ctrl.Result{}, fmt.Errorf("failed to prune cells: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

// openTopoStore opens a topology store, using the injected factory for tests
// or the real NewStoreFromRef for production.
func (r *MultigresClusterReconciler) openTopoStore(
	ref multigresv1alpha1.GlobalTopoServerRef,
) (topoclient.Store, error) {
	if r.CreateTopoStore != nil {
		return r.CreateTopoStore(ref)
	}
	return topo.NewStoreFromRef(ref)
}

// isPruningEnabled returns true if topology pruning is enabled for the cluster.
// Pruning is enabled by default (nil or empty config means enabled).
// This is a package-level function so it can be shared by reconcileTopology
// and BuildCell (which propagates the flag to the Cell's PrunePoolers field).
func isPruningEnabled(cluster *multigresv1alpha1.MultigresCluster) bool {
	if cluster.Spec.TopologyPruning == nil ||
		cluster.Spec.TopologyPruning.Enabled == nil {
		return true
	}
	return *cluster.Spec.TopologyPruning.Enabled
}

// handleTopoUnavailable handles the case where the topo server is unavailable.
// During the grace period after cluster creation, it silently requeues.
// After the grace period, it returns an error.
func (r *MultigresClusterReconciler) handleTopoUnavailable(
	cluster *multigresv1alpha1.MultigresCluster,
	logger interface{ Info(string, ...any) },
) (ctrl.Result, error) {
	resourceAge := time.Since(cluster.CreationTimestamp.Time)
	if resourceAge < topoUnavailableGracePeriod {
		logger.Info("Topology server not available yet, requeueing",
			"resourceAge", resourceAge.Round(time.Second).String(),
			"gracePeriod", topoUnavailableGracePeriod.String(),
		)
		r.Recorder.Eventf(cluster, "Normal", "TopologyWaiting",
			"Topology server not available yet (age %s), will retry",
			resourceAge.Round(time.Second))
		return ctrl.Result{RequeueAfter: topoUnavailableRequeueDelay}, nil
	}
	return ctrl.Result{}, fmt.Errorf("topology server unavailable")
}
