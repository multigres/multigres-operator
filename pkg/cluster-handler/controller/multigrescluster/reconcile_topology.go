package multigrescluster

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/topo"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/name"
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

	// localTopoServerRequeueDelay is the delay before retrying topology
	// registration when a managed local TopoServer is not ready yet.
	localTopoServerRequeueDelay = 5 * time.Second
)

// reconcileTopology registers cells and databases in the topology server
// and optionally prunes stale entries. This centralizes topology management
// that was previously split across individual cell and shard controllers.
func (r *MultigresClusterReconciler) reconcileTopology(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	res *resolver.Resolver,
	preservePendingDeletionCells ...bool,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	preservePendingCells := len(preservePendingDeletionCells) > 0 &&
		preservePendingDeletionCells[0]

	globalTopoRef, err := r.globalTopoRef(ctx, cluster, res)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get global topo ref: %w", err)
	}

	localTopoSpecs := make(map[multigresv1alpha1.CellName]*multigresv1alpha1.LocalTopoServerSpec,
		len(cluster.Spec.Cells))
	for _, cellCfg := range cluster.Spec.Cells {
		cellCfgForResolution := cellCfg
		cellCfgForResolution.CellTemplate = cluster.Spec.EffectiveCellTemplate(
			cellCfgForResolution.CellTemplate,
		)
		_, _, localTopoSpec, err := res.ResolveCell(ctx, cluster, &cellCfgForResolution)
		if err != nil {
			r.Recorder.Event(cluster, "Warning", "TemplateMissing", err.Error())
			return ctrl.Result{}, fmt.Errorf("failed to resolve cell '%s': %w", cellCfg.Name, err)
		}
		localTopoSpecs[cellCfg.Name] = localTopoSpec
		if localTopoSpec != nil && localTopoSpec.Etcd != nil {
			ready, err := r.managedLocalTopoServerReady(ctx, cluster, cellCfg)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !ready {
				logger.V(1).Info("Waiting for managed local TopoServer before registering cell",
					"cell", cellCfg.Name)
				return ctrl.Result{RequeueAfter: localTopoServerRequeueDelay}, nil
			}
		}
	}

	store, err := r.openTopoStore(ctx, cluster.Namespace, globalTopoRef)
	if err != nil {
		if topo.IsTopoUnavailable(err) {
			return r.handleTopoUnavailable(cluster, logger)
		}
		// A missing or unusable client credential is a configuration error, not a
		// transient outage, so record it in the cluster's status as well as an
		// event. The store-open error names the Secret, so the reason a cluster
		// stays not ready is visible without reading the operator logs.
		r.Recorder.Eventf(cluster, "Warning", "TopoConnectFailed",
			"Failed to connect to topology server: %v", err)
		r.markTopologyFailed(ctx, cluster, "TopoConnectFailed", err, logger)
		return ctrl.Result{}, fmt.Errorf("failed to open topology store: %w", err)
	}
	defer func() { _ = store.Close() }()

	topoCtx, cancel := context.WithTimeout(ctx, topoOperationTimeout)
	defer cancel()

	// Register all cells.
	for _, cellCfg := range cluster.Spec.Cells {
		if err := topo.RegisterCellFromSpec(
			topoCtx,
			store,
			r.Recorder,
			cluster,
			cellCfg,
			localTopoSpecs[cellCfg.Name],
			globalTopoRef,
			topo.ManagedLocalTopoServerAddress(
				name.JoinWithConstraints(
					name.DefaultConstraints, cluster.Name, string(cellCfg.Name)),
				cluster.Namespace,
			),
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

		cellNames := specCellNames(cluster)
		if preservePendingCells {
			var err error
			cellNames, err = r.cellNamesToKeepInTopology(ctx, cluster)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := topo.PruneCells(topoCtx, store, r.Recorder, cluster, cellNames); err != nil {
			if topo.IsTopoUnavailable(err) {
				return r.handleTopoUnavailable(cluster, logger)
			}
			return ctrl.Result{}, fmt.Errorf("failed to prune cells: %w", err)
		}
	}

	// Registration reached the topology server, so clear any earlier connection
	// failure. The change is persisted by the later status update on this
	// reconcile; steady state leaves the condition untouched.
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionTopologyReady,
		Status:             metav1.ConditionTrue,
		Reason:             "TopoConnected",
		Message:            "Topology server reachable",
		ObservedGeneration: cluster.Generation,
		LastTransitionTime: metav1.Now(),
	})

	return ctrl.Result{}, nil
}

// conditionTopologyReady reports whether the operator can reach the topology
// server with the configured credentials.
const conditionTopologyReady = "TopologyReady"

// markTopologyFailed sets Degraded and TopologyReady=False.
// Status write failures are logged; reconciliation will retry the original error.
func (r *MultigresClusterReconciler) markTopologyFailed(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	reason string,
	cause error,
	logger interface{ Error(error, string, ...any) },
) {
	base := cluster.DeepCopy()

	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               conditionTopologyReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            cause.Error(),
		ObservedGeneration: cluster.Generation,
		LastTransitionTime: metav1.Now(),
	})
	cluster.Status.Phase = multigresv1alpha1.PhaseDegraded
	cluster.Status.Message = cause.Error()

	// A merge patch under the same field owner status.go applies with, rather
	// than a bare Update, and the two halves of that matter separately.
	//
	// The owner is what stops this being a second manager fighting the first.
	// status.go server-side-applies the whole status as multigres-operator
	// with ForceOwnership, and phase, message and conditions are exactly the
	// fields both write, so an unowned Update made the API server attribute
	// them to a manager derived from the User-Agent and the two took turns.
	//
	// The merge patch is what stops it conflicting with this controller's own
	// earlier writes. Update sends the whole object with the resourceVersion
	// of a possibly stale in-memory copy, so it can 409 against a status
	// write made earlier in the same pass. A merge patch carries no
	// resourceVersion and touches only the fields set above, which is also
	// why this is not a server-side apply: applying a partial status as
	// multigres-operator would delete every other field that manager owns.
	//
	// This path is not rare. It fires whenever topology is unreachable.
	if err := r.Status().Patch(
		ctx,
		cluster,
		client.MergeFrom(base),
		client.FieldOwner("multigres-operator"),
	); err != nil {
		logger.Error(err, "Failed to persist topology failure to status")
	}
}

func specCellNames(cluster *multigresv1alpha1.MultigresCluster) []string {
	names := make([]string, 0, len(cluster.Spec.Cells))
	for _, c := range cluster.Spec.Cells {
		names = append(names, string(c.Name))
	}
	return names
}

func (r *MultigresClusterReconciler) cellNamesToKeepInTopology(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) ([]string, error) {
	names := make(map[string]struct{}, len(cluster.Spec.Cells))
	for _, c := range cluster.Spec.Cells {
		names[string(c.Name)] = struct{}{}
	}

	cells := &multigresv1alpha1.CellList{}
	if err := r.List(ctx, cells,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{metadata.LabelMultigresCluster: cluster.Name},
	); err != nil {
		return nil, fmt.Errorf("failed to list cells for topology pruning: %w", err)
	}
	for i := range cells.Items {
		cell := &cells.Items[i]
		if cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			continue
		}
		names[string(cell.Spec.Name)] = struct{}{}
	}

	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

func (r *MultigresClusterReconciler) managedLocalTopoServerReady(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
	cellCfg multigresv1alpha1.CellConfig,
) (bool, error) {
	cellResourceName := name.JoinWithConstraints(
		name.DefaultConstraints, cluster.Name, string(cellCfg.Name))
	cell := &multigresv1alpha1.Cell{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      cellResourceName,
		Namespace: cluster.Namespace,
	}, cell); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get Cell %q for managed local TopoServer readiness: %w",
			cellCfg.Name, err)
	}

	toposerver := &multigresv1alpha1.TopoServer{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      topo.ManagedLocalTopoServerName(cellResourceName),
		Namespace: cluster.Namespace,
	}, toposerver); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get managed local TopoServer for cell %q: %w",
			cellCfg.Name, err)
	}
	controller := metav1.GetControllerOf(toposerver)
	if controller == nil || controller.UID != cell.UID {
		return false, nil
	}

	return toposerver.Status.ObservedGeneration == toposerver.Generation &&
		toposerver.Status.Phase == multigresv1alpha1.PhaseHealthy, nil
}

// openTopoStore opens a topology store, using the injected factory for tests
// or the real NewStoreFromRef for production. The namespace is where the
// reference's client TLS Secrets live.
func (r *MultigresClusterReconciler) openTopoStore(
	ctx context.Context,
	namespace string,
	ref multigresv1alpha1.GlobalTopoServerRef,
) (topoclient.Store, error) {
	if r.CreateTopoStore != nil {
		return r.CreateTopoStore(ref)
	}
	// Read the client credential with the uncached reader: the manager's cache
	// only stores tenant-namespace Secrets carrying the operator's managed-by
	// label, and a cert-manager or user-provided topology Secret does not.
	return topo.NewStoreFromRef(ctx, r.APIReader, namespace, ref)
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
