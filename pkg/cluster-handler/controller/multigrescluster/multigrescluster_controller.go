package multigrescluster

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"go.opentelemetry.io/otel/trace"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/resolver"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// MultigresClusterReconciler reconciles a MultigresCluster object.
type MultigresClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// CreateTopoStore overrides the default topology store factory for testing.
	CreateTopoStore func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error)
}

// Reconcile resolves templates, reconciles child resources (Cells, TableGroups, TopoServer),
// and updates the MultigresCluster status and tracking labels.
//
// +kubebuilder:rbac:groups=multigres.com,resources=multigresclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=multigresclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multigres.com,resources=multigresclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=multigres.com,resources=coretemplates;celltemplates;shardtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=multigres.com,resources=cells;tablegroups;toposervers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=shards,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
func (r *MultigresClusterReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	start := time.Now()
	ctx, span := monitoring.StartReconcileSpan(
		ctx,
		"MultigresCluster.Reconcile",
		req.Name,
		req.Namespace,
		"MultigresCluster",
	)
	defer span.End()
	ctx = monitoring.EnrichLoggerWithTrace(ctx)

	l := log.FromContext(ctx)
	l.V(1).Info("reconcile started")

	cluster := &multigresv1alpha1.MultigresCluster{}
	err := r.Get(ctx, req.NamespacedName, cluster)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		monitoring.RecordSpanError(span, err)
		return ctrl.Result{}, fmt.Errorf("failed to get MultigresCluster: %w", err)
	}

	// Bridge the async webhook → reconcile trace gap.
	// If the webhook injected a traceparent into the cluster's annotations,
	// restart the span under that parent context (or link if stale).
	if parentCtx, isStale := monitoring.ExtractTraceContext(
		cluster.GetAnnotations(),
	); trace.SpanFromContext(parentCtx).
		SpanContext().
		IsValid() {
		span.End() // End the initial orphan span.
		if isStale {
			ctx, span = monitoring.Tracer.Start(ctx, "MultigresCluster.Reconcile",
				trace.WithLinks(trace.LinkFromContext(parentCtx)),
			)
		} else {
			ctx, span = monitoring.StartReconcileSpan(
				parentCtx,
				"MultigresCluster.Reconcile",
				req.Name,
				req.Namespace,
				"MultigresCluster",
			)
		}
		defer span.End()
		ctx = monitoring.EnrichLoggerWithTrace(ctx)
		l = log.FromContext(ctx)
	}

	res := resolver.NewResolver(r.Client, cluster.Namespace)

	// Apply defaults (in-memory) to ensure we have images/configs/system-catalog even if webhook didn't run.
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "MultigresCluster.PopulateDefaults")
		decisions, err := res.PopulateClusterDefaults(ctx, cluster)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			l.Error(err, "Failed to populate cluster defaults")
			r.Recorder.Eventf(
				cluster,
				"Warning",
				"FailedApply",
				"Failed to populate cluster defaults: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()

		for _, decision := range decisions {
			r.Recorder.Event(cluster, "Normal", "ImplicitDefault", decision)
		}
	}

	// Apply tracking labels for efficient webhook template-in-use lookups.
	{
		trackingLabels := collectTrackingLabels(cluster)
		labelsChanged := false
		if cluster.Labels == nil {
			cluster.Labels = make(map[string]string)
		}
		patch := client.MergeFrom(cluster.DeepCopy())
		trackingKeys := []string{
			metadata.LabelUsesCoreTemplate,
			metadata.LabelUsesCellTemplate,
			metadata.LabelUsesShardTemplate,
		}
		for _, key := range trackingKeys {
			if want, ok := trackingLabels[key]; ok {
				if cluster.Labels[key] != want {
					cluster.Labels[key] = want
					labelsChanged = true
				}
			} else if _, exists := cluster.Labels[key]; exists {
				delete(cluster.Labels, key)
				labelsChanged = true
			}
		}
		if labelsChanged {
			if err := r.Patch(ctx, cluster, patch); err != nil {
				l.Error(err, "Failed to patch tracking labels")
				return ctrl.Result{}, fmt.Errorf("failed to patch tracking labels: %w", err)
			}
			l.V(1).Info("Patched tracking labels on cluster")
		}
	}

	if !cluster.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, cluster)
	}

	{
		ctx, childSpan := monitoring.StartChildSpan(
			ctx,
			"MultigresCluster.ReconcileGlobalComponents",
		)
		if err := r.reconcileGlobalComponents(ctx, cluster, res); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			l.Error(err, "Failed to reconcile global components")
			r.Recorder.Eventf(
				cluster,
				"Warning",
				"FailedApply",
				"Failed to reconcile global components: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	{
		ctx, childSpan := monitoring.StartChildSpan(
			ctx,
			"MultigresCluster.ReconcileCertificate",
		)
		if err := r.reconcileCertificate(ctx, cluster); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			l.Error(err, "Failed to reconcile TLS Certificate")
			r.Recorder.Eventf(
				cluster,
				"Warning",
				"FailedApply",
				"Failed to reconcile TLS Certificate: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	var pendingCells, pendingDBs bool

	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "MultigresCluster.ReconcileCells")
		var err error
		pendingCells, err = r.reconcileCells(ctx, cluster, res)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			l.Error(err, "Failed to reconcile cells")
			r.Recorder.Eventf(
				cluster,
				"Warning",
				"FailedApply",
				"Failed to reconcile cells: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "MultigresCluster.ReconcileTopology")
		result, err := r.reconcileTopology(ctx, cluster, res, pendingCells)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			l.Error(err, "Failed to reconcile topology")
			r.Recorder.Eventf(
				cluster,
				"Warning",
				"FailedApply",
				"Failed to reconcile topology: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
		if result.RequeueAfter > 0 {
			return result, nil
		}
	}

	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "MultigresCluster.ReconcileDatabases")
		var err error
		pendingDBs, err = r.reconcileDatabases(ctx, cluster, res)
		if err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			l.Error(err, "Failed to reconcile databases")
			r.Recorder.Eventf(
				cluster,
				"Warning",
				"FailedApply",
				"Failed to reconcile databases: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	cluster.Status.ResolvedTemplates = collectResolvedTemplates(cluster)

	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "MultigresCluster.UpdateStatus")
		if err := r.updateStatus(ctx, cluster); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			l.Error(err, "Failed to update status")
			r.Recorder.Eventf(cluster, "Warning", "FailedApply", "Failed to update status: %v", err)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Emit cluster-level metrics
	monitoring.SetClusterInfo(cluster.Name, cluster.Namespace, string(cluster.Status.Phase))
	var totalShards int
	for _, db := range cluster.Status.Databases {
		totalShards += int(db.TotalShards)
	}
	monitoring.SetClusterTopology(
		cluster.Name,
		cluster.Namespace,
		len(cluster.Status.Cells),
		totalShards,
	)

	if pendingCells || pendingDBs {
		l.V(1).Info("Pending graceful deletions, requeueing",
			"duration", time.Since(start).String())
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	l.V(1).Info("reconcile complete", "duration", time.Since(start).String())
	r.Recorder.Event(cluster, "Normal", "Synced", "Successfully reconciled MultigresCluster")
	return ctrl.Result{}, nil
}

// handleDeletion performs best-effort cleanup by deleting Cells and TableGroups.
// Without finalizers, Kubernetes GC cascade via ownerReferences handles the rest.
func (r *MultigresClusterReconciler) handleDeletion(
	ctx context.Context,
	cluster *multigresv1alpha1.MultigresCluster,
) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	clusterLabels := client.MatchingLabels{metadata.LabelMultigresCluster: cluster.Name}
	ns := client.InNamespace(cluster.Namespace)

	// Delete all Cells owned by this cluster.
	cells := &multigresv1alpha1.CellList{}
	if err := r.List(ctx, cells, ns, clusterLabels); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list cells: %w", err)
	}
	for i := range cells.Items {
		if cells.Items[i].DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, &cells.Items[i]); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf(
					"failed to delete cell %q: %w",
					cells.Items[i].Name,
					err,
				)
			}
			l.Info("Initiated cell deletion", "cell", cells.Items[i].Name)
		}
	}

	// Delete all TableGroups owned by this cluster (cascades to Shards).
	tableGroups := &multigresv1alpha1.TableGroupList{}
	if err := r.List(ctx, tableGroups, ns, clusterLabels); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list tablegroups: %w", err)
	}
	for i := range tableGroups.Items {
		if tableGroups.Items[i].DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, &tableGroups.Items[i]); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf(
					"failed to delete tablegroup %q: %w",
					tableGroups.Items[i].Name,
					err,
				)
			}
			l.Info("Initiated tablegroup deletion", "tablegroup", tableGroups.Items[i].Name)
		}
	}

	l.Info("Cluster best-effort cleanup complete")
	r.Recorder.Event(cluster, "Normal", "CleanupComplete", "Initiated deletion of child resources")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MultigresClusterReconciler) SetupWithManager(
	mgr ctrl.Manager,
	opts ...controller.Options,
) error {
	controllerOpts := controller.Options{
		MaxConcurrentReconciles: 20,
	}
	if len(opts) > 0 {
		controllerOpts = opts[0]
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&multigresv1alpha1.MultigresCluster{},
			builder.WithPredicates(projectRefOrGenerationChangedPredicate()),
		).
		Owns(&multigresv1alpha1.Cell{}).
		Owns(&multigresv1alpha1.TableGroup{}).
		Owns(&multigresv1alpha1.TopoServer{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Watches(
			&multigresv1alpha1.CoreTemplate{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueRequestsFromTemplate),
		).
		Watches(
			&multigresv1alpha1.CellTemplate{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueRequestsFromTemplate),
		).
		Watches(
			&multigresv1alpha1.ShardTemplate{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueRequestsFromTemplate),
		).
		WithOptions(controllerOpts).
		Complete(r)
}

// projectRefOrGenerationChangedPredicate requeues when desired child state can
// change due to either spec updates or project-ref annotation updates.
func projectRefOrGenerationChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}

			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}

			oldAnnotations := e.ObjectOld.GetAnnotations()
			newAnnotations := e.ObjectNew.GetAnnotations()
			return oldAnnotations[metadata.AnnotationProjectRef] !=
				newAnnotations[metadata.AnnotationProjectRef]
		},
		GenericFunc: func(event.GenericEvent) bool {
			return true
		},
	}
}

// enqueueRequestsFromTemplate returns reconcile requests only for clusters
// whose status.resolvedTemplates references the changed template.
func (r *MultigresClusterReconciler) enqueueRequestsFromTemplate(
	ctx context.Context,
	o client.Object,
) []reconcile.Request {
	templateKind := templateKindFromObject(o)
	if templateKind == "" {
		return nil
	}

	clusters := &multigresv1alpha1.MultigresClusterList{}
	if err := r.List(ctx, clusters, client.InNamespace(o.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, c := range clusters.Items {
		if referencesTemplate(c.Status.ResolvedTemplates, templateKind, o.GetName()) {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&c),
			})
		}
	}
	return requests
}

// templateKindFromObject determines the template kind from the Go type.
// Controller-runtime strips GVK from informer objects, so we use a type switch.
func templateKindFromObject(o client.Object) string {
	switch o.(type) {
	case *multigresv1alpha1.CoreTemplate:
		return "CoreTemplate"
	case *multigresv1alpha1.CellTemplate:
		return "CellTemplate"
	case *multigresv1alpha1.ShardTemplate:
		return "ShardTemplate"
	default:
		return ""
	}
}

// referencesTemplate checks whether a cluster's resolved templates include
// the given template kind and name. Returns true for nil status (never-reconciled
// clusters are always enqueued as a safe default).
func referencesTemplate(rt *multigresv1alpha1.ResolvedTemplates, kind, name string) bool {
	if rt == nil {
		return true
	}
	switch kind {
	case "CoreTemplate":
		return slices.Contains(rt.CoreTemplates, multigresv1alpha1.TemplateRef(name))
	case "CellTemplate":
		return slices.Contains(rt.CellTemplates, multigresv1alpha1.TemplateRef(name))
	case "ShardTemplate":
		return slices.Contains(rt.ShardTemplates, multigresv1alpha1.TemplateRef(name))
	}
	return false
}

// collectResolvedTemplates walks the cluster spec and returns the
// deduplicated set of template names referenced at every level.
func collectResolvedTemplates(
	cluster *multigresv1alpha1.MultigresCluster,
) *multigresv1alpha1.ResolvedTemplates {
	rt := &multigresv1alpha1.ResolvedTemplates{}

	// Core templates referenced from multiple spec locations.
	coreSet := map[multigresv1alpha1.TemplateRef]struct{}{}
	if ref := cluster.Spec.TemplateDefaults.CoreTemplate; ref != "" {
		coreSet[ref] = struct{}{}
	}
	if gts := cluster.Spec.GlobalTopoServer; gts != nil && gts.TemplateRef != "" {
		coreSet[gts.TemplateRef] = struct{}{}
	}
	if ma := cluster.Spec.Multiadmin; ma != nil && ma.TemplateRef != "" {
		coreSet[ma.TemplateRef] = struct{}{}
	}
	if maw := cluster.Spec.MultiadminWeb; maw != nil && maw.TemplateRef != "" {
		coreSet[maw.TemplateRef] = struct{}{}
	}
	for ref := range coreSet {
		rt.CoreTemplates = append(rt.CoreTemplates, ref)
	}
	slices.Sort(rt.CoreTemplates)

	// Cell templates.
	cellSet := map[multigresv1alpha1.TemplateRef]struct{}{}
	if ref := cluster.Spec.TemplateDefaults.CellTemplate; ref != "" {
		cellSet[ref] = struct{}{}
	}
	for _, cell := range cluster.Spec.Cells {
		if cell.CellTemplate != "" {
			cellSet[cell.CellTemplate] = struct{}{}
		}
	}
	for ref := range cellSet {
		rt.CellTemplates = append(rt.CellTemplates, ref)
	}
	slices.Sort(rt.CellTemplates)

	// Shard templates.
	shardSet := map[multigresv1alpha1.TemplateRef]struct{}{}
	if ref := cluster.Spec.TemplateDefaults.ShardTemplate; ref != "" {
		shardSet[ref] = struct{}{}
	}
	for _, db := range cluster.Spec.Databases {
		for _, tg := range db.TableGroups {
			for _, shard := range tg.Shards {
				if shard.ShardTemplate != "" {
					shardSet[shard.ShardTemplate] = struct{}{}
				}
			}
		}
	}
	for ref := range shardSet {
		rt.ShardTemplates = append(rt.ShardTemplates, ref)
	}
	slices.Sort(rt.ShardTemplates)

	return rt
}

// collectTrackingLabels walks the cluster spec and returns boolean tracking
// labels indicating which template kinds are referenced. These labels enable
// efficient pre-filtering in the template deletion webhook.
func collectTrackingLabels(cluster *multigresv1alpha1.MultigresCluster) map[string]string {
	labels := make(map[string]string)

	usesCore := cluster.Spec.TemplateDefaults.CoreTemplate != "" ||
		(cluster.Spec.GlobalTopoServer != nil && cluster.Spec.GlobalTopoServer.TemplateRef != "") ||
		(cluster.Spec.Multiadmin != nil && cluster.Spec.Multiadmin.TemplateRef != "") ||
		(cluster.Spec.MultiadminWeb != nil && cluster.Spec.MultiadminWeb.TemplateRef != "")
	if usesCore {
		labels[metadata.LabelUsesCoreTemplate] = "true"
	}

	usesCell := cluster.Spec.TemplateDefaults.CellTemplate != ""
	if !usesCell {
		for _, cell := range cluster.Spec.Cells {
			if cell.CellTemplate != "" {
				usesCell = true
				break
			}
		}
	}
	if usesCell {
		labels[metadata.LabelUsesCellTemplate] = "true"
	}

	usesShard := cluster.Spec.TemplateDefaults.ShardTemplate != ""
	if !usesShard {
		for _, db := range cluster.Spec.Databases {
			for _, tg := range db.TableGroups {
				for _, s := range tg.Shards {
					if s.ShardTemplate != "" {
						usesShard = true
						break
					}
				}
				if usesShard {
					break
				}
			}
			if usesShard {
				break
			}
		}
	}
	if usesShard {
		labels[metadata.LabelUsesShardTemplate] = "true"
	}

	return labels
}
