package cell

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

const (
	// statusRecheckDelay is the interval for periodic re-reconciliation when the
	// Cell is not Healthy. This allows detection of pod-level issues like
	// CrashLoopBackOff that don't trigger Deployment watch events.
	statusRecheckDelay = 30 * time.Second

	// localTopoServerRecheckDelay is the interval for rechecking a managed local
	// TopoServer while waiting to create dependent gateway resources or complete
	// Cell deletion.
	localTopoServerRecheckDelay = 5 * time.Second
)

// CellReconciler reconciles a Cell object.
type CellReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// Reconcile manages the Multigateway deployment and per-cell services for a Cell.
func (r *CellReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := time.Now()
	ctx, span := monitoring.StartReconcileSpan(
		ctx,
		"Cell.Reconcile",
		req.Name,
		req.Namespace,
		"Cell",
	)
	defer span.End()
	ctx = monitoring.EnrichLoggerWithTrace(ctx)

	logger := log.FromContext(ctx)
	logger.V(1).Info("reconcile started")

	// Fetch the Cell instance
	cell := &multigresv1alpha1.Cell{}
	if err := r.Get(ctx, req.NamespacedName, cell); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Cell resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to get Cell")
		return ctrl.Result{}, err
	}

	// If this Cell is pending deletion (from MultigresCluster pruning),
	// wait for any managed local topology server before marking it ready.
	if cell.Annotations[multigresv1alpha1.AnnotationPendingDeletion] != "" {
		return r.handlePendingDeletion(ctx, cell)
	}

	// If being deleted, let Kubernetes GC handle cleanup
	if !cell.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Reconcile managed local TopoServer, when configured.
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Cell.ReconcileLocalTopoServer")
		if err := r.reconcileLocalTopoServer(ctx, cell); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile local TopoServer")
			r.Recorder.Eventf(
				cell,
				"Warning",
				"FailedApply",
				"Failed to reconcile local TopoServer: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}
	if ready, err := r.localTopoServerReady(ctx, cell); err != nil {
		logger.Error(err, "Failed to check local TopoServer readiness")
		return ctrl.Result{}, err
	} else if !ready {
		logger.V(1).Info("Waiting for local TopoServer before reconciling gateway")
		if err := r.patchLocalTopoServerWaitingStatus(ctx, cell); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: localTopoServerRecheckDelay}, nil
	}

	// Reconcile Multigateway Deployment
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Cell.ReconcileDeployment")
		if err := r.reconcileMultigatewayDeployment(ctx, cell); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile Multigateway Deployment")
			r.Recorder.Eventf(
				cell,
				"Warning",
				"FailedApply",
				"Failed to sync Gateway Deployment: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Reconcile Multigateway Service
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "Cell.ReconcileService")
		if err := r.reconcileMultigatewayService(ctx, cell); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile Multigateway Service")
			r.Recorder.Eventf(
				cell,
				"Warning",
				"FailedApply",
				"Failed to reconcile Multigateway Service: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Update status
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "Cell.UpdateStatus")
		if err := r.updateStatus(ctx, cell); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to update status")
			r.Recorder.Eventf(cell, "Warning", "StatusError", "Failed to update status: %v", err)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	logger.V(1).Info("reconcile complete", "duration", time.Since(start).String())
	r.Recorder.Event(cell, "Normal", "Synced", "Successfully reconciled Cell")

	if cell.Status.Phase != multigresv1alpha1.PhaseHealthy {
		return ctrl.Result{RequeueAfter: statusRecheckDelay}, nil
	}
	return ctrl.Result{}, nil
}

// reconcileMultigatewayDeployment creates or updates the Multigateway Deployment.
func (r *CellReconciler) reconcileMultigatewayDeployment(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) error {
	desired, err := BuildMultigatewayDeployment(cell, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build Multigateway Deployment: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("Deployment"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply Multigateway Deployment: %w", err)
	}

	r.Recorder.Eventf(
		cell,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}

// reconcileMultigatewayService creates or updates the Multigateway Service.
func (r *CellReconciler) reconcileMultigatewayService(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) error {
	desired, err := BuildMultigatewayService(cell, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build Multigateway Service: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Service"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply Multigateway Service: %w", err)
	}

	r.Recorder.Eventf(
		cell,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}

// reconcileLocalTopoServer creates or updates the managed local TopoServer for a Cell.
func (r *CellReconciler) reconcileLocalTopoServer(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) error {
	desired, err := BuildLocalTopoServer(cell, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build local TopoServer: %w", err)
	}
	if desired == nil {
		deleted, err := r.deleteOwnedLocalTopoServerIfExists(ctx, cell)
		if err != nil {
			return err
		}
		if deleted {
			r.Recorder.Eventf(cell, "Normal", "Deleted",
				"Deleted stale local TopoServer %s", BuildLocalTopoServerName(cell))
		}
		return nil
	}

	existing := &multigresv1alpha1.TopoServer{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      desired.Name,
		Namespace: desired.Namespace,
	}, existing); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to get local TopoServer: %w", err)
		}
	} else if !isControlledByCell(existing, cell) {
		return fmt.Errorf("refusing to manage local TopoServer %s/%s not controlled by Cell %s/%s",
			existing.Namespace, existing.Name, cell.Namespace, cell.Name)
	}

	desired.SetGroupVersionKind(multigresv1alpha1.GroupVersion.WithKind("TopoServer"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply local TopoServer: %w", err)
	}

	r.Recorder.Eventf(
		cell,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}

func (r *CellReconciler) localTopoServerReady(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) (bool, error) {
	if !hasManagedLocalTopoServer(cell) {
		return true, nil
	}

	toposerver := &multigresv1alpha1.TopoServer{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      BuildLocalTopoServerName(cell),
		Namespace: cell.Namespace,
	}, toposerver); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get local TopoServer: %w", err)
	}
	if !isControlledByCell(toposerver, cell) {
		return false, fmt.Errorf(
			"refusing to use local TopoServer %s/%s not controlled by Cell %s/%s",
			toposerver.Namespace,
			toposerver.Name,
			cell.Namespace,
			cell.Name,
		)
	}

	return toposerver.Status.ObservedGeneration == toposerver.Generation &&
		toposerver.Status.Phase == multigresv1alpha1.PhaseHealthy, nil
}

func (r *CellReconciler) handlePendingDeletion(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) (ctrl.Result, error) {
	reason := "StatelessComponent"
	message := "Cell has no managed local TopoServer; ready for deletion"

	absent, err := r.ensureLocalTopoServerDeleted(ctx, cell)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !absent {
		if err := r.patchReadyForDeletionCondition(ctx, cell, metav1.Condition{
			Type:               multigresv1alpha1.ConditionReadyForDeletion,
			Status:             metav1.ConditionFalse,
			Reason:             "LocalTopoServerDeleting",
			Message:            "Waiting for managed local TopoServer deletion",
			ObservedGeneration: cell.Generation,
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: localTopoServerRecheckDelay}, nil
	}

	if hasManagedLocalTopoServer(cell) {
		reason = "LocalTopoServerDeleted"
		message = "Managed local TopoServer is deleted; ready for deletion"
	}

	if !meta.IsStatusConditionTrue(
		cell.Status.Conditions,
		multigresv1alpha1.ConditionReadyForDeletion,
	) {
		if err := r.patchReadyForDeletionCondition(ctx, cell, metav1.Condition{
			Type:               multigresv1alpha1.ConditionReadyForDeletion,
			Status:             metav1.ConditionTrue,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: cell.Generation,
		}); err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Eventf(cell, "Normal", "ReadyForDeletion",
			"Cell %s marked ready for deletion", cell.Name)
	}
	return ctrl.Result{}, nil
}

func (r *CellReconciler) ensureLocalTopoServerDeleted(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) (bool, error) {
	deleted, err := r.deleteLocalTopoServerIfExists(ctx, cell)
	if err != nil {
		return false, err
	}
	return !deleted, nil
}

func (r *CellReconciler) deleteOwnedLocalTopoServerIfExists(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) (bool, error) {
	toposerver := &multigresv1alpha1.TopoServer{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      BuildLocalTopoServerName(cell),
		Namespace: cell.Namespace,
	}, toposerver); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get local TopoServer: %w", err)
	}
	if !isControlledByCell(toposerver, cell) {
		return false, nil
	}
	if !toposerver.DeletionTimestamp.IsZero() {
		return true, nil
	}
	if err := r.Delete(ctx, toposerver); err != nil && !errors.IsNotFound(err) {
		return false, fmt.Errorf("failed to delete local TopoServer: %w", err)
	}
	return true, nil
}

func (r *CellReconciler) deleteLocalTopoServerIfExists(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) (bool, error) {
	toposerver := &multigresv1alpha1.TopoServer{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      BuildLocalTopoServerName(cell),
		Namespace: cell.Namespace,
	}, toposerver); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get local TopoServer: %w", err)
	}
	if !isControlledByCell(toposerver, cell) {
		return false, fmt.Errorf(
			"refusing to delete local TopoServer %s/%s not controlled by Cell %s/%s",
			toposerver.Namespace,
			toposerver.Name,
			cell.Namespace,
			cell.Name,
		)
	}
	if !toposerver.DeletionTimestamp.IsZero() {
		return true, nil
	}
	if err := r.Delete(ctx, toposerver); err != nil && !errors.IsNotFound(err) {
		return false, fmt.Errorf("failed to delete local TopoServer: %w", err)
	}
	return true, nil
}

func (r *CellReconciler) patchReadyForDeletionCondition(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
	condition metav1.Condition,
) error {
	meta.SetStatusCondition(&cell.Status.Conditions, condition)
	patchObj := &multigresv1alpha1.Cell{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "Cell",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cell.Name,
			Namespace: cell.Namespace,
		},
		Status: cell.Status,
	}
	if err := r.Status().Patch(ctx, patchObj, client.Apply,
		client.FieldOwner("multigres-operator"), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to patch ReadyForDeletion on Cell: %w", err)
	}
	return nil
}

func (r *CellReconciler) patchLocalTopoServerWaitingStatus(
	ctx context.Context,
	cell *multigresv1alpha1.Cell,
) error {
	meta.SetStatusCondition(&cell.Status.Conditions, metav1.Condition{
		Type:               "Available",
		Status:             metav1.ConditionFalse,
		Reason:             "LocalTopoServerNotReady",
		Message:            "Waiting for managed local TopoServer to become ready",
		ObservedGeneration: cell.Generation,
	})
	meta.SetStatusCondition(&cell.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "LocalTopoServerNotReady",
		Message:            "Waiting for managed local TopoServer to become ready",
		ObservedGeneration: cell.Generation,
	})
	cell.Status.Phase = multigresv1alpha1.PhaseProgressing
	cell.Status.Message = "Waiting for managed local TopoServer to become ready"
	cell.Status.ObservedGeneration = cell.Generation

	patchObj := &multigresv1alpha1.Cell{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "Cell",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cell.Name,
			Namespace: cell.Namespace,
		},
		Status: cell.Status,
	}
	if err := r.Status().Patch(ctx, patchObj, client.Apply,
		client.FieldOwner("multigres-operator"), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to patch local TopoServer waiting status on Cell: %w", err)
	}
	return nil
}

// updateStatus updates the Cell status based on observed state.
func (r *CellReconciler) updateStatus(ctx context.Context, cell *multigresv1alpha1.Cell) error {
	oldPhase := cell.Status.Phase
	// Get the Multigateway Deployment to check status
	mgDeploy := &appsv1.Deployment{}
	err := r.Get(
		ctx,
		client.ObjectKey{Namespace: cell.Namespace, Name: BuildMultigatewayDeploymentName(cell)},
		mgDeploy,
	)
	if err != nil {
		if errors.IsNotFound(err) {
			// Deployment not created yet
			return nil
		}
		return fmt.Errorf("failed to get Multigateway Deployment for status: %w", err)
	}

	// Update conditions
	r.setConditions(cell, mgDeploy)
	cell.Status.GatewayReplicas = mgDeploy.Status.Replicas
	cell.Status.GatewayReadyReplicas = mgDeploy.Status.ReadyReplicas
	cell.Status.GatewayServiceName = BuildMultigatewayServiceName(cell)
	monitoring.SetCellGatewayReplicas(
		cell.Name,
		cell.Namespace,
		mgDeploy.Status.Replicas,
		mgDeploy.Status.ReadyReplicas,
	)

	// List gateway pods for crash-loop detection
	mgLabels := metadata.BuildStandardLabels(
		cell.Labels[metadata.LabelMultigresCluster], MultigatewayComponentName,
	)
	metadata.AddCellLabel(mgLabels, cell.Spec.Name)
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(cell.Namespace),
		client.MatchingLabels(metadata.GetSelectorLabels(mgLabels)),
	); err != nil {
		return fmt.Errorf("failed to list gateway pods for status: %w", err)
	}

	// Update Phase
	result := status.ComputeWorkloadPhase(status.WorkloadPhaseInput{
		Ready:              mgDeploy.Status.ReadyReplicas,
		Total:              mgDeploy.Status.Replicas,
		GenerationCurrent:  mgDeploy.Generation,
		GenerationObserved: mgDeploy.Status.ObservedGeneration,
		Pods:               podList.Items,
		ComponentName:      "Gateway",
	})
	cell.Status.Phase = result.Phase
	cell.Status.Message = result.Message

	cell.Status.ObservedGeneration = cell.Generation

	// 1. Construct the Patch Object
	patchObj := &multigresv1alpha1.Cell{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "Cell",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      cell.Name,
			Namespace: cell.Namespace,
		},
		Status: cell.Status,
	}

	// 2. Apply the Patch
	if oldPhase != cell.Status.Phase {
		r.Recorder.Eventf(
			cell,
			"Normal",
			"PhaseChange",
			"Transitioned from '%s' to '%s'",
			oldPhase,
			cell.Status.Phase,
		)
	}

	// Note: We rely on Server-Side Apply (SSA) to handle idempotency.
	// If the status hasn't changed, the API server will treat this Patch as a no-op,
	// so we don't need a manual DeepEqual check here.
	if err := r.Status().Patch(
		ctx,
		patchObj,
		client.Apply,
		client.FieldOwner("multigres-operator"),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("failed to patch status: %w", err)
	}

	return nil
}

// setConditions creates status conditions based on observed state using meta.SetStatusCondition.
func (r *CellReconciler) setConditions(
	cell *multigresv1alpha1.Cell,
	mgDeploy *appsv1.Deployment,
) {
	// Available condition - True if at least one replica is ready (serviceable)
	availCond := metav1.Condition{
		Type:               "Available",
		ObservedGeneration: cell.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             "MultigatewayUnavailable",
		Message:            "No ready replicas available",
	}

	if mgDeploy.Status.ReadyReplicas > 0 {
		availCond.Status = metav1.ConditionTrue
		availCond.Reason = "MultigatewayAvailable"
		availCond.Message = fmt.Sprintf(
			"Multigateway %d/%d replicas ready",
			mgDeploy.Status.ReadyReplicas,
			mgDeploy.Status.Replicas,
		)
	}
	meta.SetStatusCondition(&cell.Status.Conditions, availCond)

	// Ready condition - True if all replicas are ready (desired state reached)
	readyCond := metav1.Condition{
		Type:               "Ready",
		ObservedGeneration: cell.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             "MultigatewayNotReady",
		Message: fmt.Sprintf(
			"Multigateway %d/%d ready, waiting for full convergence",
			mgDeploy.Status.ReadyReplicas,
			mgDeploy.Status.Replicas,
		),
	}

	allReady := mgDeploy.Status.ObservedGeneration == mgDeploy.Generation &&
		mgDeploy.Status.ReadyReplicas == mgDeploy.Status.Replicas &&
		mgDeploy.Status.Replicas > 0

	if allReady {
		readyCond.Status = metav1.ConditionTrue
		readyCond.Reason = "MultigatewayReady"
		readyCond.Message = "All replicas match desired state"
	}
	meta.SetStatusCondition(&cell.Status.Conditions, readyCond)
}

// SetupWithManager sets up the controller with the Manager.
func (r *CellReconciler) SetupWithManager(mgr ctrl.Manager, opts ...controller.Options) error {
	controllerOpts := controller.Options{
		MaxConcurrentReconciles: 20,
	}
	if len(opts) > 0 {
		controllerOpts = opts[0]
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&multigresv1alpha1.Cell{},
			builder.WithPredicates(projectRefOrGenerationChangedPredicate()),
		).
		Owns(&multigresv1alpha1.TopoServer{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		WithOptions(controllerOpts).
		Complete(r)
}

// projectRefOrGenerationChangedPredicate requeues when desired gateway state can
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
