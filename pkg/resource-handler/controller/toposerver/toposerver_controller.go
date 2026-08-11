package toposerver

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
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres-operator/pkg/util/status"
)

const (
	// statusRecheckDelay is the interval for periodic re-reconciliation when the
	// TopoServer is not Healthy. This allows detection of pod-level issues like
	// CrashLoopBackOff that don't trigger StatefulSet watch events.
	statusRecheckDelay = 30 * time.Second
)

// TopoServerReconciler reconciles a TopoServer object.
type TopoServerReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// Reconcile manages the etcd StatefulSet, headless service, and client service for a TopoServer.
func (r *TopoServerReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (result ctrl.Result, err error) {
	start := time.Now()
	ctx, span := monitoring.StartReconcileSpan(
		ctx,
		"TopoServer.Reconcile",
		req.Name,
		req.Namespace,
		"TopoServer",
	)
	defer span.End()
	defer func() { monitoring.RecordReconcileError(err, "toposerver", req.Name, req.Namespace) }()
	ctx = monitoring.EnrichLoggerWithTrace(ctx)

	logger := log.FromContext(ctx)
	logger.V(1).Info("reconcile started")

	// Fetch the TopoServer instance
	toposerver := &multigresv1alpha1.TopoServer{}
	if err := r.Get(ctx, req.NamespacedName, toposerver); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("TopoServer resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to get TopoServer")
		return ctrl.Result{}, err
	}

	if !toposerver.DeletionTimestamp.IsZero() {
		logger.Info("TopoServer is being deleted, skipping reconciliation")
		return ctrl.Result{}, nil
	}

	// Validate StorageClass dependency before StatefulSet apply
	if err := r.validateEtcdStorageClassDependency(ctx, toposerver); err != nil {
		if isMissingStorageClassDependency(err) {
			logger.Info("StorageClass dependency missing; requeueing",
				"after", storageClassDependencyRequeue)
			return ctrl.Result{RequeueAfter: storageClassDependencyRequeue}, nil
		}
		monitoring.RecordSpanError(span, err)
		logger.Error(err, "Failed to validate etcd StorageClass")
		return ctrl.Result{}, err
	}

	// Reconcile StatefulSet
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "TopoServer.ReconcileStatefulSet")
		if err := r.reconcileStatefulSet(ctx, toposerver); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile StatefulSet")
			r.Recorder.Eventf(
				toposerver,
				"Warning",
				"FailedApply",
				"Failed to reconcile StatefulSet: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Reconcile headless Service
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "TopoServer.ReconcileHeadlessService")
		if err := r.reconcileHeadlessService(ctx, toposerver); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile headless Service")
			r.Recorder.Eventf(
				toposerver,
				"Warning",
				"FailedApply",
				"Failed to reconcile headless Service: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Reconcile client Service
	{
		ctx, childSpan := monitoring.StartChildSpan(ctx, "TopoServer.ReconcileClientService")
		if err := r.reconcileClientService(ctx, toposerver); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to reconcile client Service")
			r.Recorder.Eventf(
				toposerver,
				"Warning",
				"FailedApply",
				"Failed to reconcile client Service: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	// Update status
	{
		_, childSpan := monitoring.StartChildSpan(ctx, "TopoServer.UpdateStatus")
		if err := r.updateStatus(ctx, toposerver); err != nil {
			monitoring.RecordSpanError(childSpan, err)
			childSpan.End()
			logger.Error(err, "Failed to update status")
			r.Recorder.Eventf(
				toposerver,
				"Warning",
				"StatusError",
				"Failed to update status: %v",
				err,
			)
			return ctrl.Result{}, err
		}
		childSpan.End()
	}

	logger.V(1).Info("reconcile complete", "duration", time.Since(start).String())
	r.Recorder.Event(toposerver, "Normal", "Synced", "Successfully reconciled TopoServer")

	if toposerver.Status.Phase != multigresv1alpha1.PhaseHealthy {
		return ctrl.Result{RequeueAfter: statusRecheckDelay}, nil
	}
	return ctrl.Result{}, nil
}

// reconcileStatefulSet creates or updates the StatefulSet for TopoServer.
func (r *TopoServerReconciler) reconcileStatefulSet(
	ctx context.Context,
	toposerver *multigresv1alpha1.TopoServer,
) error {
	desired, err := BuildStatefulSet(toposerver, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build StatefulSet: %w", err)
	}

	// Server Side Apply
	desired.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind("StatefulSet"))
	if err := r.Patch(
		ctx,
		desired,
		client.Apply,
		client.ForceOwnership,
		client.FieldOwner("multigres-operator"),
	); err != nil {
		return fmt.Errorf("failed to apply StatefulSet: %w", err)
	}

	r.Recorder.Eventf(
		toposerver,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}

// reconcileHeadlessService creates or updates the headless Service for TopoServer.
func (r *TopoServerReconciler) reconcileHeadlessService(
	ctx context.Context,
	toposerver *multigresv1alpha1.TopoServer,
) error {
	desired, err := BuildHeadlessService(toposerver, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build headless Service: %w", err)
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
		return fmt.Errorf("failed to apply headless Service: %w", err)
	}

	r.Recorder.Eventf(
		toposerver,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}

// reconcileClientService creates or updates the client Service for TopoServer.
func (r *TopoServerReconciler) reconcileClientService(
	ctx context.Context,
	toposerver *multigresv1alpha1.TopoServer,
) error {
	desired, err := BuildClientService(toposerver, r.Scheme)
	if err != nil {
		return fmt.Errorf("failed to build client Service: %w", err)
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
		return fmt.Errorf("failed to apply client Service: %w", err)
	}

	r.Recorder.Eventf(
		toposerver,
		"Normal",
		"Applied",
		"Applied %s %s",
		desired.GroupVersionKind().Kind,
		desired.Name,
	)

	return nil
}

// updateStatus updates the TopoServer status based on observed state.
func (r *TopoServerReconciler) updateStatus(
	ctx context.Context,
	toposerver *multigresv1alpha1.TopoServer,
) error {
	oldPhase := toposerver.Status.Phase
	// Get the StatefulSet to check status
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKey{Namespace: toposerver.Namespace, Name: toposerver.Name}, sts)
	if err != nil {
		if errors.IsNotFound(err) {
			// StatefulSet not created yet
			return nil
		}
		return fmt.Errorf("failed to get StatefulSet for status: %w", err)
	}

	// Set service names
	toposerver.Status.ClientService = toposerver.Name
	toposerver.Status.PeerService = toposerver.Name + "-headless"

	// Update conditions
	r.setConditions(toposerver, sts)

	monitoring.SetTopoServerReplicas(
		toposerver.Name,
		toposerver.Namespace,
		sts.Status.Replicas,
		sts.Status.ReadyReplicas,
	)

	// List pods for crash-loop detection
	clusterName := toposerver.Labels[metadata.LabelMultigresCluster]
	tsLabels := metadata.BuildStandardLabels(clusterName, ComponentName)
	tsLabels = metadata.MergeLabels(tsLabels, toposerver.GetObjectMeta().GetLabels())
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(toposerver.Namespace),
		client.MatchingLabels(metadata.GetSelectorLabels(tsLabels)),
	); err != nil {
		return fmt.Errorf("failed to list toposerver pods for status: %w", err)
	}

	// Update Phase
	var totalReplicas int32
	if sts.Spec.Replicas != nil {
		totalReplicas = *sts.Spec.Replicas
	}
	result := status.ComputeWorkloadPhase(status.WorkloadPhaseInput{
		Ready:              sts.Status.ReadyReplicas,
		Total:              totalReplicas,
		GenerationCurrent:  sts.Generation,
		GenerationObserved: sts.Status.ObservedGeneration,
		Pods:               podList.Items,
		ComponentName:      "StatefulSet",
	})
	toposerver.Status.Phase = result.Phase
	toposerver.Status.Message = result.Message

	toposerver.Status.ObservedGeneration = toposerver.Generation

	// 1. Construct the Patch Object
	readyCond := meta.FindStatusCondition(toposerver.Status.Conditions, "Ready")
	var patchConditions []metav1.Condition
	if readyCond != nil {
		patchConditions = []metav1.Condition{*readyCond}
	}

	patchObj := &multigresv1alpha1.TopoServer{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "TopoServer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      toposerver.Name,
			Namespace: toposerver.Namespace,
		},
		Status: multigresv1alpha1.TopoServerStatus{
			Phase:              toposerver.Status.Phase,
			Message:            toposerver.Status.Message,
			ObservedGeneration: toposerver.Status.ObservedGeneration,
			ClientService:      toposerver.Status.ClientService,
			PeerService:        toposerver.Status.PeerService,
			Conditions:         patchConditions,
		},
	}

	// 2. Apply the Patch
	if oldPhase != toposerver.Status.Phase {
		r.Recorder.Eventf(
			toposerver,
			"Normal",
			"PhaseChange",
			"Transitioned from '%s' to '%s'",
			oldPhase,
			toposerver.Status.Phase,
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

// setConditions creates status conditions based on observed state.
func (r *TopoServerReconciler) setConditions(
	toposerver *multigresv1alpha1.TopoServer,
	sts *appsv1.StatefulSet,
) {
	// Ready condition
	readyCondition := metav1.Condition{
		Type:               "Ready",
		ObservedGeneration: toposerver.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             "NotAllReplicasReady",
		Message: fmt.Sprintf(
			"%d/%d replicas ready",
			sts.Status.ReadyReplicas,
			*sts.Spec.Replicas,
		),
	}

	if sts.Status.ObservedGeneration == sts.Generation &&
		sts.Status.ReadyReplicas == *sts.Spec.Replicas &&
		*sts.Spec.Replicas > 0 {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "AllReplicasReady"
		readyCondition.Message = fmt.Sprintf("All %d replicas are ready", sts.Status.ReadyReplicas)
	} else if sts.Status.ObservedGeneration != sts.Generation {
		readyCondition.Message = "StatefulSet update in progress"
	}

	meta.SetStatusCondition(&toposerver.Status.Conditions, readyCondition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *TopoServerReconciler) SetupWithManager(
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
		For(&multigresv1alpha1.TopoServer{}, builder.WithPredicates(
			predicate.GenerationChangedPredicate{},
		)).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		WithOptions(controllerOpts).
		Complete(r)
}
