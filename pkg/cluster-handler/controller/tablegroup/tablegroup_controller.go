package tablegroup

import (
	"context"
	"errors"
	"fmt"
	"time"

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
)

// TableGroupReconciler reconciles a TableGroup object.
type TableGroupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// Reconcile reads the state of the TableGroup and ensures its child Shards are in the desired state.
//
// +kubebuilder:rbac:groups=multigres.com,resources=tablegroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=multigres.com,resources=tablegroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=multigres.com,resources=tablegroups/finalizers,verbs=update
// +kubebuilder:rbac:groups=multigres.com,resources=shards,verbs=get;list;watch;create;update;patch;delete
func (r *TableGroupReconciler) Reconcile(
	ctx context.Context,
	req ctrl.Request,
) (ctrl.Result, error) {
	start := time.Now()
	ctx, span := monitoring.StartReconcileSpan(
		ctx,
		"TableGroup.Reconcile",
		req.Name,
		req.Namespace,
		"TableGroup",
	)
	defer span.End()
	ctx = monitoring.EnrichLoggerWithTrace(ctx)

	l := log.FromContext(ctx)
	l.V(1).Info("reconcile started")

	rc := &reconcileContext{
		r:     r,
		req:   req,
		start: start,
	}

	// The order preserves the controller lifecycle: fetch the parent, handle
	// parent deletion before normal work, read the child snapshot once, apply the
	// desired children, retire orphans through their drain handshake, then publish
	// parent status from the observed child state.
	rawSteps := []step{
		stepFetchTableGroup,
		stepHandlePendingDeletion,
		stepShortCircuitDeletion,
		stepListChildShards,
		stepApplyDesiredShards,
		stepReconcileUndesired,
		stepComputeStatus,
		stepPatchStatus,
		stepRequeueIfPending,
	}

	recordSpanErr := func(err error) { monitoring.RecordSpanError(span, err) }
	steps := make([]step, len(rawSteps))
	for i, s := range rawSteps {
		steps[i] = withStepErrorHandling(recordSpanErr, s)
	}

	return r.runSteps(ctx, rc, steps)
}

// withStepErrorHandling records a failing step's error on the span and, for a
// stepError with a non-empty reason, logs it and emits the event once the parent
// object has been fetched. Normal events are emitted inline by the steps.
func withStepErrorHandling(recordSpanErr func(error), s step) step {
	return func(ctx context.Context, rc *reconcileContext) (stepResult, error) {
		res, err := s(ctx, rc)
		if err != nil {
			recordSpanErr(err)

			var se *stepError
			if errors.As(err, &se) && se.reason != "" {
				log.FromContext(ctx).Error(se.err, se.msg)
				if rc.tg != nil {
					rc.r.Recorder.Eventf(rc.tg, se.eventType, se.reason, "%s", se.msg)
				}
			}
		}
		return res, err
	}
}

// handlePendingDeletion propagates PendingDeletion to all child Shards and sets
// ReadyForDeletion on this TableGroup once every child reports ready. This
// early-return path lists children itself because the normal child snapshot has
// not been read yet.
func (r *TableGroupReconciler) handlePendingDeletion(
	ctx context.Context,
	tg *multigresv1alpha1.TableGroup,
) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	shards := &multigresv1alpha1.ShardList{}
	if err := r.List(ctx, shards, childShardSelector(tg)...); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list shards for pending deletion: %w", err)
	}

	allReady := true
	for i := range shards.Items {
		s := &shards.Items[i]

		if s.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
			if err := r.setShardPendingDeletion(ctx, s); err != nil {
				return ctrl.Result{}, fmt.Errorf(
					"failed to set PendingDeletion on shard '%s': %w", s.Name, err)
			}
			r.Recorder.Eventf(tg, "Normal", "PendingDeletion",
				"Marked Shard %s for graceful deletion (parent pending)", s.Name)
			allReady = false
			continue
		}

		if !meta.IsStatusConditionTrue(
			s.Status.Conditions,
			multigresv1alpha1.ConditionReadyForDeletion,
		) {
			allReady = false
		}
	}

	if !allReady {
		l.V(1).Info("Waiting for child Shards to be ready for deletion")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	meta.SetStatusCondition(&tg.Status.Conditions, metav1.Condition{
		Type:               multigresv1alpha1.ConditionReadyForDeletion,
		Status:             metav1.ConditionTrue,
		Reason:             "AllShardsReady",
		Message:            "All child Shards are ready for deletion",
		ObservedGeneration: tg.Generation,
	})

	patchObj := &multigresv1alpha1.TableGroup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: multigresv1alpha1.GroupVersion.String(),
			Kind:       "TableGroup",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      tg.Name,
			Namespace: tg.Namespace,
		},
		Status: tg.Status,
	}
	if err := r.Status().Patch(ctx, patchObj, client.Apply,
		client.FieldOwner("multigres-operator"), client.ForceOwnership); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set ReadyForDeletion on TableGroup: %w", err)
	}

	r.Recorder.Eventf(tg, "Normal", "ReadyForDeletion",
		"TableGroup %s marked ready for deletion", tg.Name)
	return ctrl.Result{}, nil
}

// setShardPendingDeletion stamps the PendingDeletion annotation with an RFC3339
// UTC timestamp. Callers decide when to emit events.
func (r *TableGroupReconciler) setShardPendingDeletion(
	ctx context.Context,
	s *multigresv1alpha1.Shard,
) error {
	patch := client.MergeFrom(s.DeepCopy())
	if s.Annotations == nil {
		s.Annotations = make(map[string]string)
	}
	s.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = metav1.Now().
		UTC().Format(time.RFC3339)
	return r.Patch(ctx, s, patch)
}

// childShardSelector returns the List options for child Shards belonging to the
// given TableGroup.
func childShardSelector(tg *multigresv1alpha1.TableGroup) []client.ListOption {
	return []client.ListOption{
		client.InNamespace(tg.Namespace),
		client.MatchingLabels{
			"multigres.com/cluster":    tg.Labels["multigres.com/cluster"],
			"multigres.com/database":   string(tg.Spec.DatabaseName),
			"multigres.com/tablegroup": string(tg.Spec.TableGroupName),
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *TableGroupReconciler) SetupWithManager(
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
		For(&multigresv1alpha1.TableGroup{},
			builder.WithPredicates(projectRefOrGenerationChangedPredicate()),
		).
		Owns(&multigresv1alpha1.Shard{}).
		WithOptions(controllerOpts).
		Complete(r)
}

// projectRefOrGenerationChangedPredicate requeues when desired shard state can
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
