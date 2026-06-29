package tablegroup

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
)

// step models one named phase of the TableGroup reconcile loop. A step either
// continues to the next phase or returns the reconcile result to use.
type step func(context.Context, *reconcileContext) (stepResult, error)

type stepResult struct {
	done   bool
	result ctrl.Result
}

type reconcileContext struct {
	r *TableGroupReconciler

	req   ctrl.Request
	start time.Time

	tg             *multigresv1alpha1.TableGroup
	observedShards *multigresv1alpha1.ShardList
	// activeShardNames holds the names of the desired child Shards applied this
	// reconcile. Only these contribute to status; orphans being pruned do not.
	activeShardNames map[string]bool
	// appliedGeneration is the child generation produced by apply. Status counts
	// a child only after the child has observed that generation.
	appliedGeneration map[string]int64
	oldPhase          multigresv1alpha1.Phase
	pendingDeletion   bool
}

type stepError struct {
	err       error
	eventType string
	reason    string
	msg       string
}

func (e *stepError) Error() string {
	return e.err.Error()
}

func (e *stepError) Unwrap() error {
	return e.err
}

func newStepError(err error, eventType, reason, msg string) *stepError {
	return &stepError{
		err:       err,
		eventType: eventType,
		reason:    reason,
		msg:       msg,
	}
}

func continueStep() stepResult {
	return stepResult{}
}

func doneStep(result ctrl.Result) stepResult {
	return stepResult{
		done:   true,
		result: result,
	}
}

func (r *TableGroupReconciler) runSteps(
	ctx context.Context,
	rc *reconcileContext,
	steps []step,
) (ctrl.Result, error) {
	for _, s := range steps {
		res, err := s(ctx, rc)
		if err != nil {
			return res.result, err
		}
		if res.done {
			return res.result, nil
		}
	}
	return ctrl.Result{}, nil
}

func stepFetchTableGroup(ctx context.Context, rc *reconcileContext) (stepResult, error) {
	tg := &multigresv1alpha1.TableGroup{}
	if err := rc.r.Get(ctx, rc.req.NamespacedName, tg); err != nil {
		if apierrors.IsNotFound(err) {
			return doneStep(ctrl.Result{}), nil
		}
		return stepResult{}, fmt.Errorf("failed to get TableGroup: %w", err)
	}

	rc.tg = tg
	rc.oldPhase = tg.Status.Phase
	return continueStep(), nil
}

func stepHandlePendingDeletion(ctx context.Context, rc *reconcileContext) (stepResult, error) {
	if rc.tg.Annotations[multigresv1alpha1.AnnotationPendingDeletion] == "" {
		return continueStep(), nil
	}

	result, err := rc.r.handlePendingDeletion(ctx, rc.tg)
	if err != nil {
		return stepResult{}, err
	}
	return doneStep(result), nil
}

func stepShortCircuitDeletion(_ context.Context, rc *reconcileContext) (stepResult, error) {
	if rc.tg.GetDeletionTimestamp() != nil {
		return doneStep(ctrl.Result{}), nil
	}
	return continueStep(), nil
}
