package shard

import (
	"context"
	"errors"
	"testing"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// alwaysFailingTopoFactory implements topoclient.Factory the way an
// unreachable etcd actually looks in production: Create always errors.
// topoclient.NewWithFactory (what topo.NewStoreFromShard calls through
// topoclient.OpenServer) is lazy about this: it tries once synchronously,
// swallows the failure into a background retry loop, and returns a valid,
// non-nil Store with a nil error regardless. r.topoStore() therefore never
// errors on a dropped topology connection; every call the reconciler makes
// on the returned store does instead (WrapperConn.getConnection returns
// UNAVAILABLE synchronously once the wrapped connection is nil, no waiting
// required). TestReconcileDataPlaneFailsClosedOnTopologyOutage below builds
// a store this way specifically so it exercises that real shape rather than
// a dial-time error.
type alwaysFailingTopoFactory struct{}

func (alwaysFailingTopoFactory) Create(
	string, string,
	[]string,
) (topoclient.Conn, error) {
	return nil, errors.New("connection refused")
}

// readyPoolPod returns a pool pod fixture already carrying a True
// PoolerDataReady condition, standing in for a pooler that was previously
// observed healthy before topology observation is lost.
func readyPoolPod() *corev1.Pod {
	pod := postureTestPod()
	pod.Labels[metadata.LabelAppComponent] = PoolComponentName
	pod.Labels[metadata.LabelMultigresCell] = "cell1"
	pod.Labels[metadata.LabelMultigresPool] = "default"
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:    PoolerDataReadyCondition,
		Status:  corev1.ConditionTrue,
		Reason:  "DataPlaneReady",
		Message: "PostgreSQL is ready and the pooler is an eligible shard cohort member",
	}}
	return pod
}

// erroringTopoStore wraps a real topoclient.Store but forces
// GetMultipoolersByCell to fail with an error that is not a topo-unavailable
// signal, standing in for a hard posture.Evaluate error (e.g. a malformed
// topology record) rather than an outage.
type erroringTopoStore struct {
	topoclient.Store
	err error
}

func (s erroringTopoStore) GetMultipoolersByCell(
	context.Context,
	string,
	*topoclient.GetMultipoolersByCellOptions,
) ([]*topoclient.MultipoolerInfo, error) {
	return nil, s.err
}

// TestReconcileDataPlaneFailsClosedOnTopoStoreUnavailable covers a
// config-level dial failure: r.topoStore() itself returns an error, which in
// production means a missing or unreadable TLS Secret, or an unknown topology
// implementation name (topo.NewStoreFromShard's own error paths) rather than
// a reachable-but-unresponsive etcd. See
// TestReconcileDataPlaneFailsClosedOnTopologyOutage for the outage shape.
func TestReconcileDataPlaneFailsClosedOnTopoStoreUnavailable(t *testing.T) {
	shard := postureTestShard()
	pod := readyPoolPod()

	r, c := postureTestReconciler(t, shard, nil, pod)
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return nil, errors.New("no connection available")
	}

	result, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{})
	if err != nil {
		t.Fatalf("reconcileDataPlane() error = %v", err)
	}
	if result.RequeueAfter != topoUnavailableRequeueDelay {
		t.Errorf("requeue delay = %v, want %v", result.RequeueAfter, topoUnavailableRequeueDelay)
	}

	updated := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	condition := readinessCondition(updated.Status.Conditions)
	if condition == nil || condition.Status != corev1.ConditionUnknown ||
		condition.Reason != "ObservationUnavailable" {
		t.Fatalf("readiness condition = %#v, want Unknown/ObservationUnavailable", condition)
	}
	before := updated.ResourceVersion

	// A repeat pass with the same lost-observation state must not re-patch
	// the pod: the no-op check in reconcilePoolerReadiness must skip writes
	// once the condition already matches.
	if _, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{}); err != nil {
		t.Fatalf("second reconcileDataPlane() error = %v", err)
	}
	again := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), again); err != nil {
		t.Fatalf("get pod after second pass: %v", err)
	}
	if again.ResourceVersion != before {
		t.Errorf(
			"pod was re-patched on unchanged lost-observation state: resourceVersion %s -> %s",
			before, again.ResourceVersion,
		)
	}

	// Recovery: the next successful observation flips readiness back True.
	store, poolerID := postureTestStore(t)
	rpc := rpcclient.NewFakeClient()
	rpc.SetStatusResponse(poolerID, readyStatusResponse(&clustermetadata.ID{
		Cell: "cell1", Name: "pooler-0",
	}))
	r.PoolerClients = poolerclient.Static(rpc)
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return store, nil
	}

	if _, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{}); err != nil {
		t.Fatalf("recovery reconcileDataPlane() error = %v", err)
	}
	recovered := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), recovered); err != nil {
		t.Fatalf("get recovered pod: %v", err)
	}
	condition = readinessCondition(recovered.Status.Conditions)
	if condition == nil || condition.Status != corev1.ConditionTrue ||
		condition.Reason != "DataPlaneReady" {
		t.Fatalf("readiness condition after recovery = %#v, want True/DataPlaneReady", condition)
	}
}

// TestReconcileDataPlaneFailsClosedOnTopologyOutage mirrors a real topology
// outage exactly as production experiences it: the dial itself succeeds (see
// alwaysFailingTopoFactory), and the failure only shows up on the calls
// reconcilePosture actually makes through posture.Evaluate. This is the shape
// a plain etcd blip takes; TestReconcileDataPlaneFailsClosedOnTopoStoreUnavailable
// covers the narrower case where r.topoStore() itself errors, which a config
// problem (a bad TLS Secret, an unknown implementation name) can produce but a
// live outage cannot. TestReconcileDataPlaneFailsClosedOnPostureEvaluateError
// is narrower in a different way: the dial still succeeds, but
// GetMultipoolersByCell returns a hard, non-UNAVAILABLE error instead of an
// outage-shaped one, so posture.Evaluate itself errors rather than Evaluate
// returning a normal, if incomplete, result.
func TestReconcileDataPlaneFailsClosedOnTopologyOutage(t *testing.T) {
	shard := postureTestShard()
	pod := readyPoolPod()

	r, c := postureTestReconciler(t, shard, rpcclient.NewFakeClient(), pod)
	failingStore := topoclient.NewWithFactory(
		alwaysFailingTopoFactory{}, "", []string{""}, topoclient.NewDefaultTopoConfig(),
	)
	// The wrapper's background retry loop only stops on Close; a factory
	// that never succeeds would otherwise keep it retrying for the life of
	// the test binary.
	t.Cleanup(func() { _ = failingStore.Close() })
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return failingStore, nil
	}

	if _, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{}); err != nil {
		t.Fatalf("reconcileDataPlane() error = %v", err)
	}

	updated := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	condition := readinessCondition(updated.Status.Conditions)
	if condition == nil || condition.Status != corev1.ConditionUnknown ||
		condition.Reason != "ObservationUnavailable" {
		t.Fatalf(
			"readiness condition = %#v, want Unknown/ObservationUnavailable "+
				"(a store this shape used to read as reachable, reporting "+
				"False/AwaitingRegistration instead)",
			condition,
		)
	}

	// Recovery: the next successful observation flips readiness back True.
	store, poolerID := postureTestStore(t)
	rpc := rpcclient.NewFakeClient()
	rpc.SetStatusResponse(poolerID, readyStatusResponse(&clustermetadata.ID{
		Cell: "cell1", Name: "pooler-0",
	}))
	r.PoolerClients = poolerclient.Static(rpc)
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return store, nil
	}
	if _, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{}); err != nil {
		t.Fatalf("recovery reconcileDataPlane() error = %v", err)
	}
	recovered := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), recovered); err != nil {
		t.Fatalf("get recovered pod: %v", err)
	}
	condition = readinessCondition(recovered.Status.Conditions)
	if condition == nil || condition.Status != corev1.ConditionTrue ||
		condition.Reason != "DataPlaneReady" {
		t.Fatalf("readiness condition after recovery = %#v, want True/DataPlaneReady", condition)
	}
}

func TestReconcileDataPlaneFailsClosedOnPostureEvaluateError(t *testing.T) {
	shard := postureTestShard()
	pod := readyPoolPod()

	store, poolerID := postureTestStore(t)
	rpc := rpcclient.NewFakeClient()
	rpc.SetStatusResponse(poolerID, readyStatusResponse(&clustermetadata.ID{
		Cell: "cell1", Name: "pooler-0",
	}))

	r, c := postureTestReconciler(t, shard, rpc, pod)
	failingStore := erroringTopoStore{Store: store, err: errors.New("malformed topology record")}
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return failingStore, nil
	}

	_, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{})
	if err == nil {
		t.Fatal("reconcileDataPlane() error = nil, want posture evaluate error propagated")
	}

	updated := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), updated); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	condition := readinessCondition(updated.Status.Conditions)
	if condition == nil || condition.Status != corev1.ConditionUnknown ||
		condition.Reason != "ObservationUnavailable" {
		t.Fatalf("readiness condition = %#v, want Unknown/ObservationUnavailable", condition)
	}

	// Recovery: the next successful observation flips readiness back True.
	// A fresh store, not the one wrapped above: reconcileDataPlane closes its
	// topo connection on every pass, and Close is promoted straight through
	// erroringTopoStore to the real store it wraps.
	recoveryStore, recoveryPoolerID := postureTestStore(t)
	rpc.SetStatusResponse(recoveryPoolerID, readyStatusResponse(&clustermetadata.ID{
		Cell: "cell1", Name: "pooler-0",
	}))
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return recoveryStore, nil
	}
	if _, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{}); err != nil {
		t.Fatalf("recovery reconcileDataPlane() error = %v", err)
	}
	recovered := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), recovered); err != nil {
		t.Fatalf("get recovered pod: %v", err)
	}
	condition = readinessCondition(recovered.Status.Conditions)
	if condition == nil || condition.Status != corev1.ConditionTrue ||
		condition.Reason != "DataPlaneReady" {
		t.Fatalf("readiness condition after recovery = %#v, want True/DataPlaneReady", condition)
	}
}
