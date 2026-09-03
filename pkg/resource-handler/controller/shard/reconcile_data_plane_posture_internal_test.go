package shard

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func postureTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add shard scheme: %v", err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	return scheme
}

func postureTestShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "posture-test",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "database",
			TableGroupName: "table-group",
			ShardName:      "0",
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}
}

func postureTestReconciler(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	rpc rpcclient.MultipoolerClient,
	objects ...client.Object,
) (*ShardReconciler, client.Client) {
	t.Helper()
	scheme := postureTestScheme(t)
	allObjects := append([]client.Object{shard}, objects...)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(allObjects...).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()
	return &ShardReconciler{
		Client:          c,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(20),
		RPCClient:       rpc,
		CreateTopoStore: newMemoryTopoFactory(),
	}, c
}

func postureTestPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "pooler-0",
		Namespace: "default",
		Labels: map[string]string{
			metadata.LabelMultigresCluster:    "cluster",
			metadata.LabelMultigresDatabase:   "database",
			metadata.LabelMultigresTableGroup: "table-group",
			metadata.LabelMultigresShard:      "0",
		},
	}}
}

func postureTestStore(t *testing.T) (topoclient.Store, topoclient.ComponentID) {
	t.Helper()
	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(
		factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
	)
	id := &clustermetadata.ID{Cell: "cell1", Name: "pooler-0"}
	if err := store.RegisterMultipooler(context.Background(), &clustermetadata.Multipooler{
		Id:       id,
		Hostname: "pooler-0",
		ShardKey: &clustermetadata.ShardKey{
			Database:   "database",
			TableGroup: "table-group",
			Shard:      "0",
		},
		RoutingState: &clustermetadata.RoutingState{
			Role: clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA,
		},
	}, false); err != nil {
		t.Fatalf("register pooler: %v", err)
	}
	return store, topoclient.ComponentIDString(id)
}

func TestUpdateStatusPublishesPostureFailureAndPhaseTogether(t *testing.T) {
	shard := postureTestShard()
	shard.Status.PodPostures = map[string]string{"pooler-0": "PRIMARY"}
	shard.Status.Conditions = []metav1.Condition{{
		Type:               posture.ConditionConsistent,
		Status:             metav1.ConditionFalse,
		Reason:             "MultiplePrimaries",
		Message:            "observed two primaries",
		LastTransitionTime: metav1.Now(),
	}}
	r, c := postureTestReconciler(t, shard, nil)

	if err := r.updateStatus(t.Context(), shard, renderedConfig{}); err != nil {
		t.Fatalf("updateStatus() error = %v", err)
	}

	got := &multigresv1alpha1.Shard{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), got); err != nil {
		t.Fatalf("get updated shard: %v", err)
	}
	if got.Status.Phase != multigresv1alpha1.PhaseDegraded {
		t.Errorf("phase = %q, want %q", got.Status.Phase, multigresv1alpha1.PhaseDegraded)
	}
	if got.Status.PodPostures["pooler-0"] != "PRIMARY" {
		t.Errorf("podPostures = %v, want pooler-0 PRIMARY", got.Status.PodPostures)
	}
	postureFailurePersisted := false
	for _, condition := range got.Status.Conditions {
		if condition.Type == posture.ConditionConsistent &&
			condition.Status == metav1.ConditionFalse {
			postureFailurePersisted = true
			break
		}
	}
	if !postureFailurePersisted {
		t.Errorf("conditions = %#v, want persisted posture failure", got.Status.Conditions)
	}
}

func TestUpdateStatusKeepsIncompletePostureOutOfHealthy(t *testing.T) {
	shard := postureTestShard()
	shard.Status.Conditions = []metav1.Condition{{
		Type:               posture.ConditionConsistent,
		Status:             metav1.ConditionUnknown,
		Reason:             "ObservationIncomplete",
		Message:            "posture observation incomplete",
		LastTransitionTime: metav1.Now(),
	}}
	r, _ := postureTestReconciler(t, shard, nil)

	if err := r.updateStatus(t.Context(), shard, renderedConfig{}); err != nil {
		t.Fatalf("updateStatus() error = %v", err)
	}
	if shard.Status.Phase != multigresv1alpha1.PhaseProgressing {
		t.Errorf("phase = %q, want %q", shard.Status.Phase, multigresv1alpha1.PhaseProgressing)
	}
}

func TestReconcilePostureDebouncesFirstInconsistency(t *testing.T) {
	shard := postureTestShard()
	store, poolerID := postureTestStore(t)
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	rpc.SetStatusResponse(poolerID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
		},
	})
	r, _ := postureTestReconciler(t, shard, rpc, postureTestPod())

	pending, err := r.reconcilePosture(t.Context(), store, shard)
	if err != nil {
		t.Fatalf("first reconcilePosture() error = %v", err)
	}
	if !pending {
		t.Error("first inconsistent posture observation did not request a requeue")
	}
	if got := withPostureRequeue(
		ctrl.Result{},
		pending,
	).RequeueAfter; got != postureDebounceRequeueDelay {
		t.Errorf("first requeue delay = %v, want %v", got, postureDebounceRequeueDelay)
	}
	if conditionIsFalse(shard.Status.Conditions, posture.ConditionConsistent) {
		t.Errorf("conditions = %#v, want no failure on first observation", shard.Status.Conditions)
	}

	pending, err = r.reconcilePosture(t.Context(), store, shard)
	if err != nil {
		t.Fatalf("second reconcilePosture() error = %v", err)
	}
	if pending {
		t.Error("second inconsistent posture observation requested another debounce requeue")
	}
	if !conditionIsFalse(shard.Status.Conditions, posture.ConditionConsistent) {
		t.Errorf(
			"conditions = %#v, want posture failure on second observation",
			shard.Status.Conditions,
		)
	}
}

func TestReconcilePostureDebouncesFirstIncompleteObservation(t *testing.T) {
	shard := postureTestShard()
	shard.Status.Conditions = []metav1.Condition{{
		Type:               posture.ConditionConsistent,
		Status:             metav1.ConditionTrue,
		Reason:             "Consistent",
		Message:            "postures consistent with topology roles",
		LastTransitionTime: metav1.Now(),
	}}
	store, poolerID := postureTestStore(t)
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	rpc.Errors[poolerID] = errors.New("connection error: EOF")
	r, _ := postureTestReconciler(t, shard, rpc, postureTestPod())

	pending, err := r.reconcilePosture(t.Context(), store, shard)
	if err != nil {
		t.Fatalf("first reconcilePosture() error = %v", err)
	}
	if !pending {
		t.Error("first incomplete posture observation did not request a requeue")
	}
	if got := withPostureRequeue(
		ctrl.Result{},
		pending,
	).RequeueAfter; got != postureDebounceRequeueDelay {
		t.Errorf("first requeue delay = %v, want %v", got, postureDebounceRequeueDelay)
	}
	if !conditionIsTrue(shard.Status.Conditions, posture.ConditionConsistent) {
		t.Errorf(
			"conditions = %#v, want prior consistent condition preserved on first blip",
			shard.Status.Conditions,
		)
	}

	pending, err = r.reconcilePosture(t.Context(), store, shard)
	if err != nil {
		t.Fatalf("second reconcilePosture() error = %v", err)
	}
	if pending {
		t.Error("second incomplete posture observation requested another debounce requeue")
	}
	for _, condition := range shard.Status.Conditions {
		if condition.Type != posture.ConditionConsistent {
			continue
		}
		if condition.Status != metav1.ConditionUnknown ||
			condition.Reason != "ObservationIncomplete" {
			t.Errorf(
				"condition = %#v, want Unknown/ObservationIncomplete on second blip",
				condition,
			)
		}
	}
}

func TestReconcileDataPlaneRequeuesFirstPostureStrike(t *testing.T) {
	shard := postureTestShard()
	store, poolerID := postureTestStore(t)
	pod := postureTestPod()
	pod.Labels[metadata.LabelAppComponent] = PoolComponentName
	pod.Labels[metadata.LabelMultigresCell] = "cell1"
	pod.Labels[metadata.LabelMultigresPool] = "default"
	pod.Annotations = map[string]string{metadata.AnnotationPostgresConfigHash: "restart"}

	rpc := rpcclient.NewFakeClient()
	rpc.SetStatusResponse(poolerID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
		},
	})
	rpc.ReloadConfigResponses[poolerID] = &multipoolermanagerdatapb.ReloadConfigResponse{
		ConfigLoadTime: timestamppb.Now(),
	}
	r, _ := postureTestReconciler(t, shard, rpc, pod)
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return store, nil
	}

	result, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{
		restartHash: "restart",
		reloadHash:  "reload",
	})
	if err != nil {
		t.Fatalf("reconcileDataPlane() error = %v", err)
	}
	if result.RequeueAfter != postureDebounceRequeueDelay {
		t.Errorf("requeue delay = %v, want %v", result.RequeueAfter, postureDebounceRequeueDelay)
	}
	if !callLogHas(rpc.GetCallLog(), "ReloadConfig") {
		t.Errorf("reload phase did not run, call log = %v", rpc.GetCallLog())
	}
}

func TestWithPostureRequeueKeepsEarlierRequeue(t *testing.T) {
	result := withPostureRequeue(
		ctrl.Result{RequeueAfter: 2 * time.Second},
		true,
	)
	if result.RequeueAfter != 2*time.Second {
		t.Errorf("requeue delay = %v, want 2s", result.RequeueAfter)
	}
}

func conditionIsFalse(conditions []metav1.Condition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Status == metav1.ConditionFalse {
			return true
		}
	}
	return false
}

func conditionIsTrue(conditions []metav1.Condition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}
