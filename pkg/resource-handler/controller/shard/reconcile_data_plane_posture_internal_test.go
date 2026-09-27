package shard

import (
	"context"
	"errors"
	"strings"
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
	"github.com/multigres/multigres-operator/pkg/data-handler/backuphealth"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/data-handler/posture"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

type countingPoolerResolver struct {
	client rpcclient.MultipoolerClient
	err    error
	calls  int
}

type backupErrorClient struct {
	rpcclient.MultipoolerClient
	err error
}

func (c backupErrorClient) GetBackups(
	context.Context,
	*clustermetadata.Multipooler,
	*multipoolermanagerdatapb.GetBackupsRequest,
) (*multipoolermanagerdatapb.GetBackupsResponse, error) {
	return nil, c.err
}

func (r *countingPoolerResolver) ClientFor(
	context.Context,
	*multigresv1alpha1.Shard,
) (rpcclient.MultipoolerClient, error) {
	r.calls++
	return r.client, r.err
}

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
		PoolerClients:   poolerclient.Static(rpc),
		CreateTopoStore: newMemoryTopoFactory(),
	}, c
}

// postureTestPod is a pool pod: reconcilePosture's own pod list is scoped to
// PoolComponentName (a shard's multiorch pod carries the same four identity
// labels but is never a pooler), so a pod fixture without this label is
// invisible to it regardless of what the test otherwise sets up.
func postureTestPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "pooler-0",
		Namespace: "default",
		Labels: map[string]string{
			metadata.LabelMultigresCluster:    "cluster",
			metadata.LabelMultigresDatabase:   "database",
			metadata.LabelMultigresTableGroup: "table-group",
			metadata.LabelMultigresShard:      "0",
			metadata.LabelAppComponent:        PoolComponentName,
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

	retryAfter, err := r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("first reconcilePosture() error = %v", err)
	}
	if retryAfter != postureDebounceRequeueDelay {
		t.Error("first inconsistent posture observation did not request a requeue")
	}
	if got := withDataPlaneRequeue(
		ctrl.Result{},
		retryAfter,
		false,
	).RequeueAfter; got != postureDebounceRequeueDelay {
		t.Errorf("first requeue delay = %v, want %v", got, postureDebounceRequeueDelay)
	}
	if conditionIsFalse(shard.Status.Conditions, posture.ConditionConsistent) {
		t.Errorf("conditions = %#v, want no failure on first observation", shard.Status.Conditions)
	}

	retryAfter, err = r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("second reconcilePosture() error = %v", err)
	}
	// The mismatch is now accepted into status (PostureConsistent=False), but
	// this mock pooler never reports IsInitialized/PostgresReady, so it has
	// also never reached posture readiness. An accepted-but-not-ready shard
	// must keep requesting a requeue: nothing but this controller's own
	// backoff will ever look again, since neither a status recovery in
	// topology nor a role fix changes a Kubernetes object.
	if retryAfter <= 0 {
		t.Error("second inconsistent posture observation (still not ready) requested no requeue")
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

	retryAfter, err := r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("first reconcilePosture() error = %v", err)
	}
	if retryAfter != postureDebounceRequeueDelay {
		t.Error("first incomplete posture observation did not request a requeue")
	}
	if got := withDataPlaneRequeue(
		ctrl.Result{},
		retryAfter,
		false,
	).RequeueAfter; got != postureDebounceRequeueDelay {
		t.Errorf("first requeue delay = %v, want %v", got, postureDebounceRequeueDelay)
	}
	if !conditionIsTrue(shard.Status.Conditions, posture.ConditionConsistent) {
		t.Errorf(
			"conditions = %#v, want prior consistent condition preserved on first blip",
			shard.Status.Conditions,
		)
	}

	retryAfter, err = r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("second reconcilePosture() error = %v", err)
	}
	// Accepted into status (Unknown/ObservationIncomplete), but a pooler whose
	// Status RPC errors has also never reached posture readiness, so this must
	// still requeue rather than strand the pod until the 10h resync.
	if retryAfter <= 0 {
		t.Error("second incomplete posture observation (still not ready) requested no requeue")
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
	resolver := &countingPoolerResolver{client: rpc}
	r.PoolerClients = resolver
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
	if resolver.calls != 1 {
		t.Errorf("ClientFor calls = %d, want exactly 1 per reconcile", resolver.calls)
	}
}

func TestReconcileDataPlaneContinuesWithoutPoolerClient(t *testing.T) {
	shard := postureTestShard()
	lastBackup := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	shard.Status.LastBackupTime = &lastBackup
	shard.Status.LastBackupType = "full"
	shard.Status.Conditions = []metav1.Condition{{
		Type:               posture.ConditionConsistent,
		Status:             metav1.ConditionTrue,
		Reason:             "Consistent",
		Message:            "postures consistent with topology roles",
		LastTransitionTime: metav1.Now(),
	}, {
		Type:               backuphealth.ConditionHealthy,
		Status:             metav1.ConditionTrue,
		Reason:             "BackupRecent",
		Message:            "backup is recent",
		LastTransitionTime: metav1.Now(),
	}}
	store, _ := postureTestStore(t)
	pod := postureTestPod()
	pod.Labels[metadata.LabelAppComponent] = PoolComponentName
	pod.Labels[metadata.LabelMultigresCell] = "cell1"
	pod.Labels[metadata.LabelMultigresPool] = "default"

	rpc := rpcclient.NewFakeClient()
	r, c := postureTestReconciler(t, shard, nil, pod)
	resolver := &countingPoolerResolver{
		client: rpc,
		err:    errors.New("certificate not issued"),
	}
	r.PoolerClients = resolver
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return store, nil
	}

	result, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{})
	if err != nil {
		t.Fatalf("reconcileDataPlane() error = %v", err)
	}
	if result.RequeueAfter != poolerClientRetryDelay {
		t.Errorf("requeue delay = %v, want %v", result.RequeueAfter, poolerClientRetryDelay)
	}
	if resolver.calls != 1 {
		t.Errorf("ClientFor calls = %d, want 1", resolver.calls)
	}

	got := &multigresv1alpha1.Shard{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), got); err != nil {
		t.Fatalf("get updated shard: %v", err)
	}
	if got.Status.PodRoles["pooler-0"] != "REPLICA" {
		t.Errorf("podRoles = %v, want topology-derived pooler-0 REPLICA", got.Status.PodRoles)
	}
	if got.Status.Phase != multigresv1alpha1.PhaseProgressing {
		t.Errorf("phase = %q, want %q", got.Status.Phase, multigresv1alpha1.PhaseProgressing)
	}
	postureCondition := findPostureCondition(
		got.Status.Conditions,
		posture.ConditionConsistent,
	)
	if postureCondition == nil {
		t.Fatalf(
			"conditions = %#v, want %s condition",
			got.Status.Conditions,
			posture.ConditionConsistent,
		)
	}
	if postureCondition.Status != metav1.ConditionUnknown ||
		postureCondition.Reason != "PoolerClientUnavailable" ||
		!strings.Contains(postureCondition.Message, "certificate not issued") {
		t.Errorf(
			"condition = %#v, want Unknown/PoolerClientUnavailable with resolver error",
			postureCondition,
		)
	}
	if calls := rpc.GetCallLog(); len(calls) != 0 {
		t.Errorf("RPC phases ran with resolver error, call log = %v", calls)
	}
	backupCondition := findPostureCondition(got.Status.Conditions, backuphealth.ConditionHealthy)
	if backupCondition == nil || backupCondition.Status != metav1.ConditionUnknown ||
		backupCondition.Reason != reasonBackupCheckUnavailable {
		t.Errorf(
			"backup condition = %#v, want Unknown/%s",
			backupCondition,
			reasonBackupCheckUnavailable,
		)
	}
	if got.Status.LastBackupTime == nil || !got.Status.LastBackupTime.Equal(&lastBackup) ||
		got.Status.LastBackupType != "full" {
		t.Errorf(
			"last backup fields changed during outage: time=%v type=%q",
			got.Status.LastBackupTime,
			got.Status.LastBackupType,
		)
	}

	recorder := r.Recorder.(*record.FakeRecorder)
	if !containsEventWithReason(drainEvents(recorder), "PoolerClientUnavailable") {
		t.Error("expected PoolerClientUnavailable event")
	}

	secondStore, _ := postureTestStore(t)
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return secondStore, nil
	}
	if _, err := r.reconcileDataPlane(t.Context(), got, renderedConfig{}); err != nil {
		t.Fatalf("second reconcileDataPlane() error = %v", err)
	}
	if events := drainEvents(recorder); containsEventWithReason(events, "PoolerClientUnavailable") {
		t.Errorf(
			"repeated resolver failure re-emitted PoolerClientUnavailable, events = %v",
			events,
		)
	}
}

func TestSetBackupUnknownPreservesConfirmedFailure(t *testing.T) {
	shard := postureTestShard()
	shard.Status.Conditions = []metav1.Condition{{
		Type:               backuphealth.ConditionHealthy,
		Status:             metav1.ConditionFalse,
		Reason:             "BackupStale",
		LastTransitionTime: metav1.Now(),
	}}
	setBackupUnknownUnlessFalse(shard, "RPC unavailable")
	condition := findPostureCondition(shard.Status.Conditions, backuphealth.ConditionHealthy)
	if condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != "BackupStale" {
		t.Errorf("backup condition = %#v, want preserved False/BackupStale", condition)
	}
}

func TestReconcileDataPlaneResolverFailurePreservesConfirmedPostureFailure(t *testing.T) {
	shard := postureTestShard()
	shard.Status.Conditions = []metav1.Condition{{
		Type:               posture.ConditionConsistent,
		Status:             metav1.ConditionFalse,
		Reason:             "MultiplePrimaries",
		Message:            "observed two primaries",
		LastTransitionTime: metav1.Now(),
	}}
	store, _ := postureTestStore(t)
	pod := postureTestPod()
	pod.Labels[metadata.LabelAppComponent] = PoolComponentName
	pod.Labels[metadata.LabelMultigresCell] = "cell1"
	pod.Labels[metadata.LabelMultigresPool] = "default"

	r, c := postureTestReconciler(t, shard, nil, pod)
	r.PoolerClients = &countingPoolerResolver{err: errors.New("certificate not issued")}
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return store, nil
	}

	// Simulate one unsettled observation immediately before the transport
	// outage. Resolver failure must break that sequence.
	if strikes := r.recordPostureObservation(shard, true); strikes != 1 {
		t.Fatalf("initial strikes = %d, want 1", strikes)
	}

	result, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{})
	if err != nil {
		t.Fatalf("reconcileDataPlane() error = %v", err)
	}
	if result.RequeueAfter != poolerClientRetryDelay {
		t.Errorf("requeue delay = %v, want %v", result.RequeueAfter, poolerClientRetryDelay)
	}

	got := &multigresv1alpha1.Shard{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), got); err != nil {
		t.Fatalf("get updated shard: %v", err)
	}
	condition := findPostureCondition(got.Status.Conditions, posture.ConditionConsistent)
	if condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != "MultiplePrimaries" {
		t.Errorf("condition = %#v, want preserved False/MultiplePrimaries", condition)
	}
	if got.Status.Phase != multigresv1alpha1.PhaseDegraded {
		t.Errorf("phase = %q, want %q", got.Status.Phase, multigresv1alpha1.PhaseDegraded)
	}
	if strikes := r.recordPostureObservation(shard, true); strikes != 1 {
		t.Errorf("first post-outage strikes = %d, want 1", strikes)
	}
}

func TestReconcileDataPlaneMarksBackupUnknownOnRPCFailure(t *testing.T) {
	shard := postureTestShard()
	lastBackup := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	shard.Status.LastBackupTime = &lastBackup
	shard.Status.LastBackupType = "full"
	shard.Status.Conditions = []metav1.Condition{{
		Type:               backuphealth.ConditionHealthy,
		Status:             metav1.ConditionTrue,
		Reason:             "BackupRecent",
		LastTransitionTime: metav1.Now(),
	}}
	store, poolerID := postureTestStore(t)
	if _, err := store.UpdateMultipoolerFields(
		t.Context(),
		&clustermetadata.ID{Cell: "cell1", Name: "pooler-0"},
		func(pooler *clustermetadata.Multipooler) error {
			pooler.RoutingState.Role = clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY
			return nil
		},
	); err != nil {
		t.Fatalf("make pooler primary: %v", err)
	}

	baseRPC := rpcclient.NewFakeClient()
	baseRPC.SetStatusResponse(poolerID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
		},
	})
	rpc := backupErrorClient{
		MultipoolerClient: baseRPC,
		err:               errors.New("backup RPC unavailable"),
	}
	pod := postureTestPod()
	pod.Labels[metadata.LabelAppComponent] = PoolComponentName
	pod.Labels[metadata.LabelMultigresCell] = "cell1"
	pod.Labels[metadata.LabelMultigresPool] = "default"
	r, c := postureTestReconciler(t, shard, rpc, pod)
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return store, nil
	}

	if _, err := r.reconcileDataPlane(t.Context(), shard, renderedConfig{}); err != nil {
		t.Fatalf("reconcileDataPlane() error = %v", err)
	}
	got := &multigresv1alpha1.Shard{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), got); err != nil {
		t.Fatalf("get updated shard: %v", err)
	}
	condition := findPostureCondition(got.Status.Conditions, backuphealth.ConditionHealthy)
	if condition == nil || condition.Status != metav1.ConditionUnknown ||
		condition.Reason != reasonBackupCheckUnavailable {
		t.Errorf("backup condition = %#v, want Unknown/%s", condition, reasonBackupCheckUnavailable)
	}
	if got.Status.LastBackupTime == nil || !got.Status.LastBackupTime.Equal(&lastBackup) ||
		got.Status.LastBackupType != "full" {
		t.Errorf(
			"last backup fields changed: time=%v type=%q",
			got.Status.LastBackupTime,
			got.Status.LastBackupType,
		)
	}
}

func TestReconcilePostureClearsStaleUnknownReasonDuringDebounce(t *testing.T) {
	store, poolerID := postureTestStore(t)
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	rpc.SetStatusResponse(poolerID, &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{
			PostgresStatus: multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
		},
	})
	for _, previousReason := range []string{
		reasonPoolerClientUnavailable,
		reasonAwaitingPoolerRegistration,
	} {
		t.Run(previousReason, func(t *testing.T) {
			shard := postureTestShard()
			shard.Status.Conditions = []metav1.Condition{{
				Type:               posture.ConditionConsistent,
				Status:             metav1.ConditionUnknown,
				Reason:             previousReason,
				Message:            "observation unavailable",
				LastTransitionTime: metav1.Now(),
			}}
			r, _ := postureTestReconciler(t, shard, rpc, postureTestPod())

			retryAfter, err := r.reconcilePosture(t.Context(), store, shard, rpc)
			if err != nil {
				t.Fatalf("reconcilePosture() error = %v", err)
			}
			if retryAfter != postureDebounceRequeueDelay {
				t.Fatal("first recovered unsettled observation did not request debounce requeue")
			}
			condition := findPostureCondition(shard.Status.Conditions, posture.ConditionConsistent)
			if condition == nil || condition.Status != metav1.ConditionUnknown ||
				condition.Reason != reasonObservationPending {
				t.Errorf("condition = %#v, want Unknown/ObservationPending", condition)
			}
		})
	}
}

func TestReconcilePostureRequeuesWhileTopologyHasNoPoolers(t *testing.T) {
	shard := postureTestShard()
	shard.Status.Conditions = []metav1.Condition{{
		Type:               posture.ConditionConsistent,
		Status:             metav1.ConditionUnknown,
		Reason:             "PoolerClientUnavailable",
		Message:            "certificate not issued",
		LastTransitionTime: metav1.Now(),
	}}
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	store := topoclient.NewWithFactory(
		factory,
		"",
		[]string{""},
		topoclient.NewDefaultTopoConfig(),
	)
	defer func() { _ = store.Close() }()

	rpc := rpcclient.NewFakeClient()
	r, _ := postureTestReconciler(t, shard, rpc, postureTestPod())
	retryAfter, err := r.reconcilePosture(t.Context(), store, shard, rpc)
	if err != nil {
		t.Fatalf("reconcilePosture() error = %v", err)
	}
	if retryAfter != poolerRegistrationRetryDelay {
		t.Fatal("empty topology did not request a bootstrap requeue")
	}
	condition := findPostureCondition(shard.Status.Conditions, posture.ConditionConsistent)
	if condition == nil || condition.Status != metav1.ConditionUnknown ||
		condition.Reason != "AwaitingPoolerRegistration" {
		t.Errorf("condition = %#v, want Unknown/AwaitingPoolerRegistration", condition)
	}
}

func TestWithDataPlaneRequeueUsesEarliestDelay(t *testing.T) {
	tests := []struct {
		name                    string
		result                  ctrl.Result
		postureRetryAfter       time.Duration
		poolerClientUnavailable bool
		want                    time.Duration
	}{
		{
			name:                    "keeps earlier phase retry",
			result:                  ctrl.Result{RequeueAfter: 2 * time.Second},
			postureRetryAfter:       postureDebounceRequeueDelay,
			poolerClientUnavailable: true,
			want:                    2 * time.Second,
		},
		{
			name:                    "posture debounce wins",
			postureRetryAfter:       postureDebounceRequeueDelay,
			poolerClientUnavailable: true,
			want:                    postureDebounceRequeueDelay,
		},
		{
			name:                    "pooler client retry",
			poolerClientUnavailable: true,
			want:                    poolerClientRetryDelay,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := withDataPlaneRequeue(
				tt.result,
				tt.postureRetryAfter,
				tt.poolerClientUnavailable,
			)
			if result.RequeueAfter != tt.want {
				t.Errorf("requeue delay = %v, want %v", result.RequeueAfter, tt.want)
			}
		})
	}
}

func findPostureCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
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

// drainEvents returns every event currently buffered in the fake recorder.
func drainEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// containsEventWithReason reports whether any event message mentions reason.
func containsEventWithReason(events []string, reason string) bool {
	for _, event := range events {
		if strings.Contains(event, reason) {
			return true
		}
	}
	return false
}
