package shard

import (
	"context"
	"slices"
	"testing"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

type disruptionTopo struct {
	topoclient.Store
	poolers []*topoclient.MultipoolerInfo
}

func fourToTwoFixture(
	t *testing.T,
) (*ShardReconciler, *multigresv1alpha1.Shard, map[string]map[string]*corev1.Pod, *rpcclient.FakeClient, []*md.StatusResponse) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = multigresv1alpha1.AddToScheme(scheme)
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "shard",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "db",
			TableGroupName:   "tg",
			ShardName:        "0",
			DurabilityPolicy: multiCellAtLeast2Policy,
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": {
					Cells:           []multigresv1alpha1.CellName{"a", "b"},
					ReplicasPerCell: ptr.To(int32(1)),
				},
			},
		},
		Status: multigresv1alpha1.ShardStatus{PodRoles: map[string]string{}},
	}
	groups := map[string]map[string]*corev1.Pod{}
	objects := []client.Object{shard}
	for _, cell := range []string{"a", "b"} {
		groups[cell] = map[string]*corev1.Pod{}
		for i := 0; i < 2; i++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      BuildPoolPodName(shard, "main", cell, i),
					Namespace: shard.Namespace,
					Labels:    buildPoolLabelsWithCell(shard, "main", cell),
				},
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			}
			groups[cell][pod.Name] = pod
			objects = append(objects, pod)
			shard.Status.PodRoles[pod.Name] = "REPLICA"
		}
	}
	shard.Status.PodRoles[BuildPoolPodName(shard, "main", "a", 1)] = "PRIMARY"
	r := &ShardReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}
	rpc, responses := observeHealthyDisruption(t, r, shard, "main", "a")
	for _, response := range responses {
		response.ConsensusStatus.CurrentPosition.Position.Decision.DurabilityPolicy = topoclient.MultiCellAtLeastN(
			2,
		)
		rpc.SetStatusResponse(topoclient.ComponentIDString(response.ConsensusStatus.Id), response)
	}
	return r, shard, groups, rpc, responses
}

func TestScaleDownUnregisteredExtra(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nodeName      string
		scheduled     bool
		stillInCohort bool
		wantAction    bool
	}{
		{name: "unscheduled extra is cleaned up", wantAction: true},
		{name: "bound pod still needs registration", nodeName: "worker"},
		{name: "scheduled condition still needs registration", scheduled: true},
		{name: "missing committed member blocks cleanup", stillInCohort: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := assert.NewAborting(t)
			r, shard, groups, rpc, responses := fourToTwoFixture(t)
			name := BuildPoolPodName(shard, "main", "b", 1)
			key := client.ObjectKey{Namespace: shard.Namespace, Name: name}
			pod := &corev1.Pod{}
			c.NoError(r.Get(t.Context(), key, pod))
			pod.Status.Phase = corev1.PodPending
			pod.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			}
			c.NoError(r.Status().Update(t.Context(), pod))
			groups["b"][name] = pod.DeepCopy()
			// Keep the caller's pod stale: scheduling evidence must come from
			// the uncached API reader used by the disruption preflight.
			pod.Spec.NodeName = tc.nodeName
			c.NoError(r.Update(t.Context(), pod))
			if tc.scheduled {
				pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
					Type: corev1.PodScheduled, Status: corev1.ConditionTrue,
				})
				c.NoError(r.Status().Update(t.Context(), pod))
			}
			r.APIReader = r.Client
			store, err := r.CreateTopoStore(shard)
			c.NoError(err)
			topo := store.(*disruptionTopo)
			topo.poolers = slices.DeleteFunc(
				topo.poolers,
				func(p *topoclient.MultipoolerInfo) bool {
					return p.Id.Name == name
				},
			)
			if !tc.stillInCohort {
				for _, response := range responses {
					rule := response.ConsensusStatus.CurrentPosition.Position.Decision
					rule.CohortMembers = slices.DeleteFunc(
						rule.CohortMembers,
						func(id *cm.ID) bool { return id.Name == name },
					)
					rpc.SetStatusResponse(
						topoclient.ComponentIDString(response.ConsensusStatus.Id),
						response,
					)
				}
			}
			allowed, err := r.canStartDisruption(
				t.Context(),
				shard,
				groups["b"][name],
				&shardRolloutTracker{},
			)
			c.False(
				err != nil || allowed != tc.wantAction,
				"preflight with stale target: allowed=%v err=%v; want %v",
				allowed,
				err,
				tc.wantAction,
			)
			tracker := &shardRolloutTracker{}
			action, _, err := r.handleScaleDown(
				t.Context(),
				shard,
				"main",
				shard.Spec.Pools["main"],
				groups["b"],
				1,
				1,
				false,
				tracker,
			)
			c.NoError(err)
			if action != tc.wantAction || tracker.waitingForRecovery == tc.wantAction {
				t.Fatalf(
					"action=%v waitingForRecovery=%v; want action=%v",
					action,
					tracker.waitingForRecovery,
					tc.wantAction,
				)
			}
			if !tc.wantAction {
				return
			}
			c.True(tracker.HasStarted(), "cleanup must reserve this pass's disruption")
			// Complete the ordinary drain/PVC cleanup path, then ensure the
			// other cell's excess primary is no longer blocked by this pod.
			for range 3 {
				if _, err := r.reconcileDrainState(t.Context(), store, shard); err != nil {
					t.Fatal(err)
				}
			}
			c.NoError(r.Get(t.Context(), key, pod))
			groups["b"][name] = pod
			if _, _, err := r.handleScaleDown(
				t.Context(),
				shard,
				"main",
				shard.Spec.Pools["main"],
				groups["b"],
				1,
				1,
				false,
				&shardRolloutTracker{},
			); err != nil {
				t.Fatal(err)
			}
			if err := r.Get(t.Context(), key, &corev1.Pod{}); !apierrors.IsNotFound(err) {
				t.Fatalf("excess pod was not deleted: %v", err)
			}
			action, _, err = r.handleScaleDown(
				t.Context(),
				shard,
				"main",
				shard.Spec.Pools["main"],
				groups["a"],
				1,
				1,
				false,
				&shardRolloutTracker{},
			)
			c.False(
				err != nil || !action,
				"next scale-down did not resume: action=%v err=%v",
				action,
				err,
			)
		})
	}
}

func TestScaleDownWaitsForOtherCellsExtraPodDrain(t *testing.T) {
	for _, state := range []string{metadata.DrainStateRequested, metadata.DrainStateDraining, metadata.DrainStateAcknowledged, metadata.DrainStateReadyForDeletion, "terminating"} {
		t.Run(state, func(t *testing.T) {
			c := assert.NewAborting(t)
			r, shard, groups, _, _ := fourToTwoFixture(t)
			pod := &corev1.Pod{}
			key := client.ObjectKey{
				Namespace: shard.Namespace,
				Name:      BuildPoolPodName(shard, "main", "a", 1),
			}
			c.NoError(r.Get(t.Context(), key, pod))
			if state == "terminating" {
				pod.Finalizers = []string{"test/hold"}
				c.NoError(r.Update(t.Context(), pod))
				c.NoError(r.Delete(t.Context(), pod))
			} else {
				pod.Annotations = map[string]string{metadata.AnnotationDrainState: state}
				c.NoError(r.Update(t.Context(), pod))
			}
			// No shared in-memory tracker survives this new reconciliation.
			action, _, err := r.handleScaleDown(
				t.Context(),
				shard,
				"main",
				shard.Spec.Pools["main"],
				groups["b"],
				1,
				1,
				false,
				&shardRolloutTracker{},
			)
			c.False(err != nil || action, "overlapping drain: action=%v err=%v", action, err)
		})
	}
}

func TestScaleDownReplicaFirstAndWaitsForCohortRecovery(t *testing.T) {
	c := assert.NewAborting(t)
	r, shard, groups, rpc, responses := fourToTwoFixture(t)
	run := func(cell string) (bool, *shardRolloutTracker) {
		t.Helper()
		tracker := &shardRolloutTracker{}
		action, _, err := r.handleScaleDown(
			t.Context(),
			shard,
			"main",
			shard.Spec.Pools["main"],
			groups[cell],
			1,
			1,
			false,
			tracker,
		)
		c.NoError(err)
		return action, tracker
	}
	if action, _ := run("a"); action {
		t.Fatal("primary selected while another cell has a removable replica")
	}
	if action, _ := run("b"); !action {
		t.Fatal("replica drain did not start")
	}
	if action, _ := run("a"); action {
		t.Fatal("second reconcile started an overlapping drain")
	}
	removed := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	c.NoError(r.Delete(t.Context(), removed))
	delete(groups["b"], removed.Name)
	if action, tracker := run("a"); action || !tracker.waitingForRecovery {
		t.Fatal("must requeue while deleted member remains in committed cohort")
	}
	// Multigres commits a smaller cohort; the remaining primary is active and
	// both surviving replicas stream from it, with one member in each cell.
	for _, response := range responses {
		rule := response.ConsensusStatus.CurrentPosition.Position.Decision
		rule.CohortMembers = slices.DeleteFunc(
			rule.CohortMembers,
			func(id *cm.ID) bool { return id.Name == removed.Name },
		)
		rpc.SetStatusResponse(topoclient.ComponentIDString(response.ConsensusStatus.Id), response)
	}
	action, _ := run("a")
	c.True(action, "scale-down did not resume after cohort recovery")
}

func TestDisruptionReadsUncachedState(t *testing.T) {
	r, shard, groups, _, _ := fourToTwoFixture(t)
	objects := []client.Object{}
	for cell, group := range groups {
		for _, pod := range group {
			copy := pod.DeepCopy()
			if cell == "a" {
				copy.Annotations = map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				}
			}
			objects = append(objects, copy)
		}
	}
	r.APIReader = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(objects...).Build()
	healthy, err := r.isShardHealthy(t.Context(), shard)
	assert.NewAborting(t).
		False(err != nil || healthy, "uncached drain ignored: healthy=%v err=%v", healthy, err)
}

func TestDisruptionWithoutObservationsFailsClosed(t *testing.T) {
	r, shard, groups, _, _ := fourToTwoFixture(t)
	r.PoolerClients = nil
	tracker := &shardRolloutTracker{}
	allowed, err := r.canStartDisruption(
		t.Context(),
		shard,
		groups["b"][BuildPoolPodName(shard, "main", "b", 1)],
		tracker,
	)
	if err != nil || allowed || !tracker.waitingForRecovery {
		t.Fatalf(
			"missing observations: allowed=%v retry=%v err=%v",
			allowed,
			tracker.waitingForRecovery,
			err,
		)
	}
}

func TestScaleDownCleanupReservesShardDisruption(t *testing.T) {
	c := assert.NewAborting(t)
	r, shard, groups, _, _ := fourToTwoFixture(t)
	name := BuildPoolPodName(shard, "main", "b", 1)
	pod := &corev1.Pod{}
	c.NoError(r.Get(
		t.Context(),
		client.ObjectKey{Namespace: shard.Namespace, Name: name},
		pod,
	))
	pod.Annotations = map[string]string{
		metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
	}
	c.NoError(r.Update(t.Context(), pod))
	groups["b"][name] = pod
	tracker := &shardRolloutTracker{}
	action, _, err := r.handleScaleDown(
		t.Context(),
		shard,
		"main",
		shard.Spec.Pools["main"],
		groups["b"],
		1,
		1,
		false,
		tracker,
	)
	c.False(
		err != nil || !action || !tracker.HasStarted(),
		"cleanup did not reserve disruption: action=%v err=%v",
		action,
		err,
	)
	action, _, err = r.handleScaleDown(
		t.Context(),
		shard,
		"main",
		shard.Spec.Pools["main"],
		groups["a"],
		1,
		1,
		false,
		tracker,
	)
	c.False(
		err != nil || action,
		"cleanup allowed another drain in same pass: action=%v err=%v",
		action,
		err,
	)
}

func (s *disruptionTopo) Close() error { return nil }

func (s *disruptionTopo) GetMultipoolersByCell(
	_ context.Context,
	cell string,
	_ *topoclient.GetMultipoolersByCellOptions,
) ([]*topoclient.MultipoolerInfo, error) {
	var result []*topoclient.MultipoolerInfo
	for _, p := range s.poolers {
		if p.Id.Cell == cell {
			result = append(result, p)
		}
	}
	return result, nil
}

// observeHealthyDisruption gives controller unit fixtures a serving consensus
// cohort. Older fixtures model only the pods acted on; add explicitly desired
// supporting replicas when needed so those tests do not remove the last member.
func observeHealthyDisruption(
	t *testing.T,
	r *ShardReconciler,
	shard *multigresv1alpha1.Shard,
	poolName, cellName string,
) (*rpcclient.FakeClient, []*md.StatusResponse) {
	t.Helper()
	c := assert.NewAborting(t)
	pods := &corev1.PodList{}
	c.NoError(r.List(t.Context(), pods, client.InNamespace(shard.Namespace)))
	slices.SortFunc(pods.Items, func(a, b corev1.Pod) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	if shard.Spec.Pools == nil {
		shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{}
	}
	pool := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	if len(pool.Cells) == 0 {
		pool.Cells = []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cellName)}
	}
	if pool.ReplicasPerCell == nil {
		//nolint:gosec // Unit fixtures contain only a handful of pods.
		pool.ReplicasPerCell = ptr.To(int32(len(pods.Items)))
	}
	shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)] = pool
	missing := 3 - len(pods.Items)
	for i := 0; i < missing; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      BuildPoolPodName(shard, "support", cellName, i),
				Namespace: shard.Namespace,
				Labels: map[string]string{
					metadata.LabelMultigresPool: "support",
					metadata.LabelMultigresCell: cellName,
				},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		c.NoError(r.Create(t.Context(), pod))
		pods.Items = append(pods.Items, *pod)
	}
	if missing > 0 {
		shard.Spec.Pools["support"] = multigresv1alpha1.PoolSpec{
			Cells:           []multigresv1alpha1.CellName{multigresv1alpha1.CellName(cellName)},
			ReplicasPerCell: ptr.To(int32(missing)),
		}
	}
	store := &disruptionTopo{}
	ids := make([]*cm.ID, len(pods.Items))
	leaderIndex := -1
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		for key, value := range map[string]string{metadata.LabelMultigresCluster: shard.Labels[metadata.LabelMultigresCluster], metadata.LabelMultigresDatabase: string(shard.Spec.DatabaseName), metadata.LabelMultigresTableGroup: string(shard.Spec.TableGroupName), metadata.LabelMultigresShard: string(shard.Spec.ShardName), metadata.LabelAppComponent: PoolComponentName} {
			pod.Labels[key] = value
		}
		if pod.Labels[metadata.LabelMultigresPool] == "" {
			pod.Labels[metadata.LabelMultigresPool] = poolName
		}
		if pod.Labels[metadata.LabelMultigresCell] == "" {
			pod.Labels[metadata.LabelMultigresCell] = cellName
		}
		c.NoError(r.Update(t.Context(), pod))
		if len(pod.Status.Conditions) == 0 {
			pod.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}
			c.NoError(r.Client.Status().Update(t.Context(), pod))
		}
		ids[i] = &cm.ID{Name: pod.Name, Cell: pod.Labels[metadata.LabelMultigresCell]}
		if shard.Status.PodRoles[pod.Name] == "PRIMARY" {
			leaderIndex = i
		}
	}
	if leaderIndex < 0 {
		leaderIndex = len(ids) - 1
		if shard.Status.PodRoles == nil {
			shard.Status.PodRoles = map[string]string{}
		}
		shard.Status.PodRoles[ids[leaderIndex].Name] = "PRIMARY"
	}
	rule := &cm.ShardRule{
		RuleNumber:       &cm.RuleNumber{CoordinatorTerm: 2},
		LeaderId:         ids[leaderIndex],
		CohortMembers:    ids,
		DurabilityPolicy: topoclient.AtLeastN(2),
	}
	rpc := rpcclient.NewFakeClient()
	responses := make([]*md.StatusResponse, len(ids))
	for i, id := range ids {
		role := cm.RoutingRole_ROUTING_ROLE_REPLICA
		response := &md.StatusResponse{
			Status: &md.Status{
				IsInitialized:  true,
				PostgresReady:  true,
				PostgresStatus: md.PostgresStatus_POSTGRES_STATUS_STANDBY,
			},
			AvailabilityStatus: &cm.AvailabilityStatus{
				CohortEligibilityStatus: &cm.CohortEligibilityStatus{
					Signal: cm.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
			ConsensusStatus: &cm.ConsensusStatus{
				Id: id,
				CurrentPosition: &cm.PoolerPosition{
					Position: &cm.RulePosition{Decision: proto.Clone(rule).(*cm.ShardRule)},
				},
			},
		}
		if i == leaderIndex {
			role = cm.RoutingRole_ROUTING_ROLE_PRIMARY
			response.Status.PostgresStatus = md.PostgresStatus_POSTGRES_STATUS_PRIMARY
			response.Status.PrimaryStatus = &md.PrimaryStatus{Ready: true, ConnectedFollowers: ids}
			// Healthy primaries omit leadership status in the pinned server.
		}
		store.poolers = append(
			store.poolers,
			&topoclient.MultipoolerInfo{
				Multipooler: &cm.Multipooler{
					Id:           id,
					Hostname:     id.Name,
					RoutingState: &cm.RoutingState{Role: role},
				},
			},
		)
		responses[i] = response
		rpc.SetStatusResponse(topoclient.ComponentIDString(id), response)
	}
	r.PoolerClients = poolerclient.Static(rpc)
	r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) { return store, nil }
	return rpc, responses
}
