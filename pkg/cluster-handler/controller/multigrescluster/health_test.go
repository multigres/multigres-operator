package multigrescluster

import (
	"errors"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestFailoverHealthConditions(t *testing.T) {
	for _, tc := range []struct {
		name                                                                                          string
		quorum                                                                                        metav1.ConditionStatus
		quorumReason                                                                                  string
		stale, missing, orchDown, shardStale, shardMissing, accessFailed, external, observationFailed bool
		want                                                                                          metav1.ConditionStatus
		reason                                                                                        string
	}{
		{name: "failed shard observation clears readiness", quorum: metav1.ConditionTrue, observationFailed: true, want: metav1.ConditionUnknown, reason: "ObservationFailed"},
		{name: "healthy", quorum: metav1.ConditionTrue, want: metav1.ConditionTrue, reason: "FailoverReady"},
		{name: "quorum lost with ready pods", quorum: metav1.ConditionFalse, want: metav1.ConditionFalse, reason: "QuorumUnavailable"},
		{name: "etcd alarm with ready pods and successful registration", quorum: metav1.ConditionFalse, quorumReason: "EtcdStatusError", want: metav1.ConditionFalse, reason: "EtcdStatusError"},
		{name: "stale topology observation", quorum: metav1.ConditionTrue, stale: true, want: metav1.ConditionUnknown, reason: "TopologyHealthStale"},
		{name: "missing topology", missing: true, want: metav1.ConditionFalse, reason: "TopologyMissing"},
		{name: "known orchestrator failure takes precedence over stale quorum", quorum: metav1.ConditionTrue, stale: true, orchDown: true, want: metav1.ConditionFalse, reason: "OrchestratorUnavailable"},
		{name: "multiorch not ready", quorum: metav1.ConditionTrue, orchDown: true, want: metav1.ConditionFalse, reason: "OrchestratorUnavailable"},
		{name: "missing desired shard", quorum: metav1.ConditionTrue, shardMissing: true, want: metav1.ConditionFalse, reason: "OrchestratorUnavailable"},
		{name: "old unready shard generation", quorum: metav1.ConditionTrue, shardStale: true, orchDown: true, want: metav1.ConditionUnknown, reason: "OrchestratorHealthStale"},
		{name: "old shard generation", quorum: metav1.ConditionTrue, shardStale: true, want: metav1.ConditionUnknown, reason: "OrchestratorHealthStale"},
		{name: "topology writes fail despite quorum", quorum: metav1.ConditionTrue, accessFailed: true, want: metav1.ConditionFalse, reason: "TopologyUnavailable"},
		{name: "external topology uses registration and orch", external: true, want: metav1.ConditionTrue, reason: "FailoverReady"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
			global := &multigresv1alpha1.GlobalTopoServerSpec{Etcd: &multigresv1alpha1.EtcdSpec{}}
			if tc.external {
				global = &multigresv1alpha1.GlobalTopoServerSpec{
					External: &multigresv1alpha1.ExternalTopoServerSpec{
						Endpoints: []multigresv1alpha1.EndpointUrl{"http://external:2379"},
					},
				}
			}
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default", Generation: 1},
				Spec: multigresv1alpha1.MultigresClusterSpec{
					GlobalTopoServer: global,
					Databases: []multigresv1alpha1.DatabaseConfig{
						{
							Name: "postgres",
							TableGroups: []multigresv1alpha1.TableGroupConfig{
								{
									Name:   "default",
									Shards: []multigresv1alpha1.ShardConfig{{Name: "0-inf"}},
								},
							},
						},
					},
				},
			}
			access := metav1.Condition{
				Type:               conditionTopologyReady,
				ObservedGeneration: 1,
				Status:             metav1.ConditionTrue,
				Reason:             "TopoConnected",
				Message:            "Topology reachable",
			}
			if tc.accessFailed {
				access.Status, access.Reason, access.Message = metav1.ConditionFalse, "TopologyUnavailable", "Topology write failed"
			}
			cluster.Status.Conditions = []metav1.Condition{
				access,
				{Type: "Available", Status: metav1.ConditionTrue, Reason: "CellsReady"},
			}
			checked := metav1.Now()
			if tc.stale {
				checked = metav1.NewTime(time.Now().Add(-3 * time.Minute))
			}
			servers := []multigresv1alpha1.TopoServer{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "example-global-topo", Generation: 1},
					Status: multigresv1alpha1.TopoServerStatus{
						Phase:           multigresv1alpha1.PhaseHealthy,
						HealthCheckedAt: &checked,
						Conditions: []metav1.Condition{
							{
								Type:               "QuorumAvailable",
								ObservedGeneration: 1,
								Status:             tc.quorum,
								Reason:             "QuorumUnavailable",
								Message:            "Quorum observation",
							},
						},
					},
				},
			}
			if tc.quorumReason != "" {
				servers[0].Status.Conditions[0].Reason = tc.quorumReason
				servers[0].Status.Conditions[0].Message = "Etcd member reports NOSPACE"
			}
			if tc.missing || tc.external {
				servers = nil
			}
			shard := &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "example-shard",
					Namespace:  "default",
					Generation: 1,
					Labels:     map[string]string{metadata.LabelMultigresCluster: "example"},
				},
				Spec: multigresv1alpha1.ShardSpec{
					DatabaseName:   "postgres",
					TableGroupName: "default",
					ShardName:      "0-inf",
				},
				Status: multigresv1alpha1.ShardStatus{
					ObservedGeneration: 1,
					OrchReady:          !tc.orchDown,
				},
			}
			if tc.shardStale {
				shard.Status.ObservedGeneration = 0
			}
			objects := []client.Object{cluster}
			if !tc.shardMissing {
				objects = append(objects, shard)
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			r := &MultigresClusterReconciler{Client: c, APIReader: c}
			if tc.observationFailed {
				r.APIReader = testutil.NewFakeClientWithFailures(
					c,
					&testutil.FailureConfig{OnList: func(list client.ObjectList) error {
						if _, ok := list.(*multigresv1alpha1.ShardList); ok {
							return errors.New("shard list unavailable")
						}
						return nil
					}},
				)
			}

			r.updateHealthConditions(t.Context(), cluster, global, servers)
			condition := meta.FindStatusCondition(cluster.Status.Conditions, conditionFailoverReady)
			require.Equal(t, tc.want, condition.Status)
			require.Equal(t, tc.reason, condition.Reason)
			if tc.quorumReason != "" {
				require.Contains(t, condition.Message, "NOSPACE")
			}
			require.True(
				t,
				meta.IsStatusConditionTrue(cluster.Status.Conditions, "Available"),
				"SQL availability remains separately reported",
			)
			if tc.external {
				require.Equal(
					t,
					metav1.ConditionUnknown,
					meta.FindStatusCondition(
						cluster.Status.Conditions,
						conditionTopologyQuorumAvailable,
					).Status,
				)
			}
		})
	}
}

func TestTopologyUnavailableClearsPreviousReadinessDuringGracePeriod(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, multigresv1alpha1.AddToScheme(scheme))
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "example",
			Namespace:         "default",
			CreationTimestamp: metav1.Now(),
		},
		Status: multigresv1alpha1.MultigresClusterStatus{
			Conditions: []metav1.Condition{
				{Type: conditionTopologyReady, Status: metav1.ConditionTrue},
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := &MultigresClusterReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}
	result, err := r.handleTopoUnavailable(
		t.Context(),
		cluster,
		errors.New("linearizable read failed"),
		discardHealthLogger{},
	)
	require.NoError(t, err)
	require.Equal(t, topoUnavailableRequeueDelay, result.RequeueAfter)
	fresh := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), fresh))
	condition := meta.FindStatusCondition(fresh.Status.Conditions, conditionTopologyReady)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "linearizable read failed", condition.Message)
}

type discardHealthLogger struct{}

func (discardHealthLogger) Info(string, ...any)         {}
func (discardHealthLogger) Error(error, string, ...any) {}

func TestReconcileRefreshesFailoverStatusOnTopologyRequeue(t *testing.T) {
	scheme := setupScheme()
	core, cell, shard, cluster, _, _ := setupFixtures(t)
	cluster.CreationTimestamp = metav1.Now()
	cluster.Status.Conditions = []metav1.Condition{
		{
			Type:    conditionTopologyReady,
			Status:  metav1.ConditionTrue,
			Reason:  "TopoConnected",
			Message: "Topology reachable",
		},
		{
			Type:    conditionFailoverReady,
			Status:  metav1.ConditionTrue,
			Reason:  "FailoverReady",
			Message: "Orchestrators ready",
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(core, cell, shard, cluster).
		WithStatusSubresource(cluster).
		Build()
	r := &MultigresClusterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
		CreateTopoStore: func(multigresv1alpha1.GlobalTopoServerRef) (topoclient.Store, error) {
			return nil, errors.New("UNAVAILABLE: quorum read failed")
		},
	}
	result, err := r.Reconcile(
		t.Context(),
		ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)},
	)
	require.NoError(t, err)
	require.Equal(t, topoUnavailableRequeueDelay, result.RequeueAfter)
	fresh := &multigresv1alpha1.MultigresCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(cluster), fresh))
	require.True(t, meta.IsStatusConditionFalse(fresh.Status.Conditions, conditionTopologyReady))
	require.True(t, meta.IsStatusConditionFalse(fresh.Status.Conditions, conditionFailoverReady))
	require.Equal(t, multigresv1alpha1.PhaseDegraded, fresh.Status.Phase)
}

func TestManagedLocalTopologyQuorumIsRequired(t *testing.T) {
	scheme := setupScheme()
	global := &multigresv1alpha1.GlobalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{"http://external:2379"},
		},
	}
	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: global,
			Cells: []multigresv1alpha1.CellConfig{
				{
					Name: "local",
					Spec: &multigresv1alpha1.CellInlineSpec{
						LocalTopoServer: &multigresv1alpha1.LocalTopoServerSpec{
							Etcd: &multigresv1alpha1.EtcdSpec{},
						},
					},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &MultigresClusterReconciler{Client: c}
	condition := r.topologyQuorumCondition(t.Context(), cluster, global, nil)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "TopologyMissing", condition.Reason)
	local := healthyManagedLocalTopoServer(
		expectedManagedLocalCell(cluster.Name, "local", cluster.Namespace),
	)
	checked := metav1.Now()
	local.Status.HealthCheckedAt = &checked
	local.Status.Conditions = []metav1.Condition{
		{
			Type:               "QuorumAvailable",
			ObservedGeneration: local.Generation,
			Status:             metav1.ConditionFalse,
			Reason:             "QuorumUnavailable",
			Message:            "No linearizable read succeeded",
		},
	}
	condition = r.topologyQuorumCondition(
		t.Context(),
		cluster,
		global,
		[]multigresv1alpha1.TopoServer{*local},
	)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "QuorumUnavailable", condition.Reason)
}
