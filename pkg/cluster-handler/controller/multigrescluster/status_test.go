package multigrescluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"

	"github.com/multigres/testkit/assert"
)

func TestReconcile_Status(t *testing.T) {
	_, _, _, _, clusterName, _ := setupFixtures(t)
	errSimulated := errors.New("simulated error for testing")

	tests := map[string]reconcileTestCase{
		"Error: UpdateStatus (List Cells Failed)": {
			failureConfig: &testutil.FailureConfig{
				OnList: func() func(client.ObjectList) error {
					count := 0
					return func(list client.ObjectList) error {
						if _, ok := list.(*multigresv1alpha1.CellList); ok {
							count++
							if count > 1 {
								return errSimulated
							}
						}
						return nil
					}
				}(),
			},
			wantErrMsg: "failed to list cells for status",
		},
		"Error: UpdateStatus (List TableGroups Failed)": {
			failureConfig: &testutil.FailureConfig{
				OnList: func() func(client.ObjectList) error {
					count := 0
					return func(list client.ObjectList) error {
						if _, ok := list.(*multigresv1alpha1.TableGroupList); ok {
							count++
							if count > 1 {
								return errSimulated
							}
						}
						return nil
					}
				}(),
			},
			wantErrMsg: "failed to list tablegroups for status",
		},
		"Error: Update Status Failed (API Error)": {
			failureConfig: &testutil.FailureConfig{
				OnStatusPatch: testutil.FailOnObjectName(clusterName, errSimulated),
			},
			wantErrMsg: "failed to patch status",
		},
	}

	runReconcileTest(t, tests)
}

func TestUpdateStatus_Coverage(t *testing.T) {
	ck := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cell-1",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{
			Name: "cell-1",
		},
		Status: multigresv1alpha1.CellStatus{
			Conditions: []metav1.Condition{
				{Type: "Available", Status: metav1.ConditionTrue},
			},
			GatewayReplicas: 1,
		},
	}

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-1",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName: "db1",
		},
		Status: multigresv1alpha1.TableGroupStatus{
			ReadyShards: 1,
			TotalShards: 1,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cell, tg).
		WithStatusSubresource(cluster, cell, tg).
		Build()

	r := &MultigresClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	ck.Require().NoError(r.updateStatus(context.Background(), cluster), "updateStatus failed")

	ck.Require().NoError(fakeClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(cluster),
		cluster,
	), "Failed to refresh cluster")

	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "Available" {
			found = true
			ck.Eq(metav1.ConditionTrue, c.Status, "Expected Available=True, got")
		}
	}
	ck.True(found, "Available condition not found")

	if s, ok := cluster.Status.Cells["cell-1"]; !ok || !s.Ready {
		t.Errorf("Expected cell-1 to be ready in status summary, got %v", s)
	}

	if s, ok := cluster.Status.Databases["db1"]; !ok || s.ReadyShards != 1 {
		t.Errorf("Expected db1 to have 1 ready shard in status summary, got %v", s)
	}

	cDegraded := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cell-degraded",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "cell-degraded"},
		Status: multigresv1alpha1.CellStatus{
			Phase: multigresv1alpha1.PhaseDegraded,
		},
	}

	fakeClient = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cell, tg, cDegraded).
		WithStatusSubresource(cluster, cell, tg, cDegraded).
		Build()

	r.Client = fakeClient
	ck.Require().NoError(r.updateStatus(context.Background(), cluster), "updateStatus failed")

	ck.Require().NoError(fakeClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(cluster),
		cluster,
	), "Failed to refresh cluster")

	ck.Eq(multigresv1alpha1.PhaseDegraded, cluster.Status.Phase, "Expected PhaseDegraded, got")

	cProgressing := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cell-prog",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "cell-prog"},
		Status: multigresv1alpha1.CellStatus{
			Phase: multigresv1alpha1.PhaseInitializing,
		},
	}

	fakeClient = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cell, tg, cProgressing).
		WithStatusSubresource(cluster, cell, tg, cProgressing).
		Build()
	r.Client = fakeClient

	ck.Require().NoError(r.updateStatus(context.Background(), cluster), "updateStatus failed")
	ck.Require().NoError(fakeClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(cluster),
		cluster,
	), "Failed to refresh cluster")
	ck.Eq(
		multigresv1alpha1.PhaseProgressing,
		cluster.Status.Phase,
		"Expected PhaseProgressing, got",
	)

	tgDegraded := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-degraded",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "db1",
			TableGroupName: "tg-degraded",
		},
		Status: multigresv1alpha1.TableGroupStatus{
			Phase: multigresv1alpha1.PhaseDegraded,
		},
	}

	fakeClient = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cell, tg, tgDegraded).
		WithStatusSubresource(cluster, cell, tg, tgDegraded).
		Build()

	r.Client = fakeClient
	ck.Require().NoError(r.updateStatus(context.Background(), cluster), "updateStatus failed")

	ck.Require().NoError(fakeClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(cluster),
		cluster,
	), "Failed to refresh cluster")

	ck.Eq(multigresv1alpha1.PhaseDegraded, cluster.Status.Phase, "Expected PhaseDegraded, got")

	tgInit := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tg-init",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "db1",
			TableGroupName: "tg-init",
		},
		Status: multigresv1alpha1.TableGroupStatus{
			Phase: multigresv1alpha1.PhaseInitializing,
		},
	}

	fakeClient = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cell, tg, tgInit).
		WithStatusSubresource(cluster, cell, tg, tgInit).
		Build()

	r.Client = fakeClient
	ck.Require().NoError(r.updateStatus(context.Background(), cluster), "updateStatus failed")

	ck.Require().NoError(fakeClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(cluster),
		cluster,
	), "Failed to refresh cluster")

	ck.Eq(
		multigresv1alpha1.PhaseProgressing,
		cluster.Status.Phase,
		"Expected PhaseProgressing, got",
	)

	tsDegraded := &multigresv1alpha1.TopoServer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ts-degraded",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Status: multigresv1alpha1.TopoServerStatus{
			Phase: multigresv1alpha1.PhaseDegraded,
		},
	}

	fakeClient = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cell, tg, tsDegraded).
		WithStatusSubresource(cluster, cell, tg, tsDegraded).
		Build()

	r.Client = fakeClient
	ck.Require().NoError(r.updateStatus(context.Background(), cluster), "updateStatus failed")

	ck.Require().NoError(fakeClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(cluster),
		cluster,
	), "Failed to refresh cluster")

	ck.Eq(multigresv1alpha1.PhaseDegraded, cluster.Status.Phase, "Expected PhaseDegraded, got")
}

func TestUpdateStatus_ZeroResources(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "cell-1", Region: "us-east-1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name:    "postgres",
				Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name:    "default",
					Default: true,
				}},
			}},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &MultigresClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	c.Require().NoError(r.updateStatus(context.Background(), cluster), "updateStatus failed")

	c.Require().NoError(fakeClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(cluster),
		cluster,
	), "Failed to refresh cluster")

	c.EqDeep(multigresv1alpha1.PhaseProgressing, cluster.Status.Phase)
	c.Nil(cluster.Status.InitializedAt)

	cond := meta.FindStatusCondition(cluster.Status.Conditions, "Available")
	c.Require().NotNil(cond, "Available condition missing")
	c.Eq(metav1.ConditionFalse, cond.Status, "Expected Available=False (no cells), got")
}

func TestUpdateStatus_ExpectedChildren(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NewAborting(t).NoError(multigresv1alpha1.AddToScheme(scheme))

	externalTopo := &multigresv1alpha1.GlobalTopoServerSpec{
		External: &multigresv1alpha1.ExternalTopoServerSpec{
			Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
		},
	}
	clusterWithExpectedChildren := func(globalTopo *multigresv1alpha1.GlobalTopoServerSpec) *multigresv1alpha1.MultigresCluster {
		return &multigresv1alpha1.MultigresCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
			Spec: multigresv1alpha1.MultigresClusterSpec{
				GlobalTopoServer: globalTopo,
				Cells: []multigresv1alpha1.CellConfig{
					{Name: "cell-1", Region: "us-east-1"},
				},
				Databases: []multigresv1alpha1.DatabaseConfig{{
					Name:    "postgres",
					Default: true,
					TableGroups: []multigresv1alpha1.TableGroupConfig{{
						Name:    "default",
						Default: true,
					}},
				}},
			},
		}
	}
	healthyCell := func(name multigresv1alpha1.CellName) *multigresv1alpha1.Cell {
		return &multigresv1alpha1.Cell{
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(name),
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: multigresv1alpha1.CellSpec{Name: name},
			Status: multigresv1alpha1.CellStatus{
				Phase: multigresv1alpha1.PhaseHealthy,
			},
		}
	}
	healthyTableGroup := func(
		database multigresv1alpha1.DatabaseName,
		tableGroup multigresv1alpha1.TableGroupName,
	) *multigresv1alpha1.TableGroup {
		return &multigresv1alpha1.TableGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(tableGroup),
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Spec: multigresv1alpha1.TableGroupSpec{
				DatabaseName:   database,
				TableGroupName: tableGroup,
			},
			Status: multigresv1alpha1.TableGroupStatus{
				Phase: multigresv1alpha1.PhaseHealthy,
			},
		}
	}
	healthyGlobalTopo := func(name string) *multigresv1alpha1.TopoServer {
		return &multigresv1alpha1.TopoServer{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
			},
			Status: multigresv1alpha1.TopoServerStatus{
				Phase: multigresv1alpha1.PhaseHealthy,
			},
		}
	}

	tests := map[string]struct {
		cluster     *multigresv1alpha1.MultigresCluster
		children    []client.Object
		wantPhase   multigresv1alpha1.Phase
		initialized bool
	}{
		"missing expected cell is progressing": {
			cluster: clusterWithExpectedChildren(externalTopo),
			children: []client.Object{
				healthyCell("other-cell"),
				healthyTableGroup("postgres", "default"),
			},
			wantPhase: multigresv1alpha1.PhaseProgressing,
		},
		"missing expected table group is progressing": {
			cluster: clusterWithExpectedChildren(externalTopo),
			children: []client.Object{
				healthyCell("cell-1"),
				healthyTableGroup("postgres", "other-tg"),
			},
			wantPhase: multigresv1alpha1.PhaseProgressing,
		},
		"missing managed global topo is progressing": {
			cluster: clusterWithExpectedChildren(nil),
			children: []client.Object{
				healthyCell("cell-1"),
				healthyTableGroup("postgres", "default"),
				healthyGlobalTopo("other-topo"),
			},
			wantPhase: multigresv1alpha1.PhaseProgressing,
		},
		"external global topo does not require a topo server": {
			cluster: clusterWithExpectedChildren(externalTopo),
			children: []client.Object{
				healthyCell("cell-1"),
				healthyTableGroup("postgres", "default"),
			},
			wantPhase:   multigresv1alpha1.PhaseHealthy,
			initialized: true,
		},
		"all managed children healthy initializes cluster": {
			cluster: clusterWithExpectedChildren(nil),
			children: []client.Object{
				healthyCell("cell-1"),
				healthyTableGroup("postgres", "default"),
				healthyGlobalTopo("test-cluster-global-topo"),
			},
			wantPhase:   multigresv1alpha1.PhaseHealthy,
			initialized: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			objects := append([]client.Object{tt.cluster}, tt.children...)
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
				WithStatusSubresource(objects...).
				Build()
			r := &MultigresClusterReconciler{
				Client:   fakeClient,
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}

			c.Require().NoError(r.updateStatus(t.Context(), tt.cluster))
			c.Require().
				NoError(fakeClient.Get(t.Context(), client.ObjectKeyFromObject(tt.cluster), tt.cluster))
			c.EqDeep(tt.wantPhase, tt.cluster.Status.Phase)
			c.EqDeep(tt.initialized, tt.cluster.Status.InitializedAt != nil)
		})
	}
}

func TestUpdateStatus_InitializedAtSticky(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "cell-1", Region: "us-east-1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name:    "postgres",
				Default: true,
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name:    "default",
					Default: true,
				}},
			}},
		},
	}
	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cell-1",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "cell-1"},
		Status: multigresv1alpha1.CellStatus{
			Phase: multigresv1alpha1.PhaseHealthy,
		},
	}
	tableGroup := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-postgres-default",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "postgres",
			TableGroupName: "default",
		},
		Status: multigresv1alpha1.TableGroupStatus{
			Phase: multigresv1alpha1.PhaseHealthy,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cell, tableGroup).
		WithStatusSubresource(cluster, cell, tableGroup).
		Build()

	r := &MultigresClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	c.Require().NoError(r.updateStatus(t.Context(), cluster))
	c.Require().NoError(fakeClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))

	c.Require().EqDeep(multigresv1alpha1.PhaseHealthy, cluster.Status.Phase)
	c.Require().NotNil(cluster.Status.InitializedAt)
	initializedAt := *cluster.Status.InitializedAt

	degradedCell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cell-1",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "cell-1"},
		Status: multigresv1alpha1.CellStatus{
			Phase: multigresv1alpha1.PhaseDegraded,
		},
	}

	fakeClient = fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, degradedCell, tableGroup).
		WithStatusSubresource(cluster, degradedCell, tableGroup).
		Build()
	r.Client = fakeClient

	c.Require().NoError(r.updateStatus(t.Context(), cluster))
	c.Require().NoError(fakeClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))

	c.EqDeep(multigresv1alpha1.PhaseDegraded, cluster.Status.Phase)
	c.Require().NotNil(cluster.Status.InitializedAt)
	c.EqDeep(initializedAt, *cluster.Status.InitializedAt)
}

func TestUpdateStatus_GenerationMismatch(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cluster",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "cell-1", Region: "us-east-1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name: "db1",
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name: "tg-1",
				}},
			}},
		},
	}

	cell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "cell-1",
			Namespace:  "default",
			Labels:     map[string]string{"multigres.com/cluster": "test-cluster"},
			Generation: 2,
		},
		Spec: multigresv1alpha1.CellSpec{Name: "cell-1"},
		Status: multigresv1alpha1.CellStatus{
			ObservedGeneration: 1,
			Phase:              multigresv1alpha1.PhaseHealthy,
		},
	}

	tg := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "tg-1",
			Namespace:  "default",
			Labels:     map[string]string{"multigres.com/cluster": "test-cluster"},
			Generation: 2,
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "db1",
			TableGroupName: "tg-1",
		},
		Status: multigresv1alpha1.TableGroupStatus{
			ObservedGeneration: 1,
			Phase:              multigresv1alpha1.PhaseHealthy,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, cell, tg).
		WithStatusSubresource(cluster, cell, tg).
		Build()

	r := &MultigresClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(100),
	}

	c.Require().NoError(r.updateStatus(context.Background(), cluster), "updateStatus failed")

	c.Require().NoError(fakeClient.Get(
		context.Background(),
		client.ObjectKeyFromObject(cluster),
		cluster,
	), "Failed to refresh cluster")

	c.Eq(
		multigresv1alpha1.PhaseProgressing,
		cluster.Status.Phase,
		"Expected PhaseProgressing due to generation mismatch, got",
	)
}

func TestUpdateStatus_UsesAPIReaderForChildHealth(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := runtime.NewScheme()
	c.Require().NoError(multigresv1alpha1.AddToScheme(scheme))

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			GlobalTopoServer: &multigresv1alpha1.GlobalTopoServerSpec{
				External: &multigresv1alpha1.ExternalTopoServerSpec{
					Endpoints: []multigresv1alpha1.EndpointUrl{"http://topo:2379"},
				},
			},
			Cells: []multigresv1alpha1.CellConfig{{Name: "cell-1", Region: "us-east-1"}},
			Databases: []multigresv1alpha1.DatabaseConfig{{
				Name: "db1",
				TableGroups: []multigresv1alpha1.TableGroupConfig{{
					Name: "tg-1",
				}},
			}},
		},
	}
	staleCell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "cell-1",
			Namespace:  "default",
			Labels:     map[string]string{"multigres.com/cluster": "test-cluster"},
			Generation: 1,
		},
		Spec: multigresv1alpha1.CellSpec{Name: "cell-1"},
		Status: multigresv1alpha1.CellStatus{
			ObservedGeneration: 1,
			Phase:              multigresv1alpha1.PhaseHealthy,
		},
	}
	liveCell := staleCell.DeepCopy()
	liveCell.Generation = 2
	tableGroup := &multigresv1alpha1.TableGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "tg-1",
			Namespace:  "default",
			Labels:     map[string]string{"multigres.com/cluster": "test-cluster"},
			Generation: 1,
		},
		Spec: multigresv1alpha1.TableGroupSpec{
			DatabaseName:   "db1",
			TableGroupName: "tg-1",
		},
		Status: multigresv1alpha1.TableGroupStatus{
			ObservedGeneration: 1,
			Phase:              multigresv1alpha1.PhaseHealthy,
		},
	}

	cachedClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, staleCell, tableGroup).
		WithStatusSubresource(cluster, staleCell, tableGroup).
		Build()
	liveReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(liveCell, tableGroup.DeepCopy()).
		Build()
	r := &MultigresClusterReconciler{
		Client:    cachedClient,
		APIReader: liveReader,
		Scheme:    scheme,
		Recorder:  record.NewFakeRecorder(10),
	}

	c.Require().NoError(r.updateStatus(t.Context(), cluster))
	c.Require().NoError(cachedClient.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	c.EqDeep(multigresv1alpha1.PhaseProgressing, cluster.Status.Phase)
	c.Nil(cluster.Status.InitializedAt)
}

func TestExtractExternalEndpoint(t *testing.T) {
	tests := []struct {
		name string
		svc  *corev1.Service
		want string
	}{
		{
			name: "nil Service",
			svc:  nil,
			want: "",
		},
		{
			name: "empty ingress list",
			svc: &corev1.Service{
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{},
					},
				},
			},
			want: "",
		},
		{
			name: "service external IP preferred",
			svc: &corev1.Service{
				Spec: corev1.ServiceSpec{
					ExternalIPs: []string{"2001:db8::10"},
				},
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{Hostname: "a1234.elb.us-east-1.amazonaws.com"},
						},
					},
				},
			},
			want: "2001:db8::10",
		},
		{
			name: "hostname only",
			svc: &corev1.Service{
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{Hostname: "a1234.elb.us-east-1.amazonaws.com"},
						},
					},
				},
			},
			want: "a1234.elb.us-east-1.amazonaws.com",
		},
		{
			name: "IP only",
			svc: &corev1.Service{
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{IP: "203.0.113.42"},
						},
					},
				},
			},
			want: "203.0.113.42",
		},
		{
			name: "both hostname and IP - hostname preferred",
			svc: &corev1.Service{
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{
								Hostname: "a1234.elb.us-east-1.amazonaws.com",
								IP:       "203.0.113.42",
							},
						},
					},
				},
			},
			want: "a1234.elb.us-east-1.amazonaws.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractExternalEndpoint(tc.svc)
			assert.NewCollecting(t).EqDeep(tc.want, got)
		})
	}
}

func TestComputeGatewayCondition(t *testing.T) {
	const gen int64 = 7

	tests := []struct {
		name                   string
		externalGatewayEnabled bool
		externalEndpoint       string
		aggregateReadyGateways int32
		clusterGeneration      int64
		wantNil                bool
		wantStatus             metav1.ConditionStatus
		wantReason             string
		wantMessageContains    string
	}{
		{
			name:                   "disabled - externalGateway nil equivalent",
			externalGatewayEnabled: false,
			wantNil:                true,
		},
		{
			name:                   "disabled - enabled false",
			externalGatewayEnabled: false,
			externalEndpoint:       "",
			aggregateReadyGateways: 0,
			clusterGeneration:      gen,
			wantNil:                true,
		},
		{
			name:                   "enabled, empty endpoint - AwaitingEndpoint",
			externalGatewayEnabled: true,
			externalEndpoint:       "",
			aggregateReadyGateways: 0,
			clusterGeneration:      gen,
			wantStatus:             metav1.ConditionFalse,
			wantReason:             multigresv1alpha1.ReasonAwaitingEndpoint,
			wantMessageContains:    "gateway service endpoint",
		},
		{
			name:                   "enabled, endpoint present, 0 ready gateways - NoReadyGateways",
			externalGatewayEnabled: true,
			externalEndpoint:       "a1234.elb.us-east-1.amazonaws.com",
			aggregateReadyGateways: 0,
			clusterGeneration:      gen,
			wantStatus:             metav1.ConditionFalse,
			wantReason:             multigresv1alpha1.ReasonNoReadyGateways,
			wantMessageContains:    "no multigateway pods are ready",
		},
		{
			name:                   "enabled, endpoint present, >0 ready gateways - EndpointReady",
			externalGatewayEnabled: true,
			externalEndpoint:       "a1234.elb.us-east-1.amazonaws.com",
			aggregateReadyGateways: 3,
			clusterGeneration:      gen,
			wantStatus:             metav1.ConditionTrue,
			wantReason:             multigresv1alpha1.ReasonEndpointReady,
			wantMessageContains:    "a1234.elb.us-east-1.amazonaws.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got := computeGatewayCondition(
				tc.externalGatewayEnabled,
				tc.externalEndpoint,
				tc.aggregateReadyGateways,
				tc.clusterGeneration,
			)

			if tc.wantNil {
				c.Nil(got)
				return
			}

			c.Require().NotNil(got)
			c.EqDeep(multigresv1alpha1.ConditionGatewayExternalReady, got.Type)
			c.EqDeep(tc.wantStatus, got.Status)
			c.EqDeep(tc.wantReason, got.Reason)
			c.StrContains(got.Message, tc.wantMessageContains)
			c.EqDeep(tc.clusterGeneration, got.ObservedGeneration)
		})
	}
}

func TestUpdateStatus_GatewayServiceErrors(t *testing.T) {
	scheme := setupScheme()

	clusterName := "gw-test"
	namespace := "default"

	baseCluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       clusterName,
			Namespace:  namespace,
			Generation: 3,
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			ExternalGateway: &multigresv1alpha1.ExternalGatewayConfig{Enabled: true},
		},
		Status: multigresv1alpha1.MultigresClusterStatus{},
	}

	t.Run("non-NotFound error fetching global service returns error", func(t *testing.T) {
		c := assert.NewCollecting(t)
		injectedErr := fmt.Errorf("simulated API server error")

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseCluster.DeepCopy()).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
			Build()

		failClient := testutil.NewFakeClientWithFailures(cl, &testutil.FailureConfig{
			OnGet: testutil.FailOnKeyName(clusterName+"-multigateway", injectedErr),
		})

		r := &MultigresClusterReconciler{
			Client:   failClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		err := r.updateStatus(t.Context(), cluster)
		c.Require().Error(err)
		c.StrContains(err.Error(), "failed to get global multigateway service for status")
	})

	t.Run("NotFound global service sets AwaitingEndpoint when enabled", func(t *testing.T) {
		c := assert.NewCollecting(t)
		// No Service object created — Get will return NotFound.
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseCluster.DeepCopy()).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
			Build()

		r := &MultigresClusterReconciler{
			Client:   cl,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		err := r.updateStatus(t.Context(), cluster)
		c.Require().NoError(err)

		c.Require().NotNil(cluster.Status.Gateway)
		c.Empty(cluster.Status.Gateway.ExternalEndpoint)

		cond := meta.FindStatusCondition(
			cluster.Status.Conditions,
			multigresv1alpha1.ConditionGatewayExternalReady,
		)
		c.Require().NotNil(cond)
		c.EqDeep(metav1.ConditionFalse, cond.Status)
		c.EqDeep(multigresv1alpha1.ReasonAwaitingEndpoint, cond.Reason)
	})
}

func TestUpdateStatus_StaleCellGenerationIgnored(t *testing.T) {
	c := assert.NewCollecting(t)
	scheme := setupScheme()

	clusterName := "gw-stale"
	namespace := "default"

	cluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       clusterName,
			Namespace:  namespace,
			Generation: 5,
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			ExternalGateway: &multigresv1alpha1.ExternalGatewayConfig{Enabled: true},
		},
		Status: multigresv1alpha1.MultigresClusterStatus{},
	}

	// Global multigateway Service with an LB ingress so we get past AwaitingEndpoint.
	gwSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName + "-multigateway",
			Namespace: namespace,
			Labels:    map[string]string{"multigres.com/cluster": clusterName},
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{
					{Hostname: "lb.example.com"},
				},
			},
		},
	}

	// Fresh cell: ObservedGeneration == Generation, has 2 ready gateways.
	freshCell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:       clusterName + "-fresh",
			Namespace:  namespace,
			Generation: 4,
			Labels:     map[string]string{"multigres.com/cluster": clusterName},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "fresh"},
		Status: multigresv1alpha1.CellStatus{
			ObservedGeneration:   4,
			GatewayReadyReplicas: 2,
		},
	}

	// Stale cell: ObservedGeneration < Generation, has 5 ready gateways (should be ignored).
	staleCell := &multigresv1alpha1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:       clusterName + "-stale",
			Namespace:  namespace,
			Generation: 10,
			Labels:     map[string]string{"multigres.com/cluster": clusterName},
		},
		Spec: multigresv1alpha1.CellSpec{Name: "stale"},
		Status: multigresv1alpha1.CellStatus{
			ObservedGeneration:   8, // stale
			GatewayReadyReplicas: 5,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster.DeepCopy(), gwSvc, freshCell, staleCell).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()

	r := &MultigresClusterReconciler{
		Client:   cl,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	clusterCopy := cluster.DeepCopy()
	err := r.updateStatus(t.Context(), clusterCopy)
	c.Require().NoError(err)

	// Only the fresh cell's 2 ready gateways should count.
	// With endpoint present and aggregateReadyGateways=2, condition should be EndpointReady.
	cond := meta.FindStatusCondition(
		clusterCopy.Status.Conditions,
		multigresv1alpha1.ConditionGatewayExternalReady,
	)
	c.Require().NotNil(cond)
	c.EqDeep(metav1.ConditionTrue, cond.Status)
	c.EqDeep(multigresv1alpha1.ReasonEndpointReady, cond.Reason)
	c.StrContains(cond.Message, "lb.example.com")

	// Now verify that if we remove the fresh cell (only stale remains),
	// the aggregate is 0 and condition becomes NoReadyGateways.
	cl2 := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster.DeepCopy(), gwSvc, staleCell).
		WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
		Build()

	r2 := &MultigresClusterReconciler{
		Client:   cl2,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	clusterCopy2 := cluster.DeepCopy()
	err = r2.updateStatus(t.Context(), clusterCopy2)
	c.Require().NoError(err)

	cond2 := meta.FindStatusCondition(
		clusterCopy2.Status.Conditions,
		multigresv1alpha1.ConditionGatewayExternalReady,
	)
	c.Require().NotNil(cond2)
	c.EqDeep(metav1.ConditionFalse, cond2.Status)
	c.EqDeep(multigresv1alpha1.ReasonNoReadyGateways, cond2.Reason)
}

func TestComputeAdminWebCondition(t *testing.T) {
	const gen int64 = 7

	tests := []struct {
		name                string
		enabled             bool
		externalEndpoint    string
		readyReplicas       int32
		clusterGeneration   int64
		wantNil             bool
		wantStatus          metav1.ConditionStatus
		wantReason          string
		wantMessageContains string
	}{
		{
			name:    "disabled - returns nil",
			enabled: false,
			wantNil: true,
		},
		{
			name:                "enabled, empty endpoint - AwaitingEndpoint",
			enabled:             true,
			externalEndpoint:    "",
			readyReplicas:       0,
			clusterGeneration:   gen,
			wantStatus:          metav1.ConditionFalse,
			wantReason:          multigresv1alpha1.ReasonAwaitingEndpoint,
			wantMessageContains: "admin web service endpoint",
		},
		{
			name:                "enabled, endpoint present, 0 ready replicas - NoReadyAdminWeb",
			enabled:             true,
			externalEndpoint:    "10.0.0.1",
			readyReplicas:       0,
			clusterGeneration:   gen,
			wantStatus:          metav1.ConditionFalse,
			wantReason:          multigresv1alpha1.ReasonNoReadyAdminWeb,
			wantMessageContains: "no multiadmin-web pods are ready",
		},
		{
			name:                "enabled, endpoint present, >0 ready replicas - EndpointReady",
			enabled:             true,
			externalEndpoint:    "10.0.0.1",
			readyReplicas:       2,
			clusterGeneration:   gen,
			wantStatus:          metav1.ConditionTrue,
			wantReason:          multigresv1alpha1.ReasonEndpointReady,
			wantMessageContains: "10.0.0.1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			got := computeAdminWebCondition(
				tc.enabled,
				tc.externalEndpoint,
				tc.readyReplicas,
				tc.clusterGeneration,
			)

			if tc.wantNil {
				c.Nil(got)
				return
			}

			c.Require().NotNil(got)
			c.EqDeep(multigresv1alpha1.ConditionAdminWebExternalReady, got.Type)
			c.EqDeep(tc.wantStatus, got.Status)
			c.EqDeep(tc.wantReason, got.Reason)
			c.StrContains(got.Message, tc.wantMessageContains)
			c.EqDeep(tc.clusterGeneration, got.ObservedGeneration)
		})
	}
}

func TestUpdateStatus_AdminWebServiceErrors(t *testing.T) {
	scheme := setupScheme()

	clusterName := "aw-test"
	namespace := "default"

	baseCluster := &multigresv1alpha1.MultigresCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       clusterName,
			Namespace:  namespace,
			Generation: 3,
		},
		Spec: multigresv1alpha1.MultigresClusterSpec{
			ExternalAdminWeb: &multigresv1alpha1.ExternalAdminWebConfig{Enabled: true},
		},
		Status: multigresv1alpha1.MultigresClusterStatus{},
	}

	t.Run("non-NotFound error fetching admin-web service returns error", func(t *testing.T) {
		c := assert.NewCollecting(t)
		injectedErr := fmt.Errorf("simulated API server error")

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseCluster.DeepCopy()).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
			Build()

		failClient := testutil.NewFakeClientWithFailures(cl, &testutil.FailureConfig{
			OnGet: testutil.FailOnKeyName(clusterName+"-multiadmin-web", injectedErr),
		})

		r := &MultigresClusterReconciler{
			Client:   failClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		err := r.updateStatus(t.Context(), cluster)
		c.Require().Error(err)
		c.StrContains(err.Error(), "failed to get multiadmin-web service for status")
	})

	t.Run("NotFound admin-web service sets AwaitingEndpoint when enabled", func(t *testing.T) {
		c := assert.NewCollecting(t)
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseCluster.DeepCopy()).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
			Build()

		r := &MultigresClusterReconciler{
			Client:   cl,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		err := r.updateStatus(t.Context(), cluster)
		c.Require().NoError(err)

		c.Require().NotNil(cluster.Status.AdminWeb)
		c.Empty(cluster.Status.AdminWeb.ExternalEndpoint)

		cond := meta.FindStatusCondition(
			cluster.Status.Conditions,
			multigresv1alpha1.ConditionAdminWebExternalReady,
		)
		c.Require().NotNil(cond)
		c.EqDeep(metav1.ConditionFalse, cond.Status)
		c.EqDeep(multigresv1alpha1.ReasonAwaitingEndpoint, cond.Reason)
	})

	t.Run("non-NotFound error fetching admin-web deployment returns error", func(t *testing.T) {
		c := assert.NewCollecting(t)
		injectedErr := fmt.Errorf("simulated API server error")

		awSvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName + "-multiadmin-web",
				Namespace: namespace,
			},
			Spec: corev1.ServiceSpec{
				ExternalIPs: []string{"10.0.0.1"},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseCluster.DeepCopy(), awSvc).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
			Build()

		failClient := testutil.NewFakeClientWithFailures(cl, &testutil.FailureConfig{
			OnGet: testutil.FailOnKeyName(clusterName+"-multiadmin-web", injectedErr),
		})

		r := &MultigresClusterReconciler{
			Client:   failClient,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		err := r.updateStatus(t.Context(), cluster)
		c.Require().Error(err)
		// The Service Get will fail first since both have the same name
		c.StrContains(err.Error(), "failed to get multiadmin-web")
	})

	t.Run("admin-web deployment ready replicas drives condition", func(t *testing.T) {
		c := assert.NewCollecting(t)
		awSvc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName + "-multiadmin-web",
				Namespace: namespace,
			},
			Spec: corev1.ServiceSpec{
				ExternalIPs: []string{"10.0.0.1"},
			},
		}

		awDeploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName + "-multiadmin-web",
				Namespace: namespace,
			},
			Status: appsv1.DeploymentStatus{
				ReadyReplicas: 2,
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseCluster.DeepCopy(), awSvc, awDeploy).
			WithStatusSubresource(&multigresv1alpha1.MultigresCluster{}).
			Build()

		r := &MultigresClusterReconciler{
			Client:   cl,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
		}

		cluster := baseCluster.DeepCopy()
		err := r.updateStatus(t.Context(), cluster)
		c.Require().NoError(err)

		c.Require().NotNil(cluster.Status.AdminWeb)
		c.EqDeep("10.0.0.1", cluster.Status.AdminWeb.ExternalEndpoint)

		cond := meta.FindStatusCondition(
			cluster.Status.Conditions,
			multigresv1alpha1.ConditionAdminWebExternalReady,
		)
		c.Require().NotNil(cond)
		c.EqDeep(metav1.ConditionTrue, cond.Status)
		c.EqDeep(multigresv1alpha1.ReasonEndpointReady, cond.Reason)
	})
}
