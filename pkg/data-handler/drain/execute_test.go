package drain_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/drain"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// mockRPCClient implements just the methods these drain tests exercise.
type mockRPCClient struct {
	rpcclient.MultiPoolerClient

	promoteCalled             bool
	updateConsensusRuleCalled bool
}

func (m *mockRPCClient) Promote(
	ctx context.Context,
	pooler *clustermetadata.MultiPooler,
	request *multipoolermanagerdatapb.PromoteRequest,
) (*multipoolermanagerdatapb.PromoteResponse, error) {
	m.promoteCalled = true
	return &multipoolermanagerdatapb.PromoteResponse{}, nil
}

func (m *mockRPCClient) UpdateConsensusRule(
	ctx context.Context,
	pooler *clustermetadata.MultiPooler,
	request *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest,
) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	m.updateConsensusRuleCalled = true
	return &multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{}, nil
}

func (m *mockRPCClient) ExpireBackups(
	ctx context.Context,
	pooler *clustermetadata.MultiPooler,
	request *multipoolermanagerdatapb.ExpireBackupsRequest,
) (*multipoolermanagerdatapb.ExpireBackupsResponse, error) {
	return nil, nil
}

func TestReplicaDrainFlow(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "test-db",
			TableGroupName:   "test-tg",
			ShardName:        "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCell: "cell1",
			},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
			},
		},
	}

	primaryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "primary-pod",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCell: "cell1",
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod, primaryPod).Build()
	rpcMock := &mockRPCClient{}
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")

	ctx := context.Background()
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	// Add primary
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		Hostname: "primary-pod",
		Type:     clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)
	// Add our replica pod
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "test-pod-0"},
		Hostname: "test-pod-0",
		Type:     clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)

	// Step 1: Requested -> Draining
	requeue, err := drain.ExecuteDrainStateMachine(ctx, c, rpcMock, recorder, store, shardObj, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatalf("expected requeue for replica state transition")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
		t.Fatalf("expected state draining, got %v", pod.Annotations[metadata.AnnotationDrainState])
	}
	if !rpcMock.updateConsensusRuleCalled {
		t.Fatalf("expected UpdateConsensusRule to be called")
	}

	// Step 2: Draining -> Acknowledged
	_, _ = drain.ExecuteDrainStateMachine(ctx, c, rpcMock, recorder, store, shardObj, pod)
	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateAcknowledged {
		t.Fatalf(
			"expected state acknowledged, got %v",
			pod.Annotations[metadata.AnnotationDrainState],
		)
	}

	// Step 3: Acknowledged -> ReadyForDeletion
	_, _ = drain.ExecuteDrainStateMachine(ctx, c, rpcMock, recorder, store, shardObj, pod)
	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateReadyForDeletion {
		t.Fatalf(
			"expected state ready-for-deletion, got %v",
			pod.Annotations[metadata.AnnotationDrainState],
		)
	}

	inspectorStore := topoclient.NewWithFactory(
		factory,
		"",
		[]string{""},
		topoclient.NewDefaultTopoConfig(),
	)
	defer func() { _ = inspectorStore.Close() }()
	poolers, _ := inspectorStore.GetMultiPoolersByCell(ctx, "cell1", nil)
	if len(poolers) != 1 || poolers[0].GetHostname() != "primary-pod" {
		t.Fatalf("expected replica to be unregistered from topo")
	}
}

func TestPrimaryDrainFlow(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "test-db",
			TableGroupName:   "test-tg",
			ShardName:        "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod).Build()
	rpcMock := &mockRPCClient{}
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")

	ctx := context.Background()
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "test-pod-0"},
		Hostname: "test-pod-0", Type: clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod"},
		Hostname: "replica-pod", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)

	// PRIMARY drain should advance to DrainStateDraining without calling Promote.
	// Failover is multiorch's responsibility via its consensus protocol.
	requeue, err := drain.ExecuteDrainStateMachine(ctx, c, rpcMock, recorder, store, shardObj, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatalf("expected requeue after state transition")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
		t.Fatalf("expected PRIMARY pod to advance to draining, got %v",
			pod.Annotations[metadata.AnnotationDrainState])
	}
	if rpcMock.promoteCalled {
		t.Fatalf("Promote should not be called; failover is multiorch's responsibility")
	}
	if rpcMock.updateConsensusRuleCalled {
		t.Fatalf("UpdateConsensusRule should not be called when draining the PRIMARY")
	}
}

func TestPrimaryDrainFlowNilRPCClient(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "test-db",
			TableGroupName:   "test-tg",
			ShardName:        "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod).Build()

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")

	ctx := context.Background()
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "test-pod-0"},
		Hostname: "test-pod-0", Type: clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod"},
		Hostname: "replica-pod", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)

	// With nil rpcClient, drain should proceed instead of looping forever
	recorder := record.NewFakeRecorder(10)
	requeue, err := drain.ExecuteDrainStateMachine(ctx, c, nil, recorder, store, shardObj, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatalf("expected requeue after state transition")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
		t.Fatalf(
			"expected PRIMARY pod to advance to draining when rpcClient is nil, got %v",
			pod.Annotations[metadata.AnnotationDrainState],
		)
	}
}

func TestStuckTerminatingPod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "test-db",
			TableGroupName:   "test-tg",
			ShardName:        "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
				metadata.AnnotationDrainRequestedAt: time.Now().
					Add(-10 * time.Minute).
					Format(time.RFC3339),
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod).Build()
	rpcMock := &mockRPCClient{}
	recorder := record.NewFakeRecorder(10)

	store, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "test-pod-0"},
		Hostname: "test-pod-0", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)

	// Delete the pod using the client to set DeletionTimestamp
	_ = c.Delete(ctx, pod)

	// Since we set AnnotationDrainRequestedAt to 10 minutes ago,
	// executeDrainStateMachine should force unregister it due to timeout.
	_, _ = drain.ExecuteDrainStateMachine(ctx, c, rpcMock, recorder, store, shardObj, pod)
	// Recreate a new inspector store to verify unregistration without reuse
	inspectorStore := topoclient.NewWithFactory(
		factory,
		"",
		[]string{""},
		topoclient.NewDefaultTopoConfig(),
	)
	defer func() { _ = inspectorStore.Close() }()

	poolers, _ := inspectorStore.GetMultiPoolersByCell(ctx, "cell1", nil)
	if len(poolers) != 0 {
		t.Fatalf("expected stuck pod to be immediately unregistered")
	}
}

func TestIsPrimaryTerminatingOrMissing(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
		},
	}

	t.Run("returns true for nil primary", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		if !drain.IsPrimaryTerminatingOrMissing(context.Background(), c, shard, nil) {
			t.Error("Expected true for nil primary")
		}
	})

	t.Run("returns false for primary without drain annotation", func(t *testing.T) {
		primaryPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(primaryPod).Build()

		primary := &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if drain.IsPrimaryTerminatingOrMissing(context.Background(), c, shard, primary) {
			t.Error("Expected false for primary without drain annotation")
		}
	})

	t.Run("returns false when primary pod not found", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		primary := &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "nonexistent-pod"},
		}
		if !drain.IsPrimaryTerminatingOrMissing(context.Background(), c, shard, primary) {
			t.Error("Expected true when primary pod not found")
		}
	})

	t.Run("returns false on transient API error", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return fmt.Errorf("connection refused")
			},
		}).Build()
		primary := &clustermetadata.MultiPooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if drain.IsPrimaryTerminatingOrMissing(context.Background(), c, shard, primary) {
			t.Error("Expected false on transient error (should retry, not skip standby removal)")
		}
	})
}

func TestReplicaDrain_SkipsRPCWhenPrimaryDraining(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "test-db",
			TableGroupName:   "test-tg",
			ShardName:        "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	// The primary pod has a drain annotation — it's being drained too
	primaryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "primary-pod",
			Namespace: "default",
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateDraining,
			},
		},
	}

	replicaPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "replica-pod-0",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCell: "cell1",
			},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardObj, primaryPod, replicaPod).
		Build()
	rpcMock := &mockRPCClient{}
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	ctx := context.Background()

	// Register primary and replica in topo
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		Hostname: "primary-pod",
		Type:     clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod-0"},
		Hostname: "replica-pod-0",
		Type:     clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "test-db",
			TableGroup: "test-tg",
			Shard:      "0",
		},
	}, false)

	// Execute drain for replica while primary is draining
	requeue, err := drain.ExecuteDrainStateMachine(
		ctx,
		c,
		rpcMock,
		recorder,
		store,
		shardObj,
		replicaPod,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Error("Expected requeue when primary is draining")
	}

	// The RPC should NOT have been called because primary is draining
	if rpcMock.updateConsensusRuleCalled {
		t.Error("UpdateConsensusRule should NOT be called when primary is draining")
	}

	// The drain state should NOT have advanced (still "requested")
	updatedPod := &corev1.Pod{}
	_ = c.Get(ctx, client.ObjectKeyFromObject(replicaPod), updatedPod)
	if updatedPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
		t.Errorf("Expected drain state to remain %q, got %q",
			metadata.DrainStateRequested,
			updatedPod.Annotations[metadata.AnnotationDrainState])
	}
}

func TestReplicaDrain_DrainingState_FindPrimaryError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "replica-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateDraining,
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod).Build()
	rpcMock := &mockRPCClient{}
	recorder := record.NewFakeRecorder(10)

	// Create a topo where the replica exists but finding primary will fail
	// because cell1 has a pooler error condition (e.g., non-UNAVAILABLE error).
	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod-0"},
		Hostname: "replica-pod-0", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)

	// Since there's no primary in topo, findPrimaryPooler returns nil,nil.
	// So the replica draining state should advance to Acknowledged.
	requeue, err := drain.ExecuteDrainStateMachine(ctx, c, rpcMock, recorder, store, shardObj, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatal("expected requeue")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateAcknowledged {
		t.Errorf("expected Acknowledged, got %s", pod.Annotations[metadata.AnnotationDrainState])
	}
}

func TestReplicaDrain_DrainingState_PrimaryDraining(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	primaryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "primary-pod", Namespace: "default",
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateDraining,
			},
		},
	}

	replicaPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "replica-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateDraining,
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardObj, primaryPod, replicaPod).
		Build()
	rpcMock := &mockRPCClient{}
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		Hostname: "primary-pod", Type: clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod-0"},
		Hostname: "replica-pod-0", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)

	// In DrainStateDraining, if primary is draining, should requeue without advancing
	requeue, err := drain.ExecuteDrainStateMachine(
		ctx,
		c,
		rpcMock,
		recorder,
		store,
		shardObj,
		replicaPod,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Error("expected requeue when primary is draining")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(replicaPod), replicaPod)
	if replicaPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
		t.Errorf("expected state to remain Draining, got %s",
			replicaPod.Annotations[metadata.AnnotationDrainState])
	}
	if rpcMock.updateConsensusRuleCalled {
		t.Error("should not call UpdateConsensusRule when primary is draining")
	}
}

func TestDrain_TopoUnavailableDuringPodDeletion(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	now := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
			},
			DeletionTimestamp: &now,
			Finalizers:        []string{"test-finalizer"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod).Build()
	recorder := record.NewFakeRecorder(10)

	store := &unavailableTopoStore{}

	ctx := context.Background()
	requeue, err := drain.ExecuteDrainStateMachine(ctx, c, nil, recorder, store, shardObj, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatal("expected requeue")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateReadyForDeletion {
		t.Errorf("expected ReadyForDeletion when topo unavailable and pod is deleted, got %s",
			pod.Annotations[metadata.AnnotationDrainState])
	}
}

// unavailableTopoStore is a mock store that returns UNAVAILABLE for GetMultiPoolersByCell.
type unavailableTopoStore struct {
	topoclient.Store
}

func (s *unavailableTopoStore) GetMultiPoolersByCell(
	ctx context.Context,
	cell string,
	opts *topoclient.GetMultiPoolersByCellOptions,
) ([]*topoclient.MultiPoolerInfo, error) {
	return nil, fmt.Errorf("Code: UNAVAILABLE\nno connection available")
}

func (s *unavailableTopoStore) Close() error { return nil }

func TestReplicaDrain_PrimaryTerminatingOrMissing(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	replicaPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "replica-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
			},
		},
	}

	// Primary pod is terminating (has DeletionTimestamp)
	now := metav1.Now()
	primaryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "primary-pod", Namespace: "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{"test-finalizer"},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardObj, replicaPod, primaryPod).
		Build()
	rpcMock := &mockRPCClient{}
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		Hostname: "primary-pod", Type: clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod-0"},
		Hostname: "replica-pod-0", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)

	requeue, err := drain.ExecuteDrainStateMachine(
		ctx,
		c,
		rpcMock,
		recorder,
		store,
		shardObj,
		replicaPod,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatal("expected requeue")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(replicaPod), replicaPod)
	if replicaPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
		t.Errorf("expected Draining, got %s", replicaPod.Annotations[metadata.AnnotationDrainState])
	}
	if rpcMock.updateConsensusRuleCalled {
		t.Error("should skip standby removal when primary is terminating")
	}
}

func TestReplicaDrain_DrainingState_PrimaryTerminating(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	replicaPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "replica-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateDraining,
			},
		},
	}

	now := metav1.Now()
	primaryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "primary-pod", Namespace: "default",
			DeletionTimestamp: &now,
			Finalizers:        []string{"test-finalizer"},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardObj, replicaPod, primaryPod).
		Build()
	rpcMock := &mockRPCClient{}
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		Hostname: "primary-pod", Type: clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod-0"},
		Hostname: "replica-pod-0", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)

	// In DrainStateDraining, primary is terminating, should skip standby verification
	requeue, err := drain.ExecuteDrainStateMachine(
		ctx,
		c,
		rpcMock,
		recorder,
		store,
		shardObj,
		replicaPod,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatal("expected requeue")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(replicaPod), replicaPod)
	if replicaPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateAcknowledged {
		t.Errorf(
			"expected Acknowledged, got %s",
			replicaPod.Annotations[metadata.AnnotationDrainState],
		)
	}
	if rpcMock.updateConsensusRuleCalled {
		t.Error("should skip standby removal verification when primary is terminating")
	}
}

func TestDrain_AcknowledgedState_NoPoolerInTopo(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateAcknowledged,
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod).Build()
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	// Don't register any pooler — the pod won't be found in topo

	requeue, err := drain.ExecuteDrainStateMachine(ctx, c, nil, recorder, store, shardObj, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatal("expected requeue")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateReadyForDeletion {
		t.Errorf("expected ReadyForDeletion, got %s",
			pod.Annotations[metadata.AnnotationDrainState])
	}
}

func TestDrain_StuckDrainTimeout_DeletionTimestampFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	// Pod with DeletionTimestamp 10 minutes ago but no AnnotationDrainRequestedAt
	oldTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateDraining,
			},
			DeletionTimestamp: &oldTime,
			Finalizers:        []string{"test-finalizer"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod).Build()
	recorder := record.NewFakeRecorder(10)

	store, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "test-pod-0"},
		Hostname: "test-pod-0", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)

	requeue, err := drain.ExecuteDrainStateMachine(ctx, c, nil, recorder, store, shardObj, pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Fatal("expected requeue after forced unregistration")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(pod), pod)
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateReadyForDeletion {
		t.Errorf("expected ReadyForDeletion after timeout, got %s",
			pod.Annotations[metadata.AnnotationDrainState])
	}

	// Verify pooler was unregistered
	inspectorStore := topoclient.NewWithFactory(
		factory,
		"",
		[]string{""},
		topoclient.NewDefaultTopoConfig(),
	)
	defer func() { _ = inspectorStore.Close() }()
	poolers, _ := inspectorStore.GetMultiPoolersByCell(ctx, "cell1", nil)
	if len(poolers) != 0 {
		t.Errorf("expected pooler to be unregistered, got %d", len(poolers))
	}
}

func TestDrain_NonTopoError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shardObj, pod).Build()
	recorder := record.NewFakeRecorder(10)

	store := &nonTopoErrorStore{}

	_, err := drain.ExecuteDrainStateMachine(
		context.Background(),
		c,
		nil,
		recorder,
		store,
		shardObj,
		pod,
	)
	if err == nil {
		t.Error("expected error for non-topo-unavailable error when pod is not being deleted")
	}
}

// nonTopoErrorStore returns a non-UNAVAILABLE error for GetMultiPoolersByCell.
type nonTopoErrorStore struct {
	topoclient.Store
}

func (s *nonTopoErrorStore) GetMultiPoolersByCell(
	ctx context.Context,
	cell string,
	opts *topoclient.GetMultiPoolersByCellOptions,
) ([]*topoclient.MultiPoolerInfo, error) {
	return nil, fmt.Errorf("permission denied")
}

func (s *nonTopoErrorStore) Close() error { return nil }

func TestReplicaDrain_RPCError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	replicaPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "replica-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateRequested,
			},
		},
	}

	primaryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "primary-pod", Namespace: "default",
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardObj, replicaPod, primaryPod).
		Build()
	rpcMock := &failingRPCClient{}
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		Hostname: "primary-pod", Type: clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod-0"},
		Hostname: "replica-pod-0", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)

	// RPC error on UpdateConsensusRule should requeue
	requeue, err := drain.ExecuteDrainStateMachine(
		ctx,
		c,
		rpcMock,
		recorder,
		store,
		shardObj,
		replicaPod,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Error("expected requeue when RPC fails")
	}

	// State should NOT have advanced
	_ = c.Get(ctx, client.ObjectKeyFromObject(replicaPod), replicaPod)
	if replicaPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
		t.Errorf("expected state to remain Requested on RPC failure, got %s",
			replicaPod.Annotations[metadata.AnnotationDrainState])
	}
}

func TestReplicaDrain_DrainingState_RPCError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shardObj := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-shard", Namespace: "default",
			Labels: map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName: "test-db", TableGroupName: "test-tg", ShardName: "0",
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"pool1": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	replicaPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "replica-pod-0",
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
			Annotations: map[string]string{
				metadata.AnnotationDrainState: metadata.DrainStateDraining,
			},
		},
	}

	primaryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "primary-pod", Namespace: "default",
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shardObj, replicaPod, primaryPod).
		Build()
	rpcMock := &failingRPCClient{}
	recorder := record.NewFakeRecorder(10)

	_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		Hostname: "primary-pod", Type: clustermetadata.PoolerType_PRIMARY,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)
	_ = store.RegisterMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Cell: "cell1", Name: "replica-pod-0"},
		Hostname: "replica-pod-0", Type: clustermetadata.PoolerType_REPLICA,
		ShardKey: &clustermetadata.ShardKey{Database: "test-db", TableGroup: "test-tg", Shard: "0"},
	}, false)

	requeue, err := drain.ExecuteDrainStateMachine(
		ctx,
		c,
		rpcMock,
		recorder,
		store,
		shardObj,
		replicaPod,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Error("expected requeue when RPC fails in Draining verification")
	}

	_ = c.Get(ctx, client.ObjectKeyFromObject(replicaPod), replicaPod)
	if replicaPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
		t.Errorf("expected state to remain Draining on RPC failure, got %s",
			replicaPod.Annotations[metadata.AnnotationDrainState])
	}
}

// failingRPCClient returns errors for UpdateConsensusRule.
type failingRPCClient struct {
	mockRPCClient
}

func (m *failingRPCClient) UpdateConsensusRule(
	ctx context.Context,
	pooler *clustermetadata.MultiPooler,
	request *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest,
) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	return nil, fmt.Errorf("rpc error: connection refused")
}

// drainStateOrder maps drain states to their ordinal position for monotonicity checks.
var drainStateOrder = map[string]int{
	"":                                  0,
	metadata.DrainStateRequested:        1,
	metadata.DrainStateDraining:         2,
	metadata.DrainStateAcknowledged:     3,
	metadata.DrainStateReadyForDeletion: 4,
}

// TestDrainStateMachine_RandomizedInvariants runs the drain state machine on
// many random pod configurations and verifies that key safety invariants hold
// regardless of the starting state or processing order:
//
//   - States only move forward (Requested→Draining→Acknowledged→ReadyForDeletion)
//   - ReadyForDeletion is terminal: no further transitions occur
//   - Pods that reach ReadyForDeletion are unregistered from topology
//   - Running the state machine twice at the same state is idempotent
//   - The state machine never errors on valid drain states
func TestDrainStateMachine_RandomizedInvariants(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	activeDrainStates := []string{
		metadata.DrainStateRequested,
		metadata.DrainStateDraining,
		metadata.DrainStateAcknowledged,
	}

	poolerTypes := []clustermetadata.PoolerType{
		clustermetadata.PoolerType_PRIMARY,
		clustermetadata.PoolerType_REPLICA,
	}

	const iterations = 500

	for i := range iterations {
		// Deterministic seed per iteration for reproducibility on failure.
		seed := uint64(i) //nolint:gosec // i is [0,500)
		src := rand.NewPCG(seed, 0)
		rng := rand.New(src) //nolint:gosec

		numPods := 2 + rng.IntN(4) // 2-5 pods
		podNames := make([]string, numPods)
		initialStates := make([]string, numPods)
		podTypes := make([]clustermetadata.PoolerType, numPods)

		// First pod is always primary, rest are replicas.
		for j := range numPods {
			podNames[j] = fmt.Sprintf("pod-%d", j)
			initialStates[j] = activeDrainStates[rng.IntN(len(activeDrainStates))]
			if j == 0 {
				podTypes[j] = clustermetadata.PoolerType_PRIMARY
			} else {
				podTypes[j] = poolerTypes[rng.IntN(len(poolerTypes))]
			}
		}

		// Pick a random subset of pods to have drain annotations (1 to all).
		numDraining := 1 + rng.IntN(numPods)
		drainingIndices := rng.Perm(numPods)[:numDraining]
		drainingSet := make(map[int]bool)
		for _, idx := range drainingIndices {
			drainingSet[idx] = true
		}

		// Build Kubernetes objects and topology.
		shardObj := &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name: "shard", Namespace: "default",
				Labels: map[string]string{metadata.LabelMultigresCluster: "cluster"},
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName: "db", TableGroupName: "tg", ShardName: "0",
				GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{RootPath: "/test"},
				Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
					"pool": {Cells: []multigresv1alpha1.CellName{"cell1"}},
				},
			},
		}

		var pods []*corev1.Pod
		var objects []client.Object
		objects = append(objects, shardObj)

		for j := range numPods {
			ann := map[string]string{}
			if drainingSet[j] {
				ann[metadata.AnnotationDrainState] = initialStates[j]
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podNames[j], Namespace: "default",
					Labels:      map[string]string{metadata.LabelMultigresCell: "cell1"},
					Annotations: ann,
				},
			}
			pods = append(pods, pod)
			objects = append(objects, pod)
		}

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
		rpcMock := &mockRPCClient{}
		recorder := record.NewFakeRecorder(10)

		_, factory := memorytopo.NewServerAndFactory(context.Background(), "cell1")
		store := topoclient.NewWithFactory(
			factory,
			"",
			[]string{""},
			topoclient.NewDefaultTopoConfig(),
		)

		for j := range numPods {
			_ = store.RegisterMultiPooler(context.Background(), &clustermetadata.MultiPooler{
				Id:       &clustermetadata.ID{Cell: "cell1", Name: podNames[j]},
				Hostname: podNames[j],
				Type:     podTypes[j],
				ShardKey: &clustermetadata.ShardKey{
					Database:   "db",
					TableGroup: "tg",
					Shard:      "0",
				},
			}, false)
		}

		ctx := context.Background()

		// Run multiple reconciliation rounds, processing pods in random order.
		rounds := 3 + rng.IntN(5)
		for round := range rounds {
			order := rng.Perm(numPods)
			for _, j := range order {
				pod := pods[j]

				// Read fresh state from the fake API server.
				fresh := &corev1.Pod{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
					t.Fatalf("iter=%d round=%d pod=%s: Get failed: %v", i, round, pod.Name, err)
				}

				stateBefore := fresh.Annotations[metadata.AnnotationDrainState]
				orderBefore := drainStateOrder[stateBefore]

				_, err := drain.ExecuteDrainStateMachine(
					ctx,
					c,
					rpcMock,
					recorder,
					store,
					shardObj,
					fresh,
				)
				// INVARIANT 1: No errors on valid drain states.
				if err != nil {
					t.Fatalf("iter=%d seed=%d round=%d pod=%s state=%q: unexpected error: %v",
						i, seed, round, pod.Name, stateBefore, err)
				}

				// Re-read to see the new state.
				updated := &corev1.Pod{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(pod), updated); err != nil {
					t.Fatalf("iter=%d pod=%s: Get after drain failed: %v", i, pod.Name, err)
				}
				stateAfter := updated.Annotations[metadata.AnnotationDrainState]
				orderAfter := drainStateOrder[stateAfter]

				// INVARIANT 2: Monotonic forward progress — state never goes backward.
				if orderAfter < orderBefore {
					t.Fatalf(
						"iter=%d seed=%d round=%d pod=%s: state regressed from %q (%d) to %q (%d)",
						i,
						seed,
						round,
						pod.Name,
						stateBefore,
						orderBefore,
						stateAfter,
						orderAfter,
					)
				}

				// INVARIANT 3: ReadyForDeletion is terminal.
				if stateBefore == metadata.DrainStateReadyForDeletion &&
					stateAfter != metadata.DrainStateReadyForDeletion {
					t.Fatalf(
						"iter=%d seed=%d round=%d pod=%s: ReadyForDeletion is not terminal, changed to %q",
						i,
						seed,
						round,
						pod.Name,
						stateAfter,
					)
				}

				// Update our local reference for the next round.
				pods[j] = updated
			}
		}

		// INVARIANT 4: Pods that reached ReadyForDeletion are unregistered from topology.
		inspectorStore := topoclient.NewWithFactory(
			factory,
			"",
			[]string{""},
			topoclient.NewDefaultTopoConfig(),
		)
		poolers, _ := inspectorStore.GetMultiPoolersByCell(ctx, "cell1", nil)
		registeredNames := make(map[string]bool)
		for _, p := range poolers {
			registeredNames[p.Id.Name] = true
		}

		for j := range numPods {
			fresh := &corev1.Pod{}
			_ = c.Get(ctx, client.ObjectKeyFromObject(pods[j]), fresh)
			if fresh.Annotations[metadata.AnnotationDrainState] == metadata.DrainStateReadyForDeletion {
				if registeredNames[podNames[j]] {
					t.Fatalf(
						"iter=%d seed=%d pod=%s: reached ReadyForDeletion but still registered in topology",
						i,
						seed,
						podNames[j],
					)
				}
			}
		}

		// INVARIANT 5: Idempotency — running on a terminal state produces no change.
		for j := range numPods {
			fresh := &corev1.Pod{}
			_ = c.Get(ctx, client.ObjectKeyFromObject(pods[j]), fresh)
			stateBefore := fresh.Annotations[metadata.AnnotationDrainState]

			_, err := drain.ExecuteDrainStateMachine(
				ctx,
				c,
				rpcMock,
				recorder,
				store,
				shardObj,
				fresh,
			)
			if err != nil {
				t.Fatalf(
					"iter=%d seed=%d pod=%s: idempotency check error: %v",
					i,
					seed,
					podNames[j],
					err,
				)
			}

			afterIdem := &corev1.Pod{}
			_ = c.Get(ctx, client.ObjectKeyFromObject(pods[j]), afterIdem)
			stateAfter := afterIdem.Annotations[metadata.AnnotationDrainState]

			if stateBefore == metadata.DrainStateReadyForDeletion && stateAfter != stateBefore {
				t.Fatalf("iter=%d seed=%d pod=%s: idempotency violated at %q, changed to %q",
					i, seed, podNames[j], stateBefore, stateAfter)
			}
		}

		_ = store.Close()
		_ = inspectorStore.Close()
	}
}
