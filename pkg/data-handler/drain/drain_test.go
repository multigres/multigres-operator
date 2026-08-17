package drain_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/multigres/multigres/go/pb/clustermetadata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/drain"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

type mockTopoStore struct {
	topoclient.Store
	getMultipoolersByCellFunc func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error)
	unregisterMultipoolerFunc func(ctx context.Context, id *clustermetadata.ID) error
}

func (m *mockTopoStore) GetMultipoolersByCell(
	ctx context.Context,
	cellName string,
	opt *topoclient.GetMultipoolersByCellOptions,
) ([]*topoclient.MultipoolerInfo, error) {
	if m.getMultipoolersByCellFunc != nil {
		return m.getMultipoolersByCellFunc(ctx, cellName, opt)
	}
	return nil, nil
}

func (m *mockTopoStore) UnregisterMultipooler(ctx context.Context, id *clustermetadata.ID) error {
	if m.unregisterMultipoolerFunc != nil {
		return m.unregisterMultipoolerFunc(ctx, id)
	}
	return nil
}

type mockMultipoolerClient struct {
	rpcclient.MultipoolerClient
	UpdateConsensusRuleFunc func(ctx context.Context, pooler *clustermetadata.Multipooler, req *multipoolermanagerdata.UpdateConsensusRuleRequest) (*multipoolermanagerdata.UpdateConsensusRuleResponse, error)
	StatusFunc              func(ctx context.Context, pooler *clustermetadata.Multipooler, req *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error)
}

func (m *mockMultipoolerClient) Status(
	ctx context.Context,
	pooler *clustermetadata.Multipooler,
	req *multipoolermanagerdata.StatusRequest,
) (*multipoolermanagerdata.StatusResponse, error) {
	if m.StatusFunc != nil {
		return m.StatusFunc(ctx, pooler, req)
	}
	return nil, fmt.Errorf("unexpected Status call")
}

func (m *mockMultipoolerClient) UpdateConsensusRule(
	ctx context.Context,
	pooler *clustermetadata.Multipooler,
	req *multipoolermanagerdata.UpdateConsensusRuleRequest,
) (*multipoolermanagerdata.UpdateConsensusRuleResponse, error) {
	if m.UpdateConsensusRuleFunc != nil {
		return m.UpdateConsensusRuleFunc(ctx, pooler, req)
	}
	return nil, fmt.Errorf("unexpected UpdateConsensusRule call")
}

func TestReplicaDrainConsensusStatusGuards(t *testing.T) {
	t.Parallel()

	undecided := decidedStatusResponse()
	undecided.ConsensusStatus.CurrentPosition.Position.Proposal = &clustermetadata.ShardRule{
		RuleNumber: &clustermetadata.RuleNumber{
			CoordinatorTerm: 1,
			LeaderSubterm:   2,
		},
	}

	tests := []struct {
		name   string
		status func(context.Context, *clustermetadata.Multipooler, *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error)
	}{
		{
			name: "status RPC fails",
			status: func(context.Context, *clustermetadata.Multipooler, *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error) {
				return nil, fmt.Errorf("status unavailable")
			},
		},
		{
			name: "nil status response",
			status: func(context.Context, *clustermetadata.Multipooler, *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error) {
				return nil, nil
			},
		},
		{
			name: "missing consensus status",
			status: func(context.Context, *clustermetadata.Multipooler, *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error) {
				return &multipoolermanagerdata.StatusResponse{}, nil
			},
		},
		{
			name: "missing decided rule",
			status: func(context.Context, *clustermetadata.Multipooler, *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error) {
				return &multipoolermanagerdata.StatusResponse{
					ConsensusStatus: &clustermetadata.ConsensusStatus{
						CurrentPosition: &clustermetadata.PoolerPosition{
							Position: &clustermetadata.RulePosition{},
						},
					},
				}, nil
			},
		},
		{
			name: "undecided proposal",
			status: func(context.Context, *clustermetadata.Multipooler, *multipoolermanagerdata.StatusRequest) (*multipoolermanagerdata.StatusResponse, error) {
				return undecided, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("add Multigres scheme: %v", err)
			}
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("add core scheme: %v", err)
			}

			shard := &multigresv1alpha1.Shard{
				ObjectMeta: metav1.ObjectMeta{Name: "shard", Namespace: "default"},
				Spec: multigresv1alpha1.ShardSpec{
					Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
						"default": {Cells: []multigresv1alpha1.CellName{"cell1"}},
					},
				},
			}
			replicaPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "replica",
					Namespace: "default",
					Labels:    map[string]string{metadata.LabelMultigresCell: "cell1"},
					Annotations: map[string]string{
						metadata.AnnotationDrainState: metadata.DrainStateRequested,
					},
				},
			}
			primaryPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: "default"},
			}
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(replicaPod, primaryPod).
				Build()

			replicaInfo := &topoclient.MultipoolerInfo{Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Cell: "cell1", Name: "replica"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			}}
			primaryInfo := &topoclient.MultipoolerInfo{Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Cell: "cell1", Name: "primary"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			}}
			store := &mockTopoStore{
				getMultipoolersByCellFunc: func(context.Context, string, *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
					return []*topoclient.MultipoolerInfo{replicaInfo, primaryInfo}, nil
				},
			}

			updateCalled := false
			rpc := &mockMultipoolerClient{
				StatusFunc: tt.status,
				UpdateConsensusRuleFunc: func(context.Context, *clustermetadata.Multipooler, *multipoolermanagerdata.UpdateConsensusRuleRequest) (*multipoolermanagerdata.UpdateConsensusRuleResponse, error) {
					updateCalled = true
					return &multipoolermanagerdata.UpdateConsensusRuleResponse{}, nil
				},
			}

			requeue, err := drain.ExecuteDrainStateMachine(
				context.Background(),
				k8sClient,
				rpc,
				nil,
				store,
				shard,
				replicaPod,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !requeue {
				t.Fatal("expected drain to requeue")
			}
			if updateCalled {
				t.Fatal("UpdateConsensusRule must not be called without a decided rule")
			}

			updated := &corev1.Pod{}
			if err := k8sClient.Get(
				context.Background(),
				client.ObjectKeyFromObject(replicaPod),
				updated,
			); err != nil {
				t.Fatalf("read replica pod: %v", err)
			}
			if got := updated.Annotations[metadata.AnnotationDrainState]; got != metadata.DrainStateRequested {
				t.Fatalf("drain advanced on invalid consensus status: %q", got)
			}
		})
	}
}

func TestUpdateDrainState_NilAnnotations(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()

	requeue, err := drain.UpdateDrainState(
		context.Background(),
		c,
		pod,
		metadata.DrainStateDraining,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !requeue {
		t.Error("expected requeue")
	}
	if pod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateDraining {
		t.Errorf("expected state to be set, got %v", pod.Annotations)
	}
}

func TestIsPrimaryDraining(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
		},
	}

	t.Run("returns false for nil primary", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		if drain.IsPrimaryDraining(context.Background(), c, shard, nil) {
			t.Error("expected false for nil primary")
		}
	})

	t.Run("returns false for nil primary ID", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		primary := &clustermetadata.Multipooler{}
		if drain.IsPrimaryDraining(context.Background(), c, shard, primary) {
			t.Error("expected false for nil primary ID")
		}
	})

	t.Run("returns false when primary pod not found", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "missing-pod"},
		}
		if drain.IsPrimaryDraining(context.Background(), c, shard, primary) {
			t.Error("expected false when pod not found")
		}
	})

	t.Run("returns false when no drain annotation", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if drain.IsPrimaryDraining(context.Background(), c, shard, primary) {
			t.Error("expected false when no drain annotation")
		}
	})

	t.Run("returns true when drain annotation present", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if !drain.IsPrimaryDraining(context.Background(), c, shard, primary) {
			t.Error("expected true when drain annotation present")
		}
	})

	t.Run("returns false when drain state is ReadyForDeletion", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if drain.IsPrimaryDraining(context.Background(), c, shard, primary) {
			t.Error("expected false when drain state is ReadyForDeletion")
		}
	})

	t.Run("returns true on transient API error", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return fmt.Errorf("connection refused")
			},
		}).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if !drain.IsPrimaryDraining(context.Background(), c, shard, primary) {
			t.Error("expected true on transient error (assume draining to defer RPC)")
		}
	})
}

func TestIsPrimaryNotReady(t *testing.T) {
	t.Parallel()

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
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		if !drain.IsPrimaryNotReady(context.Background(), c, shard, nil) {
			t.Error("expected true for nil primary")
		}
	})

	t.Run("returns true for nil primary ID", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		primary := &clustermetadata.Multipooler{}
		if !drain.IsPrimaryNotReady(context.Background(), c, shard, primary) {
			t.Error("expected true for nil primary ID")
		}
	})

	t.Run("returns true when primary pod not found", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "missing-pod"},
		}
		if !drain.IsPrimaryNotReady(context.Background(), c, shard, primary) {
			t.Error("expected true when pod not found")
		}
	})

	t.Run("returns false when no ContainersReady condition", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if drain.IsPrimaryNotReady(context.Background(), c, shard, primary) {
			t.Error("expected false when no ContainersReady condition (assume ready)")
		}
	})

	t.Run("returns false when ContainersReady is True", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if drain.IsPrimaryNotReady(context.Background(), c, shard, primary) {
			t.Error("expected false when ContainersReady is True")
		}
	})

	t.Run("returns true when ContainersReady is False", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if !drain.IsPrimaryNotReady(context.Background(), c, shard, primary) {
			t.Error("expected true when ContainersReady is False")
		}
	})

	t.Run("returns true on transient API error", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return fmt.Errorf("connection refused")
			},
		}).Build()
		primary := &clustermetadata.Multipooler{
			Id: &clustermetadata.ID{Cell: "cell1", Name: "primary-pod"},
		}
		if !drain.IsPrimaryNotReady(context.Background(), c, shard, primary) {
			t.Error("expected true on transient error (fail-safe)")
		}
	})
}

func TestExecuteDrainStateMachine(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := multigresv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add to scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}

	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			Labels:    map[string]string{"multigres.com/cluster": "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {Cells: []multigresv1alpha1.CellName{"cell1"}},
			},
		},
	}

	t.Run("Malformed drain-requested-at annotation", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState:       metadata.DrainStateRequested,
					metadata.AnnotationDrainRequestedAt: "invalid-date",
				},
				Labels: map[string]string{metadata.LabelMultigresCell: "cell1"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		rpc := &mockMultipoolerClient{}
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, nil // No primary found
			},
		}
		recorder := record.NewFakeRecorder(10)

		_, _ = drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			rpc,
			recorder,
			store,
			shard,
			pod,
		)
		// It shouldn't panic, it catches the parsing error and uses time.Now()
	})

	t.Run("Force unregister error", func(t *testing.T) {
		t.Parallel()
		// Test the branch where ForceUnregisterPod fails during timeout
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateRequested,
				},
				Labels: map[string]string{metadata.LabelMultigresCell: "cell1"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		// Avoid fakeClient Builder failing due to deletion timestamp without finalizers
		pod.DeletionTimestamp = &metav1.Time{Time: time.Now().Add(-10 * time.Minute)}

		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, fmt.Errorf("fake topo list error")
			},
		}

		_, err := drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			nil,
			nil,
			store,
			shard,
			pod,
		)
		if err == nil {
			t.Errorf("expected force unregistration error")
		}
	})

	t.Run("Topology unavailable for drain retry", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateRequested,
				},
				Labels: map[string]string{metadata.LabelMultigresCell: "cell1"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, fmt.Errorf("fake UNAVAILABLE error")
			},
		}

		requeue, err := drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			nil,
			nil,
			store,
			shard,
			pod,
		)
		if err != nil {
			t.Errorf("expected nil error on topo unavailable, got %v", err)
		}
		if !requeue {
			t.Errorf("expected requeue=true")
		}
	})

	t.Run("FindPrimaryPooler error Requested state", func(t *testing.T) {
		t.Parallel()
		podInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Name: "test-pod"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateRequested,
				},
				Labels: map[string]string{
					metadata.LabelMultigresCell: "cell1",
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		callCount := 0
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				if callCount == 0 {
					callCount++
					return []*topoclient.MultipoolerInfo{podInfo}, nil
				}
				// Call inside FindPrimaryPooler fails
				return nil, fmt.Errorf("fake get primary error")
			},
		}

		requeue, err := drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			nil,
			nil,
			store,
			shard,
			pod,
		)
		if err != nil {
			t.Errorf("expected nil error when FindPrimary fails gracefully, got %v", err)
		}
		if !requeue {
			t.Errorf("expected requeue=true")
		}
	})

	t.Run("IsPrimaryNotReady in Requested state", func(t *testing.T) {
		t.Parallel()
		podInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Name: "test-pod"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			},
		}
		primaryInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Name: "primary-pod"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			},
		}
		primaryPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
				},
			},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateRequested,
				},
				Labels: map[string]string{
					metadata.LabelMultigresCell: "cell1",
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, primaryPod).Build()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{podInfo, primaryInfo}, nil
			},
		}
		rpc := &mockMultipoolerClient{}

		requeue, _ := drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			rpc,
			nil,
			store,
			shard,
			pod,
		)
		if !requeue {
			t.Errorf("expected requeue=true because primary is not ready")
		}
	})

	t.Run("FindPrimaryPooler error Draining state", func(t *testing.T) {
		t.Parallel()
		podInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Name: "test-pod"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				},
				Labels: map[string]string{
					metadata.LabelMultigresCell: "cell1",
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		callCount := 0
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				if callCount == 0 {
					callCount++
					return []*topoclient.MultipoolerInfo{podInfo}, nil
				}
				// Call inside FindPrimaryPooler fails
				return nil, fmt.Errorf("fake get primary error")
			},
		}

		requeue, err := drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			nil,
			nil,
			store,
			shard,
			pod,
		)
		if err != nil {
			t.Errorf("expected nil error when FindPrimary fails gracefully, got %v", err)
		}
		if !requeue {
			t.Errorf("expected requeue=true")
		}
	})

	t.Run("IsPrimaryNotReady in Draining state", func(t *testing.T) {
		t.Parallel()
		podInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Name: "test-pod"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			},
		}
		primaryInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Name: "primary-pod"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_PRIMARY),
			},
		}
		primaryPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "primary-pod",
				Namespace: "default",
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
				},
			},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				},
				Labels: map[string]string{
					metadata.LabelMultigresCell: "cell1",
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, primaryPod).Build()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{podInfo, primaryInfo}, nil
			},
		}
		rpc := &mockMultipoolerClient{}

		requeue, _ := drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			rpc,
			nil,
			store,
			shard,
			pod,
		)
		if !requeue {
			t.Errorf("expected requeue=true because primary is not ready in Draining state")
		}
	})

	t.Run("Error ForceUnregister in Acknowledged state", func(t *testing.T) {
		t.Parallel()
		podInfo := &topoclient.MultipoolerInfo{
			Multipooler: &clustermetadata.Multipooler{
				Id:           &clustermetadata.ID{Name: "test-pod"},
				RoutingState: routingState(clustermetadata.RoutingRole_ROUTING_ROLE_REPLICA),
			},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateAcknowledged,
				},
				Labels: map[string]string{
					metadata.LabelMultigresCell: "cell1",
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return []*topoclient.MultipoolerInfo{podInfo}, nil
			},
			unregisterMultipoolerFunc: func(ctx context.Context, id *clustermetadata.ID) error {
				return fmt.Errorf("fake unregister error")
			},
		}

		_, err := drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			nil,
			nil,
			store,
			shard,
			pod,
		)
		if err == nil {
			t.Errorf("expected force unregister error in Acknowledged state")
		}
	})

	t.Run("Unknown or unrecognized state", func(t *testing.T) {
		t.Parallel()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
				Annotations: map[string]string{
					metadata.AnnotationDrainState: "UnknownState",
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		store := &mockTopoStore{
			getMultipoolersByCellFunc: func(ctx context.Context, cellName string, opt *topoclient.GetMultipoolersByCellOptions) ([]*topoclient.MultipoolerInfo, error) {
				return nil, nil // No pooler
			},
		}

		requeue, err := drain.ExecuteDrainStateMachine(
			context.Background(),
			c,
			nil,
			nil,
			store,
			shard,
			pod,
		)
		if requeue || err != nil {
			t.Errorf("expected false, nil for unknown state")
		}
	})
}
