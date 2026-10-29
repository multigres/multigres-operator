package shard

import (
	"context"
	"errors"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

const (
	topoMovePool = "pool-1"
	topoMoveCell = "zone1"
)

var (
	oldGlobalTopo = multigresv1alpha1.GlobalTopoServerRef{
		Address:        "old-topo.default.svc:2379",
		RootPath:       "/multigres/global",
		Implementation: "etcd",
	}
	newGlobalTopo = multigresv1alpha1.GlobalTopoServerRef{
		Address:        "https://shared-topo.shared.svc:2379",
		RootPath:       "/multigres/project/global",
		Implementation: "etcd",
	}
)

func topoMoveScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func topoMovePoolSpec() multigresv1alpha1.PoolSpec {
	return multigresv1alpha1.PoolSpec{
		ReplicasPerCell: ptr.To(int32(3)),
		Cells:           []multigresv1alpha1.CellName{topoMoveCell},
		Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
	}
}

func topoMoveShard(topo multigresv1alpha1.GlobalTopoServerRef) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       "shard-uid",
			Labels:    map[string]string{metadata.LabelMultigresCluster: "test-cluster"},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "db",
			TableGroupName:   "tg",
			ShardName:        "s1",
			GlobalTopoServer: topo,
			Images: multigresv1alpha1.ShardImages{
				PgBackRestd: "registry/multigres@sha256:abc",
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				topoMovePool: topoMovePoolSpec(),
			},
		},
	}
}

// buildTopoMovePod builds pool pod index as the operator would have for a
// shard on topo, with its pooler readiness gate, and so PodReady, set to
// ready.
func buildTopoMovePod(
	t *testing.T,
	topo multigresv1alpha1.GlobalTopoServerRef,
	index int,
	ready bool,
) *corev1.Pod {
	t.Helper()
	pod, err := BuildPoolPod(
		topoMoveShard(
			topo,
		),
		topoMovePool,
		topoMoveCell,
		topoMovePoolSpec(),
		index,
		topoMoveScheme(),
	)
	if err != nil {
		t.Fatalf("BuildPoolPod: %v", err)
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: PoolerDataReadyCondition, Status: status},
		{Type: corev1.PodReady, Status: status},
	}
	return pod
}

func TestPodTopoDrifted(t *testing.T) {
	shard := topoMoveShard(newGlobalTopo)
	movedRoot := newGlobalTopo
	movedRoot.RootPath = "/multigres/elsewhere"

	tests := []struct {
		name string
		pod  func(t *testing.T) *corev1.Pod
		want bool
	}{
		{
			name: "not ready on the old address",
			pod:  func(t *testing.T) *corev1.Pod { return buildTopoMovePod(t, oldGlobalTopo, 0, false) },
			want: true,
		},
		{
			name: "not ready on the same address with a different root",
			pod:  func(t *testing.T) *corev1.Pod { return buildTopoMovePod(t, movedRoot, 0, false) },
			want: true,
		},
		{
			name: "ready on the old address",
			pod:  func(t *testing.T) *corev1.Pod { return buildTopoMovePod(t, oldGlobalTopo, 0, true) },
			want: false,
		},
		{
			name: "observed on the old address with containers not ready",
			pod: func(t *testing.T) *corev1.Pod {
				pod := buildTopoMovePod(t, oldGlobalTopo, 0, true)
				pod.Status.Conditions[1].Status = corev1.ConditionFalse
				return pod
			},
			want: false,
		},
		{
			name: "not ready on the current topology",
			pod:  func(t *testing.T) *corev1.Pod { return buildTopoMovePod(t, newGlobalTopo, 0, false) },
			want: false,
		},
		{
			name: "already draining",
			pod: func(t *testing.T) *corev1.Pod {
				pod := buildTopoMovePod(t, oldGlobalTopo, 0, false)
				pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateDraining
				return pod
			},
			want: false,
		},
		{
			name: "terminating",
			pod: func(t *testing.T) *corev1.Pod {
				pod := buildTopoMovePod(t, oldGlobalTopo, 0, false)
				pod.DeletionTimestamp = ptr.To(metav1.Now())
				return pod
			},
			want: false,
		},
		{
			name: "multipooler without topology flags",
			pod: func(t *testing.T) *corev1.Pod {
				pod := buildTopoMovePod(t, oldGlobalTopo, 0, false)
				for i := range pod.Spec.Containers {
					if pod.Spec.Containers[i].Name == multipoolerContainerName {
						pod.Spec.Containers[i].Args = []string{"multipooler"}
					}
				}
				return pod
			},
			want: false,
		},
		{
			name: "no multipooler container",
			pod: func(t *testing.T) *corev1.Pod {
				pod := buildTopoMovePod(t, oldGlobalTopo, 0, false)
				pod.Spec.Containers = nil
				return pod
			},
			want: false,
		},
		{
			name: "nil pod",
			pod:  func(*testing.T) *corev1.Pod { return nil },
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podTopoDrifted(tt.pod(t), shard); got != tt.want {
				t.Errorf("podTopoDrifted() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestHandleRollingUpdates_TopoMove covers a global-topology move on a live
// shard: every pool pod still points at the old topology, so none is Ready and
// isShardHealthy is false, which used to block the rollout for good.
func TestHandleRollingUpdates_TopoMove(t *testing.T) {
	ctx := context.Background()
	scheme := topoMoveScheme()

	type podState struct {
		topo  multigresv1alpha1.GlobalTopoServerRef
		ready bool
	}
	stranded := podState{topo: oldGlobalTopo}
	allStranded := []podState{stranded, stranded, stranded}

	drainStates := func(t *testing.T, c client.Client, pods map[string]*corev1.Pod) map[string]string {
		t.Helper()
		states := map[string]string{}
		for name, pod := range pods {
			got := &corev1.Pod{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(pod), got); err != nil {
				t.Fatalf("get pod %s: %v", name, err)
			}
			states[name] = got.Annotations[metadata.AnnotationDrainState]
		}
		return states
	}

	// setup builds the reconciler over a shard on newGlobalTopo and an
	// in-memory store standing in for that topology, empty unless a test
	// registers poolers in it.
	setup := func(t *testing.T, states []podState) (
		*ShardReconciler, client.Client, *multigresv1alpha1.Shard, map[string]*corev1.Pod,
		topoclient.Store,
	) {
		t.Helper()
		shard := topoMoveShard(newGlobalTopo)
		objs := []client.Object{shard}
		pods := map[string]*corev1.Pod{}
		for i, st := range states {
			pod := buildTopoMovePod(t, st.topo, i, st.ready)
			objs = append(objs, pod)
			pods[pod.Name] = pod
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		store, factory := memorytopo.NewServerAndFactory(ctx, topoMoveCell)
		t.Cleanup(func() { _ = store.Close() })
		r := &ShardReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: record.NewFakeRecorder(10),
			CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
				return topoclient.NewWithFactory(
					factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
				), nil
			},
		}
		return r, c, shard, pods, store
	}

	register := func(t *testing.T, store topoclient.Store, podName string, shutdown bool) {
		t.Helper()
		mp := &clustermetadata.Multipooler{
			Id:       &clustermetadata.ID{Cell: topoMoveCell, Name: podName},
			Hostname: podName,
			ShardRef: &clustermetadata.ShardRef{Database: "db", TableGroup: "tg", Shard: "s1"},
		}
		if shutdown {
			mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
				Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
			}
		}
		if err := store.RegisterMultipooler(ctx, mp, true); err != nil {
			t.Fatalf("register pooler %s: %v", podName, err)
		}
	}

	rollAndCollect := func(
		t *testing.T, r *ShardReconciler, c client.Client, shard *multigresv1alpha1.Shard,
		pods map[string]*corev1.Pod,
	) map[string]string {
		t.Helper()
		if err := r.handleRollingUpdates(
			ctx, shard, topoMovePool, topoMoveCell, topoMovePoolSpec(),
			pods, len(pods), false, false, &shardRolloutTracker{},
		); err != nil {
			t.Fatalf("handleRollingUpdates: %v", err)
		}
		return drainStates(t, c, pods)
	}

	t.Run("drains one stranded pod although the shard is unhealthy", func(t *testing.T) {
		r, c, shard, pods, _ := setup(t, allStranded)
		if healthy, err := r.isShardHealthy(ctx, shard); err != nil || healthy {
			t.Fatalf("precondition: isShardHealthy = %v, %v; want false, nil", healthy, err)
		}

		rollout := &shardRolloutTracker{}
		if err := r.handleRollingUpdates(
			ctx, shard, topoMovePool, topoMoveCell, topoMovePoolSpec(),
			pods, len(pods), false, false, rollout,
		); err != nil {
			t.Fatalf("handleRollingUpdates: %v", err)
		}

		first := BuildPoolPodName(shard, topoMovePool, topoMoveCell, 0)
		for name, state := range drainStates(t, c, pods) {
			want := ""
			if name == first {
				want = metadata.DrainStateRequested
			}
			if state != want {
				t.Errorf("pod %s drain state = %q, want %q", name, state, want)
			}
		}
		if !rollout.HasStarted() {
			t.Error("rollout tracker should be marked started")
		}
	})

	t.Run("a ready pod on the old topology still waits for shard health", func(t *testing.T) {
		// The unready pod on the current topology makes the shard unhealthy
		// without being stranded itself.
		r, c, shard, pods, _ := setup(t, []podState{
			{topo: newGlobalTopo},
			{topo: oldGlobalTopo, ready: true},
			{topo: oldGlobalTopo, ready: true},
		})

		if err := r.handleRollingUpdates(
			ctx, shard, topoMovePool, topoMoveCell, topoMovePoolSpec(),
			pods, len(pods)-1, false, false, &shardRolloutTracker{},
		); err != nil {
			t.Fatalf("handleRollingUpdates: %v", err)
		}
		for name, state := range drainStates(t, c, pods) {
			if state != "" {
				t.Errorf("pod %s drain state = %q, want none while the shard is unhealthy",
					name, state)
			}
		}
	})

	t.Run("a drain already in progress blocks the next stranded pod", func(t *testing.T) {
		r, c, shard, pods, _ := setup(t, allStranded)
		if err := r.handleRollingUpdates(
			ctx, shard, topoMovePool, topoMoveCell, topoMovePoolSpec(),
			pods, len(pods), false, true, &shardRolloutTracker{},
		); err != nil {
			t.Fatalf("handleRollingUpdates: %v", err)
		}
		for name, state := range drainStates(t, c, pods) {
			if state != "" {
				t.Errorf("pod %s drain state = %q, want none", name, state)
			}
		}
	})

	t.Run("another pool's drain this pass blocks a stranded pod", func(t *testing.T) {
		r, c, shard, pods, _ := setup(t, allStranded)
		rollout := &shardRolloutTracker{}
		rollout.SetStarted()
		if err := r.handleRollingUpdates(
			ctx, shard, topoMovePool, topoMoveCell, topoMovePoolSpec(),
			pods, len(pods), false, false, rollout,
		); err != nil {
			t.Fatalf("handleRollingUpdates: %v", err)
		}
		for name, state := range drainStates(t, c, pods) {
			if state != "" {
				t.Errorf("pod %s drain state = %q, want none", name, state)
			}
		}
	})

	t.Run("a drifted pod still registered in the shard's topology keeps the gated path",
		func(t *testing.T) {
			// An address change that still reaches the same topology, with the
			// readiness gate False for an unrelated reason (the operator could
			// not load its RPC credentials): every pooler is registered where
			// the shard looks, so nothing is stranded and nothing may bypass
			// the gates.
			r, c, shard, pods, store := setup(t, allStranded)
			for name := range pods {
				register(t, store, name, false)
			}
			for name, state := range rollAndCollect(t, r, c, shard, pods) {
				if state != "" {
					t.Errorf("pod %s drain state = %q, want none", name, state)
				}
			}
		})

	t.Run("only the drifted pods missing from the shard's topology are stranded",
		func(t *testing.T) {
			r, c, shard, pods, store := setup(t, allStranded)
			first := BuildPoolPodName(shard, topoMovePool, topoMoveCell, 0)
			second := BuildPoolPodName(shard, topoMovePool, topoMoveCell, 1)
			third := BuildPoolPodName(shard, topoMovePool, topoMoveCell, 2)
			register(t, store, first, false)
			// A shut-down registration is a pooler that has left, not one
			// that is there.
			register(t, store, second, true)
			register(t, store, third, false)
			for name, state := range rollAndCollect(t, r, c, shard, pods) {
				want := ""
				if name == second {
					want = metadata.DrainStateRequested
				}
				if state != want {
					t.Errorf("pod %s drain state = %q, want %q", name, state, want)
				}
			}
		})

	t.Run("an unreadable topology keeps the gated path", func(t *testing.T) {
		r, c, shard, pods, _ := setup(t, allStranded)
		r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return nil, errors.New("topology unavailable")
		}
		for name, state := range rollAndCollect(t, r, c, shard, pods) {
			if state != "" {
				t.Errorf("pod %s drain state = %q, want none", name, state)
			}
		}
	})

	t.Run("a drain elsewhere in the shard holds back a stranded pod", func(t *testing.T) {
		// Another pool/cell of the same shard is mid-drain from an earlier
		// pass. This pool's own isAnyPodDraining cannot see it and the
		// rollout tracker is fresh, so only a shard-wide check stops a
		// second drain.
		r, c, shard, pods, _ := setup(t, allStranded)
		other, err := BuildPoolPod(
			topoMoveShard(oldGlobalTopo), "pool-2", topoMoveCell, topoMovePoolSpec(), 0,
			topoMoveScheme(),
		)
		if err != nil {
			t.Fatalf("BuildPoolPod: %v", err)
		}
		other.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateDraining
		if err := c.Create(ctx, other); err != nil {
			t.Fatalf("create draining pod: %v", err)
		}
		if got, err := r.listDisruptionPods(
			ctx,
			shard,
		); err != nil ||
			len(got.Items) != len(pods)+1 {
			t.Fatalf("precondition: the shard-wide list should see the other pool's pod: %v, %v",
				got, err)
		}
		for name, state := range rollAndCollect(t, r, c, shard, pods) {
			if state != "" {
				t.Errorf("pod %s drain state = %q, want none while another pool is draining",
					name, state)
			}
		}
	})
}
