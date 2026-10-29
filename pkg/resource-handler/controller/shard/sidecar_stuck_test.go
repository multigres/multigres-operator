package shard

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

var stuckEpoch = time.Unix(1_700_000_000, 0)

// stuckPod builds a pod through the production builder with the sidecar on,
// created long enough ago to be past the startup deadline, with the sidecar
// reporting it never started and the pod not Initialized.
func stuckPod(t *testing.T, shard *multigresv1alpha1.Shard, idx int) *corev1.Pod {
	t.Helper()
	pod, err := BuildPoolPod(shard, "main", "z1", newTestPoolSpec(), idx, testScheme())
	require.NoError(t, err)
	pod.Namespace = shard.Namespace
	pod.UID = types.UID("uid-" + pod.Name)
	pod.CreationTimestamp = metav1.NewTime(stuckEpoch.Add(-time.Hour))
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
		Name:         PgBackRestSidecarContainerName,
		Started:      ptr.To(false),
		RestartCount: 3,
		LastTerminationState: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 137},
		},
	}}
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodInitialized, Status: corev1.ConditionFalse},
	}
	return pod
}

func stuckShard() *multigresv1alpha1.Shard {
	s := sidecarTestShard()
	s.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
		"main": newTestPoolSpec(),
	}
	return s
}

// withNewSidecarImage is the shard after the sidecar image is fixed, so every
// pod built from the original shard is stale.
func withNewSidecarImage(shard *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
	fixed := shard.DeepCopy()
	fixed.Spec.Images.PgBackRestd = "registry/multigres@sha256:fixed"
	return fixed
}

func TestSidecarStuckPod(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(pods map[string]*corev1.Pod, shard *multigresv1alpha1.Shard) *multigresv1alpha1.Shard
		want   string
	}{
		{
			name: "never started, past deadline, stale",
			want: "0",
		},
		{
			name: "never started, before deadline",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				for _, p := range pods {
					p.CreationTimestamp = metav1.NewTime(stuckEpoch.Add(-419 * time.Second))
				}
				return s
			},
		},
		{
			name: "exactly at the deadline matches",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				for _, p := range pods {
					p.CreationTimestamp = metav1.NewTime(stuckEpoch.Add(-420 * time.Second))
				}
				return s
			},
			want: "0",
		},
		{
			name: "started once and now restarting",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				for _, p := range pods {
					p.Status.Conditions = []corev1.PodCondition{
						{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
					}
				}
				return s
			},
		},
		{
			name: "alive but failing its startup probe: SIGTERM exit 0, still matches",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				for _, p := range pods {
					p.Status.InitContainerStatuses[0].LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}
				}
				return s
			},
			want: "0",
		},
		{
			name: "sidecar running and started",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				for _, p := range pods {
					p.Status.InitContainerStatuses[0].Started = ptr.To(true)
				}
				return s
			},
		},
		{
			name: "no sidecar status yet",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				for _, p := range pods {
					p.Status.InitContainerStatuses = nil
				}
				return s
			},
		},
		{
			name: "nil Started counts as never",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				for _, p := range pods {
					p.Status.InitContainerStatuses[0].Started = nil
					p.Status.InitContainerStatuses[0].RestartCount = 0
					p.Status.InitContainerStatuses[0].LastTerminationState = corev1.ContainerState{}
				}
				return s
			},
			want: "0",
		},
		{
			name: "hash current",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				return stuckShard()
			},
		},
		{
			name: "another pod draining",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				other := healthyPeer(t, s, 2)
				other.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateRequested
				pods[other.Name] = other
				return s
			},
		},
		{
			name: "another pod being deleted",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				other := healthyPeer(t, s, 2)
				other.DeletionTimestamp = ptr.To(metav1.NewTime(stuckEpoch))
				other.Finalizers = []string{"test/hold"}
				pods[other.Name] = other
				return s
			},
		},
		{
			name: "the candidate itself is draining",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				for _, p := range pods {
					p.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateRequested
				}
				return s
			},
		},
		{
			name: "two candidates: lowest name only",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				second := stuckPod(t, s, 1)
				pods[second.Name] = second
				return s
			},
			want: "0",
		},
		{
			name: "unready but not stuck peer is ignored, stuck one matches",
			mutate: func(pods map[string]*corev1.Pod, s *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
				peer := stuckPod(t, s, 1)
				peer.Status.InitContainerStatuses[0].Started = ptr.To(true)
				peer.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				}
				pods[peer.Name] = peer
				return s
			},
			want: "0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sidecarShard := stuckShard()
			pod := stuckPod(t, sidecarShard, 0)
			pods := map[string]*corev1.Pod{pod.Name: pod}
			shard := withNewSidecarImage(sidecarShard)
			if tc.mutate != nil {
				shard = tc.mutate(pods, shard)
			}
			r := stuckReconciler(t, pods, nil)

			got := r.sidecarStuckPod(t.Context(), shard, pods)
			if tc.want == "" {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, BuildPoolPodName(shard, "main", "z1", 0), got.Name)
		})
	}
}

// A pod whose own status is unreadable, or whose sidecar health we cannot
// know, must not hold the escape closed: the matcher reads only the pod's
// status, so a list failure is the one input that can degrade, and it must
// return rather than block so the gated path carries on.
func TestSidecarStuckPodDegradedInputs(t *testing.T) {
	sidecarShard := stuckShard()
	shard := withNewSidecarImage(sidecarShard)
	pod := stuckPod(t, sidecarShard, 0)
	pods := map[string]*corev1.Pod{pod.Name: pod}

	t.Run("pod list fails: no match, no error, returns", func(t *testing.T) {
		r := stuckReconciler(t, pods, ptr.To(true))
		done := make(chan *corev1.Pod, 1)
		go func() { done <- r.sidecarStuckPod(t.Context(), shard, pods) }()
		select {
		case got := <-done:
			assert.Nil(t, got)
		case <-time.After(5 * time.Second):
			t.Fatal("sidecarStuckPod did not return")
		}
	})

	t.Run("pod list recovers on the next pass", func(t *testing.T) {
		fail := true
		r := stuckReconciler(t, pods, &fail)
		assert.Nil(t, r.sidecarStuckPod(t.Context(), shard, pods))
		fail = false
		assert.NotNil(t, r.sidecarStuckPod(t.Context(), shard, pods))
	})

	t.Run("status unreadable", func(t *testing.T) {
		blank := pod.DeepCopy()
		blank.Status = corev1.PodStatus{}
		r := stuckReconciler(t, pods, nil)
		assert.Nil(t, r.sidecarStuckPod(t.Context(), shard,
			map[string]*corev1.Pod{blank.Name: blank}))
	})

	t.Run("pooler and sidecar unhealthy does not matter", func(t *testing.T) {
		// No RPC clients, topology or posture are configured on the
		// reconciler; the escape must still fire.
		r := stuckReconciler(t, pods, nil)
		assert.Nil(t, r.PoolerClients)
		assert.NotNil(t, r.sidecarStuckPod(t.Context(), shard, pods))
	})

	t.Run("deadline is bounded by the pod age, not by any health signal", func(t *testing.T) {
		young := pod.DeepCopy()
		young.CreationTimestamp = metav1.NewTime(stuckEpoch)
		ps := map[string]*corev1.Pod{young.Name: young}
		r := stuckReconciler(t, ps, nil)
		assert.Nil(t, r.sidecarStuckPod(t.Context(), shard, ps))
		now := stuckEpoch.Add(420 * time.Second)
		r.Clock = func() time.Time { return now }
		assert.NotNil(t, r.sidecarStuckPod(t.Context(), shard, ps))
	})
}

// A pod can turn live between the cached read that nominated it and the
// uncached list the escape then makes; it must not be drained.
func TestSidecarStuckPodLiveInUncachedList(t *testing.T) {
	sidecarShard := stuckShard()
	shard := withNewSidecarImage(sidecarShard)
	cached := stuckPod(t, sidecarShard, 0)
	cached.UID = "uid-1"
	pods := map[string]*corev1.Pod{cached.Name: cached}

	for _, tc := range []struct {
		name string
		live func(*corev1.Pod)
		want bool
	}{
		{name: "unchanged", live: func(*corev1.Pod) {}, want: true},
		{name: "sidecar started", live: func(p *corev1.Pod) {
			p.Status.InitContainerStatuses[0].Started = ptr.To(true)
		}},
		{name: "pod became Initialized", live: func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
			}
		}},
		{name: "recreated under the same name", live: func(p *corev1.Pod) {
			p.UID = "uid-2"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := stuckReconciler(t, pods, nil)
			livePod := cached.DeepCopy()
			tc.live(livePod)
			r.APIReader = fake.NewClientBuilder().WithScheme(stuckScheme()).
				WithObjects(livePod).Build()
			got := r.sidecarStuckPod(t.Context(), shard, pods)
			if tc.want {
				require.NotNil(t, got)
				assert.Equal(t, cached.UID, got.UID)
				return
			}
			assert.Nil(t, got)
		})
	}
}

func healthyPeer(t *testing.T, shard *multigresv1alpha1.Shard, idx int) *corev1.Pod {
	t.Helper()
	pod := stuckPod(t, shard, idx)
	pod.Status.InitContainerStatuses[0].Started = ptr.To(true)
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}
	return pod
}

// stuckReconciler serves pods through both the cached and uncached readers.
// While *failList is true every uncached pod list fails.
func stuckReconciler(t *testing.T, pods map[string]*corev1.Pod, failList *bool) *ShardReconciler {
	t.Helper()
	var objs []client.Object
	for _, p := range pods {
		objs = append(objs, p.DeepCopy())
	}
	c := fake.NewClientBuilder().WithScheme(stuckScheme()).WithObjects(objs...).Build()
	var reader client.Reader = c
	if failList != nil {
		reader = fake.NewClientBuilder().WithScheme(stuckScheme()).WithObjects(objs...).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(
					ctx context.Context,
					cl client.WithWatch,
					list client.ObjectList,
					opts ...client.ListOption,
				) error {
					if *failList {
						return errors.New("list failed")
					}
					return cl.List(ctx, list, opts...)
				},
			}).Build()
	}
	return &ShardReconciler{
		Client:    c,
		APIReader: reader,
		Scheme:    stuckScheme(),
		Recorder:  record.NewFakeRecorder(10),
		Clock:     func() time.Time { return stuckEpoch },
	}
}

func stuckScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = multigresv1alpha1.AddToScheme(scheme)
	return scheme
}
