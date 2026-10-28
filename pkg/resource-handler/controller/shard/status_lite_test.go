package shard

import (
	"errors"
	"testing"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func liteStatusTestShard() *multigresv1alpha1.Shard {
	shard := postureTestShard()
	shard.Generation = 3
	shard.Spec.Mode = multigresv1alpha1.ModeLite
	return shard
}

// liteStatusTestPod is the Lite pod as BuildLitePod builds it, running with an
// IP and the given readiness.
func liteStatusTestPod(t *testing.T, shard *multigresv1alpha1.Shard, ready bool) *corev1.Pod {
	t.Helper()
	pod, err := BuildLitePod(
		shard,
		"default",
		"cell1",
		shard.Spec.Pools["default"],
		postureTestScheme(t),
	)
	if err != nil {
		t.Fatalf("BuildLitePod() error = %v", err)
	}
	readyStatus := corev1.ConditionFalse
	if ready {
		readyStatus = corev1.ConditionTrue
	}
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		PodIP: "10.0.0.7",
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: readyStatus},
		},
	}
	return pod
}

// litePoolerID is the topology ID the Lite pooler registers under, which the
// fake RPC client keys its responses by.
func litePoolerID(shard *multigresv1alpha1.Shard) topoclient.ComponentID {
	return topoclient.ComponentIDString(&clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      BuildPoolServiceID(BuildPoolPodName(shard, "default", "cell1", 0)),
	})
}

func liteStatusResponse(role clustermetadata.PoolerType) *multipoolermanagerdatapb.StatusResponse {
	return &multipoolermanagerdatapb.StatusResponse{
		Status: &multipoolermanagerdatapb.Status{PoolerType: role},
	}
}

func TestUpdateLiteStatus(t *testing.T) {
	type want struct {
		phase          multigresv1alpha1.Phase
		readyReplicas  int32
		available      metav1.ConditionStatus
		primary        metav1.ConditionStatus
		primaryReason  string
		upToDate       metav1.ConditionStatus
		upToDateReason string
	}

	tests := map[string]struct {
		pod  func(*testing.T, *multigresv1alpha1.Shard) *corev1.Pod
		rpc  func(*multigresv1alpha1.Shard) *rpcclient.FakeClient
		want want
	}{
		"ready primary on the current spec": {
			pod: func(t *testing.T, s *multigresv1alpha1.Shard) *corev1.Pod {
				return liteStatusTestPod(t, s, true)
			},
			rpc: func(s *multigresv1alpha1.Shard) *rpcclient.FakeClient {
				rpc := rpcclient.NewFakeClient()
				rpc.SetStatusResponse(
					litePoolerID(s),
					liteStatusResponse(clustermetadata.PoolerType_PRIMARY),
				)
				return rpc
			},
			want: want{
				phase:          multigresv1alpha1.PhaseHealthy,
				readyReplicas:  1,
				available:      metav1.ConditionTrue,
				primary:        metav1.ConditionTrue,
				primaryReason:  "PoolerPrimary",
				upToDate:       metav1.ConditionTrue,
				upToDateReason: "SpecApplied",
			},
		},
		"ready pod whose pooler is not primary": {
			pod: func(t *testing.T, s *multigresv1alpha1.Shard) *corev1.Pod {
				return liteStatusTestPod(t, s, true)
			},
			rpc: func(s *multigresv1alpha1.Shard) *rpcclient.FakeClient {
				rpc := rpcclient.NewFakeClient()
				rpc.SetStatusResponse(
					litePoolerID(s),
					liteStatusResponse(clustermetadata.PoolerType_REPLICA),
				)
				return rpc
			},
			want: want{
				phase:          multigresv1alpha1.PhaseProgressing,
				readyReplicas:  1,
				available:      metav1.ConditionTrue,
				primary:        metav1.ConditionFalse,
				primaryReason:  "NotPrimary",
				upToDate:       metav1.ConditionTrue,
				upToDateReason: "SpecApplied",
			},
		},
		"unreachable pooler is not primary even when it was ready": {
			pod: func(t *testing.T, s *multigresv1alpha1.Shard) *corev1.Pod {
				return liteStatusTestPod(t, s, true)
			},
			rpc: func(s *multigresv1alpha1.Shard) *rpcclient.FakeClient {
				rpc := rpcclient.NewFakeClient()
				rpc.Errors[litePoolerID(s)] = errors.New("connection refused")
				return rpc
			},
			want: want{
				phase:          multigresv1alpha1.PhaseProgressing,
				readyReplicas:  1,
				available:      metav1.ConditionTrue,
				primary:        metav1.ConditionFalse,
				primaryReason:  "PoolerUnreachable",
				upToDate:       metav1.ConditionTrue,
				upToDateReason: "SpecApplied",
			},
		},
		"primary that is not ready yet": {
			pod: func(t *testing.T, s *multigresv1alpha1.Shard) *corev1.Pod {
				return liteStatusTestPod(t, s, false)
			},
			rpc: func(s *multigresv1alpha1.Shard) *rpcclient.FakeClient {
				rpc := rpcclient.NewFakeClient()
				rpc.SetStatusResponse(
					litePoolerID(s),
					liteStatusResponse(clustermetadata.PoolerType_PRIMARY),
				)
				return rpc
			},
			want: want{
				phase:          multigresv1alpha1.PhaseProgressing,
				readyReplicas:  0,
				available:      metav1.ConditionFalse,
				primary:        metav1.ConditionTrue,
				primaryReason:  "PoolerPrimary",
				upToDate:       metav1.ConditionTrue,
				upToDateReason: "SpecApplied",
			},
		},
		"pod built from an older spec": {
			pod: func(t *testing.T, s *multigresv1alpha1.Shard) *corev1.Pod {
				pod := liteStatusTestPod(t, s, true)
				pod.Annotations[metadata.AnnotationSpecHash] = "stale"
				return pod
			},
			rpc: func(s *multigresv1alpha1.Shard) *rpcclient.FakeClient {
				rpc := rpcclient.NewFakeClient()
				rpc.SetStatusResponse(
					litePoolerID(s),
					liteStatusResponse(clustermetadata.PoolerType_PRIMARY),
				)
				return rpc
			},
			want: want{
				phase:          multigresv1alpha1.PhaseHealthy,
				readyReplicas:  1,
				available:      metav1.ConditionTrue,
				primary:        metav1.ConditionTrue,
				primaryReason:  "PoolerPrimary",
				upToDate:       metav1.ConditionFalse,
				upToDateReason: "SpecChanged",
			},
		},
		"no pod": {
			rpc: func(*multigresv1alpha1.Shard) *rpcclient.FakeClient {
				return rpcclient.NewFakeClient()
			},
			want: want{
				phase:          multigresv1alpha1.PhaseProgressing,
				readyReplicas:  0,
				available:      metav1.ConditionFalse,
				primary:        metav1.ConditionFalse,
				primaryReason:  "PodUnavailable",
				upToDate:       metav1.ConditionFalse,
				upToDateReason: "PodMissing",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			shard := liteStatusTestShard()
			var objects []client.Object
			if tc.pod != nil {
				objects = append(objects, tc.pod(t, shard))
			}
			r, c := postureTestReconciler(t, shard, tc.rpc(shard), objects...)

			if err := r.updateStatus(t.Context(), shard, renderedConfig{}); err != nil {
				t.Fatalf("updateStatus() error = %v", err)
			}

			got := &multigresv1alpha1.Shard{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), got); err != nil {
				t.Fatalf("get shard: %v", err)
			}
			if got.Status.Phase != tc.want.phase {
				t.Errorf("phase = %q, want %q", got.Status.Phase, tc.want.phase)
			}
			if got.Status.ReadyReplicas != tc.want.readyReplicas ||
				got.Status.PoolsReady != (tc.want.readyReplicas == 1) {
				t.Errorf("readyReplicas/poolsReady = %d/%v, want %d of 1",
					got.Status.ReadyReplicas, got.Status.PoolsReady, tc.want.readyReplicas)
			}
			if got.Status.ObservedGeneration != shard.Generation {
				t.Errorf("observedGeneration = %d, want %d",
					got.Status.ObservedGeneration, shard.Generation)
			}

			for _, check := range []struct {
				condType string
				status   metav1.ConditionStatus
				reason   string
			}{
				{condType: "Available", status: tc.want.available},
				{
					condType: multigresv1alpha1.ConditionPrimary,
					status:   tc.want.primary,
					reason:   tc.want.primaryReason,
				},
				{
					condType: multigresv1alpha1.ConditionUpToDate,
					status:   tc.want.upToDate,
					reason:   tc.want.upToDateReason,
				},
			} {
				cond := meta.FindStatusCondition(got.Status.Conditions, check.condType)
				if cond == nil {
					t.Errorf("condition %s missing", check.condType)
					continue
				}
				if cond.Status != check.status {
					t.Errorf("%s = %s, want %s (%s: %s)",
						check.condType, cond.Status, check.status, cond.Reason, cond.Message)
				}
				if check.reason != "" && cond.Reason != check.reason {
					t.Errorf("%s reason = %q, want %q", check.condType, cond.Reason, check.reason)
				}
				if cond.ObservedGeneration != shard.Generation {
					t.Errorf("%s observedGeneration = %d, want %d",
						check.condType, cond.ObservedGeneration, shard.Generation)
				}
			}
		})
	}
}
