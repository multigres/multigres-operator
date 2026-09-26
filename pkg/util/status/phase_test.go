package status

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"

	"github.com/multigres/testkit/assert"
)

func TestComputePhase(t *testing.T) {
	tests := []struct {
		name  string
		ready int32
		total int32
		want  multigresv1alpha1.Phase
	}{
		{
			name:  "Zero Total -> Initializing",
			ready: 0,
			total: 0,
			want:  multigresv1alpha1.PhaseInitializing,
		},
		{
			name:  "Ready Equal Total -> Healthy",
			ready: 3,
			total: 3,
			want:  multigresv1alpha1.PhaseHealthy,
		},
		{
			name:  "Ready Less Than Total -> Progressing",
			ready: 2,
			total: 3,
			want:  multigresv1alpha1.PhaseProgressing,
		},
		{
			name:  "Ready Zero (with Positive Total) -> Progressing",
			ready: 0,
			total: 3,
			want:  multigresv1alpha1.PhaseProgressing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NewCollecting(t).Eq(tt.want, ComputePhase(tt.ready, tt.total), "ComputePhase()")
		})
	}
}

func TestIsCrashLooping(t *testing.T) {
	tests := []struct {
		name string
		pod  corev1.Pod
		want bool
	}{
		{
			name: "CrashLoopBackOff",
			pod: corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
					},
				},
			}}},
			want: true,
		},
		{
			name: "OOMKilled",
			pod: corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "OOMKilled"},
					},
				},
			}}},
			want: true,
		},
		{
			name: "Running",
			pod: corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			}}},
			want: false,
		},
		{
			name: "ImagePullBackOff",
			pod: corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"},
					},
				},
			}}},
			want: true,
		},
		{
			name: "ErrImagePull",
			pod: corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull"},
					},
				},
			}}},
			want: false,
		},
		{
			name: "NoContainerStatuses",
			pod:  corev1.Pod{},
			want: false,
		},
		{
			name: "TerminatedWithHighRestarts_NotReady",
			pod: corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{
					Ready:        false,
					RestartCount: 5,
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"},
					},
				},
			}}},
			want: true,
		},
		{
			name: "RunningWithHighRestarts_NotFlagged",
			pod: corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{
					Ready:        false,
					RestartCount: 10,
					State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				},
			}}},
			want: false,
		},
		{
			name: "TerminatedWithLowRestarts_NotFlagged",
			pod: corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{
					RestartCount: 2,
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"},
					},
				},
			}}},
			want: false,
		},
		{
			name: "InitContainer_CrashLoopBackOff",
			pod: corev1.Pod{
				Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
						},
					},
				}},
			},
			want: true,
		},
		{
			name: "InitContainer_TerminatedHighRestarts",
			pod: corev1.Pod{
				Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{
					{
						RestartCount: 5,
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{Reason: "Error"},
						},
					},
				}},
			},
			want: true,
		},
		{
			name: "InitContainer_Running_NotFlagged",
			pod: corev1.Pod{
				Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{
					{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				}},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NewCollecting(t).Eq(tt.want, IsCrashLooping(&tt.pod), "IsCrashLooping()")
		})
	}
}

func TestAnyCrashLooping(t *testing.T) {
	crashPod := corev1.Pod{
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
			},
		}},
	}
	healthyPod := corev1.Pod{
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		}},
	}
	now := metav1.Now()
	terminatingCrashPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
			},
		}},
	}

	tests := []struct {
		name string
		pods []corev1.Pod
		want bool
	}{
		{name: "EmptyList", pods: nil, want: false},
		{name: "AllHealthy", pods: []corev1.Pod{healthyPod, healthyPod}, want: false},
		{name: "OneCrashing", pods: []corev1.Pod{healthyPod, crashPod}, want: true},
		{name: "TerminatingCrashSkipped", pods: []corev1.Pod{terminatingCrashPod}, want: false},
		{
			name: "TerminatingPlusLiveCrash",
			pods: []corev1.Pod{terminatingCrashPod, crashPod},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NewCollecting(t).Eq(tt.want, AnyCrashLooping(tt.pods), "AnyCrashLooping()")
		})
	}
}

func TestComputeWorkloadPhase(t *testing.T) {
	crashPod := corev1.Pod{
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
			},
		}},
	}
	healthyPod := corev1.Pod{
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		}},
	}

	tests := []struct {
		name      string
		input     WorkloadPhaseInput
		wantPhase multigresv1alpha1.Phase
	}{
		{
			name: "Healthy",
			input: WorkloadPhaseInput{
				Ready: 3, Total: 3,
				GenerationCurrent: 1, GenerationObserved: 1,
				Pods: []corev1.Pod{healthyPod}, ComponentName: "Gateway",
			},
			wantPhase: multigresv1alpha1.PhaseHealthy,
		},
		{
			name: "Progressing_PartialReady",
			input: WorkloadPhaseInput{
				Ready: 1, Total: 3,
				GenerationCurrent: 1, GenerationObserved: 1,
				Pods: []corev1.Pod{healthyPod}, ComponentName: "Gateway",
			},
			wantPhase: multigresv1alpha1.PhaseProgressing,
		},
		{
			name: "Progressing_GenerationMismatch",
			input: WorkloadPhaseInput{
				Ready: 3, Total: 3,
				GenerationCurrent: 2, GenerationObserved: 1,
				Pods: []corev1.Pod{healthyPod}, ComponentName: "Gateway",
			},
			wantPhase: multigresv1alpha1.PhaseProgressing,
		},
		{
			name: "Degraded_CrashLoop",
			input: WorkloadPhaseInput{
				Ready: 0, Total: 3,
				GenerationCurrent: 1, GenerationObserved: 1,
				Pods: []corev1.Pod{crashPod}, ComponentName: "Gateway",
			},
			wantPhase: multigresv1alpha1.PhaseDegraded,
		},
		{
			name: "Degraded_OverridesGenerationMismatch",
			input: WorkloadPhaseInput{
				Ready: 0, Total: 3,
				GenerationCurrent: 2, GenerationObserved: 1,
				Pods: []corev1.Pod{crashPod}, ComponentName: "Gateway",
			},
			wantPhase: multigresv1alpha1.PhaseDegraded,
		},
		{
			name: "Initializing_ZeroTotal",
			input: WorkloadPhaseInput{
				Ready: 0, Total: 0,
				GenerationCurrent: 1, GenerationObserved: 1,
				Pods: []corev1.Pod{}, ComponentName: "Gateway",
			},
			wantPhase: multigresv1alpha1.PhaseInitializing,
		},
		{
			name: "NilPods_SkipsCrashDetection",
			input: WorkloadPhaseInput{
				Ready: 1, Total: 3,
				GenerationCurrent: 1, GenerationObserved: 1,
				Pods: nil, ComponentName: "Gateway",
			},
			wantPhase: multigresv1alpha1.PhaseProgressing,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := assert.NewCollecting(t)
			result := ComputeWorkloadPhase(tt.input)
			c.Eq(tt.wantPhase, result.Phase, "ComputeWorkloadPhase() phase")
			c.NotEq("", result.Message, "ComputeWorkloadPhase() returned empty message")
		})
	}
}
