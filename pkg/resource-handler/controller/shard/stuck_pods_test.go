package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const zoneLabel = "topology.kubernetes.io/zone"

func stuckTestPod(zone string, ready bool) corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Pod{
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{zoneLabel: zone},
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "workload",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"customer-pg"},
						}},
					}},
				},
			}},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
		},
	}
}

func stuckTestNode(zone, workload string, ready, unschedulable bool) corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{zoneLabel: zone, "workload": workload},
		},
		Spec: corev1.NodeSpec{Unschedulable: unschedulable},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}},
		},
	}
}

func TestStuckPoolPods(t *testing.T) {
	tests := []struct {
		name  string
		pods  []corev1.Pod
		nodes []corev1.Node
		want  int32
	}{
		{
			name: "zone down: no node in the zone",
			pods: []corev1.Pod{stuckTestPod("a", false), stuckTestPod("a", false)},
			nodes: []corev1.Node{
				stuckTestNode("b", "customer-pg", true, false),
			},
			want: 0,
		},
		{
			name: "zone down: every node in the zone is NotReady",
			pods: []corev1.Pod{stuckTestPod("a", false)},
			nodes: []corev1.Node{
				stuckTestNode("a", "customer-pg", false, false),
			},
			want: 0,
		},
		{
			name:  "no nodes at all",
			pods:  []corev1.Pod{stuckTestPod("a", false)},
			nodes: nil,
			want:  0,
		},
		{
			name: "zone healthy with pending pods",
			pods: []corev1.Pod{stuckTestPod("a", false), stuckTestPod("a", true)},
			nodes: []corev1.Node{
				stuckTestNode("a", "customer-pg", true, false),
			},
			want: 1,
		},
		{
			name: "cordoned node is ignored",
			pods: []corev1.Pod{stuckTestPod("a", false)},
			nodes: []corev1.Node{
				stuckTestNode("a", "customer-pg", true, true),
			},
			want: 0,
		},
		{
			name: "one usable node among unusable ones is enough",
			pods: []corev1.Pod{stuckTestPod("a", false), stuckTestPod("a", false)},
			nodes: []corev1.Node{
				stuckTestNode("a", "customer-pg", false, false),
				stuckTestNode("a", "customer-pg", true, true),
				stuckTestNode("a", "customer-pg", true, false),
			},
			want: 2,
		},
		{
			name: "node outside the node selector is ignored",
			pods: []corev1.Pod{stuckTestPod("a", false)},
			nodes: []corev1.Node{
				stuckTestNode("b", "customer-pg", true, false),
			},
			want: 0,
		},
		{
			name: "node failing required node affinity is ignored",
			pods: []corev1.Pod{stuckTestPod("a", false)},
			nodes: []corev1.Node{
				stuckTestNode("a", "system", true, false),
			},
			want: 0,
		},
		{
			name: "all pods ready",
			pods: []corev1.Pod{stuckTestPod("a", true), stuckTestPod("a", true)},
			nodes: []corev1.Node{
				stuckTestNode("a", "customer-pg", true, false),
			},
			want: 0,
		},
		{
			name: "pod without a Ready condition counts as not ready",
			pods: []corev1.Pod{{
				Spec: stuckTestPod("a", false).Spec,
			}},
			nodes: []corev1.Node{
				stuckTestNode("a", "customer-pg", true, false),
			},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stuckPoolPods(tt.pods, tt.nodes); got != tt.want {
				t.Errorf("stuckPoolPods() = %d, want %d", got, tt.want)
			}
		})
	}
}
