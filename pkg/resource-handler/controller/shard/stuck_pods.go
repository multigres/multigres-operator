package shard

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

// stuckPodsRefreshInterval bounds how stale the pool_pods_stuck gauge gets when
// a zone recovers without producing a pod event. Nodes are deliberately not
// watched by the shard controller, so a periodic requeue stands in for them.
const stuckPodsRefreshInterval = time.Minute

// stuckPoolPods counts pods that are not Ready although at least one Ready,
// schedulable node satisfies the pod's node selector and required node
// affinity. While a zone is down no node qualifies, so the count stays 0 and an
// alert on it does not fire for the outage itself. Taints are not considered.
func stuckPoolPods(pods []corev1.Pod, nodes []corev1.Node) int32 {
	var stuck int32
	for i := range pods {
		pod := &pods[i]
		if isPodReady(pod) {
			continue
		}
		if anyNodeCanRun(pod, nodes) {
			stuck++
		}
	}
	return stuck
}

func anyNodeCanRun(pod *corev1.Pod, nodes []corev1.Node) bool {
	affinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	for i := range nodes {
		node := &nodes[i]
		if node.Spec.Unschedulable || !isNodeReady(node) {
			continue
		}
		if ok, err := affinity.Match(node); err == nil && ok {
			return true
		}
	}
	return false
}

func isNodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
