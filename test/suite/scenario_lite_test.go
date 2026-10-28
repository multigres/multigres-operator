package suite

import (
	"fmt"
	"testing"
	"time"

	"github.com/multigres/testkit/ctrltest"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestLiteCellReadinessAndServiceRouting(t *testing.T) {
	c := newCase(t)
	fault := func(p *corev1.Pod) *ctrltest.PodFault {
		if p.Labels[metadata.LabelMultigresGatewayRole] == metadata.GatewayRoleLite {
			return &ctrltest.PodFault{NotReady: []string{"minigres"}}
		}
		return nil
	}
	c.FaultPods(fault)
	cluster := c.newCluster(
		"lite",
		func(spec *MultigresClusterSpec) { spec.Mode = multigresv1alpha1.ModeLite },
	)
	var pod corev1.Pod
	var cell multigresv1alpha1.Cell
	c.Eventually(30*time.Second, "Shard to create Lite pod and Cell to observe it", func() error {
		pods := &corev1.PodList{}
		if err := c.List(
			pods,
			client.MatchingLabels{metadata.LabelMultigresGatewayRole: metadata.GatewayRoleLite},
		); err != nil {
			return err
		}
		cells := &multigresv1alpha1.CellList{}
		if err := c.List(cells); err != nil {
			return err
		}
		if len(pods.Items) != 1 || len(cells.Items) != 1 {
			return fmt.Errorf("pods=%d cells=%d", len(pods.Items), len(cells.Items))
		}
		pod = pods.Items[0]
		cell = cells.Items[0]
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.Kind != "Shard" {
			return fmt.Errorf("pod owner=%v", owner)
		}
		if cell.Status.ObservedGeneration != cell.Generation || cell.Status.GatewayReplicas != 1 ||
			podReady(&pod) {
			return fmt.Errorf("waiting for unready Lite gateway")
		}
		return nil
	})
	c.FaultPods(nil)
	waitReady := func(want int32) {
		c.Eventually(
			15*time.Second,
			fmt.Sprintf("Cell gateway ready replicas=%d", want),
			func() error {
				if err := c.Get(client.ObjectKeyFromObject(&pod), &pod); err != nil {
					return err
				}
				if err := c.Get(client.ObjectKeyFromObject(&cell), &cell); err != nil {
					return err
				}
				if podReady(&pod) != (want == 1) || cell.Status.GatewayReadyReplicas != want {
					return fmt.Errorf(
						"podReady=%v Cell ready=%d",
						podReady(&pod),
						cell.Status.GatewayReadyReplicas,
					)
				}
				return nil
			},
		)
	}
	waitReady(1)
	for _, name := range []string{cluster.Name + "-multigateway", cell.Status.GatewayServiceName} {
		svc := &corev1.Service{}
		c.NoError(c.Get(client.ObjectKey{Namespace: c.NS, Name: name}, svc))
		c.True(
			len(svc.Spec.Selector) > 0 &&
				labels.SelectorFromSet(svc.Spec.Selector).Matches(labels.Set(pod.Labels)),
			"Service %s must select the Shard's Lite pod",
			name,
		)
	}
	deployments := &appsv1.DeploymentList{}
	c.NoError(
		c.List(
			deployments,
			client.MatchingLabels{metadata.LabelAppComponent: metadata.ComponentMultigateway},
		),
	)
	c.Len(deployments.Items, 0, "Lite has no gateway Deployment")
	c.FaultPods(fault)
	waitReady(0)
	c.FaultPods(nil)
	waitReady(1)
	for _, rec := range Suite.Reconciles.InNamespace(c.NS) {
		if rec.Controller == "cell" {
			c.Eq(
				time.Duration(0),
				rec.Result.RequeueAfter,
				"Lite Cell must follow Pod events without polling",
			)
		}
	}
}
