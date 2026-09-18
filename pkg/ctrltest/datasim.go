package ctrltest

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DataPlaneSim stands in for everything envtest does not run: kubelet, the
// Deployment/StatefulSet controllers, and the volume provisioner.
//
// Modelled:
//   - Pods reach Running with a podIP and Ready/ContainersReady/Initialized/
//     PodScheduled conditions true, and per-container Ready statuses.
//   - Deployments and StatefulSets report status matching spec.replicas.
//   - PVCs reach Bound.
//
// Not modelled: scheduling failure, eviction, container restart, node pressure,
// real volume attachment, or any ordering between them.
type DataPlaneSim struct {
	c        client.Client
	interval time.Duration
	podIPSeq int
}

// NewDataPlaneSim returns a simulator that patches status on every object it
// models, once per interval, through c.
func NewDataPlaneSim(c client.Client, interval time.Duration) *DataPlaneSim {
	return &DataPlaneSim{c: c, interval: interval}
}

// Run drives the simulator until ctx is cancelled.
func (s *DataPlaneSim) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tickPods(ctx)
			s.tickDeployments(ctx)
			s.tickStatefulSets(ctx)
			s.tickPVCs(ctx)
		}
	}
}

func (s *DataPlaneSim) tickPods(ctx context.Context) {
	list := &corev1.PodList{}
	if err := s.c.List(ctx, list); err != nil {
		return
	}
	for i := range list.Items {
		p := &list.Items[i]
		if !p.DeletionTimestamp.IsZero() {
			continue
		}
		if podSimSatisfied(p) {
			continue
		}
		base := p.DeepCopy()
		p.Status.Phase = corev1.PodRunning
		if p.Status.PodIP == "" {
			s.podIPSeq++
			p.Status.PodIP = fmt.Sprintf("10.244.%d.%d", s.podIPSeq/250, s.podIPSeq%250+1)
			p.Status.PodIPs = []corev1.PodIP{{IP: p.Status.PodIP}}
			p.Status.HostIP = "10.0.0.1"
		}
		p.Status.StartTime = &metav1.Time{Time: time.Now()}
		for _, ct := range []corev1.PodConditionType{
			corev1.PodScheduled,
			corev1.PodInitialized,
			corev1.ContainersReady,
			corev1.PodReady,
		} {
			setPodCondition(p, ct)
		}
		p.Status.ContainerStatuses = containerStatuses(p.Spec.Containers)
		p.Status.InitContainerStatuses = initContainerStatuses(p.Spec.InitContainers)
		_ = s.c.Status().Patch(ctx, p, client.MergeFrom(base))
	}
}

func podSimSatisfied(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" {
		return false
	}
	if len(p.Status.ContainerStatuses) != len(p.Spec.Containers) {
		return false
	}
	want := map[corev1.PodConditionType]bool{
		corev1.PodScheduled:    false,
		corev1.PodInitialized:  false,
		corev1.ContainersReady: false,
		corev1.PodReady:        false,
	}
	for _, c := range p.Status.Conditions {
		if _, ok := want[c.Type]; ok && c.Status == corev1.ConditionTrue {
			want[c.Type] = true
		}
	}
	for _, ok := range want {
		if !ok {
			return false
		}
	}
	return true
}

func setPodCondition(p *corev1.Pod, t corev1.PodConditionType) {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == t {
			if p.Status.Conditions[i].Status != corev1.ConditionTrue {
				p.Status.Conditions[i].Status = corev1.ConditionTrue
				p.Status.Conditions[i].LastTransitionTime = metav1.Now()
			}
			return
		}
	}
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
		Type:               t,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	})
}

func containerStatuses(cs []corev1.Container) []corev1.ContainerStatus {
	out := make([]corev1.ContainerStatus, 0, len(cs))
	for _, c := range cs {
		out = append(out, corev1.ContainerStatus{
			Name:        c.Name,
			Image:       c.Image,
			ImageID:     "sha256:" + c.Name,
			ContainerID: "containerd://" + c.Name,
			Ready:       true,
			Started:     ptr.To(true),
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()},
			},
		})
	}
	return out
}

func initContainerStatuses(cs []corev1.Container) []corev1.ContainerStatus {
	out := make([]corev1.ContainerStatus, 0, len(cs))
	for _, c := range cs {
		out = append(out, corev1.ContainerStatus{
			Name:        c.Name,
			Image:       c.Image,
			ImageID:     "sha256:" + c.Name,
			ContainerID: "containerd://" + c.Name,
			Ready:       true,
			Started:     ptr.To(true),
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   0,
					Reason:     "Completed",
					FinishedAt: metav1.Now(),
				},
			},
		})
	}
	return out
}

func (s *DataPlaneSim) tickDeployments(ctx context.Context) {
	list := &appsv1.DeploymentList{}
	if err := s.c.List(ctx, list); err != nil {
		return
	}
	for i := range list.Items {
		d := &list.Items[i]
		want := int32(1)
		if d.Spec.Replicas != nil {
			want = *d.Spec.Replicas
		}
		if d.Status.ObservedGeneration == d.Generation &&
			d.Status.ReadyReplicas == want &&
			d.Status.AvailableReplicas == want &&
			d.Status.UpdatedReplicas == want &&
			d.Status.Replicas == want {
			continue
		}
		base := d.DeepCopy()
		d.Status.ObservedGeneration = d.Generation
		d.Status.Replicas = want
		d.Status.ReadyReplicas = want
		d.Status.AvailableReplicas = want
		d.Status.UpdatedReplicas = want
		d.Status.UnavailableReplicas = 0
		d.Status.Conditions = []appsv1.DeploymentCondition{
			{
				Type:               appsv1.DeploymentAvailable,
				Status:             corev1.ConditionTrue,
				Reason:             "MinimumReplicasAvailable",
				LastTransitionTime: metav1.Now(),
				LastUpdateTime:     metav1.Now(),
			},
			{
				Type:               appsv1.DeploymentProgressing,
				Status:             corev1.ConditionTrue,
				Reason:             "NewReplicaSetAvailable",
				LastTransitionTime: metav1.Now(),
				LastUpdateTime:     metav1.Now(),
			},
		}
		_ = s.c.Status().Patch(ctx, d, client.MergeFrom(base))
	}
}

func (s *DataPlaneSim) tickStatefulSets(ctx context.Context) {
	list := &appsv1.StatefulSetList{}
	if err := s.c.List(ctx, list); err != nil {
		return
	}
	for i := range list.Items {
		ss := &list.Items[i]
		want := int32(1)
		if ss.Spec.Replicas != nil {
			want = *ss.Spec.Replicas
		}
		if ss.Status.ObservedGeneration == ss.Generation &&
			ss.Status.ReadyReplicas == want &&
			ss.Status.AvailableReplicas == want &&
			ss.Status.CurrentReplicas == want &&
			ss.Status.UpdatedReplicas == want &&
			ss.Status.Replicas == want {
			continue
		}
		base := ss.DeepCopy()
		ss.Status.ObservedGeneration = ss.Generation
		ss.Status.Replicas = want
		ss.Status.ReadyReplicas = want
		ss.Status.AvailableReplicas = want
		ss.Status.CurrentReplicas = want
		ss.Status.UpdatedReplicas = want
		ss.Status.CurrentRevision = ss.Name + "-rev"
		ss.Status.UpdateRevision = ss.Name + "-rev"
		_ = s.c.Status().Patch(ctx, ss, client.MergeFrom(base))
	}
}

func (s *DataPlaneSim) tickPVCs(ctx context.Context) {
	list := &corev1.PersistentVolumeClaimList{}
	if err := s.c.List(ctx, list); err != nil {
		return
	}
	for i := range list.Items {
		pvc := &list.Items[i]
		if pvc.Status.Phase == corev1.ClaimBound {
			continue
		}
		base := pvc.DeepCopy()
		pvc.Status.Phase = corev1.ClaimBound
		pvc.Status.AccessModes = pvc.Spec.AccessModes
		if pvc.Spec.Resources.Requests != nil {
			pvc.Status.Capacity = pvc.Spec.Resources.Requests
		}
		_ = s.c.Status().Patch(ctx, pvc, client.MergeFrom(base))
	}
}
