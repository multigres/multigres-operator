package suite

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// pvcProtectionFinalizer is what the StorageObjectInUseProtection admission
// plugin, enabled in envtest's API server, puts on every PVC.
const pvcProtectionFinalizer = "kubernetes.io/pvc-protection"

// pvcProtectionSim stands in for kube-controller-manager's pvc-protection
// controller, which envtest does not run: it removes the pvc-protection
// finalizer from a deleted PVC once no pod references it, letting the deletion
// finish. Without it every deleted PVC in the suite stays Terminating forever,
// which no cluster does.
//
// The real controller counts every pod bound to a node, in any phase. Nothing
// in envtest binds pods to nodes, so this counts every pod, which is the
// answer the real one gives once the pods it would count are scheduled.
//
// This belongs in testkit's DataPlaneSim (multigres/testkit models neither the
// finalizer's removal nor the scheduler refusing a pod on a PVC being
// deleted); delete this once it does.
type pvcProtectionSim struct {
	c        client.Client
	interval time.Duration
}

// heldPVCProtection holds the namespaces whose tests release deleted PVCs
// themselves, to observe the state before the release.
var heldPVCProtection sync.Map

func (s *pvcProtectionSim) run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *pvcProtectionSim) tick(ctx context.Context) {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := s.c.List(ctx, pvcs); err != nil {
		return
	}
	pods := map[string]*corev1.PodList{}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if pvc.DeletionTimestamp.IsZero() ||
			!slices.Contains(pvc.Finalizers, pvcProtectionFinalizer) {
			continue
		}
		if _, held := heldPVCProtection.Load(pvc.Namespace); held {
			continue
		}
		list, ok := pods[pvc.Namespace]
		if !ok {
			list = &corev1.PodList{}
			if err := s.c.List(ctx, list, client.InNamespace(pvc.Namespace)); err != nil {
				continue
			}
			pods[pvc.Namespace] = list
		}
		if pvcInUse(list.Items, pvc.Name) {
			continue
		}
		_ = removePVCProtection(ctx, s.c, pvc)
	}
}

// pvcInUse reports whether any pod references the claim.
func pvcInUse(pods []corev1.Pod, claim string) bool {
	for i := range pods {
		pod := &pods[i]
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == claim {
				return true
			}
		}
	}
	return false
}

// removePVCProtection removes the finalizer, conditioned on pvc's
// resourceVersion so a concurrent change to the finalizers is not overwritten.
func removePVCProtection(
	ctx context.Context,
	c client.Client,
	pvc *corev1.PersistentVolumeClaim,
) error {
	base := pvc.DeepCopy()
	pvc.Finalizers = slices.DeleteFunc(pvc.Finalizers, func(f string) bool {
		return f == pvcProtectionFinalizer
	})
	return c.Patch(
		ctx,
		pvc,
		client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}),
	)
}

// HoldPVCProtection stops pvcProtectionSim releasing deleted PVCs in the
// case's namespace until the test ends; releasePVCProtection releases one.
func (c *C) HoldPVCProtection(t *testing.T) {
	heldPVCProtection.Store(c.NS, true)
	t.Cleanup(func() { heldPVCProtection.Delete(c.NS) })
}
