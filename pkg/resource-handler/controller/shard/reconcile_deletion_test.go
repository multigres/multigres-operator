package shard

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestHandleDeletion(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	shardLabels := map[string]string{
		metadata.LabelMultigresCluster:    "test-cluster",
		metadata.LabelMultigresDatabase:   "testdb",
		metadata.LabelMultigresTableGroup: "default",
		metadata.LabelMultigresShard:      "shard0",
	}

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-shard",
			Namespace:         "default",
			DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
			Finalizers:        []string{"kubernetes.io/test"},
			Labels:            shardLabels,
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "shard0",
		},
	}

	makePod := func(name string, scheduled bool, drainState string) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   "default",
				Labels:      shardLabels,
				Annotations: map[string]string{},
			},
		}
		if drainState != "" {
			pod.Annotations[metadata.AnnotationDrainState] = drainState
		}
		if scheduled {
			pod.Status.Conditions = append(pod.Status.Conditions,
				corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				corev1.PodCondition{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
			)
		}
		return pod
	}

	t.Run("scheduled pod gets deleted during shard deletion", func(t *testing.T) {
		t.Parallel()

		pod := makePod("pod-0", true, "")
		shard := baseShard.DeepCopy()

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}
		if result.RequeueAfter != podTerminationRequeueDelay {
			t.Errorf("Expected requeue while pod still terminating, got %v", result.RequeueAfter)
		}

		// Pod should be deleted
		updatedPod := &corev1.Pod{}
		err = c.Get(
			context.Background(),
			types.NamespacedName{Name: "pod-0", Namespace: "default"},
			updatedPod,
		)
		if err == nil {
			t.Error("Expected pod to be deleted")
		}
	})

	t.Run("unscheduled pod gets deleted during shard deletion", func(t *testing.T) {
		t.Parallel()

		pod := makePod("pod-unsched", false, "")
		shard := baseShard.DeepCopy()

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}
		if result.RequeueAfter != podTerminationRequeueDelay {
			t.Errorf(
				"Expected requeue while unscheduled pod terminating, got %v",
				result.RequeueAfter,
			)
		}

		// Pod should be deleted
		updatedPod := &corev1.Pod{}
		err = c.Get(
			context.Background(),
			types.NamespacedName{Name: "pod-unsched", Namespace: "default"},
			updatedPod,
		)
		if err == nil {
			t.Error("Expected pod to be deleted")
		}
	})

	t.Run("ready-for-deletion pod gets deleted", func(t *testing.T) {
		t.Parallel()

		pod := makePod("pod-rfd", true, metadata.DrainStateReadyForDeletion)
		shard := baseShard.DeepCopy()

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}
		if result.RequeueAfter != podTerminationRequeueDelay {
			t.Errorf("Expected requeue while drained pod terminating, got %v", result.RequeueAfter)
		}

		// Pod should be deleted
		updatedPod := &corev1.Pod{}
		err = c.Get(
			context.Background(),
			types.NamespacedName{Name: "pod-rfd", Namespace: "default"},
			updatedPod,
		)
		if err == nil {
			t.Error("Expected pod to be deleted")
		}
	})

	t.Run("mid-drain pod gets deleted directly", func(t *testing.T) {
		t.Parallel()

		pod := makePod("pod-draining", true, metadata.DrainStateDraining)
		pod.Annotations[metadata.AnnotationDrainRequestedAt] = time.Now().UTC().Format(time.RFC3339)
		shard := baseShard.DeepCopy()

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}
		if result.RequeueAfter != podTerminationRequeueDelay {
			t.Errorf("Expected requeue while pod still terminating, got %v", result.RequeueAfter)
		}

		// Pod should be deleted directly, no drain wait
		updatedPod := &corev1.Pod{}
		err = c.Get(
			context.Background(),
			types.NamespacedName{Name: "pod-draining", Namespace: "default"},
			updatedPod,
		)
		if err == nil {
			t.Error("Expected pod to be deleted")
		}
	})

	t.Run("expired drain pod gets deleted directly", func(t *testing.T) {
		t.Parallel()

		pod := makePod("pod-timeout", true, metadata.DrainStateDraining)
		pod.Annotations[metadata.AnnotationDrainRequestedAt] = time.Now().
			Add(-2 * time.Hour).
			UTC().
			Format(time.RFC3339)
		shard := baseShard.DeepCopy()

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}
		if result.RequeueAfter != podTerminationRequeueDelay {
			t.Errorf("Expected requeue while pod still terminating, got %v", result.RequeueAfter)
		}

		// Pod should be deleted
		updatedPod := &corev1.Pod{}
		err = c.Get(
			context.Background(),
			types.NamespacedName{Name: "pod-timeout", Namespace: "default"},
			updatedPod,
		)
		if err == nil {
			t.Error("Expected pod to be deleted")
		}
	})

	t.Run("cleanup completes when no pods exist", func(t *testing.T) {
		t.Parallel()

		shard := baseShard.DeepCopy()

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Error("Expected no requeue when no pods exist")
		}
	})

	t.Run("PVC orphaned only after pods are gone", func(t *testing.T) {
		t.Parallel()

		shard := baseShard.DeepCopy()
		shard.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
			WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
		}
		pod := makePod("pod-0", true, "")
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-pvc-0",
				Namespace: "default",
				Labels:    shardLabels,
			},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod, pvc).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		// First pass: pod still present -> requeue, PVC must NOT be orphaned yet.
		result, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}
		if result.RequeueAfter != podTerminationRequeueDelay {
			t.Errorf("Expected requeue while pod alive, got %v", result.RequeueAfter)
		}
		got := &corev1.PersistentVolumeClaim{}
		if err := c.Get(context.Background(),
			types.NamespacedName{Name: "data-pvc-0", Namespace: "default"}, got); err != nil {
			t.Fatalf("failed to get PVC: %v", err)
		}
		if _, ok := got.Labels[metadata.LabelOrphan]; ok {
			t.Error("PVC must not be orphaned while a pod still references it")
		}

		// Second pass: pod deleted by the first pass -> PVC orphaned now.
		result, err = r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion (2nd pass) returned error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("Expected no requeue once pods are gone, got %v", result.RequeueAfter)
		}
		if err := c.Get(context.Background(),
			types.NamespacedName{Name: "data-pvc-0", Namespace: "default"}, got); err != nil {
			t.Fatalf("failed to get PVC after cleanup: %v", err)
		}
		if _, ok := got.Labels[metadata.LabelOrphan]; !ok {
			t.Error("PVC should be orphaned once all pods are gone")
		}
	})

	t.Run("pod stuck terminating past timeout does not block PVC cleanup", func(t *testing.T) {
		t.Parallel()

		shard := baseShard.DeepCopy()
		shard.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
			WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
		}
		// Pod stuck Terminating well past terminatingTimeout (dead kubelet).
		pod := makePod("pod-stuck", true, "")
		pod.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Add(-2 * terminatingTimeout)}
		pod.Finalizers = []string{"kubernetes.io/test"} // keep it around despite DeletionTimestamp
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-pvc-stuck",
				Namespace: "default",
				Labels:    shardLabels,
			},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod, pvc).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handleDeletion(context.Background(), shard)
		if err != nil {
			t.Fatalf("handleDeletion returned error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("Expected no requeue for pod stuck past timeout, got %v", result.RequeueAfter)
		}
		got := &corev1.PersistentVolumeClaim{}
		if err := c.Get(context.Background(),
			types.NamespacedName{Name: "data-pvc-stuck", Namespace: "default"}, got); err != nil {
			t.Fatalf("failed to get PVC: %v", err)
		}
		if _, ok := got.Labels[metadata.LabelOrphan]; !ok {
			t.Error("PVC should be orphaned even when a pod is stuck terminating past the timeout")
		}
	})
}

func TestHandlePendingDeletion(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	baseShard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pending-shard",
			Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
			Annotations: map[string]string{
				multigresv1alpha1.AnnotationPendingDeletion: "2026-01-01T00:00:00Z",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "shard-0",
		},
	}

	pendingPodLabels := map[string]string{
		metadata.LabelMultigresCluster:    "test-cluster",
		metadata.LabelMultigresDatabase:   "testdb",
		metadata.LabelMultigresTableGroup: "default",
		metadata.LabelMultigresShard:      "shard-0",
	}

	t.Run("No pods — sets ReadyForDeletion immediately", func(t *testing.T) {
		t.Parallel()
		shard := baseShard.DeepCopy()

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handlePendingDeletion(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Error("Expected no requeue when no pods exist")
		}

		updated := &multigresv1alpha1.Shard{}
		if err := c.Get(t.Context(), types.NamespacedName{
			Name: shard.Name, Namespace: shard.Namespace,
		}, updated); err != nil {
			t.Fatalf("failed to get shard: %v", err)
		}

		found := false
		for _, cond := range updated.Status.Conditions {
			if cond.Type == multigresv1alpha1.ConditionReadyForDeletion &&
				cond.Status == metav1.ConditionTrue {
				found = true
			}
		}
		if !found {
			t.Error("Expected ReadyForDeletion condition to be True")
		}
	})

	t.Run("Pods without drain — initiates drain and requeues", func(t *testing.T) {
		t.Parallel()
		shard := baseShard.DeepCopy()

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pending-shard-pool-0",
				Namespace: "default",
				Labels:    pendingPodLabels,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "postgres:16"}},
			},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handlePendingDeletion(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("Expected requeue when pods exist")
		}

		// Verify drain was initiated.
		updatedPod := &corev1.Pod{}
		if err := c.Get(t.Context(), types.NamespacedName{
			Name: pod.Name, Namespace: pod.Namespace,
		}, updatedPod); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updatedPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
			t.Errorf("Expected drain state %q, got %q",
				metadata.DrainStateRequested,
				updatedPod.Annotations[metadata.AnnotationDrainState])
		}
	})

	t.Run("All pods ready-for-deletion — sets ReadyForDeletion", func(t *testing.T) {
		t.Parallel()
		shard := baseShard.DeepCopy()

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pending-shard-pool-0",
				Namespace: "default",
				Labels:    pendingPodLabels,
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateReadyForDeletion,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "postgres:16"}},
			},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handlePendingDeletion(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// The pod was deleted, so we need to requeue to check again.
		if result.RequeueAfter == 0 {
			t.Error("Expected requeue after deleting drained pods")
		}

		// Verify the pod was deleted.
		updatedPod := &corev1.Pod{}
		err = c.Get(t.Context(), types.NamespacedName{
			Name: pod.Name, Namespace: pod.Namespace,
		}, updatedPod)
		if err == nil {
			t.Error("Expected pod to be deleted")
		}
	})

	t.Run("Mix of draining and undrained pods — requeues", func(t *testing.T) {
		t.Parallel()
		shard := baseShard.DeepCopy()

		drainingPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pending-shard-pool-0",
				Namespace: "default",
				Labels:    pendingPodLabels,
				Annotations: map[string]string{
					metadata.AnnotationDrainState: metadata.DrainStateDraining,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "postgres:16"}},
			},
		}

		undrainedPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pending-shard-pool-1",
				Namespace: "default",
				Labels:    pendingPodLabels,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "pg", Image: "postgres:16"}},
			},
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, drainingPod, undrainedPod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()

		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		result, err := r.handlePendingDeletion(t.Context(), shard)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("Expected requeue when pods are still draining")
		}

		// Verify the undrained pod now has drain state set.
		updatedPod := &corev1.Pod{}
		if err := c.Get(t.Context(), types.NamespacedName{
			Name: undrainedPod.Name, Namespace: undrainedPod.Namespace,
		}, updatedPod); err != nil {
			t.Fatalf("failed to get pod: %v", err)
		}
		if updatedPod.Annotations[metadata.AnnotationDrainState] != metadata.DrainStateRequested {
			t.Errorf("Expected drain state %q, got %q",
				metadata.DrainStateRequested,
				updatedPod.Annotations[metadata.AnnotationDrainState])
		}
	})
}
