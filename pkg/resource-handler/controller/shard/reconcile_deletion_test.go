package shard

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/monitoring"
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
			UID:               "test-shard-uid",
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
		pvc := buildDeletionTestBackupPVC(t, shard, scheme)

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
			client.ObjectKeyFromObject(pvc), got); err != nil {
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
			client.ObjectKeyFromObject(pvc), got); err != nil {
			t.Fatalf("failed to get PVC after cleanup: %v", err)
		}
		if _, ok := got.Labels[metadata.LabelOrphan]; !ok {
			t.Error("PVC should be orphaned once all pods are gone")
		}
	})

	t.Run(
		"MultigresCluster being deleted hard-deletes PVC instead of orphaning",
		func(t *testing.T) {
			t.Parallel()

			shard := baseShard.DeepCopy()
			shard.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
				WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
			}
			pvc := buildDeletionTestBackupPVC(t, shard, scheme)
			cluster := &multigresv1alpha1.MultigresCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-cluster",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					Finalizers:        []string{multigresv1alpha1.FinalizerClusterCleanup},
				},
			}

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(shard, pvc, cluster).
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
				t.Errorf("Expected no requeue once pods are gone, got %v", result.RequeueAfter)
			}
			got := &corev1.PersistentVolumeClaim{}
			err = c.Get(context.Background(),
				client.ObjectKeyFromObject(pvc), got)
			if !apierrors.IsNotFound(err) {
				t.Errorf(
					"PVC should be hard-deleted while cluster is being deleted, got err=%v",
					err,
				)
			}
		},
	)

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
		pvc := buildDeletionTestBackupPVC(t, shard, scheme)

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
			client.ObjectKeyFromObject(pvc), got); err != nil {
			t.Fatalf("failed to get PVC: %v", err)
		}
		if _, ok := got.Labels[metadata.LabelOrphan]; !ok {
			t.Error("PVC should be orphaned even when a pod is stuck terminating past the timeout")
		}
	})

	// Strangers carry the shard's identity labels without a name the shard's
	// builders produce. Neither teardown path may delete or orphan them,
	// including one controlled by another owner, and one that already carries
	// this Shard's ref must be released rather than left for garbage
	// collection to delete with the Shard. The shard's own PVCs, the backup
	// and a data PVC of a pool no longer in the spec, are deleted or
	// orphaned under Delete and released under Retain.
	for _, tc := range []struct {
		policy   multigresv1alpha1.PVCRetentionPolicyType
		churning bool
	}{
		{multigresv1alpha1.DeletePVCRetentionPolicy, false},
		{multigresv1alpha1.DeletePVCRetentionPolicy, true},
		{multigresv1alpha1.RetainPVCRetentionPolicy, false},
		{multigresv1alpha1.RetainPVCRetentionPolicy, true},
	} {
		t.Run(
			fmt.Sprintf(
				"teardown touches only the shard's own PVCs (policy=%s, cluster deleting=%v)",
				tc.policy,
				tc.churning,
			),
			func(t *testing.T) {
				t.Parallel()

				shard := baseShard.DeepCopy()
				shard.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
					WhenDeleted: tc.policy,
				}
				// Built with the Shard's ref whatever the policy: under Retain
				// that is a ref a normal reconcile has not released yet.
				backup := buildDeletionTestBackupPVC(t, shard, scheme)
				data, err := BuildPoolDataPVC(
					shard,
					"gone",
					"z1",
					newTestPoolSpec(),
					0,
					true,
					scheme,
				)
				if err != nil {
					t.Fatalf("build data PVC: %v", err)
				}
				own := []*corev1.PersistentVolumeClaim{backup, data}

				unowned := buildDeletionTestBackupPVC(t, shard, scheme)
				unowned.Name = "impostor-shared-pvc"
				unowned.OwnerReferences = nil
				controlled := unowned.DeepCopy()
				controlled.Name = "controlled-by-another"
				controlled.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: "v1",
					Kind:       "ConfigMap",
					Name:       "someone-else",
					UID:        "someone-else-uid",
					Controller: new(true),
				}}
				adopted := buildDeletionTestBackupPVC(t, shard, scheme)
				adopted.Name = "adopted-stranger"
				strangers := []*corev1.PersistentVolumeClaim{unowned, controlled, adopted}

				objs := []client.Object{shard}
				for _, pvc := range append(slices.Clone(own), strangers...) {
					objs = append(objs, pvc)
				}
				if tc.churning {
					objs = append(objs, &multigresv1alpha1.MultigresCluster{
						ObjectMeta: metav1.ObjectMeta{
							Name:              "test-cluster",
							Namespace:         "default",
							DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
							Finalizers:        []string{multigresv1alpha1.FinalizerClusterCleanup},
						},
					})
				}
				c := fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(objs...).
					WithStatusSubresource(&multigresv1alpha1.Shard{}).
					Build()
				r := &ShardReconciler{
					Client:          c,
					Scheme:          scheme,
					Recorder:        record.NewFakeRecorder(10),
					CreateTopoStore: newMemoryTopoFactory(),
				}

				if _, err := r.handleDeletion(context.Background(), shard); err != nil {
					t.Fatalf("handleDeletion returned error: %v", err)
				}

				deleteOwn := tc.policy == multigresv1alpha1.DeletePVCRetentionPolicy
				for _, pvc := range own {
					got := &corev1.PersistentVolumeClaim{}
					err := c.Get(context.Background(), client.ObjectKeyFromObject(pvc), got)
					if deleteOwn && tc.churning {
						if !apierrors.IsNotFound(err) {
							t.Errorf("own PVC %s should be deleted, got err=%v", pvc.Name, err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("get own PVC %s: %v", pvc.Name, err)
					}
					if _, ok := got.Labels[metadata.LabelOrphan]; ok != deleteOwn {
						t.Errorf(
							"own PVC %s orphan label present=%v, want %v",
							pvc.Name,
							ok,
							deleteOwn,
						)
					}
					if metav1.IsControlledBy(got, shard) {
						t.Errorf("own PVC %s still carries the Shard's ref", pvc.Name)
					}
				}
				for _, stranger := range strangers {
					got := &corev1.PersistentVolumeClaim{}
					if err := c.Get(
						context.Background(),
						client.ObjectKeyFromObject(stranger),
						got,
					); err != nil {
						t.Fatalf("stranger %s: %v", stranger.Name, err)
					}
					if _, ok := got.Labels[metadata.LabelOrphan]; ok {
						t.Errorf("stranger %s was marked orphan", stranger.Name)
					}
					want := stranger.OwnerReferences
					if stranger == adopted {
						want = nil
					}
					if len(got.OwnerReferences) != len(want) {
						t.Errorf("stranger %s owner references = %v, want %v",
							stranger.Name, got.OwnerReferences, want)
					}
				}
			},
		)
	}
}

// buildDeletionTestBackupPVC is the shard's shared backup PVC as the
// production builder makes it under a Delete policy.
func buildDeletionTestBackupPVC(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
	scheme *runtime.Scheme,
) *corev1.PersistentVolumeClaim {
	t.Helper()
	pvc, err := BuildSharedBackupPVC(shard, true, scheme)
	if err != nil {
		t.Fatalf("build backup PVC: %v", err)
	}
	return pvc
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
		metadata.LabelAppComponent:        PoolComponentName,
		metadata.LabelAppManagedBy:        metadata.ManagedByMultigres,
	}

	t.Run("Unmanaged pod with the shard's labels — not drained", func(t *testing.T) {
		t.Parallel()
		shard := baseShard.DeepCopy()

		// Someone else's pod that happens to carry the pool's labels. The
		// cache never held it, and an uncached read must not adopt it.
		strangerLabels := maps.Clone(pendingPodLabels)
		delete(strangerLabels, metadata.LabelAppManagedBy)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "stranger",
				Namespace: "default",
				Labels:    strangerLabels,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "app"}},
			},
		}
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()
		r := &ShardReconciler{
			Client:          c,
			APIReader:       c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		if _, err := r.handlePendingDeletion(t.Context(), shard); err != nil {
			t.Fatalf("handlePendingDeletion: %v", err)
		}
		got := &corev1.Pod{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), got); err != nil {
			t.Fatalf("unmanaged pod was deleted: %v", err)
		}
		if v := got.Annotations[metadata.AnnotationDrainState]; v != "" {
			t.Errorf("unmanaged pod was put through the drain: %s", v)
		}
	})

	t.Run("Only a multiorch pod — sets ReadyForDeletion, leaves it alone", func(t *testing.T) {
		t.Parallel()
		shard := baseShard.DeepCopy()

		// Multiorch pods carry the shard's labels too, but their Deployment
		// replaces any one deleted, so draining them would never end.
		multiorchLabels := maps.Clone(pendingPodLabels)
		multiorchLabels[metadata.LabelAppComponent] = MultiorchComponentName
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pending-shard-multiorch-0",
				Namespace: "default",
				Labels:    multiorchLabels,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "multiorch", Image: "multiorch"}},
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

		if _, err := r.handlePendingDeletion(t.Context(), shard); err != nil {
			t.Fatalf("handlePendingDeletion: %v", err)
		}
		updated := &multigresv1alpha1.Shard{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), updated); err != nil {
			t.Fatal(err)
		}
		if !meta.IsStatusConditionTrue(
			updated.Status.Conditions, multigresv1alpha1.ConditionReadyForDeletion,
		) {
			t.Error("want ReadyForDeletion with no pool pods left")
		}
		got := &corev1.Pod{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(pod), got); err != nil {
			t.Fatalf("multiorch pod was deleted: %v", err)
		}
		if v := got.Annotations[metadata.AnnotationDrainState]; v != "" {
			t.Errorf("multiorch pod was put through the drain: %s", v)
		}
	})

	t.Run("Pool pod not yet in the cache — does not report ready", func(t *testing.T) {
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
		// One API server holding the Shard and its pod, and a cached client
		// over it whose pod lists have not caught up. Writes go through to
		// the server either way, as they do in production.
		server := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(shard, pod).
			WithStatusSubresource(&multigresv1alpha1.Shard{}).
			Build()
		cached := interceptor.NewClient(server, interceptor.Funcs{
			List: func(
				ctx context.Context,
				c client.WithWatch,
				list client.ObjectList,
				opts ...client.ListOption,
			) error {
				if err := c.List(ctx, list, opts...); err != nil {
					return err
				}
				if pods, ok := list.(*corev1.PodList); ok {
					pods.Items = nil
				}
				return nil
			},
		})
		r := &ShardReconciler{
			Client:          cached,
			APIReader:       server,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		res, err := r.handlePendingDeletion(t.Context(), shard)
		if err != nil {
			t.Fatalf("handlePendingDeletion: %v", err)
		}
		if res.RequeueAfter == 0 {
			t.Error("want a requeue while a pool pod exists on the server")
		}
		updated := &multigresv1alpha1.Shard{}
		if err := server.Get(t.Context(), client.ObjectKeyFromObject(shard), updated); err != nil {
			t.Fatal(err)
		}
		if meta.IsStatusConditionTrue(
			updated.Status.Conditions, multigresv1alpha1.ConditionReadyForDeletion,
		) {
			t.Error("reported ReadyForDeletion off a cache that had not seen the pod")
		}
	})

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

func TestShardDeletionLifecycle_ForgetsMetrics(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	labels := map[string]string{
		metadata.LabelMultigresCluster:    "test-cluster",
		metadata.LabelMultigresDatabase:   "testdb",
		metadata.LabelMultigresTableGroup: "default",
		metadata.LabelMultigresShard:      "shard0",
	}
	newShard := func(name string) *multigresv1alpha1.Shard {
		return &multigresv1alpha1.Shard{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         "default",
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
				Finalizers:        []string{shardFinalizer},
				Labels:            labels,
			},
			Spec: multigresv1alpha1.ShardSpec{
				DatabaseName:   "testdb",
				TableGroupName: "default",
				ShardName:      "shard0",
			},
		}
	}
	reconcile := func(r *ShardReconciler, name string) {
		t.Helper()
		if _, err := r.Reconcile(t.Context(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
		}); err != nil {
			t.Fatalf("Reconcile() error: %v", err)
		}
	}

	t.Run("series are dropped once the shard is gone", func(t *testing.T) {
		const name = "lifecycle-gone"
		t.Cleanup(func() { monitoring.ForgetShard(name, "default") })
		setAllShardGauges("test-cluster", name, "default")

		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(newShard(name)).Build()
		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		reconcile(r, name) // handleDeletion removes the finalizer
		err := c.Get(t.Context(), types.NamespacedName{Name: name, Namespace: "default"},
			&multigresv1alpha1.Shard{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("expected shard to be gone after finalizer removal, got %v", err)
		}

		reconcile(r, name) // the delete event's reconcile sees NotFound
		if got := shardGaugeFamilies(t, name, "default"); len(got) != 0 {
			t.Errorf("expected no series after shard deletion, still present in %v", got)
		}
	})

	t.Run("series are kept while pods are still terminating", func(t *testing.T) {
		const name = "lifecycle-blocked"
		t.Cleanup(func() { monitoring.ForgetShard(name, "default") })
		setAllShardGauges("test-cluster", name, "default")

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-0", Namespace: "default", Labels: labels},
		}
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(newShard(name), pod).
			Build()
		r := &ShardReconciler{
			Client:          c,
			Scheme:          scheme,
			Recorder:        record.NewFakeRecorder(10),
			CreateTopoStore: newMemoryTopoFactory(),
		}

		reconcile(r, name)
		wantKept := slices.DeleteFunc(slices.Clone(wantShardGaugeFamilies), func(f string) bool {
			return f == "multigres_operator_pool_pods_stuck"
		})
		if got := shardGaugeFamilies(t, name, "default"); !slices.Equal(got, wantKept) {
			t.Errorf("series other than pool_pods_stuck must survive, got %v", got)
		}
	})
}
