package shard

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

const pvcProtection = "kubernetes.io/pvc-protection"

// deleteHeld deletes obj in c, which a pvc-protection finalizer on it keeps in
// place with a deletionTimestamp, as the API server does for a PVC in use.
func deleteHeld(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	require.Contains(t, obj.GetFinalizers(), pvcProtection)
	require.NoError(t, c.Delete(t.Context(), obj))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj))
	require.False(t, obj.GetDeletionTimestamp().IsZero())
}

func filesystemBackups(shard *multigresv1alpha1.Shard) *multigresv1alpha1.Shard {
	shard.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
	}
	return shard
}

func protected(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
	pvc.Finalizers = append(pvc.Finalizers, pvcProtection)
	return pvc
}

// poolFixture is a non-Lite shard with one pool pod, its data PVC and the
// shared backup PVC, all from the production builders.
type poolFixture struct {
	shard        *multigresv1alpha1.Shard
	pool         multigresv1alpha1.PoolSpec
	pod          *corev1.Pod
	data, backup *corev1.PersistentVolumeClaim
	scheme       *runtime.Scheme
	client       client.Client
	reconciler   *ShardReconciler
}

func newPoolFixture(t *testing.T, funcs ...interceptor.Funcs) *poolFixture {
	t.Helper()
	f := &poolFixture{
		shard:  filesystemBackups(newTestShard()),
		pool:   newTestPoolSpec(),
		scheme: reloadTestScheme(t),
	}
	var err error
	f.pod, err = BuildPoolPod(f.shard, "main", "z1", f.pool, 0, f.scheme)
	require.NoError(t, err)
	f.data, err = BuildPoolDataPVC(f.shard, "main", "z1", f.pool, 0, true, f.scheme)
	require.NoError(t, err)
	f.backup, err = BuildSharedBackupPVC(f.shard, true, f.scheme)
	require.NoError(t, err)
	protected(f.data)
	protected(f.backup)
	require.True(t, slices.ContainsFunc(f.pod.Spec.Volumes, func(v corev1.Volume) bool {
		return v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == f.backup.Name
	}), "pool pods mount the shared backup PVC")
	b := fake.NewClientBuilder().WithScheme(f.scheme).WithObjects(f.shard, f.pod, f.data, f.backup)
	if len(funcs) > 0 {
		b = b.WithInterceptorFuncs(funcs[0])
	}
	f.client = b.Build()
	f.reconciler = &ShardReconciler{
		Client: f.client, Scheme: f.scheme, Recorder: record.NewFakeRecorder(100),
	}
	return f
}

func TestTerminatingClaimsInUse(t *testing.T) {
	now := metav1.Now()
	claim := func(name string, deleting bool) corev1.PersistentVolumeClaim {
		pvc := corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if deleting {
			pvc.DeletionTimestamp = &now
			pvc.Finalizers = []string{pvcProtection}
		}
		return pvc
	}
	pod := func(name string, phase corev1.PodPhase, deleting bool, claims ...string) corev1.Pod {
		p := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status:     corev1.PodStatus{Phase: phase},
		}
		if deleting {
			p.DeletionTimestamp = &now
		}
		for _, c := range claims {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: c},
				},
			})
		}
		return p
	}
	pvcs := []corev1.PersistentVolumeClaim{
		claim("backup", true), claim("data-0", true), claim("data-1", false), claim("data-2", true),
	}

	require.Equal(t, []string{"backup", "data-0"}, terminatingClaimsInUse([]corev1.Pod{
		pod("p0", corev1.PodRunning, false, "data-0", "backup"),
		pod("p1", corev1.PodPending, false, "data-1", "backup"),
	}, pvcs, now.Time))

	// pvc-protection counts a pod object in any phase. A pod already being
	// deleted is not counted: the operator can no longer keep it.
	deleting := []corev1.Pod{
		pod("p2", corev1.PodFailed, false, "data-2"),
		pod("p3", corev1.PodRunning, true, "backup"),
		pod("p4", corev1.PodUnknown, false, "data-1"),
	}
	require.Equal(t, []string{"data-2"}, terminatingClaimsInUse(deleting, pvcs, now.Time))

	// Stuck past its deletion deadline, the pod is what holds the PVC again.
	require.Equal(t, []string{"data-2"},
		terminatingClaimsInUse(deleting, pvcs, now.Add(stuckDeletionSlack-time.Second)))
	require.Equal(t, []string{"backup", "data-2"},
		terminatingClaimsInUse(deleting, pvcs, now.Add(stuckDeletionSlack+time.Second)))

	require.Empty(t, terminatingClaimsInUse(nil, pvcs, now.Time))
}

func TestMountedTerminatingPVCsSkipsPodListWithoutDeletions(t *testing.T) {
	podLists := 0
	f := newPoolFixture(t, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				podLists++
			}
			return c.List(ctx, list, opts...)
		},
	})
	got, err := mountedTerminatingPVCs(t.Context(), f.client, f.shard)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Zero(t, podLists)

	deleteHeld(t, f.client, f.backup)
	got, err = mountedTerminatingPVCs(t.Context(), f.client, f.shard)
	require.NoError(t, err)
	require.Equal(t, []string{f.backup.Name}, got)
}

// The condition tracks the set; events fire on a change of it only, since the
// recorder rate-limits per object.
func TestSetPVCTerminatingCondition(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	r := &ShardReconciler{Recorder: recorder}
	shard := newTestShard()
	cond := func() *metav1.Condition {
		return meta.FindStatusCondition(
			shard.Status.Conditions,
			multigresv1alpha1.ConditionPVCTerminating,
		)
	}

	require.False(t, r.setPVCTerminatingCondition(shard, nil))
	require.Equal(t, metav1.ConditionFalse, cond().Status)
	require.Empty(t, drainEvents(recorder), "nothing to report")

	require.True(t, r.setPVCTerminatingCondition(shard, []string{"backup"}))
	require.Equal(t, metav1.ConditionTrue, cond().Status)
	require.Contains(t, cond().Message, "backup")
	events := drainEvents(recorder)
	require.Len(t, events, 1)
	require.True(
		t,
		strings.HasPrefix(events[0], "Warning "+reasonMountedPVCBeingDeleted),
		events[0],
	)

	require.True(t, r.setPVCTerminatingCondition(shard, []string{"backup"}))
	require.Empty(t, drainEvents(recorder), "unchanged set")

	require.True(t, r.setPVCTerminatingCondition(shard, []string{"backup", "data-0"}))
	events = drainEvents(recorder)
	require.Len(t, events, 1)
	require.Contains(t, events[0], "data-0")

	require.False(t, r.setPVCTerminatingCondition(shard, nil))
	require.Equal(t, metav1.ConditionFalse, cond().Status)
	events = drainEvents(recorder)
	require.Len(t, events, 1)
	require.True(t, strings.HasPrefix(events[0], "Warning MountedPVCReleased"), events[0])
	require.Contains(t, events[0], "data-0", "the release names what was released")

	require.False(t, r.setPVCTerminatingCondition(shard, nil))
	require.Empty(t, drainEvents(recorder))
}

func TestReconcileSharedBackupPVC_ReclaimsOrphan(t *testing.T) {
	f := newPoolFixture(t)
	f.backup.Labels[metadata.LabelOrphan] = "2026-01-01T00-00-00Z"
	require.NoError(t, f.client.Update(t.Context(), f.backup))

	require.NoError(t, f.reconciler.reconcileSharedBackupPVC(t.Context(), f.shard))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.backup), got))
	require.False(t, pvcutil.HasOrphanLabel(got))
}

// multigres-gc's deletion commits between the reconcile's read of the backup
// PVC and its clearing patch.
func TestReconcileSharedBackupPVC_DeletionAfterReadFailsReclaim(t *testing.T) {
	var f *poolFixture
	deleted := false
	f = newPoolFixture(t, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if obj.GetName() == f.backup.Name && patch.Type() == "application/merge-patch+json" &&
				!deleted {
				deleted = true
				live := &corev1.PersistentVolumeClaim{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), live))
				require.NoError(t, c.Delete(ctx, live, client.Preconditions{
					ResourceVersion: &live.ResourceVersion,
				}))
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	f.backup.Labels[metadata.LabelOrphan] = "2026-01-01T00-00-00Z"
	require.NoError(t, f.client.Update(t.Context(), f.backup))

	err := f.reconciler.reconcileSharedBackupPVC(t.Context(), f.shard)
	require.True(t, apierrors.IsConflict(err), "want Conflict, got %v", err)
	require.True(t, deleted)

	stored := &corev1.PersistentVolumeClaim{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.backup), stored))
	require.True(t, pvcutil.IsTerminating(stored))
	require.True(t, pvcutil.HasOrphanLabel(stored), "the deleted PVC is not reclaimed")

	// The next reconcile sees the deletion and writes nothing.
	require.NoError(t, f.reconciler.reconcileSharedBackupPVC(t.Context(), f.shard))
	after := &corev1.PersistentVolumeClaim{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.backup), after))
	require.Equal(t, stored.ResourceVersion, after.ResourceVersion)
}

func TestCreateMissingResources_NoPodOnTerminatingPVC(t *testing.T) {
	for _, tc := range []struct {
		name   string
		delete func(*poolFixture) client.Object
	}{
		{"data PVC", func(f *poolFixture) client.Object { return f.data }},
		{"backup PVC", func(f *poolFixture) client.Object { return f.backup }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPoolFixture(t)
			require.NoError(t, f.client.Delete(t.Context(), f.pod))
			deleteHeld(t, f.client, tc.delete(f))
			existing := &corev1.PersistentVolumeClaim{}
			require.NoError(
				t,
				f.client.Get(t.Context(), client.ObjectKeyFromObject(f.data), existing),
			)

			_, _, err := f.reconciler.createMissingResources(
				t.Context(), f.shard, "main", "z1", f.pool,
				map[string]*corev1.Pod{},
				map[string]*corev1.PersistentVolumeClaim{f.data.Name: existing},
				1,
			)
			require.NoError(t, err)
			err = f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), &corev1.Pod{})
			require.True(t, apierrors.IsNotFound(err), "no pod is created: %v", err)
		})
	}

	t.Run("baseline creates the pod", func(t *testing.T) {
		f := newPoolFixture(t)
		require.NoError(t, f.client.Delete(t.Context(), f.pod))
		_, _, err := f.reconciler.createMissingResources(
			t.Context(), f.shard, "main", "z1", f.pool,
			map[string]*corev1.Pod{},
			map[string]*corev1.PersistentVolumeClaim{f.data.Name: f.data},
			1,
		)
		require.NoError(t, err)
		require.NoError(
			t,
			f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), &corev1.Pod{}),
		)
	})
}

// canStartDisruption refuses every voluntary disruption while a shard pod
// mounts a PVC being deleted, whatever the cohort's health.
func TestCanStartDisruptionRefusesWhileMountedPVCTerminating(t *testing.T) {
	r, shard, groups, _, _ := fourToTwoFixture(t)
	filesystemBackups(shard)
	target := groups["b"][BuildPoolPodName(shard, "main", "b", 1)]
	allowed := func() (bool, *shardRolloutTracker) {
		t.Helper()
		tracker := &shardRolloutTracker{}
		ok, err := r.canStartDisruption(t.Context(), shard, target, tracker)
		require.NoError(t, err)
		return ok, tracker
	}
	ok, _ := allowed()
	require.True(t, ok, "the fixture allows a disruption to start")

	backup, err := BuildSharedBackupPVC(shard, true, r.Scheme)
	require.NoError(t, err)
	require.NoError(t, r.Create(t.Context(), protected(backup)))
	mountBackup := func(pod *corev1.Pod) {
		live := &corev1.Pod{}
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(pod), live))
		live.Spec.Volumes = append(live.Spec.Volumes, corev1.Volume{
			Name: "backup",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: backup.Name,
				},
			},
		})
		require.NoError(t, r.Update(t.Context(), live))
	}
	other := groups["a"][BuildPoolPodName(shard, "main", "a", 0)]
	mountBackup(other)
	ok, _ = allowed()
	require.True(t, ok, "a PVC in use but not being deleted does not block")

	deleteHeld(t, r.Client, backup)
	ok, tracker := allowed()
	require.False(t, ok, "a pod other than the target holds a PVC being deleted")
	require.True(t, tracker.waitingForRecovery, "the shard keeps checking")

	// A pod object holds its PVCs in any phase.
	live := &corev1.Pod{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(other), live))
	live.Status.Phase = corev1.PodFailed
	require.NoError(t, r.Status().Update(t.Context(), live))
	ok, _ = allowed()
	require.False(t, ok, "a Failed pod still holds the PVC")

	// Once the PVC has finished deleting there is nothing left to protect.
	live.Status.Phase = corev1.PodRunning
	require.NoError(t, r.Status().Update(t.Context(), live))
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(backup), backup))
	backup.Finalizers = nil
	require.NoError(t, r.Update(t.Context(), backup))
	ok, _ = allowed()
	require.True(t, ok)
}

func TestEnsureLitePodLeavesPodOnTerminatingPVC(t *testing.T) {
	for _, which := range []string{"data", "backup"} {
		t.Run(which, func(t *testing.T) {
			shard, pool := filesystemBackups(newTestLiteShard()), newTestPoolSpec()
			scheme := reloadTestScheme(t)
			pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
			require.NoError(t, err)
			pod.UID = "original-pod"
			pod.Annotations[metadata.AnnotationSpecHash] = "drifted"
			data, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
			require.NoError(t, err)
			backup, err := BuildSharedBackupPVC(shard, false, scheme)
			require.NoError(t, err)
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(pod, protected(data), protected(backup)).Build()
			r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

			target := map[string]*corev1.PersistentVolumeClaim{"data": data, "backup": backup}[which]
			deleteHeld(t, c, target)
			require.NoError(t, r.ensureLitePod(t.Context(), shard, "main", "z1", pool))
			current := &corev1.Pod{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pod), current))
			require.Equal(t, pod.UID, current.UID, "a drifted pod is not replaced")

			// Nor is a missing one created.
			require.NoError(t, c.Delete(t.Context(), current))
			require.NoError(t, r.ensureLitePod(t.Context(), shard, "main", "z1", pool))
			err = c.Get(t.Context(), client.ObjectKeyFromObject(pod), current)
			require.True(t, apierrors.IsNotFound(err), "no pod is created: %v", err)
		})
	}

	t.Run("baseline replaces the drifted pod", func(t *testing.T) {
		shard, pool := newTestLiteShard(), newTestPoolSpec()
		scheme := reloadTestScheme(t)
		pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
		require.NoError(t, err)
		pod.UID = "original-pod"
		pod.Annotations[metadata.AnnotationSpecHash] = "drifted"
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
		require.NoError(t, r.ensureLitePod(t.Context(), shard, "main", "z1", pool))
		err = c.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
		require.True(t, apierrors.IsNotFound(err), "drifted pod deleted: %v", err)
	})
}

func TestEnsureLiteDataPVCWaitsOutDeletion(t *testing.T) {
	shard, pool := newTestLiteShard(), newTestPoolSpec()
	scheme := reloadTestScheme(t)
	data, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
	require.NoError(t, err)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(protected(data)).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	deleteHeld(t, c, data)
	before := data.ResourceVersion

	pool.Storage.Size = "20Gi"
	_, err = r.ensureLiteDataPVC(t.Context(), shard, "main", "z1", pool)
	require.NoError(t, err)
	after := &corev1.PersistentVolumeClaim{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(data), after))
	require.Equal(t, before, after.ResourceVersion, "a PVC being deleted is not expanded")
}

func TestMaintenanceSurgeNotStartedOnTerminatingPVC(t *testing.T) {
	for _, which := range []string{"surge data PVC", "backup PVC"} {
		t.Run(which, func(t *testing.T) {
			f := newPoolFixture(t)
			// The surge takes the next free index, 1 for one replica.
			surgeData, err := BuildPoolDataPVC(f.shard, "main", "z1", f.pool, 1, true, f.scheme)
			require.NoError(t, err)
			require.NoError(t, f.client.Create(t.Context(), protected(surgeData)))
			target := map[string]client.Object{"surge data PVC": surgeData, "backup PVC": f.backup}[which]
			deleteHeld(t, f.client, target)
			live := &corev1.PersistentVolumeClaim{}
			require.NoError(
				t,
				f.client.Get(t.Context(), client.ObjectKeyFromObject(surgeData), live),
			)

			require.NoError(t, f.reconciler.createOrAdoptMaintenanceSurge(
				t.Context(), f.shard, "main", "z1", f.pool,
				map[string]*corev1.Pod{f.pod.Name: f.pod},
				map[string]*corev1.PersistentVolumeClaim{f.data.Name: f.data, surgeData.Name: live},
				1,
			))
			surgePod := client.ObjectKey{
				Namespace: f.shard.Namespace, Name: BuildPoolPodName(f.shard, "main", "z1", 1),
			}
			err = f.client.Get(t.Context(), surgePod, &corev1.Pod{})
			require.True(t, apierrors.IsNotFound(err), "no surge pod is created: %v", err)
		})
	}
}

// The guard reads the running pod's own volumes, not the desired pod's: a
// spec change that drops a volume must not delete the pod holding it.
func TestEnsureLitePodChecksExistingPodVolumes(t *testing.T) {
	shard, pool := filesystemBackups(newTestLiteShard()), newTestPoolSpec()
	scheme := reloadTestScheme(t)
	pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
	require.NoError(t, err)
	pod.UID = "original-pod"
	data, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
	require.NoError(t, err)
	backup, err := BuildSharedBackupPVC(shard, false, scheme)
	require.NoError(t, err)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pod, protected(data), protected(backup)).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}
	deleteHeld(t, c, backup)

	shard.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
		S3: &multigresv1alpha1.S3BackupConfig{
			Bucket:            "backups",
			CredentialsSecret: "s3-credentials",
		},
	}
	desired, err := BuildLitePod(shard, "main", "z1", pool, scheme)
	require.NoError(t, err)
	require.NotEqual(t, pod.Annotations[metadata.AnnotationSpecHash],
		desired.Annotations[metadata.AnnotationSpecHash], "the switch to S3 drifts the pod")

	require.NoError(t, r.ensureLitePod(t.Context(), shard, "main", "z1", pool))
	current := &corev1.Pod{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pod), current))
	require.Equal(t, pod.UID, current.UID)
}

// A drain that finished before the PVC began deleting stops short of deleting
// its pod.
func TestHandleScaleDownKeepsDrainedPodOnTerminatingPVC(t *testing.T) {
	for _, which := range []string{"data", "backup"} {
		t.Run(which, func(t *testing.T) {
			f := newPoolFixture(t)
			deleteHeld(
				t,
				f.client,
				map[string]client.Object{"data": f.data, "backup": f.backup}[which],
			)
			f.pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateReadyForDeletion
			require.NoError(t, f.client.Update(t.Context(), f.pod))

			_, inProgress, err := f.reconciler.handleScaleDown(
				t.Context(), f.shard, "main", f.pool,
				map[string]*corev1.Pod{f.pod.Name: f.pod}, 1, 1, false,
			)
			require.NoError(t, err)
			require.True(t, inProgress)
			current := &corev1.Pod{}
			require.NoError(
				t,
				f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), current),
			)
			stored := &corev1.PersistentVolumeClaim{}
			require.NoError(
				t,
				f.client.Get(t.Context(), client.ObjectKeyFromObject(f.data), stored),
			)
			require.False(
				t,
				pvcutil.HasOrphanLabel(stored),
				"the drained pod's PVC is not orphaned either",
			)
		})
	}

	t.Run("baseline deletes the drained pod", func(t *testing.T) {
		f := newPoolFixture(t)
		f.pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateReadyForDeletion
		require.NoError(t, f.client.Update(t.Context(), f.pod))
		_, _, err := f.reconciler.handleScaleDown(
			t.Context(), f.shard, "main", f.pool,
			map[string]*corev1.Pod{f.pod.Name: f.pod}, 1, 1, false,
		)
		require.NoError(t, err)
		err = f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), &corev1.Pod{})
		require.True(t, apierrors.IsNotFound(err), "drained pod deleted: %v", err)
	})
}

// A Failed pod still holds its PVCs, so it is not replaced while one is being
// deleted.
func TestCreateMissingResourcesKeepsTerminalPodOnTerminatingPVC(t *testing.T) {
	for _, terminating := range []bool{true, false} {
		t.Run(fmt.Sprintf("terminating=%v", terminating), func(t *testing.T) {
			f := newPoolFixture(t)
			if terminating {
				deleteHeld(t, f.client, f.backup)
			}
			f.pod.Status.Phase = corev1.PodFailed
			require.NoError(t, f.client.Status().Update(t.Context(), f.pod))

			_, _, err := f.reconciler.createMissingResources(
				t.Context(), f.shard, "main", "z1", f.pool,
				map[string]*corev1.Pod{f.pod.Name: f.pod},
				map[string]*corev1.PersistentVolumeClaim{f.data.Name: f.data},
				1,
			)
			require.NoError(t, err)
			err = f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), &corev1.Pod{})
			if terminating {
				require.NoError(t, err, "the Failed pod is kept")
			} else {
				require.True(t, apierrors.IsNotFound(err), "the Failed pod is replaced: %v", err)
			}
		})
	}
}

func TestEnqueueShardForPVC(t *testing.T) {
	f := newPoolFixture(t)
	other := newTestShard()
	other.Name = "other-shard"
	other.Spec.ShardName = "other"
	f.shard.Labels = shardLabelsForTest(f.shard)
	other.Labels = shardLabelsForTest(other)
	require.NoError(t, f.client.Update(t.Context(), f.shard))
	require.NoError(t, f.client.Create(t.Context(), other))

	for _, pvc := range []*corev1.PersistentVolumeClaim{f.data, f.backup} {
		reqs := f.reconciler.enqueueShardForPVC(t.Context(), pvc)
		require.Len(t, reqs, 1, pvc.Name)
		require.Equal(t, f.shard.Name, reqs[0].Name)
	}
	require.Empty(t, f.reconciler.enqueueShardForPVC(t.Context(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: f.shard.Namespace},
	}))
}

// shardLabelsForTest are the labels the TableGroup controller puts on a Shard.
func shardLabelsForTest(shard *multigresv1alpha1.Shard) map[string]string {
	cluster := shard.Labels[metadata.LabelMultigresCluster]
	labels := metadata.BuildStandardLabels(cluster, metadata.ComponentShard)
	metadata.AddClusterLabel(labels, cluster)
	metadata.AddDatabaseLabel(labels, shard.Spec.DatabaseName)
	metadata.AddTableGroupLabel(labels, shard.Spec.TableGroupName)
	metadata.AddShardLabel(labels, shard.Spec.ShardName)
	return labels
}

// A scaled-down pod halted short of deletion still has its own PVC released
// by policy, so the PVC is not stranded once a person deletes the pod.
func TestHandleScaleDownReleasesHaltedPodPVC(t *testing.T) {
	f := newPoolFixture(t)
	deleteHeld(t, f.client, f.backup)
	f.pod.Annotations[metadata.AnnotationDrainState] = metadata.DrainStateReadyForDeletion
	require.NoError(t, f.client.Update(t.Context(), f.pod))

	// Scaled to zero, so index 0 is past the desired replicas.
	_, inProgress, err := f.reconciler.handleScaleDown(
		t.Context(), f.shard, "main", f.pool,
		map[string]*corev1.Pod{f.pod.Name: f.pod}, 0, 0, false,
	)
	require.NoError(t, err)
	require.True(t, inProgress)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), &corev1.Pod{}))
	stored := &corev1.PersistentVolumeClaim{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.data), stored))
	require.True(t, pvcutil.HasOrphanLabel(stored), "the scaled-down PVC is marked orphan")
}

// A cache that has not yet seen a deletion offers an orphan-labelled PVC for
// reclaim; the uncached re-read finds it being deleted, which is reported as
// such rather than as an error.
func TestReclaimPVCFindsDeletionOnFreshRead(t *testing.T) {
	f := newPoolFixture(t)
	f.data.Labels[metadata.LabelOrphan] = "2026-01-01T00-00-00Z"
	require.NoError(t, f.client.Update(t.Context(), f.data))
	stale := f.data.DeepCopy()
	staleForPool := f.data.DeepCopy()
	deleteHeld(t, f.client, f.data)

	terminating, err := f.reconciler.reclaimPVC(t.Context(), stale)
	require.NoError(t, err)
	require.True(t, terminating)
	require.True(t, pvcutil.IsTerminating(stale), "the caller's copy is refreshed")

	_, _, err = f.reconciler.createMissingResources(
		t.Context(), f.shard, "main", "z1", f.pool,
		map[string]*corev1.Pod{f.pod.Name: f.pod},
		map[string]*corev1.PersistentVolumeClaim{f.data.Name: staleForPool},
		1,
	)
	require.NoError(t, err, "the pool path treats it as a PVC being deleted")
	require.True(t, pvcutil.IsTerminating(staleForPool))
}

// A cache that missed an orphaned PVC sends the pool path through Create,
// which finds it already there. The pod created on it must still clear the
// orphan mark under a fresh resourceVersion, or a multigres-gc Delete
// conditioned on the version it listed before the pod existed would succeed.
func TestCreateMissingResourcesReclaimsPVCTheCacheMissed(t *testing.T) {
	f := newPoolFixture(t)
	require.NoError(t, f.client.Delete(t.Context(), f.pod))
	f.data.Labels[metadata.LabelOrphan] = "2026-01-01T00-00-00Z"
	require.NoError(t, f.client.Update(t.Context(), f.data))
	listedByGC := f.data.DeepCopy()

	_, _, err := f.reconciler.createMissingResources(
		t.Context(), f.shard, "main", "z1", f.pool,
		map[string]*corev1.Pod{},
		map[string]*corev1.PersistentVolumeClaim{},
		1,
	)
	require.NoError(t, err)
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.pod), &corev1.Pod{}))

	stored := &corev1.PersistentVolumeClaim{}
	require.NoError(t, f.client.Get(t.Context(), client.ObjectKeyFromObject(f.data), stored))
	require.False(t, pvcutil.HasOrphanLabel(stored))
	err = f.client.Delete(t.Context(), listedByGC,
		client.Preconditions{ResourceVersion: &listedByGC.ResourceVersion})
	require.True(t, apierrors.IsConflict(err), "GC's Delete must fail, got %v", err)
}

// A backup PVC being deleted is not applied over, orphan mark or not.
func TestReconcileSharedBackupPVC_LeavesTerminatingPVCAlone(t *testing.T) {
	var f *poolFixture
	applies := 0
	f = newPoolFixture(t, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if obj.GetName() == f.backup.Name && patch.Type() == types.ApplyPatchType {
				applies++
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	require.NoError(t, f.reconciler.reconcileSharedBackupPVC(t.Context(), f.shard))
	require.Equal(t, 1, applies, "baseline: a live backup PVC is applied")

	deleteHeld(t, f.client, f.backup)
	require.NoError(t, f.reconciler.reconcileSharedBackupPVC(t.Context(), f.shard))
	require.Equal(t, 1, applies, "a backup PVC being deleted is not applied")
}
