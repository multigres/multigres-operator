package shard

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// refusePVCSpecChanges stands in for the API server's PVC update validation
// and the PersistentVolumeClaimResize admission plugin, neither of which the
// fake client implements: a write that would lower an existing PVC's storage
// request, raise it on a PVC that is unbound or whose class does not allow
// expansion, or change its storage class or access modes, is refused the way
// the real server refuses it.
func refusePVCSpecChanges(t *testing.T) interceptor.Funcs {
	t.Helper()
	return interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			c client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			desired, ok := obj.(*corev1.PersistentVolumeClaim)
			if !ok {
				return c.Patch(ctx, obj, patch, opts...)
			}
			current := &corev1.PersistentVolumeClaim{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
				if apierrors.IsNotFound(err) {
					return c.Patch(ctx, obj, patch, opts...)
				}
				return err
			}
			var errs field.ErrorList
			requestPath := field.NewPath("spec", "resources", "requests", "storage")
			want := desired.Spec.Resources.Requests[corev1.ResourceStorage]
			switch want.Cmp(current.Spec.Resources.Requests[corev1.ResourceStorage]) {
			case -1:
				errs = append(errs, field.Forbidden(
					requestPath,
					"field can not be less than status.capacity",
				))
			case 1:
				sc := &storagev1.StorageClass{}
				className := ptr.Deref(current.Spec.StorageClassName, "")
				if current.Status.Phase != corev1.ClaimBound || className == "" ||
					c.Get(ctx, client.ObjectKey{Name: className}, sc) != nil ||
					!ptr.Deref(sc.AllowVolumeExpansion, false) {
					return apierrors.NewForbidden(
						schema.GroupResource{Resource: "persistentvolumeclaims"},
						obj.GetName(),
						fmt.Errorf("only dynamically provisioned pvc can be resized and "+
							"the storageclass that provisions the pvc must support resize"),
					)
				}
			}
			if !ptr.Equal(desired.Spec.StorageClassName, current.Spec.StorageClassName) ||
				fmt.Sprint(desired.Spec.AccessModes) != fmt.Sprint(current.Spec.AccessModes) {
				errs = append(errs, field.Forbidden(field.NewPath("spec"), "spec is immutable"))
			}
			if len(errs) > 0 {
				return apierrors.NewInvalid(
					schema.GroupKind{Kind: "PersistentVolumeClaim"},
					obj.GetName(),
					errs,
				)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
}

// expandableStorageClass is a StorageClass the API server lets a PVC grow on.
func expandableStorageClass(name string) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: name},
		Provisioner:          "test",
		AllowVolumeExpansion: ptr.To(true),
	}
}

// makeExpandable binds pvc and puts it on an expandable StorageClass, the
// preconditions the API server checks before it accepts a larger request.
// It returns the class for the fake client to hold.
func makeExpandable(pvc *corev1.PersistentVolumeClaim) *storagev1.StorageClass {
	pvc.Spec.StorageClassName = ptr.To("expandable")
	pvc.Status.Phase = corev1.ClaimBound
	return expandableStorageClass("expandable")
}

// storageTestShard is a filesystem-backup shard with one pool, plus the
// backup and data PVCs the production builders give it, bound at the size
// they were built with.
func storageTestShard(t *testing.T) (
	*multigresv1alpha1.Shard,
	*corev1.PersistentVolumeClaim,
	*corev1.PersistentVolumeClaim,
) {
	t.Helper()
	shard := newTestShard()
	pool := newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	pool.Storage.Class = "data-class"
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	shard.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
		Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
			Storage: multigresv1alpha1.StorageSpec{Size: "10Gi", Class: "backup-class"},
		},
	}
	backup, err := BuildSharedBackupPVC(shard, false, nil)
	require.NoError(t, err)
	ownedBy(backup, `"f:storageClassName":{},"f:accessModes":{}`)
	data, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, nil)
	require.NoError(t, err)
	for _, pvc := range []*corev1.PersistentVolumeClaim{backup, data} {
		pvc.Status.Phase = corev1.ClaimBound
		pvc.Status.Capacity = pvc.Spec.Resources.Requests.DeepCopy()
	}
	return shard, backup, data
}

func storageTestReconciler(
	t *testing.T,
	objs ...client.Object,
) (*ShardReconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := reloadTestScheme(t)
	recorder := record.NewFakeRecorder(100)
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithReturnManagedFields().
		WithObjects(append(objs,
			expandableStorageClass("backup-class"),
			expandableStorageClass("data-class"),
			&storagev1.StorageClass{
				ObjectMeta:  metav1.ObjectMeta{Name: "fixed-class"},
				Provisioner: "test",
			},
		)...).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()
	return &ShardReconciler{
		Client:   interceptor.NewClient(base, refusePVCSpecChanges(t)),
		Scheme:   scheme,
		Recorder: recorder,
	}, recorder
}

func TestReconcileSharedBackupPVC_KeepsUnchangeableFields(t *testing.T) {
	shard, backup, _ := storageTestShard(t)
	backup.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
	r, _ := storageTestReconciler(t, shard, backup)

	// Every field the API server refuses to change, changed at once, together
	// with a label the operator does own so the apply has something to write.
	shard.Spec.SidecarBackupLocation.Filesystem.Storage = multigresv1alpha1.StorageSpec{
		Size:        "5Gi",
		Class:       "other-class",
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
	}
	require.NoError(t, r.reconcileSharedBackupPVC(t.Context(), shard))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(backup), got))
	require.Equal(t, "20Gi", ptr.To(got.Spec.Resources.Requests[corev1.ResourceStorage]).String())
	require.Equal(t, ptr.To("backup-class"), got.Spec.StorageClassName)
	require.Equal(
		t,
		[]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		got.Spec.AccessModes,
	)
}

func TestReconcileSharedBackupPVC_Grows(t *testing.T) {
	shard, backup, _ := storageTestShard(t)
	r, _ := storageTestReconciler(t, shard, backup)

	shard.Spec.SidecarBackupLocation.Filesystem.Storage.Size = "30Gi"
	require.NoError(t, r.reconcileSharedBackupPVC(t.Context(), shard))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(backup), got))
	require.Equal(t, "30Gi", ptr.To(got.Spec.Resources.Requests[corev1.ResourceStorage]).String())
}

func TestReconcileSharedBackupPVC_KeepsRequestWhenGrowthRefused(t *testing.T) {
	for name, mutate := range map[string]func(*corev1.PersistentVolumeClaim){
		"class does not allow expansion": func(pvc *corev1.PersistentVolumeClaim) {
			pvc.Spec.StorageClassName = ptr.To("fixed-class")
		},
		"class does not exist": func(pvc *corev1.PersistentVolumeClaim) {
			pvc.Spec.StorageClassName = ptr.To("missing-class")
		},
		"no class": func(pvc *corev1.PersistentVolumeClaim) {
			pvc.Spec.StorageClassName = nil
		},
		"not bound": func(pvc *corev1.PersistentVolumeClaim) {
			pvc.Status.Phase = corev1.ClaimPending
		},
	} {
		t.Run(name, func(t *testing.T) {
			shard, backup, _ := storageTestShard(t)
			mutate(backup)
			r, _ := storageTestReconciler(t, shard, backup)

			// The class is left at the PVC's own so the only refused field
			// is the request.
			shard.Spec.SidecarBackupLocation.Filesystem.Storage.Size = "30Gi"
			shard.Spec.SidecarBackupLocation.Filesystem.Storage.Class = ptr.Deref(
				backup.Spec.StorageClassName,
				"",
			)
			require.NoError(t, r.reconcileSharedBackupPVC(t.Context(), shard))

			got := &corev1.PersistentVolumeClaim{}
			require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(backup), got))
			require.Equal(
				t,
				"10Gi",
				ptr.To(got.Spec.Resources.Requests[corev1.ResourceStorage]).String(),
			)
			cond := storageSpecIgnored(t, r, shard)
			require.Equal(t, metav1.ConditionTrue, cond.Status)
			require.Contains(t, cond.Message,
				backup.Name+" keeps storage request 10Gi, spec asks for 30Gi")
		})
	}
}

func TestBackupStorageClassGated(t *testing.T) {
	shard, backup, _ := storageTestShard(t)
	check := storageClassCheck{
		backupDependency: &missingStorageClassDependencyError{className: "missing"},
	}

	r, _ := storageTestReconciler(t, shard)
	gated, err := r.backupStorageClassGated(t.Context(), shard, check)
	require.NoError(t, err)
	require.True(t, gated, "a missing class must stop the backup PVC being created")

	r, _ = storageTestReconciler(t, shard, backup)
	gated, err = r.backupStorageClassGated(t.Context(), shard, check)
	require.NoError(t, err)
	require.False(
		t,
		gated,
		"an existing backup PVC keeps its class, so nothing waits on the new one",
	)

	gated, err = r.backupStorageClassGated(t.Context(), shard, storageClassCheck{})
	require.NoError(t, err)
	require.False(t, gated)
}

func TestReconcileSharedBackupPVC_CreatesFromSpec(t *testing.T) {
	shard, backup, _ := storageTestShard(t)
	r, _ := storageTestReconciler(t, shard)

	shard.Spec.SidecarBackupLocation.Filesystem.Storage.Size = "5Gi"
	require.NoError(t, r.reconcileSharedBackupPVC(t.Context(), shard))

	got := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(backup), got))
	require.Equal(t, "5Gi", ptr.To(got.Spec.Resources.Requests[corev1.ResourceStorage]).String())
	require.Equal(t, ptr.To("backup-class"), got.Spec.StorageClassName)
}

func storageSpecIgnored(
	t *testing.T,
	r *ShardReconciler,
	shard *multigresv1alpha1.Shard,
) *metav1.Condition {
	t.Helper()
	require.NoError(t, r.setStorageSpecIgnoredCondition(t.Context(), shard))
	cond := meta.FindStatusCondition(
		shard.Status.Conditions,
		multigresv1alpha1.ConditionStorageSpecIgnored,
	)
	require.NotNil(t, cond)
	require.Equal(t, shard.Generation, cond.ObservedGeneration)
	return cond
}

func TestStorageSpecIgnoredCondition(t *testing.T) {
	shard, backup, data := storageTestShard(t)
	shard.Generation = 3
	r, recorder := storageTestReconciler(t, shard, backup, data)

	cond := storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, storageSpecAppliedReason, cond.Reason)
	require.Empty(t, drainEvents(recorder))

	// Growth is applied, not ignored, whether or not it has landed yet.
	shard.Spec.SidecarBackupLocation.Filesystem.Storage.Size = "20Gi"
	require.Equal(t, metav1.ConditionFalse, storageSpecIgnored(t, r, shard).Status)

	// A shrink of both PVCs plus a class change on the data PVC, in one edit.
	shard.Spec.SidecarBackupLocation.Filesystem.Storage.Size = "5Gi"
	pool := shard.Spec.Pools["main"]
	pool.Storage.Size = "1Gi"
	pool.Storage.Class = "faster"
	shard.Spec.Pools["main"] = pool
	cond = storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Equal(t, storageSpecIgnoredReason, cond.Reason)
	for _, want := range []string{
		backup.Name + " keeps storage request 10Gi, spec asks for 5Gi (a PVC cannot shrink)",
		data.Name + " keeps storage request 10Gi, spec asks for 1Gi (a PVC cannot shrink)",
		data.Name + ` keeps storageClassName "data-class", spec asks for "faster"`,
	} {
		require.Contains(t, cond.Message, want)
	}
	events := drainEvents(recorder)
	require.Len(t, events, 1, "one Warning on the transition to True")
	require.True(t, strings.HasPrefix(events[0], "Warning StorageSpecIgnored "), events[0])

	// Still ignored on the next pass: no second event.
	require.Equal(t, metav1.ConditionTrue, storageSpecIgnored(t, r, shard).Status)
	require.Empty(t, drainEvents(recorder))

	// The return trip.
	shard.Spec.SidecarBackupLocation.Filesystem.Storage.Size = "10Gi"
	pool.Storage.Size = "10Gi"
	pool.Storage.Class = "data-class"
	shard.Spec.Pools["main"] = pool
	require.Equal(t, metav1.ConditionFalse, storageSpecIgnored(t, r, shard).Status)
	require.Empty(t, drainEvents(recorder))
}

func TestStorageSpecIgnoredCondition_SkipsPVCsOutOfService(t *testing.T) {
	shard, backup, data := storageTestShard(t)

	orphan := data.DeepCopy()
	orphan.Name = BuildPoolDataPVCName(shard, "main", "z1", 1)
	orphan.Labels[metadata.LabelOrphan] = "2026-01-01T00-00-00Z"

	removedPool := data.DeepCopy()
	removedPool.Name = BuildPoolDataPVCName(shard, "gone", "z1", 0)
	removedPool.Labels[metadata.LabelMultigresPool] = "gone"

	r, _ := storageTestReconciler(t, shard, backup, data, orphan, removedPool)

	// S3 backups have no shared PVC, so the one left behind by an earlier
	// filesystem config is not compared.
	shard.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
	}
	cond := storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionFalse, cond.Status, cond.Message)

	// The orphan and the removed pool's PVC are larger than the live pool now
	// asks for, and still only the in-service PVC is named.
	pool := shard.Spec.Pools["main"]
	pool.Storage.Size = "1Gi"
	shard.Spec.Pools["main"] = pool
	cond = storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Contains(t, cond.Message, data.Name)
	require.NotContains(t, cond.Message, orphan.Name)
	require.NotContains(t, cond.Message, removedPool.Name)
	require.NotContains(t, cond.Message, backup.Name)
}

func TestStorageSpecIgnoredCondition_BoundsMessage(t *testing.T) {
	shard, _, _ := storageTestShard(t)
	shard.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeS3,
	}
	pool := shard.Spec.Pools["main"]
	objs := []client.Object{shard}
	for i := range maxIgnoredStorageDetails + 3 {
		pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, i, false, nil)
		require.NoError(t, err)
		objs = append(objs, pvc)
	}
	r, _ := storageTestReconciler(t, objs...)

	pool.Storage.Size = "1Gi"
	shard.Spec.Pools["main"] = pool
	cond := storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Equal(t, maxIgnoredStorageDetails, strings.Count(cond.Message, " keeps "))
	require.True(t, strings.HasSuffix(cond.Message, "; and 3 more"), cond.Message)
}

func TestStorageSpecIgnoredCondition_BoundsMessageBytes(t *testing.T) {
	shard, backup, data := storageTestShard(t)
	r, _ := storageTestReconciler(t, shard, backup, data)

	// Access modes are an unbounded string list in the CRD, and the detail
	// quotes them.
	pool := shard.Spec.Pools["main"]
	pool.Storage.AccessModes = []corev1.PersistentVolumeAccessMode{
		corev1.PersistentVolumeAccessMode(strings.Repeat("x", 40000)),
	}
	shard.Spec.Pools["main"] = pool
	cond := storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.LessOrEqual(t, len(cond.Message), maxIgnoredStorageMessage)
	require.True(t, strings.HasSuffix(cond.Message, " (truncated)"))
}

func TestStorageSpecIgnoredCondition_ReportsUnparseableSize(t *testing.T) {
	shard, backup, data := storageTestShard(t)
	r, _ := storageTestReconciler(t, shard, backup, data)

	pool := shard.Spec.Pools["main"]
	pool.Storage.Size = "10Xi"
	shard.Spec.Pools["main"] = pool
	cond := storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Contains(t, cond.Message, data.Name+`: invalid storage size "10Xi"`)
}

func TestStorageSpecIgnoredCondition_AccessModeOrder(t *testing.T) {
	shard, backup, data := storageTestShard(t)
	data.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{
		corev1.ReadWriteOnce, corev1.ReadOnlyMany,
	}
	r, _ := storageTestReconciler(t, shard, backup, data)

	pool := shard.Spec.Pools["main"]
	pool.Storage.AccessModes = []corev1.PersistentVolumeAccessMode{
		corev1.ReadOnlyMany, corev1.ReadWriteOnce,
	}
	shard.Spec.Pools["main"] = pool
	cond := storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionFalse, cond.Status, cond.Message)

	pool.Storage.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	shard.Spec.Pools["main"] = pool
	cond = storageSpecIgnored(t, r, shard)
	require.Equal(t, metav1.ConditionTrue, cond.Status)
	require.Contains(t, cond.Message, data.Name+" keeps accessModes")
}

func TestEnqueueFromStorageClass(t *testing.T) {
	// Three shards with the identity labels the builders give their PVCs.
	shardNamed := func(name, shardName string, ignored metav1.ConditionStatus) *multigresv1alpha1.Shard {
		shard := newTestShard()
		shard.Name = name
		shard.Spec.ShardName = multigresv1alpha1.ShardName(shardName)
		shard.Status.Conditions = []metav1.Condition{{
			Type:   multigresv1alpha1.ConditionStorageSpecIgnored,
			Status: ignored,
			Reason: storageSpecIgnoredReason,
			// Deliberately names no class: the match must not depend on the
			// bounded message.
			Message: "and 9 more",
		}}
		return shard
	}
	waiting := shardNamed("waiting", "0-80", metav1.ConditionTrue)
	otherClass := shardNamed("other-class", "80-inf", metav1.ConditionTrue)
	settled := shardNamed("settled", "0-inf", metav1.ConditionFalse)
	// Same identity as waiting in another namespace, on another class.
	twin := shardNamed("waiting", "0-80", metav1.ConditionTrue)
	twin.Namespace = "other"

	objs := []client.Object{waiting, otherClass, settled, twin}
	for _, tc := range []struct {
		shard *multigresv1alpha1.Shard
		class string
	}{{waiting, "fixed"}, {otherClass, "fixed-2"}, {settled, "fixed"}, {twin, "fixed-2"}} {
		pvc, err := BuildSharedBackupPVC(tc.shard, false, nil)
		require.NoError(t, err)
		pvc.Spec.StorageClassName = ptr.To(tc.class)
		objs = append(objs, pvc)
	}
	r, _ := storageTestReconciler(t, objs...)

	requests := r.enqueueFromStorageClass(t.Context(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "fixed"},
	})
	require.Len(t, requests, 1)
	require.Equal(t, client.ObjectKeyFromObject(waiting), requests[0].NamespacedName)

	require.Empty(t, r.enqueueFromStorageClass(t.Context(), &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "unused"},
	}))
}
