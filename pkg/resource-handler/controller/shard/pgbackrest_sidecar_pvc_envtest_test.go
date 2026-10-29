//go:build integration
// +build integration

package shard

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
)

// The fake client models neither server-side-apply ownership nor PVC update
// validation, so a change of backup config that wedges the shared backup PVC is
// invisible to it. This runs the real apply path against an API server with a default
// StorageClass, which is what fills in the class the user never set.
//
// A claim created from the shard-level backup config is built by reconciling a
// shard once: the apply manager and the owned fields are the same whichever
// backup config the claim derives from. The switch is the same shard
// reconciled again with the config the claim now follows.
func TestReconcileSharedBackupPVC_UpgradeAgainstAPIServer(t *testing.T) {
	cfg := testutil.SetUpEnvtest(t)
	scheme := testScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	c := testutil.SetUpClient(t, cfg, scheme)
	ctx := context.Background()

	for _, name := range []string{"std", "fast"} {
		sc := &storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: name},
			Provisioner: "example.com/none",
		}
		if name == "std" {
			sc.Annotations = map[string]string{
				"storageclass.kubernetes.io/is-default-class": "true",
			}
		}
		allow := true
		sc.AllowVolumeExpansion = &allow
		require.NoError(t, c.Create(ctx, sc))
	}

	r := &ShardReconciler{
		Client: c, APIReader: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
	}

	newShard := func(name string, backup *multigresv1alpha1.BackupConfig) *multigresv1alpha1.Shard {
		s := newTestShard()
		s.Name = name
		s.Spec.ShardName = multigresv1alpha1.ShardName(name)
		s.Namespace = "default"
		s.Spec.SidecarBackupLocation = backup
		return s
	}
	fs := func(size, class string) *multigresv1alpha1.BackupConfig {
		return fsLocation(size, class)
	}
	get := func(s *multigresv1alpha1.Shard) *corev1.PersistentVolumeClaim {
		pvc := &corev1.PersistentVolumeClaim{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{
			Name: BuildSharedBackupPVCName(s), Namespace: "default",
		}, pvc))
		return pvc
	}
	// No PV controller runs here, and the API server only accepts a changed
	// request on a bound claim, so bind it by hand.
	bind := func(t *testing.T, pvc *corev1.PersistentVolumeClaim) {
		t.Helper()
		pvc.Spec.VolumeName = "pv-" + pvc.Name
		require.NoError(t, c.Update(ctx, pvc))
		pvc.Status.Phase = corev1.ClaimBound
		pvc.Status.Capacity = pvc.Spec.Resources.Requests.DeepCopy()
		require.NoError(t, c.Status().Update(ctx, pvc))
	}
	// A claim created while no default StorageClass applied has no class. The
	// default marker is lifted for the creation and restored afterwards.
	setDefault := func(t *testing.T, isDefault bool) {
		t.Helper()
		sc := &storagev1.StorageClass{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "std"}, sc))
		sc.Annotations = map[string]string{
			"storageclass.kubernetes.io/is-default-class": fmt.Sprint(isDefault),
		}
		require.NoError(t, c.Update(ctx, sc))
	}
	newClassless := func(t *testing.T, name string) *multigresv1alpha1.Shard {
		t.Helper()
		s := newShard(name, fs("10Gi", ""))
		setDefault(t, false)
		defer setDefault(t, true)
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: BuildSharedBackupPVCName(s), Namespace: "default",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				}},
			},
		}
		require.NoError(t, c.Create(ctx, pvc))
		require.Nil(t, get(s).Spec.StorageClassName, "no default class, so none was filled in")
		return s
	}
	upgrade := func(s *multigresv1alpha1.Shard, loc *multigresv1alpha1.BackupConfig) {
		s.Spec.SidecarBackupLocation = loc
	}
	reconcile := func(t *testing.T, s *multigresv1alpha1.Shard) {
		t.Helper()
		require.NoError(t, r.reconcileSharedBackupPVC(ctx, s))
	}
	storage := func(p *corev1.PersistentVolumeClaim) string {
		q := p.Spec.Resources.Requests[corev1.ResourceStorage]
		return q.String()
	}

	t.Run("defaulted class survives the upgrade", func(t *testing.T) {
		s := newShard("up-default", fs("10Gi", ""))
		reconcile(t, s)
		require.NotNil(t, get(s).Spec.StorageClassName)
		assert.Equal(t, "std", *get(s).Spec.StorageClassName)
		upgrade(s, fs("10Gi", ""))
		reconcile(t, s)
		reconcile(t, s)
		assert.Equal(t, "std", *get(s).Spec.StorageClassName)
	})

	t.Run("a fresh pool shard applies twice", func(t *testing.T) {
		s := newShard("up-fresh", nil)
		upgrade(s, fs("10Gi", ""))
		reconcile(t, s)
		reconcile(t, s)
		assert.Equal(t, "std", *get(s).Spec.StorageClassName)
	})

	t.Run("explicit class survives the upgrade", func(t *testing.T) {
		s := newShard("up-explicit", fs("10Gi", "fast"))
		reconcile(t, s)
		upgrade(s, fs("10Gi", "fast"))
		reconcile(t, s)
		assert.Equal(t, "fast", *get(s).Spec.StorageClassName)
	})

	t.Run("location names a different class than the claim has", func(t *testing.T) {
		s := newShard("up-differs", fs("10Gi", "fast"))
		reconcile(t, s)
		upgrade(s, fs("10Gi", "std"))
		reconcile(t, s)
		assert.Equal(t, "fast", *get(s).Spec.StorageClassName)
	})

	t.Run("an owned class survives a location that drops it", func(t *testing.T) {
		s := newShard("up-drop", fs("10Gi", "fast"))
		reconcile(t, s)
		upgrade(s, fs("10Gi", ""))
		reconcile(t, s)
		assert.Equal(t, "fast", *get(s).Spec.StorageClassName)
	})

	t.Run("claim with no class gets one and binds", func(t *testing.T) {
		s := newClassless(t, "up-nil-class")
		upgrade(s, fs("10Gi", "fast"))
		reconcile(t, s)
		require.NotNil(t, get(s).Spec.StorageClassName)
		assert.Equal(t, "fast", *get(s).Spec.StorageClassName)
	})

	t.Run("a class given to a classless claim survives a location without one",
		func(t *testing.T) {
			s := newClassless(t, "up-nil-class-then-drop")
			upgrade(s, fs("10Gi", "fast"))
			reconcile(t, s)
			s.Spec.SidecarBackupLocation = fs("10Gi", "")
			reconcile(t, s)
			assert.Equal(t, "fast", *get(s).Spec.StorageClassName)
		})

	t.Run("shrink on a bound claim is clamped, not refused", func(t *testing.T) {
		s := newShard("up-shrink", fs("10Gi", ""))
		reconcile(t, s)
		bind(t, get(s))
		upgrade(s, fs("5Gi", ""))
		reconcile(t, s)
		assert.Equal(t, "10Gi", storage(get(s)))
	})

	t.Run("pending claim keeps its request", func(t *testing.T) {
		s := newShard("up-pending", fs("10Gi", ""))
		reconcile(t, s)
		upgrade(s, fs("30Gi", ""))
		reconcile(t, s)
		assert.Equal(t, "10Gi", storage(get(s)))
	})

	t.Run("a bound claim grows and never shrinks", func(t *testing.T) {
		s := newShard("up-size", fs("10Gi", ""))
		reconcile(t, s)
		bind(t, get(s))
		upgrade(s, fs("30Gi", ""))
		reconcile(t, s)
		assert.Equal(t, "30Gi", storage(get(s)))
		// The resize has not completed (capacity is still 10Gi). Lowering to or
		// below the capacity is refused by the API server, so the request stays.
		s.Spec.SidecarBackupLocation = fs("5Gi", "")
		reconcile(t, s)
		assert.Equal(t, "30Gi", storage(get(s)))
		// Strictly above the capacity is the recovery path and goes through.
		s.Spec.SidecarBackupLocation = fs("20Gi", "")
		reconcile(t, s)
		assert.Equal(t, "20Gi", storage(get(s)))
	})
}
