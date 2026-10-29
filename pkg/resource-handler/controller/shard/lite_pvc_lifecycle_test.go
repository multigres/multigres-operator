package shard

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/gc"
	pvcgc "github.com/multigres/multigres-operator/pkg/gc/pvc"
	pvcutil "github.com/multigres/multigres-operator/pkg/util/pvc"
)

func litePVCLifecycleFixture(
	t *testing.T,
	offline bool,
) (*ShardReconciler, *multigresv1alpha1.Shard, *corev1.PersistentVolumeClaim) {
	t.Helper()
	shard, pool := newTestLiteShard(), newTestPoolSpec()
	pool.Cells = []multigresv1alpha1.CellName{"z1"}
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{"main": pool}
	shard.Spec.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
		WhenDeleted: multigresv1alpha1.DeletePVCRetentionPolicy,
	}
	shard.Spec.SidecarBackupLocation = &multigresv1alpha1.BackupConfig{
		Type: multigresv1alpha1.BackupTypeFilesystem,
	}
	scheme := reloadTestScheme(t)
	pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, true, scheme)
	require.NoError(t, err)
	pvc.UID = "existing-data"
	pvc.Spec.VolumeName = "existing-volume"
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = pvc.Spec.Resources.Requests.DeepCopy()
	backup, err := BuildSharedBackupPVC(shard, true, scheme)
	require.NoError(t, err)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pvc, backup).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
	if offline {
		r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return liteRegistrationFailure{
				err: topoclient.NewError(topoclient.Timeout, "/databases/postgres"),
			}, nil
		}
	} else {
		store, factory := memorytopo.NewServerAndFactory(t.Context(), "z1")
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		r.CreateTopoStore = func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return topoclient.NewWithFactory(
				factory,
				"",
				[]string{""},
				topoclient.NewDefaultTopoConfig(),
			), nil
		}
	}
	return r, shard, pvc
}

func TestLitePVCRetentionPolicyChanges(t *testing.T) {
	for _, topology := range []string{"available", "unavailable"} {
		for _, scope := range []string{"shard", "pool override"} {
			t.Run(topology+"/"+scope, func(t *testing.T) {
				r, shard, pvc := litePVCLifecycleFixture(t, topology == "unavailable")
				for _, policy := range []multigresv1alpha1.PVCRetentionPolicyType{multigresv1alpha1.RetainPVCRetentionPolicy, multigresv1alpha1.DeletePVCRetentionPolicy} {
					if scope == "shard" {
						shard.Spec.PVCDeletionPolicy.WhenDeleted = policy
					} else {
						pool := shard.Spec.Pools["main"]
						pool.PVCDeletionPolicy = &multigresv1alpha1.PVCDeletionPolicy{
							WhenDeleted: policy,
						}
						shard.Spec.Pools["main"] = pool
					}
					_, err := r.reconcileLite(
						t.Context(),
						shard,
						reloadTestRendered(),
						storageClassCheck{},
					)
					require.NoError(t, err)
					current := &corev1.PersistentVolumeClaim{}
					require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(pvc), current))
					require.Equal(
						t,
						policy == multigresv1alpha1.DeletePVCRetentionPolicy,
						metav1.IsControlledBy(current, shard),
						"data PVC owner must follow %s",
						policy,
					)
					require.Equal(t, pvc.UID, current.UID)
					require.NoError(
						t,
						r.Get(
							t.Context(),
							client.ObjectKey{
								Namespace: shard.Namespace,
								Name:      BuildSharedBackupPVCName(shard),
							},
							current,
						),
					)
					require.Equal(
						t,
						scope == "pool override" ||
							policy == multigresv1alpha1.DeletePVCRetentionPolicy,
						metav1.IsControlledBy(current, shard),
						"backup PVC must follow shard-level policy",
					)
				}
			})
		}
	}
}

func TestLiteReusesOrphanPVCBeforePodCreation(t *testing.T) {
	for _, topology := range []string{"available", "unavailable"} {
		for _, failPatch := range []bool{false, true} {
			name := topology
			if failPatch {
				name += "/patch failure"
			}
			t.Run(name, func(t *testing.T) {
				r, shard, pvc := litePVCLifecycleFixture(t, topology == "unavailable")
				now := time.Now()
				// Shard cleanup removes its ownership and marks the retained volume for GC.
				require.NoError(
					t,
					pvcutil.MarkOrphan(
						t.Context(),
						r.Client,
						pvc,
						shard.UID,
						now.Add(-48*time.Hour),
					),
				)
				require.NoError(t, r.Delete(t.Context(), shard))
				shard.UID = "replacement-shard"
				shard.ResourceVersion = ""
				require.NoError(t, r.Create(t.Context(), shard))
				keeper := pvcgc.New(
					r.Client,
					logr.Discard(),
					gc.Options{
						DryRun:    true,
						Retention: 24 * time.Hour,
						Now:       func() time.Time { return now },
					},
				)
				before, err := keeper.Clean(t.Context())
				require.NoError(t, err)
				require.Equal(t, 1, before.WouldDelete)
				// The old shared backup claim is not involved in this data-PVC regression.
				backup := &corev1.PersistentVolumeClaim{}
				require.NoError(
					t,
					r.Get(
						t.Context(),
						client.ObjectKey{
							Namespace: shard.Namespace,
							Name:      BuildSharedBackupPVCName(shard),
						},
						backup,
					),
				)
				backup.OwnerReferences = nil
				require.NoError(t, r.Update(t.Context(), backup))
				base := r.Client.(client.WithWatch)
				podCreated := false
				r.Client = interceptor.NewClient(base, interceptor.Funcs{
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						if failPatch && obj.GetName() == pvc.Name {
							return errors.New("PVC patch rejected")
						}
						return c.Patch(ctx, obj, patch, opts...)
					},
					Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						if _, ok := obj.(*corev1.Pod); ok {
							current := &corev1.PersistentVolumeClaim{}
							require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), current))
							require.False(
								t,
								pvcutil.HasOrphanLabel(current),
								"clear the orphan label before starting a pod",
							)
							podCreated = true
						}
						return c.Create(ctx, obj, opts...)
					},
				})
				_, err = r.reconcileLite(
					t.Context(),
					shard,
					reloadTestRendered(),
					storageClassCheck{},
				)
				if failPatch {
					require.ErrorContains(t, err, "PVC patch rejected")
					require.False(t, podCreated)
					require.True(
						t,
						apierrors.IsNotFound(
							base.Get(
								t.Context(),
								client.ObjectKey{
									Namespace: shard.Namespace,
									Name:      BuildPoolPodName(shard, "main", "z1", 0),
								},
								&corev1.Pod{},
							),
						),
					)
					return
				}
				require.NoError(t, err)
				require.True(t, podCreated)
				current := &corev1.PersistentVolumeClaim{}
				require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(pvc), current))
				require.False(t, pvcutil.HasOrphanLabel(current))
				require.True(t, metav1.IsControlledBy(current, shard))
				require.Equal(t, pvc.UID, current.UID)
				require.Equal(t, pvc.Spec, current.Spec)
				after, err := keeper.Clean(t.Context())
				require.NoError(t, err)
				require.Zero(
					t,
					after.WouldDelete,
					"GC must no longer select the reused data volume",
				)
			})
		}
	}
}
