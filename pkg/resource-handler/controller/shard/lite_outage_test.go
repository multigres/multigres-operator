package shard

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/multigres/multigres/go/common/topoclient"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// Model an outage after opening the lazy etcd connection, during registration.
type liteRegistrationFailure struct {
	topoclient.Store
	err error
}

func (s liteRegistrationFailure) CreateDatabase(context.Context, string, *cm.Database) error {
	return s.err
}
func (s liteRegistrationFailure) Close() error { return nil }

func TestLiteOutageRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                                                     string
		phase                                                    corev1.PodPhase
		noPVC, pendingPVC, deletingPVC, drift, resize, hardError bool
	}{
		{name: "deleted pod"},
		{name: "evicted pod", phase: corev1.PodFailed},
		{name: "completed pod", phase: corev1.PodSucceeded},
		{name: "fresh deployment", noPVC: true},
		{name: "unbound PVC", pendingPVC: true},
		{name: "deleting PVC", deletingPVC: true},
		{name: "running pod with drift", phase: corev1.PodRunning, drift: true},
		{name: "running pod needing resize", phase: corev1.PodRunning, resize: true},
		{name: "pending pod", phase: corev1.PodPending},
		{name: "configuration error", hardError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			shard, pool := newTestLiteShard(), newTestPoolSpec()
			pool.Cells = []multigresv1alpha1.CellName{"z1"}
			shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": pool,
			}
			rendered := reloadTestRendered()
			shard.Annotations = map[string]string{
				metadata.AnnotationPostgresConfigHash: rendered.restartHash,
				metadata.AnnotationPostgresReloadHash: rendered.reloadHash,
			}
			scheme := reloadTestScheme(t)
			pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
			require.NoError(t, err)
			pod.UID = "old-pod"
			pod.Status.Phase = tc.phase
			pod.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}
			if tc.drift {
				pod.Annotations[metadata.AnnotationSpecHash] = "old-spec"
			}
			pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
			require.NoError(t, err)
			pvc.UID = "retained-data"
			pvc.Spec.VolumeName = "existing-volume"
			pvc.Status.Phase = corev1.ClaimBound
			if tc.pendingPVC {
				pvc.Status.Phase = corev1.ClaimPending
			}
			if tc.deletingPVC {
				pvc.DeletionTimestamp = ptr.To(metav1.Now())
				pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
			}
			if tc.resize {
				pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
					{
						Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
						Status: corev1.ConditionTrue,
					},
				}
			}
			objects := []client.Object{shard}
			if tc.phase != "" {
				objects = append(objects, pod)
			}
			if !tc.noPVC {
				objects = append(objects, pvc)
			}
			base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			topoErr := topoclient.NewError(topoclient.Timeout, "/databases/postgres")
			if tc.hardError {
				topoErr = grpcstatus.Error(codes.PermissionDenied, "not authorized")
			}
			r := &ShardReconciler{
				Client: testutil.NewFakeClientWithFailures(base, nil), Scheme: scheme,
				Recorder: record.NewFakeRecorder(20),
				CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
					return liteRegistrationFailure{err: topoErr}, nil
				},
			}
			result, err := r.reconcileLite(ctx, shard, rendered, storageClassCheck{})
			if tc.hardError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, topoUnavailableRequeueDelay, result.RequeueAfter)
			}
			blocked := tc.noPVC || tc.pendingPVC || tc.deletingPVC || tc.hardError
			current := &corev1.Pod{}
			if tc.phase == "" && blocked {
				require.True(
					t,
					apierrors.IsNotFound(base.Get(ctx, client.ObjectKeyFromObject(pod), current)),
				)
			} else {
				if tc.phase == corev1.PodFailed || tc.phase == corev1.PodSucceeded {
					require.True(
						t,
						apierrors.IsNotFound(
							base.Get(ctx, client.ObjectKeyFromObject(pod), current),
						),
					)
					// A second reconcile must recover without waiting for etcd to return.
					result, err = r.reconcileLite(ctx, shard, rendered, storageClassCheck{})
					require.NoError(t, err)
					require.Equal(t, topoUnavailableRequeueDelay, result.RequeueAfter)
				}
				require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(pod), current))
				if tc.phase == "" || tc.phase == corev1.PodFailed ||
					tc.phase == corev1.PodSucceeded {
					require.NotEqual(t, pod.UID, current.UID)
					require.Equal(
						t,
						pvc.Name,
						current.Spec.Volumes[0].PersistentVolumeClaim.ClaimName,
					)
					require.Contains(t, current.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
						Name: DataVolumeName, MountPath: PoolerDirMountPath,
					}, "the database cache must survive on the data PVC")
				} else {
					require.Equal(t, pod.UID, current.UID)
					require.Equal(
						t,
						pod.Annotations,
						current.Annotations,
						"do not roll out or claim a reload while offline",
					)
				}
			}
			currentPVC := &corev1.PersistentVolumeClaim{}
			err = base.Get(ctx, client.ObjectKeyFromObject(pvc), currentPVC)
			if tc.noPVC {
				require.True(t, apierrors.IsNotFound(err))
			} else {
				require.NoError(t, err)
				require.Equal(t, pvc.UID, currentPVC.UID)
				require.Equal(t, pvc.Spec, currentPVC.Spec)
			}
		})
	}
}

func TestLiteRegistrationOutageClassification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		outage bool
	}{
		{"legacy unavailable", errors.New("UNAVAILABLE: no connection available"), true},
		{"context deadline", fmt.Errorf("connect: %w", context.DeadlineExceeded), true},
		{"grpc unavailable", grpcstatus.Error(codes.Unavailable, "offline"), true},
		{"grpc deadline", grpcstatus.Error(codes.DeadlineExceeded, "offline"), true},
		{"topology timeout", fmt.Errorf("write: %w", topoclient.NewError(topoclient.Timeout, "/db")), true},
		{"missing database", topoclient.NewError(topoclient.NoNode, "/db"), false},
		{"permission denied", grpcstatus.Error(codes.PermissionDenied, "denied"), false},
		{"invalid credentials", errors.New("missing TLS Secret"), false},
		{"cancelled", context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, opening := range []bool{true, false} {
				r := &ShardReconciler{
					Recorder: record.NewFakeRecorder(10),
					CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
						if opening {
							return nil, tc.err
						}
						return liteRegistrationFailure{err: tc.err}, nil
					},
				}
				registered, err := r.registerLiteDatabase(t.Context(), newTestLiteShard())
				require.False(t, registered)
				if tc.outage {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			}
		})
	}
}
