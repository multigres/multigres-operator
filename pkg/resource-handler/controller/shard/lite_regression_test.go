package shard

import (
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

func TestLiteTerminalPodsAreReplaced(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded, corev1.PodRunning, corev1.PodUnknown} {
		t.Run(string(phase), func(t *testing.T) {
			shard, pool := newTestLiteShard(), newTestPoolSpec()
			scheme := reloadTestScheme(t)
			pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
			require.NoError(t, err)
			pod.UID = "original-pod"
			pod.Status.Phase = phase
			if phase == corev1.PodFailed {
				pod.Status.Reason = "Evicted"
			}
			pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
			require.NoError(t, err)
			pvc.UID = "original-pvc"
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, pvc).Build()
			r := &ShardReconciler{
				Client:   withShardOwner(c),
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}
			require.NoError(t, r.ensureLitePod(t.Context(), shard, "main", "z1", pool))
			current := &corev1.Pod{}
			err = c.Get(t.Context(), client.ObjectKeyFromObject(pod), current)
			if phase == corev1.PodFailed || phase == corev1.PodSucceeded {
				require.True(
					t,
					apierrors.IsNotFound(err),
					"terminal pod with matching hash must be removed: %v",
					err,
				)
				require.NoError(t, r.ensureLitePod(t.Context(), shard, "main", "z1", pool))
				require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pod), current))
				require.NotEqual(t, pod.UID, current.UID)
				require.Equal(
					t,
					pod.Annotations[metadata.AnnotationSpecHash],
					current.Annotations[metadata.AnnotationSpecHash],
				)
			} else {
				require.NoError(t, err)
				require.Equal(
					t,
					pod.UID,
					current.UID,
					"a nonterminal pod must not be replaced just because it is unavailable",
				)
			}
			currentPVC := &corev1.PersistentVolumeClaim{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pvc), currentPVC))
			require.Equal(t, pvc.UID, currentPVC.UID)
		})
	}
}

func TestLiteExistingPVCExpandsInPlace(t *testing.T) {
	for _, size := range []string{"20Gi", "10Gi", "5Gi"} {
		t.Run(size, func(t *testing.T) {
			shard, pool := newTestLiteShard(), newTestPoolSpec()
			scheme := reloadTestScheme(t)
			pool.Storage.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
			pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
			require.NoError(t, err)
			pvc.UID = "existing-volume"
			pvc.Spec.VolumeName = "bound-volume"
			pvc.Status.Capacity = corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
			r := &ShardReconciler{
				Client:   withShardOwner(c),
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}
			pool.Storage.Size = size
			wait, err := r.ensureLiteDataPVC(t.Context(), shard, "main", "z1", pool)
			require.NoError(t, err)
			if size == "20Gi" {
				require.Equal(t, liteStorageRequeue, wait)
			} else {
				require.Zero(t, wait)
			}
			got := &corev1.PersistentVolumeClaim{}
			require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pvc), got))
			want := "10Gi"
			if size == "20Gi" {
				want = size
			}
			require.Equal(t, 0, got.Spec.Resources.Requests.Storage().Cmp(resource.MustParse(want)))
			require.Equal(t, pvc.UID, got.UID)
			require.Equal(t, pvc.Spec.VolumeName, got.Spec.VolumeName)
			require.Equal(t, pvc.Spec.AccessModes, got.Spec.AccessModes)
		})
	}
}

func TestLiteFilesystemResizeReplacesReadyPod(t *testing.T) {
	for _, tt := range []struct {
		name        string
		condition   corev1.ConditionStatus
		ready       bool
		terminating bool
		replace     bool
	}{
		{name: "pending resize", condition: corev1.ConditionTrue, ready: true, replace: true},
		{name: "resize complete", condition: corev1.ConditionFalse, ready: true},
		{name: "resize unknown", condition: corev1.ConditionUnknown, ready: true},
		{name: "no resize condition", ready: true},
		{name: "pod still starting", condition: corev1.ConditionTrue},
		{name: "pod already terminating", condition: corev1.ConditionTrue, ready: true, terminating: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			shard, pool := newTestLiteShard(), newTestPoolSpec()
			scheme := reloadTestScheme(t)
			pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
			require.NoError(t, err)
			pod.UID = "original-pod"
			if tt.ready {
				pod.Status.Phase = corev1.PodRunning
				pod.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				}
			}
			if tt.terminating {
				pod.DeletionTimestamp = ptr.To(metav1.Now())
				pod.Finalizers = []string{"example.com/test-finalizer"}
			}
			pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
			require.NoError(t, err)
			pvc.UID = "original-pvc"
			pvc.Status.Capacity = corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("20Gi"),
			}
			if tt.condition != "" {
				pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{
					Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
					Status: tt.condition,
				}}
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, pvc).Build()
			r := &ShardReconciler{
				Client:   withShardOwner(c),
				Scheme:   scheme,
				Recorder: record.NewFakeRecorder(10),
			}
			pool.Storage.Size = "20Gi"
			wait, err := r.ensureLiteDataPVC(ctx, shard, "main", "z1", pool)
			require.NoError(t, err)
			if tt.condition == corev1.ConditionTrue {
				require.Equal(t, liteStorageRequeue, wait)
			} else {
				require.Zero(t, wait)
			}
			require.NoError(t, r.ensureLitePod(ctx, shard, "main", "z1", pool))
			current := &corev1.Pod{}
			err = c.Get(ctx, client.ObjectKeyFromObject(pod), current)
			if tt.replace {
				require.True(
					t,
					apierrors.IsNotFound(err),
					"ready pod must restart for a pending filesystem resize: %v",
					err,
				)
				require.NoError(t, r.ensureLitePod(ctx, shard, "main", "z1", pool))
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pod), current))
				require.NotEqual(t, pod.UID, current.UID)
				require.Equal(
					t,
					pod.Annotations[metadata.AnnotationSpecHash],
					current.Annotations[metadata.AnnotationSpecHash],
				)
				// The CSI condition may remain true while the replacement mounts
				// the volume. Let that pod start instead of deleting it again.
				require.NoError(t, r.ensureLitePod(ctx, shard, "main", "z1", pool))
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pod), current))
			} else {
				require.NoError(t, err)
				require.Equal(t, pod.UID, current.UID)
			}
			currentPVC := &corev1.PersistentVolumeClaim{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(pvc), currentPVC))
			require.Equal(t, pvc.UID, currentPVC.UID)
			require.Equal(
				t,
				0,
				currentPVC.Spec.Resources.Requests.Storage().Cmp(resource.MustParse("20Gi")),
			)
		})
	}
}

func TestLiteInternalTLSOnlyConfiguresAdminServer(t *testing.T) {
	shard := newTestLiteShard()
	shard.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}
	pod, err := BuildLitePod(shard, "main", "z1", newTestPoolSpec(), testScheme())
	require.NoError(t, err)
	c := pod.Spec.Containers[0]
	for flag, value := range map[string]string{
		"--grpc-cert":      ShardTLSCertFile,
		"--grpc-key":       ShardTLSKeyFile,
		"--grpc-ca":        ShardTLSCAFile,
		"--grpc-server-ca": ShardTLSCAFile,
	} {
		assertFlagValue(t, c.Args, flag, value)
	}
	for _, arg := range c.Args {
		require.False(t, strings.HasPrefix(arg, "--multipooler-grpc-"),
			"Minigres rejects remote-pooler TLS flags: %s", arg)
	}
	assertReadOnlyVolumeMount(t, c.VolumeMounts, ShardTLSVolumeName, ShardTLSMountPath)
}

func TestLiteReloadConvergesWithoutReplacingPod(t *testing.T) {
	for _, outcome := range []string{"applied", "expansion pending", "not synced", "needs restart", "stale address"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := t.Context()
			shard, pool := newTestLiteShard(), newTestPoolSpec()
			pool.Cells = []multigresv1alpha1.CellName{"z1"}
			shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"main": pool,
			}
			meta.SetStatusCondition(
				&shard.Status.Conditions,
				metav1.Condition{
					Type:   multigresv1alpha1.ConditionPrimary,
					Status: metav1.ConditionTrue,
					Reason: "TestPrimary",
				},
			)
			rendered := reloadTestRendered()
			rendered.content = "work_mem = '32MB'\n"
			shard.Annotations = map[string]string{
				metadata.AnnotationPostgresConfigHash: rendered.restartHash,
				metadata.AnnotationPostgresReloadHash: "old-reload",
			}
			scheme := reloadTestScheme(t)
			pod, err := BuildLitePod(shard, "main", "z1", pool, scheme)
			require.NoError(t, err)
			pod.UID = "running-pod"
			pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "192.0.2.10"}
			store, factory := memorytopo.NewServerAndFactory(ctx, "z1")
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			target := &cm.Multipooler{
				Id: &cm.ID{
					Component: cm.ID_MULTIPOOLER,
					Cell:      "z1",
					Name:      BuildPoolServiceID(pod.Name),
				},
				Hostname: pod.Status.PodIP,
				PortMap:  map[string]int32{"grpc": DefaultMultipoolerGRPCPort},
				ShardRef: &cm.ShardRef{
					Database:   string(shard.Spec.DatabaseName),
					TableGroup: string(shard.Spec.TableGroupName),
					Shard:      string(shard.Spec.ShardName),
				},
			}
			if outcome == "stale address" {
				target.Hostname = "192.0.2.11"
			}
			require.NoError(t, store.RegisterMultipooler(ctx, target, false))
			rpc := rpcclient.NewFakeClient()
			response := &md.ReloadConfigResponse{}
			if outcome == "applied" || outcome == "expansion pending" {
				response.ConfigLoadTime = timestamppb.Now()
			}
			if outcome == "needs restart" {
				response.NeedsRestart = true
			}
			rpc.ReloadConfigResponses[topoclient.ComponentIDString(target.Id)] = response
			pvc, err := BuildPoolDataPVC(shard, "main", "z1", pool, 0, false, scheme)
			require.NoError(t, err)
			pvc.Status.Capacity = corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(pool.Storage.Size),
			}
			if outcome == "expansion pending" {
				pvc.Status.Capacity[corev1.ResourceStorage] = resource.MustParse("5Gi")
			}
			base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod, pvc).Build()
			r := &ShardReconciler{
				Client: withShardOwner(testutil.NewFakeClientWithFailures(
					base,
					nil,
				)),
				Scheme:        scheme,
				Recorder:      record.NewFakeRecorder(20),
				PoolerClients: poolerclient.Static(rpc),
				CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
					return topoclient.NewWithFactory(
						factory,
						"",
						[]string{""},
						topoclient.NewDefaultTopoConfig(),
					), nil
				},
			}
			condition := r.liteUpToDateCondition(shard, "main", "z1", pool, rendered, pod)
			require.Equal(
				t,
				metav1.ConditionFalse,
				condition.Status,
				"a pending reload must not report UpToDate",
			)
			result, err := r.reconcileLite(ctx, shard, rendered, storageClassCheck{})
			require.NoError(t, err)
			current := &corev1.Pod{}
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(pod), current))
			require.Equal(t, pod.UID, current.UID)
			require.True(t, current.DeletionTimestamp.IsZero())
			require.Equal(
				t,
				pod.Annotations[metadata.AnnotationSpecHash],
				current.Annotations[metadata.AnnotationSpecHash],
			)
			require.Equal(
				t,
				outcome != "stale address",
				callLogHas(rpc.GetCallLog(), "ReloadConfig"),
			)
			condition = r.liteUpToDateCondition(shard, "main", "z1", pool, rendered, current)
			if outcome == "applied" || outcome == "expansion pending" {
				require.Equal(
					t,
					rendered.reloadHash,
					current.Annotations[metadata.AnnotationPostgresReloadHash],
				)
				require.Equal(t, metav1.ConditionTrue, condition.Status)
				if outcome == "expansion pending" {
					require.Equal(t, liteStorageRequeue, result.RequeueAfter)
				} else {
					require.Zero(t, result.RequeueAfter)
				}
			} else {
				require.Equal(
					t,
					"old-reload",
					current.Annotations[metadata.AnnotationPostgresReloadHash],
				)
				require.Equal(t, metav1.ConditionFalse, condition.Status)
				require.Equal(t, reloadRetryDelay, result.RequeueAfter)
			}
		})
	}
}
