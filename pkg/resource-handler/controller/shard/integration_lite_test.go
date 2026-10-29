//go:build integration
// +build integration

package shard_test

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	cm "github.com/multigres/multigres/go/pb/clustermetadata"
	md "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	shardcontroller "github.com/multigres/multigres-operator/pkg/resource-handler/controller/shard"
	"github.com/multigres/multigres-operator/pkg/testutil"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// newLiteTestShard returns a Lite shard with one pool in one cell and
// filesystem backups, as admission leaves it.
func newLiteTestShard(name, clusterName string) *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{metadata.LabelMultigresCluster: clusterName},
		},
		Spec: multigresv1alpha1.ShardSpec{
			Mode:           multigresv1alpha1.ModeLite,
			DatabaseName:   "testdb",
			TableGroupName: "default",
			ShardName:      "0-inf",
			LogLevels: multigresv1alpha1.ComponentLogLevels{
				Pgctld:       "info",
				Multipooler:  "info",
				Multiorch:    "info",
				Multiadmin:   "info",
				Multigateway: "info",
			},
			Multiorch: multigresv1alpha1.MultiorchSpec{
				Cells: []multigresv1alpha1.CellName{"zone1"},
			},
			Images: multigresv1alpha1.ShardImages{
				Multiorch:   "ghcr.io/multigres/multigres:main",
				Multipooler: "ghcr.io/multigres/multigres:main",
				Postgres:    "postgres:17",
			},
			GlobalTopoServer: multigresv1alpha1.GlobalTopoServerRef{
				Address: "global-topo:2379", RootPath: "/multigres/global", Implementation: "etcd",
			},
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"default": {
					Cells:           []multigresv1alpha1.CellName{"zone1"},
					Type:            "readWrite",
					ReplicasPerCell: ptr.To(int32(1)),
					Storage:         multigresv1alpha1.StorageSpec{Size: "1Gi"},
				},
			},
			Backup: &multigresv1alpha1.BackupConfig{
				Type: multigresv1alpha1.BackupTypeFilesystem,
				Filesystem: &multigresv1alpha1.FilesystemBackupConfig{
					Path:    "/backups",
					Storage: multigresv1alpha1.StorageSpec{Size: "1Gi"},
				},
			},
		},
	}
}

// liteTopo returns an in-memory topology and a store factory for the shard
// controller, so the test can read the records the controller writes.
func liteTopo(
	t *testing.T,
) (topoclient.Store, func(*multigresv1alpha1.Shard) (topoclient.Store, error)) {
	t.Helper()
	store, factory := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return topoclient.NewWithFactory(
			factory,
			"",
			[]string{""},
			topoclient.NewDefaultTopoConfig(),
		), nil
	}
}

func TestReconcileLite(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	ctx := t.Context()
	k8sClient := testutil.SetUpClient(t, mgr.GetConfig(), scheme)

	const clusterName = "lite-cluster"
	shard := newLiteTestShard("lite-shard", clusterName)

	store, topoFactory := liteTopo(t)
	var topoOffline atomic.Bool
	outageFactory := func(s *multigresv1alpha1.Shard) (topoclient.Store, error) {
		if topoOffline.Load() {
			return nil, topoclient.NewError(topoclient.Timeout, "/databases/testdb")
		}
		return topoFactory(s)
	}
	if err := (&shardcontroller.ShardReconciler{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		Scheme:          mgr.GetScheme(),
		Recorder:        mgr.GetEventRecorderFor("shard-controller"),
		CreateTopoStore: outageFactory,
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	setTestPostgresPasswordSecretRef(shard)
	createTestPostgresPasswordSecret(t, ctx, k8sClient, shard.Namespace)
	require.NoError(t, k8sClient.Create(ctx, shard))

	podKey := types.NamespacedName{
		Namespace: "default",
		Name:      shardcontroller.BuildPoolPodName(shard, "default", "zone1", 0),
	}
	pvcKey := types.NamespacedName{
		Namespace: "default",
		Name:      shardcontroller.BuildPoolDataPVCName(shard, "default", "zone1", 0),
	}

	pod := &corev1.Pod{}
	require.Eventually(t, func() bool {
		return k8sClient.Get(ctx, podKey, pod) == nil
	}, 30*time.Second, 200*time.Millisecond, "Lite pod was not created")

	// The shard registered its database before creating the pod, with the
	// cells, backup location and durability policy from its spec.
	db, err := store.GetDatabase(ctx, "testdb")
	require.NoError(t, err, "database was not registered in topology")
	require.Equal(t, []string{"zone1"}, db.GetCells())
	require.Equal(t, "/backups", db.GetBackupLocation().GetFilesystem().GetPath())
	require.Equal(t, "AT_LEAST_2", db.GetBootstrapDurabilityPolicy().GetPolicyName())
	require.Equal(t, metadata.GatewayRoleLite, pod.Labels[metadata.LabelMultigresGatewayRole])
	require.Empty(t, pod.Spec.ReadinessGates)
	require.Equal(t, "minigres", pod.Spec.Containers[0].Name)

	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, k8sClient.Get(ctx, pvcKey, pvc))
	originalPVCUID := pvc.UID
	require.Equal(t,
		[]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
		pvc.Spec.AccessModes,
	)
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{
		Namespace: "default",
		Name:      shardcontroller.BuildSharedBackupPVCName(shard),
	}, &corev1.PersistentVolumeClaim{}), "shared backup PVC the pod mounts")

	// The non-Lite path creates these before any pool pod, so their absence
	// once the pod exists shows that path did not run.
	byCluster := client.MatchingLabels{metadata.LabelMultigresCluster: clusterName}
	pdbs := &policyv1.PodDisruptionBudgetList{}
	require.NoError(t, k8sClient.List(ctx, pdbs, client.InNamespace("default"), byCluster))
	require.Empty(t, pdbs.Items, "Lite shard must not have a PodDisruptionBudget")
	deployments := &appsv1.DeploymentList{}
	require.NoError(t, k8sClient.List(ctx, deployments, client.InNamespace("default"), byCluster))
	require.Empty(t, deployments.Items, "Lite shard must not have a multiorch Deployment")
	services := &corev1.ServiceList{}
	require.NoError(t, k8sClient.List(ctx, services, client.InNamespace("default"), byCluster))
	require.Empty(t, services.Items, "Lite shard must not have multiorch or headless Services")

	// Model a bound volume: envtest has no storage controller. Minigres keeps
	// topo-database.pb in this volume alongside its PostgreSQL data.
	pvc.Status.Phase = corev1.ClaimBound
	require.NoError(t, k8sClient.Status().Update(ctx, pvc))
	topoOffline.Store(true)

	// A kubelet eviction must recover while etcd is unreachable, even though
	// the desired hash is unchanged.
	evictedUID := pod.UID
	evictedHash := pod.Annotations[metadata.AnnotationSpecHash]
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = "Evicted"
	require.NoError(t, k8sClient.Status().Update(ctx, pod))
	require.Eventually(t, func() bool {
		return k8sClient.Get(ctx, podKey, pod) == nil && pod.UID != evictedUID &&
			pod.Annotations[metadata.AnnotationSpecHash] == evictedHash
	}, 30*time.Second, 200*time.Millisecond, "evicted Lite pod was not replaced")

	// A deleted pod must also be recreated while etcd remains down, retaining
	// the same data PVC. This exercises the full controller, not just a builder.
	deletedUID := pod.UID
	require.NoError(t, k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0)))
	require.Eventually(t, func() bool {
		return k8sClient.Get(ctx, podKey, pod) == nil && pod.UID != deletedUID &&
			pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName == pvcKey.Name
	}, 30*time.Second, 200*time.Millisecond, "Lite pod was not recreated during topology outage")
	require.NoError(t, k8sClient.Get(ctx, pvcKey, pvc))
	require.Equal(t, originalPVCUID, pvc.UID)

	// Registration resumes after recovery. Removing this memory-topology
	// record makes the retry observable without changing the Shard spec.
	require.NoError(t, store.DeleteDatabase(ctx, "testdb", true))
	topoOffline.Store(false)
	require.Eventually(t, func() bool {
		_, err := store.GetDatabase(ctx, "testdb")
		return err == nil
	}, 30*time.Second, 200*time.Millisecond, "Lite topology registration did not resume")

	// A CSI filesystem-resize condition must trigger replacement even when
	// the desired pod hash is unchanged.
	beforeResizeUID := pod.UID
	beforeResizeHash := pod.Annotations[metadata.AnnotationSpecHash]
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}
	require.NoError(t, k8sClient.Status().Update(ctx, pod))
	require.NoError(t, k8sClient.Get(ctx, pvcKey, pvc))
	pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{
		Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
		Status: corev1.ConditionTrue,
	}}
	require.NoError(t, k8sClient.Status().Update(ctx, pvc))
	require.Eventually(t, func() bool {
		return k8sClient.Get(ctx, podKey, pod) == nil && pod.UID != beforeResizeUID &&
			pod.Annotations[metadata.AnnotationSpecHash] == beforeResizeHash
	}, 30*time.Second, 200*time.Millisecond, "Lite pod was not replaced for filesystem expansion")
	resizeReplacementUID := pod.UID
	require.Never(t, func() bool {
		return k8sClient.Get(ctx, podKey, pod) != nil || pod.UID != resizeReplacementUID ||
			!pod.DeletionTimestamp.IsZero()
	}, time.Second, 100*time.Millisecond, "replacement was deleted before it could mount the volume")
	require.NoError(t, k8sClient.Get(ctx, pvcKey, pvc))
	pvc.Status.Conditions = nil
	require.NoError(t, k8sClient.Status().Update(ctx, pvc))

	// A spec change that moves the spec-hash replaces the pod.
	originalUID := pod.UID
	originalHash := pod.Annotations[metadata.AnnotationSpecHash]
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &multigresv1alpha1.Shard{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(shard), current); err != nil {
			return err
		}
		base := current.DeepCopy()
		current.Spec.Images.Multipooler = "ghcr.io/multigres/multigres:next"
		return k8sClient.Patch(ctx, current, client.MergeFrom(base))
	}))

	require.Eventually(t, func() bool {
		replaced := &corev1.Pod{}
		if err := k8sClient.Get(ctx, podKey, replaced); err != nil {
			return false
		}
		return replaced.UID != originalUID &&
			replaced.Annotations[metadata.AnnotationSpecHash] != originalHash &&
			replaced.Spec.Containers[0].Image == "ghcr.io/multigres/multigres:next"
	}, 30*time.Second, 200*time.Millisecond, "Lite pod was not replaced after a spec change")

	// The data PVC survives the replacement.
	replacedPVC := &corev1.PersistentVolumeClaim{}
	require.NoError(t, k8sClient.Get(ctx, pvcKey, replacedPVC))
	require.Equal(t, originalPVCUID, replacedPVC.UID)

	// Deletion runs the shared handleDeletion path, which removes the pod and
	// then the finalizer without a multiorch or PDB to wait for.
	require.NoError(t, k8sClient.Delete(ctx, shard))
	require.Eventually(t, func() bool {
		return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(shard),
			&multigresv1alpha1.Shard{})) &&
			apierrors.IsNotFound(k8sClient.Get(ctx, podKey, &corev1.Pod{}))
	}, 30*time.Second, 200*time.Millisecond, "Lite shard and pod were not deleted")
}

// markLitePodRunning keeps the Lite pod running with an IP and Ready, standing
// in for the kubelet envtest does not run.
func markLitePodRunning(ctx context.Context, c client.Client, key types.NamespacedName) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pod := &corev1.Pod{}
			if err := c.Get(ctx, key, pod); err != nil || podReadyForRolloutTest(pod) {
				continue
			}
			pod.Status.Phase = corev1.PodRunning
			pod.Status.PodIP = "10.0.0.7"
			pod.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}
			_ = c.Status().Update(ctx, pod)
		}
	}
}

func TestLiteStatusReachesAvailableAndPrimary(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	mgr := testutil.SetUpEnvtestManager(t, scheme,
		testutil.WithCRDPaths(filepath.Join("../../../../", "config", "crd", "bases")),
	)
	ctx := t.Context()
	k8sClient := testutil.SetUpClient(t, mgr.GetConfig(), scheme)

	shard := newLiteTestShard("lite-status-shard", "lite-status-cluster")
	podName := shardcontroller.BuildPoolPodName(shard, "default", "zone1", 0)
	podKey := types.NamespacedName{Namespace: "default", Name: podName}

	store, topoFactory := liteTopo(t)

	rpc := rpcclient.NewFakeClient()
	pooler := &cm.Multipooler{
		Id: &cm.ID{
			Component: cm.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      shardcontroller.BuildPoolServiceID(podName),
		},
		Hostname: "10.0.0.7",
		PortMap:  map[string]int32{"grpc": shardcontroller.DefaultMultipoolerGRPCPort},
		ShardRef: &cm.ShardRef{Database: "testdb", TableGroup: "default", Shard: "0-inf"},
	}
	require.NoError(t, store.RegisterMultipooler(ctx, pooler, false))
	rpc.ReloadConfigResponses[topoclient.ComponentIDString(pooler.Id)] = &md.ReloadConfigResponse{
		ConfigLoadTime: timestamppb.Now(),
	}
	rpc.SetStatusResponse(topoclient.ComponentIDString(&cm.ID{
		Component: cm.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      shardcontroller.BuildPoolServiceID(podName),
	}), &md.StatusResponse{Status: &md.Status{PoolerType: cm.PoolerType_PRIMARY}})

	if err := (&shardcontroller.ShardReconciler{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		Scheme:          mgr.GetScheme(),
		Recorder:        mgr.GetEventRecorderFor("shard-controller"),
		PoolerClients:   poolerclient.Static(rpc),
		CreateTopoStore: topoFactory,
	}).SetupWithManager(mgr, controller.Options{SkipNameValidation: ptr.To(true)}); err != nil {
		t.Fatalf("Failed to create controller: %v", err)
	}

	setTestPostgresPasswordSecretRef(shard)
	createTestPostgresPasswordSecret(t, ctx, k8sClient, shard.Namespace)
	require.NoError(t, k8sClient.Create(ctx, shard))

	conditionStatus := func(s *multigresv1alpha1.Shard, condType string) metav1.ConditionStatus {
		if cond := meta.FindStatusCondition(s.Status.Conditions, condType); cond != nil {
			return cond.Status
		}
		return ""
	}

	// Before the pod runs, the shard reports it as neither available nor primary.
	require.Eventually(t, func() bool {
		got := &multigresv1alpha1.Shard{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(shard), got); err != nil {
			return false
		}
		return k8sClient.Get(ctx, podKey, &corev1.Pod{}) == nil &&
			conditionStatus(got, "Available") == metav1.ConditionFalse &&
			conditionStatus(got, multigresv1alpha1.ConditionPrimary) == metav1.ConditionFalse
	}, 30*time.Second, 200*time.Millisecond, "Lite shard did not report its pending pod")

	go markLitePodRunning(ctx, k8sClient, podKey)

	got := &multigresv1alpha1.Shard{}
	require.Eventually(t, func() bool {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(shard), got); err != nil {
			return false
		}
		return got.Status.ObservedGeneration == got.Generation &&
			conditionStatus(got, "Available") == metav1.ConditionTrue &&
			conditionStatus(got, multigresv1alpha1.ConditionPrimary) == metav1.ConditionTrue &&
			conditionStatus(got, multigresv1alpha1.ConditionUpToDate) == metav1.ConditionTrue
	}, 30*time.Second, 200*time.Millisecond, "Lite shard did not become Available and Primary")

	require.Equal(t, multigresv1alpha1.PhaseHealthy, got.Status.Phase)
	require.True(t, got.Status.PoolsReady)
	require.Equal(t, int32(1), got.Status.ReadyReplicas)

	before := &corev1.Pod{}
	require.NoError(t, k8sClient.Get(ctx, podKey, before))
	setInlineConfig(t, ctx, k8sClient, shard.Name, "work_mem", "8MB")
	waitForConfigMapContains(t, ctx, k8sClient, shard.Name, "work_mem = '8MB'")
	require.Eventually(t, func() bool {
		after := &corev1.Pod{}
		if k8sClient.Get(ctx, podKey, after) != nil ||
			k8sClient.Get(ctx, client.ObjectKeyFromObject(shard), got) != nil {
			return false
		}
		return after.UID == before.UID && after.DeletionTimestamp.IsZero() &&
			after.Annotations[metadata.AnnotationSpecHash] == before.Annotations[metadata.AnnotationSpecHash] &&
			after.Annotations[metadata.AnnotationPostgresReloadHash] != before.Annotations[metadata.AnnotationPostgresReloadHash] &&
			got.Status.ObservedGeneration == got.Generation &&
			conditionStatus(got, multigresv1alpha1.ConditionUpToDate) == metav1.ConditionTrue
	}, 30*time.Second, 200*time.Millisecond, "Lite reload did not converge without replacing the pod")
	require.NotEmpty(t, rpc.GetCallLog())
	require.Contains(
		t,
		rpc.GetCallLog(),
		"ReloadConfig("+string(topoclient.ComponentIDString(pooler.Id))+")",
	)
}
