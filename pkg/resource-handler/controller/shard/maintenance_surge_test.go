package shard

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"

	"github.com/multigres/testkit/assert"
)

func TestMaintenanceSurgeLifecycleForRollingUpdate(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	scheme := maintenanceSurgeTestScheme(t)
	shard := maintenanceSurgeTestShard()
	poolName := "primary"
	cellName := "zone-a"
	pool := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	target, err := BuildPoolPod(shard, poolName, cellName, pool, 0, scheme)
	ck.NoError(err, "build target pod")
	target.Annotations[metadata.AnnotationSpecHash] = "stale"
	setReady(target, true)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(shard, target).
		Build()
	r := &ShardReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	active, acted, err := r.reconcileCellMaintenanceSurge(
		t.Context(),
		shard,
		poolName,
		cellName,
		pool,
		map[string]*corev1.Pod{target.Name: target},
		map[string]*corev1.PersistentVolumeClaim{},
		1,
		&shardRolloutTracker{},
	)
	ck.NoError(err, "create maintenance surge")
	ck.False(
		!acted || active != 0,
		"create result = active %d, acted %v; want 0, true",
		active,
		acted,
	)

	surgeName := BuildPoolPodName(shard, poolName, cellName, 1)
	surge := &corev1.Pod{}
	ck.NoError(c.Get(
		t.Context(),
		types.NamespacedName{Name: surgeName, Namespace: shard.Namespace},
		surge,
	), "get maintenance surge")
	if !isMaintenanceSurge(surge) {
		t.Fatalf("pod %s is missing the maintenance surge annotation", surge.Name)
	}
	setReady(surge, true)
	ck.NoError(c.Status().Update(t.Context(), surge), "mark maintenance surge ready")

	localPods, localPVCs := getLocalPoolObjects(t, c, shard, poolName, cellName)
	active, acted, err = r.reconcileCellMaintenanceSurge(
		t.Context(), shard, poolName, cellName, pool, localPods, localPVCs, 1,
		&shardRolloutTracker{},
	)
	ck.NoError(err, "retain maintenance surge")
	ck.False(
		acted || active != 1,
		"retain result = active %d, acted %v; want 1, false",
		active,
		acted,
	)

	target = localPods[target.Name]
	desiredTarget, err := BuildPoolPod(shard, poolName, cellName, pool, 0, scheme)
	ck.NoError(err, "build desired target")
	base := target.DeepCopy()
	desiredHash := desiredTarget.Annotations[metadata.AnnotationSpecHash]
	target.Annotations[metadata.AnnotationSpecHash] = desiredHash
	ck.NoError(c.Patch(t.Context(), target, client.MergeFrom(base)), "mark target current")

	localPods, localPVCs = getLocalPoolObjects(t, c, shard, poolName, cellName)
	active, acted, err = r.reconcileCellMaintenanceSurge(
		t.Context(), shard, poolName, cellName, pool, localPods, localPVCs, 1,
		&shardRolloutTracker{},
	)
	ck.NoError(err, "release maintenance surge")
	ck.False(
		acted || active != 0,
		"release result = active %d, acted %v; want 0, false",
		active,
		acted,
	)
}

func TestExplicitMaintenanceRequestWaitsForSurge(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	scheme := maintenanceSurgeTestScheme(t)
	shard := maintenanceSurgeTestShard()
	poolName := "primary"
	cellName := "zone-a"
	pool := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	target, err := BuildPoolPod(shard, poolName, cellName, pool, 0, scheme)
	ck.NoError(err, "build target pod")
	target.Annotations[metadata.AnnotationMaintenanceRequested] = maintenanceAnnotationTrue
	setReady(target, true)
	peer, err := BuildPoolPod(shard, poolName, "zone-b", pool, 0, scheme)
	ck.NoError(err, "build peer pod")
	setReady(peer, true)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(shard, target, peer).
		Build()
	r := &ShardReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, acted, err := r.reconcileCellMaintenanceSurge(
		t.Context(),
		shard,
		poolName,
		cellName,
		pool,
		map[string]*corev1.Pod{target.Name: target},
		map[string]*corev1.PersistentVolumeClaim{},
		1,
		&shardRolloutTracker{},
	)
	ck.False(
		err != nil || !acted,
		"create explicit maintenance surge: acted %v, err %v",
		acted,
		err,
	)
	updatedTarget := &corev1.Pod{}
	ck.NoError(
		c.Get(t.Context(), client.ObjectKeyFromObject(target), updatedTarget),
		"get target before surge readiness",
	)
	ck.Eq(
		"",
		updatedTarget.Annotations[metadata.AnnotationMaintenanceReady],
		"maintenance request became ready before the surge was ready",
	)

	surge := &corev1.Pod{}
	ck.NoError(c.Get(
		t.Context(),
		types.NamespacedName{
			Name:      BuildPoolPodName(shard, poolName, cellName, 1),
			Namespace: shard.Namespace,
		},
		surge,
	), "get explicit maintenance surge")
	setReady(surge, true)
	ck.NoError(c.Status().Update(t.Context(), surge), "mark explicit maintenance surge ready")

	localPods, localPVCs := getLocalPoolObjects(t, c, shard, poolName, cellName)
	_, acted, err = r.reconcileCellMaintenanceSurge(
		t.Context(), shard, poolName, cellName, pool, localPods, localPVCs, 1,
		&shardRolloutTracker{},
	)
	ck.False(err != nil || !acted, "publish maintenance readiness: acted %v, err %v", acted, err)
	ck.NoError(
		c.Get(t.Context(), client.ObjectKeyFromObject(target), updatedTarget),
		"get maintenance-ready target",
	)
	ck.Eq(
		maintenanceAnnotationTrue,
		updatedTarget.Annotations[metadata.AnnotationMaintenanceReady],
		"maintenance readiness was not published after the surge became ready",
	)
}

func TestScaleUpPromotesMaintenanceSurgesToDesiredCapacity(t *testing.T) {
	t.Parallel()
	ck := assert.NewAborting(t)
	scheme := maintenanceSurgeTestScheme(t)
	shard := maintenanceSurgeTestShard()
	poolName := "primary"
	pool := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	pool.ReplicasPerCell = ptr.To(int32(2))
	shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)] = pool
	shard.Spec.Replicas = ptr.To(int32(4))

	objects := []client.Object{shard}
	for _, cellName := range []string{"zone-a", "zone-b"} {
		for index := 0; index < 2; index++ {
			pod, err := BuildPoolPod(shard, poolName, cellName, pool, index, scheme)
			ck.NoError(err, "build pooler %s/%d", cellName, index)
			if index == 1 {
				pod.Annotations[metadata.AnnotationMaintenanceSurge] = maintenanceAnnotationTrue
			}
			setReady(pod, true)
			objects = append(objects, pod)
		}
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(objects...).
		Build()
	r := &ShardReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	// The PDB must treat deterministic indices 0 and 1 as the four desired
	// replicas immediately, even before stale surge annotations are cleaned up.
	ck.NoError(r.reconcileShardPDB(t.Context(), shard), "reconcile shard PDB")
	pdb, err := BuildShardPodDisruptionBudget(shard, scheme)
	ck.NoError(err, "build shard PDB")
	ck.NoError(c.Get(t.Context(), client.ObjectKeyFromObject(pdb), pdb), "get shard PDB")
	ck.Eq(3, pdb.Spec.MinAvailable.IntValue(), "minAvailable after scale-up")

	localPods, localPVCs := getLocalPoolObjects(t, c, shard, poolName, "zone-a")
	active, acted, err := r.reconcileCellMaintenanceSurge(
		t.Context(),
		shard,
		poolName,
		"zone-a",
		pool,
		localPods,
		localPVCs,
		2,
		&shardRolloutTracker{},
	)
	ck.NoError(err, "promote surge after scale-up")
	ck.False(
		!acted || active != 0,
		"promotion result = active %d, acted %v; want 0, true",
		active,
		acted,
	)
	promoted := &corev1.Pod{}
	key := types.NamespacedName{
		Name:      BuildPoolPodName(shard, poolName, "zone-a", 1),
		Namespace: shard.Namespace,
	}
	ck.NoError(c.Get(t.Context(), key, promoted), "get promoted pooler")
	ck.False(
		isMaintenanceSurge(promoted),
		"desired pooler retained the maintenance surge annotation",
	)
}

func maintenanceSurgeTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":      corev1.AddToScheme,
		"policy":    policyv1.AddToScheme,
		"multigres": multigresv1alpha1.AddToScheme,
	} {
		assert.NewAborting(t).NoError(add(scheme), "add %s scheme", name)
	}
	return scheme
}

func maintenanceSurgeTestShard() *multigresv1alpha1.Shard {
	return &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-shard",
			Namespace: "default",
			UID:       types.UID("shard-uid"),
			Labels: map[string]string{
				metadata.LabelMultigresCluster: "test-cluster",
			},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:     "postgres",
			TableGroupName:   "default",
			ShardName:        "0-inf",
			DurabilityPolicy: multiCellAtLeast2Policy,
			Replicas:         ptr.To(int32(2)),
			Pools: map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
				"primary": {
					Cells:           []multigresv1alpha1.CellName{"zone-a", "zone-b"},
					ReplicasPerCell: ptr.To(int32(1)),
					Storage:         multigresv1alpha1.StorageSpec{Size: "10Gi"},
				},
			},
		},
	}
}

func getLocalPoolObjects(
	t *testing.T,
	c client.Client,
	shard *multigresv1alpha1.Shard,
	poolName string,
	cellName string,
) (map[string]*corev1.Pod, map[string]*corev1.PersistentVolumeClaim) {
	t.Helper()
	ck := assert.NewAborting(t)
	labels := buildPoolLabelsWithCell(shard, poolName, cellName)
	selector := client.MatchingLabels(metadata.GetSelectorLabels(labels))
	pods := &corev1.PodList{}
	ck.NoError(
		c.List(t.Context(), pods, client.InNamespace(shard.Namespace), selector),
		"list local pods",
	)
	pvcs := &corev1.PersistentVolumeClaimList{}
	ck.NoError(
		c.List(t.Context(), pvcs, client.InNamespace(shard.Namespace), selector),
		"list local PVCs",
	)
	podsByName := make(map[string]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		podsByName[pods.Items[i].Name] = &pods.Items[i]
	}
	pvcsByName := make(map[string]*corev1.PersistentVolumeClaim, len(pvcs.Items))
	for i := range pvcs.Items {
		pvcsByName[pvcs.Items[i].Name] = &pvcs.Items[i]
	}
	return podsByName, pvcsByName
}

func setReady(pod *corev1.Pod, ready bool) {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
}
