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
)

func TestMaintenanceSurgeLifecycleForRollingUpdate(t *testing.T) {
	t.Parallel()
	scheme := maintenanceSurgeTestScheme(t)
	shard := maintenanceSurgeTestShard()
	poolName := "primary"
	cellName := "zone-a"
	pool := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	target, err := BuildPoolPod(shard, poolName, cellName, pool, 0, scheme)
	if err != nil {
		t.Fatalf("build target pod: %v", err)
	}
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
	if err != nil {
		t.Fatalf("create maintenance surge: %v", err)
	}
	if !acted || active != 0 {
		t.Fatalf("create result = active %d, acted %v; want 0, true", active, acted)
	}

	surgeName := BuildPoolPodName(shard, poolName, cellName, 1)
	surge := &corev1.Pod{}
	if err := c.Get(
		t.Context(),
		types.NamespacedName{Name: surgeName, Namespace: shard.Namespace},
		surge,
	); err != nil {
		t.Fatalf("get maintenance surge: %v", err)
	}
	if !isMaintenanceSurge(surge) {
		t.Fatalf("pod %s is missing the maintenance surge annotation", surge.Name)
	}
	setReady(surge, true)
	if err := c.Status().Update(t.Context(), surge); err != nil {
		t.Fatalf("mark maintenance surge ready: %v", err)
	}

	localPods, localPVCs := getLocalPoolObjects(t, c, shard, poolName, cellName)
	active, acted, err = r.reconcileCellMaintenanceSurge(
		t.Context(), shard, poolName, cellName, pool, localPods, localPVCs, 1,
		&shardRolloutTracker{},
	)
	if err != nil {
		t.Fatalf("retain maintenance surge: %v", err)
	}
	if acted || active != 1 {
		t.Fatalf("retain result = active %d, acted %v; want 1, false", active, acted)
	}

	target = localPods[target.Name]
	desiredTarget, err := BuildPoolPod(shard, poolName, cellName, pool, 0, scheme)
	if err != nil {
		t.Fatalf("build desired target: %v", err)
	}
	base := target.DeepCopy()
	desiredHash := desiredTarget.Annotations[metadata.AnnotationSpecHash]
	target.Annotations[metadata.AnnotationSpecHash] = desiredHash
	if err := c.Patch(t.Context(), target, client.MergeFrom(base)); err != nil {
		t.Fatalf("mark target current: %v", err)
	}

	localPods, localPVCs = getLocalPoolObjects(t, c, shard, poolName, cellName)
	active, acted, err = r.reconcileCellMaintenanceSurge(
		t.Context(), shard, poolName, cellName, pool, localPods, localPVCs, 1,
		&shardRolloutTracker{},
	)
	if err != nil {
		t.Fatalf("release maintenance surge: %v", err)
	}
	if acted || active != 0 {
		t.Fatalf("release result = active %d, acted %v; want 0, false", active, acted)
	}
}

func TestExplicitMaintenanceRequestWaitsForSurge(t *testing.T) {
	t.Parallel()
	scheme := maintenanceSurgeTestScheme(t)
	shard := maintenanceSurgeTestShard()
	poolName := "primary"
	cellName := "zone-a"
	pool := shard.Spec.Pools[multigresv1alpha1.PoolName(poolName)]
	target, err := BuildPoolPod(shard, poolName, cellName, pool, 0, scheme)
	if err != nil {
		t.Fatalf("build target pod: %v", err)
	}
	target.Annotations[metadata.AnnotationMaintenanceRequested] = maintenanceAnnotationTrue
	setReady(target, true)
	peer, err := BuildPoolPod(shard, poolName, "zone-b", pool, 0, scheme)
	if err != nil {
		t.Fatalf("build peer pod: %v", err)
	}
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
	if err != nil || !acted {
		t.Fatalf("create explicit maintenance surge: acted %v, err %v", acted, err)
	}
	updatedTarget := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(target), updatedTarget); err != nil {
		t.Fatalf("get target before surge readiness: %v", err)
	}
	if updatedTarget.Annotations[metadata.AnnotationMaintenanceReady] != "" {
		t.Fatal("maintenance request became ready before the surge was ready")
	}

	surge := &corev1.Pod{}
	if err := c.Get(
		t.Context(),
		types.NamespacedName{
			Name:      BuildPoolPodName(shard, poolName, cellName, 1),
			Namespace: shard.Namespace,
		},
		surge,
	); err != nil {
		t.Fatalf("get explicit maintenance surge: %v", err)
	}
	setReady(surge, true)
	if err := c.Status().Update(t.Context(), surge); err != nil {
		t.Fatalf("mark explicit maintenance surge ready: %v", err)
	}

	localPods, localPVCs := getLocalPoolObjects(t, c, shard, poolName, cellName)
	_, acted, err = r.reconcileCellMaintenanceSurge(
		t.Context(), shard, poolName, cellName, pool, localPods, localPVCs, 1,
		&shardRolloutTracker{},
	)
	if err != nil || !acted {
		t.Fatalf("publish maintenance readiness: acted %v, err %v", acted, err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(target), updatedTarget); err != nil {
		t.Fatalf("get maintenance-ready target: %v", err)
	}
	if updatedTarget.Annotations[metadata.AnnotationMaintenanceReady] != maintenanceAnnotationTrue {
		t.Fatal("maintenance readiness was not published after the surge became ready")
	}
}

func TestScaleUpPromotesMaintenanceSurgesToDesiredCapacity(t *testing.T) {
	t.Parallel()
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
			if err != nil {
				t.Fatalf("build pooler %s/%d: %v", cellName, index, err)
			}
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
	if err := r.reconcileShardPDB(t.Context(), shard); err != nil {
		t.Fatalf("reconcile shard PDB: %v", err)
	}
	pdb, err := BuildShardPodDisruptionBudget(shard, scheme)
	if err != nil {
		t.Fatalf("build shard PDB: %v", err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(pdb), pdb); err != nil {
		t.Fatalf("get shard PDB: %v", err)
	}
	if got := pdb.Spec.MinAvailable.IntValue(); got != 3 {
		t.Fatalf("minAvailable after scale-up = %d, want 3", got)
	}

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
	if err != nil {
		t.Fatalf("promote surge after scale-up: %v", err)
	}
	if !acted || active != 0 {
		t.Fatalf("promotion result = active %d, acted %v; want 0, true", active, acted)
	}
	promoted := &corev1.Pod{}
	key := types.NamespacedName{
		Name:      BuildPoolPodName(shard, poolName, "zone-a", 1),
		Namespace: shard.Namespace,
	}
	if err := c.Get(t.Context(), key, promoted); err != nil {
		t.Fatalf("get promoted pooler: %v", err)
	}
	if isMaintenanceSurge(promoted) {
		t.Fatal("desired pooler retained the maintenance surge annotation")
	}
}

func maintenanceSurgeTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":      corev1.AddToScheme,
		"policy":    policyv1.AddToScheme,
		"multigres": multigresv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s scheme: %v", name, err)
		}
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
			Images: multigresv1alpha1.ShardImages{
				PgBackRestd: "registry/multigres@sha256:abc",
			},
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
	labels := buildPoolLabelsWithCell(shard, poolName, cellName)
	selector := client.MatchingLabels(metadata.GetSelectorLabels(labels))
	pods := &corev1.PodList{}
	if err := c.List(t.Context(), pods, client.InNamespace(shard.Namespace), selector); err != nil {
		t.Fatalf("list local pods: %v", err)
	}
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := c.List(t.Context(), pvcs, client.InNamespace(shard.Namespace), selector); err != nil {
		t.Fatalf("list local PVCs: %v", err)
	}
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
