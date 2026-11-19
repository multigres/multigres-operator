package shard

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// newTestBoundShard is a shard bound to the connection "orders-db": one pool of one pod in cell z1, which is what the TableGroup
// builds for it.
func newTestBoundShard() *multigresv1alpha1.Shard {
	shard := newTestShard()
	shard.Name = "test-cluster-postgres-legacy-0-inf"
	shard.Labels[metadata.LabelMultigresDatabase] = "postgres"
	shard.Labels[metadata.LabelMultigresTableGroup] = "legacy"
	shard.Labels[metadata.LabelMultigresShard] = "0-inf"
	shard.Labels[multigresv1alpha1.LabelDynamicTableGroup] = "true"
	shard.Annotations = map[string]string{
		multigresv1alpha1.AnnotationTableGroupID:      "9",
		multigresv1alpha1.AnnotationBackingConnection: "orders-db",
	}
	shard.Spec.TableGroupName = "legacy"
	shard.Spec.Images.Multipooler = "multigres:test"
	shard.Spec.GlobalTopoServer = multigresv1alpha1.GlobalTopoServerRef{
		Address:  "global-topo:2379",
		RootPath: "/multigres/default/test-cluster/global",
	}
	shard.Spec.LogLevels.Multipooler = "info"
	shard.Spec.Pools = map[multigresv1alpha1.PoolName]multigresv1alpha1.PoolSpec{
		"primary": {Cells: []multigresv1alpha1.CellName{"z1"}},
	}
	return shard
}

func boundPoolOf(
	t *testing.T,
	shard *multigresv1alpha1.Shard,
) (string, string, multigresv1alpha1.PoolSpec) {
	t.Helper()
	pool, cell, spec, err := boundPool(shard)
	require.NoError(t, err)
	return pool, cell, spec
}

func buildBound(t *testing.T, shard *multigresv1alpha1.Shard) *corev1.Pod {
	t.Helper()
	pool, cell, spec := boundPoolOf(t, shard)
	pod, err := BuildBoundPoolerPod(shard, pool, cell, spec, testScheme())
	require.NoError(t, err)
	return pod
}

func TestBuildBoundPoolerPod_OnlyThePooler(t *testing.T) {
	shard := newTestBoundShard()
	pod := buildBound(t, shard)

	// The name carries the cluster, database, tablegroup and its catalog id,
	// shard, and cell.
	require.True(
		t,
		strings.HasPrefix(pod.Name, "test-cluster-postgres-legacy-9-0-inf-pooler-z1"),
		pod.Name,
	)
	require.LessOrEqual(t, len(pod.Name), 63)
	require.Equal(t, BoundPoolerComponentName, pod.Labels[metadata.LabelAppComponent])
	require.Equal(t, "legacy", pod.Labels[metadata.LabelMultigresTableGroup])
	require.Equal(t, "z1", pod.Labels[metadata.LabelMultigresCell])
	require.Equal(t, metadata.ManagedByMultigres, pod.Labels[metadata.LabelAppManagedBy])

	// No PostgreSQL, pgctld, exporter, volume, readiness gate or PVC.
	require.Empty(t, pod.Spec.InitContainers)
	require.Len(t, pod.Spec.Containers, 1)
	require.Equal(t, "multipooler", pod.Spec.Containers[0].Name)
	require.Empty(t, pod.Spec.Volumes)
	require.Empty(t, pod.Spec.ReadinessGates)
	require.Empty(t, pod.Spec.Containers[0].VolumeMounts)

	c := pod.Spec.Containers[0]
	require.Equal(t, "multigres:test", c.Image)
	require.Equal(t, []string{
		"multipooler",
		"--http-port=15200",
		"--grpc-port=15270",
		"--service-map=grpc-pooler,grpc-poolermanager,grpc-consensus",
		"--topo-global-server-addresses=global-topo:2379",
		"--topo-global-root=/multigres/default/test-cluster/global",
		"--cell=z1",
		"--database=postgres",
		"--table-group=legacy",
		"--shard=0-inf",
		"--tablegroup-id=9",
		"--service-id=" + BuildPoolServiceID(pod.Name),
		"--hostname=$(POD_IP)",
		"--log-level=info",
		"--grpc-server-keepalive-enforcement-policy-permit-without-stream=true",
	}, c.Args)

	// The pooler refuses every flag that describes a PostgreSQL of its own; it
	// reads its shard's connection from the catalog.
	for _, arg := range c.Args {
		for _, banned := range []string{
			"--pg-port", "--pooler-dir", "--socket-file", "--pgctld-addr",
			"--pgbackrest", "--backing-connection", "--management-mode",
			"--source-connection", "--migration-key-file", "--connpool",
		} {
			require.False(t, strings.HasPrefix(arg, banned), "unmanaged pooler passed %s", arg)
		}
	}
	for _, port := range c.Ports {
		require.NotEqual(t, "postgres", port.Name)
	}
	for _, e := range c.Env {
		require.NotEqual(t, "PGDATA", e.Name)
		require.NotEqual(t, "PGPASSFILE", e.Name)
	}
	require.NotEmpty(t, pod.Annotations[metadata.AnnotationSpecHash])
	require.NotContains(t, pod.Annotations, metadata.AnnotationPrometheusScrape,
		"there is no exporter to scrape")
}

// Neither probe may depend on the external database: a pooler whose database
// is away must stay in its Service and keep running.
func TestBuildBoundPoolerPod_ProbesIgnoreThePostgres(t *testing.T) {
	c := buildBound(t, newTestBoundShard()).Spec.Containers[0]
	require.Equal(t, "/ready", c.ReadinessProbe.HTTPGet.Path)
	require.Equal(t, "/live", c.LivenessProbe.HTTPGet.Path)
	require.Equal(t, "/live", c.StartupProbe.HTTPGet.Path)
	require.Nil(t, c.ReadinessProbe.Exec)
	require.Nil(t, c.LivenessProbe.TCPSocket)
	// The startup wait is longer than a managed pooler's 150 seconds.
	require.Greater(t, c.StartupProbe.FailureThreshold*c.StartupProbe.PeriodSeconds, int32(150))
}

func TestBuildBoundPoolerPod_TLS(t *testing.T) {
	shard := newTestBoundShard()
	shard.Spec.InternalTLS = &multigresv1alpha1.InternalTLSConfig{Enabled: ptr.To(true)}
	pod := buildBound(t, shard)
	c := pod.Spec.Containers[0]

	// It serves with the pooler certificate and dials the default primary
	// with the same material, as multiorch dials poolers.
	for _, want := range []string{
		"--grpc-cert", ShardTLSCertFile,
		"--multipooler-grpc-cert", "--multipooler-grpc-key", "--multipooler-grpc-ca",
		"--multipooler-grpc-server-name", "--multipooler-grpc-require-tls",
	} {
		require.Contains(t, c.Args, want)
	}
	require.Len(t, pod.Spec.Volumes, 1)
	require.Equal(t, ShardTLSVolumeName, pod.Spec.Volumes[0].Name)
	require.Equal(t, []corev1.VolumeMount{shardTLSVolumeMount()}, c.VolumeMounts)
}

func TestBuildBoundPoolerPod_PlacementAndResources(t *testing.T) {
	shard := newTestBoundShard()
	shard.Spec.CellTopologyLabels = map[multigresv1alpha1.CellName]map[string]string{
		"z1": {"topology.kubernetes.io/zone": "us-east-1a"},
	}
	pool := shard.Spec.Pools["primary"]
	pool.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
	pool.Multipooler.RunAsUser = ptr.To(int64(1234))
	shard.Spec.Pools["primary"] = pool

	pod := buildBound(t, shard)
	require.Equal(t, "us-east-1a", pod.Spec.NodeSelector["topology.kubernetes.io/zone"])
	require.Equal(t, pool.Tolerations, pod.Spec.Tolerations)
	require.Equal(t, int64(1234), *pod.Spec.Containers[0].SecurityContext.RunAsUser)
}

func TestBuildBoundPoolerPod_NeedsTheTableGroupID(t *testing.T) {
	shard := newTestBoundShard()
	delete(shard.Annotations, multigresv1alpha1.AnnotationTableGroupID)
	pool, cell, spec := boundPoolOf(t, shard)
	_, err := BuildBoundPoolerPod(shard, pool, cell, spec, testScheme())
	require.ErrorContains(t, err, "tablegroup id")
}

// The spec hash covers what the pooler is told, so a changed image, log level
// or topology address replaces the pod.
func TestBuildBoundPoolerPod_SpecHashFollowsTheSpec(t *testing.T) {
	base := buildBound(t, newTestBoundShard()).Annotations[metadata.AnnotationSpecHash]
	for name, mutate := range map[string]func(*multigresv1alpha1.Shard){
		"log level": func(s *multigresv1alpha1.Shard) { s.Spec.LogLevels.Multipooler = "debug" },
		"image":     func(s *multigresv1alpha1.Shard) { s.Spec.Images.Multipooler = "multigres:next" },
		"topology": func(s *multigresv1alpha1.Shard) {
			s.Spec.GlobalTopoServer.Address = "other:2379"
		},
	} {
		shard := newTestBoundShard()
		mutate(shard)
		got := buildBound(t, shard).Annotations[metadata.AnnotationSpecHash]
		require.NotEqual(t, base, got, name)
	}
	require.Equal(
		t,
		base,
		buildBound(t, newTestBoundShard()).Annotations[metadata.AnnotationSpecHash],
	)
}

func TestBoundPool(t *testing.T) {
	shard := newTestBoundShard()
	shard.Spec.Pools["second"] = multigresv1alpha1.PoolSpec{
		Cells: []multigresv1alpha1.CellName{"z1"},
	}
	_, _, _, err := boundPool(shard)
	require.ErrorContains(t, err, "2 pools")

	shard = newTestBoundShard()
	pool := shard.Spec.Pools["primary"]
	pool.Cells = []multigresv1alpha1.CellName{"z1", "z2"}
	shard.Spec.Pools["primary"] = pool
	_, _, _, err = boundPool(shard)
	require.ErrorContains(t, err, "2 cells")
}

func boundTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	return scheme
}

func newBoundReconciler(t *testing.T, objs ...client.Object) (*ShardReconciler, client.Client) {
	t.Helper()
	scheme := boundTestScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&multigresv1alpha1.Shard{}).
		Build()
	return &ShardReconciler{
		Client:          c,
		Scheme:          scheme,
		Recorder:        record.NewFakeRecorder(100),
		CreateTopoStore: newMemoryTopoFactory(),
	}, c
}

func boundPods(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) []corev1.Pod {
	t.Helper()
	list := &corev1.PodList{}
	require.NoError(t, c.List(t.Context(), list, client.InNamespace(shard.Namespace)))
	return list.Items
}

func TestEnsureBoundPod_CreatesOne(t *testing.T) {
	shard := newTestBoundShard()
	r, c := newBoundReconciler(t, shard)
	pool, cell, spec := boundPoolOf(t, shard)

	require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))
	pods := boundPods(t, c, shard)
	require.Len(t, pods, 1)
	require.Equal(t, BuildBoundPoolerPodName(shard, "z1"), pods[0].Name)
	require.True(t, metav1.IsControlledBy(&pods[0], shard))

	// Again: nothing changes.
	require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))
	require.Len(t, boundPods(t, c, shard), 1)
}

// A changed spec replaces the pod, and the replacement is created only after
// the old one is gone: the pooler is never in two places.
func TestEnsureBoundPod_ReplacesWithoutOverlap(t *testing.T) {
	shard := newTestBoundShard()
	r, c := newBoundReconciler(t, shard)
	pool, cell, spec := boundPoolOf(t, shard)
	require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))
	original := boundPods(t, c, shard)[0]

	shard.Spec.Images.Multipooler = "multigres:next"
	require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))
	// The old pod is deleted; no second one was created beside it.
	require.Empty(t, boundPods(t, c, shard), "the replacement must wait for the old pod to be gone")

	require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))
	pods := boundPods(t, c, shard)
	require.Len(t, pods, 1)
	require.NotEqual(t, original.Annotations[metadata.AnnotationSpecHash],
		pods[0].Annotations[metadata.AnnotationSpecHash])
	require.Equal(t, "multigres:next", pods[0].Spec.Containers[0].Image)
}

// While the old pod is terminating, nothing is created, whatever else changed.
func TestEnsureBoundPod_WaitsForATerminatingPod(t *testing.T) {
	shard := newTestBoundShard()
	pod := buildBound(t, shard)
	pod.Finalizers = []string{"test/hold"}
	r, c := newBoundReconciler(t, shard, pod)
	pool, cell, spec := boundPoolOf(t, shard)
	require.NoError(t, c.Delete(t.Context(), pod))

	shard.Spec.Images.Multipooler = "multigres:next"
	require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))

	pods := boundPods(t, c, shard)
	require.Len(t, pods, 1, "no replacement beside a pod that is still terminating")
	require.False(t, pods[0].DeletionTimestamp.IsZero())
	require.Equal(t, pod.Annotations[metadata.AnnotationSpecHash],
		pods[0].Annotations[metadata.AnnotationSpecHash])
}

func TestEnsureBoundPod_ReplacesTerminalAndStrayPods(t *testing.T) {
	for name, tc := range map[string]func(*corev1.Pod){
		"failed": func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed },
		"succeeded": func(p *corev1.Pod) {
			p.Status.Phase = corev1.PodSucceeded
		},
		// A pod of the same shard under another name, as a cell change or an
		// older naming would leave.
		"stray": func(p *corev1.Pod) { p.Name = "stray-pooler" },
	} {
		t.Run(name, func(t *testing.T) {
			shard := newTestBoundShard()
			pod := buildBound(t, shard)
			tc(pod)
			r, c := newBoundReconciler(t, shard, pod)
			pool, cell, spec := boundPoolOf(t, shard)

			require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))
			require.Empty(t, boundPods(t, c, shard))
			require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))
			pods := boundPods(t, c, shard)
			require.Len(t, pods, 1)
			require.Equal(t, BuildBoundPoolerPodName(shard, "z1"), pods[0].Name)
		})
	}
}

// Pods of other shards, and of the pool machinery, are never touched.
func TestEnsureBoundPod_LeavesOthersAlone(t *testing.T) {
	shard := newTestBoundShard()
	other := newTestBoundShard()
	other.Name = "other"
	other.Spec.TableGroupName = "other"
	other.Labels[metadata.LabelMultigresTableGroup] = "other"
	otherPod := buildBound(t, other)
	managed, err := BuildPoolPod(newTestShard(), "main", "z1", newTestPoolSpec(), 0, testScheme())
	require.NoError(t, err)
	r, c := newBoundReconciler(t, shard, otherPod, managed)
	pool, cell, spec := boundPoolOf(t, shard)

	require.NoError(t, r.ensureBoundPod(t.Context(), shard, pool, cell, spec))
	require.Len(t, boundPods(t, c, shard), 3)
	for _, p := range []*corev1.Pod{otherPod, managed} {
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(p), &corev1.Pod{}))
	}
}

func TestReconcileBound_RoutesAroundTheManagedMachinery(t *testing.T) {
	shard := newTestBoundShard()
	r, c := newBoundReconciler(t, shard)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(shard)}

	res, err := r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	require.Zero(t, res.RequeueAfter)

	// The pod, and nothing else of what a managed shard has.
	require.Len(t, boundPods(t, c, shard), 1)
	for _, list := range []client.ObjectList{
		&appsv1.DeploymentList{}, &corev1.ServiceList{}, &corev1.ConfigMapList{},
		&corev1.SecretList{}, &corev1.PersistentVolumeClaimList{}, &policyv1.PodDisruptionBudgetList{},
	} {
		require.NoError(t, c.List(t.Context(), list, client.InNamespace(shard.Namespace)))
		require.Zero(t, countItems(t, list), "%T", list)
	}

	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(t.Context(), req.NamespacedName, got))
	require.Contains(t, got.Finalizers, shardFinalizer)
	require.Equal(t, multigresv1alpha1.PhaseProgressing, got.Status.Phase)
	require.False(t, got.Status.PoolsReady)

	// The pod becomes ready: the shard is healthy and counts as a pool of one.
	pod := boundPods(t, c, shard)[0]
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}
	require.NoError(t, c.Status().Update(t.Context(), &pod))
	_, err = r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), req.NamespacedName, got))
	require.Equal(t, multigresv1alpha1.PhaseHealthy, got.Status.Phase)
	require.True(t, got.Status.PoolsReady)
	require.EqualValues(t, 1, got.Status.ReadyReplicas)
	require.Equal(t, []multigresv1alpha1.CellName{"z1"}, got.Status.Cells)
}

func countItems(t *testing.T, list client.ObjectList) int {
	t.Helper()
	switch l := list.(type) {
	case *appsv1.DeploymentList:
		return len(l.Items)
	case *corev1.ServiceList:
		return len(l.Items)
	case *corev1.ConfigMapList:
		return len(l.Items)
	case *corev1.SecretList:
		return len(l.Items)
	case *corev1.PersistentVolumeClaimList:
		return len(l.Items)
	case *policyv1.PodDisruptionBudgetList:
		return len(l.Items)
	}
	t.Fatalf("unhandled list %T", list)
	return 0
}

func TestReconcileBound_CrashLoopingPodIsDegraded(t *testing.T) {
	shard := newTestBoundShard()
	pod := buildBound(t, shard)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "multipooler",
		RestartCount: 5,
		State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
		},
	}}
	r, c := newBoundReconciler(t, shard, pod)
	_, err := r.Reconcile(
		t.Context(),
		ctrl.Request{NamespacedName: client.ObjectKeyFromObject(shard)},
	)
	require.NoError(t, err)

	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(shard), got))
	require.Equal(t, multigresv1alpha1.PhaseDegraded, got.Status.Phase)
}

func TestReconcileBound_PendingDeletionRemovesThePodThenReleasesTheShard(t *testing.T) {
	shard := newTestBoundShard()
	shard.Finalizers = []string{shardFinalizer}
	shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = "now"
	pod := buildBound(t, shard)
	pod.Finalizers = []string{"test/hold"}
	r, c := newBoundReconciler(t, shard, pod)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(shard)}

	// First pass: the pod is deleted and the shard waits for it.
	res, err := r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, boundPodGoneRequeue, res.RequeueAfter)
	pods := boundPods(t, c, shard)
	require.Len(t, pods, 1)
	require.False(t, pods[0].DeletionTimestamp.IsZero())
	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(t.Context(), req.NamespacedName, got))
	require.False(t, readyForDeletion(got))

	// Still terminating: still waiting.
	res, err = r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, boundPodGoneRequeue, res.RequeueAfter)

	// Gone: the shard is released for deletion.
	pods[0].Finalizers = nil
	require.NoError(t, c.Update(t.Context(), &pods[0]))
	_, err = r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), req.NamespacedName, got))
	require.True(t, readyForDeletion(got))
	require.Empty(t, boundPods(t, c, shard))
}

func TestReconcileBound_StuckTerminatingPodDoesNotHoldDeletionForever(t *testing.T) {
	shard := newTestBoundShard()
	shard.Finalizers = []string{shardFinalizer}
	shard.Annotations[multigresv1alpha1.AnnotationPendingDeletion] = "now"
	pod := buildBound(t, shard)
	pod.Finalizers = []string{"test/hold"}
	stuck := metav1.NewTime(time.Now().Add(-2 * terminatingTimeout))
	pod.DeletionTimestamp = &stuck
	r, c := newBoundReconciler(t, shard, pod)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(shard)}

	_, err := r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	got := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(t.Context(), req.NamespacedName, got))
	require.True(t, readyForDeletion(got))
}

// Deleting a bound shard deletes its pod through the same path as any shard:
// the pod carries the labels that path selects on.
func TestHandleDeletion_RemovesTheBoundPod(t *testing.T) {
	shard := newTestBoundShard()
	shard.Finalizers = []string{shardFinalizer}
	shard.DeletionTimestamp = ptr.To(metav1.Now())
	pod := buildBound(t, shard)
	r, c := newBoundReconciler(t, shard, pod)

	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(shard)}
	res, err := r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, podTerminationRequeueDelay, res.RequeueAfter, "waits for the pod to be gone")
	err = c.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
	require.True(t, apierrors.IsNotFound(err), "the pod must be deleted with its shard: %v", err)

	// Gone: the finalizer is released.
	res, err = r.Reconcile(t.Context(), req)
	require.NoError(t, err)
	require.Zero(t, res.RequeueAfter)
}

func readyForDeletion(s *multigresv1alpha1.Shard) bool {
	return slices.ContainsFunc(s.Status.Conditions, func(c metav1.Condition) bool {
		return c.Type == multigresv1alpha1.ConditionReadyForDeletion &&
			c.Status == metav1.ConditionTrue
	})
}
