package shard

import (
	"slices"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	multigresv1alpha1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
)

// registerPooler writes a pooler record for postgres/commerce/<shard> as a
// pooler run with --tablegroup-id=id does.
func registerPooler(t *testing.T, store topoclient.Store, name, shard string, id int64) {
	t.Helper()
	mp := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      name,
		},
		Hostname: name,
		ShardRef: &clustermetadatapb.ShardRef{
			Database:   "postgres",
			TableGroup: "commerce",
			Shard:      shard,
		},
		TablegroupId: id,
	}
	require.NoError(t, store.RegisterMultipooler(t.Context(), mp, false))
}

func poolerNames(t *testing.T, store topoclient.Store) []string {
	t.Helper()
	poolers, err := store.GetMultipoolersByCell(t.Context(), "cell1", nil)
	require.NoError(t, err)
	names := []string{}
	for _, p := range poolers {
		names = append(names, p.Id.Name)
	}
	slices.Sort(names)
	return names
}

// deletedCluster is the shards' MultigresCluster while it is being deleted.
func deletedCluster() *multigresv1alpha1.MultigresCluster {
	now := metav1.Now()
	return &multigresv1alpha1.MultigresCluster{ObjectMeta: metav1.ObjectMeta{
		Name: "c", Namespace: "default", DeletionTimestamp: &now, Finalizers: []string{"test/hold"},
	}}
}

// droppedShardFixture is a deleted Shard of dynamic tablegroup id 7, the
// labels its pool pods carry, and a topology holding one pooler record of
// that id.
func droppedShardFixture(t *testing.T) (
	*runtime.Scheme, *multigresv1alpha1.Shard, map[string]string, topoclient.Store,
	func(*multigresv1alpha1.Shard) (topoclient.Store, error),
) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = multigresv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	podLabels := map[string]string{
		metadata.LabelMultigresCluster:    "c",
		metadata.LabelMultigresDatabase:   "postgres",
		metadata.LabelMultigresTableGroup: "commerce",
		metadata.LabelMultigresShard:      "0-80",
		metadata.LabelAppComponent:        PoolComponentName,
		metadata.LabelAppManagedBy:        metadata.ManagedByMultigres,
	}
	now := metav1.Now()
	shard := &multigresv1alpha1.Shard{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s", Namespace: "default",
			Labels: map[string]string{
				metadata.LabelMultigresCluster:           "c",
				multigresv1alpha1.LabelDynamicTableGroup: "true",
			},
			Annotations:       map[string]string{multigresv1alpha1.AnnotationTableGroupID: "7"},
			DeletionTimestamp: &now,
			Finalizers:        []string{shardFinalizer},
		},
		Spec: multigresv1alpha1.ShardSpec{
			DatabaseName:   "postgres",
			TableGroupName: "commerce",
			ShardName:      "0-80",
		},
	}
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	newStore := func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
		return topoclient.NewWithFactory(
			factory, "", []string{""}, topoclient.NewDefaultTopoConfig(),
		), nil
	}
	store, _ := newStore(nil)
	t.Cleanup(func() { _ = store.Close() })
	registerPooler(t, store, "this-incarnation", "0-80", 7)
	return scheme, shard, podLabels, store, newStore
}

// shardExists reports whether the Shard is still there, which it is for as
// long as its finalizer is.
func shardExists(t *testing.T, c client.Client, shard *multigresv1alpha1.Shard) bool {
	t.Helper()
	err := c.Get(t.Context(), client.ObjectKeyFromObject(shard), &multigresv1alpha1.Shard{})
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

// A dropped tablegroup's shard lets go (unregisters its pooler records and
// removes its finalizer, after which the provision claim is released and the
// drop completes) only once no pool pod exists. Every other Shard deletion
// treats a pod stuck terminating past terminatingTimeout as gone.
func TestHandleDeletion_DynamicShardWaitsForPoolerPods(t *testing.T) {
	const running, stuck = "running", "stuck"
	pod := func(labels map[string]string, state string) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "pooler-0", Namespace: "default", Labels: labels,
		}}
		if state == stuck {
			p.DeletionTimestamp = &metav1.Time{Time: time.Now().Add(-2 * terminatingTimeout)}
			p.Finalizers = []string{"test/hold"}
		}
		return p
	}
	kept := []string{"this-incarnation"}

	tests := map[string]struct {
		clusterDeleting bool
		// cachedPod is in the informer cache and on the API server; serverPod
		// is on the API server only. Empty means no such pod.
		cachedPod, serverPod string
		wantHeld             bool
		wantRecords          []string
		wantEvent            string
	}{
		"no pod left: the drop goes through": {
			wantRecords: []string{},
		},
		"a pod stuck terminating holds the drop": {
			cachedPod:   stuck,
			wantHeld:    true,
			wantRecords: kept,
			wantEvent:   "StuckTerminating",
		},
		"a stuck pod stops holding the shard once the cluster is being deleted": {
			clusterDeleting: true,
			cachedPod:       stuck,
			wantRecords:     kept, // topology goes with the cluster
		},
		"a pod the cache has not listed yet holds the drop": {
			serverPod:   running,
			wantHeld:    true,
			wantRecords: kept,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme, shard, podLabels, store, newStore := droppedShardFixture(t)
			cached := []client.Object{shard}
			var onServer []client.Object
			if tc.clusterDeleting {
				cached = append(cached, deletedCluster())
			}
			if tc.cachedPod != "" {
				cached = append(cached, pod(podLabels, tc.cachedPod))
				onServer = append(onServer, pod(podLabels, tc.cachedPod))
			}
			if tc.serverPod != "" {
				onServer = append(onServer, pod(podLabels, tc.serverPod))
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cached...).Build()
			rec := record.NewFakeRecorder(20)
			r := &ShardReconciler{
				Client: c,
				APIReader: fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(onServer...).
					Build(),
				Scheme:          scheme,
				Recorder:        rec,
				CreateTopoStore: newStore,
			}

			res, err := r.handleDeletion(t.Context(), shard.DeepCopy())
			require.NoError(t, err)

			assert.Equal(t, tc.wantHeld, res.RequeueAfter > 0, "requeue")
			assert.Equal(t, tc.wantHeld, shardExists(t, c, shard), "shard finalizer kept")
			assert.Equal(t, tc.wantRecords, poolerNames(t, store), "pooler records")
			if tc.wantEvent == "" {
				return
			}
			if assert.NotEmpty(t, rec.Events) {
				assert.Contains(t, <-rec.Events, tc.wantEvent)
			}
		})
	}
}

func TestHandleDeletion_DynamicShardTeardownOrder(t *testing.T) {
	scheme, shard, podLabels, store, newStore := droppedShardFixture(t)
	registerPooler(t, store, "older-incarnation", "0-80", 3)
	registerPooler(t, store, "no-id", "0-80", 0)
	registerPooler(t, store, "other-shard", "80-inf", 7)
	// The pod's own finalizer keeps it terminating after Delete.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:       "pooler-0",
		Namespace:  "default",
		Labels:     podLabels,
		Finalizers: []string{"test/hold"},
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard, pod).Build()
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20), CreateTopoStore: newStore,
	}

	// Pass 1: the pod is still terminating, so topology is untouched.
	res, err := r.handleDeletion(t.Context(), shard.DeepCopy())
	require.NoError(t, err)
	require.NotZero(t, res.RequeueAfter, "did not requeue while a pod was terminating")
	require.Equal(t,
		[]string{"no-id", "older-incarnation", "other-shard", "this-incarnation"},
		poolerNames(t, store), "records changed while a pod remained")

	// Pass 2: the pod is gone.
	got := &corev1.Pod{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(pod), got))
	got.Finalizers = nil
	require.NoError(t, c.Update(t.Context(), got))
	current := &multigresv1alpha1.Shard{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(shard), current))
	_, err = r.handleDeletion(t.Context(), current)
	require.NoError(t, err)

	assert.Equal(t, []string{"no-id", "older-incarnation", "other-shard"},
		poolerNames(t, store), "only id 7 of this shard is deleted")
	assert.False(t, shardExists(t, c, shard), "shard finalizer not removed")
}

func TestHandleDeletion_DynamicShardTopologyErrorKeepsFinalizer(t *testing.T) {
	scheme, shard, _, _, _ := droppedShardFixture(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(shard).Build()
	rec := record.NewFakeRecorder(20)
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: rec,
		CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			return nil, apierrors.NewServiceUnavailable("topology down")
		},
	}

	_, err := r.handleDeletion(t.Context(), shard.DeepCopy())
	require.Error(t, err, "want an error while topology is unreachable")
	if assert.NotEmpty(t, rec.Events) {
		assert.Contains(t, <-rec.Events, "TopologyError")
	}
	assert.True(t, shardExists(t, c, shard), "shard finalizer removed before topology cleanup")
}

func TestHandleDeletion_DynamicShardSkipsTopologyWhileClusterIsDeleted(t *testing.T) {
	scheme, shard, _, _, _ := droppedShardFixture(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deletedCluster(), shard).Build()
	r := &ShardReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20),
		CreateTopoStore: func(*multigresv1alpha1.Shard) (topoclient.Store, error) {
			assert.Fail(t, "topology opened while the cluster is being deleted")
			return nil, apierrors.NewServiceUnavailable("topology down")
		},
	}

	_, err := r.handleDeletion(t.Context(), shard.DeepCopy())
	require.NoError(t, err)
	assert.False(t, shardExists(t, c, shard), "shard finalizer not removed")
}
